//! Module resolution for import declarations.
//!
//! Resolves `import ./path` declarations by loading, parsing, and merging
//! imported modules' declarations into the importing module. Import paths
//! are relative to the importing file.

use knot::ast;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Resolve all imports in the module, recursively loading imported files
/// and merging their declarations. Detects import cycles.
pub fn resolve_imports(
    module: &mut ast::Module,
    source_path: &Path,
) -> Result<(), Vec<String>> {
    let canonical = source_path
        .canonicalize()
        .unwrap_or_else(|_| source_path.to_path_buf());
    let mut in_flight = HashSet::new();
    in_flight.insert(canonical.clone());
    let mut imported = HashSet::new();
    imported.insert(canonical);
    resolve_recursive(module, source_path, &mut in_flight, &mut imported)
}

fn resolve_recursive(
    module: &mut ast::Module,
    source_path: &Path,
    in_flight: &mut HashSet<PathBuf>,
    imported: &mut HashSet<PathBuf>,
) -> Result<(), Vec<String>> {
    if module.imports.is_empty() {
        return Ok(());
    }

    let base_dir = source_path.parent().unwrap_or(Path::new("."));
    let imports = std::mem::take(&mut module.imports);
    let mut errors = Vec::new();
    let mut imported_decls: Vec<ast::Decl> = Vec::new();

    for imp in &imports {
        // Resolve relative path to .knot file
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);

        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(e) => {
                errors.push(format!(
                    "cannot resolve import '{}': {} (resolved to {})",
                    imp.path,
                    e,
                    full_path.display()
                ));
                continue;
            }
        };

        // Cycle detection: only an in-flight import (currently on the
        // resolution stack) constitutes a cycle. A module that was already
        // resolved via another sibling import is *not* a cycle — it's a
        // diamond, and we silently skip the duplicate so its declarations
        // aren't merged twice.
        if in_flight.contains(&canonical) {
            errors.push(format!(
                "import cycle detected: '{}' has already been imported",
                imp.path
            ));
            continue;
        }
        if !imported.insert(canonical.clone()) {
            // Diamond import — already merged via another path.
            continue;
        }
        in_flight.insert(canonical.clone());

        // Read and parse the imported file
        let source = match std::fs::read_to_string(&canonical) {
            Ok(s) => s,
            Err(e) => {
                errors.push(format!(
                    "cannot read import '{}': {}",
                    imp.path, e
                ));
                continue;
            }
        };

        let lexer = knot::lexer::Lexer::new(&source);
        let (tokens, lex_diags) = lexer.tokenize();
        if !lex_diags.is_empty() {
            for diag in &lex_diags {
                errors.push(format!(
                    "in import '{}': {}",
                    imp.path,
                    diag.render(&source, &canonical.display().to_string())
                ));
            }
        }

        let parser = knot::parser::Parser::new(source.clone(), tokens);
        let (mut imported_module, parse_diags) = parser.parse_module();
        let has_parse_errors = parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error);
        if has_parse_errors {
            for diag in &parse_diags {
                errors.push(format!(
                    "in import '{}': {}",
                    imp.path,
                    diag.render(&source, &canonical.display().to_string())
                ));
            }
            continue;
        }

        // Recursively resolve imports of the imported module
        let sub_result = resolve_recursive(
            &mut imported_module,
            &canonical,
            in_flight,
            imported,
        );
        in_flight.remove(&canonical);
        if let Err(sub_errors) = sub_result {
            errors.extend(sub_errors);
            continue;
        }

        // Filter by export visibility: if the imported module has any `export`
        // declarations, only those (plus always-visible items) pass through.
        // If no exports exist, everything is visible (backwards compat).
        let has_exports = imported_module.decls.iter().any(|d| d.exported);
        let visible_decls = if has_exports {
            let exported_names: HashSet<String> = imported_module
                .decls
                .iter()
                .filter(|d| d.exported)
                .filter_map(|d| decl_name(&d.node))
                .collect();
            imported_module
                .decls
                .into_iter()
                .filter(|d| {
                    d.exported
                        || matches!(
                            &d.node,
                            ast::DeclKind::Migrate { .. } | ast::DeclKind::SubsetConstraint { .. }
                        )
                        || matches!(&d.node, ast::DeclKind::Impl { trait_name, .. } if exported_names.contains(trait_name))
                })
                .collect()
        } else {
            imported_module.decls
        };

        // Filter declarations based on selective import list
        let decls: Vec<ast::Decl> = if let Some(items) = &imp.items {
            let mut names: HashSet<&str> = items.iter().map(|i| i.name.as_str()).collect();
            // When a trait is selected, also include data types defined in
            // the same module that the trait or its impls may depend on
            // (e.g., `Ordering` for `Ord`). This prevents compilation
            // failures when imported impls reference data types.
            let data_names: Vec<String> = visible_decls.iter().filter_map(|d| {
                if let ast::DeclKind::Data { name, .. } = &d.node {
                    Some(name.clone())
                } else { None }
            }).collect();
            // Collect additional data type names to include (owned, to avoid borrow conflict)
            let mut extra_data: Vec<String> = Vec::new();
            for d in &visible_decls {
                if let ast::DeclKind::Trait { name: trait_name, items: trait_items, .. } = &d.node {
                    if names.contains(trait_name.as_str()) {
                        for item in trait_items {
                            if let ast::TraitItem::Method { ty, .. } = item {
                                for data_name in &data_names {
                                    if type_references_name(&ty.ty, data_name) {
                                        extra_data.push(data_name.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for name in &extra_data {
                names.insert(name.as_str());
            }
            let decls: Vec<ast::Decl> = visible_decls
                .into_iter()
                .filter(|d| should_include_decl(d, &names))
                .collect();
            // Check for requested names that don't match any declaration
            let found_names: HashSet<String> = decls
                .iter()
                .filter_map(|d| decl_name(&d.node))
                .collect();
            for item in items {
                if !found_names.contains(&item.name) {
                    errors.push(format!(
                        "import '{}': '{}' not found in module",
                        imp.path, item.name
                    ));
                }
            }
            decls
        } else {
            visible_decls
        };

        imported_decls.extend(decls);
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // Prepend imported declarations before the module's own declarations
    imported_decls.append(&mut module.decls);
    module.decls = imported_decls;

    Ok(())
}

/// Extract the primary name from a declaration, if it has one.
fn decl_name(decl: &ast::DeclKind) -> Option<String> {
    match decl {
        ast::DeclKind::Data { name, .. }
        | ast::DeclKind::TypeAlias { name, .. }
        | ast::DeclKind::Source { name, .. }
        | ast::DeclKind::View { name, .. }
        | ast::DeclKind::Derived { name, .. }
        | ast::DeclKind::Fun { name, .. }
        | ast::DeclKind::Trait { name, .. }
        | ast::DeclKind::Route { name, .. }
        | ast::DeclKind::RouteComposite { name, .. } => Some(name.clone()),
        ast::DeclKind::Impl { .. }
        | ast::DeclKind::Migrate { .. }
        | ast::DeclKind::SubsetConstraint { .. }
        | ast::DeclKind::UnitDecl { .. } => None,
    }
}

/// Check if a type AST references a given named type (e.g., data type name).
fn type_references_name(ty: &ast::Type, name: &str) -> bool {
    match &ty.node {
        ast::TypeKind::Named(n) => n == name,
        ast::TypeKind::App { func, arg } => {
            type_references_name(func, name) || type_references_name(arg, name)
        }
        ast::TypeKind::Record { fields, .. } => {
            fields.iter().any(|f| type_references_name(&f.value, name))
        }
        ast::TypeKind::Relation(inner) => type_references_name(inner, name),
        ast::TypeKind::Function { param, result } => {
            type_references_name(param, name) || type_references_name(result, name)
        }
        ast::TypeKind::Variant { constructors, .. } => constructors
            .iter()
            .any(|c| c.fields.iter().any(|f| type_references_name(&f.value, name))),
        ast::TypeKind::Effectful { ty, .. } | ast::TypeKind::IO { ty, .. } => {
            type_references_name(ty, name)
        }
        ast::TypeKind::UnitAnnotated { base, .. } => type_references_name(base, name),
        ast::TypeKind::Refined { base, .. } => type_references_name(base, name),
        ast::TypeKind::Forall { ty, .. } => type_references_name(ty, name),
        ast::TypeKind::Var(_) | ast::TypeKind::Hole => false,
    }
}

/// Check whether a declaration should be included based on a selective import list.
fn should_include_decl(decl: &ast::Decl, names: &HashSet<&str>) -> bool {
    match &decl.node {
        ast::DeclKind::Data { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::TypeAlias { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::Source { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::View { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::Derived { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::Fun { name, .. } => names.contains(name.as_str()),
        // Traits and impls: include if trait name is in the list
        ast::DeclKind::Trait { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::Impl { trait_name, .. } => names.contains(trait_name.as_str()),
        // Routes
        ast::DeclKind::Route { name, .. } => names.contains(name.as_str()),
        ast::DeclKind::RouteComposite { name, .. } => names.contains(name.as_str()),
        // Migrations, constraints, and unit declarations are always included
        ast::DeclKind::Migrate { .. } => true,
        ast::DeclKind::SubsetConstraint { .. } => true,
        ast::DeclKind::UnitDecl { .. } => true,
    }
}
