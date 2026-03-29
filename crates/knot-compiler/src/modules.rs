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
    let mut visited = HashSet::new();
    visited.insert(canonical.clone());
    resolve_recursive(module, source_path, &mut visited)
}

fn resolve_recursive(
    module: &mut ast::Module,
    source_path: &Path,
    visited: &mut HashSet<PathBuf>,
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

        // Cycle detection
        if !visited.insert(canonical.clone()) {
            errors.push(format!(
                "import cycle detected: '{}' has already been imported",
                imp.path
            ));
            continue;
        }

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
        if let Err(sub_errors) = resolve_recursive(
            &mut imported_module,
            &canonical,
            visited,
        ) {
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
            let names: HashSet<&str> = items.iter().map(|i| i.name.as_str()).collect();
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
        | ast::DeclKind::SubsetConstraint { .. } => None,
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
        // Migrations and constraints are always included
        ast::DeclKind::Migrate { .. } => true,
        ast::DeclKind::SubsetConstraint { .. } => true,
    }
}
