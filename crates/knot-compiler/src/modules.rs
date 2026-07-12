//! Module resolution for import declarations.
//!
//! Resolves `import ./path` declarations by loading, parsing, and merging
//! imported modules' declarations into the importing module. Import paths
//! are relative to the importing file.
//!
//! Declarations from every reachable module are merged into one flat list,
//! dependency-first, with the importing module's own declarations last. Each
//! module contributes its declarations exactly once no matter how many paths
//! reach it (diamond imports), and a module reached by both a selective
//! (`import ./m (a)`) and a plain (`import ./m`) import contributes the union
//! of what those imports ask for.

use knot::ast;
use knot::diagnostic::{Diagnostic, Severity};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Source-text snippets sliced from imported modules for the schema lockfile.
///
/// The lockfile is re-parsed on its own, so it must carry both the types a
/// source relation references and the source declarations themselves — for
/// declarations that live in imported modules, not just the entry module.
#[derive(Default)]
pub struct ImportedSnippets {
    /// Non-parameterized type alias / data declarations from imported modules.
    pub types: Vec<String>,
    /// Source declarations (`*name : [T]`) from imported modules.
    pub sources: Vec<String>,
}

/// Resolve all imports in the module, recursively loading imported files
/// and merging their declarations. Detects import cycles.
///
/// On success, returns the source text of every non-parameterized type alias,
/// data declaration, and source declaration found in imported files. The schema
/// lockfile embeds these snippets so that source relations defined in imported
/// modules (e.g. `*people : [Person]` with `Person` from another file) are
/// tracked and still resolve when the lockfile is re-parsed on its own.
pub fn resolve_imports(
    module: &mut ast::Module,
    source_path: &Path,
) -> Result<ImportedSnippets, Vec<String>> {
    let canonical = source_path
        .canonicalize()
        .unwrap_or_else(|_| source_path.to_path_buf());

    let mut resolver = Resolver::default();
    // The entry module's own declarations stay where they are, so nothing can
    // be merged out of it — record it as fully merged. (An import of the entry
    // file from one of its own imports is a cycle, caught by `in_flight`.)
    resolver.merged.insert(canonical.clone(), Merged::Full);

    let mut in_flight = HashSet::new();
    in_flight.insert(canonical.clone());

    let base_dir = source_path.parent().unwrap_or(Path::new("."));
    for imp in std::mem::take(&mut module.imports) {
        resolver.merge_import(&imp, base_dir, &mut in_flight);
    }

    // A name defined by two modules (or twice in one module) reaches codegen as
    // two definitions of the same symbol, which used to abort the compiler with
    // an unhandled `DuplicateDefinition` from Cranelift.
    resolver.check_duplicates(&module.decls, &canonical);

    if !resolver.errors.is_empty() {
        return Err(resolver.errors);
    }

    // Imported declarations precede the module's own — the LSP relies on the
    // module's own decls being the trailing entries of the merged list.
    let mut decls = resolver.decls;
    decls.append(&mut module.decls);
    module.decls = decls;

    let mut snippets = resolver.snippets;
    snippets.types.sort();
    snippets.types.dedup();
    snippets.sources.sort();
    snippets.sources.dedup();
    Ok(snippets)
}

/// A parsed module, cached so that a second import of the same file neither
/// re-parses it nor re-reports its diagnostics.
struct Loaded {
    source: String,
    imports: Vec<ast::Import>,
    /// The module's own declarations that cross an import boundary — i.e. what
    /// survives its export filter.
    visible: Vec<ast::Decl>,
}

/// How much of a module's visible declarations have been merged so far.
enum Merged {
    /// All of them. A further import of this module has nothing left to add.
    Full,
    /// Only the visible declarations at these indices, as asked for by the
    /// selective imports seen so far. A later import can still add the rest.
    Partial(HashSet<usize>),
}

#[derive(Default)]
struct Resolver {
    loaded: HashMap<PathBuf, Loaded>,
    /// Modules that failed to load, so a second import doesn't re-report them.
    failed: HashSet<PathBuf>,
    /// Modules whose own imports have already been merged.
    deps_merged: HashSet<PathBuf>,
    merged: HashMap<PathBuf, Merged>,
    /// Memoizes `reachable_names`.
    reachable: HashMap<PathBuf, HashSet<String>>,
    /// Guards `reachable_names` against import cycles.
    reach_visiting: HashSet<PathBuf>,
    /// The merged declarations, dependency-first, and the module each came from.
    decls: Vec<ast::Decl>,
    origins: Vec<PathBuf>,
    snippets: ImportedSnippets,
    errors: Vec<String>,
}

impl Resolver {
    /// Merge the module `imp` names into the flat declaration list, along with
    /// everything it transitively imports.
    fn merge_import(
        &mut self,
        imp: &ast::Import,
        base_dir: &Path,
        in_flight: &mut HashSet<PathBuf>,
    ) {
        let full_path = base_dir.join(PathBuf::from(&imp.path).with_extension("knot"));
        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(e) => {
                self.errors.push(format!(
                    "cannot resolve import '{}': {} (resolved to {})",
                    imp.path,
                    e,
                    full_path.display()
                ));
                return;
            }
        };

        // Only an in-flight import — one currently on the resolution stack — is
        // a cycle. A module already merged via a sibling import is a diamond,
        // and the merge bookkeeping below keeps its decls from landing twice.
        if in_flight.contains(&canonical) {
            self.errors.push(format!(
                "import cycle detected: '{}' has already been imported",
                imp.path
            ));
            return;
        }
        if self.failed.contains(&canonical) {
            return;
        }
        if !self.loaded.contains_key(&canonical) && !self.load(&canonical, &imp.path) {
            self.failed.insert(canonical);
            return;
        }

        // Merge what this module imports before the module itself, so its
        // declarations land after the ones they depend on.
        if self.deps_merged.insert(canonical.clone()) {
            in_flight.insert(canonical.clone());
            let dep_base = canonical
                .parent()
                .unwrap_or(Path::new("."))
                .to_path_buf();
            for dep in self.loaded[&canonical].imports.clone() {
                self.merge_import(&dep, &dep_base, in_flight);
            }
            in_flight.remove(&canonical);
        }

        self.merge_decls(&canonical, imp);
    }

    /// Merge the declarations this import asks for out of an already-loaded
    /// module, skipping the ones an earlier import of it already contributed.
    fn merge_decls(&mut self, canonical: &Path, imp: &ast::Import) {
        if let Some(items) = &imp.items {
            let reachable = self.reachable_names(canonical);
            for item in items {
                if !reachable.contains(&item.name) {
                    self.errors.push(format!(
                        "import '{}': '{}' not found in module",
                        imp.path, item.name
                    ));
                }
            }
        }

        // A module already merged in full has nothing left to give — including
        // to a *selective* import of it, whose names are already in scope.
        let mut merged_idx = match self.merged.get(canonical) {
            Some(Merged::Full) => return,
            Some(Merged::Partial(idx)) => idx.clone(),
            None => HashSet::new(),
        };

        let (new_decls, total) = {
            let visible = &self.loaded[canonical].visible;
            let wanted: Vec<usize> = match &imp.items {
                None => (0..visible.len()).collect(),
                Some(items) => {
                    let names = selected_names(visible, items);
                    (0..visible.len())
                        .filter(|&i| should_include_decl(&visible[i], &names))
                        .collect()
                }
            };
            let new_decls: Vec<ast::Decl> = wanted
                .into_iter()
                .filter(|&i| merged_idx.insert(i))
                .map(|i| visible[i].clone())
                .collect();
            (new_decls, visible.len())
        };

        for decl in new_decls {
            self.decls.push(decl);
            self.origins.push(canonical.to_path_buf());
        }
        let record = if merged_idx.len() == total {
            Merged::Full
        } else {
            Merged::Partial(merged_idx)
        };
        self.merged.insert(canonical.to_path_buf(), record);
    }

    /// Read, parse, and export-filter a module. Returns false if it could not
    /// be loaded, in which case the reason is already in `self.errors`.
    fn load(&mut self, canonical: &Path, import_path: &str) -> bool {
        let source = match std::fs::read_to_string(canonical) {
            Ok(s) => s,
            Err(e) => {
                self.errors
                    .push(format!("cannot read import '{}': {}", import_path, e));
                return false;
            }
        };
        let filename = canonical.display().to_string();

        let lexer = knot::lexer::Lexer::new(&source);
        let (tokens, lex_diags) = lexer.tokenize();
        // Only treat lex errors as fatal — the lexer currently only emits
        // errors, but filtering by severity (mirroring the parse-diagnostics
        // path below) future-proofs against lex warnings/info becoming
        // hard failures that abort the entire compilation.
        for diag in lex_diags.iter().filter(|d| d.severity == Severity::Error) {
            self.errors.push(format!(
                "in import '{}': {}",
                import_path,
                diag.render(&source, &filename)
            ));
        }

        let parser = knot::parser::Parser::new(source.clone(), tokens);
        let (imported_module, parse_diags) = parser.parse_module();
        if parse_diags.iter().any(|d| d.severity == Severity::Error) {
            for diag in &parse_diags {
                self.errors.push(format!(
                    "in import '{}': {}",
                    import_path,
                    diag.render(&source, &filename)
                ));
            }
            return false;
        }

        // Collect type and source declaration snippets for the schema lockfile
        // while this module's decls still pair with its own source text.
        for d in &imported_module.decls {
            let is_type_decl = matches!(
                &d.node,
                ast::DeclKind::TypeAlias { params, .. } if params.is_empty()
            ) || matches!(&d.node, ast::DeclKind::Data { .. });
            let is_source_decl = matches!(&d.node, ast::DeclKind::Source { .. });
            if is_type_decl
                && let Some(text) = source.get(d.span.start..d.span.end)
            {
                self.snippets.types.push(text.to_string());
            }
            if is_source_decl
                && let Some(text) = source.get(d.span.start..d.span.end)
            {
                self.snippets.sources.push(text.to_string());
            }
        }

        self.loaded.insert(
            canonical.to_path_buf(),
            Loaded {
                source,
                imports: imported_module.imports,
                visible: export_filter(imported_module.decls),
            },
        );
        true
    }

    /// Every name an importer can reach *through* this module: the names it
    /// makes visible itself, plus the ones its own imports bring in (which are
    /// merged into the same flat namespace). Used to validate selective imports.
    fn reachable_names(&mut self, canonical: &Path) -> HashSet<String> {
        if let Some(cached) = self.reachable.get(canonical) {
            return cached.clone();
        }
        let (mut names, imports) = match self.loaded.get(canonical) {
            Some(loaded) => (
                loaded
                    .visible
                    .iter()
                    .filter_map(|d| decl_name(&d.node))
                    .collect::<HashSet<String>>(),
                loaded.imports.clone(),
            ),
            None => return HashSet::new(),
        };
        // An import cycle is reported elsewhere; just don't chase it here.
        if !self.reach_visiting.insert(canonical.to_path_buf()) {
            return names;
        }

        let base_dir = canonical.parent().unwrap_or(Path::new(".")).to_path_buf();
        for imp in &imports {
            match &imp.items {
                Some(items) => names.extend(items.iter().map(|i| i.name.clone())),
                None => {
                    if let Ok(dep) = base_dir
                        .join(PathBuf::from(&imp.path).with_extension("knot"))
                        .canonicalize()
                    {
                        names.extend(self.reachable_names(&dep));
                    }
                }
            }
        }

        self.reach_visiting.remove(canonical);
        self.reachable.insert(canonical.to_path_buf(), names.clone());
        names
    }

    /// Report any name defined twice across the merged declarations. Two
    /// definitions of one name compile to one symbol, which Cranelift rejects
    /// with `DuplicateDefinition` — a panic, with no source location, long
    /// after the point where the mistake is visible.
    fn check_duplicates(&mut self, own_decls: &[ast::Decl], entry: &Path) {
        let mut entry_source: Option<String> = None;
        let mut seen: HashMap<(Namespace, String), PathBuf> = HashMap::new();

        let merged = self.decls.iter().zip(self.origins.iter().map(|p| p.as_path()));
        let own = own_decls.iter().zip(std::iter::repeat(entry));
        let mut errors = Vec::new();

        for (decl, origin) in merged.chain(own) {
            let Some(key) = duplicate_key(&decl.node) else {
                continue;
            };
            // The first definition stays the anchor for every later clash.
            let Some(first) = seen.get(&key).cloned() else {
                seen.insert(key, origin.to_path_buf());
                continue;
            };

            let (_, name) = key;
            let note = if first == origin {
                format!("'{}' is already defined in this module", name)
            } else {
                format!("'{}' is already defined in {}", name, first.display())
            };
            let diag = Diagnostic::error(format!("duplicate definition of '{}'", name))
                .label(decl.span, "redefined here")
                .note(note);

            let source = if origin == entry {
                entry_source
                    .get_or_insert_with(|| std::fs::read_to_string(entry).unwrap_or_default())
                    .as_str()
            } else {
                self.loaded
                    .get(origin)
                    .map(|l| l.source.as_str())
                    .unwrap_or("")
            };
            errors.push(diag.render(source, &origin.display().to_string()));
        }

        self.errors.extend(errors);
    }
}

/// Restrict a module's own declarations to those that cross an import boundary.
///
/// A module that `export`s anything exposes only what it exports; one that
/// exports nothing exposes everything (backwards compat). Judging by the
/// module's *own* declarations is essential — otherwise a transitively imported
/// module's exports would flip a no-export module into "export-only" mode and
/// silently drop all of its own decls.
fn export_filter(decls: Vec<ast::Decl>) -> Vec<ast::Decl> {
    if !decls.iter().any(|d| d.exported) {
        return decls;
    }
    let exported_names: HashSet<String> = decls
        .iter()
        .filter(|d| d.exported)
        .filter_map(|d| decl_name(&d.node))
        .collect();
    let own_traits: HashSet<&str> = decls
        .iter()
        .filter_map(|d| match &d.node {
            ast::DeclKind::Trait { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    decls
        .iter()
        .filter(|d| {
            d.exported
                || match &d.node {
                    // Migrations, constraints, and unit declarations carry no
                    // name to export: they are properties of the program, not
                    // of a module's interface, and are always visible. Dropping
                    // the unit declarations left the units their exported types
                    // are written in (`Float<M/S^2>`) undefined at the import
                    // site, which surfaced as bogus unit mismatches.
                    ast::DeclKind::Migrate { .. }
                    | ast::DeclKind::SubsetConstraint { .. }
                    | ast::DeclKind::UnitDecl { .. } => true,
                    // An impl is visible with the trait it implements. A trait
                    // this module declares must be exported to carry its impls
                    // out; a trait from anywhere else (another module, the
                    // prelude) is already visible to the importer, so its impls
                    // travel with the module that defines them.
                    ast::DeclKind::Impl { trait_name, .. } => {
                        !own_traits.contains(trait_name.as_str())
                            || exported_names.contains(trait_name)
                    }
                    _ => false,
                }
        })
        .cloned()
        .collect()
}

/// The names a selective import pulls in: the ones it lists, plus data types
/// defined in the same module that a selected trait's methods mention (e.g.
/// `Ordering` for `Ord`), which imported impls would otherwise reference
/// without a definition in scope.
fn selected_names(visible: &[ast::Decl], items: &[ast::ImportItem]) -> HashSet<String> {
    let mut names: HashSet<String> = items.iter().map(|i| i.name.clone()).collect();
    let data_names: Vec<&str> = visible
        .iter()
        .filter_map(|d| match &d.node {
            ast::DeclKind::Data { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    let mut extra: Vec<String> = Vec::new();
    for d in visible {
        if let ast::DeclKind::Trait { name, items, .. } = &d.node
            && names.contains(name.as_str())
        {
            for item in items {
                if let ast::TraitItem::Method { ty, .. } = item {
                    for data_name in &data_names {
                        if type_references_name(&ty.ty, data_name) {
                            extra.push((*data_name).to_string());
                        }
                    }
                }
            }
        }
    }
    names.extend(extra);
    names
}

/// The namespace a declaration's name occupies. Names in different namespaces
/// never collide: a source relation `*x` and a function `x` are addressed
/// differently and compile to different things, while a function and a derived
/// relation both compile to `knot_user_x`.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Namespace {
    Value,
    Relation,
    Type,
    Trait,
}

/// The name a declaration *defines*, and the namespace it defines it in.
/// `None` for declarations that define no name of their own.
fn duplicate_key(decl: &ast::DeclKind) -> Option<(Namespace, String)> {
    match decl {
        // A bare signature (`f : Int -> Int` with the definition elsewhere or
        // missing) defines nothing on its own.
        ast::DeclKind::Fun { body: None, .. } => None,
        ast::DeclKind::Fun { name, .. } | ast::DeclKind::Derived { name, .. } => {
            Some((Namespace::Value, name.clone()))
        }
        ast::DeclKind::Source { name, .. } | ast::DeclKind::View { name, .. } => {
            Some((Namespace::Relation, name.clone()))
        }
        ast::DeclKind::Data { name, .. }
        | ast::DeclKind::TypeAlias { name, .. }
        | ast::DeclKind::Route { name, .. }
        | ast::DeclKind::RouteComposite { name, .. } => Some((Namespace::Type, name.clone())),
        ast::DeclKind::Trait { name, .. } => Some((Namespace::Trait, name.clone())),
        ast::DeclKind::Impl { .. }
        | ast::DeclKind::Migrate { .. }
        | ast::DeclKind::SubsetConstraint { .. }
        | ast::DeclKind::UnitDecl { .. } => None,
    }
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
fn should_include_decl(decl: &ast::Decl, names: &HashSet<String>) -> bool {
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
