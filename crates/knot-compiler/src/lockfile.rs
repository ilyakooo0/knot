//! Schema lockfile management.
//!
//! Maintains a `<name>.schema.lock` file alongside each source file,
//! tracking persisted relation schemas and migration history.
//! The lockfile is valid Knot syntax, parsed by the same frontend.

use crate::types::TypeEnv;
use knot::ast::*;
use knot::diagnostic::{Diagnostic, Severity};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Lockfile path: `examples/todo.knot` → `examples/todo.schema.lock`
fn lockfile_path(source_path: &Path) -> PathBuf {
    let stem = source_path.file_stem().unwrap_or_default();
    let mut name = stem.to_os_string();
    name.push(".schema.lock");
    source_path.with_file_name(name)
}

struct SchemaInfo {
    /// source_name → schema descriptor ("col:type,col:type,...")
    sources: HashMap<String, String>,
    /// relation_name → Vec<(from_schema, to_schema)>
    migrations: HashMap<String, Vec<(String, String)>>,
}

/// Classification of a schema change.
#[derive(Debug, PartialEq)]
enum SchemaChange {
    Identical,
    /// Safe change that can be auto-applied (e.g. adding ADT constructors)
    Safe,
    /// Breaking change that requires a migrate block
    Breaking,
}

/// Classify the schema change between an old and new schema descriptor.
fn classify_schema_change(old: &str, new: &str) -> SchemaChange {
    if old == new {
        return SchemaChange::Identical;
    }

    let old_is_adt = old.starts_with('#');
    let new_is_adt = new.starts_with('#');

    if old_is_adt != new_is_adt {
        return SchemaChange::Breaking;
    }

    if old_is_adt {
        classify_adt_change(old, new)
    } else {
        // Record schemas: any change is breaking for now
        // (adding nullable/Maybe fields will be Safe once Maybe is in the prelude)
        classify_record_change(old, new)
    }
}

/// Parse an ADT schema into a map of constructor_name -> Vec<(field_name, field_type)>.
fn parse_adt_constructors(spec: &str) -> Vec<(String, Vec<(String, String)>)> {
    if spec.len() < 2 || !spec.starts_with('#') {
        return Vec::new();
    }
    let body = &spec[1..]; // strip '#'
    let mut ctors = Vec::new();
    for ctor_part in split_respecting_brackets(body, '|') {
        if ctor_part.is_empty() {
            continue;
        }
        let mut parts = ctor_part.splitn(2, ':');
        let name = parts.next().unwrap().to_string();
        let fields: Vec<(String, String)> = if let Some(field_spec) = parts.next() {
            split_respecting_brackets(field_spec, ';')
                .into_iter()
                .map(|f| {
                    let mut fp = f.splitn(2, '=');
                    let fname = fp.next().unwrap().to_string();
                    let fty = fp.next().unwrap_or("").to_string();
                    (fname, fty)
                })
                .collect()
        } else {
            Vec::new()
        };
        ctors.push((name, fields));
    }
    ctors
}

/// Two field lists denote the same fields (order-independent) with identical
/// types — i.e. their change classifies as `Identical`.
fn fields_match(old: &[(String, String)], new: &[(String, String)]) -> bool {
    classify_field_set(old, new) == SchemaChange::Identical
}

/// Classify the change between two field lists (order-independent), recursing
/// into nested child-table schemas via [`classify_field_type`] rather than
/// comparing bracketed descriptors as opaque strings. Returns `Identical` when
/// the lists denote the same fields with identical types, `Safe` when every old
/// field survives (identically or via a safe nested change) and the only
/// differences are additions, and `Breaking` when a field is removed or a type
/// change is breaking. Duplicate field names are handled by binding exact
/// `(name, type)` pairs first, so a first-match by name cannot mask a breaking
/// change on a duplicate.
fn classify_field_set(old: &[(String, String)], new: &[(String, String)]) -> SchemaChange {
    let mut used = vec![false; new.len()];
    let mut leftover_old: Vec<&(String, String)> = Vec::new();

    // Pass 1: bind exact (name + identical type string) matches. This consumes
    // duplicate field names against their true counterpart before the name-only
    // fallback below.
    for of in old {
        match new
            .iter()
            .enumerate()
            .position(|(i, nf)| !used[i] && nf.0 == of.0 && nf.1 == of.1)
        {
            Some(i) => used[i] = true,
            None => leftover_old.push(of),
        }
    }

    let mut any_safe = false;

    // Pass 2: bind each remaining old field by name and classify the type
    // change, recursing into nested child-table schemas.
    for of in leftover_old {
        match new
            .iter()
            .enumerate()
            .position(|(i, nf)| !used[i] && nf.0 == of.0)
        {
            Some(i) => {
                used[i] = true;
                match classify_field_type(&of.1, &new[i].1) {
                    SchemaChange::Identical => {}
                    SchemaChange::Safe => any_safe = true,
                    SchemaChange::Breaking => return SchemaChange::Breaking,
                }
            }
            None => return SchemaChange::Breaking, // field removed
        }
    }

    // Any unmatched new fields are nullable-column additions — safe, *unless*
    // the added column is an enum tag. Every other column type has an empty
    // default (`sql_null_default_literal`) so the runtime backfills existing
    // rows; a `tag` column has no default, is left NULL, and `read_sql_column`
    // panics at runtime. Classify added tag fields as Breaking so the user is
    // sent to an explicit `migrate` block instead of hitting a runtime panic.
    let mut added_tag = false;
    for (i, nf) in new.iter().enumerate() {
        if !used[i] {
            if nf.1 == "tag" {
                added_tag = true;
            } else {
                any_safe = true;
            }
        }
    }
    if added_tag {
        return SchemaChange::Breaking;
    }

    if any_safe {
        SchemaChange::Safe
    } else {
        SchemaChange::Identical
    }
}

/// Classify a single field's type change. When both the old and new type are
/// nested child-table schemas (`[...]`), recurse into the bracketed schema and
/// compare it field-by-field instead of treating the descriptor as an opaque
/// string — a reorder or safe column addition inside a nested relation is
/// applied by the runtime's `auto_apply_child_change` and must not force a
/// migrate block. Any other differing type is a breaking type change.
fn classify_field_type(old_ty: &str, new_ty: &str) -> SchemaChange {
    if old_ty == new_ty {
        return SchemaChange::Identical;
    }
    match (unbracket(old_ty), unbracket(new_ty)) {
        (Some(old_inner), Some(new_inner)) => classify_child_schema_change(old_inner, new_inner),
        // A scalar type change, or a change between a nested relation and a
        // scalar column, is breaking.
        _ => SchemaChange::Breaking,
    }
}

/// If `ty` is a nested child-table schema (`[...]`), return the inner child
/// schema descriptor without the surrounding brackets; otherwise `None`.
fn unbracket(ty: &str) -> Option<&str> {
    ty.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
}

/// Classify the change of a nested child-table schema (the inner content of a
/// `field:[...]` descriptor), mirroring the runtime's `auto_apply_child_change`:
/// removing a column/sub-field or changing a type is breaking; adding a scalar
/// column or reordering is safe. A leaf child (one with no sub-relations of its
/// own) gaining a nested sub-relation is breaking — a leaf table is created
/// without the `_id`/`_content_hash` columns a parent needs and SQLite cannot
/// add them via `ALTER TABLE`, so the runtime forces an explicit migrate block.
fn classify_child_schema_change(old: &str, new: &str) -> SchemaChange {
    if old == new {
        return SchemaChange::Identical;
    }
    // An ADT element type reuses the ADT-aware classification.
    if old.starts_with('#') || new.starts_with('#') {
        return classify_schema_change(old, new);
    }

    let old_fields = parse_record_fields(old);
    let new_fields = parse_record_fields(new);

    // Leaf child gaining its own nested sub-relation — breaking (see doc above).
    let old_has_nested = old_fields.iter().any(|(_, t)| unbracket(t).is_some());
    let new_has_nested = new_fields.iter().any(|(_, t)| unbracket(t).is_some());
    if !old_has_nested && new_has_nested {
        return SchemaChange::Breaking;
    }

    classify_field_set(&old_fields, &new_fields)
}

fn classify_adt_change(old: &str, new: &str) -> SchemaChange {
    let old_ctors = parse_adt_constructors(old);
    let new_ctors = parse_adt_constructors(new);

    // Every old constructor must exist in new with the same set of fields.
    // Field *order* is irrelevant: all constructor fields share one wide
    // nullable-column table, so physical column order carries no meaning —
    // the same way `classify_record_change` matches record fields by name.
    // Reordering must not force an unnecessary migrate block.
    for (old_name, old_fields) in &old_ctors {
        match new_ctors.iter().find(|(n, _)| n == old_name) {
            Some((_, new_fields)) => {
                if !fields_match(old_fields, new_fields) {
                    return SchemaChange::Breaking;
                }
            }
            None => return SchemaChange::Breaking,
        }
    }

    // All old constructors preserved with identical fields — new
    // constructors are safe additions (nullable columns, no data loss).
    // Also verify no old constructor was duplicated in new (which the
    // find-based loop above would not catch).
    let mut new_seen: HashSet<&String> = HashSet::new();
    for (n, _) in &new_ctors {
        if !new_seen.insert(n) {
            return SchemaChange::Breaking;
        }
    }

    // A newly-added constructor may reuse a field name that already exists in
    // another constructor. Every *pre-existing* constructor is unchanged, but
    // if the reused name now carries a type of a different column affinity, the
    // existing wide-table column keeps its old affinity and the runtime's
    // `auto_apply_adt_change` rejects the in-place migration. Classify that as
    // Breaking so the user is sent to an explicit migrate block. Affinity is
    // the runtime `ColType` mapping ("json" and nested "[...]" both round-trip
    // as JSON, so they share one affinity).
    fn field_affinity(ty: &str) -> &'static str {
        match ty {
            "int" => "int",
            "float" => "float",
            "text" => "text",
            "bool" => "bool",
            "bytes" => "bytes",
            "tag" => "tag",
            _ => "json",
        }
    }
    let mut old_affinity: HashMap<&str, &'static str> = HashMap::new();
    for (_, fields) in &old_ctors {
        for (fname, fty) in fields {
            old_affinity
                .entry(fname.as_str())
                .or_insert_with(|| field_affinity(fty));
        }
    }
    for (_, fields) in &new_ctors {
        for (fname, fty) in fields {
            if let Some(old_aff) = old_affinity.get(fname.as_str())
                && *old_aff != field_affinity(fty)
            {
                return SchemaChange::Breaking;
            }
        }
    }

    // If no new constructors were added, the schemas are semantically
    // identical (field/constructor order is irrelevant — see comment above).
    if old_ctors.len() == new_ctors.len() {
        SchemaChange::Identical
    } else {
        SchemaChange::Safe
    }
}

/// Parse a record schema into Vec<(field_name, field_type)>.
fn parse_record_fields(spec: &str) -> Vec<(String, String)> {
    if spec.is_empty() {
        return Vec::new();
    }
    split_respecting_brackets(spec, ',')
        .into_iter()
        .filter(|part| !part.is_empty())
        .map(|part| {
            if let Some(colon) = part.find(':') {
                let name = part[..colon].to_string();
                let ty = part[colon + 1..].to_string();
                (name, ty)
            } else {
                (part.to_string(), String::new())
            }
        })
        .collect()
}

/// Split a string by `sep` while respecting `[...]` and `{...}` bracket nesting.
fn split_respecting_brackets(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' | '{' => depth += 1,
            ']' | '}' => depth = depth.saturating_sub(1),
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn classify_record_change(old: &str, new: &str) -> SchemaChange {
    let old_fields = parse_record_fields(old);
    let new_fields = parse_record_fields(new);

    // Adding the *first* nested-relation field to a source is breaking, even
    // though every old field is preserved. Nested `[T]` fields live in child
    // tables keyed by a `_parent_id` FK, which requires the parent table to
    // gain an `_id INTEGER PRIMARY KEY` column. SQLite's ALTER TABLE cannot
    // add a PRIMARY KEY column to an existing table, so the runtime cannot
    // apply this in place — it must go through a migrate block (table rebuild).
    // Once the parent already has at least one nested field (and thus an
    // `_id`), further nested fields only add new child tables (a CREATE TABLE),
    // which is a safe addition. `classify_child_schema_change` enforces the
    // same rule one level down, for a leaf child gaining a sub-relation.
    let old_has_nested = old_fields.iter().any(|(_, ty)| is_nested_field(ty));
    let new_has_nested = new_fields.iter().any(|(_, ty)| is_nested_field(ty));
    if new_has_nested && !old_has_nested {
        return SchemaChange::Breaking;
    }

    // Compare field-by-field, recursing into nested child-table schemas
    // (`field:[...]`) rather than treating their bracketed descriptors as opaque
    // strings: a reorder or a safe column addition inside a nested relation is
    // Safe (the runtime's `auto_apply_child_change` applies it), only a column
    // removal or a type change is Breaking. The field-set comparison binds exact
    // `(name, type)` pairs before falling back to name-only matching, so
    // duplicate field names cannot mask a breaking change on a duplicate.
    classify_field_set(&old_fields, &new_fields)
}

/// A nested-relation field's type descriptor is bracketed (`[child_schema]`),
/// distinguishing it from inline record fields (`{...}`) and scalars. Nested
/// relations are stored in child tables and require a `_id` primary key on the
/// parent — see `classify_record_change`.
fn is_nested_field(ty: &str) -> bool {
    ty.trim_start().starts_with('[')
}

fn parse_lockfile(lock_path: &Path) -> Result<SchemaInfo, String> {
    let content = std::fs::read_to_string(lock_path)
        .map_err(|e| format!("cannot read {}: {}", lock_path.display(), e))?;

    let lexer = knot::lexer::Lexer::new(&content);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(content, tokens);
    let (module, diags) = parser.parse_module();

    if diags.iter().any(|d| d.severity == Severity::Error) {
        return Err(format!(
            "parse errors in {}; delete it and recompile to regenerate",
            lock_path.display()
        ));
    }

    let env = TypeEnv::from_module(&module);
    Ok(SchemaInfo {
        sources: env.source_schemas,
        migrations: env.migrate_schemas,
    })
}

/// Diff source schemas against the lockfile. Returns diagnostics
/// (errors for breaking changes, warnings for removed sources).
/// Returns empty vec on first compile (no lockfile yet).
pub fn check(source_path: &Path, module: &Module, type_env: &TypeEnv) -> Vec<Diagnostic> {
    let lock_path = lockfile_path(source_path);
    let mut diags = Vec::new();

    let old = if lock_path.exists() {
        match parse_lockfile(&lock_path) {
            Ok(info) => info,
            Err(e) => {
                diags.push(Diagnostic::error(e));
                return diags;
            }
        }
    } else {
        return diags;
    };

    // Detect breaking schema changes
    for (name, old_schema) in &old.sources {
        match type_env.source_schemas.get(name) {
            Some(new_schema) if new_schema == old_schema => {}
            Some(new_schema) => {
                let change = classify_schema_change(old_schema, new_schema);
                match change {
                    SchemaChange::Identical => {}
                    SchemaChange::Safe => {
                        // Safe change — lockfile will auto-update, no migrate block needed
                    }
                    SchemaChange::Breaking => {
                        if let Some(migrations) = type_env.migrate_schemas.get(name) {
                            // Validate migration chain: first from must match lockfile,
                            // last to must match source, chain must be contiguous
                            if migrations.is_empty() {
                                diags.push(
                                    Diagnostic::error(format!(
                                        "breaking schema change for '*{}' requires a migrate block",
                                        name
                                    ))
                                    .label(find_source_span(module, name), "schema changed")
                                    .note(format!("lockfile: {}", old_schema))
                                    .note(format!("source:   {}", new_schema))
                                    .note(format!(
                                        "add: migrate *{} from {{...}} to {{...}} using (\\old -> ...)",
                                        name
                                    )),
                                );
                            } else {
                                let first_from = &migrations[0].0;
                                let last_to = &migrations[migrations.len() - 1].1;

                                if classify_schema_change(first_from, old_schema) != SchemaChange::Identical
                                    || classify_schema_change(last_to, new_schema) != SchemaChange::Identical
                                {
                                    let first_span = find_migrate_span(module, name);
                                    diags.push(
                                        Diagnostic::error(format!(
                                            "migrate block for '*{}' doesn't match the schema change",
                                            name
                                        ))
                                        .label(first_span, "here")
                                        .note(format!("lockfile schema: {}", old_schema))
                                        .note(format!("source schema:   {}", new_schema))
                                        .note(format!("migrate from:    {}", first_from))
                                        .note(format!("migrate to:      {}", last_to)),
                                    );
                                }

                                // Validate chain contiguity
                                for i in 1..migrations.len() {
                                    if classify_schema_change(&migrations[i - 1].1, &migrations[i].0) != SchemaChange::Identical {
                                        diags.push(
                                            Diagnostic::error(format!(
                                                "migration chain for '*{}' is not contiguous: step {} 'to' doesn't match step {} 'from'",
                                                name, i, i + 1
                                            ))
                                            .note(format!("step {} to:   {}", i, migrations[i - 1].1))
                                            .note(format!("step {} from: {}", i + 1, migrations[i].0)),
                                        );
                                    }
                                }
                            }
                        } else {
                            diags.push(
                                Diagnostic::error(format!(
                                    "breaking schema change for '*{}' requires a migrate block",
                                    name
                                ))
                                .label(find_source_span(module, name), "schema changed")
                                .note(format!("lockfile: {}", old_schema))
                                .note(format!("source:   {}", new_schema))
                                .note(format!(
                                    "add: migrate *{} from {{...}} to {{...}} using (\\old -> ...)",
                                    name
                                )),
                            );
                        }
                    }
                }
            }
            None => {
                diags.push(Diagnostic::warning(format!(
                    "source '*{}' in lockfile but not in source — data may be orphaned",
                    name
                )));
            }
        }
    }

    // Ensure lockfile migrations aren't removed from source
    for (name, lock_migrations) in &old.migrations {
        let source_migrations = type_env.migrate_schemas.get(name);
        for (lock_from, lock_to) in lock_migrations {
            let still_present = source_migrations.is_some_and(|sm| {
                sm.iter().any(|(f, t)| {
                    classify_schema_change(f, lock_from) == SchemaChange::Identical
                        && classify_schema_change(t, lock_to) == SchemaChange::Identical
                })
            });
            if !still_present {
                diags.push(
                    Diagnostic::error(format!(
                        "migration for '*{}' removed from source but present in lockfile",
                        name
                    ))
                    .note(format!("from: {} -> to: {}", lock_from, lock_to))
                    .note("remove from lockfile explicitly to prune old migrations"),
                );
            }
        }
    }

    diags
}

/// Sources present in the lockfile but absent from the current source —
/// i.e. relations the codebase no longer declares. Codegen emits a
/// `DROP TABLE` for each so a removed source's stored data is deleted on the
/// next build's startup (after migrations, before source init). Returns an
/// empty vec when there is no lockfile or it fails to parse (the `check`
/// pass reports parse errors separately).
pub fn dropped_sources(source_path: &Path, type_env: &TypeEnv) -> Vec<String> {
    let lock_path = lockfile_path(source_path);
    if !lock_path.exists() {
        return Vec::new();
    }
    let old = match parse_lockfile(&lock_path) {
        Ok(info) => info,
        Err(_) => return Vec::new(),
    };
    old.sources
        .keys()
        .filter(|name| !type_env.source_schemas.contains_key(*name))
        .cloned()
        .collect()
}

/// Write the lockfile after a successful compile.
/// Only writes if there are source declarations to track (in the entry module
/// or any imported module).
/// `imported_type_snippets` carries type alias / data declarations from
/// imported modules (sliced from their own source files), so types referenced
/// by source declarations still resolve when the lockfile is parsed alone.
/// `imported_source_snippets` carries the source declarations from imported
/// modules, so schema changes to them are tracked by the lockfile just like
/// entry-module sources (rather than only surfacing as a runtime startup panic).
pub fn update(
    source_path: &Path,
    source_text: &str,
    module: &Module,
    imported_type_snippets: &[String],
    imported_source_snippets: &[String],
) -> Result<(), String> {
    let has_sources = module
        .decls
        .iter()
        .any(|d| matches!(&d.node, DeclKind::Source { .. }))
        || !imported_source_snippets.is_empty()
        || !record_embedded_sources(module).is_empty();

    let lock_path = lockfile_path(source_path);

    // Even when no sources remain in the codebase, prune a pre-existing
    // lockfile. Codegen already emitted a startup DROP for the removed
    // sources into this build's binary (computed from the pre-prune
    // lockfile), so keeping the entry would re-drop on every subsequent
    // build and fire the orphan warning forever. Rewriting with no sources
    // yields a header-only lockfile.
    if !has_sources && !lock_path.exists() {
        return Ok(());
    }

    let content = generate(module, source_text, imported_type_snippets, imported_source_snippets);
    // Atomic write: write to a temp file then rename, so a crash mid-write
    // doesn't leave a corrupt lockfile that hard-errors every compile.
    let tmp_path = lock_path.with_extension("lock.tmp");
    std::fs::write(&tmp_path, &content)
        .map_err(|e| format!("cannot write {}: {}", tmp_path.display(), e))?;
    std::fs::rename(&tmp_path, &lock_path)
        .map_err(|e| format!("cannot rename {} to {}: {}", tmp_path.display(), lock_path.display(), e))
}

/// Collect the record-embedded `*name : <ty>` source declarations from a
/// module's top-level record-let literals (`db = { *todos : [Todo], … }`),
/// paired with the field's span for diagnostics. Duplicate source names are a
/// compile error, so name-keyed iteration is unambiguous.
fn record_embedded_sources(module: &Module) -> Vec<(String, Type, Span)> {
    let mut out = Vec::new();
    for decl in &module.decls {
        if let DeclKind::Fun { body: Some(value), .. } = &decl.node
            && let ExprKind::Record(fields) = &value.node
        {
            for f in fields {
                if let ExprKind::SourceDecl { name, ty, .. } = &f.value.node {
                    out.push((name.clone(), ty.clone(), f.value.span));
                }
            }
        }
    }
    out
}

fn find_source_span(module: &Module, name: &str) -> Span {
    module
        .decls
        .iter()
        .find_map(|d| match &d.node {
            DeclKind::Source { name: n, .. } if n == name => Some(d.span),
            _ => None,
        })
        .or_else(|| {
            record_embedded_sources(module)
                .into_iter()
                .find_map(|(n, _, span)| (n == name).then_some(span))
        })
        .unwrap_or(Span::new(0, 0))
}

fn find_migrate_span(module: &Module, name: &str) -> Span {
    record_embedded_sources(module)
        .into_iter()
        .find_map(|(n, _, span)| (n == name).then_some(span))
        .unwrap_or(Span::new(0, 0))
}

/// Generate lockfile content by extracting declarations from source text.
fn generate(module: &Module, source_text: &str, imported_type_snippets: &[String], imported_source_snippets: &[String]) -> String {
    let mut out = String::new();
    out.push_str("-- schema.lock (auto-generated, do not edit)\n");
    out.push_str("-- Commit to source control.\n");

    // Type declarations from imported modules (sliced from their own sources)
    for snippet in imported_type_snippets {
        out.push('\n');
        out.push_str(snippet);
        out.push('\n');
    }

    // Source declarations from imported modules
    for snippet in imported_source_snippets {
        out.push('\n');
        out.push_str(snippet);
        out.push('\n');
    }

    // Type aliases (non-parameterized) and data declarations
    for decl in &module.decls {
        match &decl.node {
            DeclKind::TypeAlias { params, .. } if params.is_empty() => {
                out.push('\n');
                out.push_str(&source_text[decl.span.start..decl.span.end]);
                out.push('\n');
            }
            DeclKind::Data { .. } => {
                out.push('\n');
                out.push_str(&source_text[decl.span.start..decl.span.end]);
                out.push('\n');
            }
            _ => {}
        }
    }

    // Source declarations
    for decl in &module.decls {
        if let DeclKind::Source { .. } = &decl.node {
            out.push('\n');
            out.push_str(&source_text[decl.span.start..decl.span.end]);
            out.push('\n');
        }
    }

    // Sources embedded in record-let literals (`db = { *todos : [Todo], … }`)
    // have no standalone source span to slice; synthesize an equivalent
    // top-level `*name : <ty>` declaration. Duplicate names are a compile
    // error, so a name already emitted above as a top-level decl is skipped.
    for (name, ty, _) in record_embedded_sources(module) {
        let already = module.decls.iter().any(|d| {
            matches!(&d.node, DeclKind::Source { name: n, .. } if *n == name)
        });
        if already {
            continue;
        }
        out.push('\n');
        out.push_str(&format!("*{} : {}\n", name, knot::format::render_type(&ty)));
    }

    // Migrations attached to record-embedded source fields
    // (`{ *todos : [Todo] migrate from A to B using f }`). Synthesize the
    // equivalent top-level `migrate *name …` line so the lockfile records the
    // migration the same way regardless of where it was declared.
    for decl in &module.decls {
        if let DeclKind::Fun { body: Some(body), .. } = &decl.node
            && let ExprKind::Record(fields) = &body.node
        {
            for f in fields {
                if let ExprKind::SourceDecl { name, migrations, .. } = &f.value.node {
                    for m in migrations {
                        out.push('\n');
                        out.push_str(&format!(
                            "migrate *{}\n  from {}\n  to {}\n  using {}\n",
                            name,
                            knot::format::render_type(&m.from_ty),
                            knot::format::render_type(&m.to_ty),
                            knot::format::render_expr_source(&m.using_fn),
                        ));
                    }
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_record_schema() {
        assert_eq!(
            classify_schema_change("name:text,age:int", "name:text,age:int"),
            SchemaChange::Identical
        );
    }

    #[test]
    fn record_field_added_is_safe() {
        assert_eq!(
            classify_schema_change("name:text", "name:text,age:int"),
            SchemaChange::Safe
        );
    }

    #[test]
    fn record_field_removed_is_breaking() {
        assert_eq!(
            classify_schema_change("name:text,age:int", "name:text"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn record_field_type_changed_is_breaking() {
        assert_eq!(
            classify_schema_change("name:text,age:int", "name:text,age:float"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn adt_constructor_added_is_safe() {
        assert_eq!(
            classify_schema_change(
                "#Circle:radius=float",
                "#Circle:radius=float|Rect:width=float;height=float"
            ),
            SchemaChange::Safe
        );
    }

    #[test]
    fn adt_constructor_removed_is_breaking() {
        assert_eq!(
            classify_schema_change(
                "#Circle:radius=float|Rect:width=float",
                "#Circle:radius=float"
            ),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn adt_constructor_field_changed_is_breaking() {
        assert_eq!(
            classify_schema_change(
                "#Circle:radius=float",
                "#Circle:radius=int"
            ),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn adt_nullary_constructor_added_is_safe() {
        assert_eq!(
            classify_schema_change("#Red|Green", "#Red|Green|Blue"),
            SchemaChange::Safe
        );
    }

    #[test]
    fn identical_adt_schema() {
        assert_eq!(
            classify_schema_change("#Circle:radius=float", "#Circle:radius=float"),
            SchemaChange::Identical
        );
    }

    #[test]
    fn record_to_adt_is_breaking() {
        assert_eq!(
            classify_schema_change("name:text", "#Circle:radius=float"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn adt_constructor_field_reorder_is_not_breaking() {
        // Constructor fields all share one wide nullable-column table, so the
        // physical declaration order is irrelevant — reordering without adding
        // or removing constructors is semantically identical, matching
        // record-field reorder semantics.
        assert_eq!(
            classify_schema_change(
                "#Rect:width=float;height=float",
                "#Rect:height=float;width=float"
            ),
            SchemaChange::Identical
        );
    }

    #[test]
    fn record_field_reorder_is_identical() {
        // The record path already treats reorder as a no-op; pin it so the two
        // paths stay consistent.
        assert_eq!(
            classify_schema_change("name:text,age:int", "age:int,name:text"),
            SchemaChange::Identical
        );
    }

    // --- Nested child-table schemas (`field:[...]`) --------------------------
    // These previously compared the bracketed descriptor as an opaque string,
    // so a reorder or safe column addition inside the nested relation was
    // spuriously classified as Breaking (bug B34).

    #[test]
    fn nested_field_reorder_is_identical() {
        // Reordering columns inside `items:[...]` is name-based on the child
        // table — semantically identical, no migrate block required.
        assert_eq!(
            classify_schema_change(
                "id:int,items:[name:text,price:float]",
                "id:int,items:[price:float,name:text]"
            ),
            SchemaChange::Identical
        );
    }

    #[test]
    fn nested_field_column_added_is_safe() {
        // Adding a scalar column inside the nested relation is a nullable-column
        // addition the runtime auto-applies — Safe, not Breaking.
        assert_eq!(
            classify_schema_change("items:[name:text]", "items:[name:text,price:float]"),
            SchemaChange::Safe
        );
    }

    #[test]
    fn nested_field_column_removed_is_breaking() {
        assert_eq!(
            classify_schema_change("items:[name:text,price:float]", "items:[name:text]"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn nested_field_column_type_changed_is_breaking() {
        assert_eq!(
            classify_schema_change("items:[qty:int]", "items:[qty:float]"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn nested_field_to_scalar_is_breaking() {
        assert_eq!(
            classify_schema_change("items:[name:text]", "items:text"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn nested_reorder_with_sibling_scalar_addition_is_safe() {
        // A nested reorder combined with a top-level scalar addition: the
        // strongest classification wins (Safe), never Breaking.
        assert_eq!(
            classify_schema_change(
                "items:[name:text,price:float]",
                "items:[price:float,name:text],note:text"
            ),
            SchemaChange::Safe
        );
    }

    #[test]
    fn leaf_child_gaining_nested_subrelation_is_breaking() {
        // A leaf child table is created without the `_id`/`_content_hash`
        // columns a parent needs, and SQLite cannot add them via ALTER TABLE —
        // the runtime's `auto_apply_child_change` refuses this, so the compiler
        // must classify it Breaking rather than Safe (avoids the inverse bug).
        assert_eq!(
            classify_schema_change(
                "items:[name:text]",
                "items:[name:text,tags:[label:text]]"
            ),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn nested_within_nested_reorder_is_identical() {
        // Reordering columns two levels deep, where every child already has its
        // own nested sub-relation, is still just a reorder — Identical.
        assert_eq!(
            classify_schema_change(
                "items:[name:text,tags:[a:int,b:int]]",
                "items:[name:text,tags:[b:int,a:int]]"
            ),
            SchemaChange::Identical
        );
    }

    #[test]
    fn nested_within_nested_column_added_is_safe() {
        assert_eq!(
            classify_schema_change(
                "items:[name:text,tags:[a:int]]",
                "items:[name:text,tags:[a:int,b:int]]"
            ),
            SchemaChange::Safe
        );
    }

    #[test]
    fn nonleaf_child_gaining_additional_subrelation_is_safe() {
        // The child already has a sub-relation (`tags`), so it has the `_id`/
        // `_content_hash` scaffolding; adding a second sub-relation is a safe
        // brand-new child table, matching `auto_apply_child_change`.
        assert_eq!(
            classify_schema_change(
                "items:[name:text,tags:[label:text]]",
                "items:[name:text,tags:[label:text],notes:[body:text]]"
            ),
            SchemaChange::Safe
        );
    }

    // --- Parent `_id` requirement for a source's first nested field ----------

    #[test]
    fn first_nested_field_added_is_breaking() {
        // Adding the first nested `[T]` field requires the parent table to
        // gain an `_id INTEGER PRIMARY KEY`, which ALTER TABLE cannot do —
        // the runtime would panic at startup. It must go through a migrate
        // block, so classify it as Breaking (not Safe).
        assert_eq!(
            classify_schema_change("name:text", "name:text,todos:[title:text,done:int]"),
            SchemaChange::Breaking
        );
    }

    #[test]
    fn second_nested_field_added_is_safe() {
        // Once the parent already has a nested field (and thus an `_id`),
        // adding another nested field only creates a new child table — a
        // safe CREATE TABLE that needs no migrate block.
        assert_eq!(
            classify_schema_change(
                "name:text,todos:[title:text]",
                "name:text,todos:[title:text],tags:[tag:text]"
            ),
            SchemaChange::Safe
        );
    }

    #[test]
    fn scalar_field_added_alongside_existing_nested_is_safe() {
        // A parent that already has a nested field can still take plain
        // nullable scalar additions safely.
        assert_eq!(
            classify_schema_change(
                "name:text,todos:[title:text]",
                "name:text,todos:[title:text],age:int"
            ),
            SchemaChange::Safe
        );
    }

    #[test]
    fn inline_record_field_added_is_safe() {
        // Inline record fields (`{...}`) are stored as JSON columns, not child
        // tables, so they don't need an `_id` on the parent — still Safe.
        assert_eq!(
            classify_schema_change("name:text", "name:text,addr:{city:text,zip:text}"),
            SchemaChange::Safe
        );
    }
}
