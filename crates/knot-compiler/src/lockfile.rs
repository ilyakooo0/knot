//! Schema lockfile management.
//!
//! Maintains a `<name>.schema.lock` file alongside each source file,
//! tracking persisted relation schemas and migration history.
//! The lockfile is valid Knot syntax, parsed by the same frontend.

use crate::decl_view::{DeclViewKind, decl_views};
use crate::types::TypeEnv;
use knot::ast::*;
use knot::diagnostic::{Diagnostic, Severity};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Lockfile path: `examples/todo.knot` → `examples/todo.schema.lock`
pub fn lockfile_path(source_path: &Path) -> PathBuf {
    let stem = source_path.file_stem().unwrap_or_default();
    let mut name = stem.to_os_string();
    name.push(".schema.lock");
    source_path.with_file_name(name)
}

/// One committed migration: the schema a source was upgraded FROM, the schema
/// it was upgraded TO, and the `using` fn source that transformed each row.
#[derive(Clone)]
pub struct CommittedMigration {
    pub from_schema: String,
    pub to_schema: String,
    pub using_src: String,
}

struct SchemaInfo {
    /// source_name → last committed schema descriptor ("col:type,col:type,...")
    sources: HashMap<String, String>,
    /// source_name → ordered committed migration chain (oldest first)
    migrations: HashMap<String, Vec<CommittedMigration>>,
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

/// Parse the schema lock into an ordered per-source history. The lock stores,
/// per source, every committed schema version in order; each version after the
/// first carries the `migrate to … using …` that produced it. The last schema
/// for a source is its current committed schema; the migrations form the
/// ordered chain (oldest first).
///
/// Unlike the rest of the compiler (which uses `TypeEnv::from_program`), this
/// walks the parsed AST directly: `TypeEnv` discards the migration's `using`
/// fn source, which the lock must preserve so `knot build` can re-bake the full
/// chain into the binary.
fn parse_lockfile(lock_path: &Path) -> Result<SchemaInfo, String> {
    let content = std::fs::read_to_string(lock_path)
        .map_err(|e| format!("cannot read {}: {}", lock_path.display(), e))?;

    let lexer = knot::lexer::Lexer::new(&content);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(content.clone(), tokens);
    let (program, diags) = parser.parse_file_expr();

    if diags.iter().any(|d| d.severity == Severity::Error) {
        return Err(format!(
            "parse errors in {}; delete it and recompile to regenerate",
            lock_path.display()
        ));
    }

    let env = TypeEnv::from_program(&program);

    // Current committed schemas come from the parseable `Rel T  *name`
    // declarations (re-resolved by TypeEnv). The committed migration chain
    // comes from the `migrate_history [...]` section, which stores raw schema
    // descriptors + `using` source verbatim — old schemas are NOT re-parsable
    // type expressions (a descriptor erases the record/ADT structure), so they
    // are kept as opaque strings and never resolved.
    let migrations = collect_migrate_history(&program);

    Ok(SchemaInfo {
        sources: env.source_schemas,
        migrations,
    })
}

/// Extract the committed migration chain from the lock's
/// `migrate_history [{source "name"  from "…"  to "…"  using "…"}  …]` call.
/// Entries are positional string records; order is preserved (oldest first).
fn collect_migrate_history(program: &Expr) -> HashMap<String, Vec<CommittedMigration>> {
    let mut out: HashMap<String, Vec<CommittedMigration>> = HashMap::new();
    walk_for_history(program, &mut out);
    out
}

fn walk_for_history(e: &Expr, out: &mut HashMap<String, Vec<CommittedMigration>>) {
    match &e.node {
        ExprKind::With { record, body, .. } => {
            walk_for_history(record, out);
            walk_for_history(body, out);
        }
        ExprKind::Record(fields) => {
            for f in fields {
                walk_for_history(&f.value, out);
            }
        }
        ExprKind::List(items) => {
            for item in items {
                if let Some((name, step)) = history_entry(item) {
                    out.entry(name).or_default().push(step);
                }
            }
        }
        _ => {}
    }
}

/// Parse one `{source "…"  from "…"  to "…"  using "…"}` record literal into a
/// committed migration. Returns `None` for any other shape.
fn history_entry(e: &Expr) -> Option<(String, CommittedMigration)> {
    let ExprKind::Record(fields) = &e.node else {
        return None;
    };
    let get = |key: &str| -> Option<String> {
        fields.iter().find_map(|f| {
            if f.name == *key
                && let ExprKind::Lit(knot::ast::Literal::Text(s)) = &f.value.node
            {
                return Some(s.clone());
            }
            None
        })
    };
    Some((
        get("source")?,
        CommittedMigration {
            from_schema: get("from")?,
            to_schema: get("to")?,
            using_src: get("using")?,
        },
    ))
}

/// Diff source schemas against the lockfile. Returns diagnostics
/// (errors for breaking changes, warnings for removed sources).
/// Returns empty vec on first compile (no lockfile yet).
pub fn check(source_path: &Path, program: &Expr, type_env: &TypeEnv) -> Vec<Diagnostic> {
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

    // Diff each committed (locked) schema against the current source schema.
    // Every difference is a schema change requiring an explicit pending
    // `migrate … to … using …` block — there are no safe auto-applied changes.
    for (name, old_schema) in &old.sources {
        match type_env.source_schemas.get(name) {
            Some(new_schema) if new_schema == old_schema => {
                // No schema change. A staged migrate block here is a mistake.
                if type_env.migrate_schemas.contains_key(name) {
                    diags.push(
                        Diagnostic::error(format!(
                            "migrate block for '*{}' but the schema is unchanged from the lock",
                            name
                        ))
                        .label(find_source_span(program, name), "no schema change")
                        .note("remove the migrate block, or change the schema it targets"),
                    );
                }
            }
            Some(new_schema) => {
                // Schema changed. A pending migrate block must be present and
                // must target the current source schema; its `from` is the
                // lock's last schema (derived, not named). Pending migrations
                // are uncommitted until `knot lock` — warn.
                match type_env.migrate_schemas.get(name) {
                    Some(pending_to) => {
                        if classify_schema_change(pending_to, new_schema)
                            != SchemaChange::Identical
                        {
                            diags.push(
                                Diagnostic::error(format!(
                                    "migrate block for '*{}' doesn't match the source schema",
                                    name
                                ))
                                .label(find_source_span(program, name), "here")
                                .note(format!("source schema:  {}", new_schema))
                                .note(format!("migrate to:     {}", pending_to)),
                            );
                        } else {
                            diags.push(
                                Diagnostic::warning(format!(
                                    "uncommitted migration for '*{}' — run `knot lock` to commit it",
                                    name
                                ))
                                .label(find_source_span(program, name), "pending migration")
                                .note(format!("from (lock): {}", old_schema))
                                .note(format!("to (source): {}", new_schema)),
                            );
                        }
                    }
                    None => {
                        diags.push(
                            Diagnostic::error(format!(
                                "schema change for '*{}' requires a migrate block",
                                name
                            ))
                            .label(find_source_span(program, name), "schema changed")
                            .note(format!("lock:   {}", old_schema))
                            .note(format!("source: {}", new_schema))
                            .note(format!(
                                "add: migrate *{} to {{...}} using (\\old -> ...)",
                                name
                            )),
                        );
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

    // A committed migration's `using` fn is baked into the binary and compiled
    // without type inference — it only ever runs against the lock's recorded
    // schemas. If it references a data type that no longer exists in source
    // (deleted after the migration was committed), the binary can't be built.
    // Detect that here, with the actual type name, rather than letting codegen
    // fail with a misleading "constructor must be applied to a record".
    diags.extend(check_committed_using_types(&old, type_env));

    diags
}

/// Error for every committed migration whose `using` fn references a data type
/// the source no longer declares. `using` fns are stored as source text in the
/// lock and compiled verbatim, so a referenced type must still exist.
fn check_committed_using_types(old: &SchemaInfo, type_env: &TypeEnv) -> Vec<Diagnostic> {
    // Data-type names declared in the current source (`Active` in
    // `Active.Yes {}`). A qualified ctor `Type.Ctor` references `Type`. ADT
    // names live in `aliases` (single-variant) and `multi_variant_params`.
    let declared: HashSet<&str> = type_env
        .aliases
        .keys()
        .map(String::as_str)
        .chain(type_env.multi_variant_params.keys().map(String::as_str))
        .collect();
    let mut diags = Vec::new();
    for (source, chain) in &old.migrations {
        for step in chain {
            let Some(using) = parse_using_expr(&step.using_src) else {
                continue;
            };
            let mut referenced = Vec::new();
            collect_qualified_ctor_types(&using, &mut referenced);
            for ty_name in referenced {
                if !declared.contains(ty_name.as_str()) {
                    diags.push(Diagnostic::error(format!(
                        "committed migration for '*{source}' references data type '{ty_name}', which is no longer declared"
                    ))
                    .note(format!("using fn: {}", step.using_src))
                    .note("re-declare the type, or squash the migration history with a fresh baseline"));
                }
            }
        }
    }
    diags
}

/// Parse a stored `using` fn source string into an expression. `None` when it
/// doesn't parse (a separate diagnostic already covers unparseable history).
fn parse_using_expr(using_src: &str) -> Option<Expr> {
    let lexer = knot::lexer::Lexer::new(using_src);
    let (tokens, lex_diags) = lexer.tokenize();
    if lex_diags.iter().any(|d| d.severity == Severity::Error) {
        return None;
    }
    let parser = knot::parser::Parser::new(using_src.to_string(), tokens);
    let (program, parse_diags) = parser.parse_file_expr();
    if parse_diags.iter().any(|d| d.severity == Severity::Error) {
        return None;
    }
    Some(program)
}

/// Collect the data-type names referenced by qualified constructors
/// (`Type.Ctor`) in a `using` fn's AST.
fn collect_qualified_ctor_types(e: &Expr, out: &mut Vec<String>) {
    if let ExprKind::FieldAccess { expr, .. } = &e.node
        && let ExprKind::Constructor(type_name) = &expr.node
    {
        out.push(type_name.clone());
    }
    match &e.node {
        ExprKind::Record(fields) => {
            for f in fields {
                collect_qualified_ctor_types(&f.value, out);
            }
        }
        ExprKind::With { record, body, .. } => {
            collect_qualified_ctor_types(record, out);
            collect_qualified_ctor_types(body, out);
        }
        ExprKind::App { func, arg } => {
            collect_qualified_ctor_types(func, out);
            collect_qualified_ctor_types(arg, out);
        }
        ExprKind::Lambda { body, .. } => collect_qualified_ctor_types(body, out),
        ExprKind::List(items) => {
            for i in items {
                collect_qualified_ctor_types(i, out);
            }
        }
        _ => {}
    }
}

/// The committed migration chain per source, read from the lock. Codegen
/// bakes this into the binary so a stale database fast-forwards through every
/// committed step at startup. Returns an empty map when there is no lockfile
/// or it fails to parse (the `check` pass reports parse errors separately).
pub fn committed_migrations(source_path: &Path) -> HashMap<String, Vec<CommittedMigration>> {
    let lock_path = lockfile_path(source_path);
    if !lock_path.exists() {
        return HashMap::new();
    }
    match parse_lockfile(&lock_path) {
        Ok(info) => info.migrations,
        Err(_) => HashMap::new(),
    }
}

/// The old (pre-migration) type for each source recorded in the lock, as a
/// knot source-syntax type string — e.g. `people -> "PersonV1"`. Used by
/// `main` to type-check a pending migration's `using` fn as `Old -> New`.
///
/// The lock's `Rel T *name` declaration names the source's current (i.e.
/// pre-migration) type `T` directly, so this is a plain read of that type —
/// the lock keeps every historical type declaration verbatim, so `T` resolves
/// even after the source has moved on to a newer type.
pub fn migration_from_types(source_path: &Path) -> HashMap<String, String> {
    let lock_path = lockfile_path(source_path);
    if !lock_path.exists() {
        return HashMap::new();
    }
    let Ok(src) = std::fs::read_to_string(&lock_path) else {
        return HashMap::new();
    };
    let lexer = knot::lexer::Lexer::new(&src);
    let (tokens, lex_diags) = lexer.tokenize();
    if lex_diags.iter().any(|d| d.severity == Severity::Error) {
        return HashMap::new();
    }
    let parser = knot::parser::Parser::new(src.clone(), tokens);
    let (program, parse_diags) = parser.parse_file_expr();
    if parse_diags.iter().any(|d| d.severity == Severity::Error) {
        return HashMap::new();
    }
    // Each `Rel T *name` SourceDecl in the lock names the source's recorded
    // type. Render `T` back to source syntax so infer re-resolves it (the
    // lock's own declarations keep it in scope).
    let mut out = HashMap::new();
    for (name, ty, _) in record_embedded_sources(&program) {
        out.insert(name, knot::format::render_type(&ty));
    }
    out
}

/// The set of source names recorded in the lock (i.e. having a committed
/// current schema). Used by `knot lock` to detect unrecorded sources.
pub fn locked_sources(source_path: &Path) -> HashSet<String> {
    locked_schemas(source_path).into_keys().collect()
}

/// The committed current schema per source, read from the lock. Codegen uses
/// a source's recorded schema as the `from` of its pending migration's first
/// step (the lock holds the baseline the migration upgrades FROM).
pub fn locked_schemas(source_path: &Path) -> HashMap<String, String> {
    let lock_path = lockfile_path(source_path);
    if !lock_path.exists() {
        return HashMap::new();
    }
    match parse_lockfile(&lock_path) {
        Ok(info) => info.sources,
        Err(_) => HashMap::new(),
    }
}

/// A pending (uncommitted) migration staged on a source, with the span needed
/// to excise its `migrate` clause from the source text at `knot lock`.
pub struct PendingMigration {
    /// Span of the whole `migrate to … using …` clause (excised by `knot lock`).
    pub clause_span: Span,
    /// The `using` fn (rendered into the lock's history).
    pub using_fn: Expr,
}

/// The pending migrations staged in the current source, one per source that
/// has a `migrate to … using …` block. Used by `knot lock` to promote them.
pub fn pending_migrations(program: &Expr) -> HashMap<String, PendingMigration> {
    let mut out = HashMap::new();
    collect_pending(program, &mut out);
    out
}

fn collect_pending(e: &Expr, out: &mut HashMap<String, PendingMigration>) {
    match &e.node {
        ExprKind::Record(fields) => {
            for f in fields {
                if let ExprKind::SourceDecl {
                    name,
                    migrations,
                    ..
                } = &f.value.node
                {
                    if let Some(m) = migrations.first() {
                        out.insert(
                            name.clone(),
                            PendingMigration {
                                clause_span: m.span,
                                using_fn: m.using_fn.clone(),
                            },
                        );
                    }
                }
                collect_pending(&f.value, out);
            }
        }
        ExprKind::With { record, body, .. } => {
            collect_pending(record, out);
            collect_pending(body, out);
        }
        _ => {}
    }
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

/// The schema lock is append-only and is NEVER written during `knot build`.
/// It is written only by `knot lock` (see `cmd_lock` in main.rs), which
/// promotes the pending migration into history. This writer regenerates the
/// full lock content from the committed history plus the current source.
///
/// `history` is the committed per-source migration chain read from the
/// existing lock (empty on first lock); `program`/`type_env` describe the
/// current source (which, at `knot lock` time, includes the migration being
/// committed). The emitted lock records, per source, every committed schema
/// version in order followed by the current schema.
pub fn write_lock(
    lock_path: &Path,
    source_text: &str,
    program: &Expr,
    type_env: &TypeEnv,
    history: &HashMap<String, Vec<CommittedMigration>>,
    prior_schemas: &HashMap<String, String>,
) -> Result<(), String> {
    let content = generate(program, source_text, type_env, history, prior_schemas);
    // Atomic write: write to a temp file then rename, so a crash mid-write
    // doesn't leave a corrupt lockfile that hard-errors every compile.
    let tmp_path = lock_path.with_extension("lock.tmp");
    std::fs::write(&tmp_path, &content)
        .map_err(|e| format!("cannot write {}: {}", tmp_path.display(), e))?;
    std::fs::rename(&tmp_path, &lock_path).map_err(|e| {
        format!(
            "cannot rename {} to {}: {}",
            tmp_path.display(),
            lock_path.display(),
            e
        )
    })
}

/// Collect the record-embedded `*name : <ty>` source declarations from a
/// module's top-level record-let literals (`db = { *todos : [Todo], … }`),
/// paired with the field's span for diagnostics. Duplicate source names are a
/// compile error, so name-keyed iteration is unambiguous.
fn record_embedded_sources(program: &Expr) -> Vec<(String, Type, Span)> {
    let mut out = Vec::new();
    collect_record_sources(program, &mut out);
    out
}

/// Recursively collect `*name : <ty>` source fields from every record literal
/// in the program (the file's declarations now live inside record literals).
fn collect_record_sources(e: &Expr, out: &mut Vec<(String, Type, Span)>) {
    match &e.node {
        ExprKind::Record(fields) => {
            for f in fields {
                if let ExprKind::SourceDecl { name, ty, .. } = &f.value.node {
                    out.push((name.clone(), ty.clone(), f.value.span));
                }
                collect_record_sources(&f.value, out);
            }
        }
        ExprKind::With { record, body, .. } => {
            collect_record_sources(record, out);
            collect_record_sources(body, out);
        }
        _ => {}
    }
}

fn find_source_span(program: &Expr, name: &str) -> Span {
    record_embedded_sources(program)
        .into_iter()
        .find_map(|(n, _, span)| (n == name).then_some(span))
        .unwrap_or(Span::new(0, 0))
}

/// Bounds-checked slice of `source_text` at `span`; `None` when the span is
/// synthetic/out-of-range (post-desugar nodes can carry placeholder spans).
fn slice_span(source_text: &str, span: Span) -> Option<&str> {
    source_text.get(span.start..span.end)
}

/// Generate lockfile content recording the full committed history. Emitted as
/// a `with { … } (main)` expression so it re-parses under the
/// file-as-expression grammar. Per source, emits every committed schema
/// version (oldest first) followed by the current schema, with a
/// `migrate to … using …` clause on each version after the first.
/// Generate lockfile content: a parseable section with the current schema per
/// source, plus a `migrate_history` section holding the committed chain as raw
/// schema descriptors. Emitted as a `with { … } (main)` expression.
fn generate(
    program: &Expr,
    source_text: &str,
    type_env: &TypeEnv,
    history: &HashMap<String, Vec<CommittedMigration>>,
    prior_schemas: &HashMap<String, String>,
) -> String {
    let mut body = String::new();

    // Type aliases (non-parameterized) and data declarations — copied verbatim
    // from source so the lock's current-schema type expressions resolve.
    for decl in decl_views(program) {
        let emit = matches!(decl.kind,
            DeclViewKind::TypeAlias { params, .. } if params.is_empty())
            || matches!(decl.kind, DeclViewKind::Data { .. });
        if emit && let Some(text) = slice_span(source_text, decl.span) {
            body.push_str(text);
            body.push('\n');
        }
    }

    // Current schema per source (type-first `<ty>  *name`), re-resolvable by
    // `TypeEnv` on the next read. This is the schema `check` diffs against.
    let sources = record_embedded_sources(program);
    for (name, ty, _) in &sources {
        body.push_str(&format!("{}  *{}\n", knot::format::render_type(ty), name));
    }

    // Committed migration history as raw descriptors. Includes the migration
    // being committed now (the pending block on the source), appended last.
    body.push_str("migrate_history [\n");
    for (name, chain) in history {
        for step in chain {
            body.push_str(&history_entry_src(name, step));
        }
    }
    for (name, _, _) in &sources {
        // The pending migration being committed: from = the lock's recorded
        // current schema before this lock (the baseline it upgrades), to =
        // current source schema.
        if let Some(using_src) = record_embedded_using_fn(program, name)
            && let Some(to_schema) = type_env.source_schemas.get(name)
        {
            let from_schema = prior_schemas.get(name).cloned().unwrap_or_default();
            body.push_str(&history_entry_src(
                name,
                &CommittedMigration {
                    from_schema,
                    to_schema: to_schema.clone(),
                    using_src,
                },
            ));
        }
    }
    body.push_str("]\n");

    let mut out = String::new();
    out.push_str("-- schema.lock (append-only — written only by `knot lock`)\n");
    out.push_str("-- Commit to source control. Do not edit by hand.\n");
    out.push_str("with {\n");
    out.push_str(&body);
    out.push_str("}\n(main)\n");
    out
}

/// Render one committed migration as a `{source …  from …  to …  using …}`
/// string-record entry of the `migrate_history` list.
fn history_entry_src(name: &str, step: &CommittedMigration) -> String {
    format!(
        "  {{source \"{}\"  from \"{}\"  to \"{}\"  using \"{}\"}}\n",
        name,
        escape_lock_string(&step.from_schema),
        escape_lock_string(&step.to_schema),
        escape_lock_string(&step.using_src),
    )
}

/// Escape a string for embedding in a knot text literal in the lock.
fn escape_lock_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Find the `using` fn source of the pending migration on a record-embedded
/// source field, if present.
fn record_embedded_using_fn(program: &Expr, name: &str) -> Option<String> {
    fn walk(e: &Expr, name: &str, out: &mut Option<String>) {
        match &e.node {
            ExprKind::Record(fields) => {
                for f in fields {
                    if let ExprKind::SourceDecl {
                        name: n,
                        migrations,
                        ..
                    } = &f.value.node
                    {
                        if *n == *name
                            && let Some(m) = migrations.first()
                        {
                            *out = Some(knot::format::render_expr_source(&m.using_fn));
                        }
                    }
                    walk(&f.value, name, out);
                }
            }
            ExprKind::With { record, body, .. } => {
                walk(record, name, out);
                walk(body, name, out);
            }
            _ => {}
        }
    }
    let mut out = None;
    walk(program, name, &mut out);
    out
}
