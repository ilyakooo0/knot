//! Schema lockfile management.
//!
//! Maintains a `<name>.schema.lock` file alongside each source file,
//! tracking persisted relation schemas and migration history.
//! The lockfile is valid Knot syntax, parsed by the same frontend.

use crate::types::TypeEnv;
use knot::ast::*;
use knot::diagnostic::{Diagnostic, Severity};
use std::collections::HashMap;
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
                    let fty = fp.next().unwrap_or("text").to_string();
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

fn classify_adt_change(old: &str, new: &str) -> SchemaChange {
    let old_ctors = parse_adt_constructors(old);
    let new_ctors = parse_adt_constructors(new);

    // Every old constructor must exist in new with identical fields
    for (old_name, old_fields) in &old_ctors {
        match new_ctors.iter().find(|(n, _)| n == old_name) {
            Some((_, new_fields)) => {
                if old_fields != new_fields {
                    return SchemaChange::Breaking;
                }
            }
            None => return SchemaChange::Breaking,
        }
    }

    // All old constructors preserved — new ones are safe additions
    SchemaChange::Safe
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

/// Split a string by `sep` while respecting `[...]` bracket nesting.
fn split_respecting_brackets(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
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

    // Every old field must exist in new with same type
    for (old_name, old_ty) in &old_fields {
        match new_fields.iter().find(|(n, _)| n == old_name) {
            Some((_, new_ty)) => {
                if old_ty != new_ty {
                    return SchemaChange::Breaking;
                }
            }
            None => return SchemaChange::Breaking,
        }
    }

    // New fields added → Breaking for now (Safe once Maybe/nullable is supported)
    if new_fields.len() > old_fields.len() {
        return SchemaChange::Breaking;
    }

    SchemaChange::Identical
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

                                if first_from != old_schema || last_to != new_schema {
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
                                    if migrations[i - 1].1 != migrations[i].0 {
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
            let still_present = source_migrations.map_or(false, |sm| {
                sm.iter().any(|(f, t)| f == lock_from && t == lock_to)
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

/// Write the lockfile after a successful compile.
/// Only writes if the module has source declarations.
pub fn update(source_path: &Path, source_text: &str, module: &Module) -> Result<(), String> {
    let has_sources = module
        .decls
        .iter()
        .any(|d| matches!(&d.node, DeclKind::Source { .. }));
    if !has_sources {
        return Ok(());
    }

    let lock_path = lockfile_path(source_path);
    let content = generate(module, source_text);
    std::fs::write(&lock_path, content)
        .map_err(|e| format!("cannot write {}: {}", lock_path.display(), e))
}

fn find_source_span(module: &Module, name: &str) -> Span {
    module
        .decls
        .iter()
        .find_map(|d| match &d.node {
            DeclKind::Source { name: n, .. } if n == name => Some(d.span),
            _ => None,
        })
        .unwrap_or(Span::new(0, 0))
}

fn find_migrate_span(module: &Module, name: &str) -> Span {
    module
        .decls
        .iter()
        .find_map(|d| match &d.node {
            DeclKind::Migrate { relation, .. } if relation == name => Some(d.span),
            _ => None,
        })
        .unwrap_or(Span::new(0, 0))
}

/// Generate lockfile content by extracting declarations from source text.
fn generate(module: &Module, source_text: &str) -> String {
    let mut out = String::new();
    out.push_str("-- schema.lock (auto-generated, do not edit)\n");
    out.push_str("-- Commit to source control.\n");

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

    // Migrate declarations
    for decl in &module.decls {
        if let DeclKind::Migrate { .. } = &decl.node {
            out.push('\n');
            out.push_str(&source_text[decl.span.start..decl.span.end]);
            out.push('\n');
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
    fn record_field_added_is_breaking() {
        assert_eq!(
            classify_schema_change("name:text", "name:text,age:int"),
            SchemaChange::Breaking
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
}
