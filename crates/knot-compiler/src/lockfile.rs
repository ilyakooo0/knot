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
    /// relation_name → (from_schema, to_schema)
    migrations: HashMap<String, (String, String)>,
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
                if let Some((from, to)) = type_env.migrate_schemas.get(name) {
                    if from != old_schema || to != new_schema {
                        diags.push(
                            Diagnostic::error(format!(
                                "migrate block for '*{}' doesn't match the schema change",
                                name
                            ))
                            .label(find_migrate_span(module, name), "here")
                            .note(format!("lockfile schema: {}", old_schema))
                            .note(format!("source schema:   {}", new_schema))
                            .note(format!("migrate from:    {}", from))
                            .note(format!("migrate to:      {}", to)),
                        );
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
            None => {
                diags.push(Diagnostic::warning(format!(
                    "source '*{}' in lockfile but not in source — data may be orphaned",
                    name
                )));
            }
        }
    }

    // Ensure lockfile migrations aren't removed from source
    for (name, (lock_from, lock_to)) in &old.migrations {
        let still_present = type_env
            .migrate_schemas
            .get(name)
            .map_or(false, |(f, t)| f == lock_from && t == lock_to);
        if !still_present {
            diags.push(
                Diagnostic::error(format!(
                    "migration for '*{}' removed from source but present in lockfile",
                    name
                ))
                .note("remove from lockfile explicitly to prune old migrations"),
            );
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
