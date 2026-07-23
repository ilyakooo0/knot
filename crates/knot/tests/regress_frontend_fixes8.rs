//! Regression tests for frontend fixes (round 8):
//!
//! 1. A `migrate` clause with an inline refined `from`/`to` type silently
//!    mis-parsed: the refined type's `where` predicate is parsed as an
//!    *expression*, whose application loop consulted `can_start_atom`, which
//!    honored `stop_type_at_headers` but not `stop_type_at_migrate_clauses`.
//!    So the predicate greedily consumed the `to`/`using` clause keywords as
//!    arguments, producing "expected 'to' in migrate". `can_start_atom` now
//!    stops at `to`/`using` while parsing a migrate clause type, mirroring the
//!    route `headers`/`rateLimit` guard.
//!
//! The standalone top-level `migrate *rel …` declaration was removed; these
//! tests now exercise the record-embedded migrate CLAUSE form
//! (`*users : [T] migrate from A to B using f`), which shares the same
//! `stop_type_at_migrate_clauses` guard.

use knot::ast::ExprKind;
use knot::diagnostic::Severity;

fn parse(source: &str) -> (knot::ast::Module, Vec<knot::diagnostic::Diagnostic>) {
    let (tokens, _) = knot::lexer::Lexer::new(source).tokenize();
    knot::parser::Parser::new(source.to_string(), tokens).parse_module()
}

/// Extract the migrations of the first `SourceDecl` field in the first record
/// literal of the module's first function body.
fn first_source_migrations(module: &knot::ast::Module) -> Vec<knot::ast::SourceMigration> {
    use knot::ast::DeclKind;
    for decl in &module.decls {
        if let DeclKind::Fun {
            body: Some(body), ..
        } = &decl.node
        {
            if let ExprKind::Record(fields) = &body.node {
                for f in fields {
                    if let ExprKind::SourceDecl { migrations, .. } = &f.value.node {
                        return migrations.clone();
                    }
                }
            }
        }
    }
    Vec::new()
}

#[test]
fn record_migrate_clause_with_inline_refined_from_type_parses() {
    let src = "db =\n  { *users : [{v: Int}] migrate from Int where \\x -> x > 0 to Text using (\\x -> \"hi\")\n  }\n";
    let (module, diags) = parse(src);
    assert!(
        !diags.iter().any(|d| d.severity == Severity::Error),
        "unexpected parse errors: {:?}",
        diags
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
    let migrations = first_source_migrations(&module);
    assert_eq!(
        migrations.len(),
        1,
        "expected exactly one migrate clause to parse"
    );
    assert!(
        matches!(migrations[0].using_fn.node, ExprKind::Lambda { .. }),
        "the `using` clause must survive the refined-type predicate parse"
    );
}

#[test]
fn record_migrate_clause_multi_line_with_inline_refined_type_parses() {
    let src = "db =\n  { *users : [{v: Int}]\n      migrate from Int where \\x -> x > 0\n      to Text\n      using (\\x -> \"hi\")\n  }\n";
    let (module, diags) = parse(src);
    assert!(
        !diags.iter().any(|d| d.severity == Severity::Error),
        "unexpected parse errors: {:?}",
        diags
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .map(|d| &d.message)
            .collect::<Vec<_>>()
    );
    assert_eq!(first_source_migrations(&module).len(), 1);
}
