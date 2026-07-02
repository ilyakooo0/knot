//! Regression tests for frontend fixes (round 8):
//!
//! 1. A single-line `migrate` with an inline refined `from`/`to` type
//!    silently mis-parsed: the refined type's `where` predicate is parsed as
//!    an *expression*, whose application loop consulted `can_start_atom`,
//!    which honored `stop_type_at_headers` but not
//!    `stop_type_at_migrate_clauses`. So the predicate greedily consumed the
//!    `to`/`using` clause keywords as arguments, producing
//!    "expected 'to' in migrate declaration". `can_start_atom` now stops at
//!    `to`/`using` while parsing a migrate clause type, mirroring the route
//!    `headers`/`rateLimit` guard.

use knot::ast::DeclKind;
use knot::diagnostic::Severity;

fn parse(source: &str) -> (knot::ast::Module, Vec<knot::diagnostic::Diagnostic>) {
    let (tokens, _) = knot::lexer::Lexer::new(source).tokenize();
    knot::parser::Parser::new(source.to_string(), tokens).parse_module()
}

#[test]
fn single_line_migrate_with_inline_refined_from_type_parses() {
    let src = "*users : [{v: Int}]\nf = \\x -> \"hi\"\nmigrate *users from Int where \\x -> x > 0 to Text using f\n";
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
    let migrates: Vec<_> = module
        .decls
        .iter()
        .filter(|d| matches!(d.node, DeclKind::Migrate { .. }))
        .collect();
    assert_eq!(
        migrates.len(),
        1,
        "expected exactly one migrate declaration to parse"
    );
    if let DeclKind::Migrate { relation, using_fn, .. } = &migrates[0].node {
        assert_eq!(relation, "users");
        assert!(
            matches!(using_fn.node, knot::ast::ExprKind::Var(ref n) if n == "f"),
            "the `using f` clause must survive the refined-type predicate parse"
        );
    }
}

#[test]
fn multi_line_migrate_with_inline_refined_type_still_parses() {
    // The multi-line form already parsed (layout guards rescued it); guard
    // against a regression from the single-line fix.
    let src = "*users : [{v: Int}]\nf = \\x -> \"hi\"\nmigrate *users\n  from Int where \\x -> x > 0\n  to Text\n  using f\n";
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
    assert_eq!(
        module
            .decls
            .iter()
            .filter(|d| matches!(d.node, DeclKind::Migrate { .. }))
            .count(),
        1
    );
}
