//! Regression tests for frontend fixes (round 6):
//!
//! 1. A declaration with a missing/empty RHS swallowed the following
//!    declaration: `parse_expr_head` unconditionally skipped newlines before
//!    reading the head atom, with no column-0 guard on the *first* atom (the
//!    continuation guards only fired after it). So in `greet =\nmain = …` the
//!    parser read `main` as `greet`'s body, then choked on `main`'s `=` and
//!    dropped the entire `main` declaration. It now reports a missing
//!    expression and leaves the next declaration intact.
//!
//! 2. The formatter dropped a comment sitting between a standalone `export`
//!    and its declaration (`export -- keep me\ntype A = Int`): the comment is
//!    non-standalone and lies before the decl's span (which starts after
//!    `export`), so it matched none of the comment-association branches.

use knot::ast::DeclKind;
use knot::diagnostic::Severity;

fn parse(source: &str) -> (Vec<String>, Vec<String>) {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, diags) = parser.parse_module();
    let names = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            DeclKind::Fun { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    let errs = diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.message.clone())
        .collect();
    (names, errs)
}

#[test]
fn empty_rhs_does_not_swallow_next_declaration() {
    let (names, errs) = parse("greet =\nmain = println greet\n");
    // The following *valid* declaration must survive and parse on its own,
    // rather than being consumed as `greet`'s body (the old bug merged `main`
    // into `greet` and lost it entirely).
    assert!(
        names.contains(&"main".to_string()),
        "the following declaration must survive empty-RHS recovery, got: {names:?}"
    );
    assert!(
        !errs.is_empty(),
        "an empty RHS should report a diagnostic"
    );
}

#[test]
fn empty_rhs_chain_keeps_following_valid_declaration() {
    // `a` and `b` are incomplete (no body) and drop out via skip-and-continue
    // recovery, but the trailing valid `c = 4` must still be parsed.
    let (names, _) = parse("a =\nb =\nc = 4\n");
    assert!(
        names.contains(&"c".to_string()),
        "the valid trailing declaration should survive, got: {names:?}"
    );
}

#[test]
fn indented_continuation_still_parses_as_one_body() {
    // A legitimately indented body on the next line must NOT trigger the
    // empty-RHS guard (column > block indent).
    let (names, errs) = parse("f =\n  g x\nmain = f\n");
    assert_eq!(names, vec!["f".to_string(), "main".to_string()]);
    assert!(errs.is_empty(), "indented continuation should parse cleanly, got: {errs:?}");
}

#[test]
fn formatter_preserves_comment_between_export_and_decl() {
    let src = "export -- keep me\ntype A = Int\n";
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, _) = parser.parse_module();
    let formatted = knot::format::format_module(src, &module);
    assert!(
        formatted.contains("-- keep me"),
        "comment on the `export` line must be preserved, got:\n{formatted}"
    );
    assert!(
        formatted.contains("type A = Int"),
        "the declaration must still be present, got:\n{formatted}"
    );
}
