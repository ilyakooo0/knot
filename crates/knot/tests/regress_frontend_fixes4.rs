//! Regression tests for frontend fixes (round 4):
//!
//! 1. Record-literal parsing no longer speculatively parses the first element
//!    and then restores + reparses it on the no-`|` path. That reparse doubled
//!    the work at every nesting level, so a chain of nested record literals
//!    (`{{{...}}}`) took exponential time. The first element is now reused
//!    directly, making parsing linear — and record literals, punned fields,
//!    and record updates all still parse correctly.
//! 2. `parse_route_entries_with_prefix` recurses once per `/`-prefixed group
//!    line, so a long run of such lines overflowed the native stack and
//!    aborted the process. It now charges the shared recursion budget and
//!    emits a "nesting depth limit exceeded" diagnostic instead.

use knot::ast::{DeclKind, ExprKind};
use knot::diagnostic::Severity;

fn parse(source: &str) -> knot::ast::Module {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, _diags) = parser.parse_module();
    module
}

fn parse_errors(source: &str) -> Vec<String> {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (_module, diags) = parser.parse_module();
    diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.message.clone())
        .collect()
}

/// The body expression of the first `name = expr` declaration.
fn first_body(module: &knot::ast::Module) -> ExprKind {
    match &module.decls[0].node {
        DeclKind::Fun { body: Some(b), .. } => b.node.clone(),
        other => panic!("expected a Fun decl, got {:?}", other),
    }
}

// ── 1. Record parsing correctness + no exponential blowup ───────────────

#[test]
fn record_literal_named_fields_parse() {
    let m = parse("v = {a: 1, b: 2}\n");
    match first_body(&m) {
        ExprKind::Record(fields) => {
            let names: Vec<_> = fields.iter().map(|f| f.name.clone()).collect();
            assert_eq!(names, vec!["a", "b"]);
        }
        other => panic!("expected Record, got {:?}", other),
    }
}

#[test]
fn record_punned_fields_parse() {
    let m = parse("v = {a, b}\n");
    match first_body(&m) {
        ExprKind::Record(fields) => {
            assert_eq!(fields.len(), 2);
            // Punned `{a}` desugars to field `a` with value `a`.
            assert_eq!(fields[0].name, "a");
            assert!(matches!(fields[0].value.node, ExprKind::Var(_)));
        }
        other => panic!("expected Record, got {:?}", other),
    }
}

#[test]
fn record_update_parses() {
    let m = parse("v = {base | a: 1}\n");
    match first_body(&m) {
        ExprKind::RecordUpdate { base, fields } => {
            assert!(matches!(base.node, ExprKind::Var(_)));
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "a");
        }
        other => panic!("expected RecordUpdate, got {:?}", other),
    }
}

#[test]
fn record_update_with_field_access_base_parses() {
    // Base is a non-trivial expression — still needs the speculative path.
    let m = parse("v = {person.address | city: 1}\n");
    assert!(matches!(first_body(&m), ExprKind::RecordUpdate { .. }));
}

#[test]
fn deeply_nested_records_parse_quickly() {
    // Each level used to be parsed twice, giving O(2^n). With ~80 levels the
    // old parser would not finish in any reasonable time; the test completing
    // at all is the regression assertion. We nest record *literals* via a
    // named field so each level is a valid record.
    let depth = 80;
    let mut src = String::from("v = ");
    for _ in 0..depth {
        src.push_str("{a: ");
    }
    src.push('1');
    for _ in 0..depth {
        src.push('}');
    }
    src.push('\n');
    // Just parsing to completion proves the exponential blowup is gone.
    let _ = parse(&src);
}

// ── 2. Route path-prefix recursion is depth-bounded ─────────────────────

#[test]
fn deeply_nested_route_prefixes_diagnose_instead_of_crashing() {
    // Genuinely-nested `/`-prefix lines (each strictly more indented than the
    // last) recurse once per level, so a deep enough chain must surface a
    // recursion-depth diagnostic and return normally rather than overflowing
    // the stack. (Same-indent prefix lines are siblings, not nested — see
    // `same_indent_route_prefix_does_not_absorb_sibling`.)
    let mut src = String::from("route Api where\n");
    for depth in 0..400 {
        for _ in 0..(depth + 2) {
            src.push(' ');
        }
        src.push_str("/seg\n");
    }
    let errs = parse_errors(&src);
    assert!(
        errs.iter().any(|m| m.contains("nesting depth limit exceeded")),
        "expected a nesting-depth diagnostic, got: {:?}",
        errs
    );
}

#[test]
fn same_indent_route_prefix_does_not_absorb_sibling() {
    // A `/prefix` group must only absorb entries that are STRICTLY more
    // indented than it. A sibling entry at the same indentation is its own
    // top-level route and must NOT get the prefix prepended.
    let src = "route Api where\n  /todos\n  GET /x -> Int = GetX\n";
    let module = parse(src);
    let entries = match &module.decls[0].node {
        DeclKind::Route { entries, .. } => entries,
        other => panic!("expected a route declaration, got: {:?}", other),
    };
    assert_eq!(entries.len(), 1, "expected exactly one route entry");
    let path: Vec<&str> = entries[0]
        .path
        .iter()
        .map(|seg| match seg {
            knot::ast::PathSegment::Literal(s) => s.as_str(),
            knot::ast::PathSegment::Param { name, .. } => name.as_str(),
        })
        .collect();
    assert_eq!(
        path,
        vec!["x"],
        "same-indent sibling must keep its own path, not inherit the /todos prefix"
    );
}

#[test]
fn nested_route_prefix_absorbs_indented_child() {
    // The contrast case: an entry strictly more indented than `/prefix` IS
    // nested and gets the prefix prepended.
    let src = "route Api where\n  /todos\n    GET /x -> Int = GetX\n";
    let module = parse(src);
    let entries = match &module.decls[0].node {
        DeclKind::Route { entries, .. } => entries,
        other => panic!("expected a route declaration, got: {:?}", other),
    };
    assert_eq!(entries.len(), 1, "expected exactly one route entry");
    let path: Vec<&str> = entries[0]
        .path
        .iter()
        .map(|seg| match seg {
            knot::ast::PathSegment::Literal(s) => s.as_str(),
            knot::ast::PathSegment::Param { name, .. } => name.as_str(),
        })
        .collect();
    assert_eq!(path, vec!["todos", "x"], "indented child inherits the /todos prefix");
}

#[test]
fn modest_route_nesting_still_parses() {
    // A realistic prefix-nested route must still parse without tripping the guard.
    let src = "route Api where\n  /users\n    GET /count -> Int = GetUsers\n  /posts\n    GET /count -> Int = GetPosts\n";
    let errs = parse_errors(src);
    assert!(errs.is_empty(), "unexpected parse errors: {:?}", errs);
}
