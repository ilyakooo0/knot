//! Regression tests for frontend fixes (round 5):
//!
//! 1. The iterative parse loops that build left/right-spined ASTs —
//!    binary-operator chains (`1+1+…`), application chains (`f a b …`), and
//!    field-access chains (`x.a.b…`) — did not charge the parser's recursion
//!    budget, unlike the already-guarded nested forms (`((…))`, long `++`/`->`
//!    chains, route prefixes). A pathological flat chain therefore parsed
//!    "successfully" into an AST thousands of nodes deep, whose first recursive
//!    traversal (the default `Drop`, type inference, codegen, or a `Debug`
//!    format in a diagnostic) overflowed the native stack and aborted the
//!    process. Each loop now charges one depth unit per spine node and emits a
//!    "nesting depth limit exceeded" diagnostic instead, while modest chains
//!    still parse cleanly.

use knot::diagnostic::Severity;

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

fn assert_depth_diag(src: &str, what: &str) {
    let errs = parse_errors(src);
    assert!(
        errs.iter().any(|m| m.contains("nesting depth limit exceeded")),
        "expected a nesting-depth diagnostic for {what}, got: {errs:?}"
    );
}

#[test]
fn long_left_assoc_binop_chain_diagnoses_instead_of_crashing() {
    let mut src = String::from("x = 1");
    for _ in 0..50_000 {
        src.push_str("+1");
    }
    src.push('\n');
    assert_depth_diag(&src, "binop chain");
}

#[test]
fn long_application_chain_diagnoses_instead_of_crashing() {
    let mut src = String::from("x = f");
    for _ in 0..50_000 {
        src.push_str(" g");
    }
    src.push('\n');
    assert_depth_diag(&src, "application chain");
}

#[test]
fn long_field_access_chain_diagnoses_instead_of_crashing() {
    let mut src = String::from("x = r");
    for _ in 0..50_000 {
        src.push_str(".a");
    }
    src.push('\n');
    assert_depth_diag(&src, "field-access chain");
}

#[test]
fn long_type_application_chain_diagnoses_instead_of_crashing() {
    // The type-side application loop (`T a a a …`) builds a left-spine the same
    // way `parse_application` does. It was the one such loop that omitted the
    // depth charge, so a pathological type-app chain parsed "successfully" into
    // an unbounded-depth AST that overflowed the stack on first traversal.
    let mut src = String::from("f : T");
    for _ in 0..50_000 {
        src.push_str(" a");
    }
    src.push_str("\nf = x\n");
    assert_depth_diag(&src, "type application chain");
}

#[test]
fn modest_chains_still_parse() {
    // Realistic expressions well under the depth budget must not trip the guard.
    let cases = [
        "x = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8\n",
        "x = f a b c d e\n",
        "x = r.a.b.c.d\n",
        "x = f a.b (g c) + h d e\n",
        "f : Maybe (Result Text Int) -> List a -> Map k v\nf = x\n",
    ];
    for src in cases {
        let errs = parse_errors(src);
        assert!(errs.is_empty(), "unexpected parse errors for {src:?}: {errs:?}");
    }
}
