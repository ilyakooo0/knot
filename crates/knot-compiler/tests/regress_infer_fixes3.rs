//! Regression tests for type-inference fixes (third batch):
//!
//! 1. A single-variant, parameterless record data type is registered both
//!    nominally (constructor application yields `Con(name)`) and as a record
//!    alias (a `: name` annotation/field type resolves to the record). The two
//!    must unify, so `Box {val: 5} : Box` type-checks instead of reporting a
//!    spurious "expected {val: Int}, found Box" mismatch.

use knot::diagnostic::Diagnostic;

fn parse(src: &str) -> knot::ast::Module {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(
        parse_diags.is_empty(),
        "unexpected parse diagnostics: {:?}",
        parse_diags
    );
    module
}

fn check_src(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem) =
        knot_compiler::infer::check(&module);
    diags
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── 1. Single-variant record constructor unifies with its own type name ──

#[test]
fn single_variant_record_ctor_matches_annotation() {
    let diags = check_src(
        r#"data Box = Box {val: Int}

ann : Box
ann = Box {val: 5}

main = println (show ann)
"#,
    );
    assert_clean(&diags);
}

#[test]
fn single_variant_nullary_record_ctor_matches_annotation() {
    let diags = check_src(
        r#"data Marker = Mk {}

ann : Marker
ann = Mk {}

main = println "ok"
"#,
    );
    assert_clean(&diags);
}

#[test]
fn single_variant_record_ctor_as_function_arg() {
    // The unification also fires when the value flows into a parameter typed
    // by the data-type name, not just a top-level annotation.
    let diags = check_src(
        r#"data Box = Box {val: Int}

unwrap : Box -> Int
unwrap = \b -> b.val

main = println (show (unwrap (Box {val: 7})))
"#,
    );
    assert_clean(&diags);
}

#[test]
fn multi_variant_record_ctor_still_checks() {
    // Sanity: multi-variant types were always fine and must stay fine.
    let diags = check_src(
        r#"data Shape = Circle {r: Int} | Square {s: Int}

ann : Shape
ann = Circle {r: 5}

main = println (show ann)
"#,
    );
    assert_clean(&diags);
}

#[test]
fn single_variant_record_ctor_rejects_wrong_field_type() {
    // The subsumption must not paper over a genuine field-type mismatch.
    let diags = check_src(
        r#"data Box = Box {val: Int}

ann : Box
ann = Box {val: "nope"}

main = println (show ann)
"#,
    );
    assert!(
        !diags.is_empty(),
        "assigning Text to an Int field should still be a type error"
    );
}
