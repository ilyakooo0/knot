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
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem, _trait_calls, _show_units) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── Open effect-row subsumption respects direction ──
//
// An annotation with an *open* effect row that carries fixed effects
// (`IO {console | _}`) is a supertype: a value performing FEWER effects (here a
// pure relation read, `IO {}`) must check against it. Previously the open-vs-
// closed unification arms ignored the direction flag and rejected the open
// side's fixed effects unconditionally, yielding a spurious
// "IO has unexpected effects: {console}".

#[test]
fn open_effect_row_annotation_accepts_fewer_effects() {
    let diags = check_src(
        r#"*items : [{id: Int}]

getItems : IO {console | _} [{id: Int}]
getItems = *items
"#,
    );
    assert_clean(&diags);
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

#[test]
fn distinct_single_variant_records_do_not_unify() {
    // The single-variant subsumption must bridge a `Con` only with a
    // structural type (record/var), never with another nominal single-variant
    // `Con`. Two distinct newtypes with the same field shape must stay
    // distinct — otherwise nominal typing (and units-of-confusion safety) is
    // defeated: a `UserId` would be accepted where an `Email` is required.
    let diags = check_src(
        r#"data UserId = UserId {raw: Text}
data Email = Email {raw: Text}

greet : Email -> Text
greet = \e -> "Hello " ++ e.raw

main = println (greet (UserId {raw: "12345"}))
"#,
    );
    assert!(
        !diags.is_empty(),
        "passing a UserId where an Email is required must be a type error"
    );
}
