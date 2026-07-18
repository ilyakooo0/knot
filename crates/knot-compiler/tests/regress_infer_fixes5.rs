//! Regression tests for type-inference fixes (fifth batch):
//!
//! 1. Refined types were laundered by arithmetic: every `Num`/negation/
//!    `Semigroup` op returned the operand's type unchanged, so `Nat - Nat`
//!    was typed `Nat` even though the difference can be negative. The result
//!    now degrades to the refined type's base, so the caller must `refine` to
//!    re-introduce the refinement.
//! 2. `is_concrete_refinement_base` omitted `Ty::Con`, so a refined type over
//!    a nominal ADT base (`type Warm = Color where …`) let a plain `Color`
//!    value be laundered into `Warm` with no predicate check.
//! 3. Unit-exponent arithmetic used unchecked `i32` ops, so a type-correct
//!    program with absurd exponents crashed the compiler with an overflow
//!    panic. Exponent math now saturates.

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn errors(diags: &[Diagnostic]) -> Vec<&Diagnostic> {
    diags
        .iter()
        .filter(|d| matches!(d.severity, knot::diagnostic::Severity::Error))
        .collect()
}

#[test]
fn arithmetic_does_not_launder_refinement() {
    // `a - b` for `a, b : Nat` can be negative, so it must NOT be typed `Nat`.
    // With the result degraded to the base `Int`, the declared `Nat` return
    // forces the introducing-subsumption error demanding `refine`.
    let src = "type Nat = Int 1 where \\x -> x >= 0\n\
               sub : Nat -> Nat -> Nat\n\
               sub = \\a b -> a - b\n";
    let diags = check_src(src);
    let errs = errors(&diags);
    assert!(
        errs.iter().any(|d| d.message.contains("refine")),
        "arithmetic laundering a refinement must be rejected: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn negation_does_not_launder_refinement() {
    let src = "type Nat = Int 1 where \\x -> x >= 0\n\
               neg : Nat -> Nat\n\
               neg = \\a -> -a\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).iter().any(|d| d.message.contains("refine")),
        "negation laundering a refinement must be rejected: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn refined_arithmetic_result_is_usable_as_base() {
    // The degraded result is the base type, so returning `Int` is fine — this
    // must still type-check (no over-rejection of legitimate code).
    let src = "type Nat = Int 1 where \\x -> x >= 0\n\
               diff : Nat -> Nat -> Int 1\n\
               diff = \\a b -> a - b\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).is_empty(),
        "arithmetic on refined operands may be used at the base type: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn refinement_over_adt_base_requires_refine() {
    // A refined type over a nominal ADT base must not accept a raw base value
    // without `refine` (regression: `is_concrete_refinement_base` omitted Con).
    let src = "data Color = Red {} | Green {} | Blue {}\n\
               type Warm = Color where \\c -> True\n\
               launder : Color -> Warm\n\
               launder = \\c -> c\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).iter().any(|d| d.message.contains("refine")),
        "laundering a nominal value into a refined ADT type must be rejected: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn refinement_forgetting_direction_still_allowed() {
    // Using a refined value where its base is required (forgetting the
    // refinement) is always sound and must keep compiling.
    let src = "type Nat = Int 1 where \\x -> x >= 0\n\
               use : Nat -> Int 1\n\
               use = \\n -> n\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).is_empty(),
        "forgetting a refinement must remain allowed: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn unit_exponent_overflow_does_not_panic() {
    // A type-correct program with absurd unit exponents must not crash the
    // compiler (regression: unchecked i32 exponent arithmetic panicked).
    let src = "unit M\n\
               area : Float (M^2000000000)\n\
               area = (1.0 : Float (M^2000000000))\n\
               sq : Float (M^2000000000) -> Float 1\n\
               sq = \\x -> stripFloatUnit (x * x)\n";
    // The only assertion that matters is that `check_src` returns without
    // panicking; the exponent saturates instead of overflowing.
    let _ = check_src(src);
}
