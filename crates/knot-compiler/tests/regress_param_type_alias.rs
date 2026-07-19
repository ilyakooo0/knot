//! Parameterized type aliases applied to arguments (`Pair Int 1 Text`).
//! Previously only nullary aliases were registered; parameterized aliases were
//! dropped at collection time, so every applied reference failed to resolve.
//! Now the alias body is elaborated with fresh parameter variables and the
//! actual arguments are substituted.

use knot::diagnostic::Diagnostic;

fn check_src(src: &str) -> Vec<Diagnostic> {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (mut module, parse_diags) = parser.parse_module();
    assert!(
        parse_diags.is_empty(),
        "unexpected parse diagnostics: {:?}",
        parse_diags
    );
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _from_json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields, _with_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn param_alias_two_args_resolves() {
    let diags = check_src(
        "type Pair a b = {fst: a, snd: b}\nmain = with {p ({fst 1 snd \"x\"} : Pair Int 1 Text)} (println p.fst)\n",
    );
    assert_clean(&diags);
}

#[test]
fn param_alias_one_arg_resolves() {
    let diags = check_src(
        "type Wrapper a = {value: a}\nmain = with {w ({value 42} : Wrapper Int 1)} (println w.value)\n",
    );
    assert_clean(&diags);
}

#[test]
fn param_alias_arity_mismatch_errors() {
    let diags = check_src(
        "type Pair a b = {fst: a, snd: b}\nmain = with {p ({fst 1 snd \"x\"} : Pair Int 1)} (println 1)\n",
    );
    assert!(
        has_error(&diags, "type alias `Pair` expects 2 argument(s), but 1 were supplied"),
        "expected arity error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn param_alias_body_type_mismatch_errors() {
    let diags = check_src(
        "type Wrapper a = {value: a}\nmain = with {w ({value \"text\"} : Wrapper Int 1)} (println 1)\n",
    );
    assert!(
        has_error(&diags, "type mismatch"),
        "expected a type mismatch, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn param_alias_independent_uses_do_not_pin() {
    // Two uses of the same alias at DIFFERENT argument types must both
    // type-check (fresh parameter variables per use, not one shared pinning).
    let diags = check_src(
        "type Wrapper a = {value: a}\nmain = with {a ({value 1} : Wrapper Int 1)}\n         with {b ({value \"x\"} : Wrapper Text)} (println a.value)\n",
    );
    assert_clean(&diags);
}

#[test]
fn nullary_alias_still_resolves() {
    let diags = check_src(
        "type Point = {x: Int 1, y: Int 1}\nmain = with {p ({x 1 y 2} : Point)} (println p.x)\n",
    );
    assert_clean(&diags);
}
