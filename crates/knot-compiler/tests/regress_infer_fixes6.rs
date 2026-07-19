//! Regression tests for type-inference fixes (sixth batch):
//!
//! B21. Annotation aliases with free type variables were order-dependent.
//!      For `type Box = {val: a}` and a function `f : Box -> Box`, each
//!      textual reference to `Box` freshens the alias body's free var `a`,
//!      but those freshly-minted vars were never added to `annotation_vars`.
//!      The pre-registered scheme therefore left them unquantified and shared
//!      across every call site until the decl was re-generalized — which never
//!      happens for a decl whose callers are processed first. As a result the
//!      first use pinned the alias (e.g. `f {val: 1}` fixes it to Int) and any
//!      later use at another type (`f {val: "s"}`) was falsely rejected.
//!      Declaring `f` *before* its callers compiled, exposing the ordering bug.

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields, _with_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn errors(diags: &[Diagnostic]) -> Vec<&Diagnostic> {
    diags
        .iter()
        .filter(|d| matches!(d.severity, knot::diagnostic::Severity::Error))
        .collect()
}

/// The regressing case: `f` is declared *after* the decl that uses it at two
/// different types. Before the fix the shared alias var pinned to Int at the
/// first call and rejected the Text call.
#[test]
fn b21_alias_free_var_is_order_independent() {
    let src = "type Box = {val: a}\n\
               callBoth : Text\n\
               callBoth = show (f {val: 1}) ++ show (f {val: \"s\"})\n\
               f : Box -> Box\n\
               f = \\b -> b\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).is_empty(),
        "an alias with a free type var must be polymorphic across call sites \
         regardless of declaration order: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// Control: the same program with `f` declared *before* its caller already
/// compiled prior to the fix. It must keep compiling.
#[test]
fn b21_forward_declared_alias_still_compiles() {
    let src = "type Box = {val: a}\n\
               f : Box -> Box\n\
               f = \\b -> b\n\
               callBoth : Text\n\
               callBoth = show (f {val: 1}) ++ show (f {val: \"s\"})\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).is_empty(),
        "forward-declared alias usage regressed: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// The alias may also nest inside another type constructor. `[Box] -> [Box]`
/// still freshens the body var per reference; both list-element uses must stay
/// independent so the two call sites can specialise to Int and Text.
#[test]
fn b21_nested_alias_free_var_is_order_independent() {
    let src = "type Box = {val: a}\n\
               callBoth : Text\n\
               callBoth = show (wrap [{val: 1}]) ++ show (wrap [{val: \"s\"}])\n\
               wrap : [Box] -> [Box]\n\
               wrap = \\xs -> xs\n";
    let diags = check_src(src);
    assert!(
        errors(&diags).is_empty(),
        "a free alias var nested under `[]` must stay polymorphic across call \
         sites regardless of declaration order: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}
