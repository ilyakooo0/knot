//! Regression tests for type-inference fixes (fourth batch):
//!
//! 1. A `do`-block `where` guard (and `empty`) desugars to `__empty`, which
//!    dispatches through the monad's `Alternative` impl. The desugar gate only
//!    excludes IO do-blocks, so a user-defined monad with Functor/Applicative/
//!    Monad but no `Alternative` was desugared and then blew up at codegen with
//!    a missing-impl panic (`Alternative_<Monad>_empty`). Inference now checks,
//!    after the monad is resolved, that the type actually has an `Alternative`
//!    impl and reports a clean diagnostic instead. `[]`, `Maybe`, and `Result`
//!    all ship `Alternative`, so the common cases are unaffected.

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem,  _show_units, _sum_floats, _rel_fields, _with_fields, _ty_args, _implicit_refs) =
        knot_compiler::infer::check(&mut module);
    diags
}

#[test]
fn where_guard_over_maybe_is_accepted() {
    // Maybe has a built-in Alternative impl, so a `where` guard must not be
    // flagged.
    let src = "safeDiv : Int 1 -> Int 1 -> Maybe Int 1\nsafeDiv = \\a b -> do\n  where b != 0\n  yield (a / b)\n";
    let diags = check_src(src);
    assert!(
        !diags.iter().any(|d| d.message.contains("Alternative")),
        "Maybe + where must not trip the Alternative check, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}
