//! Regression tests for stack overflows on `do` blocks with many statements.
//!
//! A `do` block's statements are a *flat* list in the source, so the parser's
//! nesting guard never fires on them. Desugaring then expands that list into
//! one nested `__bind` application per statement, and every later pass walks
//! the result recursively — so a few hundred sequential statements used to
//! overflow the native stack during type inference, aborting the process
//! instead of producing a diagnostic or an executable.
//!
//! The passes now run on a grown stack (`knot_compiler::stack`). These tests
//! pin that: they exercise the pure-comprehension path (which desugars to a
//! bind chain) at a depth that reliably aborted the process beforehand.
//!
//! Note the depths here are deliberately modest. Inference is superlinear in
//! bind-chain length, so a test at ten thousand statements would spend minutes
//! in the type checker rather than testing anything extra — the stack ceiling
//! now sits far above the depth that is practical to compile at all. What
//! these guard is that the *process survives*, which it previously did not.

use knot::diagnostic::{Diagnostic, Severity};

/// Statements in the generated `do` block. Comfortably past the point where
/// inference overflowed a default thread stack (~150 on libtest's 2 MiB
/// workers, ~500 on the main thread's 8 MiB), while still checking quickly.
const DEPTH: usize = 400;

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

fn errors(diags: &[Diagnostic]) -> Vec<&String> {
    diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| &d.message)
        .collect()
}

/// A pure comprehension: each `x <- nums` becomes one `__bind` level, so the
/// desugared AST is `DEPTH` deep even though the source is flat.
fn deep_comprehension(depth: usize) -> String {
    let mut src = String::from("nums = [1, 2, 3]\n\nres = do\n");
    for i in 0..depth {
        src.push_str(&format!("  x{i} <- nums\n"));
    }
    src.push_str("  where x0 > 0\n");
    src.push_str("  yield {v x0}\n\nmain = println (count res)\n");
    src
}

/// A long sequential IO block. This path is *not* desugared into a bind chain
/// (codegen handles IO `do` blocks directly), so it guards the other walkers.
fn deep_io_block(depth: usize) -> String {
    let mut src = String::from("main = do\n");
    for i in 0..depth {
        src.push_str(&format!("  println {i}\n"));
    }
    src
}

#[test]
fn deep_comprehension_do_block_type_checks_without_overflow() {
    let mut module = parse(&deep_comprehension(DEPTH));
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);

    let (diags, ..) = knot_compiler::infer::check(&mut module);
    assert!(
        errors(&diags).is_empty(),
        "deep comprehension should type-check cleanly, got: {:?}",
        errors(&diags)
    );

    // Effects and stratification walk the same bind chain.
    let effect_diags = knot_compiler::effects::check(&module);
    assert!(
        errors(&effect_diags).is_empty(),
        "deep comprehension should pass effect checking, got: {:?}",
        errors(&effect_diags)
    );
    assert!(errors(&knot_compiler::stratify::check(&module)).is_empty());
}

#[test]
fn deep_sequential_io_do_block_type_checks_without_overflow() {
    let mut module = parse(&deep_io_block(DEPTH));
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);

    let (diags, ..) = knot_compiler::infer::check(&mut module);
    assert!(
        errors(&diags).is_empty(),
        "deep IO do block should type-check cleanly, got: {:?}",
        errors(&diags)
    );
}

/// The whole pipeline, through Cranelift, on a deep bind chain — inference is
/// where it used to abort, but codegen recurses over the same chain. Kept
/// shallower than `DEPTH` because codegen is the slow end of the pipeline.
#[test]
fn deep_do_block_reaches_codegen_without_overflow() {
    let src = deep_comprehension(200);
    let mut module = parse(&src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);

    let type_env = knot_compiler::types::TypeEnv::from_module(&module);
    let (diags, monad_info, type_info, _local, refine_targets, refined, from_json, elem, trait_calls, show_units, sum_floats, relation_fields, with_fields, type_arg_spans) =
        knot_compiler::infer::check(&mut module);
    assert!(errors(&diags).is_empty(), "{:?}", errors(&diags));

    let obj = knot_compiler::codegen::compile(
        &module,
        &type_env,
        "deep_do.knot",
        &monad_info,
        &refine_targets,
        &refined,
        &from_json,
        &type_info,
        &elem,
        &trait_calls,
        &show_units,
        &sum_floats,
        &relation_fields,
        &with_fields,
        &type_arg_spans,
        &std::collections::HashMap::new(),
    )
    .expect("codegen should succeed on a deep do block");
    assert!(!obj.is_empty(), "codegen produced an empty object file");
}
