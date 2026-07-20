//! Regression tests for Π-lite explicit type arguments: `\(T : Type) -> …`
//! type-witness lambdas and call-site type application `f Int x`.
//!
//! A type-witness param `T` is a compile-time-only binder (kind `Type`). The
//! lambda's type is `∀ t. Type -> body`; an application `f Int x` supplies the
//! type argument `Int`, which the typechecker substitutes for `t` and the
//! codegen erases (the witness has no runtime representation). The parser
//! glues `f Int x` into `f (Int x)` (constructor-application, like `Some 5`),
//! so both stages split the spine arity-aware.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use knot::diagnostic::Diagnostic;

// ── type-level helpers ─────────────────────────────────────────────

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
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem,  _show_units, _sum_floats, _rel_fields, _with_fields, _ty_args, _implicit_refs) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

// ── runtime helpers ────────────────────────────────────────────────

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn compile_and_run(test_name: &str, source: &str) -> (String, String, bool) {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_tyarg_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let knot = env!("CARGO_BIN_EXE_knot");
    let out = Command::new(knot)
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    assert!(
        out.status.success(),
        "knot build failed for {test_name}:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let exe = dir.join("prog");
    let c = Compiled { dir, exe };
    let run = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    (
        String::from_utf8_lossy(&run.stdout).into_owned(),
        String::from_utf8_lossy(&run.stderr).into_owned(),
        run.status.success(),
    )
}

// ── type-level tests ───────────────────────────────────────────────

/// A bare uppercase type name in argument position against a type-witness
/// lambda is consumed as a *type*, not rejected as an unknown constructor.
#[test]
fn type_arg_is_not_an_unknown_constructor() {
    let diags = check_src("apply = \\(T : Type) -> \\x -> x\nmain = apply Int 42\n");
    assert!(
        !has_error(&diags, "unknown constructor"),
        "bare `Int` type arg must not error as a constructor: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// When the witness is referenced by an annotation (`x : T`), the type
/// argument constrains the value: `apply Int "hi"` must be a type error.
#[test]
fn type_arg_constrains_annotated_value() {
    let diags = check_src("apply = \\(T : Type) -> \\x -> (x : T)\nmain = apply Int \"hi\"\n");
    assert!(
        has_error(&diags, "type mismatch"),
        "apply Int \"hi\" should be a type mismatch: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// …and the matching well-typed call passes inference.
#[test]
fn type_arg_accepts_matching_value() {
    let diags = check_src("apply = \\(T : Type) -> \\x -> (x : T)\nmain = apply Int 42\n");
    assert!(
        diags.iter().all(|d| !d.message.contains("type mismatch")),
        "apply Int 42 should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// The general case: the witness var may appear nested anywhere in the body
/// type (`\(T : Type) -> \f -> \x -> f x` — `T` is the param/result of `f`).
/// The `∀ t. Type -> …` encoding lets the caller instantiate it exactly.
#[test]
fn nested_witness_var_typechecks() {
    let diags = check_src(
        "apply2 = \\(T : Type) -> \\f -> \\x -> f x\ninc = \\n -> n + 1\nmain = apply2 Int inc 10\n",
    );
    assert!(
        !has_error(&diags, "type mismatch") && !has_error(&diags, "unknown constructor"),
        "apply2 Int inc 10 should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── runtime / erasure tests ────────────────────────────────────────

/// Erasure: the type argument has no runtime representation. `apply Int 42`
/// must run as the identity on `42` — not pass `Int` as a constructor value.
#[test]
fn erasure_identity() {
    let (stdout, stderr, ok) = compile_and_run(
        "tyarg_identity",
        "apply = \\(T : Type) -> \\x -> x\nmain = println (apply Int 42)\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.trim_start().starts_with("42"),
        "expected `42`, got:\n{stdout}"
    );
}

/// Erasure with the witness nested in the body type: `apply2 Int inc 10`
/// compiles to `inc 10` = `11`.
#[test]
fn erasure_nested_witness() {
    let (stdout, stderr, ok) = compile_and_run(
        "tyarg_apply2",
        "apply2 = \\(T : Type) -> \\f -> \\x -> f x\ninc = \\n -> n + 1\nmain = println (apply2 Int inc 10)\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.trim_start().starts_with("11"),
        "expected `11`, got:\n{stdout}"
    );
}

/// Two separate call sites of the same type-witness lambda at different types.
#[test]
fn erasure_two_types() {
    let (stdout, stderr, ok) = compile_and_run(
        "tyarg_two",
        "apply = \\(T : Type) -> \\x -> x\nmain = do\n  println (apply Int 42)\n  println (apply Text \"yo\")\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("42"), "expected `42` in:\n{stdout}");
    assert!(stdout.contains("\"yo\""), "expected `\"yo\"` in:\n{stdout}");
}

// ── multi-type-argument (curried) ─────────────────────────────────

/// A lambda with two type witnesses, fully applied in one call:
/// `const2 Int Text 99` consumes `Int` for `A` and `Text` for `B`, then the
/// value `99`. The result is the identity on `99`.
#[test]
fn multi_ty_arg_fully_applied() {
    let diags = check_src(
        "const2 = \\(A : Type) -> \\(B : Type) -> \\x -> x\nmain = const2 Int Text 99\n",
    );
    assert!(
        !has_error(&diags, "unknown constructor") && !has_error(&diags, "type mismatch"),
        "const2 Int Text 99 should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// Runtime: a multi-witness top-level def is compiled with the arity of its
/// value lambda (witness layers erased), so `const2 Int Text 99` runs as `99`.
#[test]
fn multi_ty_arg_fully_applied_runs() {
    let (stdout, stderr, ok) = compile_and_run(
        "tyarg_multi_full",
        "const2 = \\(A : Type) -> \\(B : Type) -> \\x -> x\nmain = println (const2 Int Text 99)\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.trim_start().starts_with("99"),
        "expected `99`, got:\n{stdout}"
    );
}

/// Partial application across defs: `step1 = const2 Int` keeps the remaining
/// `Forall` for `B`, so `step1 Text 99` supplies `Text` then `99`.
#[test]
fn multi_ty_arg_partial_across_defs() {
    let diags = check_src(
        "const2 = \\(A : Type) -> \\(B : Type) -> \\x -> x\nstep1 = const2 Int\nmain = step1 Text 99\n",
    );
    assert!(
        !has_error(&diags, "unknown constructor") && !has_error(&diags, "type mismatch"),
        "step1 = const2 Int; step1 Text 99 should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// Runtime for the partial-application case: `step1` is a one-value-arg
/// function (both witness layers erased), so `step1 Text 99` runs as `99`.
#[test]
fn multi_ty_arg_partial_across_defs_runs() {
    let (stdout, stderr, ok) = compile_and_run(
        "tyarg_multi_partial",
        "const2 = \\(A : Type) -> \\(B : Type) -> \\x -> x\nstep1 = const2 Int\nmain = println (step1 Text 99)\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.trim_start().starts_with("99"),
        "expected `99`, got:\n{stdout}"
    );
}
