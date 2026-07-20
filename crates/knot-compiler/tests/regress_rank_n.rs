//! Regression tests for rank-N inline `forall` on lambda parameters:
//! `\(f : (forall a. a -> a)) -> …` gives `f` a polymorphic type inside the
//! lambda body, so it can be used at several types in one call.
//!
//! The parser produces `PatKind::Annot { pat, ty }` for `(x : T)`; the
//! typechecker binds the inner pattern at the annotated type, and when `T` is
//! a `forall` the bound variable gets a polymorphic Scheme (rank-N).

use std::fs;
use std::path::PathBuf;
use std::process::Command;

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
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _m, _t, _l, _tg, _r, _j, _e, _su, _sf, _rf, _wf, _ta, _ir) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

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
        "knot_regress_rankn_{}_{}",
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

/// A monomorphic annotated lambda param parses and typechecks:
/// `\(x : Int 1) -> x + 1` applied to `5` gives `6`.
#[test]
fn annot_param_monomorphic() {
    let diags = check_src("main = ((\\(x : Int 1) -> x + 1) 5)\n");
    assert!(
        !has_error(&diags, "type mismatch") && !has_error(&diags, "expected"),
        "\\(x : Int 1) -> x + 1 should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// Rank-N: an inline `forall` param annotation lets `f` be used polymorphically
/// inside the lambda — `f` applied at `Int` and at `Text` in the same body.
#[test]
fn rank_n_param_used_at_two_types() {
    let diags = check_src(
        "useBoth = \\(f : (forall a. a -> a)) -> do\n  println (f 42)\n  println (f \"yo\")\n\
         id = \\x -> x\n\
         main = useBoth id\n",
    );
    assert!(
        !has_error(&diags, "type mismatch") && !has_error(&diags, "escapes") && !has_error(&diags, "rigid"),
        "rank-N param used at two types should typecheck: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

/// Runtime: rank-N param actually runs — `useBoth id` prints `42` then `"yo"`.
#[test]
fn rank_n_param_runs() {
    let (stdout, stderr, ok) = compile_and_run(
        "rankn_useboth",
        "useBoth = \\(f : (forall a. a -> a)) -> do\n  println (f 42)\n  println (f \"yo\")\n\
         id = \\x -> x\n\
         main = useBoth id\n",
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("42"), "expected `42` in:\n{stdout}");
    assert!(stdout.contains("\"yo\""), "expected `\"yo\"` in:\n{stdout}");
}

/// A rank-N param that is NOT polymorphic enough must be rejected: passing a
/// monomorphic `inc : Int -> Int` where `forall a. a -> a` is required.
#[test]
fn rank_n_rejects_monomorphic_arg() {
    let diags = check_src(
        "useBoth = \\(f : (forall a. a -> a)) -> do\n  println (f 42)\n  println (f \"yo\")\n\
         inc = \\n -> n + 1\n\
         main = useBoth inc\n",
    );
    assert!(
        has_error(&diags, "type mismatch") || has_error(&diags, "escapes") || has_error(&diags, "rigid"),
        "useBoth inc (monomorphic) should be a type error: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}
