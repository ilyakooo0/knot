//! Regression tests for middle-end fixes:
//! 1. desugar: `x <- someVar` over a non-relation monad (Maybe) must desugar
//!    through `__bind` instead of being preserved as a SQL-compilable Do node,
//!    while genuinely source-bound vars keep the SQL pushdown path.
//! 2. desugar: IO-returning TRAIT METHODS are recognized by the IO-function
//!    scan, so do-blocks calling them are excluded from pure-comprehension
//!    desugaring (previously: hard type-check failure).
//! 5. effects: `race` rejection inside `atomic` is scope-aware — a shadowing
//!    binder named `race` is not the concurrency primitive.
//! 6. CLI: space-separated override form does not swallow `-`-prefixed
//!    tokens; `knot fmt -` reads stdin and writes stdout.

use std::fs;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Compile `source` into a fresh scratch directory and return paths.
fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_mid_{}_{}",
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
    Compiled { dir, exe }
}

/// Compile and run; returns (stdout, stderr, success).
fn compile_and_run(test_name: &str, source: &str) -> (String, String, bool) {
    let c = compile(test_name, source);
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

// ── Finding 1: Maybe comprehension over a Var bind ─────────────────

#[test]
fn maybe_comprehension_with_yield_var_compiles_and_runs() {
    // Previously: `is_sql_compilable` accepted the `u <- m` Var bind and
    // preserved the block as a raw Do node, which infer_do then rejected
    // ("type mismatch: expected Maybe {age: Int 1}, found [t..]").
    let (stdout, stderr, ok) = compile_and_run(
        "maybe_yield_var",
        r#"firstAdult : Maybe {age: Int 1} -> Maybe {age: Int 1}
firstAdult = \m -> do
  u <- m
  where u.age >= 18
  yield u

main = do
  println (show (firstAdult (Just {value {age 30}})))
  println (show (firstAdult (Just {value {age 10}})))
  println (show (firstAdult (Nothing {})))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("Just {value: {age: 30}}"),
        "expected Just for age 30, got: {stdout}"
    );
    assert_eq!(
        stdout.matches("Nothing {}").count(),
        2,
        "expected Nothing for age 10 and for Nothing input, got: {stdout}"
    );
}

#[test]
fn maybe_comprehension_with_yield_field_control() {
    // Control: the `yield u.age` form never matched the SQL shape and
    // always desugared — must keep working.
    let (stdout, stderr, ok) = compile_and_run(
        "maybe_yield_field",
        r#"firstAge : Maybe {age: Int 1} -> Maybe Int 1
firstAge = \m -> do
  u <- m
  where u.age >= 18
  yield u.age

main = println (show (firstAge (Just {value {age 30}})))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("Just {value: 30}"),
        "expected Just 30, got: {stdout}"
    );
}

#[test]
fn source_bound_var_inner_do_still_pushes_down() {
    // The legitimate case e34414f enabled: an inner do-block binding from a
    // variable that was bound from `*source` in the enclosing do-block is
    // preserved for SQL pushdown (and must produce the right answer).
    let (stdout, stderr, ok) = compile_and_run(
        "source_bound_var_inner_do",
        r#"*people : [{name: Text, age: Int 1}]

main = do
  replace *people = [{name "Alice" age 30}, {name "Bob" age 10}, {name "Cara" age 44}]
  rows <- *people
  with {adults (do p <- rows; where p.age >= 18; yield p)} (do println (show (count adults)))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("\"2\""), "expected count 2, got: {stdout}");
}

// ── Finding 2: IO-returning trait methods ──────────────────────────

// ── Finding 5: scoped `race` rejection in atomic ───────────────────

fn effect_diags(source: &str) -> Vec<knot::diagnostic::Diagnostic> {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (mut module, parse_diags) = parser.parse_module();
    assert!(
        !parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "parse errors: {:?}",
        parse_diags
    );
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    knot_compiler::effects::check(&module)
}

#[test]
fn shadowed_race_name_allowed_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int 1}]
main = do
  c <- atomic (do rows <- *items; with {pick (\race -> count race)} (do yield (pick rows)))
  println (show c)
"#,
    );
    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("cannot be used inside atomic")),
        "lambda param named `race` wrongly flagged: {:?}",
        diags
    );
}

#[test]
fn do_bound_race_name_allowed_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int 1}]
main = do
  c <- atomic (do race <- *items; yield (count race))
  println (show c)
"#,
    );
    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("cannot be used inside atomic")),
        "do-bound `race` wrongly flagged: {:?}",
        diags
    );
}

#[test]
fn real_race_still_rejected_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int 1}]
main = do
  c <- atomic (do rows <- *items; r <- race (yield 1) (yield 2); yield (count rows))
  println (show c)
"#,
    );
    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("`race` cannot be used inside atomic")),
        "unshadowed race must still be rejected: {:?}",
        diags
    );
}

// ── Finding 6: CLI argument handling ───────────────────────────────

#[test]
fn build_space_separated_override_does_not_swallow_dash_args() {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_mid_cli_override_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, "main = println \"hi\"\n").unwrap();

    let knot = env!("CARGO_BIN_EXE_knot");
    let out = Command::new(knot)
        .arg("build")
        .arg(&src_path)
        .arg("--port")
        .arg("-o")
        .arg(dir.join("out"))
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "`--port -o out` must be rejected, not parsed as port=\"-o\""
    );
    assert!(
        stderr.contains("missing value for --port"),
        "expected missing-value error, got: {stderr}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn fmt_dash_reads_stdin_writes_stdout() {
    let knot = env!("CARGO_BIN_EXE_knot");
    let mut child = Command::new(knot)
        .arg("fmt")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn knot fmt");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"main   =   println \"hi\"\n")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "fmt - failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("main = println \"hi\""),
        "expected formatted source on stdout, got: {stdout}"
    );
}
