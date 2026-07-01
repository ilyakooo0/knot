//! Regression tests for crash / correctness fixes (sixth batch):
//!
//! 1. `parse_type`'s `forall` branch recursed into `parse_type` with no
//!    `enter_recursion()` depth guard, so a deeply nested `forall ...` type
//!    (reachable anywhere a type appears, including `(expr : Type)`) overflowed
//!    the stack and crashed the compiler with SIGSEGV instead of reporting a
//!    clean "nesting depth limit exceeded" diagnostic.
//! 2. `analyze_view` hit a hard `panic!` when a view body contained a `where`
//!    filter that isn't `<bindvar>.<field> == <constant>` (e.g. an inequality),
//!    aborting the whole compile on otherwise-valid, type-checkable input. It
//!    now returns a diagnostic.
//! 3. Join correctness: a comprehension equi-joining one relation to two others
//!    must apply BOTH join predicates (previously the hash-join planner could
//!    drop one). This end-to-end test asserts only rows satisfying both
//!    predicates survive.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct Scratch {
    dir: PathBuf,
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn scratch(name: &str) -> Scratch {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_crash6_{}_{}",
        name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    Scratch { dir }
}

fn run_build(dir: &PathBuf, source: &str) -> std::process::Output {
    let src = dir.join("prog.knot");
    fs::write(&src, source).unwrap();
    let knot = env!("CARGO_BIN_EXE_knot");
    Command::new(knot)
        .arg("build")
        .arg(&src)
        .current_dir(dir)
        .output()
        .expect("failed to spawn knot compiler")
}

/// A build that must fail *gracefully* — with a diagnostic, not a Rust panic
/// and not a signal (SIGSEGV from stack overflow / SIGABRT). Returns stderr.
fn build_graceful_error(name: &str, source: &str) -> String {
    let s = scratch(name);
    let out = run_build(&s.dir, source);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !out.status.success(),
        "expected build to fail, but it succeeded for {name}"
    );
    // `code()` is None when the process was killed by a signal (e.g. a stack
    // overflow SIGSEGV/SIGABRT). A graceful diagnostic exits with a code.
    assert!(
        out.status.code().is_some(),
        "compiler crashed via signal (stack overflow?) for {name}; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "compiler panicked instead of reporting a diagnostic for {name}; stderr:\n{stderr}"
    );
    stderr
}

#[test]
fn deeply_nested_forall_reports_diagnostic_not_stack_overflow() {
    // Thousands of nested `forall`s used to blow the parser's stack.
    let ty = "forall a. ".repeat(6000);
    let src = format!("x : {ty}Int\nmain = 0\n");
    let stderr = build_graceful_error("forall_depth", &src);
    assert!(
        stderr.contains("nesting depth limit"),
        "expected a nesting-depth diagnostic; stderr:\n{stderr}"
    );
}

#[test]
fn view_with_non_equality_where_reports_diagnostic_not_panic() {
    let src = r#"
*people = [{name: "a", age: 30}]
*adults = do
  p <- *people
  where p.age > 18
  yield {name: p.name}
main = 0
"#;
    let stderr = build_graceful_error("view_non_eq_where", src);
    assert!(
        stderr.contains("unsupported `where` filter in view body"),
        "expected the unsupported-view-filter diagnostic; stderr:\n{stderr}"
    );
}

#[test]
fn join_applies_all_predicates() {
    // `z` equi-joins to both `x` and `y`; only the row satisfying BOTH
    // `z.ka == x.k` AND `z.kb == y.k` (v = "GOOD") should survive. The row with
    // `ka = 99` (no matching `x.k`) must be filtered out.
    let src = r#"
*a : [{k: Int}]
*b : [{k: Int}]
*c : [{ka: Int, kb: Int, v: Text}]

main = do
  replace *a = [{k: 1}, {k: 2}]
  replace *b = [{k: 10}, {k: 20}]
  replace *c = [{ka: 1, kb: 10, v: "GOOD"}, {ka: 99, kb: 20, v: "BAD"}]
  as <- *a
  bs <- *b
  cs <- *c
  let joined = do
    x <- as
    y <- bs
    z <- cs
    where z.ka == x.k
    where z.kb == y.k
    yield z.v
  r <- joined
  println (show r)
"#;
    let s = scratch("join_predicates");
    let out = run_build(&s.dir, src);
    assert!(
        out.status.success(),
        "build failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let run = Command::new(s.dir.join("prog"))
        .current_dir(&s.dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        stdout.contains("GOOD"),
        "expected the doubly-matching row; stdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("BAD"),
        "a join predicate was dropped — the non-matching row leaked; stdout:\n{stdout}"
    );
}
