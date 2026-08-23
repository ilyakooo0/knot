//! Error handling: Result/Maybe do-block short-circuit, structured logging
//! (base.error is a log, not an abort), base.todo, and base.trace.

mod e2e;
mod harness;
use harness::assert_show;

#[test]
fn result_do_block_all_ok() {
    assert_show(
        "with {
Int 1 -> Result Text (Int 1)  f  (\\x -> Result.Ok {value (x + 1)}) }
         (match (do
            a <- f 1
            b <- f a
            yield b)
            Result.Ok {value v}  v
            Result.Err {error _}  (0 - 1))",
        "3",
    );
}

#[test]
fn result_do_block_short_circuits_on_err() {
    // The Err from the first bind short-circuits; the second bind never runs.
    assert_show(
        "with {
Int 1 -> Result Text (Int 1)  f  (\\x -> match (x == 0)
                          Bool.True {}  Result.Err {error \"stop\"}
                          Bool.False {}  Result.Ok {value x}) }
         (match (do
            a <- f 0
            b <- f a
            yield b)
            Result.Ok {value v}  v
            Result.Err {error _}  99)",
        "99",
    );
}

#[test]
fn maybe_bind_short_circuits_on_nothing() {
    assert_show(
        "with {
Int 1 -> Maybe (Int 1)  f  (\\x -> match (x == 0)
                          Bool.True {}  Maybe.Nothing {}
                          Bool.False {}  Maybe.Just {value x}) }
         (match (do
            a <- f 0
            b <- f a
            yield b)
            Maybe.Just {value v}  v
            Maybe.Nothing {}  7)",
        "7",
    );
}

#[test]
fn error_is_structured_log_not_abort() {
    // base.error logs a structured JSON line (to stderr) and CONTINUES — it is
    // not a process abort. (The aborting primitive is base.todo / a panic.)
    let dir = e2e::TempDir::fresh("err_log");
    e2e::build_in_dir(
        "err_log",
        r#"(do
  base.println "before"
  base.error "boom"
  base.println "after"
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("err_log"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    assert!(out.status.success(), "base.error must not abort");
    let stdout = String::from_utf8_lossy(&out.stdout);
    // error goes to stderr (a structured log), so stdout is just the printlns
    assert_eq!(stdout, "\"before\"\n\"after\"\n{}\n", "stdout: {stdout}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("\"level\":\"error\"") && stderr.contains("\"msg\":\"boom\""),
        "expected structured error log on stderr, got: {stderr}"
    );
}

#[test]
fn todo_aborts_process() {
    // base.todo is an unimplemented hole that aborts when reached.
    let dir = e2e::TempDir::fresh("todo_abort");
    e2e::build_in_dir(
        "todo_abort",
        r#"(do
  base.println "start"
  base.println base.todo
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("todo_abort"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    assert!(!out.status.success(), "base.todo should abort");
}

#[test]
fn trace_returns_value_unchanged() {
    // trace prints to stderr but returns its argument unchanged.
    assert_show("base.trace 42", "42");
    assert_show("base.trace \"x\"", "x");
}