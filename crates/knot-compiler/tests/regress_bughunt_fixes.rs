//! Regression tests for the bug-hunt fixes:
//!
//! 1. SQL `WHERE` conditions were AND-joined without parenthesizing each
//!    independent boolean stage, so a top-level `||` in one condition bound
//!    across the `AND` (SQLite: AND binds tighter than OR), matching wrong
//!    rows. `join_sql_conditions` now wraps each fragment.
//!
//! 2. Beta-reduction of a multi-parameter lambda applied to one argument
//!    wrapped the remaining parameters as binders *around* the substituted
//!    body without capture-avoidance, so a free variable of the argument that
//!    collided with a later parameter name (`(\l p -> p.age > l) p.limit`)
//!    was captured — turning a captured outer value into the row's own column
//!    in pushed-down SQL. It now bails out of the reduction (pushdown falls
//!    back to correct in-memory evaluation).
//!
//! 3. A non-nullary IO builtin aliased to a name (`readIt = readFile` or a
//!    local `let f = readFile`) and applied inside `atomic` laundered its
//!    effect past the atomic gate: the alias recorded empty effects. The
//!    alias now carries the builtin's call effects.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

// ── compile-and-run harness (bugs 1 and 2) ────────────────────────

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_bh_{}_{}",
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

/// First non-empty output line (a compiled `main` echoes its trailing `{}`
/// unit result on its own line after the program's own prints).
fn first_line(stdout: &str) -> &str {
    stdout.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("")
}

// ── Bug 1: OR inside an AND-joined WHERE must stay parenthesized ────

#[test]
fn or_in_do_block_where_does_not_escape_and_scope() {
    // Two `where` stages, one containing a top-level `||`. Only {a:2,b:3}
    // satisfies `(a==1 || a==2) && b==3`. The buggy `a==1 OR a==2 AND b==3`
    // (= `a==1 OR (a==2 AND b==3)`) also matched {a:1,b:999}, yielding 2.
    let (stdout, stderr, ok) = compile_and_run(
        "or_do_where",
        r#"type Item = {a: Int, b: Int}
*items : [Item]

main = do
  replace *items = [{a: 1, b: 999}, {a: 2, b: 3}, {a: 5, b: 3}]
  let rows = do
    t <- *items
    where t.a == 1 || t.a == 2
    where t.b == 3
    yield t
  println (count rows)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert_eq!(
        first_line(&stdout),
        "1",
        "OR must not escape the AND scope; expected exactly 1 matching row, got: {stdout:?}"
    );
}

#[test]
fn or_in_pipe_filters_does_not_escape_and_scope() {
    // Same invariant through the pipe/aggregate pushdown path.
    let (stdout, stderr, ok) = compile_and_run(
        "or_pipe_filter",
        r#"type Item = {a: Int, b: Int}
*items : [Item]

main = do
  replace *items = [{a: 1, b: 999}, {a: 2, b: 3}, {a: 5, b: 3}]
  let n = *items |> filter (\t -> t.a == 1 || t.a == 2) |> filter (\t -> t.b == 3) |> count
  println n
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert_eq!(
        first_line(&stdout),
        "1",
        "OR in a pipe filter must not escape the AND scope, got: {stdout:?}"
    );
}

// ── Bug 2: multi-param beta-reduction must not capture ─────────────

#[test]
fn partial_application_arg_var_is_not_captured_by_later_param() {
    // `cmp`'s second parameter is `p`, and the argument `p.threshold`
    // references the *outer* `p`. A capturing beta-reduction would rewrite
    // the predicate to `\p -> p.age > p.threshold` (the row's own threshold),
    // pushing down `age > threshold` — matching 0 rows here. The captured
    // outer value is 25, so the correct predicate is `age > 25`, matching 2.
    let (stdout, stderr, ok) = compile_and_run(
        "beta_capture",
        r#"type Rec = {age: Int, threshold: Int}
*people : [Rec]

cmp = \threshold p -> p.age > threshold

main = do
  replace *people = [{age: 10, threshold: 100}, {age: 50, threshold: 100}, {age: 40, threshold: 100}]
  let p = {age: 0, threshold: 25}
  let matched = filter (cmp p.threshold) *people
  println (count matched)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert_eq!(
        first_line(&stdout),
        "2",
        "captured outer value (25) must not be replaced by the row column, got: {stdout:?}"
    );
}

// ── Bug 3: aliased IO builtin must not launder past the atomic gate ─

use knot::diagnostic::{Diagnostic, Severity};

fn effect_diags(src: &str) -> Vec<Diagnostic> {
    let (tokens, lex_diags) = knot::lexer::Lexer::new(src).tokenize();
    assert!(
        !lex_diags.iter().any(|d| d.severity == Severity::Error),
        "lex errors: {lex_diags:?}"
    );
    let (mut module, parse_diags) =
        knot::parser::Parser::new(src.to_string(), tokens).parse_module();
    assert!(
        !parse_diags.iter().any(|d| d.severity == Severity::Error),
        "parse errors: {parse_diags:?}"
    );
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    knot_compiler::effects::check(&module)
}

fn assert_has_error(diags: &[Diagnostic], needle: &str) {
    assert!(
        diags
            .iter()
            .any(|d| d.severity == Severity::Error && d.message.contains(needle)),
        "expected an error containing {needle:?}, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn top_level_io_builtin_alias_is_caught_inside_atomic() {
    let diags = effect_diags(
        r#"*counter : [{n: Int}]

readIt = readFile

proc = atomic do
  c <- *counter
  contents <- readIt "secret.txt"
  yield contents
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn local_let_io_builtin_alias_is_caught_inside_atomic() {
    let diags = effect_diags(
        r#"*counter : [{n: Int}]

proc = atomic do
  c <- *counter
  let f = readFile
  contents <- f "secret.txt"
  yield contents
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}
