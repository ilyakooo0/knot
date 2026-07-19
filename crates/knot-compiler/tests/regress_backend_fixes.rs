//! Regression tests for compiler-backend bugs:
//!
//! 1. `compile_do` leaking loop-body bind variables into the caller's env —
//!    a do-bind variable shadowing an outer variable used after the block
//!    crashed the Cranelift verifier ("uses value vN from non-dominating
//!    inst").
//! 2. Chained do-local withs in a pushed-down WHERE (`with {a: 5}` then
//!    `with {b: a + 1}` over `where m.x == b`) panicking "codegen:
//!    undefined variable 'a'".
//! 3. View writes bypassing refined-type validation (invalid rows persisted
//!    through a view of a refined source).
//! 4. Binding from a local relation variable in an IO do-block not
//!    iterating (only the first row was visible; examples/groupby.knot lost
//!    all groups after the first).
//! 5. Text comparisons pushed down under COLLATE KNOT_INT comparing
//!    numerically ("07" == "7" matched in SQL but not in Knot).
//! 6. `==` on payload-bearing ADT columns pushing a tag-only SQL param —
//!    matching rows were silently dropped.
//! 7. minOn/maxOn pushdown over Text columns returning Int (runtime
//!    re-parses TEXT results as Int).
//! 8. Recursive type aliases overflowing the stack during type resolution
//!    instead of producing a diagnostic.
//! 9. Field-level refinements dropped when a refined record alias is the
//!    source element type.
//!
//! Each test compiles a small Knot program with the real `knot` binary into
//! its own scratch directory (so `knot.db` lands there) and asserts on the
//! program's output / exit status.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn scratch_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_backend_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Compile `source` into a fresh scratch directory and return paths.
fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = scratch_dir(test_name);
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

/// Compile a program that is expected to FAIL to build; returns the
/// compiler's stderr.
fn compile_expect_error(test_name: &str, source: &str) -> String {
    let dir = scratch_dir(test_name);
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let knot = env!("CARGO_BIN_EXE_knot");
    let out = Command::new(knot)
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !out.status.success(),
        "expected knot build to fail for {test_name}, but it succeeded:\nstderr: {stderr}",
    );
    let _ = fs::remove_dir_all(&dir);
    stderr
}

// ── Finding 1: do-bind variable leaking into the caller's env ──────

#[test]
fn do_bind_shadowing_outer_var_compiles_and_runs() {
    // The groupBy comprehension's bind var `t` shadows the outer `t <- now`.
    // compile_do used to leave the loop-local SSA binding for `t` in the
    // caller's env, so `show t` after the block referenced a value from a
    // non-dominating block and the Cranelift verifier panicked.
    let (stdout, stderr, ok) = compile_and_run(
        "do_bind_shadow",
        r#"*todos : [{title: Text, owner: Text, done: Int 1}]

main = do
  replace *todos = [{title: "a", owner: "Alice", done: 0}]
  t <- now
  with {workload: do
    t <- *todos
    where t.done == 0
    groupBy {t.owner}
    yield {owner: t.owner, count: count t}} (do
    println (show t)
    yield {})
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // The outer `t` is a clock timestamp (a large integer), not a row. `now`
    // is typed `IO {clock} Int Ms`, so `show` appends its unit: "<digits> Ms".
    let first = stdout.lines().next().unwrap_or("");
    let digits = first.trim_matches('"').trim_end_matches(" Ms");
    assert!(
        !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()),
        "expected the outer `t` (timestamp) to be printed, got:\n{stdout}"
    );
}

// ── Finding 2: chained do-local lets in pushed-down WHERE ──────────

#[test]
fn chained_do_local_lets_in_pushed_where() {
    // `with {b: a + 1}` references the earlier do-local `with {a: 5}`; the SQL
    // plan stored `a + 1` as a param expression and compiled it in the
    // enclosing env where `a` is unbound — "codegen: undefined variable 'a'".
    let (stdout, stderr, ok) = compile_and_run(
        "chained_lets_where",
        r#"*items : [{x: Int 1}]

main = do
  replace *items = [{x: 5}, {x: 6}]
  with {a: 5} (
    with {b: a + 1} (
      with {r: do
        m <- *items
        where m.x == b
        yield m} (do
        println (show (count r)))))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"1\""),
        "expected exactly the x == 6 row to match (b = a + 1 = 6), got:\n{stdout}"
    );
}

// ── Finding 3: view writes bypassing refined-type validation ───────

const REFINED_VIEW_PROG_HEADER: &str = r#"type Nat = Int 1 where \x -> x >= 0

*accounts : [{owner: Text, balance: Nat}]

*aliceAccounts = do
  a <- *accounts
  where a.owner == "alice"
  yield {balance: a.balance}
"#;

#[test]
fn direct_write_of_refined_source_rejected() {
    let src = format!(
        "{REFINED_VIEW_PROG_HEADER}
main = do
  replace *accounts = [{{owner: \"bob\", balance: 0 - 5}}]
  rows <- *accounts
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("direct_refined_write", &src);
    assert!(
        !ok && stderr.contains("refinement violation"),
        "expected direct write of balance -5 to be rejected:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn view_write_of_refined_source_rejected() {
    // Writing through the view used to bypass the underlying source's
    // refinement predicates entirely and persist the invalid row.
    let src = format!(
        "{REFINED_VIEW_PROG_HEADER}
main = do
  *aliceAccounts = [{{balance: 0 - 5}}]
  rows <- *accounts
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("view_refined_write", &src);
    assert!(
        !ok && stderr.contains("refinement violation"),
        "expected view write of balance -5 to be rejected:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn view_write_of_valid_rows_accepted() {
    let src = format!(
        "{REFINED_VIEW_PROG_HEADER}
main = do
  *aliceAccounts = [{{balance: 7}}]
  rows <- *accounts
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("view_valid_write", &src);
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"1\""),
        "expected the valid row to persist through the view, got:\n{stdout}"
    );
}

// ── Finding 4: bind from a local relation var in an IO do-block ────

#[test]
fn io_bind_from_let_bound_groupby_iterates_all_rows() {
    // `w <- workload` where `workload` is a with-bound groupBy comprehension
    // used to bind only once (the whole relation / first row) — the second
    // group ("Bob") never printed.
    let (stdout, stderr, ok) = compile_and_run(
        "io_bind_relation_var",
        r#"*todos : [{title: Text, owner: Text, done: Int 1}]

main = do
  replace *todos = [
    {title: "a", owner: "Alice", done: 0},
    {title: "b", owner: "Alice", done: 0},
    {title: "c", owner: "Bob", done: 0},
    {title: "d", owner: "Alice", done: 1}
  ]
  with {workload: do
    t <- *todos
    where t.done == 0
    groupBy {t.owner}
    yield {owner: t.owner, count: count t}} (do
    w <- workload
    println (w.owner ++ ": " ++ show w.count)
    yield {})
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("Alice: 2") && stdout.contains("Bob: 1"),
        "expected BOTH groups to print, got:\n{stdout}"
    );
}

#[test]
fn io_bind_from_let_bound_filter_comprehension_iterates() {
    // Same shape without groupBy: a plain with-bound comprehension.
    let (stdout, stderr, ok) = compile_and_run(
        "io_bind_relation_var_plain",
        r#"*nums : [{n: Int 1}]

main = do
  replace *nums = [{n: 1}, {n: 2}, {n: 3}]
  with {bigs: do
    x <- *nums
    where x.n > 1
    yield x} (do
    b <- bigs
    println (show b.n)
    yield {})
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"2\"") && stdout.contains("\"3\""),
        "expected both matching rows to be visited, got:\n{stdout}"
    );
}

// ── Finding 5: Text comparisons under COLLATE KNOT_INT ─────────────

#[test]
fn text_concat_equality_is_not_numeric() {
    // "0" ++ "7" == "7" compared numerically in SQL (CAST ... COLLATE
    // KNOT_INT made "07" == "7" match); Knot's in-memory semantics say no.
    let (stdout, stderr, ok) = compile_and_run(
        "text_concat_eq",
        r#"*items : [{a: Text, b: Text}]

main = do
  replace *items = [{a: "0", b: "7"}]
  with {r: do
    i <- *items
    where i.a ++ i.b == "7"
    yield i} (do
    println (show (count r)))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"0\""),
        "expected no match (\"07\" != \"7\" as Text), got:\n{stdout}"
    );
}

#[test]
fn text_concat_ordering_is_bytewise() {
    // "07" < "1" byte-wise (Knot semantics) but 7 < 1 is false numerically.
    let (stdout, stderr, ok) = compile_and_run(
        "text_concat_ord",
        r#"*items : [{a: Text, b: Text}]

main = do
  replace *items = [{a: "0", b: "7"}]
  with {r: do
    i <- *items
    where i.a ++ i.b < "1"
    yield i} (do
    println (show (count r)))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"1\""),
        "expected \"07\" < \"1\" byte-wise to match, got:\n{stdout}"
    );
}

// ── Finding 6: equality on payload-bearing ADT columns ─────────────

#[test]
fn adt_payload_column_equality_finds_matching_row() {
    // `u.st == Active {}` used to push `st = ?` with a bare-tag param while
    // the column stores JSON — the matching row was silently dropped.
    let (stdout, stderr, ok) = compile_and_run(
        "adt_payload_eq",
        r#"data Status = Active {} | Banned {reason: Text}

impl Eq Status where
  eq = \a b -> show a == show b

*users : [{name: Text, st: Status}]

main = do
  replace *users = [
    {name: "a", st: Active {}},
    {name: "b", st: Banned {reason: "spam"}}
  ]
  with {r: do
    u <- *users
    where u.st == Active {}
    yield u} (do
    println (show (count r)))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"1\""),
        "expected the Active user to match, got:\n{stdout}"
    );
}

#[test]
fn nullary_adt_tag_column_equality_still_works() {
    // All-nullary ("tag") ADT columns remain pushable and must keep working.
    let (stdout, stderr, ok) = compile_and_run(
        "adt_tag_eq",
        r#"data Color = Red {} | Blue {}

impl Eq Color where
  eq = \a b -> show a == show b

*marbles : [{n: Int 1, c: Color}]

main = do
  replace *marbles = [{n: 1, c: Red {}}, {n: 2, c: Blue {}}, {n: 3, c: Red {}}]
  with {r: do
    m <- *marbles
    where m.c == Red {}
    yield m} (do
    println (show (count r)))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"2\""),
        "expected the two Red marbles to match, got:\n{stdout}"
    );
}

// ── Finding 7: minOn/maxOn pushdown over Text columns ──────────────

#[test]
fn maxon_over_text_column_returns_text() {
    // The SQL MIN/MAX runtime re-parses TEXT results as Int (Knot Ints are
    // stored as TEXT), so maxOn over ["007", "01"] came back as Int 1.
    let (stdout, stderr, ok) = compile_and_run(
        "maxon_text",
        r#"*codes : [{c: Text}]

main = do
  replace *codes = [{c: "007"}, {c: "01"}]
  with {m: maxOn (\x -> x.c) *codes}
    (println (show m))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"01\""),
        "expected Text maximum \"01\", got:\n{stdout}"
    );
}

#[test]
fn minon_over_text_column_returns_text() {
    let (stdout, stderr, ok) = compile_and_run(
        "minon_text",
        r#"*codes : [{c: Text}]

main = do
  replace *codes = [{c: "007"}, {c: "01"}]
  with {m: minOn (\x -> x.c) *codes}
    (println (show m))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"007\""),
        "expected Text minimum \"007\", got:\n{stdout}"
    );
}

#[test]
fn maxon_over_int_column_still_pushes_and_is_numeric() {
    // Int columns keep numeric MIN/MAX semantics (9 > 10 byte-wise!).
    let (stdout, stderr, ok) = compile_and_run(
        "maxon_int",
        r#"*scores : [{s: Int 1}]

main = do
  replace *scores = [{s: 9}, {s: 10}]
  with {m: maxOn (\x -> x.s) *scores}
    (println (show m))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"10\""),
        "expected numeric maximum 10, got:\n{stdout}"
    );
}

// ── Finding 8: recursive type alias → diagnostic, not crash ────────

#[test]
fn recursive_type_alias_through_record_is_diagnosed() {
    // `type A = {x: A}` used to double the resolved type's depth on every
    // fixpoint pass and abort with a stack overflow before any diagnostic.
    let stderr = compile_expect_error(
        "recursive_alias",
        r#"type A = {x: A}

main = do
  println "hi"
"#,
    );
    assert!(
        stderr.contains("recursive type alias 'A'"),
        "expected a recursive-alias diagnostic, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("stack overflow"),
        "compiler must not crash:\n{stderr}"
    );
}

#[test]
fn mutually_recursive_type_aliases_are_diagnosed() {
    let stderr = compile_expect_error(
        "mutual_recursive_alias",
        r#"type A = {x: B}
type B = {y: A}

main = do
  println "hi"
"#,
    );
    assert!(
        stderr.contains("recursive type alias"),
        "expected a recursive-alias diagnostic, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("stack overflow"),
        "compiler must not crash:\n{stderr}"
    );
}

// ── Finding 9: refined record alias as source element type ─────────

const REFINED_ALIAS_HEADER: &str =
    r#"type P = {age: Int 1 where \x -> x >= 0} where \p -> p.age <= 200

*people : [P]
"#;

#[test]
fn refined_alias_field_refinement_enforced() {
    // Field-level refinements inside the alias base were dropped — only the
    // whole-element predicate ran, so {age: -5} persisted.
    let src = format!(
        "{REFINED_ALIAS_HEADER}
main = do
  replace *people = [{{age: 0 - 5}}]
  rows <- *people
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("refined_alias_field", &src);
    assert!(
        !ok && stderr.contains("refinement violation"),
        "expected age -5 to violate the field refinement:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn refined_alias_whole_element_refinement_still_enforced() {
    let src = format!(
        "{REFINED_ALIAS_HEADER}
main = do
  replace *people = [{{age: 300}}]
  rows <- *people
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("refined_alias_elem", &src);
    assert!(
        !ok && stderr.contains("refinement violation"),
        "expected age 300 to violate the whole-element refinement:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
fn refined_alias_valid_row_accepted() {
    let src = format!(
        "{REFINED_ALIAS_HEADER}
main = do
  replace *people = [{{age: 42}}]
  rows <- *people
  println (show (count rows))
"
    );
    let (stdout, stderr, ok) = compile_and_run("refined_alias_ok", &src);
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"1\""),
        "expected the valid row to persist, got:\n{stdout}"
    );
}

// ── groupBy phase-1 use-after-free with a non-equi-join multi-bind ──

#[test]
fn groupby_non_equi_join_multibind_survives_arena_reset() {
    // A multi-bind do-block whose primary bind (`c <- *children`) is
    // materialized inside the enclosing `p` loop, joined by a NON-equi
    // condition (`c.bound < p.pid`) that `match_equi_join` does not match —
    // so no hash-join hoist and `c` really is a nested loop.
    //
    // Phase 1 of groupBy pushes each `c` row into a temp relation, then
    // closes the pre-group loops, whose continue blocks run
    // `knot_arena_reset_to` per iteration. Before the fix the temp push did
    // NOT `knot_arena_promote` the row (unlike the yield path), so the reset
    // freed each row out from under the temp relation and
    // `knot_relation_group_by` read dangling memory — garbage keys/counts or
    // a crash. Promoting the row before the push pins it past the reset.
    let (stdout, stderr, ok) = compile_and_run(
        "groupby_non_equi_multibind",
        r#"*parents : [{pid: Int 1}]
*children : [{owner: Text, bound: Int 1}]

main = do
  replace *parents = [{pid: 100}]
  replace *children = [
    {owner: "alice", bound: 3},
    {owner: "alice", bound: 5},
    {owner: "bob", bound: 7}
  ]
  with {grouped: do
    p <- *parents
    c <- *children
    where c.bound < p.pid
    groupBy {c.owner}
    yield {owner: c.owner, n: count c}} (do
    g <- grouped
    println (g.owner ++ ":" ++ show g.n)
    yield {})
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // All three children pass `bound < 100`; grouped by owner that is
    // alice → 2, bob → 1. A use-after-free corrupts the keys or counts.
    assert!(
        stdout.contains("alice:2"),
        "expected alice's two rows to group correctly, got:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("bob:1"),
        "expected bob's single row to group correctly, got:\nstdout: {stdout}\nstderr: {stderr}"
    );
}
