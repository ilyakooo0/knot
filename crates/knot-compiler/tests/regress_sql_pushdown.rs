//! Regression tests for SQL-pushdown semantics bugs: every pushed-down
//! query/aggregate/pipe-chain must produce exactly the same observable
//! behavior as the general in-memory path (including panics), and the
//! `atomic`/closure-capture/refinement codegen must stay sound around them.
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

/// Compile `source` into a fresh scratch directory and return paths.
fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_{}_{}",
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

// ── Finding 1: pipe-chain operation order ─────────────────────────

#[test]
fn pipe_take_then_drop_respects_order() {
    let (stdout, stderr, ok) = compile_and_run(
        "pipe_order",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = [
    {n: 1}, {n: 2}, {n: 3}, {n: 4}, {n: 5},
    {n: 6}, {n: 7}, {n: 8}, {n: 9}, {n: 10}
  ]
  all <- *items
  let bad = all |> take 5 |> drop 2
  println ("take_drop_count: " ++ show (count bad))
  println ("take_drop: " ++ show bad)
  let good = all |> drop 2 |> take 3
  println ("drop_take: " ++ show good)
  let sorted_after_take = all |> take 3 |> sortBy (\x -> x.n)
  println ("sat_count: " ++ show (count sorted_after_take))
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // take 5 then drop 2 over [1..10] = [3,4,5] — NOT [3..7].
    assert!(stdout.contains("take_drop_count: 3"), "got: {stdout}");
    assert!(stdout.contains("take_drop: [{n: 3}, {n: 4}, {n: 5}]"), "got: {stdout}");
    // drop 2 |> take 3 maps to LIMIT 3 OFFSET 2 and stays correct.
    assert!(stdout.contains("drop_take: [{n: 3}, {n: 4}, {n: 5}]"), "got: {stdout}");
    // sortBy after take must sort only the taken prefix (3 rows).
    assert!(stdout.contains("sat_count: 3"), "got: {stdout}");
}

#[test]
fn pipe_count_after_take_respects_limit() {
    let (stdout, stderr, ok) = compile_and_run(
        "pipe_count_take",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = [{n: 1}, {n: 2}, {n: 3}, {n: 4}, {n: 5}]
  all <- *items
  let n = all |> take 2 |> count
  println ("n: " ++ show n)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("n: 2"), "count after take must see only 2 rows, got: {stdout}");
}

// ── Finding 2: aggregates/filter over a do-block's yield projection ──

#[test]
fn aggregates_resolve_through_yield_projection() {
    let (stdout, stderr, ok) = compile_and_run(
        "projection",
        r#"type Item = {amt: Int, qty: Int}
*items : [Item]

main = do
  replace *items = [{amt: 100, qty: 1}, {amt: 200, qty: 2}]
  items <- *items
  let s = sum (\x -> x.amt) (do
    i <- items
    yield {amt: i.qty})
  println ("sum: " ++ show s)
  let f = count (filter (\x -> x.amt > 1) (do
    i <- items
    yield {amt: i.qty}))
  println ("filtered: " ++ show f)
  let cw = countWhere (\x -> x.amt > 1) (do
    i <- items
    yield {amt: i.qty})
  println ("countWhere: " ++ show cw)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The yield renames qty → amt: aggregates must apply to qty values (1, 2).
    assert!(stdout.contains("sum: 3"), "sum must see projected column, got: {stdout}");
    assert!(stdout.contains("filtered: 1"), "filter must see projected column, got: {stdout}");
    assert!(stdout.contains("countWhere: 1"), "countWhere must see projected column, got: {stdout}");
}

// ── Finding 3: atomic SAVEPOINT with writes through a parameter ──

#[test]
fn atomic_rolls_back_writes_made_through_function_parameter() {
    let (stdout, stderr, ok) = compile_and_run(
        "atomic_param",
        r#"type Entry = {id: Int}
*log : [Entry]

runAtomic = \act -> atomic (do
  entries <- *log
  act entries
  where false
  yield 0)

main = do
  replace *log = []
  runAtomic (\entries -> replace *log = [{id: 99}])
  rows <- *log
  println ("after: " ++ show (count rows))
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The failed `where` guard must roll the write back even though the
    // write happened through the `act` parameter.
    assert!(stdout.contains("after: 0"), "write must be rolled back, got: {stdout}");
}

// ── Finding 4: refinement validation on the union-append fast path ──

#[test]
fn union_append_validates_refinements() {
    let (_stdout, stderr, ok) = compile_and_run(
        "refine_append",
        r#"type Pos = Int where \x -> x > 0
type Row = {v: Pos}
*rows : [Row]

main = do
  replace *rows = [{v: 1}]
  rows <- *rows
  *rows = union rows [{v: 0 - 5}]
  println "should not be reached"
  yield {}
"#,
    );
    assert!(!ok, "appending a refinement-violating row must abort");
    assert!(
        stderr.contains("refinement violation"),
        "expected refinement violation panic, got: {stderr}"
    );
}

// ── Finding 5: STM retry wakes on pushed-down reads ──

#[test]
fn stm_retry_wakes_on_pushed_down_count() {
    use std::time::Instant;
    let c = compile(
        "stm_wake",
        r#"type Flag = {id: Int, v: Int}
*flags : [Flag]

writer = do
  sleep 200
  rows <- *flags
  *flags = do
    f <- rows
    yield (if f.id == 1 then {f | v: 1} else f)

waiter = atomic (do
  rows <- *flags
  let n = countWhere (\g -> g.v == 1) (do
    f <- rows
    yield f)
  if n == 0 then retry else n)

main = do
  replace *flags = [{id: 1, v: 0}]
  fork writer
  n <- waiter
  println ("woken: " ++ show n)
  yield {}
"#,
    );
    let start = Instant::now();
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("run failed");
    let elapsed = start.elapsed();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("woken: 1"), "got: {stdout}");
    // Writer fires at ~200ms; the retry must be woken promptly, not hang.
    assert!(
        elapsed.as_secs() < 10,
        "retry wakeup took too long: {elapsed:?}"
    );
}

// ── Finding 6: closure capture of locals shadowing top-levels ──

#[test]
fn lambda_captures_local_shadowing_top_level() {
    let (stdout, stderr, ok) = compile_and_run(
        "shadow_capture",
        r#"type Row = {n: Int}
*xs : [Row]

offset = 100

main = do
  replace *xs = [{n: 1}, {n: 2}]
  rows <- *xs
  let offset = 5
  let ys = map (\x -> x.n + offset) rows
  println ("ys: " ++ show ys)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The do-local `offset = 5` shadows the top-level `offset = 100`.
    assert!(stdout.contains("ys: [6, 7]"), "lambda must capture the local binding, got: {stdout}");
}

// ── Findings 7, 8, 10: float arithmetic / float % / toUpper in WHERE ──

#[test]
fn float_arithmetic_and_unicode_in_where_match_in_memory() {
    let (stdout, stderr, ok) = compile_and_run(
        "float_where",
        r#"type M = {v: Float}
*m : [M]

type P = {name: Text}
*p : [P]

main = do
  replace *m = [{v: 5.5}, {v: 1.0}]
  replace *p = [{name: "ärger"}]
  rows <- *m
  let a = countWhere (\r -> r.v * 2.0 > 3.0) rows
  println ("arith: " ++ show a)
  let b = countWhere (\r -> r.v % 2.0 > 1.2) rows
  println ("fmod: " ++ show b)
  ppl <- *p
  let c = countWhere (\q -> toUpper q.name == "ÄRGER") ppl
  println ("upper: " ++ show c)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // 5.5 * 2.0 = 11.0 > 3.0 (true), 1.0 * 2.0 = 2.0 > 3.0 (false) → 1.
    // (The KNOT_INT text-cast compared "11.0" > "3.0" byte-wise → 0.)
    assert!(stdout.contains("arith: 1"), "float arithmetic WHERE, got: {stdout}");
    // fmod: 5.5 % 2.0 = 1.5 > 1.2 (true), 1.0 % 2.0 = 1.0 (false) → 1.
    // (SQLite % truncates to INTEGER: 5.5 % 2.0 = 1.0 → 0.)
    assert!(stdout.contains("fmod: 1"), "float %% WHERE, got: {stdout}");
    // Unicode case mapping: SQLite UPPER('ärger') = 'äRGER' → 0.
    assert!(stdout.contains("upper: 1"), "toUpper must be Unicode-aware, got: {stdout}");
}

// ── Finding 9: division by zero must panic, not silently drop rows ──

#[test]
fn division_by_zero_in_where_panics_like_in_memory() {
    let (_stdout, stderr, ok) = compile_and_run(
        "div_zero",
        r#"type M = {v: Float}
*m : [M]

main = do
  replace *m = [{v: 0.0}, {v: 4.0}]
  rows <- *m
  let n = countWhere (\r -> 10.0 / r.v > 1.0) rows
  println ("n: " ++ show n)
  yield {}
"#,
    );
    // SQL NULL semantics would silently return 1; the in-memory semantics
    // (division by zero panics) must be preserved.
    assert!(!ok, "division by zero must abort the program");
    assert!(
        stderr.contains("division by zero"),
        "expected division-by-zero panic, got: {stderr}"
    );
}

#[test]
fn division_by_literal_still_pushes_down_correctly() {
    let (stdout, stderr, ok) = compile_and_run(
        "div_literal",
        r#"type M = {v: Float}
*m : [M]

main = do
  replace *m = [{v: 8.0}, {v: 1.0}]
  rows <- *m
  let n = countWhere (\r -> r.v / 2.0 > 1.5) rows
  println ("n: " ++ show n)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // 8.0/2.0 = 4.0 > 1.5 (true), 1.0/2.0 = 0.5 (false) → 1.
    assert!(stdout.contains("n: 1"), "got: {stdout}");
}

// ── Finding 11: case expression on the value side of a comparison ──

#[test]
fn case_referencing_bind_var_compiles_and_evaluates() {
    let (stdout, stderr, ok) = compile_and_run(
        "case_value_side",
        r#"type R = {x: Int, big: Bool}
*rs : [R]

main = do
  replace *rs = [{x: 2, big: true}, {x: 1, big: false}, {x: 5, big: true}]
  rows <- *rs
  let n = countWhere (\r -> r.x == (case r.big of
    True -> 2
    _ -> 1)) rows
  println ("n: " ++ show n)
  yield {}
"#,
    );
    // Previously an ICE: "codegen: undefined variable 'r'" — the case
    // expression referencing the bind var was hoisted out of row scope.
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("n: 2"), "got: {stdout}");
}

// ── Int pushdown must keep working (no over-fallback regression) ──

#[test]
fn int_arithmetic_where_still_correct() {
    let (stdout, stderr, ok) = compile_and_run(
        "int_arith",
        r#"type M = {a: Int, b: Int}
*m : [M]

main = do
  replace *m = [{a: 5, b: 2}, {a: 1, b: 9}]
  rows <- *m
  let n = countWhere (\r -> r.a * r.b > 8) rows
  println ("n: " ++ show n)
  let q = countWhere (\r -> r.a % 2 == 1) rows
  println ("q: " ++ show q)
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // 5*2=10 > 8 (true), 1*9=9 > 8 (true) → 2.
    assert!(stdout.contains("n: 2"), "got: {stdout}");
    // 5 % 2 == 1 (true), 1 % 2 == 1 (true) → 2.
    assert!(stdout.contains("q: 2"), "got: {stdout}");
}

// ── Stale source_var_binds invalidation ───────────────────────────
// A variable bound from `xs <- *source` must not be SQL-pushed-down to a
// fresh table query once (a) the table has been written, or (b) the
// variable has been rebound to something else.

#[test]
fn count_of_bound_var_ignores_later_write() {
    let (stdout, stderr, ok) = compile_and_run(
        "stale_bind_write",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = []
  xs <- *items
  *items = union xs [{n: 5}]
  let c = count xs
  println (show c)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // xs was bound before the write — count xs must be 0, not a fresh
    // COUNT(*) over the post-write table (which would be 1).
    assert!(stdout.contains("\"0\""), "got: {stdout}");
}

#[test]
fn count_of_rebound_var_uses_rebound_value() {
    let (stdout, stderr, ok) = compile_and_run(
        "stale_bind_rebind",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = [{n: 1}, {n: 2}, {n: 3}]
  xs <- *items
  let xs = filter (\x -> x.n > 2) xs
  let c = count xs
  println (show c)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The let rebinds xs to the filtered relation — count must be 1,
    // not COUNT(*) over the whole table (3).
    assert!(stdout.contains("\"1\""), "got: {stdout}");
}

#[test]
fn write_inside_atomic_invalidates_outer_binding() {
    let (stdout, stderr, ok) = compile_and_run(
        "stale_bind_atomic",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = []
  xs <- *items
  atomic do
    cur <- *items
    *items = union cur [{n: 9}]
  let c = count xs
  println (show c)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The write happened in a nested (atomic) scope — the outer xs
    // binding is still pre-write, so count xs must be 0.
    assert!(stdout.contains("\"0\""), "got: {stdout}");
}

#[test]
fn write_via_user_function_invalidates_binding() {
    let (stdout, stderr, ok) = compile_and_run(
        "stale_bind_write_fn",
        r#"type Item = {n: Int}
*items : [Item]

addItem = \x -> replace *items = [{n: x}]

main = do
  replace *items = []
  xs <- *items
  _ <- addItem 7
  let c = count xs
  println (show c)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // addItem writes *items behind a function call — xs is stale, so
    // count xs must be 0, not 1.
    assert!(stdout.contains("\"0\""), "got: {stdout}");
}

// ── Beta-reduction must not skip substitution inside case bodies ──

#[test]
fn lambda_param_inside_case_under_set_matcher() {
    let (stdout, stderr, ok) = compile_and_run(
        "subst_case_param",
        r#"type Item = {n: Int}
*items : [Item]

main = do
  replace *items = []
  xs <- *items
  let addRows = \flag -> union xs (case flag of
    true -> [{n: 1}]
    _ -> [{n: 2}])
  *items = addRows true
  ys <- *items
  println (show ys)
"#,
    );
    // Previously an ICE: substitute_inner returned `case flag of ...`
    // UNCHANGED during beta-reduction, so match_union_append accepted a
    // broken AST and codegen panicked with "undefined variable 'flag'".
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("[{n: 1}]"), "got: {stdout}");
}

// ── groupBy computed keys must be a compile-time error ────────────

/// Compile `source` expecting failure; returns the compiler's stderr.
fn compile_expect_error(test_name: &str, source: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_{}_{}",
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
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let _ = fs::remove_dir_all(&dir);
    assert!(
        !out.status.success(),
        "knot build unexpectedly succeeded for {test_name}:\nstderr: {stderr}"
    );
    stderr
}

#[test]
fn group_by_computed_key_is_compile_error() {
    let stderr = compile_expect_error(
        "groupby_computed_key",
        r#"type Todo = {owner: Text, title: Text}
*todos : [Todo]

main = do
  replace *todos = [{owner: "a", title: "x"}]
  r <- do
    t <- *todos
    groupBy {k: t.owner ++ "x"}
    yield {k: t.owner, cnt: count t}
  println (show r)
"#,
    );
    // Previously compiled fine and aborted at runtime with
    // "key column 'k' not found in schema".
    assert!(
        stderr.contains("plain field accesses"),
        "expected groupBy key diagnostic, got: {stderr}"
    );
}

#[test]
fn group_by_plain_field_key_still_works() {
    let (stdout, stderr, ok) = compile_and_run(
        "groupby_plain_key",
        r#"type Todo = {owner: Text, title: Text}
*todos : [Todo]

main = do
  replace *todos = [{owner: "a", title: "x"}, {owner: "a", title: "y"}, {owner: "b", title: "z"}]
  r <- do
    t <- *todos
    groupBy {k: t.owner}
    yield {k: t.owner, cnt: count t}
  println (show r)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("{cnt: 2, k: a}"), "got: {stdout}");
    assert!(stdout.contains("{cnt: 1, k: b}"), "got: {stdout}");
}

// ── retry inside a relational sub-do within atomic ─────────────────

#[test]
fn retry_inside_relational_sub_do_completes() {
    // `retry` fires from inside a relational do-block (which has its own
    // arena frame open) nested in the atomic body. The direct jump to the
    // retry block must pop that frame — previously it leaked one frame per
    // retry iteration. Functionally: the program must still wake up on the
    // writer's update and terminate with the new row.
    let (stdout, stderr, ok) = compile_and_run(
        "retry_sub_do",
        r#"type Item = {n: Int}
*items : [Item]

writer = do
  sleep 300
  replace *items = [{n: 42}]

main = do
  replace *items = [{n: 1}]
  fork writer
  v <- atomic do
    rows <- *items
    r <- do
      t <- rows
      yield (if t.n == 42 then t else retry)
    yield r
  println ("got: " ++ show v)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("got: [{n: 42}]"), "got: {stdout}");
}
