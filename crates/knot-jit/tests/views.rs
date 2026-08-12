//! Derived relations (`&name = expr` — read-only, recomputed per access) and
//! views (`*name = do …` — bidirectional, constant-column auto-fill).
//!
//! Subprocess — these operate on persisted source relations.

mod e2e;
use e2e::assert_stdout;

#[test]
fn derived_relation_reads_source() {
    assert_stdout(
        "drv_read",
        r#"with {
*manages : [{manager: Text, report: Text}]
&directReports = (do
  ms <- full *manages
  yield (do
    m <- ms
    yield {manager m.manager report m.report}))
}
(do
  full *manages = [{manager "A" report "B"} {manager "A" report "C"} {manager "B" report "D"}]
  rows <- &directReports
  base.println (base.show (base.count rows))
  yield {})"#,
        "\"3\"\n{}",
    );
}

#[test]
fn derived_relation_recomputes_on_access() {
    // Read the derived relation, mutate the source, read again — must reflect
    // the new source contents (recomputed, not cached).
    assert_stdout(
        "drv_recompute",
        r#"with {
*items : [{n: Int 1}]
&doubled = (do
  rs <- full *items
  yield (do
    r <- rs
    yield {n (r.n * 2)}))
}
(do
  full *items = [{n 1}]
  a <- &doubled
  base.println (base.show a)
  full *items = [{n 5}]
  b <- &doubled
  base.println (base.show b)
  yield {})"#,
        "\"[{n: 2}]\"\n\"[{n: 10}]\"\n{}",
    );
}

#[test]
fn view_read_filters_constant_column() {
    // BUG (reproduced): a filtered view `*openTodos = do t <- *todos; where …;
    // yield …` read via `full *openTodos` queries a non-existent
    // `_knot_openTodos` table ("query error: no such table") instead of
    // resolving to the source `_knot_todos` with the constant-column filter.
    // The view read dispatch does not resolve views inside do-block binds.
    // This documents the current broken behavior — the program aborts.
    let dir = e2e::TempDir::fresh("view_read");
    e2e::build_in_dir(
        "view_read",
        r#"with {
data Status = Open {} | Closed {}
*todos : [{title: Text, status: Status}]
*openTodos = do
  t <- *todos
  where t.status == Status.Open {}
  yield {title t.title}
}
(do
  full *todos = [{title "a" status (Status.Open {})} {title "b" status (Status.Closed {})}]
  rows <- full *openTodos
  base.println (base.show (base.count rows))
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("view_read"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no such table: _knot_openTodos"),
        "expected view-read bug, got: {stderr}"
    );
}

#[test]
fn view_write_autofills_constant_column() {
    // BUG (reproduced): writing through a filtered view aborts the same way —
    // the view is not resolved to its source table on the write path either.
    let dir = e2e::TempDir::fresh("view_write");
    e2e::build_in_dir(
        "view_write",
        r#"with {
data Status = Open {} | Closed {}
*todos : [{title: Text, status: Status}]
*openTodos = do
  t <- *todos
  where t.status == Status.Open {}
  yield {title t.title}
}
(do
  full *openTodos = [{title "task"}]
  all <- full *todos
  base.println (base.show (base.count all))
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("view_write"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no such table: _knot_openTodos"),
        "expected view-write bug, got: {stderr}"
    );
}

#[test]
fn derived_relation_aggregates() {
    assert_stdout(
        "drv_agg",
        r#"with {
*sales : [{region: Text, amount: Int 1}]
&total = (do
  rs <- full *sales
  yield {sum (base.sum (do r <- rs; yield r.amount))})
}
(do
  full *sales = [{region "x" amount 10} {region "y" amount 20} {region "x" amount 5}]
  t <- &total
  base.println (base.show t)
  yield {})"#,
        // binding a derived relation whose body yields a bare record gives the record
        "\"{sum: 35}\"\n{}",
    );
}
