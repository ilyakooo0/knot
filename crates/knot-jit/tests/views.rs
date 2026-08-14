//! Query fields (`name <query>` — read-only, recomputed per access) and
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
directReports (do
  m <- *manages
  yield {manager m.manager report m.report})
}
(do
  full *manages = [{manager "A" report "B"} {manager "A" report "C"} {manager "B" report "D"}]
  base.println (base.show (base.count directReports))
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
doubled (do
  r <- *items
  yield {n (r.n * 2)})
}
(do
  full *items = [{n 1}]
  a <- base.run doubled
  base.println (base.show a)
  full *items = [{n 5}]
  b <- base.run doubled
  base.println (base.show b)
  yield {})"#,
        "\"[{n: 2}]\"\n\"[{n: 10}]\"\n{}",
    );
}

#[test]
fn view_read_filters_constant_column() {
    // A filtered view `*openTodos = do t <- *todos; where …; yield …` read via
    // `full *openTodos` resolves to the source `_knot_todos` with the
    // constant-column filter — only the Open todo is returned.
    assert_stdout(
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
        "\"1\"\n{}",
    );
}

#[test]
fn view_write_autofills_constant_column() {
    // Writing through a filtered view resolves to the source table and
    // auto-fills the constant column (status = Open).
    assert_stdout(
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
  base.println (base.show all)
  yield {})"#,
        "\"1\"\n\"[{status: Open, title: task}]\"\n{}",
    );
}

#[test]
fn derived_relation_aggregates() {
    assert_stdout(
        "drv_agg",
        r#"with {
*sales : [{region: Text, amount: Int 1}]
amounts (do
  r <- *sales
  yield {amount r.amount})
}
(do
  full *sales = [{region "x" amount 10} {region "y" amount 20} {region "x" amount 5}]
  base.println (base.show {sum (base.sum (do r <- amounts; yield r.amount))})
  yield {})"#,
        // a query field iterated in an aggregate comprehension over the source
        "\"{sum: 35}\"\n{}",
    );
}
