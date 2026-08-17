//! Query fields (`name <query>` — read-only, recomputed per access).
//!
//! Subprocess — these operate on persisted source relations.

mod e2e;
use e2e::assert_stdout;

#[test]
fn derived_relation_reads_source() {
    assert_stdout(
        "drv_read",
        r#"with {
*manages : Rel {manager: Text, report: Text}
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
*items : Rel {n: Int 1}
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
fn derived_relation_aggregates() {
    assert_stdout(
        "drv_agg",
        r#"with {
*sales : Rel {region: Text, amount: Int 1}
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
