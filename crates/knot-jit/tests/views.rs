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
Rel {manager Text  report Text}  *manages
directReports (|
  m <- *manages
  yield {manager m.manager  report m.report})
}
(|
  *manages = [{manager "A"  report "B"}  {manager "A"  report "C"}  {manager "B"  report "D"}]
  base.println (base.show (base.count directReports))
  yield {})"#,
        "\"[3]\"\n{}",
    );
}

#[test]
fn derived_relation_recomputes_on_access() {
    // Read the derived relation, mutate the source, read again — must reflect
    // the new source contents (recomputed, not cached).
    assert_stdout(
        "drv_recompute",
        r#"with {
Rel {n (Int 1)}  *items
doubled (|
  r <- *items
  yield {n (r.n * 2)})
}
(|
  *items = [{n 1}]
  a <- base.run [] doubled
  base.println (base.show a)
  *items = [{n 5}]
  b <- base.run [] doubled
  base.println (base.show b)
  yield {})"#,
        "\"[{n 2}]\"\n\"[{n 10}]\"\n{}",
    );
}

#[test]
fn derived_relation_aggregates() {
    assert_stdout(
        "drv_agg",
        r#"with {
Rel {region Text  amount (Int 1)}  *sales
amounts (|
  r <- *sales
  yield {amount r.amount})
}
(|
  *sales = [{region "x"  amount 10}  {region "y"  amount 20}  {region "x"  amount 5}]
  base.println (base.show {sum (base.sum (| r <- amounts; yield r.amount))})
  yield {})"#,
        // a query field iterated in an aggregate comprehension over the source;
        // sum is a one-element relation now, so the record shows `{sum [35]}`
        "\"{sum [35]}\"\n{}",
    );
}
