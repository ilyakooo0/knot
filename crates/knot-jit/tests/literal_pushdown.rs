//! Mixed-comprehension pushdown: a comprehension that binds both a persisted
//! source and an in-memory relation literal cross-joins them in SQL — the
//! literal becomes an inline `SELECT ... UNION ALL ...` FROM subquery — instead
//! of materializing the source in memory. (Without this, the bind's expr is
//! neither a source-ref nor a source-bound var, so the planner returned None
//! and the whole comprehension materialized.)

mod e2e;
use e2e::{knot_bin, TempDir};

/// Build `src` and return the build's stderr (where the in-memory-read advice
/// is reported). The pushdown succeeded iff the advice is absent.
fn build_stderr(name: &str, src: &str) -> String {
    let dir = TempDir::fresh(name);
    let src_path = dir.join(format!("{name}.knot"));
    std::fs::write(&src_path, src).unwrap();
    let out = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(&src_path)
        .arg("-o")
        .arg(dir.join(name))
        .output()
        .expect("knot build");
    assert!(out.status.success(), "build failed: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stderr).to_string()
}

#[test]
fn scalar_literal_cross_join_pushes_down() {
    let stderr = build_stderr(
        "sxcj",
        "with {\nPerson  {name Text  age (Int 1)}\nRel Person  *people\npairs  (|\n  p <- *people\n  x <- [100  200]\n  yield {name p.name  n (p.age + x)})\n}\n(| base.println (base.show pairs); yield {})\n",
    );
    assert!(
        !stderr.contains("in-memory table read"),
        "the literal should inline as a FROM subquery (no materialization); got:\n{stderr}"
    );
}

#[test]
fn record_literal_cross_join_pushes_down() {
    let stderr = build_stderr(
        "rxcj",
        "with {\nPerson  {name Text}\nRel Person  *people\ntagged  (|\n  p <- *people\n  t <- [{label \"x\"  w 1}  {label \"y\"  w 2}]\n  yield {name p.name  tag t.label})\n}\n(| base.println (base.show tagged); yield {})\n",
    );
    assert!(
        !stderr.contains("in-memory table read"),
        "the record literal should inline as a FROM subquery; got:\n{stderr}"
    );
}

#[test]
fn mixed_comprehension_correct_rows() {
    let (stdout, _stderr, code) = e2e::run_program(
        "xcj",
        "with {\nPerson  {name Text  age (Int 1)}\nRel Person  *people\npairs  (|\n  p <- *people\n  x <- [100  200]\n  yield {name p.name  n (p.age + x)})\n}\n(| full *people = [{name \"a\"  age 30}]; base.println (base.show pairs); yield {})\n",
    );
    assert_eq!(code, 0);
    // Cross product: the one person × the two literal scalars.
    assert!(stdout.contains("{n 130  name a}") && stdout.contains("{n 230  name a}"),
        "expected the cross product, got:\n{stdout}");
}

#[test]
fn where_over_literal_column_pushes_down() {
    // A `where` referencing a literal table's `_value` column must translate to
    // a SQL WHERE (not materialize).
    let stderr = build_stderr(
        "wlc",
        "with {\nPerson  {name Text  age (Int 1)}\nRel Person  *people\nfiltered  (|\n  p <- *people\n  x <- [100  200]\n  where x > 150\n  yield {name p.name  n (p.age + x)})\n}\n(| full *people = [{name \"a\"  age 30}]; base.println (base.show filtered); yield {})\n",
    );
    assert!(
        !stderr.contains("in-memory table read"),
        "a where over the literal column should push down; got:\n{stderr}"
    );
    let (stdout, _, code) = e2e::run_program(
        "wlc",
        "with {\nPerson  {name Text  age (Int 1)}\nRel Person  *people\nfiltered  (|\n  p <- *people\n  x <- [100  200]\n  where x > 150\n  yield {name p.name  n (p.age + x)})\n}\n(| full *people = [{name \"a\"  age 30}]; base.println (base.show filtered); yield {})\n",
    );
    assert_eq!(code, 0);
    // Only x=200 survives (200 > 150), so n = 30 + 200 = 230.
    assert!(stdout.contains("{n 230  name a}") && !stdout.contains("{n 130"),
        "expected only the x=200 row, got:\n{stdout}");
}

#[test]
fn literal_column_arithmetic_stays_int() {
    // `p.age + x` where x is a literal Int: the projection column must be typed
    // Int (not the float fallback), so the result renders as integers.
    let (stdout, _, code) = e2e::run_program(
        "lci",
        "with {\nPerson  {name Text  age (Int 1)}\nRel Person  *people\npairs  (|\n  p <- *people\n  x <- [100  200]\n  yield {name p.name  n (p.age + x)})\n}\n(| full *people = [{name \"a\"  age 30}]; base.println (base.show (base.map (\\r -> r.n) pairs)); yield {})\n",
    );
    assert_eq!(code, 0);
    // Integers, not floats (a float mistype would render 130.0 / 230.0).
    assert!(stdout.contains("[130, 230]") && !stdout.contains("130.0"),
        "expected integer arithmetic, got:\n{stdout}");
}
