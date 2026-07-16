//! End-to-end regression tests for runtime fixes:
//!
//! - `take`/`drop` on Text clamp negative counts (matching the Relation
//!   versions) instead of panicking with a misleading "expected Int" message.
//! - SQL-pushed `minOn`/`maxOn` on an Int column (stored as TEXT COLLATE
//!   KNOT_INT) must parse the result back to an Int so subsequent arithmetic
//!   works, instead of returning a Text value that panics on `+ 1`.
//! - In-memory `sum` over an EMPTY `[Float]` must be `Float 0.0`, not `Int 0`:
//!   with no summands the numeric type has to come from the compiler.
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
        "knot_regress_rt_{}_{}",
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

// ── Text take/drop: negative counts clamp instead of panicking ──────

#[test]
fn text_take_drop_negative_counts_clamp() {
    let (stdout, stderr, ok) = compile_and_run(
        "text_take_drop_neg",
        r#"main = do
  let n = 0 - 2
  println ("take_neg: [" ++ take n "hello" ++ "]")
  println ("drop_neg: [" ++ drop n "hello" ++ "]")
  println ("take_pos: [" ++ take 2 "hello" ++ "]")
  println ("drop_pos: [" ++ drop 2 "hello" ++ "]")
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("take_neg: []"),
        "take with negative count should clamp to empty text:\n{stdout}"
    );
    assert!(
        stdout.contains("drop_neg: [hello]"),
        "drop with negative count should be identity:\n{stdout}"
    );
    assert!(stdout.contains("take_pos: [he]"), "{stdout}");
    assert!(stdout.contains("drop_pos: [llo]"), "{stdout}");
}

// ── SQL-pushed minOn/maxOn on Int column: result must be numeric ────

#[test]
fn min_max_on_int_column_supports_arithmetic() {
    let (stdout, stderr, ok) = compile_and_run(
        "minon_int_arith",
        r#"type Employee = {name: Text, salary: Int}
*employees : [Employee]

main = do
  replace *employees = [
    {name: "a", salary: 50},
    {name: "b", salary: 30},
    {name: "c", salary: 70}
  ]
  employees <- *employees
  let lo = minOn (\e -> e.salary) employees
  let hi = maxOn (\e -> e.salary) employees
  println ("lo_plus_one: " ++ show (lo + 1))
  println ("hi_plus_one: " ++ show (hi + 1))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("lo_plus_one: 31"),
        "minOn on an Int column must yield an Int usable in arithmetic:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("hi_plus_one: 71"),
        "maxOn on an Int column must yield an Int usable in arithmetic:\n{stdout}\n{stderr}"
    );
}

// ── Pipe form: filter + minOn pushdown also parses back to Int ──────

#[test]
fn pipe_filter_min_on_int_column_supports_arithmetic() {
    let (stdout, stderr, ok) = compile_and_run(
        "pipe_minon_int_arith",
        r#"type Employee = {name: Text, dept: Text, salary: Int}
*employees : [Employee]

main = do
  replace *employees = [
    {name: "a", dept: "Eng", salary: 50},
    {name: "b", dept: "Eng", salary: 30},
    {name: "c", dept: "Sales", salary: 10}
  ]
  employees <- *employees
  let lo = employees |> filter (\e -> e.dept == "Eng") |> minOn (\e -> e.salary)
  println ("eng_lo_plus_one: " ++ show (lo + 1))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("eng_lo_plus_one: 31"),
        "filtered minOn pushdown must yield an Int:\n{stdout}\n{stderr}"
    );
}

// Referential integrity invariant: under `*sub.f <= *sup.g`, renaming a
// superset key that is still referenced by a sub row must be rejected.
// (Current codegen rewrites the relation via full DELETE+INSERT, so the abort
// comes from the DELETE-on-sup trigger; the BEFORE UPDATE OF <sup_col> trigger
// added alongside it guards the same invariant on the in-place UPDATE path as
// defense in depth. Either way the rename must not silently orphan the order.)
#[test]
fn renaming_referenced_superset_key_is_rejected() {
    let (stdout, stderr, ok) = compile_and_run(
        "fk_superset_key_rename",
        r#"type Person = {name: Text, age: Int}
*people : [Person]
*orders : [{customer: Text, amount: Int}]

*orders.customer <= *people.name

main = do
  replace *people = [{name: "Alice", age: 30}]
  replace *orders = [{customer: "Alice", amount: 100}]
  ppl <- *people
  let renamed = do
    p <- ppl
    yield (if p.name == "Alice" then {p | name: "Alicia"} else p)
  *people = renamed
  println "rename succeeded"
  yield {}
"#,
    );
    assert!(
        !ok,
        "renaming a referenced superset key must abort, but it succeeded:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !stdout.contains("rename succeeded"),
        "program continued past the rejected key rename:\nstdout: {stdout}"
    );
    assert!(
        stderr.contains("subset constraint violated"),
        "expected a subset-constraint abort, got:\nstderr: {stderr}"
    );
}

// In-memory `sum` takes the result's numeric type from its summands, so an
// empty relation used to fall back to `Int 0` — a `[Float]` sum then showed as
// "0" and serialized as an integer. A fully-applied `sum f rel` now carries the
// inferred type down to the runtime, the way the SQL-pushdown path always has.
// (These relations are in-memory literals, so no pushdown is involved.)
#[test]
fn sum_over_empty_float_relation_is_float_zero() {
    let (stdout, stderr, ok) = compile_and_run(
        "sum_empty_float",
        r#"prices = [{amount: 1.5}, {amount: 2.5}]
counts = [{qty: 2}, {qty: 3}]

main = do
  let noPrices = filter (\p -> p.amount > 100.0) prices
  let noCounts = filter (\c -> c.qty > 100) counts
  println ("empty_float: " ++ show (sum (\p -> p.amount) noPrices))
  println ("empty_int: " ++ show (sum (\c -> c.qty) noCounts))
  println ("float: " ++ show (sum (\p -> p.amount) prices))
  println ("int: " ++ show (sum (\c -> c.qty) counts))
  println ("piped_empty_float: " ++ show (noPrices |> sum (\p -> p.amount)))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("empty_float: 0.0"),
        "sum over an empty [Float] must be Float 0.0:\n{stdout}"
    );
    assert!(
        stdout.contains("piped_empty_float: 0.0"),
        "the `|> sum f` pipe form must agree with `sum f rel`:\n{stdout}"
    );
    assert!(
        stdout.contains("empty_int: 0"),
        "sum over an empty [Int] must stay Int 0:\n{stdout}"
    );
    // Non-empty sums are unchanged.
    assert!(
        stdout.contains("float: 4.0") && stdout.contains("int: 5"),
        "non-empty sums must still add up:\n{stdout}"
    );
}

// ── M1: sortBy dedup uses compare_keys (respects user Ord), not structural ──

#[test]
fn sortby_dedup_respects_custom_ord() {
    // Regression (M1): `knot_relation_sort_by` sorted by key with `compare_keys`
    // (which honors a user `Ord` impl) but deduplicated consecutive rows with
    // the structural `compare_values`. When the user order disagrees with the
    // structural one, the two passes contradict each other. Here all three
    // grades compare Equal under the custom `Ord`, so after sorting they are
    // adjacent and dedup must collapse them to a single element.
    let (stdout, stderr, ok) = compile_and_run(
        "sortby_dedup_custom_ord",
        r#"data Grade = A {} | B {} | C {}
impl Eq Grade where
  eq = \x y -> show x == show y
impl Ord Grade where
  compare = \x y -> EQ {}
main = do
  let items = [A {}, B {}, C {}]
  let sorted = sortBy (\g -> g) items
  println ("count: " ++ show (count sorted))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("count: 1"),
        "dedup must use the same custom Ord as the sort (all grades Equal → 1 element):\n{stdout}"
    );
}
