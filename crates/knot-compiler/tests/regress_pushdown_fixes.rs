//! Regression tests for codegen and SQL-pushdown fixes:
//!   1. Nested case-arm sub-patterns must be TESTED, not just bound.
//!   2. Beta-reduction must respect lambda-parameter shadowing.
//!   3. IO bind-loop guards must skip rows without pushing unit.
//!   4. groupBy must reject post-group references to non-primary binds
//!      and keys based on other variables (clean diagnostics, no ICE).
//!   5. User trait impls on primitives must disable SQL pushdown.
//!   6. Int/Int division pushed to SQL must stay Int-typed.
//!   7. `trim` must not push down (Unicode vs ASCII whitespace).
//!
//!   8/9. minOn/maxOn/sortBy over if/then/else on Int columns must not
//!   push down (CASE loses the KNOT_INT collation).
//!  10. Pushed Int-arithmetic comparisons must compare numerically so an
//!      i64 overflow can't satisfy arbitrary filters.
//!  11. Float comparisons must not push down (total_cmp vs SQL
//!      -0.0/NaN-as-NULL semantics).
//!  12. Ordered comparisons on tag (enum-ADT) columns must not push down.

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
        "knot_regress_pf_{}_{}",
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

/// Compile `source` expecting failure; returns the compiler's stderr.
fn compile_expect_error(test_name: &str, source: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_pf_{}_{}",
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

// ── Bug 1: nested case-arm sub-patterns must be tested ─────────────

#[test]
fn nested_literal_in_constructor_pattern_is_tested() {
    let (stdout, stderr, ok) = compile_and_run(
        "case_nested_lit",
        r#"main = do
  with {m (Just {value 5})} (do with {r (case m of Just {value 1} -> "one"; Just {value n} -> show n; Nothing {} -> "none")} (do println r))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // Just {value: 5} must NOT match the {value: 1} arm.
    assert!(
        stdout.contains("5") && !stdout.contains("one"),
        "nested literal pattern must be tested, got: {stdout}"
    );
}

#[test]
fn nested_literal_in_list_pattern_is_tested() {
    let (stdout, stderr, ok) = compile_and_run(
        "case_list_lit",
        r#"main = do
  with {xs [5]} (do with {r (case xs of [1] -> "one"; [n] -> show n; _ -> "many")} (do println r))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("5") && !stdout.contains("one"),
        "list element literal must be tested, got: {stdout}"
    );
}

#[test]
fn nested_literal_in_record_pattern_is_tested() {
    let (stdout, stderr, ok) = compile_and_run(
        "case_record_lit",
        r#"main = do
  with {p {tag 2 name "b"}} (do with {r (case p of {tag 1} -> "first"; {tag t} -> "tag " ++ show t)} (do println r))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("tag 2") && !stdout.contains("first"),
        "record field literal must be tested, got: {stdout}"
    );
}

#[test]
fn nested_constructor_in_record_pattern_is_tested() {
    let (stdout, stderr, ok) = compile_and_run(
        "case_nested_ctor",
        r#"main = do
  with {p {st (Nothing {}) n 7}} (do with {r (case p of {st (Just v)} -> "just"; {st (Nothing {})} -> "nothing")} (do println r))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("nothing") && !stdout.contains("just"),
        "nested constructor tag must be tested, got: {stdout}"
    );
}

// ── Bug 2: beta-reduction must respect lambda shadowing ────────────

#[test]
fn filter_param_shadowing_local_let_is_not_expanded() {
    let (stdout, stderr, ok) = compile_and_run(
        "beta_shadow",
        r#"type E = {value: Int 1}
*es : [E]

main = do
  replace *es = [{value 10}, {value 60}]
  rows <- *es
  with {q {value 99}} (do with {kept (rows |> filter (\q -> q.value > 50))} (do println ("kept: " ++ show (count kept)); println ("q: " ++ show q.value)))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The lambda's `q` shadows the let-bound `q` — the filter must compare
    // each ROW's value, not the constant 99 (which would keep both rows).
    assert!(stdout.contains("kept: 1"), "got: {stdout}");
    assert!(stdout.contains("q: 99"), "got: {stdout}");
}

// ── Bug 3: IO bind-loop guards must skip rows, not push unit ───────

#[test]
fn io_loop_where_guard_skips_row_without_unit() {
    let (stdout, stderr, ok) = compile_and_run(
        "io_loop_guard",
        r#"main = do
  r <- do
    x <- [1, 2, 3]
    where x > 1
    println ("saw " ++ show x)
    yield x
  println ("r: " ++ show r)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The guard-failed first row must be SKIPPED — previously the result
    // was [{}, 2, 3] (a unit pushed for the failed row).
    assert!(stdout.contains("r: [2, 3]"), "got: {stdout}");
    assert!(
        !stdout.contains("saw 1"),
        "guarded-out row must not run IO actions, got: {stdout}"
    );
    assert!(stdout.contains("saw 2") && stdout.contains("saw 3"), "got: {stdout}");
}

#[test]
fn io_loop_pattern_mismatch_skips_row_without_unit() {
    let (stdout, stderr, ok) = compile_and_run(
        "io_loop_pat_skip",
        r#"main = do
  r <- do
    x <- [Nothing {}, Just {value 4}]
    Just v <- x
    println ("got " ++ show v.value)
    yield v.value
  println ("r: " ++ show r)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // The Nothing row fails the `Just v <-` match and must be skipped.
    assert!(stdout.contains("r: [4]"), "got: {stdout}");
}

// ── Bug 4: groupBy diagnostics instead of verifier ICEs ────────────

#[test]
fn group_by_post_group_reference_to_other_bind_is_compile_error() {
    let stderr = compile_expect_error(
        "groupby_other_bind",
        r#"type X = {k: Text, v: Int 1}
type Y = {k: Text}
*xs : [X]
*ys : [Y]

main = do
  r <- do
    x <- *xs
    y <- *ys
    where x.k == y.k
    groupBy {g y.k}
    yield {g y.k n x.v}
  println (show r)
"#,
    );
    // Previously a Cranelift verifier panic (use of non-dominating value).
    assert!(
        stderr.contains("cannot be referenced after groupBy"),
        "expected post-groupBy reference diagnostic, got: {stderr}"
    );
}

#[test]
fn group_by_key_on_non_primary_variable_is_compile_error() {
    let stderr = compile_expect_error(
        "groupby_wrong_key_base",
        r#"type Todo = {owner: Text, title: Text}
*todos : [Todo]

cfg = {owner "a"}

main = do
  r <- do
    t <- *todos
    groupBy {g cfg.owner}
    yield {g t.owner cnt (count t)}
  println (show r)
"#,
    );
    // Previously the key column was silently attributed to the primary
    // bind's relation.
    assert!(
        stderr.contains("grouped binding"),
        "expected groupBy key base diagnostic, got: {stderr}"
    );
}

#[test]
fn group_by_on_primary_bind_still_works() {
    let (stdout, stderr, ok) = compile_and_run(
        "groupby_still_works",
        r#"type Todo = {owner: Text, title: Text}
*todos : [Todo]

main = do
  replace *todos = [{owner "a" title "x"}, {owner "a" title "y"}, {owner "b" title "z"}]
  r <- do
    t <- *todos
    groupBy {k t.owner}
    yield {k t.owner cnt (count t)}
  println (show r)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("{cnt: 2, k: a}"), "got: {stdout}");
    assert!(stdout.contains("{cnt: 1, k: b}"), "got: {stdout}");
}

// ── Bug 6: Int 1/Int division pushed to SQL stays Int-typed ──────────

#[test]
fn int_division_in_yield_projection_stays_int() {
    let (stdout, stderr, ok) = compile_and_run(
        "int_div_projection",
        r#"type T = {x: Int 1}
*t : [T]

main = do
  replace *t = [{x 5}]
  r <- do
    m <- *t
    yield {h (m.x / 2)}
  println (show r)
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // In-memory Int/Int is integer division (5 / 2 = 2); the pushed-down
    // result schema previously said float and printed 2.0.
    assert!(
        stdout.contains("{h: 2}") && !stdout.contains("2.0"),
        "Int division must stay Int 1, got: {stdout}"
    );
}

// ── Bug 7: trim must not push down ─────────────────────────────────

#[test]
fn trim_in_where_is_unicode_aware() {
    let (stdout, stderr, ok) = compile_and_run(
        "trim_unicode",
        // The name is padded with EM SPACE (U+2003): Rust's str::trim (the
        // runtime) strips it, SQLite's TRIM (ASCII space only) does not.
        "type T = {name: Text}\n\
         *p : [T]\n\
         \n\
         main = do\n  \
           replace *p = [{name \"\u{2003}x\u{2003}\"}]\n  \
           rows <- *p\n  \
           with {c (countWhere (\\r -> trim r.name == \"x\") rows)} (do\n             println (\"c: \" ++ show c))\n",
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("c: 1"),
        "trim must be Unicode-aware (in-memory), got: {stdout}"
    );
}

// ── Bugs 8/9: if/then/else over Int columns in MIN/MAX and ORDER BY ──

#[test]
fn min_on_if_else_over_int_column_matches_in_memory() {
    let (stdout, stderr, ok) = compile_and_run(
        "minon_int_case",
        r#"type T = {a: Int 1}
*t : [T]

main = do
  replace *t = [{a 9}, {a 10}]
  rows <- *t
  with {m (minOn (\r -> if r.a > 5 then r.a else 99) rows)} (do println ("m: " ++ show m))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // SQL MIN(CASE ...) compared '10' < '9' byte-wise and returned 10.
    assert!(stdout.contains("m: 9"), "got: {stdout}");
}

#[test]
fn sort_by_if_else_over_int_column_matches_in_memory() {
    let (stdout, stderr, ok) = compile_and_run(
        "sortby_int_case",
        r#"type T = {a: Int 1}
*t : [T]

main = do
  replace *t = [{a 10}, {a 9}]
  rows <- *t
  with {s (rows |> sortBy (\r -> if r.a > 5 then r.a else 0))} (do println ("s: " ++ show s))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // SQL ORDER BY CASE sorted '10' before '9' byte-wise.
    assert!(stdout.contains("s: [{a: 9}, {a: 10}]"), "got: {stdout}");
}

// ── Bug 10: Int 1-arithmetic overflow must not satisfy filters ───────

#[test]
fn int_arithmetic_overflow_in_where_compares_numerically() {
    // a * b overflows i64 (-1.6e19): SQLite computes the REAL
    // approximation, which must compare by VALUE (-1.6e19 > 5 is false).
    // The previous CAST-to-TEXT KNOT_INT comparison ranked the
    // unparseable '-1.6e+19' above every integer, wrongly including the
    // row. (In-memory evaluation panics on the overflow; the approximate
    // numeric comparison is the documented pushdown behavior.)
    let (stdout, stderr, ok) = compile_and_run(
        "int_arith_overflow",
        r#"type T = {a: Int 1, b: Int 1}
*t : [T]

main = do
  replace *t = [{a (0 - 4000000000) b 4000000000}]
  rows <- *t
  with {c (countWhere (\r -> r.a * r.b > 5) rows)} (do println ("c: " ++ show c))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("c: 0"),
        "overflowed product must not satisfy `> 5`, got: {stdout}"
    );
}

#[test]
fn int_arithmetic_where_still_pushes_correct_results() {
    // In-range Int arithmetic comparisons must keep working (no
    // over-fallback regression and the NUMERIC casts compare correctly).
    let (stdout, stderr, ok) = compile_and_run(
        "int_arith_inrange",
        r#"type T = {a: Int 1, b: Int 1}
*t : [T]

main = do
  replace *t = [{a 5 b 2}, {a 1 b 9}, {a (0 - 3) b 4}]
  rows <- *t
  with {n (countWhere (\r -> r.a * r.b > 8) rows)} (do println ("n: " ++ show n); with {m (countWhere (\r -> r.a + r.b < 2) rows)} (do println ("m: " ++ show m)))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // 10 > 8, 9 > 8, -12 > 8 → 2.
    assert!(stdout.contains("n: 2"), "got: {stdout}");
    // 7 < 2 (no), 10 < 2 (no), 1 < 2 (yes) → 1.
    assert!(stdout.contains("m: 1"), "got: {stdout}");
}

// ── Bug 11: float comparisons stay in memory ───────────────────────

#[test]
fn float_neg_zero_equality_is_consistent() {
    let (stdout, stderr, ok) = compile_and_run(
        "float_total_cmp",
        r#"type T = {x: Float 1}
*t : [T]

main = do
  replace *t = [{x 0.0}]
  rows <- *t
  with {neg (0.0 * (0.0 - 1.0))} (do with {e (countWhere (\r -> r.x == neg) rows)} (do println ("eq: " ++ show e); with {g (countWhere (\r -> r.x > neg) rows)} (do println ("gt: " ++ show g))))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    // -0.0 == 0.0 under IEEE / SQLite / Knot equality. alloc_float
    // canonicalizes -0.0 to +0.0 so both in-memory and pushed SQL agree:
    // eq: 1 (equal), gt: 0 (not greater-than).
    assert!(stdout.contains("eq: 1"), "got: {stdout}");
    assert!(stdout.contains("gt: 0"), "got: {stdout}");
}

// ── Bug 13: minOn/maxOn over a genuine Text column push down but must
//    return Text verbatim (the runtime's `is_text` flag), never re-parse a
//    numeric-looking string back to Int. ──────────────────────────────────

#[test]
fn minmax_over_text_column_returns_text_not_reparsed_int() {
    let (stdout, stderr, ok) = compile_and_run(
        "minmax_text_col",
        r#"type Z = {code: Text, n: Int 1}
*z : [Z]

main = do
  replace *z = [{code "007" n 1}, {code "005" n 2}, {code "003" n 3}]
  with {hi (maxOn (\r -> r.code) *z)} (do
    with {lo (minOn (\r -> r.code) *z)} (do
      -- `++` requires Text; if the runtime re-parsed "007" to Int 7 this would
      -- both corrupt the value ("7") and break the Text concatenation.
      println ("hi: " ++ hi)
      println ("lo: " ++ lo)))
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(stdout.contains("hi: 007"), "expected Text max '007', got: {stdout}");
    assert!(stdout.contains("lo: 003"), "expected Text min '003', got: {stdout}");
}
