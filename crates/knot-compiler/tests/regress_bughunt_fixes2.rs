//! Regression tests for a bug-hunt batch:
//!
//! 1. Associated-type projections failed to reduce when the impl head was a
//!    `Ty::Alias` — i.e. a single-variant record `data` type (`match_pattern`
//!    had no alias arm, and `apply` preserves the alias wrapper). Multi-variant
//!    `data` heads (`Ty::Con`) already worked.
//! 2. A lone `yield e` do-block in a non-relational monad (IO / Maybe) fell
//!    through to the relational `compile_do` path and wrapped its value in a
//!    singleton relation (`[e]`) instead of dispatching on the resolved monad.
//! 3. `sortBy` over a do-block pushed a Float ORDER-BY key into SQL with no
//!    `sortby_projection_pushable` guard, diverging from in-memory `total_cmp`.
//! 4. `elem (arithmetic) [literals]` pushed down `IN (...)` without casting the
//!    arithmetic needle, so an INTEGER needle never matched TEXT-bound ints.
//! 5. A user-defined constructor named `Cons` could be constructed but never
//!    pattern-matched (the parser forced the built-in two-atom list-cons form).
//!
//! Each test compiles a small Knot program with the real `knot` binary into its
//! own scratch directory and asserts on the program's output.

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

fn compile(test_name: &str, source: &str) -> Result<Compiled, String> {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_bughunt2_{}_{}",
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
    if !out.status.success() {
        return Err(format!(
            "knot build failed for {test_name}:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ));
    }
    let exe = dir.join("prog");
    Ok(Compiled { dir, exe })
}

fn compile_and_run(test_name: &str, source: &str) -> (String, String, bool) {
    let c = compile(test_name, source).unwrap_or_else(|e| panic!("{e}"));
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

// ── Finding 1: associated-type reduction on Ty::Alias heads ───────────

#[test]
fn associated_type_reduces_on_single_variant_data() {
    let (stdout, stderr, ok) = compile_and_run(
        "assoc_single_variant",
        r#"trait Container c where
  type Elem c
  toList : c -> [Elem c]

data IntBox = IntBox {v: Int 1}

impl Container IntBox where
  type Elem IntBox = Int 1
  toList b = case b of
    IntBox {v v} -> [v]

useit : IntBox -> [Int 1]
useit = \b -> toList b

main = do
  println (show (toList (IntBox {v 5})))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("[5]"),
        "expected toList (IntBox 5) == [5], got:\n{stdout}"
    );
}

#[test]
fn associated_type_reduces_with_multi_variant_control() {
    // Control: a multi-variant `data` head is a `Ty::Con` and already worked —
    // pin it alongside the single-variant (Ty::Alias) fix so both paths stay
    // green.
    let (stdout, stderr, ok) = compile_and_run(
        "assoc_multi_variant",
        r#"trait Container c where
  type Elem c
  toList : c -> [Elem c]

data Two = A {v: Int 1} | B {v: Int 1}

impl Container Two where
  type Elem Two = Int 1
  toList t = case t of
    A {v v} -> [v]
    B {v v} -> [v]

main = do
  println (show (toList (A {v 9})))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("[9]"), "expected [9], got:\n{stdout}");
}

// ── Finding 3: lone-yield do-block dispatches on the resolved monad ──

#[test]
fn single_yield_io_do_block_returns_value_not_relation() {
    // greet's body is `do { yield (...) }` typed IO {} Text. It must return the
    // Text, not a singleton relation `["hi bob"]`.
    let (stdout, stderr, ok) = compile_and_run(
        "single_yield_io",
        r#"greet : Text -> IO {} Text
greet = \n -> do
  yield ("hi " ++ n)

main = do
  g <- greet "bob"
  println g
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("hi bob") && !stdout.contains("[\"hi bob\"]"),
        "expected `hi bob` (not a relation), got:\n{stdout}"
    );
}

#[test]
fn single_yield_do_block_respects_maybe_and_relation() {
    let (stdout, stderr, ok) = compile_and_run(
        "single_yield_monads",
        r#"nums : [Int 1]
nums = do
  yield 5

mb : Maybe Int 1
mb = do
  yield 7

main = do
  println (show nums)
  println (show mb)
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("[5]"), "expected relation [5], got:\n{stdout}");
    assert!(
        stdout.contains("Just"),
        "expected Maybe to be Just 7, got:\n{stdout}"
    );
}

// ── Finding 4: sortBy Float do-block falls back to in-memory ──────────

#[test]
fn sortby_float_do_block_orders_numerically() {
    let (stdout, stderr, ok) = compile_and_run(
        "sortby_float",
        r#"*items : [{name: Text, qty: Int 1, price: Float 1}]

byqty = do
  it <- *items
  where it.qty > 0
  yield it

main = do
  replace *items = [
    {name "a" qty 10 price 2.5},
    {name "b" qty 9 price 1.5},
    {name "c" qty 2 price 3.5}
  ]
  si <- byqty
  with {bi (sortBy (\x -> x.qty) si)} (do println ("qty: " ++ show (map (\x -> x.qty) bi)); yield {})
  sf <- byqty
  with {bf (sortBy (\x -> x.price) sf)} (do println ("price: " ++ show (map (\x -> x.price) bf)); yield {})
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // Int key still pushes down and sorts numerically.
    assert!(
        stdout.contains("qty: [2, 9, 10]"),
        "expected numeric int order, got:\n{stdout}"
    );
    // Float key sorts correctly via in-memory fallback.
    assert!(
        stdout.contains("price: [1.5, 2.5, 3.5]"),
        "expected numeric float order, got:\n{stdout}"
    );
}

// ── Finding 6: a user constructor named `Cons` can be matched ────────

#[test]
fn user_cons_constructor_is_matchable() {
    // `Cons {head, tail}` is one record payload → a user constructor pattern,
    // not the built-in two-atom `Cons head tail` list form.
    let (stdout, stderr, ok) = compile_and_run(
        "user_cons",
        r#"data L = Nil {} | Cons {head: Int 1, tail: L}

sumL : L -> Int 1
sumL = \l -> case l of
  Nil {} -> 0
  Cons {head h tail t} -> h + sumL t

main = do
  println (show (sumL (Cons {head 1 tail (Cons {head 2 tail (Nil {})})})))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains('3'), "expected 1+2 == 3, got:\n{stdout}");
}

#[test]
fn builtin_list_cons_still_matches() {
    // The two-atom form `Cons a rest` must still destructure a non-empty list.
    let (stdout, stderr, ok) = compile_and_run(
        "builtin_cons",
        r#"firstOf : [Int 1] -> Int 1
firstOf = \xs -> case xs of
  Cons a rest -> a
  _ -> 0

main = do
  println (show (firstOf [7, 8, 9]))
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains('7'), "expected head 7, got:\n{stdout}");
}

// ── Finding 5: elem with an arithmetic needle pushes down correctly ──

#[test]
fn elem_arithmetic_needle_matches() {
    let (stdout, stderr, ok) = compile_and_run(
        "elem_arith",
        r#"*rows : [{a: Int 1, b: Int 1}]

query = do
  rs <- *rows
  yield (count (rs |> filter (\x -> elem (x.a + x.b) [8, 99])))

main = do
  replace *rows = [{a 5 b 3}, {a 6 b 4}]
  c <- query
  println ("matched: " ++ show c)
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // Only row a=5,b=3 sums to 8 (in the list); a=6,b=4 sums to 10 (not).
    assert!(
        stdout.contains("matched: 1"),
        "expected exactly one match, got:\n{stdout}"
    );
}
