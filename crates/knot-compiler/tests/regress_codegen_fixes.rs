//! Regression tests for codegen bugs:
//!
//! 1. SQL-pushdown bind params panicking on names not in the local Cranelift
//!    env (top-level constants, do-local lets in pushed-down plans).
//! 2. Impl methods written as constants bound to lambdas (`eq = \a b -> ...`)
//!    getting param_count 0 and crashing the Cranelift verifier.
//! 3. Operators (`==`, `<`, `+`, …) silently bypassing user-defined trait
//!    impls on primitive types (Int/Float/Text/Bool).
//! 4. Let-bound relation comprehensions inside IO do-blocks being left as
//!    unexecuted IO thunks (`expected Relation in len, got IO` at runtime).
//! 5. Trampoline curry chains emitting unsorted env-record keys for
//!    functions with >= 12 parameters ("10" sorts before "2").
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
        "knot_regress_cgfix_{}_{}",
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

// ── Finding 1: SQL-pushdown params not in the local env ───────────

#[test]
fn set_delete_where_with_top_level_constant_param() {
    // `where t.age > maxAge` pushes down to DELETE WHERE; `maxAge` is a
    // top-level constant (lives in user_fns, not the Cranelift env) — this
    // used to panic with `codegen: undefined variable 'maxAge'`.
    let (stdout, stderr, ok) = compile_and_run(
        "set_where_global",
        r#"*items : [{age: Int}]

maxAge = 5

main = do
  replace *items = [{age: 1}, {age: 9}]
  *items = do
    t <- *items
    where t.age > maxAge
    yield t
  rows <- *items
  println ("kept: " ++ show (count rows))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("kept: 1"),
        "expected 1 surviving row (age 9 > 5), got:\n{stdout}"
    );
}

#[test]
fn single_plan_with_do_local_let_param() {
    // `single (do { let lim = 1; ... where t.a == lim ... })` pushes the
    // plan to SQL (the let is substituted as an Expr param), but the STM
    // read-predicate extractor emitted `Var("lim")` for the same name —
    // which is neither in env nor a global — and panicked. The fix skips
    // the precision upgrade (keeps the broad read filter) for such names.
    let (stdout, stderr, ok) = compile_and_run(
        "single_let_param",
        r#"*items : [{a: Int}]

main = do
  replace *items = [{a: 1}, {a: 2}]
  let r = single (do
    let lim = 1
    t <- *items
    where t.a == lim
    yield t)
  println (show r)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("Just {value: {a: 1}}"),
        "expected the a==1 row, got:\n{stdout}"
    );
}

#[test]
fn pipe_take_with_top_level_constant_param() {
    // `rows |> take limitN` pushes LIMIT ? down to SQL; `limitN` is a
    // top-level constant — this used to panic in Env::get.
    let (stdout, stderr, ok) = compile_and_run(
        "pipe_take_global",
        r#"*items : [{a: Int}]

limitN = 2

main = do
  replace *items = [{a: 1}, {a: 2}, {a: 3}]
  rows <- *items
  let firstTwo = rows |> take limitN
  println ("took: " ++ show (count firstTwo))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("took: 2"),
        "expected 2 rows from take limitN, got:\n{stdout}"
    );
}

#[test]
fn count_filter_with_top_level_constant_param() {
    // count (filter ...) pushdown with a global threshold in the lambda.
    let (stdout, stderr, ok) = compile_and_run(
        "count_filter_global",
        r#"*items : [{age: Int}]

cutoff = 3

main = do
  replace *items = [{age: 1}, {age: 4}, {age: 9}]
  let n = count (filter (\t -> t.age > cutoff) *items)
  println ("n: " ++ show n)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("n: 2"),
        "expected 2 rows over cutoff, got:\n{stdout}"
    );
}

// ── Finding 2: impl methods as constants bound to lambdas ─────────

#[test]
fn impl_method_lambda_body_param_count() {
    // `eq = \a b -> true` used to register param_count 0, declaring the
    // dispatcher as (db) -> ret while call sites passed 2 args — Cranelift
    // verifier panic at compile time.
    let (stdout, stderr, ok) = compile_and_run(
        "impl_lambda_params",
        r#"impl Eq Int where
  eq = \a b -> true

main = do
  println (show (1 == 2))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("True"),
        "user eq impl (always true) must drive ==, got:\n{stdout}"
    );
}

#[test]
fn impl_method_partial_explicit_params_plus_lambda() {
    // `eq a = \b -> ...` mixes one explicit param with a trailing lambda.
    // `method_params_body` only unwrapped the lambda when the explicit
    // params were empty, so the impl declared a 1-arg function while the
    // dispatcher (using the trait signature's 2-arrow arity) called it with
    // 2 args — a signature mismatch that crashed at runtime. The fix flattens
    // explicit params together with every leading lambda.
    let (stdout, stderr, ok) = compile_and_run(
        "impl_partial_params",
        r#"data Pt = Pt {x: Int, y: Int}

impl Eq Pt where
  eq a = \b -> a.x == b.x

main = do
  println (show (Pt {x: 1, y: 2} == Pt {x: 1, y: 9}))
  println (show (Pt {x: 1, y: 2} == Pt {x: 5, y: 2}))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("True") && stdout.contains("False"),
        "partial-param eq impl must compare on x only, got:\n{stdout}"
    );
}

#[test]
fn trait_default_method_lambda_body_param_count() {
    // Same bug class for trait defaults written as lambda-bound constants.
    let (stdout, stderr, ok) = compile_and_run(
        "trait_default_lambda_params",
        r#"data Box
  = Box {n: Int}
  deriving (Tag)

trait Tag a where
  tag : a -> Text
  tag = \x -> "default"

main = do
  println (tag (Box {n: 1}))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("default"),
        "default method written as lambda must apply, got:\n{stdout}"
    );
}

// ── Finding 3: operators must use user impls on primitive types ───

#[test]
fn operators_dispatch_through_user_primitive_impls() {
    // With `impl Eq Int where eq a b = true`, both the method call and the
    // == operator must agree; == used to silently call the runtime
    // fallback and print False.
    let (stdout, stderr, ok) = compile_and_run(
        "user_prim_eq",
        r#"impl Eq Int where
  eq a b = true

main = do
  println (show (eq 1 2))
  println (show (1 == 2))
  println (show (1 != 2))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        lines[0].contains("True") && lines[1].contains("True"),
        "eq and == must both use the user impl, got:\n{stdout}"
    );
    assert!(
        lines[2].contains("False"),
        "!= must negate the user impl's eq, got:\n{stdout}"
    );
}

#[test]
fn user_primitive_eq_applies_inside_conditions() {
    // compile_condition has a separate unboxed fast path for == — it must
    // also respect the user impl.
    let (stdout, stderr, ok) = compile_and_run(
        "user_prim_eq_cond",
        r#"impl Eq Int where
  eq a b = true

main = do
  let msg = if 1 == 2 then "impl" else "fallback"
  println msg
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("impl"),
        "if-condition == must dispatch through the user impl, got:\n{stdout}"
    );
}

#[test]
fn user_primitive_num_and_ord_impls_drive_operators() {
    let (stdout, stderr, ok) = compile_and_run(
        "user_prim_num_ord",
        r#"impl Num Int where
  add a b = 42
  sub a b = a
  mul a b = b
  div a b = a
  negate a = a

main = do
  println (show (1 + 1))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("42"),
        "+ must dispatch through the user Num Int impl, got:\n{stdout}"
    );
}

#[test]
fn operators_without_user_impls_keep_builtin_semantics() {
    // No user impls anywhere: operators must keep their intrinsic fast
    // paths and produce standard results.
    let (stdout, stderr, ok) = compile_and_run(
        "no_user_impls_ops",
        r#"main = do
  println (show (1 == 2))
  println (show (2 == 2))
  println (show (1 < 2))
  println (show (1 + 2))
  println (show (2.5 * 2.0))
  println (show ("a" ++ "b"))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines[0].contains("False"), "1 == 2 must be False:\n{stdout}");
    assert!(lines[1].contains("True"), "2 == 2 must be True:\n{stdout}");
    assert!(lines[2].contains("True"), "1 < 2 must be True:\n{stdout}");
    assert!(lines[3].contains('3'), "1 + 2 must be 3:\n{stdout}");
    assert!(lines[4].contains('5'), "2.5 * 2.0 must be 5:\n{stdout}");
    assert!(lines[5].contains("ab"), "concat must work:\n{stdout}");
}

// ── Finding 4: let-bound relation comprehension in IO do-blocks ───

#[test]
fn let_bound_relation_comprehension_materializes() {
    // `let xs = do { t <- *items; where ...; yield t }` is typed [T] by
    // inference; codegen used to leave it as an unexecuted IO thunk and
    // `count xs` aborted with "expected Relation in len, got IO".
    let (stdout, stderr, ok) = compile_and_run(
        "let_comprehension",
        r#"*items : [{age: Int}]

main = do
  replace *items = [{age: 1}, {age: 9}]
  let xs = do
    t <- *items
    where t.age > 3
    yield t
  println (show (count xs))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains('1'),
        "comprehension must filter to 1 row (age 9), got:\n{stdout}"
    );
}

#[test]
fn let_bound_external_io_stays_deferred() {
    // `let action = println "hi"` followed by a bare `action` must print
    // exactly once — external-effect IO must NOT run at let time.
    let (stdout, stderr, ok) = compile_and_run(
        "let_io_deferred",
        r#"main = do
  let action = println "hi"
  action
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert_eq!(
        stdout.matches("hi").count(),
        1,
        "println bound by let must run exactly once, got:\n{stdout}"
    );
}

#[test]
fn let_bound_unused_external_io_never_runs() {
    let (stdout, stderr, ok) = compile_and_run(
        "let_io_unused",
        r#"main = do
  let action = println "never"
  println "done"
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        !stdout.contains("never"),
        "unused let-bound IO must not run, got:\n{stdout}"
    );
    assert!(stdout.contains("done"), "got:\n{stdout}");
}

// ── Finding 5: trampoline curry chains with >= 12 params ──────────

#[test]
fn thirteen_param_function_curries_in_order() {
    // Env-record keys "0".."10".. used to be emitted in numeric order,
    // violating the runtime's lexicographic invariant ("10" < "2") and
    // panicking in knot_record_from_pairs (or scrambling argument order).
    // Separators make any arg-order scrambling visible.
    let (stdout, stderr, ok) = compile_and_run(
        "tramp_13_params",
        r#"f13 = \a b c d e f g h i j k l m ->
  show a ++ "." ++ show b ++ "." ++ show c ++ "." ++ show d ++ "." ++
  show e ++ "." ++ show f ++ "." ++ show g ++ "." ++ show h ++ "." ++
  show i ++ "." ++ show j ++ "." ++ show k ++ "." ++ show l ++ "." ++ show m

apply = \g x -> g x

main = do
  let g = f13
  println (apply (g 1 2 3 4 5 6 7 8 9 10 11 12) 13)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("1.2.3.4.5.6.7.8.9.10.11.12.13"),
        "13 curried args must keep their order, got:\n{stdout}"
    );
}

#[test]
fn deriving_eq_ord_runs_with_structural_comparison() {
    // `deriving (Eq, Ord)` must type-check AND run: `==`/`<` fall back to the
    // runtime's structural comparison (constructors order by tag name). Before
    // the fix the type checker rejected the program outright.
    let (stdout, stderr, ok) = compile_and_run(
        "deriving_eq_ord",
        r#"data Color = Red {} | Blue {} deriving (Eq, Ord)
main = do
  println (show (Red {} == Blue {}))
  println (show (Red {} == Red {}))
  println (show (Blue {} < Red {}))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // Red != Blue, Red == Red, and "Blue" < "Red" lexicographically.
    assert!(stdout.contains("False"), "Red == Blue should be False:\n{stdout}");
    assert!(stdout.contains("True"), "Red == Red should be True:\n{stdout}");
}

#[test]
fn maxon_over_custom_ord_adt_does_not_silently_misorder() {
    // `maxOn` dispatches past any user `Ord` impl, so it must abort on ADT
    // keys rather than return a structural (lexicographic) answer that could
    // contradict the user's order. The program must not print a "top:" line.
    let (stdout, _stderr, _ok) = compile_and_run(
        "maxon_custom_ord_adt",
        r#"data Level = Low {} | High {}
impl Eq Level where
  eq = \a b -> show a == show b
impl Ord Level where
  compare = \a b -> case a of
    Low x -> (case b of
      Low y -> EQ {}
      _ -> LT {})
    High x -> (case b of
      High y -> EQ {}
      _ -> GT {})
type T = {lvl: Level, n: Int}
*t : [T]
main = do
  replace *t = [{lvl: Low {}, n: 1}, {lvl: High {}, n: 2}]
  let top = maxOn (\r -> r.lvl) *t
  println ("top: " ++ show top)
"#,
    );
    assert!(
        !stdout.contains("top: Low"),
        "maxOn must not silently misorder ADTs against a custom Ord:\n{stdout}"
    );
}

// ── Atomic: per-row guard/pattern skip must not roll back the txn ──

#[test]
fn atomic_loop_where_guard_skip_keeps_prior_writes() {
    // A `where` guard inside a comprehension bind loop nested in `atomic`
    // used to call `knot_stm_skip` on every failing row, setting the sticky
    // skip flag that rolls back the WHOLE transaction at atomic end — so a
    // single filtered-out row discarded writes for rows that already passed.
    // The fix gates `knot_stm_skip` on `io_loop_skip_block.is_none()` (only a
    // top-level guard aborts the atomic; a per-row guard just skips the row).
    let (stdout, stderr, ok) = compile_and_run(
        "atomic_loop_where_skip",
        r#"*log : [{id: Int}]

process = atomic do
  let items = [{id: 10, keep: true}, {id: 20, keep: false}]
  row <- items
  where row.keep
  *log = union *log [{id: row.id}]

main = do
  replace *log = []
  process
  result <- *log
  println ("count: " ++ show (count result))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("count: 1"),
        "the id:10 write (row before the failing guard) must survive, got:\n{stdout}"
    );
}

#[test]
fn atomic_loop_ctor_mismatch_skip_keeps_prior_writes() {
    // Same bug via a constructor-pattern bind: `Circle c <- shapes` inside
    // `atomic` skips non-Circle rows. A mismatch used to roll back the whole
    // transaction instead of just skipping the row.
    let (stdout, stderr, ok) = compile_and_run(
        "atomic_loop_ctor_skip",
        r#"data Shape = Circle {r: Int} | Square {s: Int}
*log : [{r: Int}]

process = atomic do
  let shapes = [Circle {r: 7}, Square {s: 3}]
  Circle c <- shapes
  *log = union *log [{r: c.r}]

main = do
  replace *log = []
  process
  result <- *log
  println ("count: " ++ show (count result))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("count: 1"),
        "the Circle write must survive the later Square mismatch, got:\n{stdout}"
    );
}

// ── View `where` filters must not be silently dropped ──────────────

#[test]
fn view_where_filter_restricts_reads_and_fills_writes() {
    // Regression: `analyze_view` only inspected the bind + yield statements,
    // discarding `where bindvar.col == const` filters entirely. Reads returned
    // ALL source rows, and writes left the filter column NULL (a later read
    // then crashed on the null field). The filter must drive both the read
    // WHERE and the write auto-fill.
    let (stdout, stderr, ok) = compile_and_run(
        "view_where_filter",
        r#"*accounts : [{owner: Text, balance: Int}]

*aliceAccounts = do
  a <- *accounts
  where a.owner == "alice"
  yield {balance: a.balance}

main = do
  replace *accounts = [{owner: "alice", balance: 1}, {owner: "bob", balance: 2}]
  filtered <- *aliceAccounts
  println ("read: " ++ show (count filtered))
  *aliceAccounts = [{balance: 7}]
  all <- *accounts
  forEach all (\r -> println ("row: " ++ r.owner ++ ":" ++ show r.balance))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("read: 1"),
        "view read must be filtered to alice's single row, got:\n{stdout}"
    );
    // The write through the view must auto-fill owner = "alice".
    assert!(
        stdout.contains("row: alice:7"),
        "view write must auto-fill the filter column (owner=alice), got:\n{stdout}"
    );
    assert!(
        !stdout.contains("row: :7"),
        "view write must not leave the filter column empty, got:\n{stdout}"
    );
}

// ── Finding 6: local binding shadowing a function name was ignored ──
// `compile_app` dispatched applied calls by name (`user_fns` / stdlib /
// SQL-pushdown special forms) WITHOUT first checking whether the name was
// locally bound, so a lambda param / let / do-bind that shadowed a function
// called the global instead of the local value — a silent wrong answer, or a
// hard runtime crash when the shadowed name was a stdlib function.

#[test]
fn local_param_shadowing_user_fn_is_called() {
    // `run`'s param `helper` shadows the top-level `helper`; `run (\y -> y*100)`
    // must apply the lambda (500), not the global `helper` (5+1=6).
    let (stdout, stderr, ok) = compile_and_run(
        "shadow_user_fn",
        r#"helper = \x -> x + 1
run = \helper -> helper 5
main = do
  let r = run (\y -> y * 100)
  println (show r)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("500"),
        "local param must shadow the top-level `helper` (expected 500), got:\n{stdout}"
    );
}

#[test]
fn local_param_shadowing_stdlib_fn_is_called() {
    // `count` is a stdlib function (in `user_fns`); shadowing it with a lambda
    // param used to dispatch to the `count` runtime and crash with
    // "expected Relation in len, got Int".
    let (stdout, stderr, ok) = compile_and_run(
        "shadow_stdlib_fn",
        r#"apply2 = \count -> count 7
main = do
  let r = apply2 (\n -> n * 2)
  println (show r)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("14"),
        "local param must shadow the stdlib `count` (expected 14), got:\n{stdout}"
    );
}

// ── BUG B4: bare refs to user decls named like zero-arg builtins ───
// The bare-`Var` arm in `compile_expr` checked the zero-arg builtin special
// cases (`now`, `randomFloat`, `randomUuid`, `generateKeyPair`,
// `generateSigningKeyPair`, `readLine`, `retry`) BEFORE consulting `user_fns`.
// Those names are not stdlib functions, so a user declaration `now = 5` was
// compiled but never referenced: the bare `now` emitted `knot_now_io`, yielding
// an `IO` value where the type checker had inferred `Int`. `now + 1` then called
// `knot_value_add(Value::IO, Int)` and panicked at runtime. The applied-call
// path always consulted `user_fns` first; only bare references diverged.

#[test]
fn bare_ref_to_user_decl_named_like_zero_arg_builtin() {
    // `now = 5` shadows the `now` builtin; the bare reference `now` must read
    // the user's `Int` constant (so `now + 1` is 6), not emit `knot_now_io`.
    let (stdout, stderr, ok) = compile_and_run(
        "shadow_zero_arg_builtin_now",
        r#"now = 5
main = println (show (now + 1))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains('6'),
        "bare `now` must read the user constant (expected 6), got:\n{stdout}"
    );
}

// ── when/unless guards not gating relation writes ─────────────────
//
// `Set`/`ReplaceSet` are typed `IO {} {}`, but their `compile_expr` arms emit
// the write inline. In *statement* position that is right (the write is meant
// to run); in *argument* position it fired while the call's arguments were
// being built, so `when False (*rel = …)` performed the write and handed `when`
// only the unit result — the guard had nothing left to suppress. The `do`-block
// form (`when False do *rel = …`) was unaffected, because a do-block argument
// already compiles to a deferred IO thunk. Arguments now defer writes the same
// way, so the callee decides whether to run them.

#[test]
fn when_false_does_not_run_relation_write() {
    // `when false` / `unless true` must leave the relation untouched, whether
    // the write is a bare argument or a `do`-block, and whether the guard is a
    // literal or a runtime value.
    let (stdout, stderr, ok) = compile_and_run(
        "when_guard_gates_write",
        r#"*items : [{a: Int}]

flag = false

main = do
  replace *items = [{a: 1}, {a: 2}]
  when false (replace *items = [{a: 99}])
  unless true (replace *items = [])
  when flag (*items = union *items [{a: 99}])
  when false do
    replace *items = [{a: 99}]
  rows <- *items
  println ("count: " ++ show (count rows))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("count: 2"),
        "a falsy when/unless guard must suppress the write (expected 2 rows), got:\n{stdout}"
    );
}

#[test]
fn when_true_still_runs_relation_write_exactly_once() {
    // The other half of the fix: deferring the write must not drop it, or run
    // it twice. The append form makes a double-run observable — running the
    // thunk twice would add two rows instead of one.
    let (stdout, stderr, ok) = compile_and_run(
        "when_guard_runs_write_once",
        r#"*items : [{a: Int}]

flag = true

main = do
  replace *items = [{a: 1}]
  when true (*items = union *items [{a: 2}])
  unless false (*items = union *items [{a: 3}])
  when flag (*items = union *items [{a: 4}])
  rows <- *items
  println ("count: " ++ show (count rows))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("count: 4"),
        "a truthy when/unless guard must run the write exactly once (expected 4 rows), got:\n{stdout}"
    );
}

// ── Finding 6: `<-`-bound source comprehension silently yielding {} ─

#[test]
fn bind_bound_source_comprehension_yields_all_rows() {
    // `xs <- do { r <- *items; where …; yield … }` is a comprehension whose
    // only IO is the relation read. It used to go through `compile_expr` →
    // `compile_io_do` (is_io_do_block sees the SourceRef bind), where `where`
    // is a GUARD over the whole relation value rather than a per-row filter:
    // `r.v` silently meant "the FIRST row's v", and a false guard skipped the
    // rest of the block and bound `{}` — total data loss. The `let`-bound form
    // has always compiled through the relational loop path, so the two forms
    // disagreed on identical source. They must now agree.
    let (stdout, stderr, ok) = compile_and_run(
        "bind_source_comprehension",
        r#"*items : [{k: Text, v: Int}]

main = do
  replace *items = [{k: "a", v: 1}, {k: "b", v: 5}, {k: "c", v: 9}]

  -- first row (v = 1) fails the filter: the guard reading used to bail out
  -- of the whole block and yield {}
  bound <- do
    r <- *items
    where r.v > 3
    yield r.k
  let letted = do
    r <- *items
    where r.v > 3
    yield r.k

  -- first row passes: the guard reading yielded just that one row's value
  -- ("a") instead of accumulating every match
  boundAll <- do
    r <- *items
    where r.v > 0
    yield r.k

  println ("bound: " ++ show bound)
  println ("letted: " ++ show letted)
  println ("boundAll: " ++ show boundAll)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // The filter drops the first row — the regression yielded `{}` here.
    assert!(
        stdout.contains("bound: [b, c]"),
        "a `<-`-bound source comprehension must accumulate every matching row, got:\n{stdout}"
    );
    // The `<-` and `let` forms must not disagree on identical source.
    assert!(
        stdout.contains("letted: [b, c]"),
        "the let-bound form must keep working, got:\n{stdout}"
    );
    // The filter keeps every row — the regression yielded just `a`.
    assert!(
        stdout.contains("boundAll: [a, b, c]"),
        "`where` must be a per-row filter, not a guard on the first row, got:\n{stdout}"
    );
}
