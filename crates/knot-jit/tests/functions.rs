//! Higher-order functions, control flow, dictionaries, higher-rank types.

mod e2e;
mod harness;
use e2e::assert_stdout;
use harness::{assert_compile_err, assert_show, assert_show_set};

// ── Lambdas & application ────────────────────────────────────────────────

#[test]
fn lambda_apply() {
    assert_show("(\\x -> x * 2) 21", "42");
}

#[test]
fn lambda_multi_arg() {
    assert_show("(\\x y -> x + y) 3 4", "7");
}

#[test]
fn closure_capture() {
    assert_show("(\\x -> (\\y -> x + y)) 10 5", "15");
}

#[test]
fn shadowing_inner_scope_rejected() {
    // Rebinding a `with`-name from an enclosing scope is a clean error.
    assert_compile_err(
        "with {\nfoo 1\n}\n((with {\nfoo 2\n}\nfoo))",
        "shadowing is not allowed",
    );
}

// NOTE: rebinding a name twice within the SAME `with` block passes inference
// but then PANICS the compiler in codegen (Result::unwrap on
// DuplicateDefinition) instead of producing a clean diagnostic — a compiler
// robustness bug, verified against a compiled binary. Not asserted here.

#[test]
fn pipe_forward() {
    assert_show("[3  1  2] |> base.sortBy (\\n -> n)", "[1, 2, 3]");
    assert_show("5 |> (\\n -> n * n)", "25");
}

// ── Higher-order over relations ──────────────────────────────────────────

#[test]
fn hof_composition() {
    assert_show_set(
        "base.map (\\n -> n + 1) (base.filter (\\n -> n % 2 == 0) [1  2  3  4  5  6])",
        &["3", "5", "7"],
    );
}

#[test]
fn traverse_maybe_and_result() {
    // traverse sequences Maybe/Result applicatives (per the documented "f is
    // IO, Maybe, or Result"): Just/Ok of the collected values, short-circuiting
    // to Nothing/Err on the first failure. Verified via a compiled binary —
    // the runtime dispatch compares the constructor leaf so a qualified
    // `Maybe.Just` tag dispatches correctly.
    assert_stdout(
        "traverse_maybe",
        r#"with {
Int 1 -> Maybe (Int 1)  pos  (\x -> ? (x > 0)
  Bool.True {}  Maybe.Just {value x}
  Bool.False {}  Maybe.Nothing {})
}
(do
  base.println (base.show (base.traverse pos [1  2  3]))
  base.println (base.show (base.traverse pos [1  (-2)  3]))
  yield {})"#,
        "\"Just {value [1, 2, 3]}\"\n\"Nothing\"\n{}",
    );
}

// ── Control flow ─────────────────────────────────────────────────────────

#[test]
fn when_unless() {
    // when/unless are IO-conditional; here we just confirm a match on Bool.
    assert_show(
        "(? (2 > 1)\n  Bool.True {}  1\n  Bool.False {}  0)",
        "1",
    );
}

#[test]
fn case_as_expression() {
    assert_show(
        "base.length (? (1 == 1)\n  Bool.True {}  \"yes\"\n  Bool.False {}  \"nope\")",
        "3",
    );
}

#[test]
fn id_function() {
    assert_show("base.id 42", "42");
    assert_show("base.id \"hi\"", "hi");
}

// ── Dictionary constraints (^field) ──────────────────────────────────────

// ── Dictionary constraints (^field) & morphs ────────────────────────────
//
// `(^into)` resolves against an annotated toplevel binding's declared type,
// matching `base.morph.<from>To<to>.into` fields (which take dimensionless
// types, e.g. `Text -> Maybe (Int 1)` is `Text -> Maybe Int` internally).
// This needs real toplevel bindings, so it's exercised in the subprocess
// suite (morph_resolution), not the JIT. An inline un-annotated `(^into)`
// is ambiguous and rejected:

#[test]
fn implicit_field_ambiguous_rejected() {
    assert_compile_err("(^into) \"42\"", "ambiguous projection");
}

// ── Higher-rank types (forall) ───────────────────────────────────────────

#[test]
fn forall_rank2() {
    assert_show(
        "with {\n_  applyTwice  (\\((forall a. a -> a)  f) -> {asText (f \"text\")  asInt (f 42)})\n}\n(applyTwice (\\y -> y))",
        "{asInt 42  asText text}",
    );
}

#[test]
fn type_witness_unit_annotated_and_hole_args() {
    // A type-witness function `\(Type  T)` takes a type argument positionally.
    // Beyond a bare head (`Int`), the argument may be unit-annotated (`Float 1`,
    // `(Float 1)`) or a `_` hole.
    assert_show(
        "with {\n_  asT  ((\\(Type  T)  x -> x))\n}\n(base.show (asT Float 1 42))",
        "42",
    );
    assert_show(
        "with {\n_  asT  ((\\(Type  T)  x -> x))\n}\n(base.show (asT (Float 1) 42))",
        "42",
    );
    assert_show(
        "with {\n_  asT  ((\\(Type  T)  x -> x))\n}\n(base.show (asT _ 42))",
        "42",
    );
    // A parenthesized parameterized/nested type argument also works: the inner
    // type-head spans are erased with the type.
    assert_show(
        "with {\n_  asT  ((\\(Type  T)  x -> x))\n}\n(base.show (asT (Maybe Int) (Maybe.Just {value 3})))",
        "Just {value 3}",
    );
    assert_show(
        "with {\n_  asT  ((\\(Type  T)  x -> x))\n}\n(base.show (asT (Maybe (Int 1)) (Maybe.Just {value 3})))",
        "Just {value 3}",
    );
}

#[test]
fn lambda_param_type_prefix() {
    // `\(T  x)` — a type-prefix param annotation (gap-separated), the
    // pattern-position analogue of the `Type  name` signature form.
    assert_show(
        "with {\n_  addOne  (\\(Int 1  x) -> x + 1)\n}\n(base.show (addOne 41))",
        "42",
    );
}

#[test]
fn lambda_param_type_prefix_forall() {
    // The prefix form also carries a `forall` type.
    assert_show(
        "with {\n_  applyTwice  (\\((forall a. a -> a)  f) -> {asText (f \"text\")  asInt (f 42)})\n}\n(base.show (applyTwice (\\y -> y)))",
        "{asInt 42  asText text}",
    );
}

// ── Type annotations & inference ─────────────────────────────────────────

#[test]
fn explicit_annotation() {
    assert_show("(base.the (Int 1) 42)", "42");
}

#[test]
fn annotation_mismatch_rejected() {
    assert_compile_err("(base.the (Text) 42)", "");
}

#[test]
fn wildcard_annotation() {
    assert_show("(base.the (Int _) 42)", "42");
}

// ── Numeric edge cases ───────────────────────────────────────────────────

#[test]
fn int_overflow_wraps_or_handles() {
    // Just ensure large arithmetic evaluates (semantics: i64).
    assert_show("1000000 * 1000000", "1000000000000");
}

#[test]
fn division_by_zero_float() {
    // Float division by zero yields inf per IEEE; just ensure it evaluates.
    assert_show("base.show (1.0 / 0.0) == base.show (1.0 / 0.0)", "True");
}

#[test]
fn modulo_negative() {
    assert_show("(0 - 7) % 3", "-1"); // Rust-style remainder
}

#[test]
fn precedence() {
    assert_show("2 + 3 * 4", "14");
    assert_show("(2 + 3) * 4", "20");
    assert_show("10 - 2 - 3", "5"); // left assoc
}

#[test]
fn comparison_chains_via_and() {
    assert_show("(1 < 2) && (2 < 3)", "True");
}

// ── IO laziness in argument position ─────────────────────────────────────

/// Any IO-yielding expression passed as a function argument must be deferred
/// into an IO thunk the callee runs — not executed eagerly at the call site.
/// `compile_arg_expr` deferred only `Set`/`FullSet`/`Atomic` leaf nodes; a
/// `match`/`if`/`do`/`with` whose *branches* yield IO fell through to
/// `compile_expr` and ran the side effect while the argument was being built,
/// even when the callee dropped the result. Deferral must be driven by
/// `expr_is_io` (any IO-typed expression), not the leaf node kind.
#[test]
fn io_arg_to_dropping_fn_does_not_run() {
    // `ignore` drops its argument; the side effect must NOT fire.
    assert_stdout(
        "ioarg_match",
        r#"with {
_  ignore  (\io -> 7)
}
(do
  ignore (? (Bool.True {})
    (Bool.True {})   (base.println "FIRED")
    (Bool.False {})  (base.println "other"))
  base.println "done"
  yield {})"#,
        "\"done\"\n{}",
    );
}

/// A `do`-block IO passed to a dropping function must not run its writes.
#[test]
fn io_arg_do_block_to_dropping_fn_does_not_run() {
    assert_stdout(
        "ioarg_do",
        r#"with {
_  ignore  (\io -> 7)
}
(do
  ignore (do
    base.println "FIRED"
    yield {})
  base.println "done"
  yield {})"#,
        "\"done\"\n{}",
    );
}

/// A callee that RUNS the IO argument still executes it (deferred, not lost).
#[test]
fn io_arg_to_running_fn_still_runs() {
    assert_stdout(
        "ioarg_run",
        r#"with {
_  runIt  (\io -> base.run io)
}
(do
  runIt (? (Bool.True {})
    (Bool.True {})   (base.println "ran")
    (Bool.False {})  (base.println "other"))
  base.println "done"
  yield {})"#,
        "\"ran\"\n\"done\"\n{}",
    );
}

/// An IO *builtin passed as a function* (a function value, not an IO
/// computation) must NOT be deferred into a thunk — the callee calls it.
/// `expr_is_io` returns true for `base.println` (a FieldAccess on base with an
/// IO builtin); deferring it handed the callee an IO thunk where it expected a
/// function ("cannot call IO, expected Function"). Only IO-*computation* forms
/// defer, not function values. `base.traverse` runs the fn per element, so the
/// effects fire only if `base.println` reaches it as a callable fn.
#[test]
fn io_builtin_passed_as_function_is_not_deferred() {
    assert_stdout(
        "iofn",
        r#"(do
  base.traverse base.println [1  2]
  base.println "done"
  yield {})"#,
        "1\n2\n\"done\"\n{}",
    );
}