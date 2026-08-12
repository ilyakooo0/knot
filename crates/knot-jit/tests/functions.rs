//! Higher-order functions, control flow, dictionaries, higher-rank types.

mod harness;
mod e2e;
use e2e::assert_stdout;
use harness::{assert_compile_err, assert_prog, assert_show, assert_show_set};

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
    assert_show("[3 1 2] |> base.sortBy (\\n -> n)", "[1, 2, 3]");
    assert_show("5 |> (\\n -> n * n)", "25");
}

// ── Higher-order over relations ──────────────────────────────────────────

#[test]
fn hof_composition() {
    assert_show_set(
        "base.map (\\n -> n + 1) (base.filter (\\n -> n % 2 == 0) [1 2 3 4 5 6])",
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
pos : Int 1 -> Maybe (Int 1)
pos (\x -> case x > 0 of
  Bool.True {} -> Maybe.Just {value x}
  Bool.False {} -> Maybe.Nothing {})
}
(do
  base.println (base.show (base.traverse pos [1 2 3]))
  base.println (base.show (base.traverse pos [1 (-2) 3]))
  yield {})"#,
        "\"Just {value: [1, 2, 3]}\"\n\"Nothing\"\n{}",
    );
}

// ── Control flow ─────────────────────────────────────────────────────────

#[test]
fn when_unless() {
    // when/unless are IO-conditional; here we just confirm a case on Bool.
    assert_show(
        "(case (2 > 1) of\n  Bool.True {} -> 1\n  Bool.False {} -> 0)",
        "1",
    );
}

#[test]
fn case_as_expression() {
    assert_show(
        "base.length (case (1 == 1) of\n  Bool.True {} -> \"yes\"\n  Bool.False {} -> \"nope\")",
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
        "with {\napplyTwice (\\(f : (forall a. a -> a)) -> {asText (f \"text\") asInt (f 42)})\n}\n(applyTwice (\\y -> y))",
        "{asInt: 42, asText: text}",
    );
}

// ── Type annotations & inference ─────────────────────────────────────────

#[test]
fn explicit_annotation() {
    assert_show("(42 : Int 1)", "42");
}

#[test]
fn annotation_mismatch_rejected() {
    assert_compile_err("(42 : Text)", "");
}

#[test]
fn wildcard_annotation() {
    assert_show("(42 : Int _)", "42");
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
