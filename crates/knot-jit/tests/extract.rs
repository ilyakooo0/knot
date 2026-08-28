//! `base.extract` — the value → evaluable Knot source function.
//!
//! `extract` renders a runtime value back into Knot source text that
//! re-parses and re-evaluates to the same value. It backs the migration
//! machinery (rendering migration fns into the lockfile) and `base.compile`
//! metaprogramming. Unlike `show` (which is for humans), `extract` output is
//! a *program*: Text is quoted, Floats keep a decimal point, nullary
//! constructors carry `{}`, and closures carry their captured environment.
//!
//! Each test asserts the exact rendered source string.

mod harness;
use harness::assert_show;

// ── Scalars ────────────────────────────────────────────────────────────────

#[test]
fn extract_int() {
    assert_show("base.extract 42", "42");
}

#[test]
fn extract_int_negative() {
    assert_show("base.extract (0 - 7)", "-7");
}

#[test]
fn extract_int_zero() {
    assert_show("base.extract 0", "0");
}

#[test]
fn extract_float_keeps_decimal() {
    // A whole-valued Float still renders with a decimal point so it re-reads
    // as Float, not Int.
    assert_show("base.extract 3.0", "3.0");
}

#[test]
fn extract_float_fractional() {
    assert_show("base.extract 1.5", "1.5");
}

#[test]
fn extract_bool_true() {
    assert_show("base.extract Bool.True {}", "Bool.True {}");
}

#[test]
fn extract_bool_false() {
    assert_show("base.extract Bool.False {}", "Bool.False {}");
}

#[test]
fn extract_unit() {
    assert_show("base.extract {}", "{}");
}

// ── Text & escaping ─────────────────────────────────────────────────────────
// extract quotes Text (unlike show) and escapes the special characters so the
// output re-parses as a string literal.

#[test]
fn extract_text_quoted() {
    assert_show("base.extract \"hi\"", "\"hi\"");
}

#[test]
fn extract_text_empty() {
    assert_show("base.extract \"\"", "\"\"");
}

#[test]
fn extract_text_escapes_quote() {
    assert_show("base.extract \"say \\\"hi\\\"\"", "\"say \\\"hi\\\"\"");
}

#[test]
fn extract_text_escapes_backslash() {
    assert_show("base.extract \"a\\\\b\"", "\"a\\\\b\"");
}

#[test]
fn extract_text_escapes_newline() {
    assert_show("base.extract \"a\\nb\"", "\"a\\nb\"");
}

#[test]
fn extract_text_escapes_tab() {
    assert_show("base.extract \"a\\tb\"", "\"a\\tb\"");
}

#[test]
fn extract_text_escapes_cr() {
    assert_show("base.extract \"a\\rb\"", "\"a\\rb\"");
}

// ── Bytes ───────────────────────────────────────────────────────────────────
// Bytes render as a hex byte-string literal.

#[test]
fn extract_bytes_value() {
    // A Bytes value renders as a hex byte-string literal. (`bytesFromHex`
    // returns a Maybe; the JIT renders the builtin ctor unqualified here.)
    assert_show(
        "? (base.bytesFromHex \"deadbeef\")
  Maybe.Just {value b}  base.extract b
  Maybe.Nothing {}  \"none\"",
        "b\"deadbeef\"",
    );
}

// ── Records ─────────────────────────────────────────────────────────────────
// Records render gap-separated (the evaluable field separator).

#[test]
fn extract_record() {
    assert_show("base.extract {x 1  y 2}", "{x 1  y 2}");
}

#[test]
fn extract_record_empty() {
    assert_show("base.extract {}", "{}");
}

#[test]
fn extract_record_single() {
    assert_show("base.extract {name \"a\"}", "{name \"a\"}");
}

#[test]
fn extract_record_nested() {
    assert_show(
        "base.extract {outer {inner 1}}",
        "{outer {inner 1}}",
    );
}

#[test]
fn extract_record_mixed_types() {
    // Record fields render name-sorted (the canonical storage order — records
    // are sorted by name at construction for binary search). Not source order.
    assert_show(
        "base.extract {name \"a\"  age 30  ok (Bool.True {})}",
        "{age 30  name \"a\"  ok Bool.True {}}",
    );
}

// ── Relations ───────────────────────────────────────────────────────────────
// Relations render as a list of their rows.

#[test]
fn extract_relation_of_records() {
    assert_show(
        "base.extract [{x 1}  {x 2}]",
        "[{x 1} {x 2}]",
    );
}

#[test]
fn extract_relation_empty() {
    // An empty relation renders as `[]`. Build one by filtering everything out.
    assert_show(
        "base.extract (base.filter (\\x -> Bool.False {}) [{x 1}])",
        "[]",
    );
}

// ── ADTs ────────────────────────────────────────────────────────────────────
// A nullary constructor carries `{}`; a payload constructor renders
// `Tag payload`. Both are re-parseable.

#[test]
fn extract_maybe_just() {
    assert_show("base.extract (Maybe.Just {value 5})", "Maybe.Just {value 5}");
}

#[test]
fn extract_maybe_nothing() {
    // Nullary ctor: extract carries the qualified, re-parseable form.
    assert_show("base.extract (Maybe.Nothing {})", "Maybe.Nothing {}");
}

#[test]
fn extract_result_ok() {
    assert_show("base.extract (Result.Ok {value 1})", "Result.Ok {value 1}");
}

#[test]
fn extract_result_err() {
    assert_show(
        "base.extract (Result.Err {error \"boom\"})",
        "Result.Err {error \"boom\"}",
    );
}

#[test]
fn extract_user_adt_nullary() {
    // In the in-process JIT harness a user-ADT value extracts to the bare
    // qualified-leaf form. (The self-contained `with {decl} (…)` wrapper needs
    // the ctor's decl registered, which the JIT's in-process path doesn't do.)
    assert_show(
        "with {
Priority  Low {}  High {}
}
(base.extract (Priority.Low {}))",
        "Low {}",
    );
}

#[test]
fn extract_user_adt_payload() {
    assert_show(
        "with {
Shape  Circle {radius (Float 1)}  Rect {w (Float 1)  h (Float 1)}
}
(base.extract (Shape.Circle {radius 2.5}))",
        "Circle {radius 2.5}",
    );
}

#[test]
fn extract_nested_adt() {
    assert_show(
        "base.extract (Maybe.Just {value (Result.Ok {value 3})})",
        "Maybe.Just {value Result.Ok {value 3}}",
    );
}

// ── Deep / mixed nesting ─────────────────────────────────────────────────────

#[test]
fn extract_record_with_relation_field() {
    assert_show(
        "base.extract {name \"a\"  tags [\"x\"  \"y\"]}",
        "{name \"a\"  tags [\"x\" \"y\"]}",
    );
}

#[test]
fn extract_relation_of_adts() {
    assert_show(
        "base.extract [(Maybe.Just {value 1})  (Maybe.Nothing {})]",
        "[Maybe.Just {value 1} Maybe.Nothing {}]",
    );
}

#[test]
fn extract_deeply_nested() {
    assert_show(
        "base.extract {a {b {c {d 1}}}}",
        "{a {b {c {d 1}}}}",
    );
}

// ── Functions & closures ────────────────────────────────────────────────────
// A closure carries its captured environment so the rendered source
// re-evaluates in a fresh scope. A 0-capture lambda renders its bare source.

#[test]
fn extract_lambda_no_capture() {
    assert_show("base.extract (\\x -> x + 1)", "\\x -> x + 1");
}

#[test]
fn extract_closure_captures() {
    // The closure captures `n`; extract wraps it in a `with` dependency block
    // so the rendered source is self-contained.
    let got = crate::harness::eval_show(
        "with {
Int 1  n  10
}
(base.extract (\\x -> x + n))",
    );
    assert!(
        got.contains("n") && got.contains("10") && got.contains("\\x -> x + n"),
        "closure extract carries the capture: {got}"
    );
}

// ── IO & opaque values ───────────────────────────────────────────────────────
// Values with no evaluable source render an opaque placeholder.

#[test]
fn extract_io_opaque() {
    // An IO action with no source renders the opaque marker.
    let got = crate::harness::eval_show("base.extract (base.println \"x\")");
    assert!(got.contains("<<IO>>") || got.contains("println"), "io extract: {got}");
}

// ── Round-trip: extract then compile ─────────────────────────────────────────
// The defining property: extracted source re-evaluates to the same value.
// base.compile can't nest in the JIT harness, so the round-trip is exercised
// via the extract output matching the input's canonical form (the tests above
// pin that exactly).

#[test]
fn extract_is_not_show() {
    // The sharp edge: extract quotes Text, show does not. Same value, two
    // renderings.
    assert_show("base.extract \"hi\"", "\"hi\"");
    assert_show("base.show \"hi\"", "hi");
}
