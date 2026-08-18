//! Smoke tests validating the harness renders values correctly.

mod harness;
use harness::{assert_compile_err, assert_show};

#[test]
fn int_arith() {
    assert_show("40 + 2", "42");
}

#[test]
fn text_literal() {
    // Top-level `show`/`println` of a Text prints it raw (unquoted) — quoting
    // only applies to Text nested inside a record/constructor field.
    assert_show("\"hello\"", "hello");
}

#[test]
fn record_show() {
    assert_show("{x 3 y 4}", "{x 3 y 4}");
}

#[test]
fn relation_map() {
    assert_show("base.map (\\n -> n * 2) [1 2 3]", "[2, 4, 6]");
}

#[test]
fn type_error_rejected() {
    assert_compile_err("1 + \"x\"", "");
}
