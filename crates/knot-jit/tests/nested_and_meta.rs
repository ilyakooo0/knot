//! Nested relations (a `[T]` field inside a row) and remaining builtins
//! (`base.extract`, `base.compile`, `base.hash`).

mod harness;
use harness::{assert_show, assert_show_set};

// ── Nested relations ─────────────────────────────────────────────────────

#[test]
fn nested_relation_construction() {
    assert_show(
        "{team \"eng\"  members [{name \"a\"}]}",
        "{members [{name a}]  team eng}",
    );
}

#[test]
fn nested_relation_count() {
    assert_show(
        "base.count {team \"eng\"  members [{name \"a\"}  {name \"b\"}]}.members",
        "2",
    );
}

#[test]
fn nested_relation_map() {
    assert_show_set(
        "base.map (\\m -> m.name) {members [{name \"a\"}  {name \"b\"}]}.members",
        &["a", "b"],
    );
}

#[test]
fn deeply_nested() {
    assert_show("{a {b {c [{x 1}]}}}.a.b.c", "[{x 1}]");
}

#[test]
fn nested_empty() {
    assert_show("{members (base.the (Rel {name Text}) [])}", "{members []}");
}

// ── base.hash ────────────────────────────────────────────────────────────

#[test]
fn hash_deterministic() {
    assert_show("base.hash 42 == base.hash 42", "True");
}

#[test]
fn hash_distinct_inputs() {
    assert_show("base.hash 42 == base.hash 43", "False");
}

#[test]
fn hash_text_vs_int() {
    assert_show("base.hash \"42\" == base.hash 42", "False");
}

// ── base.extract ─────────────────────────────────────────────────────────

#[test]
fn extract_scalar() {
    assert_show("base.extract 42", "42");
}

#[test]
fn extract_record() {
    // extract produces evaluable knot source: records use space separators.
    assert_show("base.extract {x 1  y 2}", "{x 1  y 2}");
}

#[test]
fn extract_text_quoted() {
    // extract produces evaluable source, so Text IS quoted here (unlike show).
    assert_show("base.extract \"hi\"", "\"hi\"");
}

// ── base.compile ─────────────────────────────────────────────────────────
// base.compile : Text -> Result Text a (Err = the compile-error message).
// The in-process JIT harness can't nest a JIT compile, so these live in the
// subprocess suite (see persistence.rs::compile_*). Verified there:
//   base.compile "40 + 2" : Result Text (Int 1)  →  Ok 42
//   type-mismatched / invalid source             →  Err <message>
