//! Numeric and Text utility builtins: abs, floor, clamp, intMin/intMax,
//! intToFloat, textToInt/textToFloat, the ASCII trim/match family, distinct,
//! sortByDesc, upsertBy, single, elem, unify, strip/dress, not, id, countWhere.
//!
//! Pure value layer — runs in the JIT harness.

mod harness;
use harness::assert_show;

// ── Numeric ────────────────────────────────────────────────────────────────

#[test]
fn abs_neg_and_pos() {
    assert_show("base.abs (0 - 7)", "7");
    assert_show("base.abs 7", "7");
    assert_show("base.abs 0", "0");
}

#[test]
fn floor_rounds_toward_neg_inf() {
    assert_show("base.floor 2.7", "2");
    assert_show("base.floor (0.0 - 2.7)", "-3"); // toward -inf, not truncate
    assert_show("base.floor 3.0", "3");
}

#[test]
fn clamp_bounds() {
    // clamp lo hi x
    assert_show("base.clamp 1 10 5", "5"); // in range
    assert_show("base.clamp 1 10 0", "1"); // below lo
    assert_show("base.clamp 1 10 99", "10"); // above hi
}

#[test]
fn int_min_max() {
    assert_show("base.intMin 3 7", "3");
    assert_show("base.intMax 3 7", "7");
    assert_show("base.intMin (0 - 1) 0", "-1");
}

#[test]
fn int_to_float() {
    assert_show("base.intToFloat 3", "3.0");
    assert_show("base.intToFloat 0", "0.0");
}

#[test]
fn text_to_int() {
    assert_show("base.textToInt \"42\"", "Just {value 42}");
    assert_show("base.textToInt \"-7\"", "Just {value -7}");
    assert_show("base.textToInt \"abc\"", "Nothing");
    assert_show("base.textToInt \"12x\"", "Nothing");
}

#[test]
fn text_to_float() {
    assert_show("base.textToFloat \"1.5\"", "Just {value 1.5}");
    assert_show("base.textToFloat \"nope\"", "Nothing");
}

// ── ASCII trim / match ──────────────────────────────────────────────────────

#[test]
fn trim_ascii_variants() {
    assert_show("base.trimAscii \"  hi  \"", "hi");
    assert_show("base.ltrimAscii \"  hi  \"", "hi  ");
    assert_show("base.rtrimAscii \"  hi  \"", "  hi");
}

#[test]
fn ascii_case() {
    assert_show("base.toAsciiLower \"HeLLo\"", "hello");
    assert_show("base.toAsciiUpper \"HeLLo\"", "HELLO");
}

#[test]
fn trim_full_unicode() {
    // trim (unicode-aware) vs trimAscii
    assert_show("base.trim \"  x  \"", "x");
}

// ── Collection extras ──────────────────────────────────────────────────────

#[test]
fn distinct_dedups() {
    // distinct preserves first-occurrence order (does not sort)
    assert_show("base.distinct [3  1  3  2  1]", "[3, 1, 2]");
    assert_show("base.distinct [1  1  1]", "[1]");
    assert_show("base.distinct []", "[]");
}

#[test]
fn sort_by_desc() {
    assert_show("base.sortByDesc (\\x -> x) [1  3  2]", "[3, 2, 1]");
}

#[test]
fn upsert_by_replaces_or_appends() {
    // replace matching
    assert_show(
        "base.upsertBy (\\r -> r.id == 1) {id 1  v 99} [{id 1  v 5}  {id 2  v 6}]",
        "[{id 1  v 99}, {id 2  v 6}]",
    );
    // append when no match
    assert_show(
        "base.upsertBy (\\r -> r.id == 9) {id 9  v 0} [{id 1  v 5}]",
        "[{id 1  v 5}, {id 9  v 0}]",
    );
}

#[test]
fn single_maybe() {
    assert_show("base.single [42]", "Just {value 42}");
    assert_show("base.single []", "Nothing");
    assert_show("base.single [1  2]", "Nothing"); // more than one
}

#[test]
fn elem_membership() {
    assert_show("base.elem 2 [1  2  3]", "True");
    assert_show("base.elem 9 [1  2  3]", "False");
}

#[test]
fn count_where() {
    assert_show("base.countWhere (\\x -> x > 2) [1  2  3  4]", "2");
}

// ── Record / misc ──────────────────────────────────────────────────────────

#[test]
fn unify_right_biased() {
    // right-biased record merge
    assert_show("base.unify {a 1  b 2} {b 99  c 3}", "{a 1  b 99  c 3}");
}

#[test]
fn strip_dress_units() {
    // strip drops the unit, dress adds one (dimensionless round-trip)
    assert_show("base.strip (base.the (Int 1) 42)", "42");
    assert_show("base.dress 7", "7");
}

#[test]
fn int_literal_int_float_polymorphism() {
    // An integer literal is Int by default…
    assert_show("42", "42");
    assert_show("base.show (40 + 2)", "42");
    // …but Float when a Float context demands it.
    assert_show("base.show (base.the (Float 1) 42)", "42.0");
    assert_show("base.show ((base.the (Float 1) 42) + 0.5)", "42.5");
    // A lambda-param `*` still composes units (the literal is dimensionless).
    assert_show("base.show ((\\x -> x * 2) 21)", "42");
}

#[test]
fn not_and_id() {
    assert_show("base.id 5", "5");
    // not is a reserved keyword; Bool negation is via !=/match. base.id only here.
}