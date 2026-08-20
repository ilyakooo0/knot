//! Exhaustive scalar builtin tests: text, bytes, JSON, numbers, units.

mod harness;
use harness::{assert_compile_err, assert_show, assert_show_set};

// ── Text ─────────────────────────────────────────────────────────────────

#[test]
fn text_case() {
    assert_show("base.toUpper \"hello\"", "HELLO");
    assert_show("base.toLower \"HeLLo\"", "hello");
    assert_show("base.toAsciiUpper \"hello\"", "HELLO");
    assert_show("base.toAsciiLower \"HeLLo\"", "hello");
}

#[test]
fn text_length() {
    assert_show("base.length \"hello\"", "5");
    assert_show("base.byteLength \"héllo\"", "6"); // é is 2 UTF-8 bytes
}

#[test]
fn text_trim() {
    assert_show("base.trim \"  hi  \"", "hi");
    assert_show("base.trimAscii \"  hi  \"", "hi");
    assert_show("base.ltrimAscii \"  hi  \"", "hi  ");
    assert_show("base.rtrimAscii \"  hi  \"", "  hi");
}

#[test]
fn text_predicates() {
    // All three take the substring first, then the text.
    assert_show("base.contains \"ell\" \"hello\"", "True");
    assert_show("base.contains \"z\" \"hello\"", "False");
    assert_show("base.startsWith \"he\" \"hello\"", "True");
    assert_show("base.endsWith \"lo\" \"hello\"", "True");
}

#[test]
fn text_chars() {
    assert_show_set("base.chars \"abc\"", &["a", "b", "c"]);
}

#[test]
fn text_concat_op() {
    assert_show("\"foo\" ++ \"bar\"", "foobar");
}

#[test]
fn text_reverse() {
    assert_show("base.reverse \"abc\"", "cba");
}

// ── Numbers ──────────────────────────────────────────────────────────────

#[test]
fn int_arithmetic() {
    assert_show("7 - 10", "-3");
    assert_show("6 * 7", "42");
    assert_show("17 % 5", "2");
    assert_show("0 - 5", "-5"); // unary minus via binary
}

#[test]
fn int_division() {
    assert_show("17 / 5", "3"); // integer division truncates
}

#[test]
fn float_arithmetic() {
    assert_show("1.5 + 2.5", "4.0");
    assert_show("10.0 / 4.0", "2.5");
}

#[test]
fn abs_clamp_minmax() {
    assert_show("base.abs (0 - 5)", "5");
    assert_show("base.abs 5", "5");
    assert_show("base.intMin 3 7", "3");
    assert_show("base.intMax 3 7", "7");
    assert_show("base.clamp 0 10 42", "10");
    assert_show("base.clamp 0 10 (0 - 3)", "0");
    assert_show("base.clamp 0 10 5", "5");
}

#[test]
fn floor_and_widen() {
    assert_show("base.floor 3.7", "3");
    assert_show("base.floor (0.0 - 3.2)", "-4");
    assert_show("base.intToFloat 3", "3.0");
}

#[test]
fn parse_numbers() {
    assert_show("base.textToInt \"42\"", "Just {value 42}");
    assert_show("base.textToInt \"abc\"", "Nothing");
    assert_show("base.textToFloat \"2.5\"", "Just {value 2.5}");
}

#[test]
fn comparison_operators() {
    assert_show("3 < 4", "True");
    assert_show("3 > 4", "False");
    assert_show("3 <= 3", "True");
    assert_show("3 >= 4", "False");
    assert_show("3 == 3", "True");
    assert_show("3 != 3", "False");
}

#[test]
fn bool_logic() {
    assert_show("(3 > 2) && (1 > 0)", "True");
    assert_show("(3 > 2) && (1 > 5)", "False");
    assert_show("(3 < 2) || (1 > 0)", "True");
    // `not` is a reserved keyword and `base.not` is rejected too ("keyword
    // cannot be used as a variable name") — Boolean negation is the `!=`
    // operator / a `case` on Bool, not a callable builtin. Not asserted here.
}

// ── Bytes ────────────────────────────────────────────────────────────────

#[test]
fn bytes_roundtrip() {
    assert_show(
        "base.bytesToText (base.textToBytes \"hi\")",
        "Just {value hi}",
    );
}

#[test]
fn bytes_length_get() {
    assert_show("base.bytesLength (base.textToBytes \"abc\")", "3");
    // bytesGet is index-first: Int -> Bytes -> Maybe Int.
    assert_show(
        "base.bytesGet 0 (base.textToBytes \"abc\")",
        "Just {value 97}",
    );
    assert_show("base.bytesGet 9 (base.textToBytes \"abc\")", "Nothing");
}

#[test]
fn bytes_hex() {
    assert_show("base.bytesToHex (base.textToBytes \"ab\")", "6162");
    // bytesFromHex returns Maybe Bytes; the Bytes payload shows as bare hex.
    assert_show("base.bytesFromHex \"6162\"", "Just {value 6162}");
}

#[test]
fn bytes_concat_slice() {
    assert_show(
        "base.bytesToHex (base.bytesConcat (base.textToBytes \"a\") (base.textToBytes \"b\"))",
        "6162",
    );
    // bytesSlice is (start, end, bytes) but `end` is a LENGTH-ish bound that
    // is inclusive of the last index here: 1 3 over "hello" yields "ell".
    assert_show(
        "base.bytesToHex (base.bytesSlice 1 3 (base.textToBytes \"hello\"))",
        "656c6c",
    );
}

#[test]
fn bytes_invalid_utf8() {
    // 0xff is never valid UTF-8. bytesFromHex gives Maybe Bytes, so unwrap via
    // a case before decoding to Text.
    assert_show(
        "(case base.bytesFromHex \"ff\" of\n  Maybe.Just {value b} -> base.bytesToText b\n  Maybe.Nothing {} -> Maybe.Just {value \"unexpected\"})",
        "Nothing",
    );
}

// ── JSON ─────────────────────────────────────────────────────────────────

#[test]
fn json_scalars() {
    assert_show("base.toJson 42", "42");
    assert_show("base.toJson \"hi\"", "\"hi\"");
    // Bool is a primitive Value::Bool (not a tagged ADT), so the qualified
    // `Bool.True {}` serializes as a JSON boolean — same as a comparison
    // result. (`1 == 1` and `Bool.True {}` are the same runtime value.)
    assert_show("base.toJson (Bool.True {})", "true");
    assert_show("base.toJson (Bool.False {})", "false");
}

#[test]
fn json_record() {
    assert_show(
        "base.toJson {name \"a\" age 3}",
        "{\"age\":3,\"name\":\"a\"}",
    );
}

#[test]
fn json_parse_roundtrip() {
    assert_show(
        "(the (Maybe (Int 1)) (base.parseJson \"42\"))",
        "Just {value 42}",
    );
}

#[test]
fn json_parse_invalid() {
    assert_show("(the (Maybe (Int 1)) (base.parseJson \"{bad\"))", "Nothing");
}

// ── Units of measure ─────────────────────────────────────────────────────

#[test]
fn strip_with_unit() {
    assert_show("base.stripUnit (the (Int Ms) 250)", "250");
    assert_show("base.stripFloatUnit (the (Float Ms) 2.5)", "2.5");
}

#[test]
fn unit_arithmetic_consistent() {
    // Same-unit arithmetic keeps the unit; show renders just the number here
    // (dimensionless check), but the point is it type-checks.
    assert_show("(the (Int Ms) 100) + (the (Int Ms) 50)", "150");
}

#[test]
fn unit_mismatch_rejected() {
    // Adding incompatible units is a compile error.
    assert_compile_err(
        "with { unit Ms unit Usd } ((the (Int Ms) 100) + (the (Int Usd) 50))",
        "",
    );
}

// ── show rendering ───────────────────────────────────────────────────────

#[test]
fn show_nested_structure() {
    assert_show(
        "{point {x 1 y 2} tags [\"a\"  \"b\"]}",
        "{point {x 1 y 2} tags [a, b]}",
    );
}
