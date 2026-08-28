//! Deep Text and Bytes edge cases: contains/startsWith/endsWith, index/get,
//! concat/slice, hex round-trip, byteLength vs length (unicode), toJson of
//! nested structures, and split/join-like operations.

mod harness;
use harness::assert_show;

// ── Text predicates ────────────────────────────────────────────────────────

#[test]
fn text_predicates() {
    // (needle, haystack) order
    assert_show("base.contains \"wor\" \"hello world\"", "True");
    assert_show("base.contains \"xyz\" \"hello\"", "False");
    assert_show("base.startsWith \"he\" \"hello\"", "True");
    assert_show("base.endsWith \"lo\" \"hello\"", "True");
    assert_show("base.startsWith \"lo\" \"hello\"", "False");
}

#[test]
fn text_empty_edge_cases() {
    assert_show("base.length \"\"", "0");
    // Argument order is (needle, haystack). Empty needle is always found; an
    // empty haystack contains/has-as-prefix only the empty needle.
    assert_show("base.contains \"\" \"abc\"", "True"); // empty needle
    assert_show("base.contains \"abc\" \"\"", "False"); // empty haystack
    assert_show("base.startsWith \"\" \"abc\"", "True"); // empty needle is a prefix
    assert_show("base.startsWith \"abc\" \"\"", "False");
    assert_show("base.endsWith \"\" \"abc\"", "True");
}

// ── Bytes ──────────────────────────────────────────────────────────────────

#[test]
fn bytes_hex_roundtrip() {
    assert_show("base.bytesToHex (base.textToBytes \"hi\")", "6869");
    // hexDecode returns Maybe Bytes (Nothing on malformed) — unwrap to round-trip
    assert_show(
        "? (base.hexDecode (base.bytesToHex (base.textToBytes \"hi\")))
           Maybe.Just {value b}  base.bytesToText b
           Maybe.Nothing {}  Maybe.Nothing {}",
        "Just {value hi}",
    );
}

#[test]
fn bytes_length_vs_text_length() {
    // byteLength counts UTF-8 bytes; length counts chars. 'é' is 1 char, 2 bytes.
    assert_show("base.length \"é\"", "1");
    assert_show("base.byteLength \"é\"", "2");
    assert_show("base.bytesLength (base.textToBytes \"é\")", "2");
}

#[test]
fn bytes_concat_and_slice() {
    assert_show(
        "base.bytesToHex (base.bytesConcat (base.textToBytes \"ab\") (base.textToBytes \"cd\"))",
        "61626364",
    );
    // bytesSlice start len
    assert_show(
        "base.bytesToText (base.bytesSlice 1 2 (base.textToBytes \"abcd\"))",
        "Just {value bc}",
    );
}

#[test]
fn bytes_from_hex() {
    assert_show("base.bytesFromHex \"zz\"", "Nothing");
    // bytesFromHex returns Maybe Bytes; show wraps it
    assert_show("base.bytesFromHex \"6162\"", "Just {value 6162}");
}

// ── toJson of nested / special ─────────────────────────────────────────────

#[test]
fn tojson_nested_records_and_lists() {
    assert_show(
        "base.toJson {a [1  2]  b {c \"x\"}}",
        "{\"a\":[1,2],\"b\":{\"c\":\"x\"}}",
    );
}

#[test]
fn tojson_bool_is_primitive() {
    // Bool is a primitive Value::Bool, not a tagged ADT. The qualified
    // `Bool.True {}` and a comparison result are the SAME runtime value, so
    // both serialize as a JSON boolean (not a `__knot_ctor` envelope).
    assert_show("base.toJson (Bool.True {})", "true");
    assert_show("base.toJson (Bool.False {})", "false");
    assert_show("base.toJson (1 == 1)", "true");
}

#[test]
fn tojson_escapes_strings() {
    assert_show("base.toJson \"a\\\"b\"", "\"a\\\"b\"");
    assert_show("base.toJson \"line\\nbreak\"", "\"line\\nbreak\"");
}

#[test]
fn tojson_maybe_nothing() {
    assert_show("base.toJson (Maybe.Nothing {})", "null");
    assert_show("base.toJson (Maybe.Just {value 5})", "5");
}

// ── take/drop/chars on edge ────────────────────────────────────────────────

#[test]
fn take_drop_beyond_length() {
    assert_show("base.take 10 \"abc\"", "abc");
    assert_show("base.drop 10 \"abc\"", "");
    assert_show("base.take 0 \"abc\"", "");
}

#[test]
fn chars_of_text() {
    assert_show("base.chars \"ab\"", "[a, b]");
    assert_show("base.chars \"\"", "[]");
    assert_show("base.count (base.chars \"héllo\")", "5");
}