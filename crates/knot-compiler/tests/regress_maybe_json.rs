//! End-to-end tests for the Maybe ↔ JSON `null` convention:
//!
//! - Wire encoding (toJson, HTTP bodies): `Nothing` serializes as `null`,
//!   `Just x` serializes as `x`'s JSON (no `__knot_ctor` wrapper).
//! - Wire decoding (parseJson with an inferable target type): `null` and
//!   absent fields decode to `Nothing`, present values are `Just`-wrapped.
//! - SQLite storage is unaffected: Maybe values in JSON columns keep the
//!   `__knot_ctor` marker so existing databases round-trip.
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
        "knot_regress_mj_{}_{}",
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

// ── toJson: Nothing → null, Just x → x ──────────────────────────────

#[test]
fn to_json_maybe_encodes_null_and_bare_value() {
    let (stdout, stderr, ok) = compile_and_run(
        "tojson_maybe",
        r#"main = do
  println (toJson {name: "a", nick: Just {value: "x"}})
  println (toJson {name: "b", nick: Nothing {}})
  println (toJson (Just {value: 42}))
  println (toJson (Nothing {}))
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains(r#"{"name":"a","nick":"x"}"#),
        "Just must serialize as the bare inner value:\n{stdout}"
    );
    assert!(
        stdout.contains(r#"{"name":"b","nick":null}"#),
        "Nothing must serialize as null:\n{stdout}"
    );
    assert!(stdout.contains("\"42\""), "top-level Just 42 → 42:\n{stdout}");
    assert!(stdout.contains("\"null\""), "top-level Nothing → null:\n{stdout}");
    assert!(
        !stdout.contains("__knot_ctor"),
        "Maybe must not leak the internal constructor marker:\n{stdout}"
    );
}

// ── parseJson: null/absent → Nothing, present → Just ────────────────

#[test]
fn parse_json_maybe_decodes_null_absent_and_wraps_present() {
    let (stdout, stderr, ok) = compile_and_run(
        "parsejson_maybe",
        r#"type Person = {name: Text, nick: Maybe Text}

showPerson = \p -> case p.nick of
  Nothing {} -> println (p.name ++ ": none")
  Just {value} -> println (p.name ++ ": " ++ value)

showResult = \m -> case m of
  Nothing {} -> println "parse failed"
  Just {value} -> showPerson value

main = do
  showResult (parseJson "{\"name\":\"a\",\"nick\":null}" : Maybe Person)
  showResult (parseJson "{\"name\":\"b\"}" : Maybe Person)
  showResult (parseJson "{\"name\":\"c\",\"nick\":\"hey\"}" : Maybe Person)
  showResult (parseJson "not valid json!" : Maybe Person)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("a: none"), "null field must decode to Nothing:\n{stdout}");
    assert!(stdout.contains("b: none"), "absent field must decode to Nothing:\n{stdout}");
    assert!(
        stdout.contains("c: hey"),
        "present field must be Just-wrapped:\n{stdout}"
    );
    assert!(
        stdout.contains("parse failed"),
        "parseJson returns Maybe — malformed input must decode to Nothing:\n{stdout}"
    );
}

#[test]
fn parse_json_maybe_in_relation_rows() {
    let (stdout, stderr, ok) = compile_and_run(
        "parsejson_maybe_rel",
        r#"showRow = \r -> case r.a of
  Nothing {} -> println "row: none"
  Just {value} -> println ("row: " ++ show value)

main = do
  case parseJson "[{\"a\":null},{\"a\":7}]" : Maybe [{a: Maybe Int}] of
    Nothing {} -> println "parse failed"
    Just {value} -> forEach value showRow
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("row: none"), "{stdout}");
    assert!(stdout.contains("row: 7"), "{stdout}");
}

// ── toJson → parseJson round-trip ───────────────────────────────────

#[test]
fn maybe_to_json_parse_json_round_trip() {
    let (stdout, stderr, ok) = compile_and_run(
        "maybe_roundtrip",
        r#"type Person = {name: Text, nick: Maybe Text}

showPerson = \p -> case p.nick of
  Nothing {} -> println (p.name ++ ": none")
  Just {value} -> println (p.name ++ ": " ++ value)

showResult = \m -> case m of
  Nothing {} -> println "parse failed"
  Just {value} -> showPerson value

main = do
  showResult (parseJson (toJson {name: "a", nick: Just {value: "x"}}) : Maybe Person)
  showResult (parseJson (toJson {name: "b", nick: Nothing {}}) : Maybe Person)
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("a: x"), "Just must survive the round-trip:\n{stdout}");
    assert!(stdout.contains("b: none"), "Nothing must survive the round-trip:\n{stdout}");
}

// ── SQLite storage keeps the __knot_ctor marker ─────────────────────

#[test]
fn maybe_db_storage_keeps_marker_format() {
    let c = compile(
        "maybe_db_marker",
        r#"*items : [{n: Int, status: Maybe Text}]

showRow = \r -> case r.status of
  Nothing {} -> println (show r.n ++ ": none")
  Just {value} -> println (show r.n ++ ": " ++ value)

main = do
  replace *items = [{n: 1, status: Just {value: "active"}}, {n: 2, status: Nothing {}}]
  rows <- *items
  forEach rows showRow
"#,
    );
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "program failed:\n{stdout}");
    assert!(stdout.contains("1: active"), "{stdout}");
    assert!(stdout.contains("2: none"), "{stdout}");

    // The raw SQLite file must hold the storage encoding (constructor
    // marker), NOT the wire encoding — the schema-less DB read path can't
    // Just-wrap bare values, and existing databases hold the marker.
    let db_bytes = fs::read(c.dir.join("prog.db")).expect("prog.db must exist");
    let needle = b"__knot_ctor";
    let found = db_bytes
        .windows(needle.len())
        .any(|w| w == needle);
    assert!(
        found,
        "Maybe values in SQLite JSON columns must keep the __knot_ctor marker"
    );
}
