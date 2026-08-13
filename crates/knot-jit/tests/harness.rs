//! Shared helpers for the exhaustive JIT value test-suite.
//!
//! `compile_and_run` returns the forced `Value*` of a knot source string. For
//! pure programs we want to assert on the *value*, not just that compilation
//! succeeded, so these helpers run a snippet and render its result to a
//! `String` via the runtime's own `knot_value_show` (the same renderer
//! `base.show` uses), letting tests assert exact output.
//!
//! The test binary links `knot-runtime`, so `Value` and `knot_value_show` are
//! directly available.

// Each integration-test file is its own crate and uses only the subset of
// these shared helpers it needs, so any one crate sees the rest as "unused".
#![allow(dead_code)]

use knot_jit::compile_and_run;
use knot_runtime::Value;

pub fn init() {
    knot_compile_rt::knot_compile_rt_init()
}

/// Open a fresh in-memory db for a snippet.
pub fn db() -> *mut std::ffi::c_void {
    static PATH: &[u8] = b":memory:";
    knot_runtime::knot_db_open(PATH.as_ptr(), PATH.len())
}

/// Read a Rust `String` out of a `Value*` that must be a `Text`.
///
/// # Safety
/// Dereferences the raw `Value*` produced by the JIT. Only valid while the
/// host runtime arena is alive (it is, for the duration of the test) and only
/// for a non-tagged heap `Value`.
unsafe fn text_of(v: *mut Value) -> String {
    assert!(!v.is_null(), "value is null");
    // Tagged pointers encode leaf values inline; Text is never tagged, so a
    // Text result is always a real heap pointer we can deref.
    match unsafe { &*v } {
        Value::Text(s) => s.to_string(),
        _ => panic!("expected Text from knot_value_show, got a non-Text Value"),
    }
}

/// Compile + run `src` and return `base.show` of its value.
///
/// The snippet's *body* is the value under test; we wrap it so the program
/// evaluates to `base.show <body>`, yielding a `Text` we can read back.
pub fn eval_show(body: &str) -> String {
    init();
    let db = db();
    let src = format!("base.show ({body})");
    let out = compile_and_run(&src, db, None)
        .unwrap_or_else(|e| panic!("compile failed for `{body}`: {e}"));
    unsafe { text_of(out.value.cast()) }
}

/// Compile + run `src` (a full program, usually a `do` block ending in a
/// `yield`) and return `base.show` of the program's value.
pub fn eval_prog_show(program: &str) -> String {
    init();
    let db = db();
    let src = format!("base.show ({program})");
    let out = compile_and_run(&src, db, None)
        .unwrap_or_else(|e| panic!("compile failed for program: {e}\n---\n{program}"));
    unsafe { text_of(out.value.cast()) }
}

/// Assert that `body` evaluates to a value whose `show` equals `expected`.
pub fn assert_show(body: &str, expected: &str) {
    let got = eval_show(body);
    assert_eq!(got, expected, "for `{body}`");
}

/// Relations are unordered: two relations with the same rows may `show` in a
/// different order. `assert_show_set` splits the `[...]` top-level row list
/// and compares as a sorted set, so tests don't bake in hash order. Only for
/// flat relations of comma-free rows (records are fine — we split on `}, `).
pub fn assert_show_set(body: &str, expected_rows: &[&str]) {
    let got = eval_show(body);
    let inner = got
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or_else(|| panic!("expected a relation `[...]`, got {got}"));
    let mut rows: Vec<String> = if inner.trim().is_empty() {
        Vec::new()
    } else {
        // Rows are records `{...}` or scalars; split on ", " at depth 0.
        let mut out = Vec::new();
        let mut depth = 0i32;
        let mut cur = String::new();
        for c in inner.chars() {
            match c {
                '{' | '[' => depth += 1,
                '}' | ']' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                    continue;
                }
                _ => {}
            }
            cur.push(c);
        }
        if !cur.trim().is_empty() {
            out.push(cur.trim().to_string());
        }
        out
    };
    let mut want: Vec<String> = expected_rows.iter().map(|s| s.to_string()).collect();
    rows.sort();
    want.sort();
    assert_eq!(rows, want, "for `{body}` (set compare)");
}

/// Assert that a full `program` (a `do` block / comprehension) evaluates to a
/// value whose `show` equals `expected`.
pub fn assert_prog(program: &str, expected: &str) {
    let got = eval_prog_show(program);
    assert_eq!(got, expected, "for program:\n{program}");
}

/// Assert that `body` fails to compile (lex/parse/type/codegen error),
/// optionally requiring the error to mention `needle`.
pub fn assert_compile_err(body: &str, needle: &str) {
    init();
    let db = db();
    let src = format!("base.show ({body})");
    match compile_and_run(&src, db, None) {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                needle.is_empty() || msg.contains(needle),
                "error for `{body}` did not contain `{needle}`:\n{msg}"
            );
        }
        Ok(_) => panic!("expected compile error for `{body}`, but it compiled"),
    }
}
