//! End-to-end regression tests for implicit dictionaries: `(^field : T) =>`
//! signature constraints.
//!
//! A `^`-constrained function is elaborated to take a hidden leading dictionary
//! record; at each full-arity callsite the compiler searches the lexical scope
//! for an in-scope record supplying `field` at the required type and splices it
//! as the leading argument. Resolution is per-callsite (so one function can
//! resolve to different records at different instantiations), and a `with`
//! frame shadowing the field takes precedence over outer records.
//!
//! Each test compiles a small Knot program with the real `knot` binary into a
//! scratch directory and asserts on its output.

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

fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_dict_{}_{}",
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

fn build_fails_with(test_name: &str, source: &str, needle: &str) {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_dict_fail_{}_{}",
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
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !out.status.success(),
        "build unexpectedly succeeded for {test_name}"
    );
    assert!(
        stderr.contains(needle) || stdout.contains(needle),
        "expected error containing {needle:?}, got:\nstdout: {stdout}\nstderr: {stderr}"
    );
    let _ = fs::remove_dir_all(&dir);
}

const CLAMP: &str = r#"
clamp : (^compare : a -> a -> Int 1) => a -> a -> a -> a
clamp = \lo hi x -> if ((^compare) x lo) < 0 then lo else if ((^compare) x hi) > 0 then hi else x
"#;

/// The `compare` record resolves per-callsite: Int literals pick the Int
/// dictionary, Text literals the Text one.
#[test]
fn resolves_per_callsite_int_and_text() {
    let (stdout, stderr, ok) = compile_and_run(
        "dict_per_callsite",
        &format!(
            r#"intOrd = {{compare (\a b -> if a > b then 1 else if a < b then (0 - 1) else 0)}}
textOrd = {{compare (\a b -> if a > b then 1 else if a < b then (0 - 1) else 0)}}
{CLAMP}
main = do
    println (show (with intOrd (clamp 0 10 42)))
    println (show (with textOrd (clamp "a" "m" "z")))
"#
        ),
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("\"10\""), "Int clamp:\n{stdout}");
    assert!(stdout.contains("\"m\""), "Text clamp:\n{stdout}");
}

/// A `with` frame binding `compare` shadows outer records — the descending
/// dictionary clamps 42 to the lower bound 0.
#[test]
fn with_frame_shadows_outer_dictionary() {
    let (stdout, stderr, ok) = compile_and_run(
        "dict_with_shadow",
        &format!(
            r#"intOrd = {{compare (\a b -> if a > b then 1 else if a < b then (0 - 1) else 0)}}
intOrdDesc = {{compare (\a b -> if a < b then 1 else if a > b then (0 - 1) else 0)}}
{CLAMP}
main = do
    println (show (with intOrd (clamp 0 10 42)))
    println (show (with intOrdDesc (clamp 0 10 42)))
"#
        ),
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("\"10\""), "ascending clamp:\n{stdout}");
    assert!(stdout.contains("\"0\""), "descending clamp:\n{stdout}");
}

/// A constrained function can be CALLED from another function's body at full
/// arity; the dictionary resolves at that callsite from the enclosing `with`.
/// (Passing a constrained function partially-applied — e.g. `map (clamp lo hi)`
/// — is not yet supported: only full-arity callsites resolve a dictionary.)
#[test]
fn constrained_call_inside_another_body() {
    let (stdout, stderr, ok) = compile_and_run(
        "dict_nested_call",
        &format!(
            r#"intOrd = {{compare (\a b -> if a > b then 1 else if a < b then (0 - 1) else 0)}}
{CLAMP}
clampHi = \lo hi x -> clamp lo hi x
main = do
    println (show (with intOrd (clampHi 0 10 42)))
"#
        ),
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    assert!(
        stdout.contains("\"10\""),
        "nested full-arity clamp call resolves the dict:\n{stdout}"
    );
}

/// No in-scope record supplying the constrained field is a compile error.
#[test]
fn missing_dictionary_is_an_error() {
    build_fails_with(
        "dict_missing",
        &format!(
            r#"{CLAMP}
main = do
    println (show (clamp 0 10 42))
"#
        ),
        "no in-scope record supplies an implicit dictionary field 'compare'",
    );
}
