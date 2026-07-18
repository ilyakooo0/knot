//! Regression tests for a bug-hunt batch:
//!
//! 1. The `race`-in-`atomic` syntactic guard (`effects.rs`) seeded its
//!    shadowed-name set empty, ignoring enclosing binders in `self.shadowed`.
//!    A value named `race` bound in an enclosing scope and merely referenced
//!    inside an `atomic` block was wrongly rejected as the `race` primitive.
//! 2. Splitting `ElemPushdownOk` into `literal`/`dynamic` sets (to fix the
//!    dynamic `[Int]` `json_each` storage-class mismatch) must NOT regress the
//!    literal-list `elem` pushdown, which binds elements as TEXT and matches
//!    the TEXT-stored Int column correctly.
//!
//! Each test compiles a small Knot program with the real `knot` binary into its
//! own scratch directory and asserts on compilation / program output.

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

fn compile(test_name: &str, source: &str) -> Result<Compiled, String> {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_bughunt4_{}_{}",
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
    if !out.status.success() {
        return Err(format!(
            "knot build failed for {test_name}:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ));
    }
    let exe = dir.join("prog");
    Ok(Compiled { dir, exe })
}

fn compile_and_run(test_name: &str, source: &str) -> (String, String, bool) {
    let c = compile(test_name, source).unwrap_or_else(|e| panic!("{e}"));
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

// ── Finding 1: a shadowed `race` binder is allowed inside `atomic` ──

#[test]
fn shadowed_race_in_atomic_compiles() {
    // `race` is a lambda parameter (an ordinary value), merely referenced
    // inside a DB-only atomic block. This must compile — the enclosing binder
    // shadows the `race` primitive.
    let (stdout, stderr, ok) = compile_and_run(
        "shadowed_race_atomic",
        r#"*items : [{v: Int 1}]

store = \race ->
  atomic do
    cur <- *items
    replace *items = [{v: race}]

main = do
  store 7
  xs <- *items
  forEach xs (\x -> println ("v = " ++ show x.v))
  yield {}
"#,
    );
    assert!(
        ok,
        "shadowed `race` in atomic should compile & run:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("v = 7"),
        "expected the stored value, got:\n{stdout}"
    );
}

#[test]
fn genuine_race_in_atomic_still_rejected() {
    // Control: an UNshadowed `race` primitive inside `atomic` must still error,
    // so the shadowing fix didn't disable the guard entirely.
    let err = compile(
        "genuine_race_atomic",
        r#"*items : [{v: Int 1}]

bad = atomic do
  cur <- *items
  r <- race (println "a") (println "b")
  replace *items = [{v: 1}]

main = bad
"#,
    );
    let msg = match err {
        Ok(_) => panic!("an unshadowed `race` inside atomic must still be rejected"),
        Err(m) => m,
    };
    assert!(
        msg.contains("race") && msg.contains("atomic"),
        "expected a `race`-in-atomic diagnostic, got:\n{msg}"
    );
}

// ── Finding 2: literal-list `elem` pushdown over `[Int]` stays correct ──

#[test]
fn literal_int_elem_pushdown_matches() {
    // The literal `IN (?, ?, ?)` path binds each element as TEXT, matching the
    // TEXT-stored Int column. Splitting the pushdown gate must not regress it.
    let (stdout, stderr, ok) = compile_and_run(
        "literal_int_elem",
        r#"*rows : [{status: Int 1}]

query = do
  rs <- *rows
  yield (count (rs |> filter (\x -> elem x.status [1, 3, 10])))

main = do
  replace *rows = [{status: 1}, {status: 5}, {status: 10}]
  c <- query
  println ("matched: " ++ show c)
  yield {}
"#,
    );
    assert!(ok, "program failed:\nstdout: {stdout}\nstderr: {stderr}");
    // status 1 and 10 are in [1,3,10]; status 5 is not.
    assert!(
        stdout.contains("matched: 2"),
        "expected exactly two matches, got:\n{stdout}"
    );
}
