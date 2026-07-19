//! End-to-end regression tests for `show` on values carrying a unit of measure.
//!
//! DESIGN ("`show` and Units") specifies that `show` on a value with a concrete
//! unit appends the canonical unit string — `show (42.0 : Float M)` is `"42.0 M"` — and
//! prints just the number when the unit is polymorphic or absent. Units are
//! erased before runtime, so the unit reaches the emitted code only if type
//! inference resolves it per `show` call site and codegen emits it as a
//! constant. That path did not exist: every `show` compiled to a plain
//! `knot_value_show` and the suffix was silently dropped.
//!
//! Each test compiles a small Knot program with the real `knot` binary into its
//! own scratch directory (so `knot.db` lands there) and asserts on the output.

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

/// Compile and run `source` in a fresh scratch directory; returns stdout.
fn compile_and_run(test_name: &str, source: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_show_units_{}_{}",
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

    let c = Compiled { dir: dir.clone(), exe: dir.join("prog") };
    let run = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    assert!(
        run.status.success(),
        "program failed for {test_name}:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr),
    );
    String::from_utf8_lossy(&run.stdout).into_owned()
}

/// Assert the program printed exactly `expected` as one line.
///
/// `println` renders a `Text` with surrounding quotes, so `println (show x)`
/// emits `"42.0 M"` — quotes included. Matching the whole quoted line (rather
/// than a substring) pins the suffix down: a `show` that dropped its unit, or
/// added one it shouldn't, fails here.
fn assert_printed(stdout: &str, expected: &str) {
    let quoted = format!("\"{}\"", expected);
    assert!(
        stdout.lines().any(|l| l == quoted),
        "expected a line {quoted}, got:\n{stdout}"
    );
}

#[test]
fn show_appends_concrete_unit_suffix() {
    // The three examples DESIGN gives verbatim, plus an Int-carried unit.
    // Units live on types, so literals are annotated via `(x : Float M)`.
    let stdout = compile_and_run(
        "concrete",
        r#"
main = do
  println (show (42.0 : Float M))
  println (show (9.8 : Float (M / S^2)))
  println (show 3.14)
  println (show (1500 : Int Usd))
"#,
    );
    assert_printed(&stdout, "42.0 M");
    assert_printed(&stdout, "9.8 M/S^2");
    assert_printed(&stdout, "3.14");
    assert_printed(&stdout, "1500 Usd");
}

#[test]
fn show_appends_unit_computed_by_unit_algebra() {
    // The unit is not written at the `show` call site — it falls out of the
    // unit algebra on `/`. It must resolve to a concrete unit by the time the
    // post-inference pass reads the call site.
    let stdout = compile_and_run(
        "algebra",
        r#"main = with {distance (100.0 : Float M) time (4.0 : Float S)} (do println (show (distance / time)); println (show (2.5 : Float Speed)))
"#,
    );
    assert_printed(&stdout, "25.0 M/S");
    // Units need no declaration: `Speed` is an opaque named unit and shows as-is.
    assert_printed(&stdout, "2.5 Speed");
}

#[test]
fn show_omits_polymorphic_and_absent_units() {
    // Inside a unit-generic function the concrete unit is not known at the
    // `show` call site, so DESIGN says print just the number — a `<u>` suffix
    // would be meaningless. A plain `Float`/`Int` likewise has nothing to add.
    let stdout = compile_and_run(
        "polymorphic",
        r#"
describe : Float u -> Text
describe = \x -> show x

main = do
  println (describe (7.5 : Float M))
  println (show 7.5)
  println (show 42)
"#,
    );
    // No line anywhere may carry a unit: `describe`'s `show x` is compiled once,
    // for `∀u. Float u`, so appending "M" there would be wrong for every other
    // caller.
    assert!(
        !stdout.contains('M'),
        "polymorphic/dimensionless show must not print a unit:\n{stdout}"
    );
    assert_printed(&stdout, "7.5");
    assert_printed(&stdout, "42");
}
