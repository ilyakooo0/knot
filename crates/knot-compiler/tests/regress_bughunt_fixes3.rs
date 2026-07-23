//! Regression tests for a bug-hunt batch (compiler side):
//!
//! 1. `set` on a *scalar* source emitted a spurious `r *rel` read effect, so an
//!    honest `{w *rel}` signature was rejected and had to be widened to
//!    `{rw *rel}`. Both the type-level (`infer`) and the effect-checker
//!    (`effects`) paths carried the defect.
//! 2. Stratification's `diff`-negation detector only matched a bare
//!    `Var("diff")` head, so a self-negating recursive derived relation written
//!    through a local alias (`let d = diff`) or a transparent wrapper
//!    (`(diff : T)`) escaped the check and oscillated at runtime instead of
//!    being rejected at compile time.
//!
//! Each test drives the real `knot` binary over a scratch directory.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct Scratch {
    dir: PathBuf,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

impl Scratch {
    fn new(test_name: &str) -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "knot_regress_bughunt3_{}_{}",
            test_name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch { dir }
    }

    fn write(&self, name: &str, source: &str) {
        fs::write(self.dir.join(name), source).unwrap();
    }

    /// Run `knot build <entry>`; returns (ok, stdout, stderr).
    fn build(&self, entry: &str) -> (bool, String, String) {
        let knot = env!("CARGO_BIN_EXE_knot");
        let out = Command::new(knot)
            .arg("build")
            .arg(self.dir.join(entry))
            .current_dir(&self.dir)
            .output()
            .expect("failed to spawn knot compiler");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// Run a previously-built executable; returns its stdout.
    fn run(&self, exe: &str) -> String {
        let out = Command::new(self.dir.join(exe))
            .current_dir(&self.dir)
            .output()
            .expect("failed to run compiled program");
        assert!(
            out.status.success(),
            "program exited non-zero:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

// ── Finding 2: `set` on a scalar source is write-only ────────────────────

#[test]
fn scalar_set_is_write_only() {
    let s = Scratch::new("scalar_set");
    // A pure overwrite of a scalar source references nothing, so `bump` must
    // type-check with an honest `{w *counter}` signature (no spurious read).
    s.write(
        "prog.knot",
        r#"*counter : Int 1

bump : IO {w *counter} {}
bump = do
  *counter = 5
  yield {}

main = do
  bump
  c <- *counter
  println (show c)
  yield {}
"#,
    );
    let (ok, stdout, stderr) = s.build("prog.knot");
    assert!(
        ok,
        "scalar `set` with `{{w *counter}}` should compile.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(s.run("prog").contains('5'), "expected the counter value 5 in output");
}

#[test]
fn scalar_read_modify_write_still_needs_read() {
    // Control for Finding 2: when the value DOES read the scalar (via a bind),
    // the read effect must still be required — a `{w}`-only signature fails.
    let s = Scratch::new("scalar_rmw");
    s.write(
        "prog.knot",
        r#"*counter : Int 1

bump : IO {w *counter} {}
bump = do
  c <- *counter
  *counter = c + 1
  yield {}

main = do
  bump
  yield {}
"#,
    );
    let (ok, _stdout, stderr) = s.build("prog.knot");
    assert!(!ok, "read-modify-write annotated `{{w}}`-only must fail");
    assert!(
        stderr.contains("r *counter"),
        "expected a missing `r *counter` effect error, got:\n{stderr}"
    );
}

// ── Finding 3: stratification sees aliased / wrapped `diff` ───────────────

fn diff_body(diff_expr: &str) -> String {
    format!(
        "type Item = {{v: Int 1}}\n*items : [Item]\n\n&bad = do\n  self <- &bad\n  all <- *items\n{diff_expr}\n  yield result\n\nmain = do\n  rows <- &bad\n  println (show (count rows))\n  yield {{}}\n"
    )
}

#[test]
fn stratify_detects_aliased_diff_self_negation() {
    let s = Scratch::new("strat_alias");
    s.write(
        "prog.knot",
        &diff_body("  result <- with {d diff} (do\n    r <- d all self\n    yield r)"),
    );
    let (ok, _stdout, stderr) = s.build("prog.knot");
    assert!(!ok, "aliased-`diff` self-negation must be rejected");
    assert!(
        stderr.contains("unstratifiable recursion"),
        "expected a stratification error, got:\n{stderr}"
    );
}

#[test]
fn stratify_detects_annotated_diff_self_negation() {
    let s = Scratch::new("strat_annot");
    s.write(
        "prog.knot",
        &diff_body("  result <- (diff : [Item] -> [Item] -> [Item]) all self"),
    );
    let (ok, _stdout, stderr) = s.build("prog.knot");
    assert!(!ok, "annotated-`diff` self-negation must be rejected");
    assert!(
        stderr.contains("unstratifiable recursion"),
        "expected a stratification error, got:\n{stderr}"
    );
}
