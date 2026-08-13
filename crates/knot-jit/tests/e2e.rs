//! Subprocess end-to-end harness: compile a knot program to a native binary
//! and run it, asserting on stdout/stderr. This is the path for tests the
//! in-process JIT can't fully evaluate — persisted source relations (`*rel`),
//! file IO, concurrency, HTTP servers, and anything whose observable behavior
//! is a process-level effect.
//!
//! Each test writes a `.knot` source to a temp dir, builds it with the
//! workspace `knot` binary, runs it there (so its `<name>.db` is isolated),
//! and inspects output.

// Shared across integration-test crates, each of which uses only a subset.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// A knot-e2e working dir that deletes itself (and the ~200MB binary inside)
/// on drop. Without this, repeated test runs fill the disk.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn fresh(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "knot_e2e_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir); // clear any stale same-pid dir
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

/// Locate the workspace `knot` compiler binary. Cargo sets
/// `CARGO_BIN_EXE_<name>` for binaries in the *same* package, but knot lives
/// in a sibling crate, so resolve it from the target dir.
pub fn knot_bin() -> PathBuf {
    // tests run with CWD = the crate dir (crates/knot-jit)
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // workspace root
    p.push("target");
    p.push("debug");
    p.push("knot");
    assert!(p.exists(), "knot binary not built at {}", p.display());
    p
}

/// Compile `src` to a binary in a fresh temp dir and run it, returning
/// (stdout, stderr, exit_code). The program runs with the temp dir as CWD so
/// any `<name>.db` it creates is isolated and removed on cleanup.
pub fn run_program(name: &str, src: &str) -> (String, String, i32) {
    let dir = TempDir::fresh(name); // removed (with the binary) on return
    let src_path = dir.join(format!("{name}.knot"));
    std::fs::write(&src_path, src).unwrap();
    let bin_path = dir.join(name);

    let build = Command::new(knot_bin())
        .arg("build")
        .arg(&src_path)
        .arg("-o")
        .arg(&bin_path)
        .output()
        .expect("failed to run knot build");
    assert!(
        build.status.success(),
        "knot build failed for {name}:\n{}",
        String::from_utf8_lossy(&build.stderr)
    );

    let run = Command::new(&bin_path)
        .current_dir(dir.path())
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&run.stdout).to_string();
    let stderr = String::from_utf8_lossy(&run.stderr).to_string();
    let code = run.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

/// Assert the program builds, exits 0, and its stdout (trimmed) equals
/// `expected_lines` joined by newlines.
pub fn assert_stdout(name: &str, src: &str, expected: &str) {
    let (out, err, code) = run_program(name, src);
    assert_eq!(code, 0, "program {name} exited {code}\nstderr:\n{err}");
    assert_eq!(
        out.trim_end(),
        expected.trim_end(),
        "stdout mismatch for {name}"
    );
}

/// Build a program without running it. The caller owns the returned TempDir —
/// run the binary (e.g. a long-lived HTTP server) and let the dir drop to
/// remove the binary.
pub fn build_program(name: &str, src: &str) -> (PathBuf, TempDir) {
    let dir = TempDir::fresh(name);
    build_in_dir(name, src, dir.path());
    (dir.join(name), dir)
}

/// Build `src` as `dir/name`, reusing `dir` (so a later binary shares the
/// same `<db>` files as an earlier one — for schema-evolution tests).
pub fn build_in_dir(name: &str, src: &str, dir: &Path) {
    let src_path = dir.join(format!("{name}.knot"));
    std::fs::write(&src_path, src).unwrap();
    let bin_path = dir.join(name);
    let build = Command::new(knot_bin())
        .arg("build")
        .arg(&src_path)
        .arg("-o")
        .arg(&bin_path)
        .output()
        .expect("failed to run knot build");
    assert!(
        build.status.success(),
        "knot build failed for {name}:\n{}",
        String::from_utf8_lossy(&build.stderr)
    );
}

/// Run an already-built binary in `dir`, returning trimmed stdout.
pub fn run_bin(bin: &Path, dir: &Path) -> String {
    let out = Command::new(bin)
        .current_dir(dir)
        .output()
        .expect("failed to run binary");
    assert!(
        out.status.success(),
        "binary {} exited {:?}\nstderr:\n{}",
        bin.display(),
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim_end().to_string()
}
