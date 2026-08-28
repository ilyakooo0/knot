//! Single-value persistence: a source declared with a bare (non-`Rel`) type
//! (`Person *owner`, `Int 1 *counter`) persists ONE value across runs. Written
//! with `*name = expr` (no `full`), read back as the bare value via `*name`.
//! The stored value is type-annotated (the schema), so a rebuilt binary reads
//! it back typed.

mod e2e;
use e2e::{TempDir, build_in_dir, knot_bin, run_bin};

fn dir_for(name: &str) -> TempDir {
    TempDir::fresh(name)
}

/// Build `src` as `dir/<name>` and run it, returning trimmed stdout.
fn build_and_run(dir: &TempDir, name: &str, src: &str) -> String {
    build_in_dir(name, src, dir.path());
    run_bin(&dir.join(name), dir.path())
}

/// A record single value writes and reads in one run.
#[test]
fn single_value_record_write_read() {
    let dir = dir_for("sv_rec");
    let out = build_and_run(
        &dir,
        "sv_rec",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  *owner = {name "Alice"}
  base.println (base.show *owner)
  yield {})"#,
    );
    assert_eq!(out, "\"{name Alice}\"\n{}");
}

/// A scalar single value writes and reads.
#[test]
fn single_value_scalar_write_read() {
    let dir = dir_for("sv_int");
    let out = build_and_run(
        &dir,
        "sv_int",
        r#"with {
Int 1  *counter
}
(|
  *counter = 41
  base.println (base.show *counter)
  yield {})"#,
    );
    assert_eq!(out, "\"41\"\n{}");
}

/// An ADT single value writes and reads.
#[test]
fn single_value_adt_write_read() {
    let dir = dir_for("sv_adt");
    let out = build_and_run(
        &dir,
        "sv_adt",
        r#"with {
Status  Open {}  Closed {}
Status  *current
}
(|
  *current = (Status.Open {})
  base.println (base.show *current)
  yield {})"#,
    );
    assert_eq!(out, "\"Open\"\n{}");
}

/// Cross-run persistence: run 1 writes, run 2 (rebuilt, same schema) reads it.
#[test]
fn single_value_persists_across_runs() {
    let dir = dir_for("sv_x");
    build_and_run(
        &dir,
        "sv_x",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  *owner = {name "Alice"}
  yield {})"#,
    );
    let out = build_and_run(
        &dir,
        "sv_x",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  base.println (base.show *owner)
  yield {})"#,
    );
    assert_eq!(out, "\"{name Alice}\"\n{}", "the value persists across runs");
}

/// Overwrite: a second `*name = expr` replaces the value.
#[test]
fn single_value_overwrite() {
    let dir = dir_for("sv_ow");
    build_and_run(
        &dir,
        "sv_ow",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  *owner = {name "Alice"}
  yield {})"#,
    );
    build_and_run(
        &dir,
        "sv_ow",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  *owner = {name "Bob"}
  yield {})"#,
    );
    let out = build_and_run(
        &dir,
        "sv_ow",
        r#"with {
Person  {name Text}
Person  *owner
}
(|
  base.println (base.show *owner)
  yield {})"#,
    );
    assert_eq!(out, "\"{name Bob}\"\n{}", "the second write replaced the value");
}

/// A name declared as both a relation and a single value is a compile error.
#[test]
fn single_value_conflicts_with_relation() {
    let dir = dir_for("sv_conflict");
    let src = r#"with {
Person  {name Text}
Rel Person  *owner
Person  *owner
}
(|
  yield {})"#;
    std::fs::write(dir.join("sv_conflict.knot"), src).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("sv_conflict.knot"))
        .arg("-o")
        .arg(dir.join("sv_conflict"))
        .current_dir(dir.path())
        .output()
        .expect("build");
    assert!(
        !build.status.success(),
        "a name declared as both a relation and a single value must fail the build"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("declared more than once"),
        "the error names the duplicate declaration: {stderr}"
    );
}
