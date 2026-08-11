//! Schema evolution (`migrate from V1 to V2 using <fn>`).
//!
//! Two REAL, REPRODUCED bugs are documented by these tests (asserted against
//! current behavior, each marked). They are genuine defects, not semantics to
//! bless — see the inline comments.
//!
//! A v1 program persists rows under the old shape; a v2 program (SAME binary
//! name, so it shares `prog.db`) with a `migrate` declaration reads them back
//! upgraded.

mod e2e;
use e2e::{build_in_dir, knot_bin, run_bin, TempDir};

fn dir_for(name: &str) -> TempDir {
    TempDir::fresh(name)
}

/// Build `src` as `dir/<name>` and run it, returning trimmed stdout.
fn build_and_run(dir: &TempDir, name: &str, src: &str) -> String {
    build_in_dir(name, src, dir.path());
    run_bin(&dir.join(name), dir.path())
}

#[test]
fn migrate_backfills_new_field_count() {
    let dir = dir_for("mig_count");
    // v1: people have only a name. Rows persisted under PersonV1.
    let out1 = build_and_run(
        &dir,
        "mig_count",
        r#"with {
type PersonV1 = {name: Text}
*people : [PersonV1]
}
(do
  full *people = [{name "Alice"} {name "Bob"}]
  base.println (base.show (base.count *people))
  yield {})"#,
    );
    assert_eq!(out1, "\"2\"\n{}");

    // v2 (same binary name → same db): add `active`, backfill via migrate.
    // The migration RUNS: both legacy rows survive the upgrade.
    let out2 = build_and_run(
        &dir,
        "mig_count",
        r#"with {
data Active = Yes {} | No {}
type PersonV1 = {name: Text}
type PersonV2 = {name: Text, active: Active}
*people : [PersonV2]
  migrate from PersonV1 to PersonV2 using \p -> {name p.name active (Active.Yes {})}
}
(do
  base.println (base.show (base.count *people))
  yield {})"#,
    );
    assert_eq!(out2, "\"2\"\n{}");
}

#[test]
fn migrate_backfilled_adt_field_renders_empty() {
    // BUG (reproduced): rows upgraded by `migrate` show their backfilled ADT
    // field as EMPTY — `{active: , name: Alice}` — where a directly-built
    // `Active.Yes {}` renders `{active: Yes, name: x}`. The migration's ADT
    // reconstruction is broken (constructor not attached on read-back).
    let dir = dir_for("mig_show");
    build_and_run(
        &dir,
        "mig_show",
        r#"with {
type PersonV1 = {name: Text}
*people : [PersonV1]
}
(do
  full *people = [{name "Alice"}]
  yield {})"#,
    );
    let out = build_and_run(
        &dir,
        "mig_show",
        r#"with {
data Active = Yes {} | No {}
type PersonV1 = {name: Text}
type PersonV2 = {name: Text, active: Active}
*people : [PersonV2]
  migrate from PersonV1 to PersonV2 using \p -> {name p.name active (Active.Yes {})}
}
(do
  people <- full *people
  base.println (base.show people)
  yield {})"#,
    );
    // Current (buggy) behavior: `active:` has no constructor printed.
    assert_eq!(out, "\"[{active: , name: Alice}]\"\n{}");
}

#[test]
fn migrate_lockfile_roundtrip_broken() {
    // BUG (reproduced): after a program with a `migrate` declaration runs
    // once (writing `prog.schema.lock`), rebuilding the SAME source fails
    // with "parse errors in prog.schema.lock". Root cause: the lockfile is
    // WRITTEN as `migrate *people from PersonV1 to PersonV2 using …` but the
    // PARSER only accepts `migrate from … to … using …` (no `*rel` target),
    // so the writer and parser disagree on migration syntax and the lockfile
    // can't be re-read.
    let dir = dir_for("mig_lock");
    let src = r#"with {
data Active = Yes {} | No {}
type PersonV1 = {name: Text}
type PersonV2 = {name: Text, active: Active}
*people : [PersonV2]
  migrate from PersonV1 to PersonV2 using \p -> {name p.name active (Active.Yes {})}
}
(do
  full *people = [{name "A" active (Active.No {})}]
  base.println (base.show (base.count *people))
  yield {})"#;
    build_and_run(&dir, "mig_lock", src);
    assert!(dir.join("mig_lock.schema.lock").exists(), "lockfile written");

    // Rebuild the same source: must currently FAIL to build.
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_lock.knot"))
        .arg("-o")
        .arg(dir.join("mig_lock"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "expected rebuild to fail (lockfile round-trip bug), but it succeeded"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains(".schema.lock; delete it and recompile"),
        "unexpected rebuild error: {stderr}"
    );
}
