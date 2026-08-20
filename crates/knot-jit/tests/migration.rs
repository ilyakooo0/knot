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
use e2e::{TempDir, build_in_dir, knot_bin, run_bin};

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
PersonV1  {name Text}
Rel PersonV1  *people
}
(do
  full *people = [{name "Alice"}  {name "Bob"}]
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
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  migrate  from PersonV1  to PersonV2  using \p -> {name p.name  active (Active.Yes {})}
}
(do
  base.println (base.show (base.count *people))
  yield {})"#,
    );
    assert_eq!(out2, "\"2\"\n{}");
}

#[test]
fn migrate_backfilled_adt_field_renders_constructor() {
    // Rows upgraded by `migrate` render their backfilled ADT field with its
    // constructor — `{active Yes name Alice}` — matching a directly-built
    // `Active.Yes {}`. This requires the `migrate` fn to actually run: the
    // codegen must emit `knot_source_migrate` for top-level `with`-block
    // sources (previously only record-embedded sources were collected, so the
    // migration never ran and `active` stayed empty).
    let dir = dir_for("mig_show");
    build_and_run(
        &dir,
        "mig_show",
        r#"with {
PersonV1  {name Text}
Rel PersonV1  *people
}
(do
  full *people = [{name "Alice"}]
  yield {})"#,
    );
    let out = build_and_run(
        &dir,
        "mig_show",
        r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  migrate  from PersonV1  to PersonV2  using \p -> {name p.name  active (Active.Yes {})}
}
(do
  people <- *people
  base.println (base.show people)
  yield {})"#,
    );
    // Fixed: the migrated ADT field renders its constructor.
    assert_eq!(out, "\"[{active Yes  name Alice}]\"\n{}");
}

#[test]
fn migrate_lockfile_roundtrip() {
    // Regression: a program with a `migrate` declaration, once run (writing
    // `prog.schema.lock`), must rebuild cleanly. The lockfile writer and
    // parser used to disagree on migration syntax (writer emitted
    // `migrate *name from …`, parser accepted only the nameless
    // `migrate from …` form, and rejected a multi-line clause), so the
    // lockfile couldn't be re-read. Fixed: writer emits the nameless form,
    // parser skips newlines after `migrate`.
    let dir = dir_for("mig_lock");
    let src = r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  migrate  from PersonV1  to PersonV2  using \p -> {name p.name  active (Active.Yes {})}
}
(do
  full *people = [{name "A"  active (Active.No {})}]
  base.println (base.show (base.count *people))
  yield {})"#;
    build_and_run(&dir, "mig_lock", src);
    assert!(
        dir.join("mig_lock.schema.lock").exists(),
        "lockfile written"
    );

    // Rebuild the same source: must succeed and still read the migrated row.
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_lock.knot"))
        .arg("-o")
        .arg(dir.join("mig_lock"))
        .output()
        .expect("knot build");
    assert!(
        build.status.success(),
        "rebuild failed (lockfile round-trip broke): {}",
        String::from_utf8_lossy(&build.stderr)
    );
    assert_eq!(run_bin(&dir.join("mig_lock"), dir.path()), "\"1\"\n{}");
}
