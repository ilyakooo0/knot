//! Schema evolution under the schema-lock-owned migration model.
//!
//! The lock (`<name>.schema.lock`) is the append-only, build-input owner of
//! migration history. Source holds only the current schema plus at most one
//! pending migration clause — a bare `\old -> …` lambda under the `Rel` decl
//! (the old schema is derived from the lock).
//! `knot lock` snapshots schemas and promotes pending migrations into the
//! lock, stripping the clause from source. A binary carrying an uncommitted
//! migration opens a content-hashed FORK of the database, never touching the
//! main DB; `knot lock` commits and the next run fast-forwards the main DB.

mod e2e;
use e2e::{TempDir, build_in_dir, knot_bin, run_bin};

fn dir_for(name: &str) -> TempDir {
    TempDir::fresh(name)
}

/// Run `knot lock <file>` in `dir`, asserting success.
fn lock_in_dir(dir: &std::path::Path, name: &str) {
    let out = std::process::Command::new(knot_bin())
        .arg("lock")
        .arg(dir.join(format!("{name}.knot")))
        .output()
        .expect("knot lock");
    assert!(
        out.status.success(),
        "knot lock failed for {name}:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Build `src` as `dir/<name>` and run it, returning trimmed stdout.
fn build_and_run(dir: &TempDir, name: &str, src: &str) -> String {
    build_in_dir(name, src, dir.path());
    run_bin(&dir.join(name), dir.path())
}

const V1: &str = r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
Rel PersonV1  *people
}
(do
  full *people = [{name "Alice"}]
  yield {})"#;

const V2_PENDING: &str = r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  \p -> {name p.name  active (Active.Yes {})}
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;

/// The full lifecycle: lock a v1 baseline, change the schema with a pending
/// migration, run it on a fork (main untouched), then `knot lock` and run the
/// committed binary, which fast-forwards the main DB.
#[test]
fn migrate_lock_lifecycle() {
    let dir = dir_for("mig_life");
    // v1: build, run (creates main DB with Alice), lock the baseline.
    build_and_run(&dir, "mig_life", V1);
    lock_in_dir(dir.path(), "mig_life");
    let main_before = std::fs::read(dir.join("mig_life.db")).unwrap();

    // v2 with a pending migration runs on a FORK: migrated rows visible, but
    // the main DB file is byte-identical and a fork file appears.
    let out = build_and_run(&dir, "mig_life", V2_PENDING);
    assert_eq!(out, "\"[{active Yes  name Alice}]\"\n{}");
    assert_eq!(
        std::fs::read(dir.join("mig_life.db")).unwrap(),
        main_before,
        "uncommitted run must not touch the main DB"
    );
    let forks: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".fork-"))
        .collect();
    assert_eq!(forks.len(), 1, "exactly one fork for the pending migration");

    // `knot lock` commits: the migrate clause is stripped from source and the
    // lock records the migration. Rebuild + run fast-forwards the MAIN DB.
    lock_in_dir(dir.path(), "mig_life");
    let stripped = std::fs::read_to_string(dir.join("mig_life.knot")).unwrap();
    assert!(
        !stripped.contains("migrate"),
        "knot lock strips the migrate clause from source"
    );
    let lock = std::fs::read_to_string(dir.join("mig_life.schema.lock")).unwrap();
    assert!(lock.contains("migrate_history"), "lock records the history");
    assert!(lock.contains("{name Text}"), "lock records the from-schema");

    // The committed binary no longer carries the pending migration; running it
    // migrates the main DB (no fork token) — but the source no longer has the
    // migrate clause, so this run is a no-op over the already-locked schema.
    let out2 = build_and_run(&dir, "mig_life", &stripped);
    assert_eq!(out2, "\"[{active Yes  name Alice}]\"\n{}");
}

/// An uncommitted run forks; re-running the SAME pending migration reuses the
/// fork; EDITING the `using` fn (same target schema) forks fresh.
#[test]
fn migrate_fork_identity() {
    let dir = dir_for("mig_forkid");
    build_and_run(&dir, "mig_forkid", V1);
    lock_in_dir(dir.path(), "mig_forkid");

    let fork_count = |dir: &TempDir| {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".fork-"))
            .count()
    };

    build_and_run(&dir, "mig_forkid", V2_PENDING);
    assert_eq!(fork_count(&dir), 1);
    // Same pending content → the existing fork is reused, no new fork.
    build_and_run(&dir, "mig_forkid", V2_PENDING);
    assert_eq!(fork_count(&dir), 1);
    // Edit ONLY the using fn (Active.Yes → Active.No, same target schema) → a
    // fresh fork.
    let edited = V2_PENDING.replace("Active.Yes {}", "Active.No {}");
    let out = build_and_run(&dir, "mig_forkid", &edited);
    assert_eq!(out, "\"[{active No  name Alice}]\"\n{}");
    assert_eq!(fork_count(&dir), 2, "edited using-fn forks fresh");
}

/// Building a source whose schema drifted from the lock without a migrate
/// block is a compile error; a pending migration produces the uncommitted
/// warning instead.
#[test]
fn migrate_drift_requires_block() {
    let dir = dir_for("mig_drift");
    build_and_run(&dir, "mig_drift", V1);
    lock_in_dir(dir.path(), "mig_drift");

    // Schema changed (added `active`) but NO migrate block → build fails.
    let no_block = r#"with {
Active  Yes {}  No {}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
}
(do
  yield {})"#;
    std::fs::write(dir.join("mig_drift.knot"), no_block).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_drift.knot"))
        .arg("-o")
        .arg(dir.join("mig_drift"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "schema drift without a migrate block must fail"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("requires a migrate block"),
        "error names the missing migrate block: {stderr}"
    );

    // With the pending block the build succeeds and warns (uncommitted).
    std::fs::write(dir.join("mig_drift.knot"), V2_PENDING).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_drift.knot"))
        .arg("-o")
        .arg(dir.join("mig_drift"))
        .output()
        .expect("knot build");
    assert!(build.status.success());
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("uncommitted migration"),
        "pending migration warns: {stderr}"
    );
}

/// A pending migration whose `using` fn produces the wrong shape is a
/// compile-time type error (`Old -> New`), not a runtime abort after the
/// migration starts writing.
#[test]
fn migrate_using_shape_checked() {
    let dir = dir_for("mig_using");
    build_and_run(&dir, "mig_using", V1);
    lock_in_dir(dir.path(), "mig_using");

    // The `using` fn yields `{wrongfield}` — not the target `{name, active}`.
    let bad_using = r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  \p -> {wrongfield p.name}
}
(do
  yield {})"#;
    std::fs::write(dir.join("mig_using.knot"), bad_using).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_using.knot"))
        .arg("-o")
        .arg(dir.join("mig_using"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "a wrong-shape using fn must fail the build"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("using") && stderr.contains("migration"),
        "error names the migration's using fn: {stderr}"
    );

    // Reading a field the old row lacks is also caught at build time.
    let bad_read = r#"with {
Active  Yes {}  No {}
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
  \p -> {name p.nonexistent  active (Active.Yes {})}
}
(do
  yield {})"#;
    std::fs::write(dir.join("mig_using.knot"), bad_read).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_using.knot"))
        .arg("-o")
        .arg(dir.join("mig_using"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "reading a field absent from the old row must fail the build"
    );
}

/// Deleting a data type that a committed migration's `using` fn still
/// references is caught at lock-check time with a clear error naming the type
/// — not a misleading codegen "constructor must be applied to a record".
#[test]
fn migrate_committed_using_dangling_type() {
    let dir = dir_for("mig_dangle");
    build_and_run(&dir, "mig_dangle", V1);
    lock_in_dir(dir.path(), "mig_dangle");
    build_in_dir("mig_dangle", V2_PENDING, dir.path());
    lock_in_dir(dir.path(), "mig_dangle");

    // Delete the `Active` data type entirely — but the committed migration's
    // `using` fn (`Active.Yes {}`) still references it.
    let deleted = r#"with {
PersonV1  {name Text}
PersonV2  {name Text  active Active}
Rel PersonV2  *people
}
(do
  yield {})"#;
    std::fs::write(dir.join("mig_dangle.knot"), deleted).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_dangle.knot"))
        .arg("-o")
        .arg(dir.join("mig_dangle"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "deleting a type a committed migration references must fail the build"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("references data type 'Active'"),
        "error names the deleted type: {stderr}"
    );
    assert!(
        !stderr.contains("must be applied to a record"),
        "must not fall through to the misleading codegen message: {stderr}"
    );
}

/// A source may stage at most one pending migration (its `from` is derived from
/// the lock, which is unambiguous only for a single step). Two `migrate` clauses
/// on one source are a build error — and `knot lock` rejects them too (both parse
/// the same source through the same diagnostic).
#[test]
fn multiple_pending_migrations_on_one_source_is_an_error() {
    let dir = dir_for("mig_two_pending");
    let two = r#"with {
V1  {name Text}
V2  {name Text  active Text}
V3  {name Text  active Text  extra Text}
Rel V3  *people
  \p -> {name p.name  active "yes"}
  \p -> {name p.name  active p.active  extra "x"}
}
(do
  yield {})"#;
    std::fs::write(dir.join("mig_two_pending.knot"), two).unwrap();

    // knot build
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_two_pending.knot"))
        .arg("-o")
        .arg(dir.join("mig_two_pending"))
        .output()
        .expect("knot build");
    assert!(!build.status.success(), "two pending migrations must fail the build");
    assert!(
        String::from_utf8_lossy(&build.stderr).contains("more than one pending migration clause"),
        "build error names the problem: {}",
        String::from_utf8_lossy(&build.stderr)
    );

    // knot lock
    let lock = std::process::Command::new(knot_bin())
        .arg("lock")
        .arg(dir.join("mig_two_pending.knot"))
        .output()
        .expect("knot lock");
    assert!(!lock.status.success(), "two pending migrations must fail the lock");
    assert!(
        String::from_utf8_lossy(&lock.stderr).contains("more than one pending migration clause"),
        "lock error names the problem: {}",
        String::from_utf8_lossy(&lock.stderr)
    );
}

/// A revert chain (`A → B → A`) revisits a schema. The migration cursor
/// (`applied` step count) makes position explicit, so the committed binary
/// replays the chain once on a stale DB and is a no-op on subsequent runs —
/// the same schema appearing twice does not confuse resumability.
#[test]
fn migrate_revert_chain_is_idempotent() {
    let dir = dir_for("mig_revert");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(do
  full *people = [{name "alice"}  {name "bob"}]
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    // Baseline.
    build_and_run(&dir, "mig_revert", v1);
    lock_in_dir(dir.path(), "mig_revert");

    // Up to V2, lock.
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  active Text}
Rel V2  *people
  \p -> {name p.name  active "UP"}
}
(do
  yield {})"#;
    build_and_run(&dir, "mig_revert", v2);
    lock_in_dir(dir.path(), "mig_revert");

    // Revert to V1, lock.
    let v1_revert = r#"with {
V1  {name Text}
V2  {name Text  active Text}
Rel V1  *people
  \p -> {name p.name}
}
(do
  yield {})"#;
    build_and_run(&dir, "mig_revert", v1_revert);
    lock_in_dir(dir.path(), "mig_revert");

    // Committed binary (no pending clause): first run replays the chain on the
    // stale main DB, second run is a no-op. Both must show the reverted data.
    let committed = r#"with {
V1  {name Text}
V2  {name Text  active Text}
Rel V1  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out1 = build_and_run(&dir, "mig_revert", committed);
    assert_eq!(out1, "\"[{name alice}, {name bob}]\"\n{}", "first run lands on reverted schema");
    let out2 = build_and_run(&dir, "mig_revert", committed);
    assert_eq!(out2, out1, "second run is idempotent");
}
