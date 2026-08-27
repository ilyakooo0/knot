//! Migration corner cases — the schema-transition matrix beyond the happy-path
//! lifecycle covered in `migration.rs`.
//!
//! Each test drives the full lifecycle: baseline v1 → `knot lock` → v2 with a
//! pending migration (runs on a fork) → `knot lock` → committed v2 (no clause)
//! that fast-forwards the main DB — then asserts the committed read. The point
//! is the committed read: a pending migration runs on a fork, so only the
//! committed replay proves the data actually landed.
//!
//! Coverage is organized by the kind of transition:
//!   - field shape: add / drop / rename / reorder
//!   - transform: identity, computed (constant), copy-through
//!   - nested data: relation-of-scalars, relation-of-records, relation-of-
//!     relations, record field, record field whose shape changes, payload-ADT
//!     field
//!   - source shape: record source, ADT source
//!   - data volume: empty relation, several rows
//!   - chains: multi-step A→B→C fast-forward
//!
//! Known migration-fn limitations (not bugs in storage — the lambda's
//! typecheck scope): arithmetic on a refined-Int field and `base.show` are
//! unavailable inside the migration lambda. These transitions are expressed
//! with copy/identity transforms instead.

mod e2e;
use e2e::{TempDir, build_in_dir, knot_bin, run_bin};

fn dir_for(name: &str) -> TempDir {
    TempDir::fresh(name)
}

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

fn build_and_run(dir: &TempDir, name: &str, src: &str) -> String {
    build_in_dir(name, src, dir.path());
    run_bin(&dir.join(name), dir.path())
}

/// The full lifecycle: v1 baseline → lock → pending v2 (fork) → lock →
/// committed v2 (no clause). Returns the committed run's stdout.
fn lifecycle(dir: &TempDir, name: &str, v1: &str, v2_pending: &str, v2_committed: &str) -> String {
    build_and_run(dir, name, v1);
    lock_in_dir(dir.path(), name);
    build_and_run(dir, name, v2_pending);
    lock_in_dir(dir.path(), name);
    build_and_run(dir, name, v2_committed)
}

// ── Field shape ──────────────────────────────────────────────────────────────

/// Add a field with a constant default.
#[test]
fn corner_add_field_constant_default() {
    let dir = dir_for("mig_add_field");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(do
  full *people = [{name "a"}  {name "b"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  active (Int 1)}
Rel V2  *people
  \p -> {name p.name  active 1}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  active (Int 1)}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_add_field", v1, v2, committed);
    assert_eq!(out, "\"[{active 1  name a}, {active 1  name b}]\"\n{}");
}

/// Drop a field; the surviving field's data is intact.
#[test]
fn corner_drop_field() {
    let dir = dir_for("mig_drop_field");
    let v1 = r#"with {
V1  {name Text  age (Int 1)}
Rel V1  *people
}
(do
  full *people = [{name "a"  age 5}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  age (Int 1)}
V2  {name Text}
Rel V2  *people
  \p -> {name p.name}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text  age (Int 1)}
V2  {name Text}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_drop_field", v1, v2, committed);
    assert_eq!(out, "\"[{name a}]\"\n{}");
}

/// Rename a field (the transform maps the old name onto the new one).
#[test]
fn corner_rename_field() {
    let dir = dir_for("mig_rename");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(do
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {fullName Text}
Rel V2  *people
  \p -> {fullName p.name}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {fullName Text}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rename", v1, v2, committed);
    assert_eq!(out, "\"[{fullName a}]\"\n{}");
}

/// Reorder fields; the transform spells the new field order explicitly.
#[test]
fn corner_reorder_fields() {
    let dir = dir_for("mig_reorder");
    let v1 = r#"with {
V1  {a Text  b (Int 1)}
Rel V1  *r
}
(do
  full *r = [{a "x"  b 1}]
  yield {})"#;
    let v2 = r#"with {
V1  {a Text  b (Int 1)}
V2  {b (Int 1)  a Text}
Rel V2  *r
  \p -> {b p.b  a p.a}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {a Text  b (Int 1)}
V2  {b (Int 1)  a Text}
Rel V2  *r
}
(do
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_reorder", v1, v2, committed);
    assert_eq!(out, "\"[{a x  b 1}]\"\n{}");
}

// ── Nested data ──────────────────────────────────────────────────────────────

/// A relation-of-scalars field survives a migration that adds a sibling field.
/// (Regression: the child element/link tables were orphaned on migration.)
#[test]
fn corner_relation_of_scalars_field() {
    let dir = dir_for("mig_rel_scalars");
    let v1 = r#"with {
V1  {name Text  tags (Rel Text)}
Rel V1  *people
}
(do
  full *people = [{name "a"  tags ["x"  "y"]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  tags (Rel Text)  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  tags p.tags  extra 0}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  tags (Rel Text)  extra (Int 1)}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rel_scalars", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  name a  tags [x, y]}]\"\n{}");
}

/// A relation-of-records field (the deepest nesting: element records in a
/// content-addressed child table) survives a migration.
#[test]
fn corner_relation_of_records_field() {
    let dir = dir_for("mig_rel_records");
    let v1 = r#"with {
Pet  {name Text  legs (Int 1)}
V1  {name Text  pets (Rel Pet)}
Rel V1  *people
}
(do
  full *people = [{name "a"  pets [{name "pup"  legs 4}]}]
  yield {})"#;
    let v2 = r#"with {
Pet  {name Text  legs (Int 1)}
V1  {name Text  pets (Rel Pet)}
V2  {name Text  pets (Rel Pet)  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  pets p.pets  extra 0}
}
(do
  yield {})"#;
    let committed = r#"with {
Pet  {name Text  legs (Int 1)}
V1  {name Text  pets (Rel Pet)}
V2  {name Text  pets (Rel Pet)  extra (Int 1)}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rel_records", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  name a  pets [{legs 4  name pup}]}]\"\n{}");
}

/// A relation-of-relations field (two nesting levels of child tables) survives.
#[test]
fn corner_relation_of_relations_field() {
    let dir = dir_for("mig_rel_rel");
    let v1 = r#"with {
V1  {name Text  grid (Rel (Rel (Int 1)))}
Rel V1  *r
}
(do
  full *r = [{name "a"  grid [[1  2]  [3]]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  grid (Rel (Rel (Int 1)))}
V2  {name Text  grid (Rel (Rel (Int 1)))  extra (Int 1)}
Rel V2  *r
  \p -> {name p.name  grid p.grid  extra 0}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text  grid (Rel (Rel (Int 1)))}
V2  {name Text  grid (Rel (Rel (Int 1)))  extra (Int 1)}
Rel V2  *r
}
(do
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rel_rel", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  grid [[1, 2], [3]]  name a}]\"\n{}");
}

/// An inline record field (a RecRef child table) survives unchanged.
#[test]
fn corner_record_field_unchanged() {
    let dir = dir_for("mig_rec_field");
    let v1 = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
Rel V1  *people
}
(do
  full *people = [{name "a"  addr {city "x"}}]
  yield {})"#;
    let v2 = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
V2  {name Text  addr Addr  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  addr p.addr  extra 0}
}
(do
  yield {})"#;
    let committed = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
V2  {name Text  addr Addr  extra (Int 1)}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rec_field", v1, v2, committed);
    assert_eq!(out, "\"[{addr {city x}  extra 0  name a}]\"\n{}");
}

/// An inline record field whose INNER shape changes (the child table is
/// rebuilt in the new shape). (Regression: RecRef child orphaned on migration.)
#[test]
fn corner_record_field_reshaped() {
    let dir = dir_for("mig_rec_reshape");
    let v1 = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
Rel V1  *people
}
(do
  full *people = [{name "a"  addr {city "x"}}]
  yield {})"#;
    let v2 = r#"with {
Addr  {city Text}
AddrV2  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
V2  {name Text  addr AddrV2}
Rel V2  *people
  \p -> {name p.name  addr {city p.addr.city  zip 0}}
}
(do
  yield {})"#;
    let committed = r#"with {
Addr  {city Text}
AddrV2  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
V2  {name Text  addr AddrV2}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rec_reshape", v1, v2, committed);
    assert_eq!(out, "\"[{addr {city x  zip 0}  name a}]\"\n{}");
}

/// A payload-ADT field (an AdtRef child table) survives a migration.
#[test]
fn corner_payload_adt_field() {
    let dir = dir_for("mig_adt_field");
    let v1 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {name Text  sh Shape}
Rel V1  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}]
  yield {})"#;
    let v2 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {name Text  sh Shape}
V2  {name Text  sh Shape  extra (Int 1)}
Rel V2  *es
  \p -> {name p.name  sh p.sh  extra 0}
}
(do
  yield {})"#;
    let committed = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {name Text  sh Shape}
V2  {name Text  sh Shape  extra (Int 1)}
Rel V2  *es
}
(do
  rows <- *es
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_adt_field", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  name a  sh Circle {radius 3}}]\"\n{}");
}

// ── Source shape ─────────────────────────────────────────────────────────────

/// A whole-ADT source migrates, transforming each constructor via a match.
#[test]
fn corner_adt_source_migration() {
    let dir = dir_for("mig_adt_source");
    let v1 = r#"with {
Ev  Ping {}  Msg {text Text}
Rel Ev  *evs
}
(do
  full *evs = [(Ev.Ping {})  (Ev.Msg {text "hi"})]
  yield {})"#;
    let v2 = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text  loud (Int 1)}
Rel EvV2  *evs
  \e -> (match (e)
    Ev.Ping {}  (EvV2.Ping {})
    Ev.Msg {text t}  (EvV2.Msg {text t  loud 0}))
}
(do
  yield {})"#;
    let committed = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text  loud (Int 1)}
Rel EvV2  *evs
}
(do
  rows <- *evs
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_adt_source", v1, v2, committed);
    assert_eq!(out, "\"2\"\n{}");
}

// ── Data volume ──────────────────────────────────────────────────────────────

/// An empty relation migrates cleanly (no rows to transform, no crash).
#[test]
fn corner_empty_relation() {
    let dir = dir_for("mig_empty");
    let v1 = r#"with {
V1  {a (Int 1)}
Rel V1  *r
}
(do
  yield {})"#;
    let v2 = r#"with {
V1  {a (Int 1)}
V2  {a (Int 1)  b (Int 1)}
Rel V2  *r
  \p -> {a p.a  b 0}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {a (Int 1)}
V2  {a (Int 1)  b (Int 1)}
Rel V2  *r
}
(do
  rows <- *r
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_empty", v1, v2, committed);
    assert_eq!(out, "\"0\"\n{}");
}

/// Several rows all survive; the transform applies to each.
#[test]
fn corner_multiple_rows() {
    let dir = dir_for("mig_multi");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(do
  full *people = [{name "a"}  {name "b"}  {name "c"}  {name "d"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  tag (Int 1)}
Rel V2  *people
  \p -> {name p.name  tag 7}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  tag (Int 1)}
Rel V2  *people
}
(do
  rows <- *people
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_multi", v1, v2, committed);
    assert_eq!(out, "\"4\"\n{}");
}

// ── Chains ───────────────────────────────────────────────────────────────────

/// A three-step chain A→B→C fast-forwards a stale DB in one run, applying each
/// step's transform in order (each adds a field derived from the prior).
#[test]
fn corner_multi_step_chain() {
    let dir = dir_for("mig_chain");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *r
}
(do
  full *r = [{name "x"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  b Text}
Rel V2  *r
  \p -> {name p.name  b p.name}
}
(do
  yield {})"#;
    let v3 = r#"with {
V1  {name Text}
V2  {name Text  b Text}
V3  {name Text  b Text  c Text}
Rel V3  *r
  \p -> {name p.name  b p.b  c p.b}
}
(do
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  b Text}
V3  {name Text  b Text  c Text}
Rel V3  *r
}
(do
  rows <- *r
  base.println (base.show rows)
  yield {})"#;

    // Lock v1, then v2, then v3 — but never RUN v2/v3's migration until the
    // committed v3 binary replays the whole chain on the stale v1 DB.
    build_and_run(&dir, "mig_chain", v1);
    lock_in_dir(dir.path(), "mig_chain");
    // Stage v2 (pending) and lock it without a committed run in between.
    std::fs::write(dir.join("mig_chain.knot"), v2).unwrap();
    lock_in_dir(dir.path(), "mig_chain");
    std::fs::write(dir.join("mig_chain.knot"), v3).unwrap();
    lock_in_dir(dir.path(), "mig_chain");
    // Now the committed v3 binary replays A→B→C on the stale v1 data.
    let out = build_and_run(&dir, "mig_chain", committed);
    assert_eq!(out, "\"[{b x  c x  name x}]\"\n{}");
}
