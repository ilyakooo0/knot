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
(|
  full *people = [{name "a"}  {name "b"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  active (Int 1)}
Rel V2  *people
  \p -> {name p.name  active 1}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  active (Int 1)}
Rel V2  *people
}
(|
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
(|
  full *people = [{name "a"  age 5}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  age (Int 1)}
V2  {name Text}
Rel V2  *people
  \p -> {name p.name}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  age (Int 1)}
V2  {name Text}
Rel V2  *people
}
(|
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
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {fullName Text}
Rel V2  *people
  \p -> {fullName p.name}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {fullName Text}
Rel V2  *people
}
(|
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
(|
  full *r = [{a "x"  b 1}]
  yield {})"#;
    let v2 = r#"with {
V1  {a Text  b (Int 1)}
V2  {b (Int 1)  a Text}
Rel V2  *r
  \p -> {b p.b  a p.a}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {a Text  b (Int 1)}
V2  {b (Int 1)  a Text}
Rel V2  *r
}
(|
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
(|
  full *people = [{name "a"  tags ["x"  "y"]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  tags (Rel Text)  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  tags p.tags  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  tags (Rel Text)  extra (Int 1)}
Rel V2  *people
}
(|
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
(|
  full *people = [{name "a"  pets [{name "pup"  legs 4}]}]
  yield {})"#;
    let v2 = r#"with {
Pet  {name Text  legs (Int 1)}
V1  {name Text  pets (Rel Pet)}
V2  {name Text  pets (Rel Pet)  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  pets p.pets  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
Pet  {name Text  legs (Int 1)}
V1  {name Text  pets (Rel Pet)}
V2  {name Text  pets (Rel Pet)  extra (Int 1)}
Rel V2  *people
}
(|
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
(|
  full *r = [{name "a"  grid [[1  2]  [3]]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  grid (Rel (Rel (Int 1)))}
V2  {name Text  grid (Rel (Rel (Int 1)))  extra (Int 1)}
Rel V2  *r
  \p -> {name p.name  grid p.grid  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  grid (Rel (Rel (Int 1)))}
V2  {name Text  grid (Rel (Rel (Int 1)))  extra (Int 1)}
Rel V2  *r
}
(|
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
(|
  full *people = [{name "a"  addr {city "x"}}]
  yield {})"#;
    let v2 = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
V2  {name Text  addr Addr  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  addr p.addr  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
Addr  {city Text}
V1  {name Text  addr Addr}
V2  {name Text  addr Addr  extra (Int 1)}
Rel V2  *people
}
(|
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
(|
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
(|
  yield {})"#;
    let committed = r#"with {
Addr  {city Text}
AddrV2  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
V2  {name Text  addr AddrV2}
Rel V2  *people
}
(|
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
(|
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}]
  yield {})"#;
    let v2 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {name Text  sh Shape}
V2  {name Text  sh Shape  extra (Int 1)}
Rel V2  *es
  \p -> {name p.name  sh p.sh  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {name Text  sh Shape}
V2  {name Text  sh Shape  extra (Int 1)}
Rel V2  *es
}
(|
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
(|
  full *evs = [(Ev.Ping {})  (Ev.Msg {text "hi"})]
  yield {})"#;
    let v2 = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text  loud (Int 1)}
Rel EvV2  *evs
  \e -> (? (e)
    Ev.Ping {}  (EvV2.Ping {})
    Ev.Msg {text t}  (EvV2.Msg {text t  loud 0}))
}
(|
  yield {})"#;
    let committed = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text  loud (Int 1)}
Rel EvV2  *evs
}
(|
  rows <- *evs
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_adt_source", v1, v2, committed);
    assert_eq!(out, "\"[2]\"\n{}");
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
(|
  yield {})"#;
    let v2 = r#"with {
V1  {a (Int 1)}
V2  {a (Int 1)  b (Int 1)}
Rel V2  *r
  \p -> {a p.a  b 0}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {a (Int 1)}
V2  {a (Int 1)  b (Int 1)}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_empty", v1, v2, committed);
    assert_eq!(out, "\"[0]\"\n{}");
}

/// Several rows all survive; the transform applies to each.
#[test]
fn corner_multiple_rows() {
    let dir = dir_for("mig_multi");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}  {name "b"}  {name "c"}  {name "d"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  tag (Int 1)}
Rel V2  *people
  \p -> {name p.name  tag 7}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  tag (Int 1)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show (base.count rows))
  yield {})"#;
    let out = lifecycle(&dir, "mig_multi", v1, v2, committed);
    assert_eq!(out, "\"[4]\"\n{}");
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
(|
  full *r = [{name "x"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  b Text}
Rel V2  *r
  \p -> {name p.name  b p.name}
}
(|
  yield {})"#;
    let v3 = r#"with {
V1  {name Text}
V2  {name Text  b Text}
V3  {name Text  b Text  c Text}
Rel V3  *r
  \p -> {name p.name  b p.b  c p.b}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  b Text}
V3  {name Text  b Text  c Text}
Rel V3  *r
}
(|
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

// ── Relation-field add/drop ──────────────────────────────────────────────────

/// Add a relation field where none existed: the transform supplies the new
/// relation's rows.
#[test]
fn corner_add_relation_field() {
    let dir = dir_for("mig_add_rel");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  tags (Rel Text)}
Rel V2  *people
  \p -> {name p.name  tags ["new"]}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  tags (Rel Text)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_add_rel", v1, v2, committed);
    assert_eq!(out, "\"[{name a  tags [new]}]\"\n{}");
}

/// Drop a relation field; the scalar survivors are intact and the child tables
/// are gone (the read shows only the remaining field).
#[test]
fn corner_drop_relation_field() {
    let dir = dir_for("mig_drop_rel");
    let v1 = r#"with {
V1  {name Text  tags (Rel Text)}
Rel V1  *people
}
(|
  full *people = [{name "a"  tags ["x"]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text}
Rel V2  *people
  \p -> {name p.name}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_drop_rel", v1, v2, committed);
    assert_eq!(out, "\"[{name a}]\"\n{}");
}

/// Change a relation field's element type (Rel Text -> Rel Int): the transform
/// supplies fresh rows of the new element type.
#[test]
fn corner_change_relation_element_type() {
    let dir = dir_for("mig_rel_elem");
    let v1 = r#"with {
V1  {name Text  tags (Rel Text)}
Rel V1  *people
}
(|
  full *people = [{name "a"  tags ["x"]}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  nums (Rel (Int 1))}
Rel V2  *people
  \p -> {name p.name  nums [1  2]}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  tags (Rel Text)}
V2  {name Text  nums (Rel (Int 1))}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rel_elem", v1, v2, committed);
    assert_eq!(out, "\"[{name a  nums [1, 2]}]\"\n{}");
}

// ── Multi-source programs ────────────────────────────────────────────────────

/// Two sources in one program; only one migrates. The other is untouched.
#[test]
fn corner_unrelated_source_untouched() {
    let dir = dir_for("mig_two_src");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
W  {id (Int 1)}
Rel W  *widgets
}
(|
  full *people = [{name "a"}]
  full *widgets = [{id 1}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  extra 0}
W  {id (Int 1)}
Rel W  *widgets
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V2  *people
W  {id (Int 1)}
Rel W  *widgets
}
(|
  p <- *people
  w <- *widgets
  base.println (base.show p)
  base.println (base.show w)
  yield {})"#;
    let out = lifecycle(&dir, "mig_two_src", v1, v2, committed);
    // The migrated source has the new field; the untouched source is intact.
    assert_eq!(out, "\"[{extra 0  name a}]\"\n\"[{id 1}]\"\n{}");
}

/// Drop a source entirely (one of two removed); the survivor is intact.
#[test]
fn corner_drop_source() {
    let dir = dir_for("mig_drop_src");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
W  {id (Int 1)}
Rel W  *widgets
}
(|
  full *people = [{name "a"}]
  full *widgets = [{id 1}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_drop_src", v1, v2, committed);
    assert_eq!(out, "\"[{name a}]\"\n{}");
}

// ── Type-shape transitions ───────────────────────────────────────────────────

/// A tag-enum (all-nullary ADT) field survives a migration.
#[test]
fn corner_tag_enum_field() {
    let dir = dir_for("mig_tag_enum");
    let v1 = r#"with {
Color  Red {}  Green {}
V1  {name Text  col Color}
Rel V1  *es
}
(|
  full *es = [{name "a"  col (Color.Red {})}]
  yield {})"#;
    let v2 = r#"with {
Color  Red {}  Green {}
V1  {name Text  col Color}
V2  {name Text  col Color  extra (Int 1)}
Rel V2  *es
  \p -> {name p.name  col p.col  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
Color  Red {}  Green {}
V1  {name Text  col Color}
V2  {name Text  col Color  extra (Int 1)}
Rel V2  *es
}
(|
  rows <- *es
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_tag_enum", v1, v2, committed);
    assert_eq!(out, "\"[{col Red  extra 0  name a}]\"\n{}");
}

/// Retype a scalar field (Text -> Int): the transform produces the new type.
#[test]
fn corner_retype_scalar() {
    let dir = dir_for("mig_retype");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name (Int 1)}
Rel V2  *people
  \p -> {name 42}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name (Int 1)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_retype", v1, v2, committed);
    assert_eq!(out, "\"[{name 42}]\"\n{}");
}

/// The transform computes a fresh payload-ADT value from an old scalar field.
#[test]
fn corner_transform_to_payload_adt() {
    let dir = dir_for("mig_to_adt");
    let v1 = r#"with {
V1  {n (Int 1)}
Rel V1  *r
}
(|
  full *r = [{n 5}]
  yield {})"#;
    let v2 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {n (Int 1)}
V2  {sh Shape}
Rel V2  *r
  \p -> {sh (Shape.Circle {radius p.n})}
}
(|
  yield {})"#;
    let committed = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
V1  {n (Int 1)}
V2  {sh Shape}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_to_adt", v1, v2, committed);
    assert_eq!(out, "\"[{sh Circle {radius 5}}]\"\n{}");
}

// ── Constraints ─────────────────────────────────────────────────────────────

/// A source carrying a uniqueness constraint migrates; the constraint index
/// survives and the data is intact.
#[test]
fn corner_migration_with_unique_constraint() {
    let dir = dir_for("mig_uniq");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
*people <= *people.name
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  extra 0}
*people <= *people.name
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V2  *people
*people <= *people.name
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_uniq", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  name a}]\"\n{}");
}

// ── Reverts ─────────────────────────────────────────────────────────────────

/// A revert chain A→B→A with data: the field is added then dropped, and the
/// surviving data reads back through both transforms.
#[test]
fn corner_revert_chain_with_data() {
    let dir = dir_for("mig_revert_data");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  extra 1}
}
(|
  yield {})"#;
    let v1_revert = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V1  *people
  \p -> {name p.name}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name Text  extra (Int 1)}
Rel V1  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;

    build_and_run(&dir, "mig_revert_data", v1);
    lock_in_dir(dir.path(), "mig_revert_data");
    build_and_run(&dir, "mig_revert_data", v2);
    lock_in_dir(dir.path(), "mig_revert_data");
    build_and_run(&dir, "mig_revert_data", v1_revert);
    lock_in_dir(dir.path(), "mig_revert_data");
    let out = build_and_run(&dir, "mig_revert_data", committed);
    assert_eq!(out, "\"[{name a}]\"\n{}");
}

// ── Error cases ─────────────────────────────────────────────────────────────

/// A migration clause on an UNCHANGED schema is rejected: there's nothing to
/// migrate, so the clause is meaningless. This pins the "no-op migration"
/// guard.
#[test]
fn corner_migration_clause_on_unchanged_schema_is_an_error() {
    let dir = dir_for("mig_noop");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    build_and_run(&dir, "mig_noop", v1);
    lock_in_dir(dir.path(), "mig_noop");

    // Same schema, but a migration clause is attached — no schema change to
    // justify it.
    let noop = r#"with {
V1  {name Text}
Rel V1  *people
  \p -> {name p.name}
}
(|
  yield {})"#;
    std::fs::write(dir.join("mig_noop.knot"), noop).unwrap();
    let build = std::process::Command::new(knot_bin())
        .arg("build")
        .arg(dir.join("mig_noop.knot"))
        .arg("-o")
        .arg(dir.join("mig_noop"))
        .output()
        .expect("knot build");
    assert!(
        !build.status.success(),
        "a migration clause on an unchanged schema must fail the build"
    );
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        stderr.contains("schema is unchanged"),
        "error names the unchanged-schema guard: {stderr}"
    );
}

// ── Optional & scalar types ──────────────────────────────────────────────────

/// A `Maybe` field survives a migration — both `Just` and `Nothing` rows.
#[test]
fn corner_maybe_field() {
    let dir = dir_for("mig_maybe");
    let v1 = r#"with {
V1  {name Text  nick (Maybe Text)}
Rel V1  *people
}
(|
  full *people = [{name "a"  nick (Maybe.Just {value "al"})}  {name "b"  nick (Maybe.Nothing {})}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  nick (Maybe Text)}
V2  {name Text  nick (Maybe Text)  extra (Int 1)}
Rel V2  *people
  \p -> {name p.name  nick p.nick  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  nick (Maybe Text)}
V2  {name Text  nick (Maybe Text)  extra (Int 1)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_maybe", v1, v2, committed);
    assert_eq!(
        out,
        "\"[{extra 0  name a  nick Just {value al}}, {extra 0  name b  nick Nothing}]\"\n{}"
    );
}

/// A `Float` field survives a migration.
#[test]
fn corner_float_field() {
    let dir = dir_for("mig_float");
    let v1 = r#"with {
V1  {name Text  score (Float 1)}
Rel V1  *r
}
(|
  full *r = [{name "a"  score 1.5}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text  score (Float 1)}
V2  {name Text  score (Float 1)  extra (Int 1)}
Rel V2  *r
  \p -> {name p.name  score p.score  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text  score (Float 1)}
V2  {name Text  score (Float 1)  extra (Int 1)}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_float", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  name a  score 1.5}]\"\n{}");
}

// ── Deep nesting ─────────────────────────────────────────────────────────────

/// A record field containing a relation of payload-ADTs — the deepest nesting
/// (record -> relation -> ADT child tables) — survives a migration.
#[test]
fn corner_deeply_nested() {
    let dir = dir_for("mig_deep");
    let v1 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Inner  {shapes (Rel Shape)}
V1  {name Text  inner Inner}
Rel V1  *r
}
(|
  full *r = [{name "a"  inner {shapes [(Shape.Point {})]}}]
  yield {})"#;
    let v2 = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Inner  {shapes (Rel Shape)}
V1  {name Text  inner Inner}
V2  {name Text  inner Inner  extra (Int 1)}
Rel V2  *r
  \p -> {name p.name  inner p.inner  extra 0}
}
(|
  yield {})"#;
    let committed = r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Inner  {shapes (Rel Shape)}
V1  {name Text  inner Inner}
V2  {name Text  inner Inner  extra (Int 1)}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_deep", v1, v2, committed);
    assert_eq!(out, "\"[{extra 0  inner {shapes [Point]}  name a}]\"\n{}");
}

// ── Transform reads ──────────────────────────────────────────────────────────

/// The transform reads a nested record field's subfield into a top-level column.
#[test]
fn corner_transform_reads_nested_subfield() {
    let dir = dir_for("mig_read_nested");
    let v1 = r#"with {
Addr  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
Rel V1  *people
}
(|
  full *people = [{name "a"  addr {city "x"  zip 100}}]
  yield {})"#;
    let v2 = r#"with {
Addr  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
V2  {name Text  zip (Int 1)}
Rel V2  *people
  \p -> {name p.name  zip p.addr.zip}
}
(|
  yield {})"#;
    let committed = r#"with {
Addr  {city Text  zip (Int 1)}
V1  {name Text  addr Addr}
V2  {name Text  zip (Int 1)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_read_nested", v1, v2, committed);
    assert_eq!(out, "\"[{name a  zip 100}]\"\n{}");
}

/// Rename AND retype a field in one migration.
#[test]
fn corner_rename_and_retype() {
    let dir = dir_for("mig_rename_retype");
    let v1 = r#"with {
V1  {old Text}
Rel V1  *r
}
(|
  full *r = [{old "x"}]
  yield {})"#;
    let v2 = r#"with {
V1  {old Text}
V2  {new (Int 1)}
Rel V2  *r
  \p -> {new 1}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {old Text}
V2  {new (Int 1)}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_rename_retype", v1, v2, committed);
    assert_eq!(out, "\"[{new 1}]\"\n{}");
}

// ── Constraints ─────────────────────────────────────────────────────────────

/// A source with a referential (subset) constraint migrates; the constraint
/// triggers survive the table swap and the data is intact. (Regression: the
/// swap fired a trigger against the absent table and aborted.)
#[test]
fn corner_migration_with_subset_constraint() {
    let dir = dir_for("mig_subset");
    let v1 = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
Rel V1  *pets
*pets.email <= *owners.email
}
(|
  full *owners = [{email "a@x"}]
  full *pets = [{email "a@x"}]
  yield {})"#;
    let v2 = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
V2  {email Text  extra (Int 1)}
Rel V2  *pets
  \p -> {email p.email  extra 0}
*pets.email <= *owners.email
}
(|
  yield {})"#;
    let committed = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
V2  {email Text  extra (Int 1)}
Rel V2  *pets
*pets.email <= *owners.email
}
(|
  rows <- *pets
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_subset", v1, v2, committed);
    assert_eq!(out, "\"[{email a@x  extra 0}]\"\n{}");
}

/// After migrating a subset-constrained source, the constraint still bites: a
/// new row referencing a missing superset key is rejected.
#[test]
fn corner_subset_constraint_still_enforced_after_migration() {
    let dir = dir_for("mig_subset_enforced");
    let v1 = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
Rel V1  *pets
*pets.email <= *owners.email
}
(|
  full *owners = [{email "a@x"}]
  full *pets = [{email "a@x"}]
  yield {})"#;
    let v2 = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
V2  {email Text  extra (Int 1)}
Rel V2  *pets
  \p -> {email p.email  extra 0}
*pets.email <= *owners.email
}
(|
  yield {})"#;
    // Migrate + commit.
    build_and_run(&dir, "mig_subset_enforced", v1);
    lock_in_dir(dir.path(), "mig_subset_enforced");
    build_and_run(&dir, "mig_subset_enforced", v2);
    lock_in_dir(dir.path(), "mig_subset_enforced");

    // Now insert a pet referencing a nonexistent owner — the constraint must
    // still fire after the migration.
    let bad_insert = r#"with {
Owner  {email Text}
Rel Owner  *owners
V1  {email Text}
V2  {email Text  extra (Int 1)}
Rel V2  *pets
*pets.email <= *owners.email
}
(|
  pets <- *pets
  *pets = base.union pets [{email "missing@x"  extra 0}]
  yield {})"#;
    std::fs::write(dir.join("mig_subset_enforced.knot"), bad_insert).unwrap();
    build_in_dir("mig_subset_enforced", bad_insert, dir.path());
    let run = std::process::Command::new(dir.join("mig_subset_enforced"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        !run.status.success() && stderr.contains("subset constraint violated"),
        "the subset constraint still fires after migration: {stderr}"
    );
}

// ── Migration-lambda expressiveness ─────────────────────────────────────────
// These exercise what the migration lambda can reference now that it's
// type-checked with the full builtin scope (deferred past `pre_register`).

/// The transform wraps a scalar in `Maybe.Just` — a builtin-ADT constructor in
/// the migration lambda. (Regression: the lambda was checked before `Maybe`
/// was in scope.)
#[test]
fn corner_scalar_to_maybe() {
    let dir = dir_for("mig_to_maybe");
    let v1 = r#"with {
V1  {name Text}
Rel V1  *people
}
(|
  full *people = [{name "a"}]
  yield {})"#;
    let v2 = r#"with {
V1  {name Text}
V2  {name (Maybe Text)}
Rel V2  *people
  \p -> {name (Maybe.Just {value p.name})}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name Text}
V2  {name (Maybe Text)}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_to_maybe", v1, v2, committed);
    assert_eq!(out, "\"[{name Just {value a}}]\"\n{}");
}

/// The transform unwraps a `Maybe` via a match — pattern-matching a builtin
/// ADT's constructors in the migration lambda.
#[test]
fn corner_maybe_to_scalar() {
    let dir = dir_for("mig_from_maybe");
    let v1 = r#"with {
V1  {name (Maybe Text)}
Rel V1  *people
}
(|
  full *people = [{name (Maybe.Just {value "a"})}  {name (Maybe.Nothing {})}]
  yield {})"#;
    let v2 = r#"with {
V1  {name (Maybe Text)}
V2  {name Text}
Rel V2  *people
  \p -> {name (? (p.name)
    Maybe.Just {value v}  v
    Maybe.Nothing {}  "anon")}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {name (Maybe Text)}
V2  {name Text}
Rel V2  *people
}
(|
  rows <- *people
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_from_maybe", v1, v2, committed);
    assert_eq!(out, "\"[{name a}, {name anon}]\"\n{}");
}

/// The transform computes a new field arithmetically from an old one. A
/// refined-Int field supports `Num` ops in the migration lambda. (Regression:
/// arithmetic on a refined field failed before builtins were in scope.)
#[test]
fn corner_arithmetic_computed_field() {
    let dir = dir_for("mig_arith");
    let v1 = r#"with {
V1  {age (Int 1)}
Rel V1  *r
}
(|
  full *r = [{age 5}]
  yield {})"#;
    let v2 = r#"with {
V1  {age (Int 1)}
V2  {age (Int 1)  doubled (Int 1)}
Rel V2  *r
  \p -> {age p.age  doubled (p.age * 2)}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {age (Int 1)}
V2  {age (Int 1)  doubled (Int 1)}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_arith", v1, v2, committed);
    assert_eq!(out, "\"[{age 5  doubled 10}]\"\n{}");
}

/// The transform retypes via a stdlib call (`base.show`). (Regression: `base`
/// wasn't in the migration lambda's scope.)
#[test]
fn corner_retype_via_base_show() {
    let dir = dir_for("mig_base_show");
    let v1 = r#"with {
V1  {n (Int 1)}
Rel V1  *r
}
(|
  full *r = [{n 7}]
  yield {})"#;
    let v2 = r#"with {
V1  {n (Int 1)}
V2  {n Text}
Rel V2  *r
  \p -> {n (base.show p.n)}
}
(|
  yield {})"#;
    let committed = r#"with {
V1  {n (Int 1)}
V2  {n Text}
Rel V2  *r
}
(|
  rows <- *r
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_base_show", v1, v2, committed);
    assert_eq!(out, "\"[{n 7}]\"\n{}");
}

// ── ADT evolution ────────────────────────────────────────────────────────────

/// An ADT source gains a constructor (additive): existing rows keep their tag.
#[test]
fn corner_adt_gains_constructor() {
    let dir = dir_for("mig_adt_add");
    let v1 = r#"with {
Ev  Ping {}  Msg {text Text}
Rel Ev  *evs
}
(|
  full *evs = [(Ev.Ping {})  (Ev.Msg {text "hi"})]
  yield {})"#;
    let v2 = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text}  Kick {}
Rel EvV2  *evs
  \e -> (? (e)
    Ev.Ping {}  (EvV2.Ping {})
    Ev.Msg {text t}  (EvV2.Msg {text t}))
}
(|
  yield {})"#;
    let committed = r#"with {
Ev  Ping {}  Msg {text Text}
EvV2  Ping {}  Msg {text Text}  Kick {}
Rel EvV2  *evs
}
(|
  rows <- *evs
  base.println (base.show rows)
  yield {})"#;
    let out = lifecycle(&dir, "mig_adt_add", v1, v2, committed);
    assert_eq!(out, "\"[Ping, Msg {text hi}]\"\n{}");
}
