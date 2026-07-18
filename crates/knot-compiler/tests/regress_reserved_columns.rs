//! Regression tests for reserved internal column names.
//!
//! A persisted field maps onto a SQLite column of the same name, and the
//! runtime keeps its own bookkeeping columns in the `_` namespace: `_id`
//! (parent tables with nested children), `_parent_id` / `_content_hash`
//! (child tables), `_tag` (ADT tables), and `_value` (scalar sources). A user
//! field with one of those names compiled cleanly and then aborted on the
//! first run — `CREATE TABLE` got two `_id` columns and the process died with
//! "duplicate column name: _id" before running a line of the program.
//!
//! `types::check_reserved_field_names` now rejects `_`-prefixed field names in
//! the types that become tables (source element types, both sides of a
//! `migrate`), reaching through aliases, data declarations, and nested
//! relations. Types that are never persisted keep their freedom.

use knot::diagnostic::{Diagnostic, Severity};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn check(src: &str) -> Vec<Diagnostic> {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(parse_diags.is_empty(), "parse diagnostics: {parse_diags:?}");
    knot_compiler::types::check_reserved_field_names(&module)
}

/// Assert exactly one error, naming `field` and the relation it is persisted in.
fn assert_rejects(src: &str, field: &str, relation: &str) {
    let diags = check(src);
    assert_eq!(diags.len(), 1, "expected exactly one diagnostic, got: {diags:?}");
    let d = &diags[0];
    assert_eq!(d.severity, Severity::Error);
    assert!(
        d.message.contains(field) && d.message.contains(relation),
        "diagnostic should name field '{field}' and relation '{relation}': {}",
        d.message
    );
    assert!(!d.labels.is_empty(), "diagnostic should be anchored at the field");
}

fn assert_accepts(src: &str) {
    let diags = check(src);
    assert!(diags.is_empty(), "expected no diagnostics, got: {diags:?}");
}

// ── The four internal columns, each in the shape that used to abort ──

#[test]
fn id_field_on_parent_with_nested_children_rejected() {
    // `posts` has a nested relation, so its table gets an `_id` primary key —
    // "duplicate column name: _id" at init.
    assert_rejects(
        r#"type Tag = {label: Text}
type Post = {_id: Int 1, title: Text, tags: [Tag]}
*posts : [Post]
main = 1
"#,
        "_id",
        "posts",
    );
}

#[test]
fn tag_field_on_adt_relation_rejected() {
    // A direct ADT relation is stored in a wide table keyed by `_tag`.
    assert_rejects(
        r#"data Shape = Circle {_tag: Text, radius: Float 1} | Rect {width: Float 1, height: Float 1}
*shapes : [Shape]
main = 1
"#,
        "_tag",
        "shapes",
    );
}

#[test]
fn parent_id_field_in_child_table_rejected() {
    // `items` lives in a child table, whose FK column is `_parent_id`.
    assert_rejects(
        r#"type Item = {_parent_id: Int 1, label: Text}
type Box = {name: Text, items: [Item]}
*boxes : [Box]
main = 1
"#,
        "_parent_id",
        "boxes",
    );
}

#[test]
fn content_hash_field_rejected() {
    assert_rejects(
        r#"type Line = {sku: Text}
type Order = {_content_hash: Text, lines: [Line]}
*orders : [Order]
main = 1
"#,
        "_content_hash",
        "orders",
    );
}

#[test]
fn value_field_rejected() {
    // `_value` is the column synthesized for scalar sources (`*n : Int 1`), so a
    // one-field `{_value: Int 1}` record is indistinguishable from one: it does
    // not collide in SQL, it makes codegen take the scalar-source path.
    assert_rejects(
        r#"*counters : [{_value: Int 1}]
main = 1
"#,
        "_value",
        "counters",
    );
}

// ── Reaching the offending field ──

#[test]
fn reserved_field_behind_alias_chain_rejected() {
    // The field sits two aliases away from the source that persists it.
    assert_rejects(
        r#"type Inner = {_id: Int 1, name: Text}
type Outer = Inner
*people : [Outer]
main = 1
"#,
        "_id",
        "people",
    );
}

#[test]
fn reserved_field_in_nested_record_rejected() {
    // Nested records round-trip through a JSON column rather than a table of
    // their own, but the alias is one edit away from being persisted directly,
    // and the rule is the field name, not the column it happens to land in.
    assert_rejects(
        r#"*people : [{name: Text, meta: {_tag: Text}}]
main = 1
"#,
        "_tag",
        "people",
    );
}

#[test]
fn reserved_field_in_migrate_type_rejected() {
    // Both sides of a `migrate` produce a schema, so both are checked. Only
    // the old type carries the bad field here.
    let diags = check(
        r#"type Old = {_id: Int 1, name: Text}
type New = {name: Text}
*people : [New]
migrate *people from [Old] to [New] using \p -> {name: p.name}
main = 1
"#,
    );
    assert_eq!(diags.len(), 1, "expected one diagnostic, got: {diags:?}");
    assert!(diags[0].message.contains("_id"), "{}", diags[0].message);
}

#[test]
fn cyclic_alias_terminates() {
    // `check_alias_cycles` reports the cycle; this pass must not chase it.
    let diags = check(
        r#"type A = B
type B = A
*xs : [{v: A}]
main = 1
"#,
    );
    assert!(diags.is_empty(), "got: {diags:?}");
}

#[test]
fn shared_alias_reported_once_per_field() {
    // One bad alias behind two sources is one mistake, not two.
    let diags = check(
        r#"type Row = {_id: Int 1, name: Text}
*a : [Row]
*b : [Row]
main = 1
"#,
    );
    assert_eq!(diags.len(), 1, "expected one diagnostic, got: {diags:?}");
}

// ── What stays legal ──

#[test]
fn ordinary_field_names_accepted() {
    assert_accepts(
        r#"type Tag = {label: Text}
type Post = {id: Int 1, title: Text, tags: [Tag]}
*posts : [Post]
main = 1
"#,
    );
}

#[test]
fn reserved_name_in_unpersisted_record_accepted() {
    // `Local` never reaches a table, so its field names are its own business.
    assert_accepts(
        r#"type Local = {_id: Int 1}
type Post = {title: Text}
*posts : [Post]
idOf = \l -> l._id
main = 1
"#,
    );
}

#[test]
fn scalar_source_accepted() {
    // The compiler synthesizes the `_value` column for `*counter : Int 1`; that
    // is not a user field and must not trip the check.
    assert_accepts(
        r#"*counter : Int 1
*tags : [Text]
main = 1
"#,
    );
}

// ── End to end: rejected at build, not at first run ──

#[test]
fn build_fails_with_clear_error() {
    let dir: PathBuf = std::env::temp_dir().join(format!(
        "knot_regress_reserved_columns_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("prog.knot");
    fs::write(
        &src,
        r#"type Tag = {label: Text}
type Post = {_id: Int 1, title: Text, tags: [Tag]}

*posts : [Post]

main = do
  replace *posts = [{_id: 1, title: "hi", tags: [{label: "x"}]}]
  p <- *posts
  yield p.title
"#,
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_knot"))
        .arg("build")
        .arg(&src)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(!out.status.success(), "build should fail; stderr: {stderr}");
    assert!(
        stderr.contains("_id") && stderr.contains("reserved"),
        "error should explain the reserved name; stderr: {stderr}"
    );
    assert!(
        !dir.join("prog").exists(),
        "no executable should be produced for a rejected program"
    );

    let _ = fs::remove_dir_all(&dir);
}
