//! Regression tests for compiler↔runtime schema-descriptor contract bugs.
//! All three used to compile cleanly and then panic at table init on every
//! run:
//!
//! 1. ADT relation with a record (or payload-ADT) constructor field:
//!    `col_type_str` emits "json" for the field but `parse_adt_schema` had
//!    no "json" arm → panic `unknown ADT field type 'json'`.
//! 2. Nested relation of ADT element type inside a record
//!    (`shapes: [Shape]`): emission produced
//!    `shapes:[#Circle:radius=float|Dot]` but `parse_record_schema` routes
//!    every `[...]` into nested-RECORD child-table parsing → panic.
//!    Contract chosen: non-record-element nested relations are stored as a
//!    single `json` column holding the whole relation (descriptor
//!    `shapes:json`); only record-element nested relations keep the
//!    `field:[child_schema]` child-table form.
//! 3. Nested relation of scalar element type (`tags: [Text]`): emission
//!    produced `tags:[text]` → runtime panic on `text` having no `:`.
//!    Same contract as 2: stored as a `json` column (`tags:json`), with
//!    rows deduped on write so set semantics hold.
//!
//! Each test compiles a small Knot program with the real `knot` binary into
//! its own scratch directory (so the program's `.db` lands there) and
//! asserts on the program's output / exit status. Programs are run twice to
//! exercise cross-session re-init against an existing database and reading
//! back persisted values written by the previous process.

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

fn scratch_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_schema_contract_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Compile `source` into a fresh scratch directory and return paths.
fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = scratch_dir(test_name);
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
    let exe = dir.join("prog");
    Compiled { dir, exe }
}

/// Run an already-compiled program once; returns (stdout, stderr, success).
fn run(c: &Compiled) -> (String, String, bool) {
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

// ── Bug 1: ADT relation with record / payload-ADT constructor fields ──

#[test]
fn adt_relation_with_record_field_round_trips() {
    // `center` is a record field inside a constructor of a direct ADT
    // relation. Its wide-table column descriptor is `center=json`; the
    // runtime used to panic at init with "unknown ADT field type 'json'".
    let c = compile(
        "adt_record_field",
        r#"data Shape = Circle {center: {x: Float 1, y: Float 1}, radius: Float 1} | Dot {}

*shapes : [Shape]

main = do
  pre <- *shapes
  println ("pre: " ++ show (count pre))
  replace *shapes = [Circle {center: {x: 1.5, y: 2.5}, radius: 3.0}, Dot {}]
  ss <- *shapes
  forEach ss (\s -> case s of
    Circle k -> println ("circle " ++ show k.center.x ++ " " ++ show k.center.y ++ " r=" ++ show k.radius)
    Dot d -> println "dot")
"#,
    );

    let (out1, err1, ok1) = run(&c);
    assert!(ok1, "first run failed:\nstdout: {out1}\nstderr: {err1}");
    assert!(out1.contains("pre: 0"), "unexpected first-run output: {out1}");
    assert!(out1.contains("circle 1.5 2.5 r=3.0"), "record payload did not round-trip: {out1}");
    assert!(out1.contains("dot"), "nullary constructor missing: {out1}");

    // Second process: re-init against the existing DB must not panic, and
    // the initial read must see the row persisted by the first process
    // (i.e. the json column reconstructs across sessions).
    let (out2, err2, ok2) = run(&c);
    assert!(ok2, "second run failed:\nstdout: {out2}\nstderr: {err2}");
    assert!(out2.contains("pre: 2"), "persisted rows not visible across sessions: {out2}");
    assert!(out2.contains("circle 1.5 2.5 r=3.0"), "second-run read failed: {out2}");
}

#[test]
fn adt_relation_with_payload_adt_field_round_trips() {
    // A payload-bearing ADT as a constructor field also maps to a json
    // column (`inner=json`) in the wide table.
    let c = compile(
        "adt_payload_adt_field",
        r#"data Inner = A {n: Int 1} | B {}
data Outer = W {label: Text, inner: Inner} | E {}

*outers : [Outer]

main = do
  replace *outers = [W {label: "w1", inner: A {n: 42}}, W {label: "w2", inner: B {}}, E {}]
  os <- *outers
  forEach os (\o -> case o of
    W w -> case w.inner of
      A a -> println (w.label ++ " A " ++ show a.n)
      B b -> println (w.label ++ " B")
    E e -> println "E")
"#,
    );

    for pass in 1..=2 {
        let (out, err, ok) = run(&c);
        assert!(ok, "run {pass} failed:\nstdout: {out}\nstderr: {err}");
        assert!(out.contains("w1 A 42"), "run {pass}: payload ADT field lost: {out}");
        assert!(out.contains("w2 B"), "run {pass}: nullary inner ctor lost: {out}");
        assert!(out.contains("E"), "run {pass}: nullary outer ctor lost: {out}");
    }
}

// ── Bug 2: nested relation of ADT element type inside a record ──────

#[test]
fn nested_relation_of_adt_elements_round_trips() {
    let c = compile(
        "nested_adt_relation",
        r#"data Shape = Circle {radius: Float 1} | Dot {}

*drawings : [{name: Text, shapes: [Shape]}]

main = do
  pre <- *drawings
  println ("pre: " ++ show (count pre))
  replace *drawings = [{name: "d1", shapes: [Circle {radius: 2.0}, Dot {}]}]
  ds <- *drawings
  forEach ds (\d -> do
    println d.name
    println ("n=" ++ show (count d.shapes))
    forEach d.shapes (\s -> case s of
      Circle k -> println ("circle " ++ show k.radius)
      Dot x -> println "dot"))
"#,
    );

    let (out1, err1, ok1) = run(&c);
    assert!(ok1, "first run failed:\nstdout: {out1}\nstderr: {err1}");
    assert!(out1.contains("pre: 0"), "unexpected first-run output: {out1}");
    assert!(out1.contains("d1"), "parent row missing: {out1}");
    assert!(out1.contains("n=2"), "nested relation count wrong: {out1}");
    assert!(out1.contains("circle 2.0"), "payload constructor lost: {out1}");
    assert!(out1.contains("dot"), "nullary constructor lost: {out1}");

    let (out2, err2, ok2) = run(&c);
    assert!(ok2, "second run failed:\nstdout: {out2}\nstderr: {err2}");
    assert!(out2.contains("pre: 1"), "persisted parent row not visible across sessions: {out2}");
    assert!(out2.contains("circle 2.0") && out2.contains("dot"), "second-run read failed: {out2}");
}

#[test]
fn nested_adt_relation_dedups_on_write() {
    // Set semantics: duplicate constructors inside the nested relation are
    // deduped when the relation is serialized into the json column.
    let c = compile(
        "nested_adt_dedup",
        r#"data Shape = Circle {radius: Float 1} | Dot {}

*drawings : [{name: Text, shapes: [Shape]}]

main = do
  replace *drawings = [{name: "d", shapes: [Dot {}, Dot {}, Circle {radius: 1.0}, Dot {}, Circle {radius: 1.0}]}]
  ds <- *drawings
  forEach ds (\d -> println ("n=" ++ show (count d.shapes)))
"#,
    );

    let (out, err, ok) = run(&c);
    assert!(ok, "run failed:\nstdout: {out}\nstderr: {err}");
    assert!(out.contains("n=2"), "nested ADT relation not deduped: {out}");
}

// ── Bug 3: nested relation of scalar element type inside a record ───

#[test]
fn nested_relation_of_scalar_elements_round_trips() {
    let c = compile(
        "nested_scalar_relation",
        r#"*posts : [{title: Text, tags: [Text]}]

main = do
  pre <- *posts
  println ("pre: " ++ show (count pre))
  replace *posts = [{title: "a", tags: ["x", "y"]}]
  ps <- *posts
  forEach ps (\p -> do
    println p.title
    forEach p.tags (\t -> println ("tag " ++ t)))
"#,
    );

    let (out1, err1, ok1) = run(&c);
    assert!(ok1, "first run failed:\nstdout: {out1}\nstderr: {err1}");
    assert!(out1.contains("pre: 0"), "unexpected first-run output: {out1}");
    assert!(out1.contains("tag x") && out1.contains("tag y"), "bare scalars did not round-trip: {out1}");

    let (out2, err2, ok2) = run(&c);
    assert!(ok2, "second run failed:\nstdout: {out2}\nstderr: {err2}");
    assert!(out2.contains("pre: 1"), "persisted row not visible across sessions: {out2}");
    assert!(out2.contains("tag x") && out2.contains("tag y"), "second-run read failed: {out2}");
}

#[test]
fn nested_scalar_relation_dedups_on_write() {
    let c = compile(
        "nested_scalar_dedup",
        r#"*posts : [{title: Text, tags: [Text]}]

main = do
  replace *posts = [{title: "a", tags: ["x", "y", "x", "y", "x"]}]
  ps <- *posts
  forEach ps (\p -> do
    println ("n=" ++ show (count p.tags))
    forEach p.tags (\t -> println ("tag " ++ t)))
"#,
    );

    let (out, err, ok) = run(&c);
    assert!(ok, "run failed:\nstdout: {out}\nstderr: {err}");
    assert!(out.contains("n=2"), "nested scalar relation not deduped: {out}");
    assert!(out.contains("tag x") && out.contains("tag y"), "deduped values wrong: {out}");
}

// ── Comprehension binds over json-column nested fields ──────────────

#[test]
fn comprehension_binds_over_json_nested_fields() {
    // `m <- t.field` binds in pure comprehensions must iterate the
    // in-memory relation read back from the json column, including
    // constructor-pattern binds that filter by tag.
    let c = compile(
        "comprehension_json_fields",
        r#"data Shape = Circle {radius: Float 1} | Dot {}

*drawings : [{name: Text, shapes: [Shape]}]
*posts : [{title: Text, tags: [Text]}]

main = do
  replace *drawings = [{name: "d1", shapes: [Circle {radius: 2.0}, Dot {}]}]
  replace *posts = [{title: "a", tags: ["x", "y"]}]
  let circles = do
        d <- *drawings
        Circle k <- d.shapes
        yield {r: k.radius}
  forEach circles (\c -> println ("r=" ++ show c.r))
  let allTags = do
        p <- *posts
        t <- p.tags
        yield {tag: t}
  forEach allTags (\t -> println ("tag=" ++ t.tag))
"#,
    );

    let (out, err, ok) = run(&c);
    assert!(ok, "run failed:\nstdout: {out}\nstderr: {err}");
    assert!(out.contains("r=2.0"), "constructor-pattern bind over json field failed: {out}");
    assert!(
        out.contains("tag=x") && out.contains("tag=y"),
        "scalar bind over json field failed: {out}"
    );
}

// ── Mixed: record-element child tables still work alongside json cols ──

#[test]
fn record_child_table_with_inner_scalar_relation_round_trips() {
    // A record-element nested relation (child table, unchanged contract)
    // whose rows themselves contain a scalar-element nested relation
    // (json column inside the child table).
    let c = compile(
        "child_table_with_json_col",
        r#"*teams : [{name: Text, members: [{handle: Text, skills: [Text]}]}]

main = do
  replace *teams = [{name: "t1", members: [{handle: "ana", skills: ["rust", "sql"]}, {handle: "bo", skills: []}]}]
  ts <- *teams
  forEach ts (\t -> do
    println t.name
    forEach t.members (\m -> do
      println (m.handle ++ " n=" ++ show (count m.skills))
      forEach m.skills (\s -> println ("skill " ++ s))))
"#,
    );

    for pass in 1..=2 {
        let (out, err, ok) = run(&c);
        assert!(ok, "run {pass} failed:\nstdout: {out}\nstderr: {err}");
        assert!(out.contains("t1"), "run {pass}: parent missing: {out}");
        assert!(out.contains("ana n=2"), "run {pass}: child row / inner count wrong: {out}");
        assert!(out.contains("bo n=0"), "run {pass}: empty inner relation wrong: {out}");
        assert!(
            out.contains("skill rust") && out.contains("skill sql"),
            "run {pass}: inner scalars lost: {out}"
        );
    }
}
