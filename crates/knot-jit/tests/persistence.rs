//! Subprocess e2e tests for persisted relations and IO — the features the
//! in-process JIT can't fully evaluate (source relations, file IO, and any
//! process-level effect). Builds and runs real binaries.

mod e2e;
use e2e::assert_stdout;

#[test]
fn persisted_relation_groupby() {
    assert_stdout(
        "groupby",
        r#"with {
Todo  {owner Text  done (Int 1)}
Rel Todo  *todos
}
(do
  full *todos = [{owner "x"  done 0}  {owner "x"  done 1}  {owner "y"  done 0}]
  groups <- (do
    t <- *todos
    where t.done == 0
    groupBy {owner t.owner}
    yield {owner t.owner  count (base.count t)})
  base.println (base.show groups)
  yield {})"#,
        // Set semantics: identical rows are deduped on write (INSERT OR
        // IGNORE), so distinct open rows only. owners x and y each have 1.
        "\"[{count 1  owner x}, {count 1  owner y}]\"\n{}",
    );
}

#[test]
fn persisted_relation_read_write() {
    assert_stdout(
        "persist",
        r#"with {
C  {n (Int 1)}
Rel C  *cs
}
(do
  full *cs = [{n 1}  {n 2}  {n 3}]
  rows <- *cs
  base.println (base.show (base.count rows))
  base.println (base.show (base.sum (base.map (\c -> c.n) rows)))
  yield {})"#,
        "\"3\"\n\"6\"\n{}",
    );
}

#[test]
fn file_write_read_roundtrip() {
    assert_stdout(
        "fileio",
        r#"(do
  base.writeFile "note.txt" "hello knot"
  content <- base.readFile "note.txt"
  base.println content
  yield {})"#,
        "\"hello knot\"\n{}",
    );
}

#[test]
fn morph_resolution() {
    // `(^into)` resolves against an annotated toplevel binding's declared
    // type, via base.morph.<from>To<to>.into.
    assert_stdout(
        "morph",
        r#"with {
Maybe (Int 1)  asInt  ((^into) "42")
Text  asText  ((^into) 7)
}
(do
  base.println (base.show asInt)
  base.println asText
  yield {})"#,
        "\"Just {value 42}\"\n\"7\"\n{}",
    );
}

#[test]
fn traverse_io() {
    // Only IO is a supported traverse applicative.
    assert_stdout(
        "traverse",
        r#"(do
  r <- (base.traverse (\n -> base.println (base.show (n * 2))) [1  2])
  base.println (base.show r)
  yield {})"#,
        "\"2\"\n\"4\"\n\"[{}, {}]\"\n{}",
    );
}

#[test]
fn compile_result_ok() {
    assert_stdout(
        "compile_ok",
        r#"(match (base.the (Result Text (Int 1)) (base.compile "40 + 2"))
  Result.Ok {value v}  base.println ("ok: " ++ base.show v)
  Result.Err {error e}  base.println ("err: " ++ e))"#,
        "\"ok: 42\"\n{}",
    );
}

#[test]
fn compile_result_err_on_mismatch() {
    assert_stdout(
        "compile_err",
        r#"(match (base.the (Result Text (Int 1)) (base.compile "\"text\""))
  Result.Ok {value v}  base.println "ok"
  Result.Err {error e}  base.println "err")"#,
        "\"err\"\n{}",
    );
}

#[test]
fn compile_err_on_invalid_source() {
    assert_stdout(
        "compile_bad",
        r#"(match (base.the (Result Text (Int 1)) (base.compile "1 +"))
  Result.Ok {value v}  base.println "ok"
  Result.Err {error e}  base.println "err")"#,
        "\"err\"\n{}",
    );
}

#[test]
fn atomic_transfer() {
    // `atomic do ...` returns the relation written; bind it (or `_`) so the
    // enclosing do-block's value isn't the relation.
    assert_stdout(
        "atomic",
        r#"with {
Account  {name Text  balance (Int 1)}
Rel Account  *accounts
}
(do
  full *accounts = [{name "from"  balance 100}  {name "to"  balance 0}]
  _ <- atomic do
    rows <- *accounts
    *accounts = base.map (\a ->
      match (a.name == "from")
        Bool.True {}  (base.unify a {balance (a.balance - 40)})
        Bool.False {}  (match (a.name == "to")
          Bool.True {}  (base.unify a {balance (a.balance + 40)})
          Bool.False {}  a)) rows
    yield {}
  base.println (base.show *accounts)
  yield {})"#,
        "\"[{balance 60  name from}, {balance 40  name to}]\"\n{}",
    );
}

/// A source whose element is a variant ADT persists as one wide row per
/// constructor (a `_tag` column plus nullable per-constructor payload columns).
/// This used to core-dump at runtime ("cannot convert Relation to SQL") because
/// a bare ADT name resolves to `Named`, falling to the `_value:text` scalar
/// fallback — `relation_inner_schema` now re-resolves the ADT structure.
#[test]
fn persisted_adt_element_source() {
    assert_stdout(
        "persistadt",
        r#"with {
Shape  Circle {radius (Int 1)}  Square {side (Int 1)}  Point {}
Rel Shape  *shapes
}
(do
  full *shapes = [Shape.Circle {radius 3}  Shape.Square {side 2}  Shape.Point {}]
  rows <- *shapes
  base.println (base.show rows)
  yield {})"#,
        "\"[Circle {radius 3}, Square {side 2}, Point]\"\n{}",
    );
}

/// An Int column is stored as a native SQLite INTEGER, not as TEXT. (Ints were
/// historically stored as `TEXT COLLATE KNOT_INT` — a leftover from when knot's
/// Int was a bignum. `Int` is `i64` now, so it fits SQLite's INTEGER directly.)
#[test]
fn int_column_is_native_integer() {
    let dir = e2e::TempDir::fresh("intstorage");
    e2e::build_in_dir(
        "intstorage",
        r#"with {
C  {n (Int 1)}
Rel C  *cs
}
(do
  full *cs = [{n 42}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("intstorage"), dir.path());

    // Probe the physical storage type with SQLite's `typeof()` (via python3 —
    // no sqlite3 CLI on this host, and the test crate has no rusqlite dep).
    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             print(c.execute('select typeof(n), n from _knot_cs').fetchone())",
        )
        .arg(dir.join("intstorage.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    assert!(
        out.contains("('integer', 42)"),
        "Int column must be a native INTEGER, got: {out}"
    );
}

/// A function value can't be persisted. A source whose element type contains a
/// function field must be a **compile error**, not a runtime crash: the schema
/// catch-all used to type it `fn:text`, then `full *rs = [{f (\n -> n)}]`
/// aborted the process with `cannot convert Function to SQL`. Nested functions
/// (inside a record/variant payload) must also be rejected — they used to
/// serialize to a dead display string that crashed on call.
#[test]
fn function_field_is_a_compile_error() {
    // Top-level function field.
    let dir = e2e::TempDir::fresh("fnfield");
    let src = r#"with {
R  {name Text  f (Int 1 -> Int 1)}
Rel R  *rs
}
(do
  full *rs = [{name "x"  f (\n -> n + 1)}]
  yield {})"#;
    let src_path = dir.join("fnfield.knot");
    std::fs::write(&src_path, src).unwrap();
    let build = std::process::Command::new(e2e::knot_bin())
        .arg("build")
        .arg(&src_path)
        .arg("-o")
        .arg(dir.join("fnfield"))
        .output()
        .expect("knot build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        !build.status.success(),
        "function field must fail the build, not crash at runtime.\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("function"),
        "error should name the function field: {stderr}"
    );
}