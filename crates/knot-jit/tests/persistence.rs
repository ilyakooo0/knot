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
        r#"(? (base.the (Result Text (Int 1)) (base.compile "40 + 2"))
  Result.Ok {value v}  base.println ("ok: " ++ base.show v)
  Result.Err {error e}  base.println ("err: " ++ e))"#,
        "\"ok: 42\"\n{}",
    );
}

#[test]
fn compile_result_err_on_mismatch() {
    assert_stdout(
        "compile_err",
        r#"(? (base.the (Result Text (Int 1)) (base.compile "\"text\""))
  Result.Ok {value v}  base.println "ok"
  Result.Err {error e}  base.println "err")"#,
        "\"err\"\n{}",
    );
}

#[test]
fn compile_err_on_invalid_source() {
    assert_stdout(
        "compile_bad",
        r#"(? (base.the (Result Text (Int 1)) (base.compile "1 +"))
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
      ? (a.name == "from")
        Bool.True {}  (base.unify a {balance (a.balance - 40)})
        Bool.False {}  (? (a.name == "to")
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

/// A payload-bearing ADT used as a *field inside a record* must round-trip its
/// payload through JSON, not collapse to the bare constructor name. A named
/// ADT field type resolves to `ResolvedType::Named` (multi-variant ADTs stay
/// named so nested references resolve), which made `col_type_str` fall to the
/// `_ => "text"` catch-all — storing just `"Circle"` and dropping the payload,
/// so reading it back gave `Unit` and matching on the payload panicked
/// ("expected Record in field access, got Unit"). The fix re-resolves the
/// named ADT and routes payload-bearing ones to `json` (enums stay `tag`).
#[test]
fn persisted_adt_field_in_record_keeps_payload() {
    assert_stdout(
        "persistadtfield",
        r#"with {
Shape  Circle {radius (Int 1)}  Square {side (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}  {name "b"  sh (Shape.Point {})}]
  rows <- *es
  base.println (base.show rows)
  yield {})"#,
        "\"[{name a  sh Circle {radius 3}}, {name b  sh Point}]\"\n{}",
    );
}

/// A payload-ADT field is content-addressed: two entries holding the *same*
/// value share one row in the child table (`_knot_es__/sh`), keyed by the
/// value's 32-byte content hash, and the parent's `sh` column is a BLOB ref.
#[test]
fn persisted_adt_field_is_content_addressed() {
    let dir = e2e::TempDir::fresh("adtrefdedup");
    e2e::build_in_dir(
        "adtrefdedup",
        r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}  {name "b"  sh (Shape.Circle {radius 3})}  {name "c"  sh (Shape.Point {})}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("adtrefdedup"), dir.path());

    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             n=c.execute('select count(*) from \"_knot_es__/sh\"').fetchone()[0]; \
             typeof=c.execute('select typeof(sh) from _knot_es limit 1').fetchone()[0]; \
             refs=c.execute('select count(distinct sh) from _knot_es').fetchone()[0]; \
             print((n, typeof, refs))",
        )
        .arg(dir.join("adtrefdedup.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    // 3 entries, but Circle{radius 3} is shared: 2 child rows, 2 distinct refs.
    assert!(
        out.contains("(2, 'blob', 2)"),
        "expected 2 deduped child rows + 2 distinct blob refs, got: {out}"
    );
}

/// A nested-record field is content-addressed into a child table with real
/// columns (no JSON): the parent holds a BLOB hash, the child holds the fields.
#[test]
fn persisted_record_field_is_content_addressed() {
    let dir = e2e::TempDir::fresh("recref");
    e2e::build_in_dir(
        "recref",
        r#"with {
Entry  {name Text  addr {city Text  zip (Int 1)}}
Rel Entry  *es
}
(do
  full *es = [{name "a"  addr {city "paris"  zip 75001}}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("recref"), dir.path());

    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             pt=c.execute('select typeof(addr) from _knot_es').fetchone()[0]; \
             child=c.execute('select city, zip, typeof(zip) from \"_knot_es__/addr\"').fetchone(); \
             print((pt, child))",
        )
        .arg(dir.join("recref.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    assert!(
        out.contains("'blob'") && out.contains("('paris', 75001, 'integer')"),
        "record field must be a blob hash + real child columns, got: {out}"
    );
}

/// A scalar-relation field is stored as a content-addressed element table
/// (`_hash` + `_value`) plus a many-to-many link table (`_parent_key`,
/// `_elem_hash`). Shared elements are stored once; membership is edges.
#[test]
fn persisted_scalar_relation_field_is_child_table() {
    let dir = e2e::TempDir::fresh("relscalar");
    e2e::build_in_dir(
        "relscalar",
        r#"with {
Entry  {name Text  tags (Rel Text)}
Rel Entry  *es
}
(do
  full *es = [{name "a"  tags ["x"  "y"]}  {name "b"  tags ["y"  "z"]}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("relscalar"), dir.path());

    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             elems=c.execute('select _value, typeof(_value) from \"_knot_es__/tags\" order by _value').fetchall(); \
             ecols=[r[1] for r in c.execute('pragma table_info(\"_knot_es__/tags\")')]; \
             lcols=[r[1] for r in c.execute('pragma table_info(\"_knot_es__/tags__link\")')]; \
             nlinks=c.execute('select count(*) from \"_knot_es__/tags__link\"').fetchone()[0]; \
             print((elems, ecols, lcols, nlinks))",
        )
        .arg(dir.join("relscalar.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    // "y" is shared by both parents → exactly 3 distinct elements (x,y,z),
    // 4 edges (a→x, a→y, b→y, b→z), element table keyed by _hash with a text
    // _value, link table with _parent_key/_elem_hash.
    assert!(
        out.contains("[('x', 'text'), ('y', 'text'), ('z', 'text')]")
            && out.contains("'_hash'") && out.contains("'_value'")
            && out.contains("'_parent_key'") && out.contains("'_elem_hash'")
            && out.contains(", 4)"),
        "scalar relation must be a content-addressed element table + link edges, got: {out}"
    );
}

/// A relation nested inside an ADT payload field (`Circle {tags (Rel Tag), ..}`)
/// is stored as element+link tables keyed by the ADT child row's `_hash` — the
/// link model lets a content-addressed value own a collection. The ADT child
/// has no column for the relation field.
#[test]
fn persisted_adt_payload_relation_round_trips() {
    assert_stdout(
        "adtpayloadrel",
        r#"with {
Tag  {value Text}
Shape  Circle {tags (Rel Tag)  radius (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {tags [{value "x"}  {value "y"}]  radius 5})}]
  rows <- *es
  base.println (base.show rows)
  yield {})"#,
        "\"[{name a  sh Circle {radius 5  tags [{value x}, {value y}]}}]\"\n{}\n",
    );

    // Physical layout: the ADT child has no `tags` column; the relation lives in
    // element+link tables beneath it.
    let dir = e2e::TempDir::fresh("adtpayloadrel");
    e2e::build_in_dir(
        "adtpayloadrel",
        r#"with {
Tag  {value Text}
Shape  Circle {tags (Rel Tag)  radius (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {tags [{value "x"}]  radius 5})}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("adtpayloadrel"), dir.path());
    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             shcols=[r[1] for r in c.execute('pragma table_info(\"_knot_es__/sh\")')]; \
             tags=c.execute('select value from \"_knot_es__/sh__/tags\"').fetchall(); \
             nlinks=c.execute('select count(*) from \"_knot_es__/sh__/tags__link\"').fetchone()[0]; \
             print((shcols, tags, nlinks))",
        )
        .arg(dir.join("adtpayloadrel.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    // sh child columns are exactly [_hash, _tag, radius] (no `tags` column);
    // the relation's element table holds ('x',) and 1 edge links it.
    assert!(
        out.contains("['_hash', '_tag', 'radius']")
            && out.contains("[('x',)]")
            && out.contains(", 1)"),
        "ADT-payload relation: no tags column on the ADT child, element+link beneath, got: {out}"
    );
}

/// A relation-of-relations field (`grid (Rel (Rel Int))`) round-trips with full
/// content — each inner relation is content-addressed as an element, and its own
/// elements go to a nested link/element table. Previously the inner relations
/// were silently dropped (read back as empty).
#[test]
fn persisted_relation_of_relations_round_trips() {
    assert_stdout(
        "relofrel",
        r#"with {
Entry  {name Text  grid (Rel (Rel (Int 1)))}
Rel Entry  *es
}
(do
  full *es = [{name "a"  grid [[1  2]  [3]]}]
  rows <- *es
  base.println (base.show rows)
  yield {})"#,
        "\"[{grid [[1, 2], [3]]  name a}]\"\n{}\n",
    );
}

/// Payload columns are real, indexable columns on the child table (the point of
/// the content-addressed encoding over JSON): a pushdown filter on a payload
/// field runs against the `radius` INTEGER column.
#[test]
fn persisted_adt_field_payload_column_is_real() {
    let dir = e2e::TempDir::fresh("adtrefcol");
    e2e::build_in_dir(
        "adtrefcol",
        r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("adtrefcol"), dir.path());

    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             cols=[r[1] for r in c.execute('pragma table_info(\"_knot_es__/sh\")')]; \
             ty=c.execute('select typeof(radius) from \"_knot_es__/sh\"').fetchone()[0]; \
             print((cols, ty))",
        )
        .arg(dir.join("adtrefcol.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    assert!(
        out.contains("'radius'") && out.contains("'integer'"),
        "child table must expose a real INTEGER `radius` column, got: {out}"
    );
}

/// Rewriting the source wipes and re-derives the ADT child table — no stale
/// child rows accumulate across a full rewrite (the reclaim path: persistence
/// is delete-all + reinsert, and `delete_record_table` wipes the ADT child).
#[test]
fn persisted_adt_field_rewrite_reclaims_child_rows() {
    let dir = e2e::TempDir::fresh("adtrefrewrite");
    e2e::build_in_dir(
        "adtrefrewrite",
        r#"with {
Shape  Circle {radius (Int 1)}  Point {}
Entry  {name Text  sh Shape}
Rel Entry  *es
}
(do
  full *es = [{name "a"  sh (Shape.Circle {radius 3})}]
  full *es = [{name "b"  sh (Shape.Point {})}]
  yield {})"#,
        dir.path(),
    );
    e2e::run_bin(&dir.join("adtrefrewrite"), dir.path());

    let probe = std::process::Command::new("python3")
        .arg("-c")
        .arg(
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); \
             print(c.execute('select _tag, radius from \"_knot_es__/sh\"').fetchall())",
        )
        .arg(dir.join("adtrefrewrite.db"))
        .output()
        .expect("python3 sqlite probe");
    let out = String::from_utf8_lossy(&probe.stdout);
    // After the rewrite, only Point remains — the Circle child row is gone.
    assert!(
        out.contains("[('Point', None)]"),
        "expected only the Point child row after rewrite, got: {out}"
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

/// A function buried in a named ADT's constructor payload (`f : S` where
/// `S.Wrap {g (Int -> Int)}`) must also be rejected: the field type is the
/// nominal `Con("S")`, so a check that only walks type *args* misses it — the
/// JSON path then silently dropped the payload (storing just `"Wrap"`).
#[test]
fn function_in_adt_payload_is_a_compile_error() {
    let dir = e2e::TempDir::fresh("fnadt");
    let src = r#"with {
S  Wrap {g (Int 1 -> Int 1)}  Nope {}
R  {name Text  f S}
Rel R  *rs
}
(do
  full *rs = [{name "x"  f (S.Wrap {g (\n -> n)})}]
  yield {})"#;
    let src_path = dir.join("fnadt.knot");
    std::fs::write(&src_path, src).unwrap();
    let build = std::process::Command::new(e2e::knot_bin())
        .arg("build")
        .arg(&src_path)
        .arg("-o")
        .arg(dir.join("fnadt"))
        .output()
        .expect("knot build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        !build.status.success(),
        "fn in an ADT constructor payload must fail the build.\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("can't be persisted"),
        "error should say the field can't be persisted: {stderr}"
    );
}

/// SUM over an Int column that overflows i64 must produce a clean overflow
/// panic (matching checked-arithmetic overflow elsewhere), not a raw
/// `query_sum error: integer overflow` followed by "failed to initiate panic,
/// aborting". SQLite's `SUM()` over native INTEGER errors on overflow instead
/// of promoting to REAL, so the runtime's REAL-coercion path never fires —
/// the error must be caught and reported as the intended overflow.
#[test]
fn int_sum_overflow_panics_cleanly() {
    let dir = e2e::TempDir::fresh("sumovf");
    e2e::build_in_dir(
        "sumovf",
        r#"with {
E  {n (Int 1)}
Rel E  *emps
}
(do
  full *emps = [{n 9223372036854775807}  {n 1}]
  base.println (base.show (*emps |> base.map (\e -> e.n) |> base.sum))
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("sumovf"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "overflowing Int SUM must fail, got stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        stderr
    );
    assert!(
        stderr.contains("outside the i64 range") || stderr.contains("overflow"),
        "should report an overflow with a clear message, not a crash-abort.\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("query_sum error"),
        "should not surface the raw SQLite error: {stderr}"
    );
    // Note: the process still aborts with "failed to initiate panic" because
    // every panic that crosses Cranelift-generated code fails to unwind on this
    // platform — the *intended* checked-arith overflow (`i64::MAX + 1` in pure
    // knot) aborts the same way. That is the existing, general JIT-panic
    // behavior, not something this fix changes; the fix is that the *message*
    // is the clean overflow report, not the raw SQLite error.
}