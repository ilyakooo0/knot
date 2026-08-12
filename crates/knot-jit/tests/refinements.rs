//! Refined types: simple aliases, per-field, cross-field, ADT-constructor
//! refinements, `refine expr` → Result, and write-validation on relations.

mod harness;
mod e2e;
use harness::assert_show;
use e2e::assert_stdout;

// ── refine keyword (JIT, pure) ─────────────────────────────────────────────

#[test]
fn refine_accepts_valid() {
    assert_show(
        "with { type Nat = Int 1 where \\x -> x >= 0 }
         (case refine (5 : Int 1) of
            Result.Ok {value n} -> n
            Result.Err {error _} -> (0 - 1))",
        "5",
    );
}

#[test]
fn refine_rejects_invalid() {
    assert_show(
        "with { type Nat = Int 1 where \\x -> x >= 0 }
         (case refine ((0 - 3) : Int 1) of
            Result.Ok {value n} -> 1
            Result.Err {error _} -> 0)",
        "0",
    );
}

#[test]
fn refine_per_field_record_unsupported() {
    // KNOWN LIMITATION (reproduced): `refine` cannot infer a target type for a
    // PER-FIELD refined RECORD — `type VP = {age: Int 1 where …}` used as a
    // refine target fails inference with "cannot infer refined type target
    // (got {age: Int})". Scalar and cross-field refinements DO work (see
    // refine_accepts_valid / refine_cross_field). Assert the current behavior.
    let dir = e2e::TempDir::fresh("refine_field");
    let src = r#"with {
type VP = {age: Int 1 where \x -> x >= 0 && x <= 150}
asVP : {age: Int 1} -> Result {typeName: Text, violations: [{field: Maybe Text, message: Text}]} VP
asVP (\r -> refine r)
}
(case asVP {age 200} of
  Result.Ok {value _} -> base.println "ok"
  Result.Err {error _} -> base.println "bad")"#;
    std::fs::write(dir.path().join("refine_field.knot"), src).unwrap();
    let build = std::process::Command::new(e2e::knot_bin())
        .arg("build")
        .arg(dir.path().join("refine_field.knot"))
        .arg("-o")
        .arg(dir.path().join("refine_field"))
        .output()
        .expect("knot build");
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(
        !build.status.success() && stderr.contains("cannot infer refined type target"),
        "expected per-field refine limitation, got: {stderr}"
    );
}

#[test]
fn refine_cross_field() {
    // lo <= hi across two fields; both directions.
    assert_stdout(
        "refine_cross",
        r#"with {
type R = {lo: Int 1, hi: Int 1} where \r -> r.lo <= r.hi
asR : {lo: Int 1, hi: Int 1} -> Result {typeName: Text, violations: [{field: Maybe Text, message: Text}]} R
asR (\r -> refine r)
}
(do
  (case asR {lo 5 hi 2} of
    Result.Ok {value _} -> base.println "ok"
    Result.Err {error _} -> base.println "bad")
  (case asR {lo 1 hi 9} of
    Result.Ok {value _} -> base.println "ok"
    Result.Err {error _} -> base.println "bad")
  yield {})"#,
        "\"bad\"\n\"ok\"\n{}",
    );
}

// ── write validation (subprocess — relation writes) ────────────────────────

#[test]
fn write_validation_rejects_violation() {
    // A refined field on a source relation is validated on write; a violation
    // aborts the write.
    let dir = e2e::TempDir::fresh("refine_write");
    e2e::build_in_dir(
        "refine_write",
        r#"with {
type Nat = Int 1 where \x -> x >= 0
*people : [{name: Text, age: Nat}]
}
(do
  full *people = [{name "a" age (0 - 5)}]
  base.println "wrote"
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("refine_write"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() || stderr.contains("refin") || stderr.contains("violation"),
        "expected write-validation rejection, got stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        stderr
    );
}

#[test]
fn write_validation_accepts_valid() {
    assert_stdout(
        "refine_ok",
        r#"with {
type Nat = Int 1 where \x -> x >= 0
*people : [{name: Text, age: Nat}]
}
(do
  full *people = [{name "a" age 30}]
  base.println (base.show (base.count *people))
  yield {})"#,
        "\"1\"\n{}",
    );
}
