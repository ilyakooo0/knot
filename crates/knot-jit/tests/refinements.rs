//! Refined types: simple aliases, per-field, cross-field, ADT-constructor
//! refinements, `refine expr` → Result, and write-validation on relations.

mod e2e;
mod harness;
use e2e::assert_stdout;
use harness::assert_show;

// ── refine keyword (JIT, pure) ─────────────────────────────────────────────

#[test]
fn refine_accepts_valid() {
    assert_show(
        "with { Nat  Int 1 where \\x ->  x >= 0}\n         (match refine (base.the (Int 1) 5)\n            Result.Ok {value n}  n\n            Result.Err {error _}  (0 - 1))",
        "5",
    );
}

#[test]
fn refine_rejects_invalid() {
    assert_show(
        "with { Nat  Int 1 where \\x ->  x >= 0}\n         (match refine (base.the (Int 1) (0 - 3))\n            Result.Ok {value n}  1\n            Result.Err {error _}  0)",
        "0",
    );
}

#[test]
fn refine_per_field_record() {
    // `refine` targets a record alias whose refinement lives on a field
    // (`VP  {age (Int 1 where …)}`). The alias is registered with a
    // synthesized whole-record predicate (`\r -> r.age >= 0 && r.age <= 150`),
    // so a valid record yields Ok and an out-of-range field yields Err.
    assert_stdout(
        "refine_field_ok",
        r#"with {
VP  {age (Int 1 where \x -> x >= 0 && x <= 150)}
{age (Int 1)} -> Result {typeName Text  violations (Rel {field (Maybe Text)  message Text})} VP  asVP  (\r -> refine r)
}
(match asVP {age 30}
  Result.Ok {value _}  base.println "ok"
  Result.Err {error _}  base.println "bad")"#,
        "\"ok\"\n{}",
    );
    assert_stdout(
        "refine_field_bad",
        r#"with {
VP  {age (Int 1 where \x -> x >= 0 && x <= 150)}
{age (Int 1)} -> Result {typeName Text  violations (Rel {field (Maybe Text)  message Text})} VP  asVP  (\r -> refine r)
}
(match asVP {age 200}
  Result.Ok {value _}  base.println "ok"
  Result.Err {error _}  base.println "bad")"#,
        "\"bad\"\n{}",
    );
}

#[test]
fn refine_cross_field() {
    // lo <= hi across two fields; both directions.
    assert_stdout(
        "refine_cross",
        r#"with {
R  {lo (Int 1)  hi (Int 1)} where \r ->  r.lo <=  r.hi
{lo (Int 1)  hi (Int 1)} -> Result {typeName Text  violations (Rel {field (Maybe Text)  message Text})} R  asR  (\r -> refine r)
}
(do
  (match asR {lo 5  hi 2}
    Result.Ok {value _}  base.println "ok"
    Result.Err {error _}  base.println "bad")
  (match asR {lo 1  hi 9}
    Result.Ok {value _}  base.println "ok"
    Result.Err {error _}  base.println "bad")
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
Nat  Int 1 where \x ->  x >= 0
Rel {name Text  age Nat}  *people
}
(do
  full *people = [{name "a"  age (0 - 5)}]
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
Nat  Int 1 where \x ->  x >= 0
Rel {name Text  age Nat}  *people
}
(do
  full *people = [{name "a"  age 30}]
  base.println (base.show (base.count *people))
  yield {})"#,
        "\"1\"\n{}",
    );
}