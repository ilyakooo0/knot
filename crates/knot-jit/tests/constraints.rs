//! Subset constraints (referential integrity + uniqueness) and morph.

mod e2e;
use e2e::assert_stdout;

#[test]
fn subset_constraint_accepts_valid() {
    assert_stdout(
        "subset_ok",
        r#"with {
Person  {name Text age (Int 1) email Text}
Rel Person  *people
Rel {customer Text amount (Int 1)}  *orders
*orders.customer <= *people.name
}
(do
  full *people = [{name "Alice" age 30 email "a@x"}  {name "Bob" age 25 email "b@x"}]
  full *orders = [{customer "Alice" amount 100}]
  base.println (base.show (base.count *orders))
  yield {})"#,
        "\"1\"\n{}",
    );
}

#[test]
fn subset_constraint_rejects_orphan() {
    // An order referencing a non-existent customer must be rejected.
    let dir = e2e::TempDir::fresh("subset_orphan");
    e2e::build_in_dir(
        "subset_orphan",
        r#"with {
Person  {name Text age (Int 1) email Text}
Rel Person  *people
Rel {customer Text amount (Int 1)}  *orders
*orders.customer <= *people.name
}
(do
  full *people = [{name "Alice" age 30 email "a@x"}]
  full *orders = [{customer "Nobody" amount 100}]
  base.println "wrote"
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("subset_orphan"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "expected referential-integrity rejection, got stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        stderr
    );
}

#[test]
fn uniqueness_constraint_rejects_duplicate() {
    // `*people <= *people.email` enforces email uniqueness.
    let dir = e2e::TempDir::fresh("subset_uniq");
    e2e::build_in_dir(
        "subset_uniq",
        r#"with {
Person  {name Text email Text}
Rel Person  *people
*people <= *people.email
}
(do
  full *people = [{name "A" email "dup@x"}  {name "B" email "dup@x"}]
  base.println "wrote"
  yield {})"#,
        dir.path(),
    );
    let out = std::process::Command::new(dir.path().join("subset_uniq"))
        .current_dir(dir.path())
        .output()
        .expect("run");
    assert!(
        !out.status.success(),
        "expected uniqueness rejection, got stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn morph_into_text_to_bytes() {
    // base.morph.<from>To<to>.into is consumed via the (^into) projection.
    // A direct call to the record's `into` field converts Text -> Bytes.
    assert_stdout(
        "morph_tb",
        r#"(do
  base.println (base.show (base.morph.textToBytes.into "ab"))
  yield {})"#,
        "\"6162\"\n{}",
    );
}

#[test]
fn now_and_random_return_io() {
    // now/randomInt/randomUuid produce values; just check they run and show
    assert_stdout(
        "io_misc",
        r#"(do
  t <- base.now
  r <- base.randomInt 100
  u <- base.randomUuid
  base.println (base.show (base.bytesLength (base.textToBytes (base.show t)) > 0))
  base.println (base.show (r >= 0 && r < 100))
  base.println (base.show (base.length (base.show u) > 0))
  yield {})"#,
        "\"True\"\n\"True\"\n\"True\"\n{}",
    );
}
