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
type Todo = {owner: Text, done: Int 1}
*todos : [Todo]
}
(do
  full *todos = [{owner "x" done 0} {owner "x" done 1} {owner "y" done 0}]
  groups <- (do
    t <- *todos
    where t.done == 0
    groupBy {owner t.owner}
    yield {owner t.owner count (base.count t)})
  base.println (base.show groups)
  yield {})"#,
        // Set semantics: identical rows are deduped on write (INSERT OR
        // IGNORE), so distinct open rows only. owners x and y each have 1.
        "\"[{count: 1, owner: x}, {count: 1, owner: y}]\"\n{}",
    );
}

#[test]
fn persisted_relation_read_write() {
    assert_stdout(
        "persist",
        r#"with {
type C = {n: Int 1}
*cs : [C]
}
(do
  full *cs = [{n 1} {n 2} {n 3}]
  rows <- full *cs
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
fn atomic_transfer() {
    // `atomic do ...` returns the relation written; bind it (or `_`) so the
    // enclosing do-block's value isn't the relation.
    assert_stdout(
        "atomic",
        r#"with {
type Account = {name: Text, balance: Int 1}
*accounts : [Account]
}
(do
  full *accounts = [{name "from" balance 100} {name "to" balance 0}]
  _ <- atomic do
    rows <- full *accounts
    *accounts = base.map (\a ->
      case a.name == "from" of
        Bool.True {} -> (base.unify a {balance (a.balance - 40)})
        Bool.False {} -> (case a.name == "to" of
          Bool.True {} -> (base.unify a {balance (a.balance + 40)})
          Bool.False {} -> a)) rows
    yield {}
  base.println (base.show (full *accounts))
  yield {})"#,
        "\"[{balance: 60, name: from}, {balance: 40, name: to}]\"\n{}",
    );
}
