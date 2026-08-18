//! Concurrency e2e: base.fork / base.race / base.sleep.

mod e2e;
use e2e::{assert_stdout, run_program};

/// Assert the program exits 0 and its stdout lines, as a sorted set, equal
/// `expected` (sorted). For concurrent programs where line order is
/// nondeterministic.
fn assert_stdout_unordered(name: &str, src: &str, expected: &[&str]) {
    let (out, err, code) = run_program(name, src);
    assert_eq!(code, 0, "program {name} exited {code}\nstderr:\n{err}");
    let mut got: Vec<&str> = out.trim_end().lines().collect();
    let mut want: Vec<&str> = expected.to_vec();
    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want, "stdout line-set mismatch for {name}");
}

#[test]
fn fork_runs_concurrently() {
    assert_stdout_unordered(
        "fork",
        r#"(do
  base.fork (base.println "from fork")
  base.println "from main"
  base.sleep 200
  yield {})"#,
        &["\"from fork\"", "\"from main\"", "{}"],
    );
}

#[test]
fn race_returns_winner() {
    // race kills the loser. Here the fast branch wins and the slow branch
    // (sleep 200) is killed before its println. Observed: the winner's result
    // surfaces as Result.Ok. Deterministic (fast never sleeps).
    assert_stdout(
        "race",
        r#"(do
  winner <- base.race
    (do
      base.sleep 200
      base.println "slow"
      yield {})
    (base.println "fast")
  base.println (base.show winner)
  yield {})"#,
        "\"fast\"\n\"Ok {value {}}\"\n{}",
    );
}

#[test]
fn sleep_pauses() {
    assert_stdout(
        "sleep",
        r#"(do
  base.println "a"
  base.sleep 50
  base.println "b"
  yield {})"#,
        "\"a\"\n\"b\"\n{}",
    );
}
