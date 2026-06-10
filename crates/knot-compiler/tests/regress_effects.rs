//! Regression tests for effect-inference bugs.
//!
//! Each test mirrors the real compiler pipeline up to the effect checker
//! (lex → parse → prelude injection → desugar → `effects::check`) so the
//! checker sees the same AST shapes `knot build` would hand it.

use knot::diagnostic::{Diagnostic, Severity};

fn effect_diags(src: &str) -> Vec<Diagnostic> {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        !lex_diags.iter().any(|d| d.severity == Severity::Error),
        "lex errors: {:?}",
        lex_diags
    );
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (mut module, parse_diags) = parser.parse_module();
    assert!(
        !parse_diags.iter().any(|d| d.severity == Severity::Error),
        "parse errors: {:?}",
        parse_diags
    );
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    knot_compiler::effects::check(&module)
}

fn assert_no_diags(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no effect diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

fn assert_has_error(diags: &[Diagnostic], message_contains: &str, note_contains: &str) {
    assert!(
        diags.iter().any(|d| {
            d.severity == Severity::Error
                && d.message.contains(message_contains)
                && (note_contains.is_empty()
                    || d.notes.iter().any(|n| n.contains(note_contains)))
        }),
        "expected error containing {:?} (note containing {:?}), got: {:?}",
        message_contains,
        note_contains,
        diags
            .iter()
            .map(|d| (d.severity, &d.message, &d.notes))
            .collect::<Vec<_>>()
    );
}

// ── Bug 1: derived relations must see user-function effects ─────────

/// A derived relation whose body calls a user function declared with no
/// annotation must inherit that function's effects (`r *b` here). The old
/// implementation ran the derived/view fixpoint before the funs fixpoint,
/// so the derived relation's effect set stayed empty forever.
#[test]
fn derived_relation_inherits_function_effects_missing_annotation_flagged() {
    let diags = effect_diags(
        r#"
type B = {x: Int}
*b : [B]

readsB = \u -> *b

&d = readsB {}

main : IO {console} {}
main = do
  rows <- &d
  println (show (count rows))
  yield {}
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects", "r *b");
}

/// The flip side: with the correct annotation there must be no bogus
/// "declared effects are not used: {r *b}" warning.
#[test]
fn derived_relation_correct_annotation_accepted_without_warning() {
    let diags = effect_diags(
        r#"
type B = {x: Int}
*b : [B]

readsB = \u -> *b

&d = readsB {}

main : IO {r *b, console} {}
main = do
  rows <- &d
  println (show (count rows))
  yield {}
"#,
    );
    assert_no_diags(&diags);
}

// ── Bug 2: lambda effects propagate from every argument position ────

/// A lambda passed as a NON-final argument to a row-polymorphic callee must
/// contribute its effects. The old App handler only applied lambda
/// propagation to the outermost (syntactically last) argument.
#[test]
fn lambda_effects_propagate_from_non_final_argument() {
    let diags = effect_diags(
        r#"
withCb2 : (Int -> IO {| e} {}) -> Int -> IO {| e} {}
withCb2 = \cb x -> cb x

main : IO {} {}
main = withCb2 (\u -> println "hi") 1
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects", "console");
}

/// Same program with the effect declared — accepted, no warnings.
#[test]
fn lambda_effects_in_non_final_argument_accepted_when_declared() {
    let diags = effect_diags(
        r#"
withCb2 : (Int -> IO {| e} {}) -> Int -> IO {| e} {}
withCb2 = \cb x -> cb x

main : IO {console} {}
main = withCb2 (\u -> println "hi") 1
"#,
    );
    assert_no_diags(&diags);
}

/// The final-argument case must keep working too.
#[test]
fn lambda_effects_propagate_from_final_argument() {
    let diags = effect_diags(
        r#"
withCb : Int -> (Int -> IO {| e} {}) -> IO {| e} {}
withCb = \x cb -> cb x

main : IO {} {}
main = withCb 1 (\u -> println "hi")
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects", "console");
}

// ── Bug 3: parameter-side effect rows do not license body effects ───

/// Effects declared only on a callback PARAMETER's type must not count as
/// the function's own declared effects: the result row here is `IO {}`,
/// so a body that prints must be rejected.
#[test]
fn param_side_effect_annotation_does_not_license_body_effects() {
    let diags = effect_diags(
        r#"
f : (Int -> IO {console} {}) -> IO {} {}
f = \cb -> println "leaky"
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects", "console");
}

/// Declaring the effect on the RESULT side still works.
#[test]
fn result_side_effect_annotation_accepted() {
    let diags = effect_diags(
        r#"
f : (Int -> IO {console} {}) -> IO {console} {}
f = \cb -> println "ok"
"#,
    );
    assert_no_diags(&diags);
}

// ── Bug 4: atomic relation reads through local bindings ─────────────

/// A relation read performed through a let-bound lambda inside the atomic
/// body must not trigger the "atomic block must interact with relations"
/// hard error.
#[test]
fn atomic_relation_read_through_local_binding_accepted() {
    let diags = effect_diags(
        r#"
type Item = {id: Int}
*items : [Item]

main = do
  n <- atomic (do
    let f = \u -> *items
    rows <- f {}
    yield (count rows))
  println (show n)
  yield {}
"#,
    );
    assert_no_diags(&diags);
}

/// The local-lambda tracking must also surface the read in the enclosing
/// declaration's inferred effects (so annotations stay accurate).
#[test]
fn local_lambda_relation_read_counts_toward_declared_effects() {
    let diags = effect_diags(
        r#"
type Item = {id: Int}
*items : [Item]

main : IO {console} {}
main = do
  n <- atomic (do
    let f = \u -> *items
    rows <- f {}
    yield (count rows))
  println (show n)
  yield {}
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects", "*items");
}

/// An atomic block that truly never touches relations is still rejected.
#[test]
fn atomic_with_no_relation_ops_still_rejected() {
    let diags = effect_diags(
        r#"
main = do
  n <- atomic (do
    let f = \u -> 42
    x <- f {}
    yield x)
  println (show n)
  yield {}
"#,
    );
    assert_has_error(&diags, "atomic block must interact with relations", "");
}

/// IO through a let-bound lambda called inside atomic is now caught by the
/// IO-in-atomic check (the local binding's effects are visible).
#[test]
fn atomic_io_through_local_binding_rejected() {
    let diags = effect_diags(
        r#"
type Item = {id: Int}
*items : [Item]

main = do
  n <- atomic (do
    rows <- *items
    let f = \u -> println "side effect"
    x <- f {}
    yield (count rows))
  println (show n)
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks", "console");
}

// ── Bug 5: `race` rejected inside atomic ─────────────────────────────

#[test]
fn race_inside_atomic_rejected() {
    let diags = effect_diags(
        r#"
type Counter = {n: Int}
*counter : [Counter]

main = do
  r <- atomic (race *counter *counter)
  println "done"
  yield {}
"#,
    );
    assert_has_error(&diags, "`race` cannot be used inside atomic blocks", "");
}

/// Indirect usage through a locally-bound lambda inside the atomic body is
/// caught by the same syntactic walk.
#[test]
fn race_inside_atomic_via_local_lambda_rejected() {
    let diags = effect_diags(
        r#"
type Counter = {n: Int}
*counter : [Counter]

main = do
  r <- atomic (do
    let f = \u -> race *counter *counter
    x <- f {}
    yield x)
  println "done"
  yield {}
"#,
    );
    assert_has_error(&diags, "`race` cannot be used inside atomic blocks", "");
}

/// `race` outside atomic remains allowed.
#[test]
fn race_outside_atomic_allowed() {
    let diags = effect_diags(
        r#"
slow = do
  sleep 1000<Ms>
  yield "slow"

fast = do
  sleep 50<Ms>
  yield "fast"

main = do
  r <- race slow fast
  println "done"
  yield {}
"#,
    );
    assert_no_diags(&diags);
}
