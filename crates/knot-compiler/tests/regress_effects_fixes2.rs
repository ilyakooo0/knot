//! Regression tests for effect-inference fixes (second batch):
//!
//! 5. Impl method bodies and trait default bodies are visible to effect
//!    inference: a trait-method call site carries the union of effects
//!    across all known impls, so IO inside an impl can't run inside
//!    `atomic`, and relation access through a trait method satisfies the
//!    "atomic must interact with relations" validation.
//! 6. A let-bound lambda referenced *by name* (not as a lambda literal)
//!    carries its body's effects.
//! 7. Unannotated higher-order helpers propagate the effects of their
//!    lambda arguments (only signatures declaring a closed effect row
//!    absorb them).
//! 12. The "atomic block must interact with relations" hard error stays
//!     quiet when the body calls something that is not provably
//!     relation-free (e.g. a parameter-typed callable).

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

fn assert_has_error(diags: &[Diagnostic], message_contains: &str) {
    assert!(
        diags
            .iter()
            .any(|d| d.severity == Severity::Error
                && d.message.contains(message_contains)),
        "expected error containing {:?}, got: {:?}",
        message_contains,
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

fn assert_no_error(diags: &[Diagnostic], message_contains: &str) {
    assert!(
        !diags.iter().any(|d| d.message.contains(message_contains)),
        "expected NO diagnostic containing {:?}, got: {:?}",
        message_contains,
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── 5. Trait/impl bodies visible to effect inference ────────────────

#[test]
fn impl_method_console_io_inside_atomic_rejected() {
    // `tick`'s impl calls println; the trait-method call inside the
    // atomic block must surface that console effect.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

trait Ticker a where
  tick : a -> IO _ {}

impl Ticker Int where
  tick = \x -> println "tick"

main = do
  r <- atomic (do
    rows <- *items
    t <- tick 1
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn trait_default_body_io_inside_atomic_rejected() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]

trait Ticker a where
  tick : a -> IO _ {}
  tick = \x -> println "default tick"

impl Ticker Int where
  tick = \x -> println "impl tick"

main = do
  r <- atomic (do
    rows <- *items
    t <- tick 1
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn atomic_relation_access_via_trait_method_accepted() {
    // The atomic block's only relation interaction goes through a trait
    // method whose impl reads *items — pre-fix this was a false
    // "atomic block must interact with relations" error.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

trait Reader a where
  readAll : a -> IO _ [{n: Int}]

impl Reader Int where
  readAll = \x -> *items

main = do
  r <- atomic (do
    t <- readAll 1
    yield {})
  yield {}
"#,
    );
    assert_no_error(&diags, "atomic block must interact with relations");
    assert_no_diags(&diags);
}

#[test]
fn trait_method_union_includes_all_impls() {
    // Two impls with different effects: a call site must carry the UNION
    // (here, the console effect of the Text impl) even when only the Int
    // impl could be the dynamic target — conservative and sound.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

trait Ticker a where
  tick : a -> IO _ {}

impl Ticker Int where
  tick = \x -> yield {}

impl Ticker Text where
  tick = \x -> println "noisy"

main = do
  r <- atomic (do
    rows <- *items
    t <- tick 1
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

// ── 6. Let-bound lambda referenced by name carries effects ──────────

#[test]
fn let_bound_lambda_passed_by_name_carries_console_effect() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]

runIt : (Int -> IO {| e} {}) -> IO {| e} {}
runIt = \f -> f 1

main = do
  r <- atomic (do
    rows <- *items
    let cb = \u -> println "x"
    q <- runIt cb
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

// ── 7. Unannotated higher-order helpers propagate lambda effects ─────

#[test]
fn unannotated_hof_propagates_lambda_io_into_atomic_check() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]

apply = \f x -> f x

main = do
  r <- atomic (do
    rows <- *items
    q <- apply (\u -> println "x") 1
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn closed_row_annotation_still_absorbs_callback_effects() {
    // `quiet` declares a closed effect row (`IO {r *items} {}`): per the
    // documented annotation semantics it absorbs its callback's effects,
    // so the caller must NOT be charged with console.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

quiet : (Int -> IO {console} {}) -> IO {r *items} {}
quiet = \f -> *items

caller : IO {r *items} {}
caller = quiet (\x -> println "boo")

main = do
  c <- caller
  yield {}
"#,
    );
    assert_no_error(&diags, "inferred effects exceed declared effects");
}

#[test]
fn pure_lambda_args_add_no_effects() {
    // Conservative propagation must not invent effects for pure lambdas.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

apply = \f x -> f x

main : IO {console} {}
main = do
  n <- apply (\u -> u + 1) 1
  p <- println (show n)
  yield {}
"#,
    );
    assert_no_diags(&diags);
}

// ── 12. Opaque callables suppress the must-interact hard error ───────

#[test]
fn atomic_calling_parameter_typed_callable_not_flagged() {
    // `action` is a lambda parameter — the body is not provably
    // relation-free, so the hard error must stay quiet.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

helper = \action -> atomic (do
  r <- action {}
  yield {})

main = do
  q <- helper (\u -> *items)
  yield {}
"#,
    );
    assert_no_error(&diags, "atomic block must interact with relations");
}

#[test]
fn inner_binder_shadowing_let_effectful_name_is_not_laundered() {
    // `log` is a let-bound console lambda. The inner `\log -> log` shadows it
    // with an opaque parameter of the same name; a reference to that parameter
    // must NOT resolve to the outer binding's `{console}` effect (which would
    // spuriously trip the IO-in-atomic gate). The atomic body only writes.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

apply = \g x -> g x

main = do
  atomic (do
    let log = \u -> println "hi"
    _ <- apply (\log -> log) (*items)
    replace *items = [])
  yield {}
"#,
    );
    assert_no_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn provably_relation_free_atomic_still_rejected() {
    let diags = effect_diags(
        r#"main = do
  n <- atomic (do
    let x = 1 + 2
    yield x)
  yield {}
"#,
    );
    assert_has_error(&diags, "atomic block must interact with relations");
}

// ── Pipe-LHS lambda effect propagation ──────────────────────────────
//
// `x |> f` desugars to `f x`, so a lambda on the LHS is an *argument* to
// the row-polymorphic callee. The effect checker must thread the lambda
// body's effects through exactly as it does for the direct-application
// form `f x` — otherwise the console write below is invisible and slips
// past the IO-in-atomic gate (the unsafe under-approximation direction).

#[test]
fn pipe_lhs_lambda_io_propagates_into_atomic_check() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]

apply = \f -> f 1

main = do
  r <- atomic (do
    rows <- *items
    q <- (\u -> println "x") |> apply
    yield {})
  yield {}
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn pipe_lhs_lambda_io_matches_direct_application() {
    // The pipe form must agree with the equivalent `apply (\u -> ...)`
    // direct application: the caller is charged with `console`, so a
    // `main : IO {} {}`-style pure context would be rejected. Here we just
    // confirm the console effect reaches `main` by exceeding a declared
    // empty row.
    let diags = effect_diags(
        r#"apply = \f -> f 1

main : IO {} {}
main = do
  q <- (\u -> println "x") |> apply
  yield {}
"#,
    );
    assert_has_error(&diags, "inferred effects exceed declared effects");
}

#[test]
fn pipe_lhs_pure_lambda_adds_no_effects() {
    // Symmetric to `pure_lambda_args_add_no_effects`: a pure lambda piped
    // into a HOF must not invent effects.
    let diags = effect_diags(
        r#"apply = \f -> f 1

main : IO {console} {}
main = do
  n <- (\u -> u + 1) |> apply
  p <- println (show n)
  yield {}
"#,
    );
    assert_no_diags(&diags);
}

#[test]
fn io_laundered_into_atomic_via_record_field_rejected() {
    // A lambda doing console IO is stashed in a record field, the record is
    // passed into a known helper, and the helper invokes the field inside an
    // `atomic` block. The IO is hidden behind an opaque (field-access) call one
    // level down the call chain and the lambda is bound in the enclosing
    // declaration — the atomic gate must still reject it.
    let diags = effect_diags(
        r#"*items : [{id: Int}]

makeRec = \cb -> {fn: cb}

useRec = \r -> do
  xs <- *items
  (r.fn) {}

doStuff = do
  let r = makeRec (\u -> println "hidden IO inside atomic")
  atomic (useRec r)

main = doStuff
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn clean_atomic_in_impl_method_not_poisoned_by_unrelated_io_lambda() {
    // A clean `atomic` block (relation writes only) inside an IMPL METHOD must
    // compile. The atomic gate's enclosing-body root lookup keys on
    // `current_decl_name`/`decl_bodies`; during the final diagnostic re-walk
    // these used to be a stale leftover from the last top-level fun (`noise`
    // here), whose body holds an unrelated `\u -> println` IO lambda. Rooting
    // the laundered-IO search there wrongly attributed that lambda to the
    // method's atomic block — a false positive that rejected a valid program.
    let diags = effect_diags(
        r#"*items : [{id: Int}]

writeItems = \u -> replace *items = [{id: 7}]

useRec = \r -> do
  xs <- *items
  (r.fn) {}

trait Doer a where
  doIt : a -> IO {rw *items} {}

impl Doer Int where
  doIt = \x -> do
    let r = {fn: writeItems}
    atomic (useRec r)

main : IO {rw *items} {}
main = doIt 1

noise : IO {console} {}
noise = do
  let bad = \u -> println "unrelated IO in another decl"
  println "x"
"#,
    );
    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("IO effects are not allowed inside atomic blocks")),
        "clean atomic in impl method wrongly flagged: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn pure_hof_builtin_in_atomic_not_flagged_opaque() {
    // `sortBy`/`countWhere`/`minOn`/`maxOn` are pure builtins; they were
    // missing from `PURE_BUILTINS`, so the atomic gate's opaque-callee scan
    // treated a call to them as a call through an opaque value. Combined with a
    // legitimate IO call (`now`) in the enclosing declaration but OUTSIDE the
    // atomic block, the gate wrongly rooted its laundered-IO search at the
    // whole declaration and rejected the (DB-only) atomic body.
    let diags = effect_diags(
        r#"*items : [{n: Int, expiresAt: Int}]

handle = do
  t <- now
  atomic (do
    rows <- *items
    *items = sortBy (\x -> x.expiresAt) rows)

main = handle
"#,
    );
    assert_no_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn prelude_hof_callback_param_in_atomic_not_flagged_opaque() {
    // A prelude HOF (`findFirst`) applies its own callback parameter inside its
    // body. The opaque-callee scan used to recurse into `findFirst` and flag
    // `pred x` as a call through an opaque value (the parameter), producing a
    // false "IO not allowed in atomic" when the enclosing decl did IO (`now`)
    // outside the atomic. A callee's own params are now treated as known.
    let diags = effect_diags(
        r#"*items : [{n: Int}]

lookup = \rows -> findFirst rows (\r -> r.n > 0)

handle = do
  t <- now
  atomic (do
    rows <- *items
    let hit = lookup rows
    *items = rows)

main = handle
"#,
    );
    assert_no_error(&diags, "IO effects are not allowed inside atomic blocks");
}

// ── fork reached through a helper inside atomic ─────────────────────

#[test]
fn fork_via_helper_inside_atomic_accepted() {
    // `fork` is intentionally permitted inside atomic (its spawned IO runs on an
    // independent connection). When the fork lives one call deep — `helper`
    // forks console IO — the atomic gate used to falsely reject the block: the
    // helper's stored effects baked in the forked `console`, and the syntactic
    // opaque-callee scan saw a reachable IO lambda. Both views are now
    // fork-stripped, so this is accepted.
    let diags = effect_diags(
        r#"*people : [{name: Text}]

helper = \u -> fork (println "hi")

main =
  atomic (do
    _ <- *people
    helper {})
"#,
    );
    assert_no_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn non_forked_io_via_helper_inside_atomic_still_rejected() {
    // The fork-stripping must not over-permit: a helper that does *direct*
    // (non-forked) console IO inside atomic is still rejected.
    let diags = effect_diags(
        r#"*people : [{name: Text}]

helper = \u -> println "hi"

main =
  atomic (do
    _ <- *people
    helper {})
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}

#[test]
fn helper_mixing_direct_and_forked_io_inside_atomic_rejected() {
    // A helper that forks some IO but ALSO performs direct IO is rejected for
    // the direct part — only the forked subtree is exempt.
    let diags = effect_diags(
        r#"*people : [{name: Text}]

helper = \u -> do
  _ <- println "direct"
  fork (println "forked")

main =
  atomic (do
    _ <- *people
    helper {})
"#,
    );
    assert_has_error(&diags, "IO effects are not allowed inside atomic blocks");
}
