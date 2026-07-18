//! Regression tests for analysis-pass fixes (second batch):
//!
//! 1. Exhaustiveness checking must not count constructor arms with
//!    refutable sub-patterns (literals, nested constructors) as covering
//!    their constructor.
//! 2. IO-promoted comprehensions (plain relation binds or groupBy plus a
//!    final yield) evaluate to the whole relation of yielded values, so
//!    they must be typed `IO [elem]`, not `IO elem`.
//! 3. Impl method bodies are checked against the trait signature with
//!    skolemised method-local type variables — an impl pinning a
//!    caller-chosen variable to a concrete type is rejected.
//! 4. Rank-2 skolems must not escape through unannotated wrappers.
//! 8. Refine-target unification runs before deferred-constraint checking,
//!    so constraints on variables concretized by refine resolution are
//!    actually validated.
//! 9. `subst_ty` recurses into `Ty::Alias` bodies (and alias references
//!    freshen their free vars), so an alias like `type Box = {val: a}`
//!    can be used at two different types.
//! 10. A do-block whose IO lives inside an *applied* lambda routes through
//!     the IO path instead of being desugared as a pure comprehension.
//! 11. `fetchWith` options are type-checked against the runtime's expected
//!     `{headers: [{name, value}]}` shape, and `fetch` on a non-route
//!     constructor is a type error instead of a codegen panic.

use knot::diagnostic::Diagnostic;

fn parse(src: &str) -> knot::ast::Module {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(
        parse_diags.is_empty(),
        "unexpected parse diagnostics: {:?}",
        parse_diags
    );
    module
}

fn check_src(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── 1. Exhaustiveness with refutable sub-patterns ───────────────────

#[test]
fn literal_payload_does_not_cover_constructor() {
    // `Circle {radius: 1.0}` matches only one radius — Circle is NOT
    // covered, so the match must be rejected as non-exhaustive.
    let diags = check_src(
        r#"data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}

describe = \s -> case s of
  Circle {radius: 1.0} -> "unit circle"
  Rect r -> "rect"

main = println (describe (Circle {radius: 2.0}))
"#,
    );
    assert!(
        has_error(&diags, "non-exhaustive pattern match"),
        "expected non-exhaustive error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
    assert!(
        has_error(&diags, "Circle"),
        "the missing constructor should be named: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn wildcard_fixes_literal_payload_match() {
    let diags = check_src(
        r#"data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}

describe = \s -> case s of
  Circle {radius: 1.0} -> "unit circle"
  Rect r -> "rect"
  _ -> "other"

main = println (describe (Circle {radius: 2.0}))
"#,
    );
    assert_clean(&diags);
}

#[test]
fn irrefutable_payloads_still_exhaustive() {
    let diags = check_src(
        r#"data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}

describe = \s -> case s of
  Circle c -> "circle"
  Rect {width: w, height: h} -> "rect"

main = println (describe (Circle {radius: 2.0}))
"#,
    );
    assert_clean(&diags);
}

#[test]
fn nested_constructor_payload_does_not_cover() {
    // `Wrap {s: Circle c}` only matches Wrap values holding a Circle —
    // a Rect payload would panic at runtime, so a wildcard is required.
    let diags = check_src(
        r#"data Shape = Circle {radius: Float 1} | Rect {width: Float 1}
data Wrapper = Wrap {s: Shape}

unwrap = \w -> case w of
  Wrap {s: Circle c} -> c.radius

main = println (show (unwrap (Wrap {s: Circle {radius: 1.0}})))
"#,
    );
    assert!(
        has_error(&diags, "non-exhaustive pattern match"),
        "expected non-exhaustive error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn cons_with_literal_head_does_not_cover_nonempty_lists() {
    let diags = check_src(
        r#"f = \xs -> case xs of
  [] -> 0
  Cons 1 rest -> 1

main = println (show (f [2, 3]))
"#,
    );
    assert!(
        has_error(&diags, "non-exhaustive pattern match"),
        "expected non-exhaustive error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn cons_with_irrefutable_head_and_tail_is_exhaustive() {
    let diags = check_src(
        r#"f = \xs -> case xs of
  [] -> 0
  Cons h t -> h

main = println (show (f [2, 3]))
"#,
    );
    assert_clean(&diags);
}

// ── 2. IO-promoted comprehensions type as IO [elem] ─────────────────

#[test]
fn groupby_comprehension_types_as_io_relation() {
    // `workload` accumulates one row per group, so its type must be
    // `IO [{owner, n}]` — `count workload` only typechecks as a list.
    let diags = check_src(
        r#"*todos : [{title: Text, owner: Text, done: Int 1}]

main = do
  replace *todos = [{title: "a", owner: "Alice", done: 0}]
  let workload = do
    t <- *todos
    where t.done == 0
    groupBy {t.owner}
    yield {owner: t.owner, n: count t}
  c <- println (show (count workload))
  w <- workload
  p <- println (w.owner ++ ": " ++ show w.n)
  yield {}
"#,
    );
    assert_clean(&diags);
}

#[test]
fn io_block_with_plain_relation_bind_yields_list() {
    // The plain bind `x <- [1, 2]` makes the block a comprehension that
    // loops and accumulates its yields, so `xs : IO [Int]` and `count ys`
    // must typecheck.
    let diags = check_src(
        r#"xs = do
  x <- [1, 2]
  p <- println (show x)
  yield x

main = do
  ys <- xs
  q <- println (show (count ys))
  yield {}
"#,
    );
    assert_clean(&diags);
}

#[test]
fn io_block_without_comprehension_binds_keeps_pure_yield() {
    // No relation binds: `yield 42` is the block's result value, so the
    // block types as `IO Int` (not `IO [Int]`).
    let diags = check_src(
        r#"answer = do
  p <- println "computing"
  yield 42

main = do
  n <- answer
  q <- println (show (n + 1))
  yield {}
"#,
    );
    assert_clean(&diags);
}

// ── 3. Impl methods checked against skolemised trait signature ──────

#[test]
fn impl_pinning_method_type_var_rejected() {
    // `conv : a -> b` promises a caller-chosen `b`; the impl returning
    // Int pins it and must be rejected.
    let diags = check_src(
        r#"trait Conv a where
  conv : a -> b

impl Conv Int where
  conv = \x -> x + 1

main = println "x"
"#,
    );
    assert!(
        has_error(&diags, "less polymorphic"),
        "expected 'less polymorphic' error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn impl_leaving_method_type_var_polymorphic_ok() {
    let diags = check_src(
        r#"trait Pick a where
  pick : a -> b -> a

impl Pick Int where
  pick = \x y -> x

main = println (show (pick 1 "anything"))
"#,
    );
    assert_clean(&diags);
}

// ── 4. Rank-2 skolem escape through unannotated wrappers ────────────

#[test]
fn rank2_skolem_escape_through_wrapper_rejected() {
    // `g`'s param type gets bound toward the skolem of `takesPoly`'s
    // argument; generalizing it would let `g (\x -> x + 1)` pass a
    // monomorphic function where a polymorphic one is required.
    let diags = check_src(
        r#"takesPoly : (forall a. a -> a) -> Int 1
takesPoly = \f -> f 1

g = \h -> takesPoly h

main = do
  let r = g (\x -> x + 1)
  println (show r)
"#,
    );
    assert!(
        has_error(&diags, "escape"),
        "expected escape error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn direct_polymorphic_argument_ok() {
    let diags = check_src(
        r#"takesPoly : (forall a. a -> a) -> Int 1
takesPoly = \f -> f 1

main = println (show (takesPoly (\x -> x)))
"#,
    );
    assert_clean(&diags);
}

// ── 8. Refine-target unification before constraint checking ─────────

#[test]
fn constraint_on_refine_concretized_var_is_checked() {
    // `foo x` defers a `Foo` constraint on x's type variable (which stays
    // out of main's generalized type — `x <- []` keeps it monomorphic and
    // local); the refine annotation resolves x to Int (Nat's base) only
    // during refine-target unification. Pre-fix, check_constraints ran
    // BEFORE that unification, hit the Ty::Var skip, and the missing
    // `Foo Int` impl was silently not reported.
    let diags = check_src(
        r#"trait Foo a where
  foo : a -> Int 1

type Nat = Int 1 where \x -> x >= 0

main = do
  x <- []
  let n = foo x
  let r = ((refine x) : Result RefinementError Nat)
  yield n
"#,
    );
    assert!(
        has_error(&diags, "no implementation of trait 'Foo'"),
        "expected missing-impl error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// ── 9. Alias bodies with free type variables ─────────────────────────

#[test]
fn alias_with_free_var_usable_at_two_types() {
    let diags = check_src(
        r#"type Box = {val: a}

b1 : Box
b1 = {val: 1}

b2 : Box
b2 = {val: "s"}

main = println "ok"
"#,
    );
    assert_clean(&diags);
}

// ── 10. Applied lambdas with IO bodies route through the IO path ────

#[test]
fn applied_lambda_io_do_block_typechecks() {
    // Pre-fix, the desugarer treated `(\u -> println (show u)) x` as
    // pure, desugared the block as a comprehension, and inference
    // produced a confusing IO-vs-relation mismatch.
    let diags = check_src(
        r#"main = do
  x <- [1, 2]
  (\u -> println (show u)) x
"#,
    );
    assert_clean(&diags);
}

#[test]
fn applied_lambda_io_do_block_not_desugared_as_pure() {
    use knot::ast::{DeclKind, ExprKind};
    let mut module = parse(
        r#"main = do
  x <- [1, 2]
  (\u -> println (show u)) x
"#,
    );
    knot_compiler::desugar::desugar(&mut module);
    let main_body = module
        .decls
        .iter()
        .find_map(|d| match &d.node {
            DeclKind::Fun { name, body: Some(body), .. } if name == "main" => {
                Some(body)
            }
            _ => None,
        })
        .expect("main decl");
    assert!(
        matches!(&main_body.node, ExprKind::Do(_)),
        "IO do-block must remain a Do node for the IO codegen path, got: {:?}",
        main_body.node
    );
}

// ── 11. fetchWith options / non-route constructors ──────────────────

#[test]
fn fetchwith_bad_options_rejected() {
    let diags = check_src(
        r#"route Api where
  GET /users/{id: Int 1} -> {name: Text} = GetUser

main = do
  r <- fetchWith "http://localhost:1" 42 (GetUser {id: 1})
  yield {}
"#,
    );
    assert!(
        has_error(&diags, "type mismatch"),
        "expected type mismatch on options, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn fetchwith_headers_options_accepted() {
    let diags = check_src(
        r#"route Api where
  GET /users/{id: Int 1} -> {name: Text} = GetUser

main = do
  r <- fetchWith "http://localhost:1" {headers: [{name: "X-A", value: "b"}]} (GetUser {id: 1})
  yield {}
"#,
    );
    assert_clean(&diags);
}

#[test]
fn fetch_on_non_route_constructor_rejected() {
    // Pre-fix this passed inference and PANICKED the compiler in codegen
    // ("fetch: no route entry found for constructor 'Mk'").
    let diags = check_src(
        r#"data Foo = Mk {x: Int 1}

main = do
  r <- fetch "http://localhost:1" (Mk {x: 1})
  yield {}
"#,
    );
    assert!(
        has_error(&diags, "not a route constructor"),
        "expected route-constructor error, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn fetch_bare_nullary_route_constructor_accepted() {
    // B37: `fetch url Ctor` with a bare nullary route constructor was
    // spuriously rejected in inference — `record_arg` fell back to the
    // Constructor node itself, whose inferred type is the ADT (`Api`),
    // then unified against the empty expected record ("expected {}, found
    // Api"). fetch_ctor_name and compile_fetch both already support the
    // form; only inference rejected it, and only for multi-endpoint routes.
    let diags = check_src(
        r#"route Api where
  GET /health -> {status: Text} = Health
  GET /users/{id: Int 1} -> {name: Text} = GetUser

main = do
  r <- fetch "http://localhost:1" Health
  yield {}
"#,
    );
    assert_clean(&diags);
}

#[test]
fn fetchwith_bare_nullary_route_constructor_accepted() {
    // Same bare-constructor form, through fetchWith (which adds an
    // options argument before the constructor).
    let diags = check_src(
        r#"route Api where
  GET /health -> {status: Text} = Health
  GET /users/{id: Int 1} -> {name: Text} = GetUser

main = do
  r <- fetchWith "http://localhost:1" {headers: [{name: "X-A", value: "b"}]} Health
  yield {}
"#,
    );
    assert_clean(&diags);
}

// A `race`/`fork` result's effect-union row must not be laundered through a
// value annotated with fewer effects. Passing `race (console) (console)` to a
// parameter typed `IO {}` is an effect-soundness violation: the union resolves
// to `{console}` *after* body checking, and the post-hoc resolution must not
// silently overwrite the closed `IO {}` requirement the argument was checked
// against. The fix records the closed upper bound at the unify site and
// re-checks it once the union is known.
#[test]
fn race_result_cannot_launder_effects_through_pure_param() {
    let diags = check_src(
        r#"runPure : IO {} (Result {} {}) -> IO {console} {}
runPure = \io -> do
  r <- io
  println "ran"

main : IO {console} {}
main = runPure (race (println "a") (println "b"))
"#,
    );
    assert!(
        has_error(&diags, "effects not allowed by the expected type"),
        "expected effect-laundering rejection, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn fork_result_cannot_launder_effects_through_pure_param() {
    let diags = check_src(
        r#"runPure : IO {} {} -> IO {console} {}
runPure = \io -> do
  r <- io
  println "ran"

main : IO {console} {}
main = runPure (fork (println "a"))
"#,
    );
    assert!(
        has_error(&diags, "effects not allowed by the expected type"),
        "expected fork effect-laundering rejection, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

// The common, correct usages must still type-check: an upper bound recorded
// at a *larger* closed requirement — or the do-block monad row whose effects
// the declaration's own annotation governs — accommodates the union.
#[test]
fn race_in_io_do_block_still_type_checks() {
    let diags = check_src(
        r#"main : IO {console} {}
main = do
  r <- race (println "a") (println "b")
  println "done"
"#,
    );
    assert_clean(&diags);
}

#[test]
fn race_result_through_matching_effect_param_type_checks() {
    let diags = check_src(
        r#"runConsole : IO {console} (Result {} {}) -> IO {console} {}
runConsole = \io -> do
  r <- io
  println "ran"

main : IO {console} {}
main = runConsole (race (println "a") (println "b"))
"#,
    );
    assert_clean(&diags);
}
