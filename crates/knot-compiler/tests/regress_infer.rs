//! Regression tests for type-inference bugs (infer.rs / types.rs):
//! rank-2 unsoundness, refine-target resolution, cyclic type aliases,
//! unit algebra, shared-constructor exhaustiveness, and route composition.

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

fn check_full(
    src: &str,
) -> (Vec<Diagnostic>, knot_compiler::infer::RefineTargets) {
    let mut module = parse(src);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, refine_targets, _refined, _json, _elem) =
        knot_compiler::infer::check(&module);
    (diags, refine_targets)
}

fn check_src(src: &str) -> Vec<Diagnostic> {
    check_full(src).0
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

// ── 1. Rank-2 soundness: Forall in required position must skolemise ──

#[test]
fn rank2_forall_in_required_position_is_rejected() {
    // `alias` claims to accept any `Int -> Int`, but its body forwards to a
    // function requiring a polymorphic `forall a. a -> a`. Accepting this
    // lets a monomorphic increment be applied to a Bool at runtime.
    let src = r#"takesPoly : (forall a. a -> a) -> Int
takesPoly = \f -> if f true then f 1 else 0
alias : (Int -> Int) -> Int
alias = takesPoly
main = println (alias (\x -> x + 1))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "rigid type variable"),
        "expected a rigid-variable escape error, got: {:?}",
        diags
    );
}

#[test]
fn rank2_alias_at_same_polytype_is_accepted() {
    // Re-exporting a rank-2 function at the SAME rank-2 type is fine.
    let src = r#"takesPoly : (forall a. a -> a) -> Int
takesPoly = \f -> if f true then f 1 else 0
alias : (forall a. a -> a) -> Int
alias = takesPoly
main = alias (\x -> x)
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn rank2_application_still_works() {
    // Applying a rank-2 function to a polymorphic lambda must keep working
    // (the provided-position instantiation path).
    let src = r#"takesPoly : (forall a. a -> a) -> Int
takesPoly = \f -> if f true then f 1 else 0
main = takesPoly (\x -> x)
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 2. refine target resolution: contextual + deterministic ──────────

#[test]
fn refine_target_honors_annotation_with_shared_base() {
    // Nat and Pos share the base type Int; the annotation must decide.
    let src = r#"type Nat = Int where \x -> x >= 0
type Pos = Int where \x -> x > 0
toNat : Int -> Result RefinementError Nat
toNat = \x -> refine x
main = case toNat 0 of
  Ok {value} -> println "ok"
  Err {error} -> println "err"
"#;
    let (diags, targets) = check_full(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
    let resolved: Vec<&String> = targets.values().collect();
    assert_eq!(resolved.len(), 1, "expected one refine target: {:?}", targets);
    assert_eq!(resolved[0], "Nat", "annotation must pick Nat, not Pos");
}

#[test]
fn refine_target_resolution_is_deterministic() {
    let src = r#"type Nat = Int where \x -> x >= 0
type Pos = Int where \x -> x > 0
toPos : Int -> Result RefinementError Pos
toPos = \x -> refine x
main = toPos 1
"#;
    for _ in 0..20 {
        let (diags, targets) = check_full(src);
        assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
        let resolved: Vec<&String> = targets.values().collect();
        assert_eq!(resolved, vec!["Pos"]);
    }
}

#[test]
fn refine_without_context_and_shared_base_is_ambiguous() {
    let src = r#"type Nat = Int where \x -> x >= 0
type Pos = Int where \x -> x > 0
f = \x -> case refine (x + 0) of
  Ok {value} -> 1
  Err {error} -> 0
main = f 5
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "ambiguous refined type target"),
        "expected ambiguity diagnostic, got: {:?}",
        diags
    );
    assert!(
        has_error(&diags, "Nat, Pos"),
        "candidates should be listed in sorted order: {:?}",
        diags
    );
}

#[test]
fn refine_with_unique_base_still_infers() {
    let src = r#"type Nat = Int where \x -> x >= 0
f = \x -> case refine (x + 0) of
  Ok {value} -> 1
  Err {error} -> 0
main = f 5
"#;
    let (diags, targets) = check_full(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
    let resolved: Vec<&String> = targets.values().collect();
    assert_eq!(resolved, vec!["Nat"]);
}

// ── 3. Cyclic type aliases must be a diagnostic, not a stack overflow ─

#[test]
fn cyclic_type_alias_two_step() {
    let diags = check_src("type A = B\ntype B = A\nmain = 1");
    assert!(
        has_error(&diags, "cyclic type alias"),
        "expected cyclic alias diagnostic, got: {:?}",
        diags
    );
}

#[test]
fn cyclic_type_alias_self_reference() {
    let diags = check_src("type A = A\nmain = 1");
    assert!(has_error(&diags, "cyclic type alias"), "got: {:?}", diags);
}

#[test]
fn cyclic_type_alias_through_record() {
    let diags =
        check_src("type A = {x: B}\ntype B = {y: A}\nmain = 1");
    assert!(has_error(&diags, "cyclic type alias"), "got: {:?}", diags);
}

#[test]
fn alias_chain_referencing_cycle_does_not_diverge() {
    // C is not itself cyclic but reaches one — must terminate with the
    // cycle diagnostic, not hang or overflow.
    let diags = check_src("type A = B\ntype B = A\ntype C = A\nmain = 1");
    assert!(has_error(&diags, "cyclic type alias"), "got: {:?}", diags);
}

#[test]
fn acyclic_forward_alias_chain_still_resolves() {
    let diags = check_src(
        "type A = B\ntype B = C\ntype C = Int\nf : A -> Int\nf = \\x -> x + 1\nmain = f 1",
    );
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn cyclic_alias_in_source_type_does_not_overflow_type_env() {
    // types.rs collect_source_refinements used to recurse without a
    // seen-set; building the TypeEnv must terminate.
    let src = r#"type A = B
type B = A
*xs : [A]
main = do
  x <- *xs
  yield x
"#;
    let module = parse(src);
    let _env = knot_compiler::types::TypeEnv::from_module(&module);
    // Reaching this line is the assertion (no stack overflow).
}

// ── 4. Unit algebra: `*`/`/` with unresolved operand ─────────────────

#[test]
fn mul_with_unresolved_operand_and_unit_is_rejected() {
    let src = r#"unit M
f = \x -> x * 2.0<M>
bad = (f 3.0<M>) + 4.0<M>
main = bad
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "cannot infer the unit"),
        "expected unit-inference diagnostic, got: {:?}",
        diags
    );
}

#[test]
fn mul_with_annotated_operand_composes_units() {
    // With an explicit annotation the product is M^2 and adding M fails.
    let src = r#"unit M
f = \x -> (x : Float<M>) * 2.0<M>
bad = (f 3.0<M>) + 4.0<M>
main = bad
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch: M^2 vs M"),
        "expected composed-unit mismatch, got: {:?}",
        diags
    );
}

#[test]
fn mul_with_dimensionless_annotation_is_accepted() {
    let src = r#"unit M
g = \x -> (x : Float) * 2.0<M>
ok = (g 3.0) + 4.0<M>
main = ok
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn plain_numeric_mul_lambda_still_works() {
    let diags = check_src("f = \\x -> x * 2\nmain = f 3");
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 5. `+`/`-`/`%` must not strip a RHS unit ─────────────────────────

#[test]
fn add_keeps_unit_from_rhs() {
    let src = r#"unit M
unit S
x = 1 + (2 : Int<M>)
bad = x + (3 : Int<S>)
main = bad
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "RHS unit was stripped; expected mismatch, got: {:?}",
        diags
    );
}

#[test]
fn add_keeps_unit_from_lhs() {
    let src = r#"unit M
unit S
x = (2 : Int<M>) + 1
bad = x + (3 : Int<S>)
main = bad
"#;
    let diags = check_src(src);
    assert!(has_error(&diags, "unit mismatch"), "got: {:?}", diags);
}

#[test]
fn add_matching_units_still_fine() {
    let src = r#"unit M
x = 1 + (2 : Int<M>)
ok = x + (3 : Int<M>)
main = ok
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 6. Exhaustiveness with shared constructor names ──────────────────

#[test]
fn shared_ctor_name_does_not_break_exhaustiveness() {
    // B also defines X; matching all of A's constructors must still be
    // recognized as exhaustive regardless of declaration order.
    let src = r#"data A = X {} | Y {}
data B = X {} | Z {}
g = \v -> case v of
  X {} -> 1
  Y {} -> 2
main = g (Y {})
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn shared_ctor_name_other_adt_also_matchable() {
    let src = r#"data A = X {} | Y {}
data B = X {} | Z {}
g = \v -> case v of
  X {} -> 1
  Z {} -> 2
main = g (Z {})
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn shared_ctor_name_still_reports_missing() {
    let src = r#"data A = X {} | Y {}
data B = X {} | Z {}
g = \v -> case v of
  X {} -> 1
main = g (Y {})
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "non-exhaustive pattern match"),
        "got: {:?}",
        diags
    );
}

// ── 7. Route composition: order-independent, unknown names reported ──

#[test]
fn route_composition_is_order_independent() {
    let src = r#"route AApi where
  GET /a -> Text = GetA

route BApi where
  GET /b -> Text = GetB

route All = AB | BApi
route AB = AApi

main = do
  let server = serve All where
    GetA = \r -> do
      yield Ok {value: "a"}
    GetB = \r -> do
      yield Ok {value: "b"}
  listen 8080 server
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn route_composition_unknown_component_is_an_error() {
    let src = r#"route AApi where
  GET /a -> Text = GetA

route All = AApi | Typo
main = 1
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "not a declared route"),
        "typo'd component must not be silently dropped: {:?}",
        diags
    );
}

#[test]
fn route_composition_cycle_is_an_error() {
    let src = "route A = B\nroute B = A\nmain = 1\n";
    let diags = check_src(src);
    assert!(
        has_error(&diags, "cyclic route composition"),
        "got: {:?}",
        diags
    );
}

// ── 8. Trait constraints must survive let-generalization ──

#[test]
fn unannotated_fn_constraint_checked_at_use_site() {
    // `callGreet` is unannotated; the `Greet` obligation from its body must
    // be captured by generalization and re-checked when applied to Bool.
    let src = r#"trait Greet a where
  greet : a -> Text
impl Greet Int where
  greet n = "int"
callGreet = \x -> greet x
main = println (callGreet true)
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "no implementation of trait 'Greet' for type 'Bool'"),
        "dropped constraint must resurface at the use site: {:?}",
        diags
    );
}

#[test]
fn unannotated_fn_ord_constraint_checked_at_use_site() {
    let src = r#"myMin = \a b -> if a < b then a else b
main = println (show (myMin true false))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "no implementation of trait 'Ord' for type 'Bool'"),
        "Ord obligation must follow the generalized scheme: {:?}",
        diags
    );
}

#[test]
fn unannotated_fn_constraint_satisfied_use_accepted() {
    // The same generalized function applied at a type WITH the impl is fine.
    let src = r#"trait Greet a where
  greet : a -> Text
impl Greet Int where
  greet n = "int"
callGreet = \x -> greet x
main = println (callGreet 1)
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn do_let_generalized_constraint_checked_at_use_site() {
    // The do-`let` generalization path must capture constraints too.
    let src = r#"main = do
  let cmp = \a b -> a < b
  println (show (cmp true false))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "no implementation of trait 'Ord' for type 'Bool'"),
        "do-let generalization dropped the Ord constraint: {:?}",
        diags
    );
}

// ── 9. `if` over two concrete IO values with different effects ──

#[test]
fn if_branches_with_different_io_effects_merge() {
    // `*a` and `*b` produce concrete `Ty::IO` values with different
    // read-effect sets; `if` must widen to the union like `case` does.
    let src = r#"*a : [{x: Int}]
*b : [{x: Int}]
main = do
  rows <- if true then *a else *b
  println (show (count rows))
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn annotated_io_effects_still_strict() {
    // Widening branch merges must not weaken explicit annotations: a body
    // with fs effects can't claim `IO {console}`.
    let src = r#"f : IO {console} {}
f = do
  writeFile "x.txt" "y"
main = f
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "IO effects don't match"),
        "explicit annotation must still reject extra effects: {:?}",
        diags
    );
}

// ── 10. Unit-composition guard must cover polymorphic unit variables ──

#[test]
fn unit_var_times_unknown_operand_is_rejected() {
    // `x : Float<u>` (a unit VARIABLE) multiplied by a lambda param that is
    // unresolved at the `*` node must still be rejected — typing `x * y` as
    // `u` instead of `u^2` would be unsound. The composition check is
    // deferred until the operand resolves (here `y` unifies with `x` via
    // the application), so the rejection surfaces as a precise mismatch
    // between the annotated result `u` and the composed product `u^2`.
    let src = r#"sq : Float<u> -> Float<u>
sq = \x -> (\y -> x * y) x
main = println (show (sq 2.0))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "polymorphic unit times its own square must be rejected: {:?}",
        diags
    );
}

#[test]
fn scalar_mul_with_late_resolved_field_operand_compiles() {
    // Scalar (dimensionless) multiplication must work even when the
    // dimensionless operand is a record-field access on an un-annotated
    // lambda parameter whose record type is only pinned AFTER the `*` node
    // (here by `any`'s second argument). The composition check defers
    // instead of demanding an annotation; `f.failures` resolves to `Int`
    // and `base * f.failures` is `Int<Ms>`.
    let src = r#"unit Ms
base : Int<Ms>
base = 30000
cap : Int<Ms>
cap = 480000
shouldRetry : Text -> Int<Ms> -> [{server: Text, failedAt: Int<Ms>, failures: Int}] -> Bool
shouldRetry = \server t failures ->
  not (any (\f -> f.server == server && t - f.failedAt < base * f.failures) failures)
main = println (show (shouldRetry "x" 1000 []))
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn deferred_operand_resolving_to_unit_composes_soundly() {
    // When the late-resolved operand turns out to be unit-bearing, the
    // deferred check must compose units (Ms * Ms = Ms^2), not preserve one
    // side's unit — comparing the product against Int<Ms> is a mismatch.
    let src = r#"unit Ms
base : Int<Ms>
base = 30000
cap : Int<Ms>
cap = 480000
check : [{dur: Int<Ms>}] -> Bool
check = \rows -> any (\f -> cap < base * f.dur) rows
main = println (show (check []))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "Ms * Ms compared against Ms must be rejected: {:?}",
        diags
    );
}

#[test]
fn concrete_unit_times_unknown_operand_still_rejected() {
    let src = r#"unit M
f = \y -> 2.0<M> * y
main = println (show (f 3.0))
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "cannot infer the unit of an operand"),
        "got: {:?}",
        diags
    );
}

#[test]
fn unitless_float_arithmetic_still_compiles() {
    let src = r#"f = \x y -> x * y
main = println (show (f 2.0 3.0))
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 11. User-annotated `\/` effect-union rows ──

#[test]
fn user_annotated_effect_union_sequencing_accepted() {
    // Sequencing both row-polymorphic args inside a do block must satisfy
    // the declared union `r1 \/ r2` instead of forcing r1 = r2.
    let src = r#"combine : IO {| r1} {} -> IO {| r2} {} -> IO {| r1 \/ r2} {}
combine = \a b -> do
  a
  b
main = combine (println "left") (println "right")
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn user_annotated_effect_union_with_io_builtin_accepted() {
    // Variant with a recognizable IO builtin (do block is NOT desugared,
    // exercising the infer_do row-merge path).
    let src = r#"combine : IO {| r1} {} -> IO {| r2} {} -> IO {console | r1 \/ r2} {}
combine = \a b -> do
  println "seq"
  a
  b
main = combine (println "left") (println "right")
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn effect_row_equality_without_union_still_rejected() {
    // Without a declared `\/` union, sequencing two distinct rigid rows
    // into a single-row result must keep failing.
    let src = r#"combine : IO {| r1} {} -> IO {| r2} {} -> IO {| r1} {}
combine = \a b -> do
  a
  b
main = combine (println "left") (println "right")
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "cannot unify rigid type variables"),
        "rigid rows must not silently merge without a `\\/` union: {:?}",
        diags
    );
}

// ── 12. IO-vs-Relation unification in IO do blocks ──

#[test]
fn io_relation_and_plain_relation_branches_unify() {
    // `*items` is `IO {} [T]`; the else branch is a plain `[T]` literal.
    // The relation must unify with the IO's *inner* type, not the
    // relation's element with the inner (which produced nonsense
    // "expected {x: Int}, found [{x: Int}]" mismatches).
    let src = r#"*items : [{x: Int}]
main = do
  rows <- if true then *items else [{x: 1}]
  println (show (count rows))
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 13. Refinement collection must follow plain alias chains ──

fn refinements_for(src: &str, source: &str) -> Vec<(Option<String>, String)> {
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    env.source_refinements
        .get(source)
        .map(|v| {
            v.iter()
                .map(|(field, name, _)| (field.clone(), name.clone()))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn refinement_direct_field_alias_collected() {
    let src = r#"type Nat = Int where \x -> x >= 0
*people : [{age: Nat}]
main = 1
"#;
    let refs = refinements_for(src, "people");
    assert_eq!(refs, vec![(Some("age".to_string()), "Nat".to_string())]);
}

#[test]
fn refinement_field_alias_to_refined_collected() {
    // `Age` is a plain alias to the refined `Nat` — the predicate must
    // still be registered for the field (previously bypassed).
    let src = r#"type Nat = Int where \x -> x >= 0
type Age = Nat
*people : [{age: Age}]
main = 1
"#;
    let refs = refinements_for(src, "people");
    assert_eq!(refs, vec![(Some("age".to_string()), "Nat".to_string())]);
}

#[test]
fn refinement_multi_step_alias_chain_collected() {
    let src = r#"type Nat = Int where \x -> x >= 0
type B = Nat
type A = B
*people : [{age: A}]
main = 1
"#;
    let refs = refinements_for(src, "people");
    assert_eq!(refs, vec![(Some("age".to_string()), "Nat".to_string())]);
}

#[test]
fn refinement_whole_element_alias_collected() {
    let src = r#"type Nat = Int where \x -> x >= 0
type Age = Nat
*scores : [Age]
main = 1
"#;
    let refs = refinements_for(src, "scores");
    assert_eq!(refs, vec![(None, "Nat".to_string())]);
}

#[test]
fn refinement_aliased_record_with_aliased_field_collected() {
    // Record alias containing a field whose type is an alias chain to a
    // refined type.
    let src = r#"type Nat = Int where \x -> x >= 0
type Age = Nat
type Person = {age: Age}
*people : [Person]
main = 1
"#;
    let refs = refinements_for(src, "people");
    assert_eq!(refs, vec![(Some("age".to_string()), "Nat".to_string())]);
}

#[test]
fn refinement_alias_cycle_does_not_hang() {
    // `type A = B; type B = A` — cycle is diagnosed elsewhere; the
    // refinement walk must just terminate without predicates.
    let src = r#"type A = B
type B = A
*xs : [{v: A}]
main = 1
"#;
    let refs = refinements_for(src, "xs");
    assert!(refs.is_empty(), "got: {:?}", refs);
}

#[test]
fn refinement_plain_alias_to_unrefined_not_collected() {
    let src = r#"type Name = Text
*people : [{name: Name}]
main = 1
"#;
    let refs = refinements_for(src, "people");
    assert!(refs.is_empty(), "got: {:?}", refs);
}
