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
