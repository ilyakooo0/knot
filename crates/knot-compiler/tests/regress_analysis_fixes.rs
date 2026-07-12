//! Regression tests for analysis-pass fixes:
//! 1. unit-var generalization through substituted lambda params
//! 2. Var x Var unit composition under `*`/`/`
//! 3. `race` laundered into `atomic` through a helper
//! 4. IO lambda laundered into `atomic` through an opaque (record-field) callee
//! 5. effectful lambda checked against a closed `IO {}` row (type level)
//! 6. view-write effect attribution to the backing source
//! 7. parameterized ADT fields (Maybe/Result) persisted as json columns
//! 8. nested/stacked/ADT-constructor refinement enforcement
//! 9. migrate with relation-wrapped types
//! 10. monad_info keying across merged files (prelude/import span collisions)
//! 11. expr_is_io and do-local let-bound IO lambdas
//! 12. unused-source detection for relations referenced only by `migrate`
//! 13. `traverse f []` empty-input result per applicative
//! 14. unbounded inlining of a recursive helper hung codegen
//! 15. int literals outside the i64 range (and the unwritable `i64::MIN`)
//! 16. relation literals deduplicate — a relation is a set
//! 17. binding a nested-relation field iterates every element

use knot::diagnostic::Diagnostic;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

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
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _refine, _refined, _json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

/// Mirror the LSP pipeline (prelude → desugar → TypeEnv) and run the SQL lint.
fn sql_lint_diags(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    knot_compiler::sql_lint::check(&module, &env)
}

fn effect_diags(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    knot_compiler::effects::check(&module)
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

// ── End-to-end harness (mirrors regress_sql_pushdown.rs) ──────────

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Compile `source` (plus any extra files) into a fresh scratch directory.
fn compile_files(test_name: &str, source: &str, extra: &[(&str, &str)]) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_analysis_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    for (name, text) in extra {
        fs::write(dir.join(name), text).unwrap();
    }
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let knot = env!("CARGO_BIN_EXE_knot");
    let out = Command::new(knot)
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    assert!(
        out.status.success(),
        "knot build failed for {test_name}:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let exe = dir.join("prog");
    Compiled { dir, exe }
}

fn run(c: &Compiled) -> (String, String, bool) {
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

fn compile_and_run(test_name: &str, source: &str) -> (String, String, bool) {
    let c = compile_files(test_name, source, &[]);
    run(&c)
}

/// Compile a program expected to FAIL to build; returns the compiler's stderr.
fn compile_expect_error(test_name: &str, source: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_analysis_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let knot = env!("CARGO_BIN_EXE_knot");
    let out = Command::new(knot)
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let succeeded = out.status.success();
    let _ = fs::remove_dir_all(&dir);
    assert!(
        !succeeded,
        "expected knot build to fail for {test_name}, but it succeeded"
    );
    stderr
}

fn lex_diags(src: &str) -> Vec<Diagnostic> {
    let (_, diags) = knot::lexer::Lexer::new(src).tokenize();
    diags
}

// ── 1. collect_free_unit_vars must follow the substitution ─────────

#[test]
fn unit_var_behind_substituted_lambda_param_is_not_generalized() {
    // `p` is bound as Scheme::mono(Var α); `stripFloatUnit p` substitutes
    // α := Float<u1>. The let-generalization of `g` must NOT quantify u1
    // (it is env-bound through p), so using g at both <M> and <S> is a
    // unit mismatch — previously this compiled.
    let src = r#"unit M
unit S
bad = \p -> do
  let stripped = stripFloatUnit p
  let g = \y -> y + p
  println (show (g 1.0<M>))
  println (show (g 1.0<S>))
  yield {}
main = bad 2.0<M>
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "expected unit mismatch from monomorphic env-bound unit var, got: {:?}",
        diags
    );
}

// ── 2. Var x Var unit composition is deferred, not unified ─────────

#[test]
fn var_times_var_composes_units_instead_of_unifying() {
    // area : composing M * M = M^2; adding M must be rejected.
    let src = r#"unit M
area = \w h -> w * h
v = (area 3.0<M> 4.0<M>) + 5.0<M>
main = println (show v)
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "M * M used at M must be a unit mismatch (M^2 vs M), got: {:?}",
        diags
    );
}

#[test]
fn constrained_annotation_defers_unit_composition() {
    // B12: with an explicit `Num a =>` constraint the scheme goes through the
    // `has_constraints` branch, which rebuilds the type from the annotation
    // with fresh vars. The deferred `x * y` unit-binop was captured against
    // the body-check skolems, which vanish from the rebuilt type, so at each
    // call site the binop's result floated free of the return type and
    // resolution degraded to a vacuous unify — the M^2 product was wrongly
    // accepted as Float<M>. Re-mapping the skolems onto the fresh vars ties
    // the product back to `a`, so `M * M` used at `M` is a unit mismatch.
    let src = r#"unit M
scale : Num a => a -> a -> a
scale = \x y -> x * y
v = (scale 3.0<M> 4.0<M>) + 1.0<M>
main = println (show v)
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "unit mismatch"),
        "M * M from a `Num a =>`-constrained scale used at M must be a unit \
         mismatch (M^2 vs M), got: {:?}",
        diags
    );
}

#[test]
fn constrained_annotation_dimensionless_use_still_ok() {
    // Companion to B12: the same constrained function must remain usable at
    // dimensionless numeric types, and each call site must freshen its own
    // deferred composition (Int and Float uses independently).
    let src = r#"scale : Num a => a -> a -> a
scale = \x y -> x * y
i = scale 3 4
f = scale 5.0 6.0
main = do
  println (show i)
  println (show f)
"#;
    let diags = check_src(src);
    assert!(
        diags.is_empty(),
        "constrained scale at dimensionless types must type-check, got: {:?}",
        diags
    );
}

#[test]
fn var_times_var_accepts_mixed_units() {
    // Float<M> * Float<S> through an unannotated lambda must be ACCEPTED
    // (the old code unified both operands and falsely rejected).
    let src = r#"unit M
unit S
f = \x y -> x * y
v = f 3.0<M> 4.0<S>
main = println (show v)
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn var_times_var_dimensionless_still_works() {
    let src = r#"f = \x y -> x * y
main = println (show (f 2.0 3.0))
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 3. race through a wrapper is rejected inside atomic ───────────

#[test]
fn race_through_helper_rejected_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]
raceIt = \a b -> race a b
main = do
  c <- atomic (do
    rows <- *items
    r <- raceIt (yield 1) (yield 2)
    yield (count rows))
  println (show c)
"#,
    );
    assert!(
        has_error(&diags, "cannot be used inside atomic"),
        "race laundered through raceIt must be rejected: {:?}",
        diags
    );
}

#[test]
fn shadowed_race_in_helper_not_flagged() {
    // A helper whose *parameter* is named `race` is not the builtin.
    let diags = effect_diags(
        r#"*items : [{n: Int}]
pickIt = \race -> count race
main = do
  c <- atomic (do
    rows <- *items
    yield (pickIt rows))
  println (show c)
"#,
    );
    assert!(
        !has_error(&diags, "cannot be used inside atomic"),
        "shadowed `race` param wrongly flagged: {:?}",
        diags
    );
}

// ── 4. IO lambda reachable through an opaque callee in atomic ─────

#[test]
fn record_field_io_lambda_rejected_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]
r = {fn: \u -> println "hidden"}
main = do
  c <- atomic (do
    rows <- *items
    r.fn {}
    yield (count rows))
  println (show c)
"#,
    );
    assert!(
        has_error(&diags, "IO effects are not allowed inside atomic"),
        "IO lambda hidden in a record field must be rejected: {:?}",
        diags
    );
}

#[test]
fn pure_record_field_lambda_still_allowed_inside_atomic() {
    let diags = effect_diags(
        r#"*items : [{n: Int}]
r = {fn: \u -> u}
main = do
  c <- atomic (do
    rows <- *items
    r.fn {}
    yield (count rows))
  println (show c)
"#,
    );
    assert!(
        !has_error(&diags, "IO effects are not allowed inside atomic"),
        "pure record-field lambda wrongly rejected: {:?}",
        diags
    );
}

// ── 5. effectful lambda cannot check against closed IO {} ─────────

#[test]
fn effectful_lambda_rejected_against_closed_empty_io_row() {
    let src = r#"runIt : ({} -> IO {} {}) -> IO {} {}
runIt = \cb -> cb {}
main = runIt (\u -> println "laundered")
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "IO effects don't match"),
        "console lambda checked against IO {{}} must be rejected: {:?}",
        diags
    );
}

#[test]
fn pure_lambda_accepted_where_console_io_expected() {
    // The sound subsumption direction must keep working: an IO with FEWER
    // effects than the expected row is fine.
    let src = r#"runIt : ({} -> IO {console} {}) -> IO {console} {}
runIt = \cb -> cb {}
main = runIt (\u -> yield {})
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 6. view writes attribute effects to the backing source ────────

#[test]
fn view_write_attributes_effects_to_backing_source() {
    let src = r#"*people : [{name: Text, age: Int}]
*adults = do
  p <- *people
  where p.age >= 18
  yield p
writeView = \rows -> replace *adults = rows
main = println "ok"
"#;
    let module = parse(src);
    let (_diags, effects) = knot_compiler::effects::check_with_effects(&module);
    let wv = &effects["writeView"];
    assert!(
        wv.writes.contains("people"),
        "writing through the view must write the backing source, got: {}",
        wv
    );
    assert!(
        wv.reads.contains("people"),
        "writing through the view must read the backing source, got: {}",
        wv
    );
    assert!(
        wv.writes.contains("adults"),
        "the view itself is still written, got: {}",
        wv
    );
}

// ── 7. parameterized ADT fields persist as json columns ───────────

#[test]
fn maybe_field_maps_to_json_column() {
    let src = r#"type User = {name: Text, nick: Maybe Text}
*users : [User]
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(
        env.source_schemas["users"], "name:text,nick:json",
        "Maybe Text field must use the json column type"
    );
}

#[test]
fn parameterized_single_variant_data_type_substitutes_args() {
    // `data Box a = Box {value: a}` applied as `Box Int` must produce the
    // schema "value:int" — substituting the type argument into the field.
    // Before the fix, the resolved alias collapsed the type parameter to
    // `Named("unknown")`, producing the meaningless "value:text".
    let src = r#"data Box a = Box {value: a}
*boxes : [Box Int]
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(
        env.source_schemas["boxes"], "value:int",
        "Box Int must substitute Int for the type parameter (got {:?})",
        env.source_schemas["boxes"]
    );
}

#[test]
fn alias_applying_later_declared_parameterized_data_type_resolves() {
    // B32: a type alias that *applies* a parameterized data type declared
    // AFTER the alias must still resolve to the correct shape. During the
    // first resolution pass `Box` is not yet registered, so `Box Int`
    // collapses to `Named("unknown")` and the alias would ship the
    // meaningless `_value:text` schema. The re-resolve pass must recover the
    // application from the alias's original AST once `Box` is known, yielding
    // `value:int`.
    let src = r#"type Wrapped = [Box Int]
data Box a = Box {value: a}
*w : Wrapped
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(
        env.source_schemas["w"], "value:int",
        "alias applying later-declared `Box Int` must resolve to value:int (got {:?})",
        env.source_schemas["w"]
    );
}

#[test]
fn parameterized_single_variant_data_type_substitutes_record_arg() {
    // Substitution must recurse into structured arguments, producing the
    // "json" column type for a record-typed parameter.
    let src = r#"data Box a = Box {value: a}
*boxes : [Box {name: Text}]
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(
        env.source_schemas["boxes"], "value:json",
        "Box {{name: Text}} must use the json column type (got {:?})",
        env.source_schemas["boxes"]
    );
}

#[test]
fn maybe_field_round_trips_just_and_nothing() {
    let (stdout, stderr, ok) = compile_and_run(
        "maybe_field_roundtrip",
        r#"type User = {name: Text, nick: Maybe Text}
*users : [User]
main = do
  replace *users = [{name: "al", nick: Just {value: "big al"}}, {name: "bo", nick: Nothing {}}]
  rows <- *users
  u <- rows
  where u.name == "al"
  case u.nick of
    Just {value} -> println ("nick: " ++ value)
    _ -> println "none"
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("nick: big al"),
        "Just value must round-trip through SQLite, got: {stdout}"
    );
}

#[test]
fn nothing_field_round_trips() {
    let (stdout, stderr, ok) = compile_and_run(
        "nothing_field_roundtrip",
        r#"type User = {name: Text, nick: Maybe Text}
*users : [User]
main = do
  replace *users = [{name: "bo", nick: Nothing {}}]
  rows <- *users
  u <- rows
  case u.nick of
    Just {value} -> println ("nick: " ++ value)
    _ -> println "none"
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("none"),
        "Nothing must round-trip through SQLite, got: {stdout}"
    );
}

// ── 8. nested refinement enforcement ───────────────────────────────

#[test]
fn refined_field_inside_nested_relation_is_enforced() {
    let (_stdout, stderr, ok) = compile_and_run(
        "refine_nested_relation",
        r#"type Pos = Int where \x -> x > 0
type Order = {id: Int, items: [{qty: Pos}]}
*orders : [Order]
main = do
  replace *orders = [{id: 1, items: [{qty: 0 - 5}]}]
  println "should not be reached"
  yield {}
"#,
    );
    assert!(!ok, "violating nested-relation refinement must abort");
    assert!(
        stderr.contains("refinement violation"),
        "expected refinement violation, got: {stderr}"
    );
}

#[test]
fn refined_field_inside_nested_record_alias_is_enforced() {
    let (_stdout, stderr, ok) = compile_and_run(
        "refine_nested_record_alias",
        r#"type Zip = Int where \x -> x > 0
type Addr = {zip: Zip}
type Person = {name: Text, addr: Addr}
*people : [Person]
main = do
  replace *people = [{name: "al", addr: {zip: 0 - 1}}]
  println "should not be reached"
  yield {}
"#,
    );
    assert!(!ok, "violating nested-record-alias refinement must abort");
    assert!(
        stderr.contains("refinement violation"),
        "expected refinement violation, got: {stderr}"
    );
}

#[test]
fn adt_constructor_field_refinement_is_enforced() {
    let (_stdout, stderr, ok) = compile_and_run(
        "refine_adt_ctor_field",
        r#"data Shape = Circle {radius: Float where \r -> r > 0.0} | Rect {w: Float}
*shapes : [Shape]
main = do
  replace *shapes = [Circle {radius: 0.0 - 1.0}]
  println "should not be reached"
  yield {}
"#,
    );
    assert!(!ok, "violating ADT constructor refinement must abort");
    assert!(
        stderr.contains("refinement violation"),
        "expected refinement violation, got: {stderr}"
    );
}

#[test]
fn stacked_inline_over_refined_alias_enforces_both() {
    // `age: Nat where \x -> x < 150` — the Nat predicate (x >= 0) must
    // still apply; previously only the inline predicate was kept.
    let (_stdout, stderr, ok) = compile_and_run(
        "refine_stacked",
        r#"type Nat = Int where \x -> x >= 0
type Person = {age: Nat where \x -> x < 150}
*people : [Person]
main = do
  replace *people = [{age: 0 - 5}]
  println "should not be reached"
  yield {}
"#,
    );
    assert!(!ok, "violating the aliased Nat predicate must abort");
    assert!(
        stderr.contains("refinement violation"),
        "expected refinement violation, got: {stderr}"
    );
}

#[test]
fn valid_nested_refined_data_still_inserts() {
    let (stdout, stderr, ok) = compile_and_run(
        "refine_nested_valid",
        r#"type Pos = Int where \x -> x > 0
type Zip = Int where \x -> x > 0
type Addr = {zip: Zip}
data Shape = Circle {radius: Float where \r -> r > 0.0} | Rect {w: Float}
type Order = {id: Int, items: [{qty: Pos}], addr: Addr}
*orders : [Order]
*shapes : [Shape]
main = do
  replace *orders = [{id: 1, items: [{qty: 3}], addr: {zip: 90210}}]
  replace *shapes = [Circle {radius: 1.5}, Rect {w: 2.0}]
  println "all good"
  yield {}
"#,
    );
    assert!(ok, "valid data must not trip nested refinements: {stderr}");
    assert!(stdout.contains("all good"), "got: {stdout}");
}

// ── 9. migrate with relation-wrapped types ─────────────────────────

#[test]
fn migrate_bracketed_relation_types_produce_record_schemas() {
    let src = r#"type Order = {customer: Text, qty: Int}
*orders : [Order]
migrate *orders from [{customer: Text}] to [{customer: Text, qty: Int}] using \r -> {customer: r.customer, qty: 0}
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    let migs = &env.migrate_schemas["orders"];
    assert_eq!(
        migs[0].0, "customer:text",
        "from-schema must unwrap the relation"
    );
    assert_eq!(
        migs[0].1, "customer:text,qty:int",
        "to-schema must unwrap the relation"
    );
}

#[test]
fn sql_lint_suppressed_when_user_primitive_impl_disables_pushdown() {
    // An out-of-order pipe (`take` before `filter`) cannot push to SQL, so the
    // lint normally reports it. But when the program defines a user operator
    // impl on a primitive type, codegen disables SQL pushdown wholesale and
    // evaluates everything in memory — so the lint must stay silent rather
    // than imply (by reporting only this construct) that others push down.
    let base = r#"*items : [{name: Text, qty: Int}]
firstFew : IO {} [{name: Text, qty: Int}]
firstFew = *items |> take 3 |> filter (\m -> m.qty > 0)
main = println "ok"
"#;
    let baseline = sql_lint_diags(base);
    assert!(
        !baseline.is_empty(),
        "out-of-order pipe should normally produce a pushdown lint"
    );

    let with_impl = format!("{base}\nimpl Eq Int where eq = \\a b -> True\n");
    let gated = sql_lint_diags(&with_impl);
    assert!(
        gated.is_empty(),
        "a user primitive operator impl must suppress pushdown lints, got: {:?}",
        gated.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn migrate_scalar_source_runs_end_to_end() {
    // Two-phase: build/run a scalar `Int` source (seeds db + lockfile), then
    // build/run a version that migrates it to `Float`. Exercises both the
    // compile-time schema match (the `_value:` wrapping) and the runtime
    // migration transform (unwrap `{_value: x}` → bare scalar → migrate fn →
    // re-wrap), which previously failed at compile time and then at runtime.
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_analysis_migscalar_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let knot = env!("CARGO_BIN_EXE_knot");
    let src = dir.join("prog.knot");

    let build_run = |source: &str| -> (String, bool) {
        fs::write(&src, source).unwrap();
        let build = Command::new(knot)
            .arg("build")
            .arg(&src)
            .current_dir(&dir)
            .output()
            .expect("spawn knot build");
        if !build.status.success() {
            return (String::from_utf8_lossy(&build.stderr).into_owned(), false);
        }
        let out = Command::new(dir.join("prog"))
            .current_dir(&dir)
            .output()
            .expect("run prog");
        (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            out.status.success(),
        )
    };

    let (_, ok1) = build_run("*counter : Int\nmain = do\n  *counter = 5\n  yield {}\n");
    assert!(ok1, "phase 1 (Int source) should build and run");

    let (stdout, ok2) = build_run(
        "*counter : Float\n\
         migrate *counter from Int to Float using \\old -> 0.0\n\
         main = do\n  c <- *counter\n  println (show c)\n  yield {}\n",
    );
    let _ = fs::remove_dir_all(&dir);
    assert!(ok2, "phase 2 (migrate Int→Float) should build and run: {stdout}");
    assert!(
        stdout.contains("0.0"),
        "migrated scalar value should be 0.0, got: {stdout}"
    );
}

#[test]
fn migrate_scalar_source_schema_matches_source_schema() {
    // A scalar source is stored under a `_value:<scalar>` schema. The migrate
    // path must wrap its from/to schemas the same way; otherwise the lockfile
    // (`_value:int`) and the migrate descriptor (`int`) never match and scalar
    // source migrations are impossible.
    let src = r#"*counter : Float
migrate *counter from Int to Float using \old -> 0.0
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(
        env.source_schemas["counter"], "_value:float",
        "scalar source schema is _value-wrapped"
    );
    let migs = &env.migrate_schemas["counter"];
    assert_eq!(
        migs[0],
        ("_value:int".to_string(), "_value:float".to_string()),
        "migrate from/to schemas must be _value-wrapped to match the source/lockfile schema"
    );
}

#[test]
fn migrate_relation_of_scalar_schema_matches_source_schema() {
    // Same contract for a relation-of-scalar source (`[Text]` → `_value:text`).
    let src = r#"*tags : [Int]
migrate *tags from [Text] to [Int] using \r -> r
main = println "ok"
"#;
    let module = parse(src);
    let env = knot_compiler::types::TypeEnv::from_module(&module);
    assert_eq!(env.source_schemas["tags"], "_value:int");
    let migs = &env.migrate_schemas["tags"];
    assert_eq!(
        migs[0],
        ("_value:text".to_string(), "_value:int".to_string()),
    );
}

#[test]
fn migrate_bracketed_relation_types_run_end_to_end() {
    let (stdout, stderr, ok) = compile_and_run(
        "migrate_bracketed",
        r#"type Order = {customer: Text, qty: Int}
*orders : [Order]
migrate *orders from [{customer: Text}] to [{customer: Text, qty: Int}] using \r -> {customer: r.customer, qty: 0}
main = do
  rows <- *orders
  println ("rows: " ++ show (count rows))
  yield {}
"#,
    );
    assert!(ok, "bracketed migrate must not panic at startup: {stderr}");
    assert!(stdout.contains("rows: 0"), "got: {stdout}");
}

// ── 10. monad_info keying across merged files ──────────────────────

#[test]
fn do_blocks_at_identical_offsets_in_different_files_use_their_own_monads() {
    // lib.knot holds a Maybe comprehension; prog.knot holds a [Int]
    // comprehension. The padding comment places prog's `do` at exactly the
    // same byte offset as lib's `do`, which used to collide in monad_info
    // (spans are not shifted when imported modules are merged) and compile
    // the Maybe do-block with Relation binds — a runtime panic.
    let lib = "f1 : Maybe Int -> Maybe Int\nf1 = \\m -> do\n  x <- m\n  yield (x + 1)\n";
    let lib_do_off = lib.find("do").unwrap();
    let prefix = "import ./lib\n";
    let fn_line = "f2 = \\r -> do";
    // Pad with a comment so that prog's `do` starts at lib_do_off.
    let do_col = fn_line.len() - 2; // offset of "do" within fn_line
    let pad_len = lib_do_off
        .checked_sub(prefix.len() + do_col + 1) // +1 for the newline after the comment
        .expect("lib do offset too small for padding");
    let padding = format!("--{}\n", "p".repeat(pad_len.saturating_sub(3)));
    let mut prog = String::new();
    prog.push_str(prefix);
    prog.push_str(&padding);
    prog.push_str(fn_line);
    prog.push_str("\n  x <- r\n  yield (x + 1)\n");
    prog.push_str(
        r#"main = do
  let res = f1 (Just {value: 41})
  case res of
    Just {value} -> println ("maybe: " ++ show value)
    _ -> println "none"
  println ("list: " ++ show (count (f2 [1, 2, 3])))
  yield {}
"#,
    );
    let c = compile_files("monad_span_collision", &prog, &[("lib.knot", lib)]);
    let (stdout, stderr, ok) = run(&c);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("maybe: 42"),
        "Maybe do-block must use the Maybe monad, got: {stdout}"
    );
    assert!(
        stdout.contains("list: 3"),
        "[Int] do-block must use the Relation monad, got: {stdout}"
    );
}

// ── 11. do-local let-bound IO lambdas ──────────────────────────────

#[test]
fn let_bound_io_lambda_in_do_compiles_and_runs() {
    // Previously desugared as a pure comprehension (expr_is_io ignored
    // let-bound lambda bodies) and failed with a misleading type error.
    let (stdout, stderr, ok) = compile_and_run(
        "let_bound_io_lambda",
        r#"main = do
  let f = \y -> println (show y)
  f 1
  f 2
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains('1') && stdout.contains('2'),
        "let-bound IO lambda must run, got: {stdout}"
    );
}

// ── 12. migrate counts as a use of its relation ────────────────────

#[test]
fn source_referenced_only_by_migrate_is_not_unused() {
    let src = r#"*orders : [{customer: Text}]
migrate *orders from [{customer: Text}] to [{customer: Text}] using \r -> r
main = println "ok"
"#;
    let module = parse(src);
    let diags = knot_compiler::unused::check(&module.decls);
    assert!(
        !diags
            .iter()
            .any(|d| d.message.contains("unused source: `orders`")),
        "migrated source wrongly flagged unused: {:?}",
        diags
    );
}

// ── 13. traverse over an empty relation ────────────────────────────

#[test]
fn traverse_empty_in_io_context_yields_empty_relation() {
    let (stdout, stderr, ok) = compile_and_run(
        "traverse_empty_io",
        r#"sendIt = \x -> println (show x.n)
noRows : [{n: Int}]
noRows = []
main = do
  results <- traverse sendIt noRows
  println ("count: " ++ show (count results))
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("count: 0"),
        "traverse f [] in IO must produce IO [], got: {stdout}"
    );
}

#[test]
fn traverse_empty_in_maybe_context_yields_just_empty() {
    let (stdout, stderr, ok) = compile_and_run(
        "traverse_empty_maybe",
        r#"half = \x -> if x.n > 0 then Just {value: x.n} else Nothing {}
noRows : [{n: Int}]
noRows = []
main = do
  let res = traverse half noRows
  case res of
    Just {value} -> println ("got: " ++ show (count value))
    _ -> println "nothing"
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("got: 0"),
        "traverse f [] in Maybe must produce Just [], got: {stdout}"
    );
}

#[test]
fn traverse_nonempty_still_dispatches_on_elements() {
    let (stdout, stderr, ok) = compile_and_run(
        "traverse_nonempty",
        r#"half = \x -> if x.n > 0 then Just {value: x.n} else Nothing {}
main = do
  let res = traverse half [{n: 1}, {n: 2}]
  case res of
    Just {value} -> println ("got: " ++ show (count value))
    _ -> println "nothing"
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("got: 2"),
        "non-empty traverse unchanged, got: {stdout}"
    );
}
// ── 14. recursive helpers must not be inlined by the pushdown matchers ──
//
// Codegen's `beta_reduce` inlines named functions so the SQL matchers can see
// through them. It used to unroll a *recursive* function without bound: each
// unroll substitutes the body, which reintroduces the call, and copies the
// argument into every occurrence of the parameter — so the term grows
// multiplicatively and the compile never finishes. A partially applied helper
// (`afterChar ","`) hits the multi-param path, which reduces under the
// remaining binder and blows up fastest; the one-parameter shape merely grew
// quadratically and finished. Both must now compile promptly (issue #71).

/// `knot build`, failed rather than left hanging if it runs long — a
/// regression here is a non-terminating compile, not a wrong answer.
fn compile_within(test_name: &str, source: &str, limit: Duration) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_analysis_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_knot"))
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .spawn()
        .expect("failed to spawn knot compiler");

    let deadline = Instant::now() + limit;
    loop {
        match child.try_wait().expect("failed to poll knot compiler") {
            Some(status) => {
                assert!(status.success(), "knot build failed for {test_name}");
                break;
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "knot build did not finish within {limit:?} for {test_name} — \
                     the recursive helper is being inlined without bound again"
                );
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    let exe = dir.join("prog");
    Compiled { dir, exe }
}

#[test]
fn partially_applied_recursive_helper_compiles() {
    let c = compile_within(
        "recursive_partial_app",
        r#"afterChar : Text -> Text -> Text
afterChar = \sep s -> if s == ""
  then ""
  else if take 1 s == sep
  then drop 1 s
  else afterChar sep (drop 1 s)

splitOnComma : Text -> [Text]
splitOnComma = \s -> if s == ""
  then ([] : [Text])
  else union [afterChar "," s] (splitOnComma (afterChar "," s))

main = do
  yield (splitOnComma "a,b,c")
"#,
        Duration::from_secs(120),
    );
    let (stdout, stderr, ok) = run(&c);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("b,c") && stdout.contains("\"c\""),
        "recursive split must still produce its suffixes, got: {stdout}"
    );
}

/// The single-argument helper — the shape that already compiled, but only after
/// burning the whole inlining budget. Pinned so the fix keeps it working.
#[test]
fn directly_recursive_helper_compiles() {
    let c = compile_within(
        "recursive_direct",
        r#"afterComma : Text -> Text
afterComma = \s -> if s == ""
  then ""
  else if take 1 s == ","
  then drop 1 s
  else afterComma (drop 1 s)

splitOnComma : Text -> [Text]
splitOnComma = \s -> if s == ""
  then ([] : [Text])
  else union [afterComma s] (splitOnComma (afterComma s))

main = do
  yield (splitOnComma "a,b,c")
"#,
        Duration::from_secs(120),
    );
    let (stdout, stderr, ok) = run(&c);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("b,c") && stdout.contains("\"c\""),
        "recursive split must still produce its suffixes, got: {stdout}"
    );
}

// ── 15. Int literals outside the i64 range are a compile-time error ─
//
// The lexer hands the digits to the parser as a string and the runtime parsed
// them with `i64::from_str`, so `99999999999999999999` built cleanly and then
// aborted (SIGABRT) on the runtime's `unwrap`. The range check now runs in the
// lexer, and a prefix `-` folds into the literal so `i64::MIN` is writable.

#[test]
fn out_of_range_int_literal_is_a_lex_error() {
    let diags = lex_diags("main = do\n  let x = 99999999999999999999\n  yield {}\n");
    assert!(
        has_error(&diags, "integer literal is out of range"),
        "expected an out-of-range diagnostic, got: {diags:?}"
    );
}

#[test]
fn out_of_range_int_literal_fails_the_build() {
    let stderr = compile_expect_error(
        "int_overflow",
        "main = do\n  let x = 99999999999999999999\n  println (show x)\n  yield {}\n",
    );
    assert!(
        stderr.contains("integer literal is out of range"),
        "expected an out-of-range build error, got: {stderr}"
    );
}

#[test]
fn i64_min_magnitude_under_binary_minus_is_still_an_error() {
    // Only a PREFIX `-` folds into the literal; as the right operand of a
    // subtraction the magnitude genuinely overflows.
    let diags = lex_diags("f = \\x -> x - 9223372036854775808\n");
    assert!(
        has_error(&diags, "integer literal is out of range"),
        "expected an out-of-range diagnostic, got: {diags:?}"
    );
}

#[test]
fn i64_bounds_are_writable_and_run() {
    let (stdout, stderr, ok) = compile_and_run(
        "int_bounds",
        r#"main = do
  println (show (-9223372036854775808))
  println (show 9223372036854775807)
  println (show (10 - -3))
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("-9223372036854775808") && stdout.contains("9223372036854775807"),
        "i64 bounds should round-trip, got: {stdout}"
    );
    assert!(stdout.contains("13"), "`10 - -3` should be 13, got: {stdout}");
}

// ── 16. Relation literals are sets, so they deduplicate ────────────
//
// `[1, 1, 2]` kept three rows while `==` and `union` compared it as a set, so
// `count [1, 1, 2]` disagreed with `[1, 1, 2] == [1, 2]`.

#[test]
fn relation_literal_deduplicates() {
    let (stdout, stderr, ok) = compile_and_run(
        "list_literal_dedup",
        r#"main = do
  println (show ([1, 1, 2] == [1, 2]))
  println (show (count [1, 1, 2]))
  println (show (count [{a: 1}, {a: 1}, {a: 2}]))
  println (show (count [1, 2, 3]))
  yield {}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines[0].contains("True"), "set equality unchanged: {stdout}");
    assert!(lines[1].contains('2'), "count [1, 1, 2] should be 2: {stdout}");
    assert!(
        lines[2].contains('2'),
        "duplicate records should collapse too: {stdout}"
    );
    assert!(
        lines[3].contains('3'),
        "distinct elements must all survive: {stdout}"
    );
}

// ── 17. Binding a nested-relation field iterates every element ─────
//
// `m <- t.members` in an IO do-block kept only the first member: codegen could
// not see that the field's type is a relation, so it bound the whole relation
// instead of iterating it. Inference now records relation-typed field accesses,
// and a nested comprehension loop splices its rows into the enclosing one
// instead of nesting them one relation deep.

#[test]
fn nested_relation_field_bind_iterates_every_element() {
    let (stdout, stderr, ok) = compile_and_run(
        "nested_field_bind",
        r#"type Team = {name: Text, members: [{who: Text}]}

*teams : [Team]

main = do
  replace *teams = [{name: "A", members: [{who: "x"}, {who: "y"}, {who: "z"}]}]
  t <- *teams
  m <- t.members
  yield m.who
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains(r#"["x", "y", "z"]"#),
        "every member should be yielded, got: {stdout}"
    );
}

#[test]
fn nested_source_binds_yield_one_flat_relation() {
    // The same loop-accumulation fix: a comprehension over two sources is one
    // relation of pairs, not a relation of per-row relations.
    let (stdout, stderr, ok) = compile_and_run(
        "nested_source_binds",
        r#"*names : [{n: Text}]
*tags : [{t: Text}]

main = do
  replace *names = [{n: "A"}]
  replace *tags = [{t: "p"}, {t: "q"}]
  a <- *names
  b <- *tags
  yield {n: a.n, t: b.t}
"#,
    );
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains(r#"{n: "A", t: "p"}"#) && stdout.contains(r#"{n: "A", t: "q"}"#),
        "both cross-join rows should appear, got: {stdout}"
    );
    assert!(
        !stdout.contains("[["),
        "the pairs must be one flat relation, not nested per outer row: {stdout}"
    );
}
