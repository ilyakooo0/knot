//! Regression tests for type-inference fixes:
//! 1. views (alias and comprehension forms) accepted now that relation
//!    reads are IO-typed,
//! 2. derived relations (annotated and un-annotated) unwrap the IO from
//!    their bodies,
//! 3. type-mismatch diagnostics orient expected/found on the
//!    provided/required roles (both synthesis and check mode),
//! 7. constructor-pattern binds directly on a source relation inside a
//!    let-bound comprehension.

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
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _json, _elem, _trait_calls, _show_units) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

// ── 1. Views ─────────────────────────────────────────────────────────

#[test]
fn view_alias_of_source_typechecks() {
    // `*alias = *todos` — the body infers as `IO {r *todos} [{title: Text}]`
    // now that relation reads are IO; the view arm must unwrap the IO
    // before unifying with the view's relation type.
    let src = r#"*todos : [{title: Text}]
*alias = *todos

main = do
  replace *todos = [{title: "buy milk"}]
  xs <- *alias
  x <- xs
  p <- println x.title
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn view_comprehension_typechecks() {
    // The DESIGN.md comprehension form: binds iterate ELEMENTS of the
    // source relation and the view's type is `[yieldType]` — a plain
    // IO-unwrap is not enough here.
    let src = r#"*todos : [{title: Text, done: Bool}]
*mine = do
  t <- *todos
  where t.done
  yield {title: t.title}

main = do
  replace *todos = [{title: "a", done: true}, {title: "b", done: false}]
  xs <- *mine
  m <- xs
  p <- println m.title
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn view_comprehension_element_type_mismatch_still_rejected() {
    // The comprehension typing must still catch element-type errors:
    // the view is annotated `[{title: Int}]` but yields Text titles.
    let src = r#"*todos : [{title: Text}]
*bad : [{title: Int}] = do
  t <- *todos
  yield {title: t.title}

main = println "x"
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "type mismatch"),
        "expected a type mismatch diagnostic, got: {:?}",
        diags
    );
}

// ── 2. Derived relations ─────────────────────────────────────────────

#[test]
fn annotated_derived_with_io_body_typechecks() {
    let src = r#"*nums : [{n: Int}]
&doubled : [{n: Int}] = do
  nums <- *nums
  yield (do x <- nums
            yield {n: x.n + x.n})

main = do
  replace *nums = [{n: 3}]
  ds <- &doubled
  d <- ds
  p <- println (show d.n)
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn unannotated_derived_binds_relation_type_not_io() {
    // The fresh var for an un-annotated derived must resolve to the plain
    // relation type, not `IO {} [T]` — otherwise `&name` references
    // produce a nested `IO (IO [T])` and element access only works
    // through unification leniency.
    let src = r#"*nums : [{n: Int}]
&tripled = do
  nums <- *nums
  yield (do x <- nums
            yield {n: x.n * 3})

main = do
  replace *nums = [{n: 4}]
  ds <- &tripled
  d <- ds
  p <- println (show d.n)
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

// ── 3. Diagnostic orientation ────────────────────────────────────────

#[test]
fn synthesis_mode_mismatch_orients_expected_on_required_side() {
    // `unify(actual, expected)` with t1 = Int (provided), t2 = Bool
    // (required) must report "expected Bool, found Int".
    let src = r#"main = if 1 then println "x" else println "y"
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "expected Bool, found Int"),
        "expected `expected Bool, found Int`, got: {:?}",
        diags
    );
    assert!(
        !has_error(&diags, "expected Int, found Bool"),
        "inverted diagnostic orientation: {:?}",
        diags
    );
}

#[test]
fn check_mode_mismatch_orientation_unchanged() {
    // The check-mode path (`check_expr` pushes the expected type with
    // t1_provided = false) must STILL read "expected Int, found Text".
    let src = r#"x : {n: Int}
x = {n: "text"}
main = println (show x.n)
"#;
    let diags = check_src(src);
    assert!(
        has_error(&diags, "expected Int, found Text"),
        "expected `expected Int, found Text`, got: {:?}",
        diags
    );
    assert!(
        !has_error(&diags, "expected Text, found Int"),
        "inverted diagnostic orientation: {:?}",
        diags
    );
}

// ── 7. Constructor-pattern bind directly on a source relation ───────

#[test]
fn ctor_pattern_bind_on_source_in_let_comprehension() {
    // `Circle c <- *shapes` directly (no intermediate `rows <- *shapes`)
    // must typecheck: the pattern matches relation ELEMENTS (filter
    // semantics), not the whole `[Shape]`.
    let src = r#"data Shape = Circle {radius: Float} | Rect {width: Float, height: Float}
*shapes : [Shape]

main = do
  let circles = do
        Circle c <- *shapes
        yield {radius: c.radius}
  p <- println "ok"
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}

#[test]
fn ctor_pattern_bind_via_intermediate_still_typechecks() {
    // The previously-working two-step form must keep working.
    let src = r#"data Shape = Circle {radius: Float} | Rect {width: Float, height: Float}
*shapes : [Shape]

main = do
  let circles = do
        rows <- *shapes
        Circle c <- rows
        yield {radius: c.radius}
  p <- println "ok"
  yield {}
"#;
    let diags = check_src(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
}
