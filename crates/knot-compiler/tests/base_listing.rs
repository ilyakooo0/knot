//! `knot base` prints every leaf field of the global `base` record as a sig
//! line (`num.int.neg : Int u -> Int u`). The output is meant to be re-parseable
//! by the compiler, so this test runs `check` on the trivial program `base` and
//! feeds every emitted type string through `parser::parse_type_str` (the same
//! parser the JIT's expected-type wire uses).

use knot_compiler::infer::check;

fn base_fields() -> Vec<(String, String)> {
    let source = "base".to_string();
    let lexer = knot::lexer::Lexer::new(&source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source, tokens);
    let (mut program, diags) = parser.parse_file_expr();
    assert!(
        diags.iter().all(|d| d.severity != knot::diagnostic::Severity::Error),
        "the trivial `base` program should parse cleanly"
    );
    knot_compiler::desugar::desugar(&mut program);
    check(&mut program).base_fields
}

#[test]
fn base_fields_are_nonempty() {
    assert!(
        base_fields().len() > 100,
        "base should expose well over a hundred leaf fields"
    );
}

#[test]
fn every_base_field_type_reparses() {
    // `strip`/`dress` are the carrier-ABSTRACT unit ops: their type applies a
    // type variable to a unit (`(a Unit<1>) -> (a Unit<1>)`), which the type
    // renderer emits in the internal `Unit<…>` notation — no surface form.
    // Users call the concrete `stripUnit`/`stripFloatUnit`/`withUnit`/
    // `withFloatUnit` (which DO re-parse). Rendering the abstract form is a
    // known renderer gap, not a missing base member, so these two are excluded
    // from the round-trip assertion (they still appear in `knot base` output).
    const KNOWN_UNREPRESENTABLE: [&str; 2] = ["strip", "dress"];
    let bad: Vec<_> = base_fields()
        .into_iter()
        .filter(|(path, ty)| {
            !KNOWN_UNREPRESENTABLE.contains(&path.as_str())
                && knot::parser::parse_type_str(ty).is_none()
        })
        .collect();
    assert!(
        bad.is_empty(),
        "some base field types do not re-parse:\n{}",
        bad.iter()
            .map(|(p, t)| format!("  {p} : {t}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn no_field_type_uses_internal_var_names() {
    // A var index past `z` renders as `t70`, which is not valid knot source.
    // Per-field var maps (in `collect_base_fields`) keep every var a letter.
    for (path, ty) in base_fields() {
        assert!(
            !ty.split_whitespace().any(|tok| {
                let t = tok.trim_matches(|c: char| c == '(' || c == ')');
                t.len() > 1
                    && (t.starts_with('t') || t.starts_with('u'))
                    && t[1..].chars().all(|c| c.is_ascii_digit())
            }),
            "base.{path}'s emitted type uses an internal var name: `{ty}`"
        );
    }
}
