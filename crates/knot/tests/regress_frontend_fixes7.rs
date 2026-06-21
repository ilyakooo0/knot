//! Regression tests for frontend fixes (round 7):
//!
//! 1. `try_parse_effects` conflated "no effects found" with "parse error after
//!    committing to an effect keyword". On a malformed effect row like
//!    `IO {r *x, w} {}` (the `w` lacks its `*`), the old code `?`-propagated
//!    `None` after consuming `w`, discarding the partial parse AND leaving the
//!    surrounding type-row parse desynced — which could swallow the rest of the
//!    declaration. It now `break`s on the malformed keyword, keeping any
//!    effects parsed so far and a saner cursor, and recovers cleanly.
//!
//! 2. The synthetic `Annot` node wrapping a `let pat : Type = value` binding
//!    took only the value's span, excluding the `: Type`. The span now covers
//!    the union of the value and type spans (and crucially is never inverted —
//!    in a `let` the type precedes the value).

use knot::ast::{DeclKind, ExprKind, StmtKind};

fn parse(source: &str) -> (knot::ast::Module, Vec<knot::diagnostic::Diagnostic>) {
    let (tokens, _) = knot::lexer::Lexer::new(source).tokenize();
    knot::parser::Parser::new(source.to_string(), tokens).parse_module()
}

fn fun_names(module: &knot::ast::Module) -> Vec<String> {
    module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            DeclKind::Fun { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn malformed_effect_row_recovers_without_dropping_next_decl() {
    // `w` is missing its `*`. The effect-row parse must report the error and
    // recover, leaving the following `g` declaration intact.
    let src = "f : IO {r *x, w} {}\nf = println \"hi\"\ng = println \"bye\"\n";
    let (module, diags) = parse(src);
    assert!(
        !diags.is_empty(),
        "expected a diagnostic for the malformed effect row"
    );
    let names = fun_names(&module);
    assert!(
        names.contains(&"g".to_string()),
        "the declaration after the malformed effect row must survive, got: {:?}",
        names
    );
}

#[test]
fn well_formed_effect_row_still_parses() {
    // The break-on-error change must not regress the happy path: a fully
    // well-formed effect row parses with no diagnostics.
    let src = "f : IO {r *x, w *y} {}\nf = println \"hi\"\n";
    let (module, diags) = parse(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
    assert!(fun_names(&module).contains(&"f".to_string()));
}

#[test]
fn let_annotation_span_covers_the_type() {
    // The `Annot` wrapping `let x : Int = 5` must span both the type and the
    // value (never inverted). We locate the binding and assert its span covers
    // the `Int = 5` source range.
    let src = "main = do\n  let x : Int = 5\n  yield x\n";
    let (module, diags) = parse(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);

    let int_off = src.find("Int").unwrap();
    let five_off = src.find('5').unwrap();

    let mut found = false;
    for decl in &module.decls {
        if let DeclKind::Fun { body: Some(b), .. } = &decl.node {
            if let ExprKind::Do(stmts) = &b.node {
                for st in stmts {
                    if let StmtKind::Let { expr, .. } = &st.node {
                        if let ExprKind::Annot { ty, .. } = &expr.node {
                            // span must be non-inverted and cover both the type
                            // (which precedes the value) and the value.
                            assert!(expr.span.start <= expr.span.end, "inverted span");
                            assert!(
                                expr.span.start <= int_off,
                                "Annot span should start at/before the type"
                            );
                            assert!(
                                expr.span.end >= five_off + 1,
                                "Annot span should extend through the value"
                            );
                            // sanity: the type really is the `Int` we expect
                            assert!(ty.span.start <= int_off + 3);
                            found = true;
                        }
                    }
                }
            }
        }
    }
    assert!(found, "did not find the annotated let binding");
}
