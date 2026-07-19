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
//! 2. A field annotation like `with {x: (value : Type)}` produced an `Annot`
//!    node that took only the value's span, excluding the `: Type`. The span
//!    now covers the union of the value and type spans (and crucially is never
//!    inverted).

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
fn with_annotation_span_covers_the_type() {
    // The `Annot` in `with {x (5 : Int)}` must span both the type and the
    // value (never inverted). We locate the binding and assert its span covers
    // the `5 : Int` source range.
    let src = "main = do\n  with {x (5 : Int)} (yield x)\n";
    let (module, diags) = parse(src);
    assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);

    let int_off = src.find("Int").unwrap();
    let five_off = src.find('5').unwrap();

    let mut found = false;
    for decl in &module.decls {
        if let DeclKind::Fun { body: Some(b), .. } = &decl.node
            && let ExprKind::Do(stmts) = &b.node {
                for st in stmts {
                    if let StmtKind::Expr(e) = &st.node
                        && let ExprKind::With { record, .. } = &e.node
                        && let ExprKind::Record(fields) = &record.node {
                            for f in fields {
                                if let ExprKind::Annot { ty, .. } = &f.value.node {
                                    // span must be non-inverted and cover both the
                                    // type and the value.
                                    assert!(
                                        f.value.span.start <= f.value.span.end,
                                        "inverted span"
                                    );
                                    assert!(
                                        f.value.span.start <= five_off,
                                        "Annot span should start at/before the value"
                                    );
                                    assert!(
                                        f.value.span.end > int_off,
                                        "Annot span should extend through the type"
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
    assert!(found, "did not find the annotated with binding");
}
