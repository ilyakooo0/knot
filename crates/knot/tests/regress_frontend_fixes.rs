//! Regression tests for frontend fixes:
//!
//! 1. Formatter must never pun record-UPDATE fields (the parser only accepts
//!    punning in record literals).
//! 2. Formatter must parenthesize `Annot` inner expressions whose parse tail
//!    is greedy (`parse_expr` consumes a trailing `: Type`), so the
//!    annotation stays attached to the whole expression on reparse.
//! 3. Parser: a binary operator on a new line only continues the expression
//!    when indented PAST the enclosing block indent (same rule as
//!    application continuation) — so `-1 -> 2` at block indent is a case
//!    arm, not a subtraction absorbed into the previous arm's body.
//! 4. Formatter only coalesces `r *x, w *x` into `rw *x` when the pair is
//!    adjacent and in Reads-then-Writes order — exactly what `rw *x`
//!    reparses to — preserving effect-list order in the AST.

use knot::ast::{BinOp, DeclKind, ExprKind, Literal, StmtKind};
use knot::diagnostic::Severity;

fn parse(source: &str) -> Result<knot::ast::Module, String> {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    let lex_errs: Vec<String> = lex_diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.render(source, "<input>"))
        .collect();
    if !lex_errs.is_empty() {
        return Err(format!("lex errors:\n{}", lex_errs.join("\n")));
    }
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, diags) = parser.parse_module();
    let errs: Vec<String> = diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.render(source, "<input>"))
        .collect();
    if !errs.is_empty() {
        return Err(format!("parse errors:\n{}", errs.join("\n")));
    }
    Ok(module)
}

/// `Debug`-printed module with `span` payloads stripped, so structural
/// equality tolerates any byte-position drift caused by formatting.
/// (Same normalization as `format_roundtrip.rs`.)
fn normalize(module: &knot::ast::Module) -> String {
    let raw = format!("{:#?}", module);
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c == 's' {
            let lookahead: String = chars.clone().take(11).collect();
            if lookahead.starts_with("pan: Span {") {
                let mut depth = 0;
                for c2 in chars.by_ref() {
                    if c2 == '{' {
                        depth += 1;
                    } else if c2 == '}' {
                        if depth == 0 {
                            break;
                        }
                        depth -= 1;
                    }
                }
                while let Some(&c2) = chars.peek() {
                    if c2 == ',' || c2 == ' ' || c2 == '\n' {
                        chars.next();
                    } else {
                        break;
                    }
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Format `source`, verify the output reparses without diagnostics to the
/// same AST (modulo spans), and that formatting is idempotent. Returns the
/// formatted text for additional assertions.
fn check_str(label: &str, source: &str) -> String {
    let m1 = parse(source).unwrap_or_else(|e| panic!("parse {}: {}", label, e));
    let f1 = knot::format::format_module(source, &m1);
    let m2 = parse(&f1).unwrap_or_else(|e| {
        panic!(
            "{}: formatted output failed to parse:\n--- output ---\n{}\n--- error ---\n{}",
            label, f1, e
        )
    });
    let n1 = normalize(&m1);
    let n2 = normalize(&m2);
    assert_eq!(
        n1, n2,
        "{}: AST changed after formatting\n--- output ---\n{}",
        label, f1
    );
    let f2 = knot::format::format_module(&f1, &m2);
    assert_eq!(f1, f2, "{}: formatter is not idempotent", label);
    f1
}

fn decl_body<'a>(m: &'a knot::ast::Module, name: &str) -> &'a knot::ast::Expr {
    for d in &m.decls {
        if let DeclKind::Fun { name: n, body: Some(body), .. } = &d.node
            && n == name {
                return body;
            }
    }
    panic!("no Fun decl named {}", name);
}

// ── 1. Record-update fields are never punned ─────────────────────────

#[test]
fn record_update_var_pun_not_applied() {
    // `{t | age: age}` must NOT format to `{t | age}` (parse error in updates).
    let out = check_str("update-var-pun", "f = \\t age -> {t | age: age}\n");
    assert!(out.contains("{t | age: age}"), "output: {}", out);
}

#[test]
fn record_update_field_access_pun_not_applied() {
    // `{t | name: u.name}` must NOT format to `{t | u.name}` (unparseable).
    let out = check_str("update-fieldaccess-pun", "f = \\t u -> {t | name: u.name}\n");
    assert!(out.contains("{t | name: u.name}"), "output: {}", out);
}

#[test]
fn record_update_block_path_not_punned() {
    // Long enough to force the multi-line record-update renderer.
    let src = "f = \\someRecordValue veryLongFieldNameOne veryLongFieldNameTwo veryLongFieldNameThree -> {someRecordValue | veryLongFieldNameOne: veryLongFieldNameOne, veryLongFieldNameTwo: veryLongFieldNameTwo, veryLongFieldNameThree: veryLongFieldNameThree}\n";
    let out = check_str("update-block-pun", src);
    assert!(
        out.contains("veryLongFieldNameOne: veryLongFieldNameOne"),
        "output: {}",
        out
    );
}

#[test]
fn record_literal_punning_still_works() {
    // Punning in record LITERALS is valid and should be preserved.
    let out = check_str("literal-pun", "f = \\name age -> {name, age}\n");
    assert!(out.contains("{name, age}"), "output: {}", out);
}

// ── 2. Annotations on greedy-tail expressions keep their parens ──────

#[test]
fn annot_on_lambda_keeps_parens() {
    let src = "f = (\\x -> x) : Int -> Int\n";
    let out = check_str("annot-lambda", src);
    // The annotation must stay on the whole lambda, not migrate to its body.
    let m = parse(&out).unwrap();
    match &decl_body(&m, "f").node {
        ExprKind::Annot { expr, .. } => {
            assert!(
                matches!(&expr.node, ExprKind::Lambda { .. }),
                "annot inner is not a lambda: {:?}",
                expr.node
            );
        }
        other => panic!("expected Annot at top, got {:?}", other),
    }
}

#[test]
fn annot_on_if_keeps_parens() {
    let src = "g = (if true then 1 else 2) : Int\n";
    let out = check_str("annot-if", src);
    let m = parse(&out).unwrap();
    match &decl_body(&m, "g").node {
        ExprKind::Annot { expr, .. } => {
            assert!(
                matches!(&expr.node, ExprKind::If { .. }),
                "annot inner is not an if: {:?}",
                expr.node
            );
        }
        other => panic!("expected Annot at top, got {:?}", other),
    }
}

#[test]
fn annot_on_atomic_keeps_parens() {
    let src = "h = (atomic foo) : Int\n";
    let out = check_str("annot-atomic", src);
    let m = parse(&out).unwrap();
    match &decl_body(&m, "h").node {
        ExprKind::Annot { expr, .. } => {
            assert!(
                matches!(&expr.node, ExprKind::Atomic(_)),
                "annot inner is not atomic: {:?}",
                expr.node
            );
        }
        other => panic!("expected Annot at top, got {:?}", other),
    }
}

#[test]
fn annot_on_refine_keeps_parens() {
    check_str("annot-refine", "r1 = (refine foo) : Int\n");
}

#[test]
fn annot_on_set_keeps_parens() {
    check_str("annot-set", "s1 = (*nums = [1]) : {}\n");
}

#[test]
fn annot_on_replace_set_keeps_parens() {
    check_str("annot-replace", "s2 = (replace *nums = [1]) : {}\n");
}

#[test]
fn annot_on_case_round_trips() {
    // Case forces multi-line rendering, exercising the block Annot path.
    check_str(
        "annot-case",
        "q = \\x -> ((case x of 0 -> 1; _ -> 2) : Int)\n",
    )
;
}

#[test]
fn annot_on_do_round_trips() {
    // Do forces multi-line rendering, exercising the block Annot path.
    check_str("annot-do", "w = (do yield 1) : [Int]\n");
}

#[test]
fn annot_on_long_lambda_block_path() {
    // Over TARGET_WIDTH so the multi-line Annot renderer is used.
    let src = "f = (\\someExtremelyLongParameterName -> someExtremelyLongParameterName + someExtremelyLongParameterName + 1) : Int -> Int\n";
    check_str("annot-lambda-block", src);
}

#[test]
fn annot_on_binop_unchanged() {
    // BinOps do not have a greedy tail; no extra parens needed (or wanted).
    let out = check_str("annot-binop", "f = (1 + 2) : Int\n");
    assert!(out.contains("(1 + 2 : Int)"), "output: {}", out);
}

// ── 3. Operator at block indent starts a new item, not a continuation ─

#[test]
fn negative_case_arm_at_block_indent() {
    let src = "f = \\x -> case x of\n  0 -> 1\n  -1 -> 2\n  _ -> 0\n";
    let m = parse(src).unwrap_or_else(|e| panic!("negative arm: {}", e));
    match &decl_body(&m, "f").node {
        ExprKind::Lambda { body, .. } => match &body.node {
            ExprKind::Case { arms, .. } => {
                assert_eq!(arms.len(), 3, "expected 3 arms");
                // First arm body must be `1`, not `1 - 1`.
                assert!(
                    matches!(&arms[0].body.node, ExprKind::Lit(_)),
                    "first arm body absorbed the next line: {:?}",
                    arms[0].body.node
                );
            }
            other => panic!("expected Case, got {:?}", other),
        },
        other => panic!("expected Lambda, got {:?}", other),
    }
}

#[test]
fn do_block_negative_statement_at_block_indent() {
    let src = "main = do\n  with {x: 5} (do\n    -1)\n";
    let m = parse(src).unwrap_or_else(|e| panic!("do neg stmt: {}", e));
    match &decl_body(&m, "main").node {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 1, "expected 1 statement, got {:?}", stmts);
            match &stmts[0].node {
                StmtKind::Expr(e) => match &e.node {
                    ExprKind::With { record, body } => {
                        assert!(
                            matches!(&record.node, ExprKind::Record(_)),
                            "with record: {:?}",
                            record.node
                        );
                        match &body.node {
                            ExprKind::Do(inner) => {
                                assert_eq!(
                                    inner.len(),
                                    1,
                                    "expected 1 inner statement, got {:?}",
                                    inner
                                );
                                // A prefix `-` over an integer literal folds into a
                                // single negative literal at parse time (that is the
                                // only way to write `i64::MIN`), so the statement is
                                // `Lit(-1)`, not `Neg(Lit(1))`.
                                assert!(
                                    matches!(
                                        &inner[0].node,
                                        StmtKind::Expr(e)
                                            if matches!(&e.node, ExprKind::Lit(Literal::Int(n)) if n == "-1")
                                    ),
                                    "inner stmt is not the negative literal -1: {:?}",
                                    inner[0].node
                                );
                            }
                            other => panic!("expected inner Do, got {:?}", other),
                        }
                    }
                    other => panic!("expected With, got {:?}", other),
                },
                other => panic!("expected Expr(with), got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn inline_case_with_negative_arm_round_trips() {
    // The formatter lays this out multi-line with `-1 -> 2` at the block
    // indent; the output must reparse to the same AST.
    check_str(
        "inline-neg-arm",
        "f = \\x -> (case x of 0 -> 1; -1 -> 2; _ -> 3)\n",
    );
}

#[test]
fn deeper_indent_binop_continuation_still_works() {
    // Operator indented PAST the block indent continues the expression.
    let src = "x = a\n  + b\n";
    let m = parse(src).unwrap_or_else(|e| panic!("continuation: {}", e));
    assert!(
        matches!(&decl_body(&m, "x").node, ExprKind::BinOp { op: BinOp::Add, .. }),
        "expected Add continuation"
    );
}

#[test]
fn deeper_indent_binop_continuation_in_do_block() {
    // Operator indented PAST the block indent continues the expression —
    // here the `with` record value absorbs the continuation.
    let src = "main = do\n  with {x: 1\n    + 2} x\n";
    let m = parse(src).unwrap_or_else(|e| panic!("do continuation: {}", e));
    match &decl_body(&m, "main").node {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 1, "expected 1 statement, got {:?}", stmts);
            match &stmts[0].node {
                StmtKind::Expr(e) => match &e.node {
                    ExprKind::With { record, .. } => match &record.node {
                        ExprKind::Record(fields) => {
                            assert_eq!(fields.len(), 1, "expected 1 field");
                            assert!(
                                matches!(
                                    &fields[0].value.node,
                                    ExprKind::BinOp { op: BinOp::Add, .. }
                                ),
                                "with value should be the continued Add: {:?}",
                                fields[0].value.node
                            );
                        }
                        other => panic!("expected Record, got {:?}", other),
                    },
                    other => panic!("expected With, got {:?}", other),
                },
                other => panic!("expected Expr(with), got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn pipeline_continuation_inside_parens_still_works() {
    // Inside delimiters, layout is free-form: continuation at any column.
    let src = "q = (*emps\n|> filter (\\e -> e.salary > 75))\n";
    parse(src).unwrap_or_else(|e| panic!("paren continuation: {}", e));
}

// ── 4. Effect coalescing preserves AST order ──────────────────────────

#[test]
fn effects_writes_then_reads_not_coalesced() {
    let out = check_str("effects-wr", "type F = {w *people, r *people} Text\n");
    assert!(out.contains("w *people, r *people"), "output: {}", out);
}

#[test]
fn effects_reads_then_writes_coalesced_to_rw() {
    let out = check_str("effects-rw", "type G = {r *people, w *people} Text\n");
    assert!(out.contains("rw *people"), "output: {}", out);
}

#[test]
fn effects_rw_shorthand_round_trips() {
    let out = check_str("effects-rw-short", "type H = {rw *people} Text\n");
    assert!(out.contains("rw *people"), "output: {}", out);
}

#[test]
fn effects_interleaved_not_coalesced() {
    // `w *t` ... `r *t` with `clock` between: coalescing would reorder.
    let out = check_str("effects-interleaved", "type I = {w *t, clock, r *t} Text\n");
    assert!(out.contains("w *t, clock, r *t"), "output: {}", out);
}

#[test]
fn effects_reads_writes_interleaved_rw_order() {
    // Reads-then-Writes but separated by another effect must stay separate.
    let out = check_str("effects-r-clock-w", "type J = {r *t, clock, w *t} Text\n");
    assert!(out.contains("r *t, clock, w *t"), "output: {}", out);
}
