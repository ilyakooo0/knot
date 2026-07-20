//! Regression tests for frontend fixes (round 3):
//!
//! 1. Impl-method and trait-default-body parameters are registered as bound
//!    variables, so the time-unit sugar (`2 ms`) never consumes a parameter
//!    named `ms`/`seconds`/... as a unit suffix.
//! 2. The formatter's verbatim fallback (decls with internal comments)
//!    normalizes a tab to ONE space — the parser's column weight — so mixed
//!    tab/space sibling indentation keeps its block structure. Additionally,
//!    `format_module` re-parses its own output and returns the source
//!    unchanged if the output fails to parse or changes the AST.
//! 3. `with {name: value} body` prints back as `with`, not as the parser's
//!    internal representation.
//! 4. Constructor payloads get the same postfix handling as function
//!    application arguments: `Just x.y` parses as `Just (x.y)`.
//! 5. A backslash at end-of-line inside a (byte) string literal reports an
//!    unterminated literal at the line break instead of swallowing the next
//!    line as an "unknown escape".

use knot::ast::{BinOp, DeclKind, ExprKind, ImplItem, StmtKind, TraitItem};
use knot::diagnostic::Severity;
use knot::lexer::TokenKind;

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

/// Does the expression tree mention a `Var` with this name anywhere?
fn expr_mentions_var(e: &knot::ast::Expr, name: &str) -> bool {
    match &e.node {
        ExprKind::Var(n) => n == name,
        ExprKind::Lit(_)
        | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_)
        | ExprKind::DerivedRef(_)
        | ExprKind::TypeCtor { .. }
        | ExprKind::DataCtor { .. } => false,
        ExprKind::Record(fields) => fields.iter().any(|f| expr_mentions_var(&f.value, name)),
        ExprKind::RecordUpdate { base, fields } => {
            expr_mentions_var(base, name)
                || fields.iter().any(|f| expr_mentions_var(&f.value, name))
        }
        ExprKind::FieldAccess { expr, .. } => expr_mentions_var(expr, name),
        ExprKind::With { record, body } => {
            expr_mentions_var(record, name) || expr_mentions_var(body, name)
        }
        ExprKind::List(items) => items.iter().any(|i| expr_mentions_var(i, name)),
        ExprKind::Lambda { body, .. } => expr_mentions_var(body, name),
        ExprKind::App { func, arg } => {
            expr_mentions_var(func, name) || expr_mentions_var(arg, name)
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_mentions_var(lhs, name) || expr_mentions_var(rhs, name)
        }
        ExprKind::UnaryOp { operand, .. } => expr_mentions_var(operand, name),
        ExprKind::If { cond, then_branch, else_branch } => {
            expr_mentions_var(cond, name)
                || expr_mentions_var(then_branch, name)
                || expr_mentions_var(else_branch, name)
        }
        ExprKind::Case { scrutinee, arms } => {
            expr_mentions_var(scrutinee, name)
                || arms.iter().any(|a| expr_mentions_var(&a.body, name))
        }
        ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
            StmtKind::Bind { expr, .. } => expr_mentions_var(expr, name),
            StmtKind::Where { cond } => expr_mentions_var(cond, name),
            StmtKind::GroupBy { key } => expr_mentions_var(key, name),
            StmtKind::Expr(e) => expr_mentions_var(e, name),
        }),
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            expr_mentions_var(target, name) || expr_mentions_var(value, name)
        }
        ExprKind::Atomic(inner) | ExprKind::Refine(inner) => expr_mentions_var(inner, name),
        ExprKind::TimeUnitLit { value, .. } => expr_mentions_var(value, name),
        ExprKind::Annot { expr, .. } => expr_mentions_var(expr, name),
        ExprKind::Serve { handlers, .. } => {
            handlers.iter().any(|h| expr_mentions_var(&h.body, name))
        }
    }
}

// ── 1. impl/trait method params suppress time-unit sugar ────────────

#[test]
fn impl_method_param_named_ms_is_not_unit_sugar() {
    let src = "impl Waitable Int where\n  f ms = wait 2 ms\n";
    let m = parse(src).expect("parse");
    let DeclKind::Impl { items, .. } = &m.decls[0].node else {
        panic!("expected Impl, got {:?}", m.decls[0].node);
    };
    let ImplItem::Method { body, .. } = &items[0] else {
        panic!("expected Method, got {:?}", items[0]);
    };
    assert!(
        expr_mentions_var(body, "ms"),
        "`ms` was desugared to a unit factor: {:?}",
        body
    );
    // `wait 2 ms` must be `(wait 2) ms`, not `wait (2 * 1)`.
    let ExprKind::App { arg, .. } = &body.node else {
        panic!("expected App, got {:?}", body);
    };
    assert!(
        matches!(&arg.node, ExprKind::Var(n) if n == "ms"),
        "argument vanished into unit sugar: {:?}",
        arg
    );
}

#[test]
fn trait_default_body_param_named_seconds_is_not_unit_sugar() {
    let src = "trait Waitable a where\n  f : a -> Int\n  f seconds = wait 2 seconds\n";
    let m = parse(src).expect("parse");
    let DeclKind::Trait { items, .. } = &m.decls[0].node else {
        panic!("expected Trait, got {:?}", m.decls[0].node);
    };
    let body = items
        .iter()
        .find_map(|it| match it {
            TraitItem::Method { default_body: Some(b), .. } => Some(b),
            _ => None,
        })
        .expect("expected a default body");
    assert!(
        expr_mentions_var(body, "seconds"),
        "`seconds` was desugared to a unit factor: {:?}",
        body
    );
    let ExprKind::App { arg, .. } = &body.node else {
        panic!("expected App, got {:?}", body);
    };
    assert!(
        matches!(&arg.node, ExprKind::Var(n) if n == "seconds"),
        "argument vanished into unit sugar: {:?}",
        arg
    );
}

#[test]
fn impl_method_unit_sugar_scope_ends_after_body() {
    // The param scope closes after the method body, so unit sugar still
    // applies in the next method.
    let src = "impl Waitable Int where\n  f ms = wait 2 ms\n  g x = sleep (3 ms)\n";
    let m = parse(src).expect("parse");
    let DeclKind::Impl { items, .. } = &m.decls[0].node else {
        panic!("expected Impl, got {:?}", m.decls[0].node);
    };
    let ImplItem::Method { body, .. } = &items[1] else {
        panic!("expected Method, got {:?}", items[1]);
    };
    let ExprKind::App { arg, .. } = &body.node else {
        panic!("expected App, got {:?}", body);
    };
    assert!(
        matches!(
            &arg.node,
            ExprKind::TimeUnitLit { value, .. }
                if matches!(&value.node, ExprKind::BinOp { op: BinOp::Mul, .. })
        ),
        "expected `3 ms` to desugar to a TimeUnitLit wrapping multiplication, got {:?}",
        arg
    );
}

// ── 2. verbatim fallback: tabs weigh one column; output always reparses ──

#[test]
fn verbatim_fallback_tab_normalization_preserves_block_structure() {
    // The internal comment forces the verbatim fallback. The first case arm
    // is tab-indented (parser column 1), the second is one-space-indented
    // (also column 1) — siblings. Replacing the tab with TWO spaces would
    // move the first arm to column 2 and break the block on reparse.
    let src = "f = case g of -- keep\n\tA {} -> 1\n _ -> 2\n";
    let out = check_str("tab_mix", src);
    assert!(
        !out.contains('\t'),
        "tab survived formatting (reparse fallback triggered?):\n{}",
        out
    );
    assert!(out.contains("-- keep"), "comment lost:\n{}", out);
}

#[test]
fn format_module_never_changes_the_ast() {
    // Belt-and-braces: even for tab-free input with internal comments the
    // verbatim path must round-trip.
    let src = "main = do -- comment\n  x <- foo\n  yield x\n";
    check_str("verbatim_roundtrip", src);
}

// ── 3. with survives formatting ────────────────────────────────────

#[test]
fn with_prints_back_as_with() {
    let out = check_str("with_simple", "f = with {x 1} x + 2\n");
    assert!(
        out.contains("with {x 1} x + 2"),
        "with was rewritten:\n{}",
        out
    );
}

#[test]
fn with_annotated_field_prints_back() {
    let out = check_str("with_annot", "f = with {x (5 : Int)} x\n");
    assert!(
        out.contains("with {x (5 : Int)} x"),
        "annotated with was rewritten:\n{}",
        out
    );
}

#[test]
fn nested_with_prints_back() {
    let out = check_str("with_nested", "f = with {x 1} with {y 2} x + y\n");
    assert!(
        out.contains("with {x 1} with {y 2} x + y"),
        "nested with was rewritten:\n{}",
        out
    );
}

#[test]
fn with_multi_field_record_prints_back() {
    let out = check_str(
        "with_multi",
        "f = \\p -> with {lo p.lo hi p.hi} hi - lo\n",
    );
    assert!(
        out.contains("with {lo p.lo hi p.hi} hi - lo"),
        "multi-field with was rewritten:\n{}",
        out
    );
}

#[test]
fn with_as_binop_operand_keeps_parens() {
    let out = check_str("with_rhs", "f = 1 + (with {x 2} x)\n");
    assert!(
        out.contains("1 + (with {x 2} x)"),
        "with operand lost parens:\n{}",
        out
    );
}

#[test]
fn with_under_postfix_annotation_keeps_parens() {
    // The with body is parsed with `parse_expr`, which greedily reattaches
    // a trailing `: Type` — the formatter must keep the grouping parens.
    check_str("with_annot_tail", "f = (with {x 1} x) : Int\n");
}

#[test]
fn with_do_body_round_trips() {
    check_str(
        "with_do_body",
        "f = with {xs [1, 2]} do\n  y <- xs\n  yield y\n",
    );
}

#[test]
fn applied_with_keeps_parens() {
    // `(with {x h} x) 2` — a with in application head position.
    check_str("with_applied", "g = (with {x h} x) 2\n");
}

#[test]
fn explicit_lambda_application_still_prints_as_lambda() {
    // A user-written `(\x -> x + 1) 2` must print unchanged (it is not a
    // `with` expression).
    let out = check_str("real_lambda_app", "f = (\\x -> x + 1) 2\n");
    assert!(
        out.contains("(\\x -> x + 1) 2"),
        "explicit lambda application was rewritten:\n{}",
        out
    );
    assert!(
        !out.contains("with"),
        "lambda app misdetected as with:\n{}",
        out
    );
}

// ── 4. constructor payloads use application-argument postfix rules ──

#[test]
fn constructor_payload_takes_field_access() {
    let src = "f = \\p -> Just p.name\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Lambda { body, .. } = &b.node else {
        panic!("expected Lambda, got {:?}", b);
    };
    let ExprKind::App { func, arg } = &body.node else {
        panic!("expected App, got {:?}", body);
    };
    assert!(matches!(&func.node, ExprKind::Constructor(n) if n == "Just"));
    assert!(
        matches!(&arg.node, ExprKind::FieldAccess { field, .. } if field == "name"),
        "payload did not take the field access (dot attached to the App): {:?}",
        arg
    );
}

#[test]
fn constructor_payload_field_access_round_trips() {
    let out = check_str("ctor_payload_dot", "f = \\p -> Just p.name\n");
    assert!(out.contains("Just p.name"), "output: {}", out);
}

// ── 5. backslash at end-of-line inside string literals ──────────────

#[test]
fn backslash_at_eol_reports_unterminated_string() {
    let src = "f = \"abc\\\ng = 1\n";
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, diags) = lexer.tokenize();
    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("unterminated string literal")),
        "expected unterminated string diagnostic, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
    // The next line must NOT be swallowed into the string literal.
    let text = tokens.iter().find_map(|t| match &t.kind {
        TokenKind::Text(s) => Some(s.clone()),
        _ => None,
    });
    assert_eq!(text.as_deref(), Some("abc"), "string swallowed the next line");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(&t.kind, TokenKind::Lower(n) if n == "g")),
        "the line after the broken string was lost: {:?}",
        tokens
    );
}

#[test]
fn backslash_at_eol_reports_unterminated_byte_string() {
    let src = "f = b\"ab\\\ng = 1\n";
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, diags) = lexer.tokenize();
    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("unterminated byte string literal")),
        "expected unterminated byte string diagnostic, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
    let bytes = tokens.iter().find_map(|t| match &t.kind {
        TokenKind::Bytes(b) => Some(b.clone()),
        _ => None,
    });
    assert_eq!(
        bytes.as_deref(),
        Some(b"ab".as_slice()),
        "byte string swallowed the next line"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(&t.kind, TokenKind::Lower(n) if n == "g")),
        "the line after the broken byte string was lost: {:?}",
        tokens
    );
}

#[test]
fn backslash_cr_at_eol_reports_unterminated_string() {
    // CR (alone or as part of CRLF) is also a line break.
    let src = "f = \"abc\\\r\ng = 1\n";
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, diags) = lexer.tokenize();
    assert!(
        diags
            .iter()
            .any(|d| d.message.contains("unterminated string literal")),
        "expected unterminated string diagnostic, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(&t.kind, TokenKind::Lower(n) if n == "g")),
        "the line after the broken string was lost: {:?}",
        tokens
    );
}
