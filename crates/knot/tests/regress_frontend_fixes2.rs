//! Regression tests for frontend fixes (round 2):
//!
//! 1. A refined route response type (`-> Int where \v -> v > 0`) must not
//!    swallow the following `headers`/`rateLimit` clauses into the predicate
//!    expression.
//! 2. Deeply nested parens/brackets/braces (expressions and types) must
//!    produce a "nesting depth limit exceeded" diagnostic, not a stack
//!    overflow.
//! 3. An empty `trait ... where` / `impl ... where` block must not capture
//!    the next top-level declaration: layout blocks require items indented
//!    strictly past the enclosing block.
//! 4. Formatter parenthesizes a `Pow` base that is itself a unit expression
//!    (`(M^2)^3` must not print as `M^2^3`).
//! 5. Time-unit literal sugar (`2 ms`) is suppressed when the identifier is
//!    a locally-bound variable (lambda param, do-bind, do-let, let-in, case
//!    binder).
//! 6. Formatter keeps required parens around `yield` in application
//!    argument position (`f (yield)`).
//! 7. A bare `Cons` constructor pattern prints as `Cons`, not `Cons {}`
//!    (which would reparse via the reserved `Cons head tail` path).
//! 8. Formatter parenthesizes the right operand of unit `*`/`/` so
//!    right-nested products don't re-associate on reparse.
//! 9. `import ./foo-bar` — dashed import path segments parse.

use knot::ast::{BinOp, DeclKind, ExprKind, PatKind, StmtKind, TypeKind};
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

// ── 1. Refined route response type + headers/rateLimit clauses ──────

fn route_entries(m: &knot::ast::Module) -> &[knot::ast::RouteEntry] {
    match &m.decls[0].node {
        DeclKind::Route { entries, .. } => entries,
        other => panic!("expected Route decl, got {:?}", other),
    }
}

#[test]
fn refined_response_type_keeps_rate_limit() {
    let src = "route Api where\n  POST {name: Text} /items -> Int where \\v -> v > 0 rateLimit {key: \\i -> \\c -> Nothing, limit: {requests: 10, window: 1000}} = CreateItem\n";
    let m = parse(src).expect("parse");
    let entries = route_entries(&m);
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert!(
        e.rate_limit.is_some(),
        "rateLimit clause was swallowed by the refined predicate: {:?}",
        e
    );
    // The response type must be `Int where \v -> v > 0` with the predicate
    // being exactly the comparison lambda — nothing appended.
    let rty = e.response_ty.as_ref().expect("response type");
    match &rty.node {
        TypeKind::Refined { base, predicate } => {
            assert!(
                matches!(&base.node, TypeKind::Named(n) if n == "Int"),
                "base: {:?}",
                base
            );
            match &predicate.node {
                ExprKind::Lambda { params, body } => {
                    assert_eq!(params.len(), 1);
                    assert!(
                        matches!(&body.node, ExprKind::BinOp { op: BinOp::Gt, .. }),
                        "predicate body absorbed trailing tokens: {:?}",
                        body
                    );
                }
                other => panic!("expected lambda predicate, got {:?}", other),
            }
        }
        other => panic!("expected refined response type, got {:?}", other),
    }
}

#[test]
fn refined_response_type_keeps_response_headers() {
    let src = "route Api where\n  GET /items -> Int where \\v -> v > 0 headers {etag: Text} = GetItems\n";
    let m = parse(src).expect("parse");
    let e = &route_entries(&m)[0];
    assert_eq!(
        e.response_headers.len(),
        1,
        "headers clause was swallowed by the refined predicate: {:?}",
        e
    );
    assert_eq!(e.response_headers[0].name, "etag");
    let rty = e.response_ty.as_ref().expect("response type");
    match &rty.node {
        TypeKind::Refined { predicate, .. } => match &predicate.node {
            ExprKind::Lambda { body, .. } => {
                assert!(
                    matches!(&body.node, ExprKind::BinOp { op: BinOp::Gt, .. }),
                    "predicate body absorbed trailing tokens: {:?}",
                    body
                );
            }
            other => panic!("expected lambda predicate, got {:?}", other),
        },
        other => panic!("expected refined response type, got {:?}", other),
    }
}

#[test]
fn refined_response_type_with_rate_limit_round_trips() {
    check_str(
        "refined_rate_limit",
        "route Api where\n  POST {name: Text} /items -> Int where \\v -> v > 0 rateLimit {key: \\i -> \\c -> Nothing, limit: {requests: 10, window: 1000}} = CreateItem\n",
    );
}

// ── 2. Deep nesting produces a diagnostic instead of a stack overflow ──

fn assert_depth_diagnostic(label: &str, src: &str) {
    let err = parse(src).err().unwrap_or_else(|| {
        panic!("{}: expected nesting-depth diagnostic, parse succeeded", label)
    });
    assert!(
        err.contains("nesting depth limit exceeded"),
        "{}: expected depth diagnostic, got:\n{}",
        label,
        &err[..err.len().min(500)]
    );
}

#[test]
fn deep_nested_parens_expr_is_diagnosed() {
    let n = 3000;
    let src = format!("f = {}1{}\n", "(".repeat(n), ")".repeat(n));
    assert_depth_diagnostic("parens_expr", &src);
}

#[test]
fn deep_nested_brackets_expr_is_diagnosed() {
    let n = 3000;
    let src = format!("f = {}1{}\n", "[".repeat(n), "]".repeat(n));
    assert_depth_diagnostic("brackets_expr", &src);
}

#[test]
fn deep_nested_braces_expr_is_diagnosed() {
    let n = 3000;
    let src = format!("f = {}1{}\n", "{a: ".repeat(n), "}".repeat(n));
    assert_depth_diagnostic("braces_expr", &src);
}

#[test]
fn deep_nested_parens_type_is_diagnosed() {
    let n = 3000;
    let src = format!("f : {}Int{}\n", "(".repeat(n), ")".repeat(n));
    assert_depth_diagnostic("parens_type", &src);
}

#[test]
fn deep_nested_brackets_type_is_diagnosed() {
    let n = 3000;
    let src = format!("f : {}Int{}\n", "[".repeat(n), "]".repeat(n));
    assert_depth_diagnostic("brackets_type", &src);
}

#[test]
fn deep_nested_record_type_is_diagnosed() {
    let n = 3000;
    let src = format!("f : {}Int{}\n", "{a: ".repeat(n), "}".repeat(n));
    assert_depth_diagnostic("braces_type", &src);
}

#[test]
fn moderately_nested_parens_still_parse() {
    let n = 50;
    let src = format!("f = {}1{}\n", "(".repeat(n), ")".repeat(n));
    parse(&src).expect("50-deep parens should parse fine");
}

// ── 3. Empty trait/impl blocks don't capture the next declaration ──

#[test]
fn empty_trait_block_does_not_capture_next_decl() {
    let src = "trait Foo a where\n\nbar = 1\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.decls.len(), 2, "decls: {:?}", m.decls);
    match &m.decls[0].node {
        DeclKind::Trait { items, .. } => {
            assert!(items.is_empty(), "trait captured items: {:?}", items)
        }
        other => panic!("expected Trait, got {:?}", other),
    }
    assert!(
        matches!(&m.decls[1].node, DeclKind::Fun { name, .. } if name == "bar"),
        "second decl: {:?}",
        m.decls[1]
    );
}

#[test]
fn empty_impl_block_does_not_capture_next_decl() {
    let src = "impl Show Int where\n\nbar = 2\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.decls.len(), 2, "decls: {:?}", m.decls);
    match &m.decls[0].node {
        DeclKind::Impl { items, .. } => {
            assert!(items.is_empty(), "impl captured items: {:?}", items)
        }
        other => panic!("expected Impl, got {:?}", other),
    }
    assert!(
        matches!(&m.decls[1].node, DeclKind::Fun { name, .. } if name == "bar"),
        "second decl: {:?}",
        m.decls[1]
    );
}

#[test]
fn indented_trait_block_still_parses() {
    let src = "trait Foo a where\n  bar : a -> Text\n  baz = \\x -> x\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.decls.len(), 1);
    match &m.decls[0].node {
        DeclKind::Trait { items, .. } => assert_eq!(items.len(), 2, "items: {:?}", items),
        other => panic!("expected Trait, got {:?}", other),
    }
}

#[test]
fn indented_impl_and_serve_and_do_blocks_still_parse() {
    let src = "impl Show Int where\n  show = \\x -> \"i\"\n\nmain = do\n  x <- foo\n  yield x\n\nsrv = serve Api where\n  GetItems = \\req -> handler req\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.decls.len(), 3, "decls: {:?}", m.decls);
    match &m.decls[0].node {
        DeclKind::Impl { items, .. } => assert_eq!(items.len(), 1),
        other => panic!("expected Impl, got {:?}", other),
    }
    match &m.decls[1].node {
        DeclKind::Fun { body: Some(b), .. } => match &b.node {
            ExprKind::Do(stmts) => assert_eq!(stmts.len(), 2),
            other => panic!("expected Do, got {:?}", other),
        },
        other => panic!("expected Fun, got {:?}", other),
    }
    match &m.decls[2].node {
        DeclKind::Fun { body: Some(b), .. } => match &b.node {
            ExprKind::Serve { handlers, .. } => assert_eq!(handlers.len(), 1),
            other => panic!("expected Serve, got {:?}", other),
        },
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn case_arms_on_same_line_still_parse() {
    let src = "f = \\x -> case x of A {} -> 1; B {} -> 2\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.decls.len(), 1);
}

// ── 4. Unit Pow base parenthesization round-trips ──────────────────

#[test]
fn unit_pow_of_pow_round_trips() {
    let out = check_str("pow_pow", "x : Float ((M^2)^3)\nx = 1.0\n");
    assert!(out.contains("(M^2)^3"), "output: {}", out);
}

#[test]
fn unit_pow_of_product_round_trips() {
    let out = check_str("pow_mul", "x : Float ((M * S)^2)\nx = 1.0\n");
    assert!(out.contains("(M * S)^2"), "output: {}", out);
}

#[test]
fn unit_simple_pow_stays_minimal() {
    let out = check_str("pow_simple", "x : Float (M^2 * S)\nx = 1.0\n");
    assert!(out.contains("M^2 * S"), "output: {}", out);
}

// ── 5. Time-unit sugar suppressed for locally-bound identifiers ──────

/// Walk `e` and return true if any node is a Var with the given name.
fn expr_mentions_var(e: &knot::ast::Expr, name: &str) -> bool {
    match &e.node {
        ExprKind::Var(n) => n == name,
        ExprKind::App { func, arg } => {
            expr_mentions_var(func, name) || expr_mentions_var(arg, name)
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_mentions_var(lhs, name) || expr_mentions_var(rhs, name)
        }
        ExprKind::Lambda { body, .. } => expr_mentions_var(body, name),
        _ => false,
    }
}

#[test]
fn lambda_param_named_ms_is_not_unit_sugar() {
    let src = "f = \\ms -> g 2 ms\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Lambda { body, .. } = &b.node else {
        panic!("expected Lambda, got {:?}", b);
    };
    // Must be `(g 2) ms` — an application — not `g (2 * 1)`.
    assert!(
        expr_mentions_var(body, "ms"),
        "`ms` was desugared to a unit factor: {:?}",
        body
    );
    match &body.node {
        ExprKind::App { arg, .. } => {
            assert!(matches!(&arg.node, ExprKind::Var(n) if n == "ms"), "arg: {:?}", arg)
        }
        other => panic!("expected App, got {:?}", other),
    }
}

#[test]
fn do_bind_named_seconds_is_not_unit_sugar() {
    let src = "main = do\n  seconds <- foo\n  yield g 2 seconds\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Do(stmts) = &b.node else {
        panic!("expected Do, got {:?}", b);
    };
    let StmtKind::Expr(yield_expr) = &stmts[1].node else {
        panic!("expected Expr stmt, got {:?}", stmts[1]);
    };
    assert!(
        expr_mentions_var(yield_expr, "seconds"),
        "`seconds` was desugared to a unit factor: {:?}",
        yield_expr
    );
}

#[test]
fn do_let_named_ms_is_not_unit_sugar() {
    let src = "main = do\n  let ms = 5\n  yield g 2 ms\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Do(stmts) = &b.node else {
        panic!("expected Do, got {:?}", b);
    };
    let StmtKind::Expr(yield_expr) = &stmts[1].node else {
        panic!("expected Expr stmt, got {:?}", stmts[1]);
    };
    assert!(
        expr_mentions_var(yield_expr, "ms"),
        "`ms` was desugared to a unit factor: {:?}",
        yield_expr
    );
}

#[test]
fn let_in_named_ms_is_not_unit_sugar() {
    let src = "f = let ms = 5 in g 2 ms\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    // let-in desugars to `(\ms -> g 2 ms) 5`.
    let ExprKind::App { func, .. } = &b.node else {
        panic!("expected App, got {:?}", b);
    };
    let ExprKind::Lambda { body, .. } = &func.node else {
        panic!("expected Lambda, got {:?}", func);
    };
    assert!(
        expr_mentions_var(body, "ms"),
        "`ms` was desugared to a unit factor: {:?}",
        body
    );
}

#[test]
fn case_binder_named_ms_is_not_unit_sugar() {
    let src = "f = \\x -> case x of\n  Just {value: ms} -> g 2 ms\n  Nothing {} -> 0\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Lambda { body, .. } = &b.node else {
        panic!("expected Lambda, got {:?}", b);
    };
    let ExprKind::Case { arms, .. } = &body.node else {
        panic!("expected Case, got {:?}", body);
    };
    assert!(
        expr_mentions_var(&arms[0].body, "ms"),
        "`ms` was desugared to a unit factor: {:?}",
        arms[0].body
    );
}

#[test]
fn unbound_time_unit_sugar_still_desugars() {
    // No local binding named `ms` — `2 ms` is unit sugar: `2 * 1`.
    let src = "main = sleep (2 ms)\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::App { arg, .. } = &b.node else {
        panic!("expected App, got {:?}", b);
    };
    assert!(
        matches!(
            &arg.node,
            ExprKind::TimeUnitLit { value, .. }
                if matches!(&value.node, ExprKind::BinOp { op: BinOp::Mul, .. })
        ),
        "expected `2 ms` to desugar to a TimeUnitLit wrapping multiplication, got {:?}",
        arg
    );
}

#[test]
fn time_unit_sugar_unaffected_after_binder_scope_ends() {
    // The `\ms -> ...` scope is closed by the time `h` is parsed.
    let src = "f = \\ms -> ms\nh = sleep (3 seconds)\n";
    let m = parse(src).expect("parse");
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[1].node else {
        panic!("expected Fun");
    };
    let ExprKind::App { arg, .. } = &b.node else {
        panic!("expected App, got {:?}", b);
    };
    assert!(
        matches!(
            &arg.node,
            ExprKind::TimeUnitLit { value, .. }
                if matches!(&value.node, ExprKind::BinOp { op: BinOp::Mul, .. })
        ),
        "expected `3 seconds` to desugar to a TimeUnitLit wrapping multiplication, got {:?}",
        arg
    );
}

// ── 5b. B51: `knot fmt` preserves time-unit sugar, not raw multiply ──

#[test]
fn b51_time_unit_sugar_survives_formatting() {
    // Regression for B51: `2 seconds` must round-trip through the formatter
    // as `2 seconds`, not the desugared `2 * 1000`. `check_str` also asserts
    // the output reparses to the same AST and is idempotent.
    let out = check_str("b51_seconds", "main = sleep (2 seconds)\n");
    assert!(
        out.contains("2 seconds"),
        "time-unit sugar was rewritten to raw multiplication:\n{}",
        out
    );
    assert!(
        !out.contains('*'),
        "formatter leaked the desugared multiplication factor:\n{}",
        out
    );
    // The juxtaposition reads like an application, so it stays parenthesized
    // in argument position.
    assert!(
        out.contains("sleep (2 seconds)"),
        "expected parenthesized argument `sleep (2 seconds)`:\n{}",
        out
    );
}

#[test]
fn b51_time_unit_sugar_all_units_survive_formatting() {
    for (src, rendered) in [
        ("main = sleep (500 ms)\n", "500 ms"),
        ("main = sleep (30 minutes)\n", "30 minutes"),
        ("main = sleep (2 hours)\n", "2 hours"),
        ("main = sleep (365 days)\n", "365 days"),
        ("main = sleep (2 weeks)\n", "2 weeks"),
        ("main = sleep (1.5 hours)\n", "1.5 hours"),
    ] {
        let out = check_str("b51_units", src);
        assert!(
            out.contains(rendered),
            "expected `{}` preserved in formatter output, got:\n{}",
            rendered,
            out
        );
        assert!(
            !out.contains('*'),
            "formatter leaked a multiplication factor for `{}`:\n{}",
            rendered,
            out
        );
    }
}

// ── 6. `yield` keeps parens in argument position ────────────────────

#[test]
fn yield_in_argument_position_round_trips() {
    let out = check_str("yield_arg", "g = f (yield)\n");
    assert!(out.contains("f (yield)"), "output: {}", out);
}

#[test]
fn yield_statement_stays_bare() {
    let out = check_str("yield_stmt", "main = do\n  x <- foo\n  yield x\n");
    assert!(out.contains("yield x"), "output: {}", out);
    assert!(!out.contains("(yield)"), "output: {}", out);
}

// ── 7. Bare `Cons` constructor pattern round-trips ──────────────────

#[test]
fn bare_cons_pattern_round_trips() {
    let src = "f = \\x -> case x of\n  Cons -> 1\n  _ -> 2\n";
    let m = parse(src).expect("parse");
    // Verify the parse really is Constructor("Cons", Record([])).
    let DeclKind::Fun { body: Some(b), .. } = &m.decls[0].node else {
        panic!("expected Fun");
    };
    let ExprKind::Lambda { body, .. } = &b.node else {
        panic!("expected Lambda");
    };
    let ExprKind::Case { arms, .. } = &body.node else {
        panic!("expected Case, got {:?}", body);
    };
    match &arms[0].pat.node {
        PatKind::Constructor { name, payload } => {
            assert_eq!(name, "Cons");
            assert!(
                matches!(&payload.node, PatKind::Record(fs) if fs.is_empty()),
                "payload: {:?}",
                payload
            );
        }
        other => panic!("expected Constructor pattern, got {:?}", other),
    }
    let out = check_str("bare_cons", src);
    assert!(!out.contains("Cons {}"), "output: {}", out);
}

#[test]
fn cons_head_tail_pattern_still_round_trips() {
    let out = check_str(
        "cons_head_tail",
        "f = \\x -> case x of\n  Cons h t -> 1\n  _ -> 2\n",
    );
    assert!(out.contains("Cons h t"), "output: {}", out);
}

// ── 8. Right-nested unit products keep parens ───────────────────────

#[test]
fn right_nested_unit_product_round_trips() {
    let out = check_str("unit_mul_right", "x : Float (M * (S * Kg))\nx = 1.0\n");
    assert!(out.contains("M * (S * Kg)"), "output: {}", out);
}

#[test]
fn right_nested_unit_quotient_round_trips() {
    let out = check_str("unit_div_right", "x : Float (M / (S / Kg))\nx = 1.0\n");
    assert!(out.contains("M / (S / Kg)"), "output: {}", out);
}

#[test]
fn left_nested_unit_product_stays_minimal() {
    let out = check_str("unit_mul_left", "x : Float (M * S * Kg)\nx = 1.0\n");
    assert!(out.contains("M * S * Kg"), "output: {}", out);
}

// ── 9. Dashed import path segments ──────────────────────────────────

#[test]
fn dashed_import_path_parses() {
    let src = "import ./foo-bar\n\nmain = 1\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.imports.len(), 1);
    assert_eq!(m.imports[0].path, "./foo-bar");
}

#[test]
fn dashed_import_path_with_items_parses() {
    let src = "import ../lib/my-utils-extra (helper, Thing)\n\nmain = 1\n";
    let m = parse(src).expect("parse");
    assert_eq!(m.imports.len(), 1);
    assert_eq!(m.imports[0].path, "../lib/my-utils-extra");
    let items = m.imports[0].items.as_ref().expect("items");
    assert_eq!(items.len(), 2);
}

#[test]
fn dashed_import_round_trips() {
    let out = check_str("dashed_import", "import ./foo-bar\n\nmain = 1\n");
    assert!(out.contains("import ./foo-bar"), "output: {}", out);
}
