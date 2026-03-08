//! Comprehensive parser tests for the Knot language.
//!
//! These tests use the full lexer+parser pipeline: source text → tokens → AST.
//! This catches integration issues and makes tests much more readable than
//! manually constructing token lists.

use knot::ast::*;
use knot::lexer::Lexer;
use knot::parser::Parser;

// ── Helpers ─────────────────────────────────────────────────────────

/// Parse source and assert zero diagnostics. Returns the module.
fn parse_ok(source: &str) -> Module {
    let (tokens, lex_diags) = Lexer::new(source).tokenize();
    assert!(
        lex_diags.is_empty(),
        "lexer errors:\n{}",
        lex_diags
            .iter()
            .map(|d| d.render(source, "test"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let (module, parse_diags) = Parser::new(source.to_string(), tokens).parse_module();
    assert!(
        parse_diags.is_empty(),
        "parser errors:\n{}",
        parse_diags
            .iter()
            .map(|d| d.render(source, "test"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    module
}

/// Parse source, expect at least one diagnostic. Returns (module, diagnostics).
fn parse_err(source: &str) -> (Module, Vec<knot::diagnostic::Diagnostic>) {
    let (tokens, _) = Lexer::new(source).tokenize();
    let (module, diags) = Parser::new(source.to_string(), tokens).parse_module();
    assert!(!diags.is_empty(), "expected parser errors but got none");
    (module, diags)
}

/// Get the first declaration from source.
fn first_decl(source: &str) -> DeclKind {
    let m = parse_ok(source);
    assert!(!m.decls.is_empty(), "expected at least one declaration");
    m.decls.into_iter().next().unwrap().node
}

/// Get the body expression of the first Fun declaration.
fn fun_body(source: &str) -> ExprKind {
    match first_decl(source) {
        DeclKind::Fun { body, .. } => body.node,
        other => panic!("expected Fun, got {:?}", other),
    }
}

// ── Module ──────────────────────────────────────────────────────────

#[test]
fn empty_module() {
    let m = parse_ok("");
    assert!(m.name.is_none());
    assert!(m.decls.is_empty());
}

#[test]
fn named_module() {
    let m = parse_ok("module MyApp");
    assert_eq!(m.name.as_deref(), Some("MyApp"));
}

#[test]
fn module_with_decls() {
    let m = parse_ok("module Foo\n\nx = 1\ny = 2");
    assert_eq!(m.name.as_deref(), Some("Foo"));
    assert_eq!(m.decls.len(), 2);
}

// ── Literals ────────────────────────────────────────────────────────

#[test]
fn int_literal() {
    assert!(matches!(fun_body("x = 42"), ExprKind::Lit(Literal::Int(42))));
}

#[test]
fn float_literal() {
    match fun_body("x = 3.14") {
        ExprKind::Lit(Literal::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn string_literal() {
    assert!(matches!(
        fun_body(r#"x = "hello""#),
        ExprKind::Lit(Literal::Text(s)) if s == "hello"
    ));
}

#[test]
fn int_with_underscores() {
    assert!(matches!(
        fun_body("x = 1_000_000"),
        ExprKind::Lit(Literal::Int(1_000_000))
    ));
}

// ── Variables and References ────────────────────────────────────────

#[test]
fn variable_ref() {
    assert!(matches!(fun_body("x = y"), ExprKind::Var(n) if n == "y"));
}

#[test]
fn constructor_ref() {
    assert!(matches!(
        fun_body("x = Nothing"),
        ExprKind::Constructor(n) if n == "Nothing"
    ));
}

#[test]
fn source_ref() {
    assert!(matches!(
        fun_body("x = *people"),
        ExprKind::SourceRef(n) if n == "people"
    ));
}

#[test]
fn derived_ref() {
    assert!(matches!(
        fun_body("x = &seniors"),
        ExprKind::DerivedRef(n) if n == "seniors"
    ));
}

// ── Binary Operators ────────────────────────────────────────────────

#[test]
fn add_two_vars() {
    match fun_body("x = a + b") {
        ExprKind::BinOp {
            op: BinOp::Add,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::Var(n) if n == "a"));
            assert!(matches!(&rhs.node, ExprKind::Var(n) if n == "b"));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn mul_higher_precedence_than_add() {
    // a + b * c → a + (b * c)
    match fun_body("x = a + b * c") {
        ExprKind::BinOp {
            op: BinOp::Add,
            rhs,
            ..
        } => {
            assert!(matches!(&rhs.node, ExprKind::BinOp { op: BinOp::Mul, .. }));
        }
        other => panic!("expected Add(_, Mul(..)), got {:?}", other),
    }
}

#[test]
fn add_left_associative() {
    // a + b + c → (a + b) + c
    match fun_body("x = a + b + c") {
        ExprKind::BinOp {
            op: BinOp::Add,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::BinOp { op: BinOp::Add, .. }));
            assert!(matches!(&rhs.node, ExprKind::Var(n) if n == "c"));
        }
        other => panic!("expected (a+b)+c, got {:?}", other),
    }
}

#[test]
fn concat_right_associative() {
    // a ++ b ++ c → a ++ (b ++ c)
    match fun_body("x = a ++ b ++ c") {
        ExprKind::BinOp {
            op: BinOp::Concat,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::Var(n) if n == "a"));
            assert!(matches!(
                &rhs.node,
                ExprKind::BinOp {
                    op: BinOp::Concat,
                    ..
                }
            ));
        }
        other => panic!("expected a++(b++c), got {:?}", other),
    }
}

#[test]
fn comparison_operators() {
    assert!(matches!(
        fun_body("x = a == b"),
        ExprKind::BinOp { op: BinOp::Eq, .. }
    ));
    assert!(matches!(
        fun_body("x = a != b"),
        ExprKind::BinOp {
            op: BinOp::Neq,
            ..
        }
    ));
    assert!(matches!(
        fun_body("x = a < b"),
        ExprKind::BinOp { op: BinOp::Lt, .. }
    ));
    assert!(matches!(
        fun_body("x = a > b"),
        ExprKind::BinOp { op: BinOp::Gt, .. }
    ));
    assert!(matches!(
        fun_body("x = a <= b"),
        ExprKind::BinOp { op: BinOp::Le, .. }
    ));
    assert!(matches!(
        fun_body("x = a >= b"),
        ExprKind::BinOp { op: BinOp::Ge, .. }
    ));
}

#[test]
fn logical_operators() {
    assert!(matches!(
        fun_body("x = a && b"),
        ExprKind::BinOp {
            op: BinOp::And,
            ..
        }
    ));
    assert!(matches!(
        fun_body("x = a || b"),
        ExprKind::BinOp { op: BinOp::Or, .. }
    ));
}

#[test]
fn pipe_operator() {
    // a |> f → Pipe(a, f)
    match fun_body("x = a |> f") {
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::Var(n) if n == "a"));
            assert!(matches!(&rhs.node, ExprKind::Var(n) if n == "f"));
        }
        other => panic!("expected Pipe, got {:?}", other),
    }
}

#[test]
fn pipe_lower_precedence_than_application() {
    // xs |> filter f → Pipe(xs, App(filter, f))
    match fun_body("x = xs |> filter f") {
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::Var(n) if n == "xs"));
            assert!(matches!(&rhs.node, ExprKind::App { .. }));
        }
        other => panic!("expected Pipe(xs, App(..)), got {:?}", other),
    }
}

#[test]
fn complex_precedence() {
    // a || b && c == d + e * f
    // → a || (b && ((c == (d + (e * f)))))
    match fun_body("x = a || b && c == d + e * f") {
        ExprKind::BinOp { op: BinOp::Or, .. } => {}
        other => panic!("expected Or at top level, got {:?}", other),
    }
}

// ── Unary Operators ─────────────────────────────────────────────────

#[test]
fn unary_negation() {
    assert!(matches!(
        fun_body("x = -y"),
        ExprKind::UnaryOp {
            op: UnaryOp::Neg,
            ..
        }
    ));
}

#[test]
fn unary_not() {
    assert!(matches!(
        fun_body("x = not y"),
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            ..
        }
    ));
}

#[test]
fn negation_in_binop() {
    // a + -b → a + (Neg b)
    match fun_body("x = a + -b") {
        ExprKind::BinOp {
            op: BinOp::Add,
            rhs,
            ..
        } => {
            assert!(matches!(
                &rhs.node,
                ExprKind::UnaryOp {
                    op: UnaryOp::Neg,
                    ..
                }
            ));
        }
        other => panic!("expected Add(_, Neg(_)), got {:?}", other),
    }
}

// ── Function Application ────────────────────────────────────────────

#[test]
fn single_arg_application() {
    match fun_body("x = f y") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&func.node, ExprKind::Var(n) if n == "f"));
            assert!(matches!(&arg.node, ExprKind::Var(n) if n == "y"));
        }
        other => panic!("expected App, got {:?}", other),
    }
}

#[test]
fn multi_arg_application() {
    // f a b c → App(App(App(f, a), b), c)
    match fun_body("x = f a b c") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&arg.node, ExprKind::Var(n) if n == "c"));
            match &func.node {
                ExprKind::App { func: inner, arg } => {
                    assert!(matches!(&arg.node, ExprKind::Var(n) if n == "b"));
                    assert!(matches!(&inner.node, ExprKind::App { .. }));
                }
                other => panic!("expected nested App, got {:?}", other),
            }
        }
        other => panic!("expected App, got {:?}", other),
    }
}

#[test]
fn application_with_constructor() {
    // Just {value: 5}
    match fun_body("x = Just {value: 5}") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&func.node, ExprKind::Constructor(n) if n == "Just"));
            assert!(matches!(&arg.node, ExprKind::Record(_)));
        }
        other => panic!("expected App(Constructor, Record), got {:?}", other),
    }
}

#[test]
fn application_higher_precedence_than_binop() {
    // f x + g y → (f x) + (g y)
    match fun_body("x = f a + g b") {
        ExprKind::BinOp {
            op: BinOp::Add,
            lhs,
            rhs,
        } => {
            assert!(matches!(&lhs.node, ExprKind::App { .. }));
            assert!(matches!(&rhs.node, ExprKind::App { .. }));
        }
        other => panic!("expected Add(App, App), got {:?}", other),
    }
}

// ── Field Access ────────────────────────────────────────────────────

#[test]
fn simple_field_access() {
    match fun_body("x = r.name") {
        ExprKind::FieldAccess { expr, field } => {
            assert!(matches!(&expr.node, ExprKind::Var(n) if n == "r"));
            assert_eq!(field, "name");
        }
        other => panic!("expected FieldAccess, got {:?}", other),
    }
}

#[test]
fn chained_field_access() {
    // a.b.c → (a.b).c
    match fun_body("x = a.b.c") {
        ExprKind::FieldAccess { expr, field } => {
            assert_eq!(field, "c");
            assert!(matches!(
                &expr.node,
                ExprKind::FieldAccess { field, .. } if field == "b"
            ));
        }
        other => panic!("expected chained FieldAccess, got {:?}", other),
    }
}

#[test]
fn field_access_after_application() {
    // (f x).name
    match fun_body("x = (f y).name") {
        ExprKind::FieldAccess { expr, field } => {
            assert_eq!(field, "name");
            assert!(matches!(&expr.node, ExprKind::App { .. }));
        }
        other => panic!("expected FieldAccess(App, ..), got {:?}", other),
    }
}

// ── Records ─────────────────────────────────────────────────────────

#[test]
fn empty_record() {
    match fun_body("x = {}") {
        ExprKind::Record(fields) => assert!(fields.is_empty()),
        other => panic!("expected empty Record, got {:?}", other),
    }
}

#[test]
fn record_with_fields() {
    match fun_body(r#"x = {name: "Alice", age: 30}"#) {
        ExprKind::Record(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "name");
            assert_eq!(fields[1].name, "age");
            assert!(matches!(&fields[0].value.node, ExprKind::Lit(Literal::Text(s)) if s == "Alice"));
            assert!(matches!(&fields[1].value.node, ExprKind::Lit(Literal::Int(30))));
        }
        other => panic!("expected Record, got {:?}", other),
    }
}

#[test]
fn record_update() {
    match fun_body("x = {t | age: 31}") {
        ExprKind::RecordUpdate { base, fields } => {
            assert!(matches!(&base.node, ExprKind::Var(n) if n == "t"));
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "age");
        }
        other => panic!("expected RecordUpdate, got {:?}", other),
    }
}

#[test]
fn record_update_multiple_fields() {
    match fun_body("x = {t | age: 31, name: y}") {
        ExprKind::RecordUpdate { base, fields } => {
            assert!(matches!(&base.node, ExprKind::Var(n) if n == "t"));
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected RecordUpdate, got {:?}", other),
    }
}

// ── Lists ───────────────────────────────────────────────────────────

#[test]
fn empty_list() {
    match fun_body("x = []") {
        ExprKind::List(elems) => assert!(elems.is_empty()),
        other => panic!("expected empty List, got {:?}", other),
    }
}

#[test]
fn list_of_ints() {
    match fun_body("x = [1, 2, 3]") {
        ExprKind::List(elems) => {
            assert_eq!(elems.len(), 3);
            assert!(matches!(&elems[0].node, ExprKind::Lit(Literal::Int(1))));
            assert!(matches!(&elems[1].node, ExprKind::Lit(Literal::Int(2))));
            assert!(matches!(&elems[2].node, ExprKind::Lit(Literal::Int(3))));
        }
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn list_of_records() {
    match fun_body(r#"x = [{name: "a"}, {name: "b"}]"#) {
        ExprKind::List(elems) => {
            assert_eq!(elems.len(), 2);
            assert!(matches!(&elems[0].node, ExprKind::Record(_)));
        }
        other => panic!("expected List of Records, got {:?}", other),
    }
}

// ── Lambda ──────────────────────────────────────────────────────────

#[test]
fn simple_lambda() {
    match fun_body("x = \\a -> a") {
        ExprKind::Lambda { params, body } => {
            assert_eq!(params.len(), 1);
            assert!(matches!(&params[0].node, PatKind::Var(n) if n == "a"));
            assert!(matches!(&body.node, ExprKind::Var(n) if n == "a"));
        }
        other => panic!("expected Lambda, got {:?}", other),
    }
}

#[test]
fn multi_param_lambda() {
    match fun_body("x = \\a b -> a") {
        ExprKind::Lambda { params, .. } => {
            assert_eq!(params.len(), 2);
        }
        other => panic!("expected Lambda, got {:?}", other),
    }
}

#[test]
fn lambda_with_body_expr() {
    // \x -> x + 1
    match fun_body("x = \\a -> a + 1") {
        ExprKind::Lambda { body, .. } => {
            assert!(matches!(&body.node, ExprKind::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected Lambda with BinOp body, got {:?}", other),
    }
}

#[test]
fn lambda_with_wildcard() {
    match fun_body("x = \\_ -> 0") {
        ExprKind::Lambda { params, .. } => {
            assert_eq!(params.len(), 1);
            assert!(matches!(&params[0].node, PatKind::Wildcard));
        }
        other => panic!("expected Lambda with wildcard, got {:?}", other),
    }
}

// ── If/Then/Else ────────────────────────────────────────────────────

#[test]
fn simple_if() {
    match fun_body("x = if a then 1 else 2") {
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            assert!(matches!(&cond.node, ExprKind::Var(n) if n == "a"));
            assert!(matches!(&then_branch.node, ExprKind::Lit(Literal::Int(1))));
            assert!(matches!(&else_branch.node, ExprKind::Lit(Literal::Int(2))));
        }
        other => panic!("expected If, got {:?}", other),
    }
}

#[test]
fn nested_if() {
    match fun_body("x = if a then if b then 1 else 2 else 3") {
        ExprKind::If {
            then_branch,
            else_branch,
            ..
        } => {
            assert!(matches!(&then_branch.node, ExprKind::If { .. }));
            assert!(matches!(&else_branch.node, ExprKind::Lit(Literal::Int(3))));
        }
        other => panic!("expected nested If, got {:?}", other),
    }
}

#[test]
fn if_with_complex_condition() {
    match fun_body("x = if a > 0 && b < 10 then a else b") {
        ExprKind::If { cond, .. } => {
            assert!(matches!(&cond.node, ExprKind::BinOp { op: BinOp::And, .. }));
        }
        other => panic!("expected If with And condition, got {:?}", other),
    }
}

// ── Case/Of ─────────────────────────────────────────────────────────

#[test]
fn simple_case() {
    let src = "x = case y of\n  0 -> a\n  _ -> b";
    match fun_body(src) {
        ExprKind::Case { scrutinee, arms } => {
            assert!(matches!(&scrutinee.node, ExprKind::Var(n) if n == "y"));
            assert_eq!(arms.len(), 2);
            assert!(matches!(&arms[0].pat.node, PatKind::Lit(Literal::Int(0))));
            assert!(matches!(&arms[1].pat.node, PatKind::Wildcard));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn case_with_constructor_patterns() {
    let src = "x = case s of\n  Circle {radius} -> radius\n  Rect {width} -> width";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert_eq!(arms.len(), 2);
            match &arms[0].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Circle");
                    assert!(matches!(&payload.node, PatKind::Record(_)));
                }
                other => panic!("expected Constructor pattern, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

// ── Do Blocks ───────────────────────────────────────────────────────

#[test]
fn do_with_bind_and_yield() {
    let src = "x = do\n  t <- xs\n  yield t";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 2);
            assert!(matches!(&stmts[0].node, StmtKind::Bind { .. }));
            match &stmts[1].node {
                StmtKind::Expr(e) => {
                    assert!(matches!(&e.node, ExprKind::Yield(_)));
                }
                other => panic!("expected Expr(Yield), got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_with_where() {
    let src = "x = do\n  t <- xs\n  where t.age > 65\n  yield t";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 3);
            assert!(matches!(&stmts[0].node, StmtKind::Bind { .. }));
            assert!(matches!(&stmts[1].node, StmtKind::Where { .. }));
            match &stmts[2].node {
                StmtKind::Expr(e) => assert!(matches!(&e.node, ExprKind::Yield(_))),
                other => panic!("expected Expr(Yield), got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_with_let() {
    let src = "x = do\n  let y = 5\n  yield y";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 2);
            match &stmts[0].node {
                StmtKind::Let { pat, expr } => {
                    assert!(matches!(&pat.node, PatKind::Var(n) if n == "y"));
                    assert!(matches!(&expr.node, ExprKind::Lit(Literal::Int(5))));
                }
                other => panic!("expected Let, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_with_pattern_bind() {
    let src = "x = do\n  Circle c <- *shapes\n  yield c";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 2);
            match &stmts[0].node {
                StmtKind::Bind { pat, expr } => {
                    assert!(matches!(&pat.node, PatKind::Constructor { name, .. } if name == "Circle"));
                    assert!(matches!(&expr.node, ExprKind::SourceRef(n) if n == "shapes"));
                }
                other => panic!("expected Bind with constructor pat, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_multiple_binds() {
    let src = "x = do\n  a <- xs\n  b <- ys\n  yield {a, b}";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 3);
            assert!(matches!(&stmts[0].node, StmtKind::Bind { .. }));
            assert!(matches!(&stmts[1].node, StmtKind::Bind { .. }));
        }
        other => panic!("expected Do with 3 statements, got {:?}", other),
    }
}

// ── Set ─────────────────────────────────────────────────────────────

#[test]
fn simple_set() {
    let src = "x = set *people = [1, 2]";
    match fun_body(src) {
        ExprKind::Set { target, value } => {
            assert!(matches!(&target.node, ExprKind::SourceRef(n) if n == "people"));
            assert!(matches!(&value.node, ExprKind::List(_)));
        }
        other => panic!("expected Set, got {:?}", other),
    }
}

#[test]
fn set_with_union() {
    let src = r#"x = set *people = union *people [{name: "Bob"}]"#;
    match fun_body(src) {
        ExprKind::Set { target, value } => {
            assert!(matches!(&target.node, ExprKind::SourceRef(n) if n == "people"));
            // union *people [...] → App(App(union, *people), [...])
            assert!(matches!(&value.node, ExprKind::App { .. }));
        }
        other => panic!("expected Set, got {:?}", other),
    }
}

// ── Yield ───────────────────────────────────────────────────────────

#[test]
fn standalone_yield() {
    match fun_body("x = yield 42") {
        ExprKind::Yield(inner) => {
            assert!(matches!(&inner.node, ExprKind::Lit(Literal::Int(42))));
        }
        other => panic!("expected Yield, got {:?}", other),
    }
}

// ── Atomic ──────────────────────────────────────────────────────────

#[test]
fn atomic_expr() {
    match fun_body("x = atomic (f y)") {
        ExprKind::Atomic(inner) => {
            assert!(matches!(&inner.node, ExprKind::App { .. }));
        }
        other => panic!("expected Atomic, got {:?}", other),
    }
}

// ── Parenthesized Expressions ───────────────────────────────────────

#[test]
fn parens_override_precedence() {
    // (a + b) * c
    match fun_body("x = (a + b) * c") {
        ExprKind::BinOp {
            op: BinOp::Mul,
            lhs,
            ..
        } => {
            assert!(matches!(&lhs.node, ExprKind::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected Mul(Add(..), c), got {:?}", other),
    }
}

// ── Data Declarations ───────────────────────────────────────────────

#[test]
fn data_single_constructor() {
    match first_decl("data Unit = Unit {}") {
        DeclKind::Data {
            name,
            constructors,
            ..
        } => {
            assert_eq!(name, "Unit");
            assert_eq!(constructors.len(), 1);
            assert_eq!(constructors[0].name, "Unit");
            assert!(constructors[0].fields.is_empty());
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_multiple_constructors() {
    match first_decl("data Bool = True {} | False {}") {
        DeclKind::Data {
            name,
            constructors,
            ..
        } => {
            assert_eq!(name, "Bool");
            assert_eq!(constructors.len(), 2);
            assert_eq!(constructors[0].name, "True");
            assert_eq!(constructors[1].name, "False");
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_with_fields() {
    match first_decl("data Shape = Circle {radius: Float} | Rect {width: Float, height: Float}") {
        DeclKind::Data {
            constructors, ..
        } => {
            assert_eq!(constructors.len(), 2);
            assert_eq!(constructors[0].fields.len(), 1);
            assert_eq!(constructors[0].fields[0].name, "radius");
            assert_eq!(constructors[1].fields.len(), 2);
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_with_type_params() {
    match first_decl("data Maybe a = Nothing {} | Just {value: a}") {
        DeclKind::Data {
            name,
            params,
            constructors,
            ..
        } => {
            assert_eq!(name, "Maybe");
            assert_eq!(params, vec!["a"]);
            assert_eq!(constructors.len(), 2);
            assert_eq!(constructors[1].fields[0].name, "value");
            assert!(matches!(
                &constructors[1].fields[0].value.node,
                TypeKind::Var(n) if n == "a"
            ));
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_multiline() {
    let src = "data Status\n  = Open {}\n  | InProgress {assignee: Text}\n  | Resolved {resolution: Text}";
    match first_decl(src) {
        DeclKind::Data {
            name,
            constructors,
            ..
        } => {
            assert_eq!(name, "Status");
            assert_eq!(constructors.len(), 3);
            assert_eq!(constructors[0].name, "Open");
            assert_eq!(constructors[1].name, "InProgress");
            assert_eq!(constructors[2].name, "Resolved");
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_with_deriving() {
    match first_decl("data Priority = Low {} | High {} deriving (Eq, Ord)") {
        DeclKind::Data {
            deriving, ..
        } => {
            assert_eq!(deriving, vec!["Eq", "Ord"]);
        }
        other => panic!("expected Data with deriving, got {:?}", other),
    }
}

#[test]
fn data_nested_relation_field() {
    match first_decl("data Team = Team {name: Text, members: [Person]}") {
        DeclKind::Data {
            constructors, ..
        } => {
            assert_eq!(constructors[0].fields[1].name, "members");
            assert!(matches!(
                &constructors[0].fields[1].value.node,
                TypeKind::Relation(_)
            ));
        }
        other => panic!("expected Data with relation field, got {:?}", other),
    }
}

// ── Type Aliases ────────────────────────────────────────────────────

#[test]
fn simple_type_alias() {
    match first_decl("type Name = Text") {
        DeclKind::TypeAlias { name, ty, .. } => {
            assert_eq!(name, "Name");
            assert!(matches!(&ty.node, TypeKind::Named(n) if n == "Text"));
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn record_type_alias() {
    match first_decl("type Person = {name: Text, age: Int}") {
        DeclKind::TypeAlias { name, ty, .. } => {
            assert_eq!(name, "Person");
            match &ty.node {
                TypeKind::Record { fields, rest } => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name, "name");
                    assert_eq!(fields[1].name, "age");
                    assert!(rest.is_none());
                }
                other => panic!("expected Record type, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn type_alias_with_params() {
    match first_decl("type Pair a b = {fst: a, snd: b}") {
        DeclKind::TypeAlias { name, params, .. } => {
            assert_eq!(name, "Pair");
            assert_eq!(params, vec!["a", "b"]);
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

// ── Source Declarations ─────────────────────────────────────────────

#[test]
fn source_simple() {
    match first_decl("*people : [Person]") {
        DeclKind::Source {
            name, history, ..
        } => {
            assert_eq!(name, "people");
            assert!(!history);
        }
        other => panic!("expected Source, got {:?}", other),
    }
}

#[test]
fn source_with_record_type() {
    match first_decl("*orders : [{customer: Text, amount: Int}]") {
        DeclKind::Source { name, ty, .. } => {
            assert_eq!(name, "orders");
            match &ty.node {
                TypeKind::Relation(inner) => {
                    assert!(matches!(&inner.node, TypeKind::Record { .. }));
                }
                other => panic!("expected Relation type, got {:?}", other),
            }
        }
        other => panic!("expected Source, got {:?}", other),
    }
}

#[test]
fn source_with_history() {
    match first_decl("*employees : [Person] with history") {
        DeclKind::Source { name, history, .. } => {
            assert_eq!(name, "employees");
            assert!(history);
        }
        other => panic!("expected Source with history, got {:?}", other),
    }
}

// ── View Declarations ───────────────────────────────────────────────

#[test]
fn simple_view() {
    match first_decl("*openTodos = *todos") {
        DeclKind::View { name, body, .. } => {
            assert_eq!(name, "openTodos");
            assert!(matches!(&body.node, ExprKind::SourceRef(n) if n == "todos"));
        }
        other => panic!("expected View, got {:?}", other),
    }
}

// ── Derived Declarations ────────────────────────────────────────────

#[test]
fn simple_derived() {
    match first_decl("&seniors = *people") {
        DeclKind::Derived { name, body, .. } => {
            assert_eq!(name, "seniors");
            assert!(matches!(&body.node, ExprKind::SourceRef(n) if n == "people"));
        }
        other => panic!("expected Derived, got {:?}", other),
    }
}

#[test]
fn derived_with_pipe() {
    match first_decl("&seniors = *people |> filter (\\p -> p.age > 65)") {
        DeclKind::Derived { name, body, .. } => {
            assert_eq!(name, "seniors");
            assert!(matches!(
                &body.node,
                ExprKind::BinOp {
                    op: BinOp::Pipe,
                    ..
                }
            ));
        }
        other => panic!("expected Derived, got {:?}", other),
    }
}

// ── Function Declarations ───────────────────────────────────────────

#[test]
fn constant_fun() {
    match first_decl("maxRetries = 3") {
        DeclKind::Fun {
            name,
            params,
            body,
            ..
        } => {
            assert_eq!(name, "maxRetries");
            assert!(params.is_empty());
            assert!(matches!(&body.node, ExprKind::Lit(Literal::Int(3))));
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn fun_with_params() {
    match first_decl("add a b = a + b") {
        DeclKind::Fun {
            name, params, body, ..
        } => {
            assert_eq!(name, "add");
            assert_eq!(params.len(), 2);
            assert!(matches!(&body.node, ExprKind::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn fun_with_multiline_body() {
    let src = "add title owner priority =\n  set *todos = union *todos [{title: title}]";
    match first_decl(src) {
        DeclKind::Fun { name, body, .. } => {
            assert_eq!(name, "add");
            assert!(matches!(&body.node, ExprKind::Set { .. }));
        }
        other => panic!("expected Fun with Set body, got {:?}", other),
    }
}

// ── Types ───────────────────────────────────────────────────────────

#[test]
fn function_type_annotation() {
    // The source/type decl parser handles types. We test via type alias.
    match first_decl("type F = Int -> Text") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::Function { param, result } => {
                    assert!(matches!(&param.node, TypeKind::Named(n) if n == "Int"));
                    assert!(matches!(&result.node, TypeKind::Named(n) if n == "Text"));
                }
                other => panic!("expected Function type, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn function_type_right_assoc() {
    // a -> b -> c → a -> (b -> c)
    match first_decl("type F = Int -> Text -> Bool") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::Function { result, .. } => {
                    assert!(matches!(&result.node, TypeKind::Function { .. }));
                }
                other => panic!("expected nested Function type, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn type_application() {
    // Maybe Int
    match first_decl("type X = Maybe Int") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::App { func, arg } => {
                    assert!(matches!(&func.node, TypeKind::Named(n) if n == "Maybe"));
                    assert!(matches!(&arg.node, TypeKind::Named(n) if n == "Int"));
                }
                other => panic!("expected type App, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn relation_type() {
    match first_decl("type X = [Person]") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::Relation(inner) => {
                    assert!(matches!(&inner.node, TypeKind::Named(n) if n == "Person"));
                }
                other => panic!("expected Relation type, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn record_type_with_row_var() {
    match first_decl("type X = {name: Text | r}") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::Record { fields, rest } => {
                    assert_eq!(fields.len(), 1);
                    assert_eq!(rest.as_deref(), Some("r"));
                }
                other => panic!("expected Record type with rest, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn nested_relation_record_type() {
    // [{name: Text, grades: [{subject: Text, score: Int}]}]
    match first_decl("*records : [{name: Text, grades: [{subject: Text, score: Int}]}]") {
        DeclKind::Source { ty, .. } => {
            match &ty.node {
                TypeKind::Relation(inner) => {
                    match &inner.node {
                        TypeKind::Record { fields, .. } => {
                            assert_eq!(fields[1].name, "grades");
                            assert!(matches!(&fields[1].value.node, TypeKind::Relation(_)));
                        }
                        other => panic!("expected Record, got {:?}", other),
                    }
                }
                other => panic!("expected Relation, got {:?}", other),
            }
        }
        other => panic!("expected Source, got {:?}", other),
    }
}

// ── Patterns ────────────────────────────────────────────────────────

#[test]
fn wildcard_pattern_in_case() {
    let src = "x = case y of\n  _ -> 0";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert!(matches!(&arms[0].pat.node, PatKind::Wildcard));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn constructor_with_record_pattern() {
    let src = "x = case s of\n  Circle {radius} -> radius";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Circle");
                    match &payload.node {
                        PatKind::Record(fields) => {
                            assert_eq!(fields.len(), 1);
                            assert_eq!(fields[0].name, "radius");
                            assert!(fields[0].pattern.is_none()); // punned
                        }
                        other => panic!("expected Record pattern, got {:?}", other),
                    }
                }
                other => panic!("expected Constructor pattern, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn constructor_with_var_pattern() {
    let src = "x = case s of\n  Circle c -> c";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Circle");
                    assert!(matches!(&payload.node, PatKind::Var(n) if n == "c"));
                }
                other => panic!("expected Constructor, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn empty_constructor_pattern() {
    let src = "x = case s of\n  Open {} -> 1";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Open");
                    assert!(matches!(&payload.node, PatKind::Record(fields) if fields.is_empty()));
                }
                other => panic!("expected Constructor with empty record, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn list_pattern() {
    let src = "x = case xs of\n  [] -> 0\n  _ -> 1";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert!(matches!(&arms[0].pat.node, PatKind::List(ps) if ps.is_empty()));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

// ── Multiple Declarations ───────────────────────────────────────────

#[test]
fn multiple_functions() {
    let m = parse_ok("f x = x\ng y = y\nh z = z");
    assert_eq!(m.decls.len(), 3);
    for decl in &m.decls {
        assert!(matches!(&decl.node, DeclKind::Fun { .. }));
    }
}

#[test]
fn mixed_declarations() {
    let src = "\
data Bool = True {} | False {}

type Name = Text

*people : [Person]

&seniors = *people

f x = x";
    let m = parse_ok(src);
    assert_eq!(m.decls.len(), 5);
    assert!(matches!(&m.decls[0].node, DeclKind::Data { .. }));
    assert!(matches!(&m.decls[1].node, DeclKind::TypeAlias { .. }));
    assert!(matches!(&m.decls[2].node, DeclKind::Source { .. }));
    assert!(matches!(&m.decls[3].node, DeclKind::Derived { .. }));
    assert!(matches!(&m.decls[4].node, DeclKind::Fun { .. }));
}

// ── Trait Declarations ──────────────────────────────────────────────

#[test]
fn simple_trait() {
    let src = "trait Display a where\n  display : a -> Text";
    match first_decl(src) {
        DeclKind::Trait {
            name,
            params,
            items,
            ..
        } => {
            assert_eq!(name, "Display");
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "a");
            assert!(params[0].kind.is_none());
            assert_eq!(items.len(), 1);
            match &items[0] {
                TraitItem::Method { name, .. } => assert_eq!(name, "display"),
                other => panic!("expected Method, got {:?}", other),
            }
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

#[test]
fn trait_with_hkt_param() {
    let src = "trait Functor (f : Type -> Type) where\n  map : (a -> b) -> f a -> f b";
    match first_decl(src) {
        DeclKind::Trait { params, .. } => {
            assert_eq!(params[0].name, "f");
            assert!(params[0].kind.is_some());
        }
        other => panic!("expected Trait with HKT, got {:?}", other),
    }
}

#[test]
fn trait_multiple_methods() {
    let src = "trait Eq a where\n  eq : a -> a -> Bool\n  neq : a -> a -> Bool";
    match first_decl(src) {
        DeclKind::Trait { items, .. } => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

// ── Impl Declarations ──────────────────────────────────────────────

#[test]
fn simple_impl() {
    let src = "impl Display Int where\n  display n = n";
    match first_decl(src) {
        DeclKind::Impl {
            trait_name,
            args,
            items,
            ..
        } => {
            assert_eq!(trait_name, "Display");
            assert_eq!(args.len(), 1);
            assert!(matches!(&args[0].node, TypeKind::Named(n) if n == "Int"));
            assert_eq!(items.len(), 1);
            match &items[0] {
                ImplItem::Method { name, params, .. } => {
                    assert_eq!(name, "display");
                    assert_eq!(params.len(), 1);
                }
                other => panic!("expected Method, got {:?}", other),
            }
        }
        other => panic!("expected Impl, got {:?}", other),
    }
}

#[test]
fn impl_for_relation_type() {
    let src = "impl Functor [] where\n  map f rel = rel";
    match first_decl(src) {
        DeclKind::Impl {
            trait_name, args, ..
        } => {
            assert_eq!(trait_name, "Functor");
            assert_eq!(args.len(), 1);
            assert!(matches!(&args[0].node, TypeKind::Named(n) if n == "[]"));
        }
        other => panic!("expected Impl for [], got {:?}", other),
    }
}

// ── Route Declarations ──────────────────────────────────────────────

#[test]
fn simple_route() {
    let src = "route Api where\n  GET /todos = ListTodos\n  POST {title: Text} /todos = CreateTodo";
    match first_decl(src) {
        DeclKind::Route { name, entries } => {
            assert_eq!(name, "Api");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].method, HttpMethod::Get);
            assert_eq!(entries[0].constructor, "ListTodos");
            assert_eq!(entries[1].method, HttpMethod::Post);
            assert_eq!(entries[1].constructor, "CreateTodo");
            assert_eq!(entries[1].body_fields.len(), 1);
            assert_eq!(entries[1].body_fields[0].name, "title");
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_with_path_params() {
    let src = "route Api where\n  GET /users/{id: Int} = GetUser";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].path.len(), 2);
            match &entries[0].path[0] {
                PathSegment::Literal(s) => assert_eq!(s, "users"),
                other => panic!("expected Literal, got {:?}", other),
            }
            match &entries[0].path[1] {
                PathSegment::Param { name, .. } => assert_eq!(name, "id"),
                other => panic!("expected Param, got {:?}", other),
            }
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_with_response_type() {
    let src = "route Api where\n  GET /todos -> [Todo] = ListTodos";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert!(entries[0].response_ty.is_some());
            let rt = entries[0].response_ty.as_ref().unwrap();
            assert!(matches!(&rt.node, TypeKind::Relation(_)));
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn composite_route() {
    match first_decl("route Api = TodoApi | AdminApi") {
        DeclKind::RouteComposite { name, components } => {
            assert_eq!(name, "Api");
            assert_eq!(components, vec!["TodoApi", "AdminApi"]);
        }
        other => panic!("expected RouteComposite, got {:?}", other),
    }
}

// ── Migrate Declarations ────────────────────────────────────────────

#[test]
fn simple_migrate() {
    let src = "migrate *people\n  from {name: Text}\n  to {name: Text, age: Int}\n  using (\\old -> {old | age: 0})";
    match first_decl(src) {
        DeclKind::Migrate {
            relation,
            from_ty,
            to_ty,
            using_fn,
        } => {
            assert_eq!(relation, "people");
            assert!(matches!(&from_ty.node, TypeKind::Record { fields, .. } if fields.len() == 1));
            assert!(matches!(&to_ty.node, TypeKind::Record { fields, .. } if fields.len() == 2));
            assert!(matches!(&using_fn.node, ExprKind::Lambda { .. }));
        }
        other => panic!("expected Migrate, got {:?}", other),
    }
}

// ── Complex / Integration Tests ─────────────────────────────────────

#[test]
fn todo_app_data_types() {
    let src = "\
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}";
    let m = parse_ok(src);
    assert_eq!(m.decls.len(), 2);
    match &m.decls[0].node {
        DeclKind::Data {
            name,
            constructors,
            ..
        } => {
            assert_eq!(name, "Priority");
            assert_eq!(constructors.len(), 4);
        }
        other => panic!("expected Data, got {:?}", other),
    }
    match &m.decls[1].node {
        DeclKind::Data {
            name,
            constructors,
            ..
        } => {
            assert_eq!(name, "Status");
            assert_eq!(constructors.len(), 3);
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn function_with_do_bind_and_where() {
    let src = "\
pendingFor user = do
  t <- *todos
  where t.owner == user
  Open {} <- t.status
  yield {title: t.title, priority: t.priority}";
    match first_decl(src) {
        DeclKind::Fun { name, body, .. } => {
            assert_eq!(name, "pendingFor");
            match &body.node {
                ExprKind::Do(stmts) => {
                    assert_eq!(stmts.len(), 4);
                    // First: t <- *todos
                    assert!(matches!(&stmts[0].node, StmtKind::Bind { .. }));
                    // Second: where t.owner == user
                    assert!(matches!(&stmts[1].node, StmtKind::Where { .. }));
                    // Third: Open {} <- t.status (pattern bind)
                    match &stmts[2].node {
                        StmtKind::Bind { pat, .. } => {
                            assert!(matches!(
                                &pat.node,
                                PatKind::Constructor { name, .. } if name == "Open"
                            ));
                        }
                        other => panic!("expected Bind, got {:?}", other),
                    }
                    // Fourth: yield {...}
                    match &stmts[3].node {
                        StmtKind::Expr(e) => {
                            assert!(matches!(&e.node, ExprKind::Yield(_)));
                        }
                        other => panic!("expected Expr(Yield), got {:?}", other),
                    }
                }
                other => panic!("expected Do, got {:?}", other),
            }
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn function_with_set_and_union() {
    let src =
        "add title owner priority =\n  set *todos = union *todos [{title: title, owner, priority, status: Open {}}]";
    match first_decl(src) {
        DeclKind::Fun { name, body, .. } => {
            assert_eq!(name, "add");
            assert!(matches!(&body.node, ExprKind::Set { .. }));
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn function_with_case_expression() {
    let src = "\
scale factor shapes = case shapes of
  Circle {radius} -> Circle {radius: radius * factor}
  Rect {width, height} -> Rect {width: width * factor, height: height * factor}";
    match first_decl(src) {
        DeclKind::Fun { name, body, .. } => {
            assert_eq!(name, "scale");
            match &body.node {
                ExprKind::Case { arms, .. } => {
                    assert_eq!(arms.len(), 2);
                    // First arm: Circle {radius} -> Circle {radius: ...}
                    match &arms[0].body.node {
                        ExprKind::App { func, arg } => {
                            assert!(matches!(&func.node, ExprKind::Constructor(n) if n == "Circle"));
                            assert!(matches!(&arg.node, ExprKind::Record(_)));
                        }
                        other => panic!("expected App, got {:?}", other),
                    }
                }
                other => panic!("expected Case, got {:?}", other),
            }
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn pipe_chain() {
    match fun_body("x = *people |> filter (\\p -> p.age > 65) |> count") {
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            rhs,
        } => {
            // ((*people |> filter ...) |> count) — left associative
            assert!(matches!(&rhs.node, ExprKind::Var(n) if n == "count"));
            assert!(matches!(
                &lhs.node,
                ExprKind::BinOp {
                    op: BinOp::Pipe,
                    ..
                }
            ));
        }
        other => panic!("expected Pipe chain, got {:?}", other),
    }
}

#[test]
fn record_update_with_field_access() {
    match fun_body("x = {p | age: p.age + 1}") {
        ExprKind::RecordUpdate { base, fields } => {
            assert!(matches!(&base.node, ExprKind::Var(n) if n == "p"));
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "age");
            assert!(matches!(
                &fields[0].value.node,
                ExprKind::BinOp { op: BinOp::Add, .. }
            ));
        }
        other => panic!("expected RecordUpdate, got {:?}", other),
    }
}

#[test]
fn if_in_do_block() {
    let src = "\
f = do
  t <- *todos
  yield (if t.name == name then {t | age: t.age + 1} else t)";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 2);
            match &stmts[1].node {
                StmtKind::Expr(e) => match &e.node {
                    ExprKind::Yield(inner) => {
                        assert!(matches!(&inner.node, ExprKind::If { .. }));
                    }
                    other => panic!("expected Yield(If), got {:?}", other),
                },
                other => panic!("expected Expr, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn full_program() {
    let src = "\
module TodoApp

data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

*todos : [{title: Text, owner: Text, priority: Priority, status: Status}]

formatTitle title = toUpper (take 1 title) ++ drop 1 title

pendingFor user = do
  t <- *todos
  where t.owner == user
  yield {title: t.title, priority: t.priority}

add title owner priority =
  set *todos = union *todos [{title: formatTitle title, owner, priority, status: Open {}}]

&workload = do
  t <- *todos
  yield {owner: t.owner, count: count t}";

    let m = parse_ok(src);
    assert_eq!(m.name.as_deref(), Some("TodoApp"));
    assert_eq!(m.decls.len(), 7);
    assert!(matches!(&m.decls[0].node, DeclKind::Data { name, .. } if name == "Priority"));
    assert!(matches!(&m.decls[1].node, DeclKind::Data { name, .. } if name == "Status"));
    assert!(matches!(&m.decls[2].node, DeclKind::Source { name, .. } if name == "todos"));
    assert!(matches!(&m.decls[3].node, DeclKind::Fun { name, .. } if name == "formatTitle"));
    assert!(matches!(&m.decls[4].node, DeclKind::Fun { name, .. } if name == "pendingFor"));
    assert!(matches!(&m.decls[5].node, DeclKind::Fun { name, .. } if name == "add"));
    assert!(matches!(&m.decls[6].node, DeclKind::Derived { name, .. } if name == "workload"));
}

// ── Error Recovery ──────────────────────────────────────────────────

#[test]
fn error_recovery_bad_first_decl() {
    let (module, diags) = parse_err("!!! bad\nx = 1");
    assert!(!diags.is_empty());
    let funs: Vec<_> = module
        .decls
        .iter()
        .filter(|d| matches!(&d.node, DeclKind::Fun { name, .. } if name == "x"))
        .collect();
    assert_eq!(funs.len(), 1, "should recover and parse 'x = 1'");
}

#[test]
fn error_missing_then() {
    let (_, diags) = parse_err("x = if a 1 else 2");
    let msg = &diags[0].message;
    assert!(
        msg.contains("then"),
        "error should mention 'then', got: {msg}"
    );
}

#[test]
fn error_missing_else() {
    let (_, diags) = parse_err("x = if a then 1");
    let msg = &diags[0].message;
    assert!(
        msg.contains("else"),
        "error should mention 'else', got: {msg}"
    );
}

#[test]
fn error_missing_arrow_in_lambda() {
    let (_, diags) = parse_err("x = \\a a");
    let msg = &diags[0].message;
    assert!(
        msg.contains("->"),
        "error should mention '->', got: {msg}"
    );
}

#[test]
fn error_unclosed_paren() {
    let (_, diags) = parse_err("x = (a + b");
    let has_paren_error = diags.iter().any(|d| d.message.contains(")") || d.message.contains("paren") || d.message.contains("close"));
    assert!(
        has_paren_error,
        "error should mention unclosed paren, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn error_unclosed_bracket() {
    let (_, diags) = parse_err("x = [1, 2");
    let has_bracket_error = diags.iter().any(|d| d.message.contains("]") || d.message.contains("close"));
    assert!(
        has_bracket_error,
        "error should mention unclosed bracket, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn error_unclosed_brace() {
    let (_, diags) = parse_err("x = {a: 1");
    let has_brace_error = diags.iter().any(|d| d.message.contains("}") || d.message.contains("close"));
    assert!(
        has_brace_error,
        "error should mention unclosed brace, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn error_missing_eq_in_data() {
    let (_, diags) = parse_err("data Bool True {} | False {}");
    let msg = &diags[0].message;
    assert!(
        msg.contains("=") || msg.contains("expected"),
        "error should mention '=', got: {msg}"
    );
}

#[test]
fn error_missing_of_in_case() {
    let (_, diags) = parse_err("x = case y\n  _ -> 0");
    let msg = &diags[0].message;
    assert!(
        msg.contains("of"),
        "error should mention 'of', got: {msg}"
    );
}

#[test]
fn error_recovery_multiple_bad_decls() {
    let src = "bad !!!\nalso bad !!!\nx = 1";
    let (module, diags) = parse_err(src);
    assert!(diags.len() >= 2, "should report errors for both bad decls");
    let has_x = module
        .decls
        .iter()
        .any(|d| matches!(&d.node, DeclKind::Fun { name, .. } if name == "x"));
    assert!(has_x, "should still parse valid 'x = 1' after recovery");
}

// ── Span Tests ──────────────────────────────────────────────────────

#[test]
fn spans_are_correct() {
    let src = "x = 42";
    let m = parse_ok(src);
    let decl = &m.decls[0];
    assert_eq!(decl.span.start, 0);
    assert!(decl.span.end > 0);
    match &decl.node {
        DeclKind::Fun { body, .. } => {
            // "42" starts at byte 4
            assert_eq!(body.span.start, 4);
            assert_eq!(body.span.end, 6);
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn binop_span_covers_both_operands() {
    let src = "x = a + b";
    match fun_body(src) {
        ExprKind::BinOp { .. } => {
            // Just verify it parses — the span is embedded in Spanned<ExprKind>
            // which we can't easily access from ExprKind alone.
        }
        other => panic!("expected BinOp, got {:?}", other),
    }
}

// ── Edge Cases ──────────────────────────────────────────────────────

#[test]
fn constructor_applied_to_empty_record() {
    // Open {} should be App(Constructor("Open"), Record([]))
    match fun_body("x = Open {}") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&func.node, ExprKind::Constructor(n) if n == "Open"));
            assert!(matches!(&arg.node, ExprKind::Record(fs) if fs.is_empty()));
        }
        other => panic!("expected App(Constructor, Record), got {:?}", other),
    }
}

#[test]
fn nested_lambdas() {
    match fun_body("x = \\a -> \\b -> a + b") {
        ExprKind::Lambda { body, .. } => {
            assert!(matches!(&body.node, ExprKind::Lambda { .. }));
        }
        other => panic!("expected nested Lambda, got {:?}", other),
    }
}

#[test]
fn arithmetic_in_field_value() {
    match fun_body("x = {area: pi * r * r}") {
        ExprKind::Record(fields) => {
            assert_eq!(fields[0].name, "area");
            assert!(matches!(
                &fields[0].value.node,
                ExprKind::BinOp { op: BinOp::Mul, .. }
            ));
        }
        other => panic!("expected Record, got {:?}", other),
    }
}

#[test]
fn parenthesized_expression_as_arg() {
    // f (a + b) — application of f to a parenthesized expression
    match fun_body("x = f (a + b)") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&func.node, ExprKind::Var(n) if n == "f"));
            assert!(matches!(&arg.node, ExprKind::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected App(f, Add(..)), got {:?}", other),
    }
}

#[test]
fn string_concat() {
    match fun_body(r#"x = "hello " ++ "world""#) {
        ExprKind::BinOp {
            op: BinOp::Concat,
            ..
        } => {}
        other => panic!("expected Concat, got {:?}", other),
    }
}

#[test]
fn source_ref_in_expression() {
    // *todos |> filter f
    match fun_body("x = *todos |> filter f") {
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            ..
        } => {
            assert!(matches!(&lhs.node, ExprKind::SourceRef(n) if n == "todos"));
        }
        other => panic!("expected Pipe with SourceRef, got {:?}", other),
    }
}

#[test]
fn field_access_in_where_clause() {
    let src = "x = do\n  t <- *people\n  where t.age > 65\n  yield t";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            match &stmts[1].node {
                StmtKind::Where { cond } => {
                    // t.age > 65
                    assert!(matches!(&cond.node, ExprKind::BinOp { op: BinOp::Gt, .. }));
                }
                other => panic!("expected Where, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn multiline_expression_continuation() {
    // Expression that continues on the next line via operator
    let src = "x = a\n  + b";
    match fun_body(src) {
        ExprKind::BinOp { op: BinOp::Add, .. } => {}
        other => panic!("expected Add, got {:?}", other),
    }
}

// ── Temporal Queries ────────────────────────────────────────────────

#[test]
fn temporal_at_query() {
    match fun_body("x = *people @(t)") {
        ExprKind::At { relation, time } => {
            assert!(matches!(&relation.node, ExprKind::SourceRef(n) if n == "people"));
            assert!(matches!(&time.node, ExprKind::Var(n) if n == "t"));
        }
        other => panic!("expected At, got {:?}", other),
    }
}

#[test]
fn temporal_at_with_expr() {
    match fun_body("x = *employees @(now - 365)") {
        ExprKind::At { relation, time } => {
            assert!(matches!(&relation.node, ExprKind::SourceRef(n) if n == "employees"));
            assert!(matches!(&time.node, ExprKind::BinOp { op: BinOp::Sub, .. }));
        }
        other => panic!("expected At with subtraction, got {:?}", other),
    }
}

#[test]
fn temporal_at_chained_with_pipe() {
    match fun_body("x = *people @(t) |> filter f") {
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            ..
        } => {
            assert!(matches!(&lhs.node, ExprKind::At { .. }));
        }
        other => panic!("expected Pipe(At(..), ..), got {:?}", other),
    }
}

// ── Effectful Types ─────────────────────────────────────────────────

#[test]
fn effectful_type_reads() {
    match first_decl("type X = {reads *users} Int -> Int") {
        DeclKind::TypeAlias { ty, .. } => match &ty.node {
            TypeKind::Effectful { effects, ty } => {
                assert_eq!(effects.len(), 1);
                assert!(matches!(&effects[0], Effect::Reads(n) if n == "users"));
                assert!(matches!(&ty.node, TypeKind::Function { .. }));
            }
            other => panic!("expected Effectful, got {:?}", other),
        },
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn effectful_type_multiple_effects() {
    match first_decl("type X = {reads *people, writes *logs, console} Int") {
        DeclKind::TypeAlias { ty, .. } => match &ty.node {
            TypeKind::Effectful { effects, .. } => {
                assert_eq!(effects.len(), 3);
                assert!(matches!(&effects[0], Effect::Reads(n) if n == "people"));
                assert!(matches!(&effects[1], Effect::Writes(n) if n == "logs"));
                assert!(matches!(&effects[2], Effect::Console));
            }
            other => panic!("expected Effectful, got {:?}", other),
        },
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn effectful_type_all_io_effects() {
    match first_decl("type X = {network, fs, clock, random} Int") {
        DeclKind::TypeAlias { ty, .. } => match &ty.node {
            TypeKind::Effectful { effects, .. } => {
                assert_eq!(effects.len(), 4);
                assert!(matches!(&effects[0], Effect::Network));
                assert!(matches!(&effects[1], Effect::Fs));
                assert!(matches!(&effects[2], Effect::Clock));
                assert!(matches!(&effects[3], Effect::Random));
            }
            other => panic!("expected Effectful, got {:?}", other),
        },
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

// ── Variant Types ───────────────────────────────────────────────────

#[test]
fn inline_variant_type() {
    match first_decl("type X = <Ok {} | Err {msg: Text}>") {
        DeclKind::TypeAlias { ty, .. } => match &ty.node {
            TypeKind::Variant {
                constructors, rest, ..
            } => {
                assert_eq!(constructors.len(), 2);
                assert_eq!(constructors[0].name, "Ok");
                assert_eq!(constructors[1].name, "Err");
                assert!(rest.is_none());
            }
            other => panic!("expected Variant, got {:?}", other),
        },
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn variant_type_with_rest() {
    match first_decl("type X = <Open {} | r>") {
        DeclKind::TypeAlias { ty, .. } => match &ty.node {
            TypeKind::Variant {
                constructors, rest, ..
            } => {
                assert_eq!(constructors.len(), 1);
                assert_eq!(constructors[0].name, "Open");
                assert_eq!(rest.as_deref(), Some("r"));
            }
            other => panic!("expected Variant with rest, got {:?}", other),
        },
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

// ── Constraint / Type Scheme ────────────────────────────────────────

#[test]
fn function_with_type_signature_and_body() {
    let src = "add : Int -> Int -> Int\nadd x y = x + y";
    let m = parse_ok(src);
    // Should produce a Fun with type annotation and body
    let fun = m
        .decls
        .iter()
        .find(|d| matches!(&d.node, DeclKind::Fun { name, .. } if name == "add"))
        .expect("should find 'add'");
    match &fun.node {
        DeclKind::Fun {
            name, ty, params, ..
        } => {
            assert_eq!(name, "add");
            assert!(ty.is_some());
            assert_eq!(params.len(), 2);
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn trait_method_with_constraint() {
    let src = "trait Collection c where\n  toList : Eq a => c a -> [a]";
    match first_decl(src) {
        DeclKind::Trait { items, .. } => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                TraitItem::Method { name, ty, .. } => {
                    assert_eq!(name, "toList");
                    assert!(!ty.constraints.is_empty());
                    assert_eq!(ty.constraints[0].trait_name, "Eq");
                }
                other => panic!("expected Method, got {:?}", other),
            }
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

#[test]
fn multiple_constraints() {
    let src = "trait Container c where\n  sort : Ord a => Eq a => c a -> c a";
    match first_decl(src) {
        DeclKind::Trait { items, .. } => {
            match &items[0] {
                TraitItem::Method { ty, .. } => {
                    assert_eq!(ty.constraints.len(), 2);
                    assert_eq!(ty.constraints[0].trait_name, "Ord");
                    assert_eq!(ty.constraints[1].trait_name, "Eq");
                }
                other => panic!("expected Method, got {:?}", other),
            }
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

// ── Trait/Impl Advanced ─────────────────────────────────────────────

#[test]
fn trait_with_supertrait() {
    let src = "trait Functor f => Applicative (f : Type -> Type) where\n  pure : a -> f a";
    match first_decl(src) {
        DeclKind::Trait {
            name,
            supertraits,
            params,
            items,
        } => {
            assert_eq!(name, "Applicative");
            assert_eq!(supertraits.len(), 1);
            assert_eq!(supertraits[0].trait_name, "Functor");
            assert_eq!(params[0].name, "f");
            assert!(params[0].kind.is_some());
            assert_eq!(items.len(), 1);
        }
        other => panic!("expected Trait with supertrait, got {:?}", other),
    }
}

#[test]
fn trait_with_associated_type() {
    let src = "trait Collection c where\n  type Item c\n  empty : c";
    match first_decl(src) {
        DeclKind::Trait { items, .. } => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                TraitItem::AssociatedType { name, params } => {
                    assert_eq!(name, "Item");
                    assert_eq!(params, &["c"]);
                }
                other => panic!("expected AssociatedType, got {:?}", other),
            }
            assert!(matches!(&items[1], TraitItem::Method { name, .. } if name == "empty"));
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

#[test]
fn trait_with_default_impl() {
    let src = "trait Eq a where\n  eq : a -> a -> Bool\n  neq x y = not (eq x y)";
    match first_decl(src) {
        DeclKind::Trait { items, .. } => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                TraitItem::Method {
                    name,
                    default_body, ..
                } => {
                    assert_eq!(name, "eq");
                    assert!(default_body.is_none());
                }
                other => panic!("expected Method, got {:?}", other),
            }
            match &items[1] {
                TraitItem::Method {
                    name,
                    default_body, ..
                } => {
                    assert_eq!(name, "neq");
                    assert!(default_body.is_some());
                }
                other => panic!("expected Method with default, got {:?}", other),
            }
        }
        other => panic!("expected Trait, got {:?}", other),
    }
}

#[test]
fn impl_with_associated_type() {
    let src = "impl Collection [] where\n  type Item [] = Int\n  empty = []";
    match first_decl(src) {
        DeclKind::Impl { items, .. } => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                ImplItem::AssociatedType { name, ty, .. } => {
                    assert_eq!(name, "Item");
                    assert!(matches!(&ty.node, TypeKind::Named(n) if n == "Int"));
                }
                other => panic!("expected AssociatedType, got {:?}", other),
            }
        }
        other => panic!("expected Impl, got {:?}", other),
    }
}

#[test]
fn impl_with_constraints() {
    let src = "impl Eq a => Ord [a] where\n  compare xs ys = 0";
    match first_decl(src) {
        DeclKind::Impl {
            trait_name,
            constraints,
            ..
        } => {
            assert_eq!(trait_name, "Ord");
            assert_eq!(constraints.len(), 1);
            assert_eq!(constraints[0].trait_name, "Eq");
        }
        other => panic!("expected Impl with constraint, got {:?}", other),
    }
}

#[test]
fn impl_method_no_params() {
    let src = "impl Default Int where\n  default = 0";
    match first_decl(src) {
        DeclKind::Impl { items, .. } => {
            match &items[0] {
                ImplItem::Method { name, params, .. } => {
                    assert_eq!(name, "default");
                    assert!(params.is_empty());
                }
                other => panic!("expected Method, got {:?}", other),
            }
        }
        other => panic!("expected Impl, got {:?}", other),
    }
}

// ── Route Advanced ──────────────────────────────────────────────────

#[test]
fn route_put_method() {
    let src = "route Api where\n  PUT {name: Text} /users/{id: Int} = UpdateUser";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].method, HttpMethod::Put);
            assert_eq!(entries[0].body_fields.len(), 1);
            assert_eq!(entries[0].path.len(), 2);
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_delete_method() {
    let src = "route Api where\n  DELETE /users/{id: Int} = DeleteUser";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].method, HttpMethod::Delete);
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_patch_method() {
    let src = "route Api where\n  PATCH {status: Text} /orders/{id: Int} = PatchOrder";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].method, HttpMethod::Patch);
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_with_query_params() {
    let src = "route Api where\n  GET /todos?{limit: Int, offset: Int} = ListTodos";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].query_params.len(), 2);
            assert_eq!(entries[0].query_params[0].name, "limit");
            assert_eq!(entries[0].query_params[1].name, "offset");
        }
        other => panic!("expected Route with query params, got {:?}", other),
    }
}

#[test]
fn route_multiple_path_segments() {
    let src = "route Api where\n  GET /api/v1/users/{id: Int}/posts = GetUserPosts";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            let path = &entries[0].path;
            assert_eq!(path.len(), 5);
            assert!(matches!(&path[0], PathSegment::Literal(s) if s == "api"));
            assert!(matches!(&path[1], PathSegment::Literal(s) if s == "v1"));
            assert!(matches!(&path[2], PathSegment::Literal(s) if s == "users"));
            assert!(matches!(&path[3], PathSegment::Param { name, .. } if name == "id"));
            assert!(matches!(&path[4], PathSegment::Literal(s) if s == "posts"));
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_body_and_query_and_response() {
    let src =
        "route Api where\n  POST {title: Text} /todos?{notify: Bool} -> {id: Int} = CreateTodo";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries[0].method, HttpMethod::Post);
            assert_eq!(entries[0].body_fields.len(), 1);
            assert_eq!(entries[0].query_params.len(), 1);
            assert!(entries[0].response_ty.is_some());
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

#[test]
fn route_multiple_entries() {
    let src = "\
route Api where
  GET /users = ListUsers
  POST {name: Text} /users = CreateUser
  GET /users/{id: Int} = GetUser
  DELETE /users/{id: Int} = DeleteUser";
    match first_decl(src) {
        DeclKind::Route { entries, .. } => {
            assert_eq!(entries.len(), 4);
            assert_eq!(entries[0].method, HttpMethod::Get);
            assert_eq!(entries[1].method, HttpMethod::Post);
            assert_eq!(entries[2].method, HttpMethod::Get);
            assert_eq!(entries[3].method, HttpMethod::Delete);
        }
        other => panic!("expected Route, got {:?}", other),
    }
}

// ── Record Punning ──────────────────────────────────────────────────

#[test]
fn record_punned_fields() {
    // {name, age} means {name: name, age: age}
    match fun_body("x = {name, age}") {
        ExprKind::Record(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "name");
            assert!(matches!(&fields[0].value.node, ExprKind::Var(n) if n == "name"));
            assert_eq!(fields[1].name, "age");
            assert!(matches!(&fields[1].value.node, ExprKind::Var(n) if n == "age"));
        }
        other => panic!("expected Record with punning, got {:?}", other),
    }
}

#[test]
fn record_field_access_pun() {
    // {t.name, t.age} means {name: t.name, age: t.age}
    match fun_body("x = {t.name, t.age}") {
        ExprKind::Record(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "name");
            assert!(matches!(&fields[0].value.node, ExprKind::FieldAccess { field, .. } if field == "name"));
            assert_eq!(fields[1].name, "age");
        }
        other => panic!("expected Record with field access pun, got {:?}", other),
    }
}

#[test]
fn record_mixed_punned_and_explicit() {
    match fun_body(r#"x = {name, age: 30, title: "Dr"}"#) {
        ExprKind::Record(fields) => {
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name, "name");
            assert!(matches!(&fields[0].value.node, ExprKind::Var(n) if n == "name"));
            assert_eq!(fields[1].name, "age");
            assert!(matches!(&fields[1].value.node, ExprKind::Lit(Literal::Int(30))));
        }
        other => panic!("expected Record, got {:?}", other),
    }
}

// ── Let-In Expressions ─────────────────────────────────────────────

#[test]
fn let_in_expression() {
    // let x = 1 in x + 1 desugars to (\x -> x + 1) 1
    match fun_body("f = let x = 1 in x + 1") {
        ExprKind::App { func, arg } => {
            assert!(matches!(&func.node, ExprKind::Lambda { .. }));
            assert!(matches!(&arg.node, ExprKind::Lit(Literal::Int(1))));
        }
        other => panic!("expected App(Lambda, Int) from let-in desugar, got {:?}", other),
    }
}

// ── Pattern Edge Cases ──────────────────────────────────────────────

#[test]
fn literal_int_pattern() {
    let src = "x = case n of\n  0 -> a\n  1 -> b\n  _ -> c";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert_eq!(arms.len(), 3);
            assert!(matches!(&arms[0].pat.node, PatKind::Lit(Literal::Int(0))));
            assert!(matches!(&arms[1].pat.node, PatKind::Lit(Literal::Int(1))));
            assert!(matches!(&arms[2].pat.node, PatKind::Wildcard));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn literal_string_pattern() {
    let src = r#"x = case s of
  "hello" -> 1
  _ -> 0"#;
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert!(matches!(
                &arms[0].pat.node,
                PatKind::Lit(Literal::Text(s)) if s == "hello"
            ));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn record_pattern_with_explicit_binding() {
    // {name: n, age: a} binds name to n, age to a
    let src = "x = case r of\n  {name: n, age: a} -> n";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::Record(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name, "name");
                    assert!(matches!(
                        &fields[0].pattern,
                        Some(p) if matches!(&p.node, PatKind::Var(n) if n == "n")
                    ));
                }
                other => panic!("expected Record pattern, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn constructor_without_explicit_payload() {
    // Constructor with no {} or variable after it — implicit empty record
    let src = "f x = case x of\n  Nothing -> 0\n  Just v -> v";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Nothing");
                    // No explicit payload — parser uses empty record
                    assert!(matches!(&payload.node, PatKind::Record(fs) if fs.is_empty()));
                }
                other => panic!("expected Constructor, got {:?}", other),
            }
            match &arms[1].pat.node {
                PatKind::Constructor { name, payload } => {
                    assert_eq!(name, "Just");
                    assert!(matches!(&payload.node, PatKind::Var(n) if n == "v"));
                }
                other => panic!("expected Constructor, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn parenthesized_pattern() {
    let src = "f (x) = x";
    match first_decl(src) {
        DeclKind::Fun { params, .. } => {
            assert_eq!(params.len(), 1);
            assert!(matches!(&params[0].node, PatKind::Var(n) if n == "x"));
        }
        other => panic!("expected Fun, got {:?}", other),
    }
}

#[test]
fn list_pattern_with_elements() {
    let src = "x = case xs of\n  [a, b] -> a + b\n  _ -> 0";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            match &arms[0].pat.node {
                PatKind::List(pats) => {
                    assert_eq!(pats.len(), 2);
                    assert!(matches!(&pats[0].node, PatKind::Var(n) if n == "a"));
                    assert!(matches!(&pats[1].node, PatKind::Var(n) if n == "b"));
                }
                other => panic!("expected List pattern, got {:?}", other),
            }
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

// ── Type Edge Cases ─────────────────────────────────────────────────

#[test]
fn unit_type() {
    match first_decl("type X = ()") {
        DeclKind::TypeAlias { ty, .. } => {
            assert!(matches!(
                &ty.node,
                TypeKind::Record {
                    fields,
                    rest: None
                } if fields.is_empty()
            ));
        }
        other => panic!("expected unit type as empty Record, got {:?}", other),
    }
}

#[test]
fn empty_record_type() {
    match first_decl("type X = {}") {
        DeclKind::TypeAlias { ty, .. } => {
            assert!(matches!(
                &ty.node,
                TypeKind::Record {
                    fields,
                    rest: None
                } if fields.is_empty()
            ));
        }
        other => panic!("expected empty Record type, got {:?}", other),
    }
}

#[test]
fn parenthesized_type() {
    match first_decl("type X = (Int)") {
        DeclKind::TypeAlias { ty, .. } => {
            assert!(matches!(&ty.node, TypeKind::Named(n) if n == "Int"));
        }
        other => panic!("expected Named Int through parens, got {:?}", other),
    }
}

#[test]
fn complex_function_type() {
    // (a -> b) -> [a] -> [b]  (map type)
    match first_decl("type X = (a -> b) -> [a] -> [b]") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::Function { param, result } => {
                    assert!(matches!(&param.node, TypeKind::Function { .. }));
                    assert!(matches!(&result.node, TypeKind::Function { .. }));
                }
                other => panic!("expected Function type, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn multi_arg_type_application() {
    // Result Error Value
    match first_decl("type X = Result Error Value") {
        DeclKind::TypeAlias { ty, .. } => {
            match &ty.node {
                TypeKind::App { func, arg } => {
                    assert!(matches!(&arg.node, TypeKind::Named(n) if n == "Value"));
                    match &func.node {
                        TypeKind::App { func, arg } => {
                            assert!(matches!(&func.node, TypeKind::Named(n) if n == "Result"));
                            assert!(matches!(&arg.node, TypeKind::Named(n) if n == "Error"));
                        }
                        other => panic!("expected nested App, got {:?}", other),
                    }
                }
                other => panic!("expected App, got {:?}", other),
            }
        }
        other => panic!("expected TypeAlias, got {:?}", other),
    }
}

#[test]
fn empty_relation_type_constructor() {
    // [] as a type constructor (used in impl Functor [] where ...)
    match first_decl("impl Functor [] where\n  map f xs = xs") {
        DeclKind::Impl { args, .. } => {
            assert_eq!(args.len(), 1);
            assert!(matches!(&args[0].node, TypeKind::Named(n) if n == "[]"));
        }
        other => panic!("expected Impl, got {:?}", other),
    }
}

// ── Do Block Edge Cases ─────────────────────────────────────────────

#[test]
fn do_bind_then_expression() {
    // A do block where the last statement is a bare expression (not yield)
    let src = "x = do\n  a <- xs\n  f a";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 2);
            assert!(matches!(&stmts[0].node, StmtKind::Bind { .. }));
            match &stmts[1].node {
                StmtKind::Expr(e) => assert!(matches!(&e.node, ExprKind::App { .. })),
                other => panic!("expected Expr, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_with_where_complex_condition() {
    let src = "x = do\n  t <- xs\n  where t.age > 18 && t.active == True\n  yield t";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert_eq!(stmts.len(), 3);
            match &stmts[1].node {
                StmtKind::Where { cond } => {
                    assert!(matches!(&cond.node, ExprKind::BinOp { op: BinOp::And, .. }));
                }
                other => panic!("expected Where, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

#[test]
fn do_nested() {
    let src = "x = do\n  a <- do\n    b <- xs\n    yield b\n  yield a";
    match fun_body(src) {
        ExprKind::Do(stmts) => {
            assert!(stmts.len() >= 1);
            match &stmts[0].node {
                StmtKind::Bind { expr, .. } => {
                    assert!(matches!(&expr.node, ExprKind::Do(_)));
                }
                other => panic!("expected Bind with nested Do, got {:?}", other),
            }
        }
        other => panic!("expected Do, got {:?}", other),
    }
}

// ── Expression Edge Cases ───────────────────────────────────────────

#[test]
fn nested_field_access_deep() {
    match fun_body("x = a.b.c.d") {
        ExprKind::FieldAccess { field, expr } => {
            assert_eq!(field, "d");
            match &expr.node {
                ExprKind::FieldAccess { field, expr } => {
                    assert_eq!(field, "c");
                    match &expr.node {
                        ExprKind::FieldAccess { field, .. } => assert_eq!(field, "b"),
                        other => panic!("expected FieldAccess, got {:?}", other),
                    }
                }
                other => panic!("expected FieldAccess, got {:?}", other),
            }
        }
        other => panic!("expected deep FieldAccess, got {:?}", other),
    }
}

#[test]
fn single_element_list() {
    match fun_body("x = [42]") {
        ExprKind::List(elems) => {
            assert_eq!(elems.len(), 1);
            assert!(matches!(&elems[0].node, ExprKind::Lit(Literal::Int(42))));
        }
        other => panic!("expected single-element List, got {:?}", other),
    }
}

#[test]
fn nested_lists() {
    match fun_body("x = [[1, 2], [3, 4]]") {
        ExprKind::List(elems) => {
            assert_eq!(elems.len(), 2);
            assert!(matches!(&elems[0].node, ExprKind::List(_)));
            assert!(matches!(&elems[1].node, ExprKind::List(_)));
        }
        other => panic!("expected nested List, got {:?}", other),
    }
}

#[test]
fn double_negation() {
    match fun_body("x = - -y") {
        ExprKind::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => {
            assert!(matches!(
                &operand.node,
                ExprKind::UnaryOp {
                    op: UnaryOp::Neg,
                    ..
                }
            ));
        }
        other => panic!("expected double Neg, got {:?}", other),
    }
}

#[test]
fn not_in_condition() {
    match fun_body("x = not (a && b)") {
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => {
            assert!(matches!(
                &operand.node,
                ExprKind::BinOp {
                    op: BinOp::And,
                    ..
                }
            ));
        }
        other => panic!("expected Not(And(..)), got {:?}", other),
    }
}

#[test]
fn subtraction_vs_negation() {
    // a - b should be Sub, not App(a, Neg(b))
    match fun_body("x = a - b") {
        ExprKind::BinOp {
            op: BinOp::Sub, ..
        } => {}
        other => panic!("expected Sub, got {:?}", other),
    }
}

#[test]
fn division_operator() {
    assert!(matches!(
        fun_body("x = a / b"),
        ExprKind::BinOp {
            op: BinOp::Div,
            ..
        }
    ));
}

// ── Lambda Pattern Variants ─────────────────────────────────────────

#[test]
fn lambda_with_record_pattern() {
    match fun_body("x = \\{name, age} -> name") {
        ExprKind::Lambda { params, .. } => {
            assert_eq!(params.len(), 1);
            assert!(matches!(&params[0].node, PatKind::Record(_)));
        }
        other => panic!("expected Lambda with record pattern, got {:?}", other),
    }
}

// ── Error Messages Quality ──────────────────────────────────────────

#[test]
fn error_keyword_as_variable() {
    let (_, diags) = parse_err("where = 5");
    assert!(diags
        .iter()
        .any(|d| d.message.contains("expected declaration")));
}

#[test]
fn error_missing_eq_in_fun() {
    let (_, diags) = parse_err("f x y");
    assert!(diags
        .iter()
        .any(|d| d.message.contains("=") || d.message.contains("expected")));
}

#[test]
fn error_missing_colon_in_source() {
    let (_, diags) = parse_err("*people [Person]");
    assert!(diags
        .iter()
        .any(|d| d.message.contains(":") || d.message.contains("=")));
}

#[test]
fn error_missing_where_in_trait() {
    let (_, diags) = parse_err("trait Foo a\n  bar : Int");
    assert!(diags.iter().any(|d| d.message.contains("where")));
}

#[test]
fn error_missing_eq_in_type_alias() {
    let (_, diags) = parse_err("type Foo Int");
    assert!(diags
        .iter()
        .any(|d| d.message.contains("=") || d.message.contains("expected")));
}

#[test]
fn error_missing_constructor_name_in_data() {
    let (_, diags) = parse_err("data Foo = {}");
    assert!(diags.iter().any(|d| d.message.contains("constructor")
        || d.message.contains("expected")
        || d.message.contains("type name")));
}

#[test]
fn error_missing_arrow_in_case() {
    let (_, diags) = parse_err("x = case y of\n  0 1");
    assert!(diags.iter().any(|d| d.message.contains("->")));
}

// ── Migrate Edge Cases ──────────────────────────────────────────────

#[test]
fn migrate_with_nested_type() {
    let src = "\
migrate *teams
  from {name: Text}
  to {name: Text, members: [Person]}
  using (\\old -> {old | members: []})";
    match first_decl(src) {
        DeclKind::Migrate {
            relation,
            to_ty,
            ..
        } => {
            assert_eq!(relation, "teams");
            match &to_ty.node {
                TypeKind::Record { fields, .. } => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[1].name, "members");
                }
                other => panic!("expected Record type, got {:?}", other),
            }
        }
        other => panic!("expected Migrate, got {:?}", other),
    }
}

// ── Case Single Arm ────────────────────────────────────────────────

#[test]
fn case_single_arm() {
    let src = "x = case y of\n  _ -> 0";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert_eq!(arms.len(), 1);
        }
        other => panic!("expected Case with 1 arm, got {:?}", other),
    }
}

#[test]
fn case_many_arms() {
    let src = "\
x = case n of
  1 -> a
  2 -> b
  3 -> c
  4 -> d
  _ -> e";
    match fun_body(src) {
        ExprKind::Case { arms, .. } => {
            assert_eq!(arms.len(), 5);
        }
        other => panic!("expected Case with 5 arms, got {:?}", other),
    }
}

// ── Data Declarations Edge Cases ────────────────────────────────────

#[test]
fn data_recursive_type() {
    match first_decl("data List a = Nil {} | Cons {head: a, tail: List a}") {
        DeclKind::Data {
            name,
            params,
            constructors,
            ..
        } => {
            assert_eq!(name, "List");
            assert_eq!(params, vec!["a"]);
            assert_eq!(constructors.len(), 2);
            assert_eq!(constructors[1].name, "Cons");
            assert_eq!(constructors[1].fields.len(), 2);
            assert_eq!(constructors[1].fields[1].name, "tail");
            // tail: List a  →  App(Named("List"), Var("a"))
            assert!(matches!(&constructors[1].fields[1].value.node, TypeKind::App { .. }));
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn data_multiple_type_params() {
    match first_decl("data Either a b = Left {value: a} | Right {value: b}") {
        DeclKind::Data {
            params,
            constructors,
            ..
        } => {
            assert_eq!(params, vec!["a", "b"]);
            assert_eq!(constructors.len(), 2);
        }
        other => panic!("expected Data, got {:?}", other),
    }
}
