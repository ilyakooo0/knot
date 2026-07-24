//! Standard prelude: ordinary polymorphic functions injected into every program.
//!
//! The user-facing trait/typeclass system has been removed; the prelude is now
//! a small set of plain functions. Builtin operator semantics (`+`, `<`, `++`,
//! unary `-`, `==`) are enforced intrinsically by the type checker and code
//! generator, and monadic do-blocks dispatch structurally, so no trait
//! declarations are needed here.
//!
//! A `.knot` file is a single expression, so the prelude is injected by
//! wrapping the program: `with { …prelude record… } <program>`. The program's
//! own bindings shadow the prelude's (a `with` body's later/literal bindings
//! win for the same name).

use knot::ast;

/// Byte offset added to every parsed prelude span so prelude spans can never
/// collide with user-file spans (bug B39). Chosen far above any plausible real
/// file size and above `desugar::SYNTH_SPAN_BASE` (1 << 31) so it also clears
/// the synthesized monad-span range.
pub(crate) const PRELUDE_SPAN_OFFSET: usize = 1 << 40;

/// Knot source for the standard prelude: a bare record literal whose fields
/// are the prelude's bindings.
const PRELUDE_SOURCE: &str = r#"
{
data Ordering = LT {} | EQ {} | GT {}

min : a -> a -> a
min (\a b -> if a < b then a else b)

max : a -> a -> a
max (\a b -> if a > b then a else b)

when : Bool -> IO {| e} {} -> IO {| e} {}
when (\cond action -> if cond then action else yield {})

unless : Bool -> IO {| e} {} -> IO {| e} {}
unless (\cond action -> if cond then yield {} else action)
}
"#;

/// Parse the prelude record and wrap the program's expression in
/// `with {prelude} expr`, so prelude names are in scope throughout.
pub fn inject_prelude(expr: &mut ast::Expr) {
    let lexer = knot::lexer::Lexer::new(PRELUDE_SOURCE);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        !lex_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to lex: {:?}",
        lex_diags
    );
    let parser = knot::parser::Parser::new(PRELUDE_SOURCE.to_string(), tokens);
    let (prelude_record, parse_diags) = parser.parse_file_expr();
    assert!(
        !parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to parse: {:?}",
        parse_diags
    );

    let mut record = prelude_record;
    shift_expr_spans(&mut record, PRELUDE_SPAN_OFFSET);

    let span = expr.span;
    let body = std::mem::replace(
        expr,
        ast::Spanned::new(ast::ExprKind::Record(Vec::new()), span),
    );
    *expr = ast::Spanned::new(
        ast::ExprKind::With {
            record: Box::new(record),
            body: Box::new(body),
        },
        span,
    );
}

// ── Prelude span shifting (bug B39) ──────────────────────────────────
//
// Add `offset` to every declaration/expression/statement/pattern span in a
// prelude decl so prelude spans can never alias user-file spans in
// `monad_info` (and the other span-keyed inference maps). Type spans are left
// alone — they never key `monad_info`. Mirrors the AST shape walked by
// `unused::walk_decl`; keep the two in sync when the AST grows a node.
//
// "Every span" includes the standalone `name_span`/`api_span` fields, not just
// the `Spanned` wrappers: inference keys a punned record field's binder on
// `FieldPat::name_span`, so leaving it unshifted leaks a raw PRELUDE_SOURCE
// offset into `local_type_info` — a span the LSP cannot tell apart from a
// user span, since its provenance filter can only compare byte ranges. It
// then anchors an inlay hint at that offset in the user's file.

fn shift_expr_spans(e: &mut ast::Expr, offset: usize) {
    use ast::ExprKind::*;
    e.span.start += offset;
    e.span.end += offset;
    match &mut e.node {
        App { func, arg } => {
            shift_expr_spans(func, offset);
            shift_expr_spans(arg, offset);
        }
        With { record, body } => {
            shift_expr_spans(record, offset);
            shift_expr_spans(body, offset);
        }
        Lambda { params, body, .. } => {
            for p in params {
                shift_pat_spans(p, offset);
            }
            shift_expr_spans(body, offset);
        }
        BinOp { lhs, rhs, .. } => {
            shift_expr_spans(lhs, offset);
            shift_expr_spans(rhs, offset);
        }
        UnaryOp { operand, .. } => shift_expr_spans(operand, offset),
        If { cond, then_branch, else_branch } => {
            shift_expr_spans(cond, offset);
            shift_expr_spans(then_branch, offset);
            shift_expr_spans(else_branch, offset);
        }
        Case { scrutinee, arms } => {
            shift_expr_spans(scrutinee, offset);
            for arm in arms {
                shift_pat_spans(&mut arm.pat, offset);
                shift_expr_spans(&mut arm.body, offset);
            }
        }
        Do(stmts) => {
            for s in stmts {
                shift_stmt_spans(s, offset);
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            shift_expr_spans(target, offset);
            shift_expr_spans(value, offset);
        }
        Atomic(inner) | Refine(inner) => shift_expr_spans(inner, offset),
        TimeUnitLit { value, .. } => shift_expr_spans(value, offset),
        Record(fields) => {
            for fl in fields {
                shift_expr_spans(&mut fl.value, offset);
            }
        }
        RecordUpdate { base, fields } => {
            shift_expr_spans(base, offset);
            for fl in fields {
                shift_expr_spans(&mut fl.value, offset);
            }
        }
        List(items) => {
            for it in items {
                shift_expr_spans(it, offset);
            }
        }
        FieldAccess { expr, .. } | Annot { expr, .. } => shift_expr_spans(expr, offset),
        Serve { api_span, handlers, .. } => {
            api_span.start += offset;
            api_span.end += offset;
            for h in handlers {
                h.endpoint_span.start += offset;
                h.endpoint_span.end += offset;
                shift_expr_spans(&mut h.body, offset);
            }
        }
        Lit(_) | Var(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) | ImplicitRef(_) => {}
        TypeCtor { .. } | DataCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } | RouteCompositeDecl { .. } => {}
        ViewDecl { body, .. } | DerivedDecl { body, .. } => shift_expr_spans(body, offset),
    }
}

fn shift_stmt_spans(s: &mut ast::Stmt, offset: usize) {
    use ast::StmtKind::*;
    s.span.start += offset;
    s.span.end += offset;
    match &mut s.node {
        Bind { pat, expr } => {
            shift_pat_spans(pat, offset);
            shift_expr_spans(expr, offset);
        }
        Where { cond } => shift_expr_spans(cond, offset),
        GroupBy { key } => shift_expr_spans(key, offset),
        Expr(e) => shift_expr_spans(e, offset),
    }
}

fn shift_pat_spans(p: &mut ast::Pat, offset: usize) {
    use ast::PatKind::*;
    p.span.start += offset;
    p.span.end += offset;
    match &mut p.node {
        Var(_) | Wildcard | Lit(_) => {}
        Constructor { payload, .. } => shift_pat_spans(payload, offset),
        Record(fields) => {
            for fp in fields {
                // The field-name token's own span. For a punned field
                // (`{value}`) this IS the binder's span, and inference records
                // it in `binding_types` — so it must be shifted like any other
                // binder span, or it escapes as a raw prelude offset.
                fp.name_span.start += offset;
                fp.name_span.end += offset;
                if let Some(inner) = &mut fp.pattern {
                    shift_pat_spans(inner, offset);
                }
            }
        }
        List(items) => {
            for it in items {
                shift_pat_spans(it, offset);
            }
        }
        Cons { head, tail } => {
            shift_pat_spans(head, offset);
            shift_pat_spans(tail, offset);
        }
        Annot { pat, .. } => shift_pat_spans(pat, offset),
    }
}


