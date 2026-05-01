//! `textDocument/selectionRange` handler.

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span};

use crate::state::ServerState;
use crate::utils::{offset_to_position, position_to_offset, span_to_range};

// ── Selection Range ─────────────────────────────────────────────────

pub(crate) fn handle_selection_range(
    state: &ServerState,
    params: &SelectionRangeParams,
) -> Option<Vec<SelectionRange>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut results = Vec::new();

    for pos in &params.positions {
        let offset = position_to_offset(&doc.source, *pos);
        let selection = build_selection_range(&doc.module, &doc.source, offset);
        results.push(selection);
    }

    Some(results)
}

fn build_selection_range(module: &Module, source: &str, offset: usize) -> SelectionRange {
    // Collect all AST spans that contain the offset, from largest to smallest
    let mut spans = Vec::new();

    for decl in &module.decls {
        if decl.span.start <= offset && offset < decl.span.end {
            spans.push(decl.span);
            match &decl.node {
                DeclKind::Fun { body: Some(body), .. }
                | DeclKind::View { body, .. }
                | DeclKind::Derived { body, .. } => {
                    collect_containing_spans(body, offset, &mut spans);
                }
                DeclKind::Fun { body: None, .. } => {}
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { body, .. } = item {
                            collect_containing_spans(body, offset, &mut spans);
                        }
                    }
                }
                DeclKind::Trait { items, .. } => {
                    for item in items {
                        if let ast::TraitItem::Method {
                            default_body: Some(body),
                            ..
                        } = item
                        {
                            collect_containing_spans(body, offset, &mut spans);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Sort by size (largest first) and deduplicate
    spans.sort_by(|a, b| {
        let a_size = a.end - a.start;
        let b_size = b.end - b.start;
        b_size.cmp(&a_size)
    });
    spans.dedup();

    // Build linked list from largest to smallest
    let mut selection = SelectionRange {
        range: Range {
            start: Position::new(0, 0),
            end: offset_to_position(source, source.len()),
        },
        parent: None,
    };

    for span in &spans {
        selection = SelectionRange {
            range: span_to_range(*span, source),
            parent: Some(Box::new(selection)),
        };
    }

    selection
}

fn collect_containing_spans(expr: &ast::Expr, offset: usize, spans: &mut Vec<Span>) {
    if expr.span.start > offset || offset >= expr.span.end {
        return;
    }
    spans.push(expr.span);

    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            collect_containing_spans(func, offset, spans);
            collect_containing_spans(arg, offset, spans);
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_containing_spans(body, offset, spans);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_containing_spans(lhs, offset, spans);
            collect_containing_spans(rhs, offset, spans);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_containing_spans(operand, offset, spans);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_containing_spans(cond, offset, spans);
            collect_containing_spans(then_branch, offset, spans);
            collect_containing_spans(else_branch, offset, spans);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_containing_spans(scrutinee, offset, spans);
            for arm in arms {
                collect_containing_spans(&arm.body, offset, spans);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_containing_spans(expr, offset, spans);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_containing_spans(e, offset, spans);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_containing_spans(key, offset, spans);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            collect_containing_spans(e, offset, spans);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_containing_spans(target, offset, spans);
            collect_containing_spans(value, offset, spans);
        }
        ast::ExprKind::At { relation, time } => {
            collect_containing_spans(relation, offset, spans);
            collect_containing_spans(time, offset, spans);
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_containing_spans(expr, offset, spans);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_containing_spans(&f.value, offset, spans);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_containing_spans(base, offset, spans);
            for f in fields {
                collect_containing_spans(&f.value, offset, spans);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_containing_spans(e, offset, spans);
            }
        }
        _ => {}
    }
}
