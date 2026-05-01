//! `textDocument/linkedEditingRange` handler. Links record-field-name
//! occurrences inside a single declaration so renaming one auto-renames the
//! others.

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};

use crate::state::ServerState;
use crate::utils::{find_word_in_source, position_to_offset, span_to_range, word_at_position};

// ── Linked Editing Range ────────────────────────────────────────────

pub(crate) fn handle_linked_editing_range(
    state: &ServerState,
    params: &LinkedEditingRangeParams,
) -> Option<LinkedEditingRanges> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Check if cursor is on a record field name (either in a record expression,
    // pattern, or type declaration) — link all occurrences of the same field
    // within the same declaration scope
    let mut linked_ranges = Vec::new();

    // Find the enclosing declaration
    for decl in &doc.module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }

        // Collect all field name positions within this declaration
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                collect_field_name_spans(body, word, &doc.source, &mut linked_ranges);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_field_name_spans(body, word, &doc.source, &mut linked_ranges);
                    }
                }
            }
            _ => {}
        }
    }

    if linked_ranges.len() <= 1 {
        return None;
    }

    Some(LinkedEditingRanges {
        ranges: linked_ranges,
        word_pattern: None,
    })
}

fn collect_field_name_spans(
    expr: &ast::Expr,
    field_name: &str,
    source: &str,
    ranges: &mut Vec<Range>,
) {
    match &expr.node {
        ast::ExprKind::Record(fields) => {
            // Search per-field in the slice between the previous field's value
            // and the current field's value — this is where the field name
            // sits in source text. Searching the whole record expression is
            // wrong: `find_word_in_source` returns the *first* match, so two
            // fields with the same name would yield duplicate spans, and a
            // value subexpression containing the same identifier would
            // hijack the location.
            let mut search_start = expr.span.start;
            for f in fields {
                if f.name == field_name {
                    if let Some(span) =
                        find_word_in_source(source, field_name, search_start, f.value.span.start)
                    {
                        ranges.push(span_to_range(span, source));
                    }
                }
                collect_field_name_spans(&f.value, field_name, source, ranges);
                search_start = f.value.span.end;
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_field_name_spans(base, field_name, source, ranges);
            let mut search_start = base.span.end;
            for f in fields {
                if f.name == field_name {
                    if let Some(span) =
                        find_word_in_source(source, field_name, search_start, f.value.span.start)
                    {
                        ranges.push(span_to_range(span, source));
                    }
                }
                collect_field_name_spans(&f.value, field_name, source, ranges);
                search_start = f.value.span.end;
            }
        }
        ast::ExprKind::FieldAccess {
            expr: inner, field, ..
        } => {
            if field == field_name {
                let field_start = expr.span.end - field.len();
                ranges.push(span_to_range(
                    Span::new(field_start, expr.span.end),
                    source,
                ));
            }
            collect_field_name_spans(inner, field_name, source, ranges);
        }
        ast::ExprKind::App { func, arg } => {
            collect_field_name_spans(func, field_name, source, ranges);
            collect_field_name_spans(arg, field_name, source, ranges);
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_field_name_spans(body, field_name, source, ranges);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_field_name_spans(lhs, field_name, source, ranges);
            collect_field_name_spans(rhs, field_name, source, ranges);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_field_name_spans(cond, field_name, source, ranges);
            collect_field_name_spans(then_branch, field_name, source, ranges);
            collect_field_name_spans(else_branch, field_name, source, ranges);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_field_name_spans(scrutinee, field_name, source, ranges);
            for arm in arms {
                collect_field_name_spans(&arm.body, field_name, source, ranges);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_field_name_spans(expr, field_name, source, ranges);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_field_name_spans(e, field_name, source, ranges);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_field_name_spans(key, field_name, source, ranges);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => collect_field_name_spans(e, field_name, source, ranges),
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_field_name_spans(target, field_name, source, ranges);
            collect_field_name_spans(value, field_name, source, ranges);
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_field_name_spans(e, field_name, source, ranges);
            }
        }
        _ => {}
    }
}
