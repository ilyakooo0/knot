//! `textDocument/foldingRange` handler.

use lsp_types::*;

use knot::ast::{self, DeclKind};

use crate::state::ServerState;
use crate::utils::span_to_range;

// ── Folding Ranges ──────────────────────────────────────────────────

pub(crate) fn handle_folding_range(
    state: &ServerState,
    params: &FoldingRangeParams,
) -> Option<Vec<FoldingRange>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut ranges = Vec::new();

    for decl in &doc.module.decls {
        let range = span_to_range(decl.span, &doc.source);
        if range.end.line > range.start.line {
            ranges.push(FoldingRange {
                start_line: range.start.line,
                start_character: Some(range.start.character),
                end_line: range.end.line,
                end_character: Some(range.end.character),
                kind: Some(FoldingRangeKind::Region),
                ..Default::default()
            });
        }

        // Fold sub-expressions within declarations
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                collect_folding_ranges_expr(body, &doc.source, &mut ranges);
            }
            DeclKind::Fun { body: None, .. } => {}
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_folding_ranges_expr(body, &doc.source, &mut ranges);
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
                        collect_folding_ranges_expr(body, &doc.source, &mut ranges);
                    }
                }
            }
            _ => {}
        }
    }

    // Fold imports if there are multiple
    if doc.module.imports.len() > 1 {
        let first = &doc.module.imports[0];
        let last = &doc.module.imports[doc.module.imports.len() - 1];
        let start = span_to_range(first.span, &doc.source);
        let end = span_to_range(last.span, &doc.source);
        if end.end.line > start.start.line {
            ranges.push(FoldingRange {
                start_line: start.start.line,
                start_character: None,
                end_line: end.end.line,
                end_character: None,
                kind: Some(FoldingRangeKind::Imports),
                ..Default::default()
            });
        }
    }

    Some(ranges)
}

fn collect_folding_ranges_expr(expr: &ast::Expr, source: &str, ranges: &mut Vec<FoldingRange>) {
    let range = span_to_range(expr.span, source);

    match &expr.node {
        ast::ExprKind::Do(_) | ast::ExprKind::Case { .. } => {
            if range.end.line > range.start.line {
                ranges.push(FoldingRange {
                    start_line: range.start.line,
                    start_character: Some(range.start.character),
                    end_line: range.end.line,
                    end_character: Some(range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
        }
        ast::ExprKind::If {
            then_branch,
            else_branch,
            ..
        } => {
            let then_range = span_to_range(then_branch.span, source);
            if then_range.end.line > then_range.start.line {
                ranges.push(FoldingRange {
                    start_line: then_range.start.line,
                    start_character: Some(then_range.start.character),
                    end_line: then_range.end.line,
                    end_character: Some(then_range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
            let else_range = span_to_range(else_branch.span, source);
            if else_range.end.line > else_range.start.line {
                ranges.push(FoldingRange {
                    start_line: else_range.start.line,
                    start_character: Some(else_range.start.character),
                    end_line: else_range.end.line,
                    end_character: Some(else_range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
        }
        _ => {}
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_folding_ranges_expr(expr, source, ranges);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_folding_ranges_expr(e, source, ranges);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_folding_ranges_expr(key, source, ranges);
                    }
                }
            }
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_folding_ranges_expr(scrutinee, source, ranges);
            for arm in arms {
                collect_folding_ranges_expr(&arm.body, source, ranges);
            }
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_folding_ranges_expr(body, source, ranges);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_folding_ranges_expr(cond, source, ranges);
            collect_folding_ranges_expr(then_branch, source, ranges);
            collect_folding_ranges_expr(else_branch, source, ranges);
        }
        ast::ExprKind::App { func, arg } => {
            collect_folding_ranges_expr(func, source, ranges);
            collect_folding_ranges_expr(arg, source, ranges);
        }
        _ => {}
    }
}
