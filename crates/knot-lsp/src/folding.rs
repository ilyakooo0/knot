//! `textDocument/foldingRange` handler.

use lsp_types::*;

use knot::ast::{self, ExprKind};
use crate::utils::top_fields;

use crate::state::ServerState;
use crate::utils::span_to_range;

// ── Folding Ranges ──────────────────────────────────────────────────

pub(crate) fn handle_folding_range(
    state: &ServerState,
    params: &FoldingRangeParams,
) -> Option<Vec<FoldingRange>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut ranges = Vec::new();

    for decl in top_fields(&doc.module) {
        let range = span_to_range(decl.value.span, &doc.source);
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
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                collect_folding_ranges_expr(body, &doc.source, &mut ranges);
            }
            ExprKind::SourceDecl { .. } | ExprKind::DataCtor { .. }
            | ExprKind::TypeCtor { .. } | ExprKind::RouteDecl { .. }
            | ExprKind::RouteCompositeDecl { .. } | ExprKind::SubsetConstraint { .. } => {}
            _ => {
                // A named function field.
                collect_folding_ranges_expr(&decl.value, &doc.source, &mut ranges);
            }
        }
    }

    // The `if/then/else` arm in `collect_folding_ranges_expr` explicitly folds
    // its `then`/`else` branches, and the recursion below also folds them when
    // a branch is a foldable container (do/case/lambda/atomic/record), so the
    // identical span can be pushed twice. Drop exact-duplicate ranges.
    let mut seen = std::collections::HashSet::new();
    ranges.retain(|r| {
        seen.insert((
            r.start_line,
            r.start_character,
            r.end_line,
            r.end_character,
        ))
    });

    Some(ranges)
}

fn collect_folding_ranges_expr(expr: &ast::Expr, source: &str, ranges: &mut Vec<FoldingRange>) {
    let range = span_to_range(expr.span, source);

    match &expr.node {
        ast::ExprKind::Do(_)
        | ast::ExprKind::Case { .. }
        | ast::ExprKind::Lambda { .. }
        | ast::ExprKind::Atomic { .. }
        | ast::ExprKind::Record(_)
        | ast::ExprKind::Serve { .. }
            if range.end.line > range.start.line => {
                ranges.push(FoldingRange {
                    start_line: range.start.line,
                    start_character: Some(range.start.character),
                    end_line: range.end.line,
                    end_character: Some(range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
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

    // Recurse into every sub-expression-bearing variant so a foldable
    // `do`/`case`/`lambda` nested under a container (an `atomic`, a record
    // field, a list element, `set x = do {...}`, a binop, an annotation, a
    // `serve` handler, …) still produces a folding range.
    match &expr.node {
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } => {
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
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_folding_ranges_expr(&f.value, source, ranges);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_folding_ranges_expr(base, source, ranges);
            for f in fields {
                collect_folding_ranges_expr(&f.value, source, ranges);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_folding_ranges_expr(expr, source, ranges);
        }
        ast::ExprKind::List(items) => {
            for item in items {
                collect_folding_ranges_expr(item, source, ranges);
            }
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_folding_ranges_expr(lhs, source, ranges);
            collect_folding_ranges_expr(rhs, source, ranges);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_folding_ranges_expr(operand, source, ranges);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            collect_folding_ranges_expr(target, source, ranges);
            collect_folding_ranges_expr(value, source, ranges);
        }
        ast::ExprKind::Atomic(inner) | ast::ExprKind::Refine(inner) => {
            collect_folding_ranges_expr(inner, source, ranges);
        }
        ast::ExprKind::TimeUnitLit { value, .. } => {
            collect_folding_ranges_expr(value, source, ranges);
        }
        ast::ExprKind::Annot { expr, .. } => {
            collect_folding_ranges_expr(expr, source, ranges);
        }
        ast::ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                collect_folding_ranges_expr(&h.body, source, ranges);
            }
        }
        _ => {}
    }
}
