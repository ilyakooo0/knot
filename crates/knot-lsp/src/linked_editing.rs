//! `textDocument/linkedEditingRange` handler. Links record-field-name
//! occurrences inside a single declaration so renaming one auto-renames the
//! others.

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};

use crate::state::ServerState;
use crate::utils::{
    find_word_in_source, position_to_offset, recurse_expr, span_to_range, word_at_position,
};

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
    let mut linked_spans: Vec<Span> = Vec::new();

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
                collect_field_name_spans(body, word, &doc.source, &mut linked_spans);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_field_name_spans(body, word, &doc.source, &mut linked_spans);
                    }
                }
            }
            _ => {}
        }
    }

    if linked_spans.len() <= 1 {
        return None;
    }

    // Only activate when the cursor is actually on one of the field-name
    // tokens. `word_at_position` matches the identifier under the cursor
    // regardless of role, so without this a local variable / function name
    // that merely shares a record field's spelling would get linked to that
    // unrelated field's occurrences.
    if !linked_spans
        .iter()
        .any(|s| offset >= s.start && offset <= s.end)
    {
        return None;
    }

    let linked_ranges = linked_spans
        .iter()
        .map(|s| span_to_range(*s, &doc.source))
        .collect();

    Some(LinkedEditingRanges {
        ranges: linked_ranges,
        word_pattern: None,
    })
}

fn collect_field_name_spans(
    expr: &ast::Expr,
    field_name: &str,
    source: &str,
    ranges: &mut Vec<Span>,
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
                        ranges.push(span);
                    }
                }
                search_start = f.value.span.end;
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            let mut search_start = base.span.end;
            for f in fields {
                if f.name == field_name {
                    if let Some(span) =
                        find_word_in_source(source, field_name, search_start, f.value.span.start)
                    {
                        ranges.push(span);
                    }
                }
                search_start = f.value.span.end;
            }
        }
        ast::ExprKind::FieldAccess {
            expr: inner, field, ..
        } => {
            // The field token is the suffix of the access expression. Guard
            // against underflow on malformed/stale spans, against the
            // computed start overlapping the receiver expression, and
            // against the slice not actually holding the field name —
            // mirroring `rename.rs::field_sites_in_expr`.
            if field == field_name && expr.span.end >= field.len() {
                let start = expr.span.end - field.len();
                if start >= inner.span.end
                    && source.get(start..expr.span.end) == Some(field.as_str())
                {
                    ranges.push(Span::new(start, expr.span.end));
                }
            }
        }
        _ => {}
    }
    // Recurse into all sub-expressions via the shared walker — the manual
    // recursion this replaces missed UnaryOp/Annot/UnitLit/Serve, leaving
    // field occurrences inside them unlinked.
    recurse_expr(expr, |e| collect_field_name_spans(e, field_name, source, ranges));
}
