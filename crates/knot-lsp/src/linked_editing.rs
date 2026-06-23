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

        // Collect all field name positions within this declaration. `pun_seen`
        // records whether `word` also appears as a *punned* record-pattern field
        // (`{name}` ≡ `{name: name}`), where the single token both names the
        // field and binds a variable used in the arm body. Renaming such a token
        // via linked editing would rename the field but silently leave the
        // variable's uses behind — so when a pun is present we suppress linked
        // editing entirely and defer to the proper rename, which handles puns.
        let mut pun_seen = false;
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                collect_field_name_spans(body, word, &doc.source, &mut linked_spans, &mut pun_seen);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_field_name_spans(
                            body,
                            word,
                            &doc.source,
                            &mut linked_spans,
                            &mut pun_seen,
                        );
                    }
                }
            }
            _ => {}
        }
        if pun_seen {
            return None;
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
    pun_seen: &mut bool,
) {
    // Field names also occur in record *patterns* that destructure values
    // (`case p of {name: n} -> …`, `\{name: n} -> …`, `{name: n} <- …`).
    // `recurse_expr` does not descend into patterns, so reach each binder node's
    // immediate patterns here; nested binders are reached as `recurse_expr`
    // walks into lambda/case/do sub-expressions.
    match &expr.node {
        ast::ExprKind::Lambda { params, .. } => {
            for p in params {
                collect_pat_field_spans(p, field_name, source, ranges, pun_seen);
            }
        }
        ast::ExprKind::Case { arms, .. } => {
            for arm in arms {
                collect_pat_field_spans(&arm.pat, field_name, source, ranges, pun_seen);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } => {
                        collect_pat_field_spans(pat, field_name, source, ranges, pun_seen);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
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
    recurse_expr(expr, |e| {
        collect_field_name_spans(e, field_name, source, ranges, pun_seen)
    });
}

/// Collect record-field-name token spans matching `field_name` inside a
/// pattern. Explicit fields (`{name: sub}`) yield a linkable span; a punned
/// field (`{name}`) that matches sets `pun_seen` instead of yielding a span,
/// because its token is also a variable binder and cannot be renamed in
/// isolation. Mirrors `inlay_hints::record_field_name_spans`' per-field window
/// scan so the right `name` token is located even when several fields share a
/// spelling or a sub-pattern repeats the name.
fn collect_pat_field_spans(
    pat: &ast::Pat,
    field_name: &str,
    source: &str,
    ranges: &mut Vec<Span>,
    pun_seen: &mut bool,
) {
    match &pat.node {
        ast::PatKind::Record(fields) => {
            let mut search_start = pat.span.start;
            for f in fields {
                match &f.pattern {
                    Some(sub) => {
                        if f.name == field_name {
                            if let Some(span) = find_word_in_source(
                                source,
                                field_name,
                                search_start,
                                sub.span.start,
                            ) {
                                ranges.push(span);
                            }
                        }
                        search_start = sub.span.end;
                    }
                    None => {
                        // Punned field: advance the window past its token
                        // regardless of match, but never emit a linkable span.
                        if let Some(span) =
                            find_word_in_source(source, &f.name, search_start, pat.span.end)
                        {
                            if f.name == field_name {
                                *pun_seen = true;
                            }
                            search_start = span.end;
                        }
                    }
                }
            }
            for f in fields {
                if let Some(sub) = &f.pattern {
                    collect_pat_field_spans(sub, field_name, source, ranges, pun_seen);
                }
            }
        }
        ast::PatKind::Constructor { payload, .. } => {
            collect_pat_field_spans(payload, field_name, source, ranges, pun_seen);
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_field_spans(p, field_name, source, ranges, pun_seen);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_field_spans(head, field_name, source, ranges, pun_seen);
            collect_pat_field_spans(tail, field_name, source, ranges, pun_seen);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;
    use lsp_types::{Position, TextDocumentIdentifier, TextDocumentPositionParams};

    fn params(uri: &Uri, pos: Position) -> LinkedEditingRangeParams {
        LinkedEditingRangeParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            work_done_progress_params: Default::default(),
        }
    }

    // The cursor offset of the Nth (0-based) occurrence of `needle`, placed one
    // byte in so it lands inside the token.
    fn nth_offset(src: &str, needle: &str, n: usize) -> usize {
        let mut from = 0;
        for _ in 0..n {
            from = src[from..].find(needle).unwrap() + from + needle.len();
        }
        src[from..].find(needle).unwrap() + from + 1
    }

    #[test]
    fn explicit_pattern_field_links_with_expression_field() {
        // `name` appears in an explicit (non-pun) record pattern AND in a record
        // expression within the same declaration. Both must be linked so editing
        // one mirrors to the other — previously only the expression side was
        // collected, leaving the pattern occurrence stale.
        let mut ws = TestWorkspace::new();
        let src = "extract = \\p ->\n  case p of\n    {name: n, age: a} -> {name: n, age: a}\n";
        let uri = ws.open("main", src);
        // Cursor on the expression-side `name` (2nd occurrence).
        let pos = offset_to_position(src, nth_offset(src, "name", 1));
        let resp = handle_linked_editing_range(&ws.state, &params(&uri, pos))
            .expect("linked editing should activate across pattern + expression");
        assert_eq!(
            resp.ranges.len(),
            2,
            "both the pattern field and the expression field must be linked"
        );
    }

    #[test]
    fn punned_pattern_field_suppresses_linked_editing() {
        // `{name}` is a pun: the token both names the field and binds the `name`
        // variable used in the body. Linking it would rename the field but leave
        // the variable's uses stale, so linked editing must NOT activate even
        // though the expression side has two `name` field occurrences.
        let mut ws = TestWorkspace::new();
        let src = "extract = \\p ->\n  case p of\n    {name} -> {name: name, other: name}\n";
        let uri = ws.open("main", src);
        // Cursor on the first expression-side `name:` field (2nd occurrence of
        // `name` overall; 1st is the pun in the pattern).
        let pos = offset_to_position(src, nth_offset(src, "name", 1));
        let resp = handle_linked_editing_range(&ws.state, &params(&uri, pos));
        assert!(
            resp.is_none(),
            "a punned pattern field must suppress linked editing (defer to rename)"
        );
    }

    #[test]
    fn expression_only_fields_still_link() {
        // No patterns involved — two record-expression occurrences of `name`
        // must still link, and no spurious pun suppression should fire.
        let mut ws = TestWorkspace::new();
        let src = "f = \\n -> {name: n, other: {name: n}}\n";
        let uri = ws.open("main", src);
        let pos = offset_to_position(src, nth_offset(src, "name", 0));
        let resp = handle_linked_editing_range(&ws.state, &params(&uri, pos))
            .expect("two expression field occurrences must link");
        assert_eq!(resp.ranges.len(), 2);
    }
}
