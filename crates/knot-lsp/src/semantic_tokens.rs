//! `textDocument/semanticTokens/full` handler. Walks the AST to label
//! identifiers, literals, and operator tokens with their semantic-tokens types.

use std::collections::HashSet;

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};

use crate::builtins::EFFECTFUL_BUILTINS;
use crate::legend::{
    MOD_DECLARATION, MOD_EFFECTFUL, MOD_MUTATION, MOD_READONLY, TOK_ENUM_MEMBER, TOK_FUNCTION,
    TOK_NAMESPACE, TOK_NUMBER, TOK_PARAMETER, TOK_PROPERTY, TOK_STRING, TOK_STRUCT, TOK_TYPE,
    TOK_VARIABLE,
};
use crate::state::ServerState;
use crate::utils::{find_word_in_source, position_to_offset};

// ── Semantic Tokens ─────────────────────────────────────────────────

pub(crate) fn handle_semantic_tokens_full(
    state: &mut ServerState,
    params: &SemanticTokensParams,
) -> Option<SemanticTokensResult> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let raw_tokens = collect_tokens(doc, None);
    let encoded = delta_encode_tokens(&raw_tokens, &doc.source);

    let result_id = next_result_id(state);
    state
        .semantic_token_cache
        .insert(params.text_document.uri.clone(), (result_id.clone(), encoded.clone()));
    crate::state::enforce_uri_cache_cap(
        &mut state.semantic_token_cache,
        &state.documents,
        crate::state::MAX_SEMANTIC_TOKEN_CACHE,
    );

    Some(SemanticTokensResult::Tokens(SemanticTokens {
        result_id: Some(result_id),
        data: encoded,
    }))
}

/// `textDocument/semanticTokens/full/delta` — given the result_id of a
/// previously-emitted token list, return the patch needed to bring the
/// editor's copy in sync with the latest tokens. Falls back to a full
/// response when the cached entry is missing or its result_id doesn't match.
pub(crate) fn handle_semantic_tokens_full_delta(
    state: &mut ServerState,
    params: &SemanticTokensDeltaParams,
) -> Option<SemanticTokensFullDeltaResult> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let raw_tokens = collect_tokens(doc, None);
    let new_tokens = delta_encode_tokens(&raw_tokens, &doc.source);

    let cached = state
        .semantic_token_cache
        .get(uri)
        .filter(|(rid, _)| *rid == params.previous_result_id)
        .cloned();

    let result_id = next_result_id(state);

    match cached {
        Some((_, prev)) => {
            // Compute a single replace-edit covering the changed middle
            // section. We work on the SemanticToken array directly — its
            // u32 fields make equality comparisons cheap and the wire
            // format mirrors them 1:1.
            let edits = diff_token_lists(&prev, &new_tokens);
            state
                .semantic_token_cache
                .insert(uri.clone(), (result_id.clone(), new_tokens));
            crate::state::enforce_uri_cache_cap(
                &mut state.semantic_token_cache,
                &state.documents,
                crate::state::MAX_SEMANTIC_TOKEN_CACHE,
            );
            Some(SemanticTokensFullDeltaResult::TokensDelta(SemanticTokensDelta {
                result_id: Some(result_id),
                edits,
            }))
        }
        None => {
            state
                .semantic_token_cache
                .insert(uri.clone(), (result_id.clone(), new_tokens.clone()));
            crate::state::enforce_uri_cache_cap(
                &mut state.semantic_token_cache,
                &state.documents,
                crate::state::MAX_SEMANTIC_TOKEN_CACHE,
            );
            Some(SemanticTokensFullDeltaResult::Tokens(SemanticTokens {
                result_id: Some(result_id),
                data: new_tokens,
            }))
        }
    }
}

fn next_result_id(state: &mut ServerState) -> String {
    state.semantic_token_counter = state.semantic_token_counter.wrapping_add(1);
    format!("v{}", state.semantic_token_counter)
}

/// Compute a minimal patch turning `prev` into `new` as a list of
/// `SemanticTokensEdit`s. The LSP wire format describes each edit as a
/// (start, deleteCount, data) triple over the raw `Vec<SemanticToken>`. We
/// strip a common prefix and a common suffix and emit a single replace edit
/// covering the divergent middle — sufficient for typical edits while
/// staying within the spec.
fn diff_token_lists(
    prev: &[SemanticToken],
    new: &[SemanticToken],
) -> Vec<SemanticTokensEdit> {
    if prev == new {
        return Vec::new();
    }
    let mut start = 0usize;
    while start < prev.len()
        && start < new.len()
        && tokens_equal(&prev[start], &new[start])
    {
        start += 1;
    }
    let mut prev_end = prev.len();
    let mut new_end = new.len();
    while prev_end > start
        && new_end > start
        && tokens_equal(&prev[prev_end - 1], &new[new_end - 1])
    {
        prev_end -= 1;
        new_end -= 1;
    }
    let delete_count = (prev_end - start) as u32;
    let replacement: Vec<SemanticToken> = new[start..new_end].to_vec();
    if delete_count == 0 && replacement.is_empty() {
        return Vec::new();
    }
    // Per the spec, each edit's `start` is in u5 token-tuple offset.
    // SemanticToken fields are 5 u32s, but the LSP `start` field is the
    // *flat token array* index expressed as a count of u32s — so the index
    // we computed (token-tuple count) needs multiplying by 5.
    vec![SemanticTokensEdit {
        start: (start as u32) * 5,
        delete_count: delete_count * 5,
        data: Some(replacement),
    }]
}

fn tokens_equal(a: &SemanticToken, b: &SemanticToken) -> bool {
    a.delta_line == b.delta_line
        && a.delta_start == b.delta_start
        && a.length == b.length
        && a.token_type == b.token_type
        && a.token_modifiers_bitset == b.token_modifiers_bitset
}

/// `textDocument/semanticTokens/range` — emit tokens only for the visible
/// viewport. Useful for very large files where a full-document walk would
/// stall the editor. The byte-range filter is applied on raw tokens before
/// delta-encoding so column deltas restart cleanly inside the range.
pub(crate) fn handle_semantic_tokens_range(
    state: &ServerState,
    params: &SemanticTokensRangeParams,
) -> Option<SemanticTokensRangeResult> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let start = position_to_offset(&doc.source, params.range.start);
    let end = position_to_offset(&doc.source, params.range.end);
    let raw_tokens = collect_tokens(doc, Some((start, end)));
    let encoded = delta_encode_tokens(&raw_tokens, &doc.source);
    Some(SemanticTokensRangeResult::Tokens(SemanticTokens {
        result_id: None,
        data: encoded,
    }))
}

/// Walk the document and collect raw tokens, optionally filtered to a byte
/// range. Pulled out so `full` and `range` share token production verbatim
/// — the only difference is which subset of tokens we delta-encode.
fn collect_tokens(
    doc: &crate::state::DocumentState,
    range: Option<(usize, usize)>,
) -> Vec<RawToken> {
    let mut raw_tokens = Vec::new();
    let mut collector = TokenCollector {
        tokens: &mut raw_tokens,
        source: &doc.source,
    };

    for decl in &doc.module.decls {
        // Coarse pre-filter: skip decls whose span doesn't overlap the
        // requested range. Fine-grained per-token filtering still happens
        // below, but this avoids walking unrelated subtrees.
        if let Some((rs, re)) = range {
            if decl.span.end < rs || decl.span.start > re {
                continue;
            }
        }
        collector.visit_decl(decl);
    }

    // Add keyword and operator tokens from lexer
    let ast_token_starts: HashSet<usize> = raw_tokens.iter().map(|t| t.start).collect();
    for (span, tok_type) in &doc.keyword_tokens {
        if !ast_token_starts.contains(&span.start) {
            raw_tokens.push(RawToken {
                start: span.start,
                length: span.end - span.start,
                token_type: *tok_type,
                modifiers: 0,
            });
        }
    }

    if let Some((rs, re)) = range {
        raw_tokens.retain(|t| t.start + t.length > rs && t.start < re);
    }

    raw_tokens.sort_by_key(|t| (t.start, t.length));
    raw_tokens
}

pub(crate) struct RawToken {
    pub start: usize,
    pub length: usize,
    pub token_type: u32,
    pub modifiers: u32,
}

struct TokenCollector<'a> {
    tokens: &'a mut Vec<RawToken>,
    source: &'a str,
}

impl<'a> TokenCollector<'a> {
    fn add(&mut self, span: Span, token_type: u32, modifiers: u32) {
        if span.start < span.end && span.end <= self.source.len() {
            let text = &self.source[span.start..span.end];
            if !text.contains('\n') {
                self.tokens.push(RawToken {
                    start: span.start,
                    length: span.end - span.start,
                    token_type,
                    modifiers,
                });
            } else {
                // Split multi-line tokens into per-line tokens
                let mut offset = span.start;
                for line in text.split('\n') {
                    if !line.is_empty() {
                        self.tokens.push(RawToken {
                            start: offset,
                            length: line.len(),
                            token_type,
                            modifiers,
                        });
                    }
                    offset += line.len() + 1; // +1 for the '\n'
                }
            }
        }
    }

    fn visit_decl(&mut self, decl: &ast::Decl) {
        match &decl.node {
            DeclKind::Fun { name, body, ty, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.start + name.len() + 20) {
                    self.add(s, TOK_FUNCTION, MOD_DECLARATION);
                }
                if let Some(scheme) = ty {
                    self.visit_type(&scheme.ty);
                }
                if let Some(body) = body {
                    self.visit_expr(body);
                }
            }
            DeclKind::Data {
                name, constructors, ..
            } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_STRUCT, MOD_DECLARATION);
                }
                for ctor in constructors {
                    if let Some(s) = find_word_in_source(self.source, &ctor.name, decl.span.start, decl.span.end) {
                        self.add(s, TOK_ENUM_MEMBER, MOD_DECLARATION);
                    }
                    for f in &ctor.fields {
                        self.visit_type(&f.value);
                    }
                }
            }
            DeclKind::TypeAlias { ty, .. } => {
                // Refined-type aliases (`type Nat = Int where \x -> x >= 0`)
                // contain expression-bodied predicates; walk the type so the
                // predicate's tokens get highlighted alongside the rest of
                // the file.
                self.visit_type(ty);
            }
            DeclKind::Source { name, ty, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
                self.visit_type(ty);
            }
            DeclKind::View { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
                self.visit_expr(body);
            }
            DeclKind::Derived { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION | MOD_READONLY);
                }
                self.visit_expr(body);
            }
            DeclKind::Trait { name, items, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        default_params,
                        default_body: Some(body),
                        ..
                    } = item
                    {
                        for p in default_params {
                            self.visit_pat(p, true);
                        }
                        self.visit_expr(body);
                    }
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { params, body, .. } = item {
                        for p in params {
                            self.visit_pat(p, true);
                        }
                        self.visit_expr(body);
                    }
                }
            }
            DeclKind::Migrate { using_fn, .. } => {
                self.visit_expr(using_fn);
            }
            DeclKind::UnitDecl { name, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
            }
            _ => {}
        }
    }

    fn visit_expr(&mut self, expr: &ast::Expr) {
        match &expr.node {
            ast::ExprKind::Var(name) => {
                let modifier = if EFFECTFUL_BUILTINS.contains(&name.as_str()) {
                    MOD_EFFECTFUL
                } else {
                    0
                };
                self.add(expr.span, TOK_VARIABLE, modifier);
            }
            ast::ExprKind::Constructor(_) => {
                self.add(expr.span, TOK_ENUM_MEMBER, 0);
            }
            ast::ExprKind::SourceRef(_) => {
                self.add(expr.span, TOK_NAMESPACE, 0);
            }
            ast::ExprKind::DerivedRef(_) => {
                self.add(expr.span, TOK_NAMESPACE, MOD_READONLY);
            }
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                self.visit_expr(inner);
                // Field name span: the part after the `.`
                let field_start = expr.span.end - field.len();
                if field_start < expr.span.end {
                    self.add(Span::new(field_start, expr.span.end), TOK_PROPERTY, 0);
                }
            }
            ast::ExprKind::Lit(ast::Literal::Int(_) | ast::Literal::Float(_)) => {
                self.add(expr.span, TOK_NUMBER, 0);
            }
            ast::ExprKind::Lit(ast::Literal::Text(_)) => {
                self.add(expr.span, TOK_STRING, 0);
            }
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    self.visit_pat(p, true);
                }
                self.visit_expr(body);
            }
            ast::ExprKind::App { func, arg } => {
                self.visit_expr(func);
                self.visit_expr(arg);
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.visit_expr(lhs);
                self.visit_expr(rhs);
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.visit_expr(operand),
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                self.visit_expr(cond);
                self.visit_expr(then_branch);
                self.visit_expr(else_branch);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                self.visit_expr(scrutinee);
                for arm in arms {
                    self.visit_pat(&arm.pat, false);
                    self.visit_expr(&arm.body);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
                            self.visit_expr(expr);
                            self.visit_pat(pat, false);
                        }
                        ast::StmtKind::Let { pat, expr } => {
                            self.visit_expr(expr);
                            self.visit_pat(pat, false);
                        }
                        ast::StmtKind::Where { cond } => self.visit_expr(cond),
                        ast::StmtKind::GroupBy { key } => self.visit_expr(key),
                        ast::StmtKind::Expr(e) => self.visit_expr(e),
                    }
                }
            }
            ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => self.visit_expr(e),
            ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
                // Highlight mutation targets distinctly. We re-emit the target
                // span with a MUTATION modifier overlaying whatever inner type
                // visit_expr would assign.
                if let ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) = &target.node {
                    self.add(target.span, TOK_NAMESPACE, MOD_MUTATION);
                } else {
                    self.visit_expr(target);
                }
                self.visit_expr(value);
            }
            ast::ExprKind::At { relation, time } => {
                self.visit_expr(relation);
                self.visit_expr(time);
            }
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    self.visit_expr(&f.value);
                }
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                self.visit_expr(base);
                for f in fields {
                    self.visit_expr(&f.value);
                }
            }
            ast::ExprKind::List(elems) => {
                for e in elems {
                    self.visit_expr(e);
                }
            }
            ast::ExprKind::UnitLit { value, .. } => {
                self.visit_expr(value);
            }
            ast::ExprKind::Annot { expr: inner, .. } => {
                self.visit_expr(inner);
            }
            _ => {}
        }
    }

    /// Walk a type expression and emit semantic tokens for refined-type
    /// predicate bodies. Refined predicates are arbitrary `Expr`s, so the
    /// existing expression visitor handles them; this method just locates
    /// and recurses through the type AST to find each `Refined` node.
    fn visit_type(&mut self, ty: &ast::Type) {
        match &ty.node {
            ast::TypeKind::Refined { base, predicate } => {
                self.visit_type(base);
                self.visit_expr(predicate);
            }
            ast::TypeKind::Record { fields, .. } => {
                for f in fields {
                    self.visit_type(&f.value);
                }
            }
            ast::TypeKind::Variant { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        self.visit_type(&f.value);
                    }
                }
            }
            ast::TypeKind::Relation(inner) | ast::TypeKind::Forall { ty: inner, .. } => {
                self.visit_type(inner);
            }
            ast::TypeKind::App { func, arg } => {
                self.visit_type(func);
                self.visit_type(arg);
            }
            ast::TypeKind::Function { param, result } => {
                self.visit_type(param);
                self.visit_type(result);
            }
            ast::TypeKind::IO { ty: inner, .. } | ast::TypeKind::Effectful { ty: inner, .. } => {
                self.visit_type(inner);
            }
            ast::TypeKind::UnitAnnotated { base, .. } => {
                self.visit_type(base);
            }
            _ => {}
        }
    }

    fn visit_pat(&mut self, pat: &ast::Pat, is_param: bool) {
        match &pat.node {
            ast::PatKind::Var(_) => {
                let tok = if is_param { TOK_PARAMETER } else { TOK_VARIABLE };
                self.add(pat.span, tok, MOD_DECLARATION);
            }
            ast::PatKind::Constructor { payload, .. } => {
                // Visit payload (the constructor name itself is part of pat.span)
                self.visit_pat(payload, false);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        self.visit_pat(p, false);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    self.visit_pat(p, false);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;

    fn full_params(uri: &Uri) -> SemanticTokensParams {
        SemanticTokensParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    fn delta_params(uri: &Uri, prev: &str) -> SemanticTokensDeltaParams {
        SemanticTokensDeltaParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            previous_result_id: prev.to_string(),
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    #[test]
    fn semantic_tokens_full_caches_result_id() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        let resp = handle_semantic_tokens_full(&mut ws.state, &full_params(&uri))
            .expect("full tokens");
        let id = match resp {
            SemanticTokensResult::Tokens(t) => t.result_id,
            _ => panic!("expected Tokens variant"),
        };
        assert!(id.is_some(), "result_id should be set");
        assert!(
            ws.state.semantic_token_cache.contains_key(&uri),
            "cache should be populated"
        );
    }

    #[test]
    fn semantic_tokens_delta_returns_empty_edits_when_unchanged() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        let full = handle_semantic_tokens_full(&mut ws.state, &full_params(&uri))
            .expect("full");
        let prev_id = match full {
            SemanticTokensResult::Tokens(t) => t.result_id.unwrap(),
            _ => panic!(),
        };
        let delta = handle_semantic_tokens_full_delta(&mut ws.state, &delta_params(&uri, &prev_id))
            .expect("delta");
        match delta {
            SemanticTokensFullDeltaResult::TokensDelta(d) => {
                assert!(d.edits.is_empty(), "no changes → no edits");
            }
            _ => panic!("expected TokensDelta variant"),
        }
    }

    #[test]
    fn semantic_tokens_delta_falls_back_to_full_on_unknown_id() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        // Skip the initial full request — the cache is empty, so delta
        // should bail out to a full response rather than diffing nothing.
        let delta = handle_semantic_tokens_full_delta(&mut ws.state, &delta_params(&uri, "stale"))
            .expect("delta");
        assert!(
            matches!(delta, SemanticTokensFullDeltaResult::Tokens(_)),
            "stale prev_result_id should produce a full response"
        );
    }

    #[test]
    fn delta_encode_handles_out_of_order_tokens_without_underflow() {
        // Tokens are normally sorted by start offset; the encoder's reset
        // path handles the out-of-order case by rewinding the cursor. Before
        // the saturating-arithmetic fix, the rewind left `prev_line` /
        // `prev_char` reflecting the later token, so the second iteration
        // computed `0 - prev_line` and panicked in debug builds.
        let source = "abc\ndef\n";
        let tokens = vec![
            super::RawToken {
                start: 4,
                length: 3,
                token_type: 0,
                modifiers: 0,
            },
            super::RawToken {
                start: 0,
                length: 3,
                token_type: 0,
                modifiers: 0,
            },
        ];
        let encoded = super::delta_encode_tokens(&tokens, source);
        assert_eq!(encoded.len(), 2);
        // The clamp produces a (lossy but well-formed) encoding rather than
        // a panic. Exact deltas don't matter — just that we got back two
        // entries with no overflow propagated through `length`.
        assert!(encoded.iter().all(|t| t.length == 3));
    }

    #[test]
    fn semantic_tokens_delta_emits_edits_for_changed_tokens() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        let full = handle_semantic_tokens_full(&mut ws.state, &full_params(&uri))
            .expect("full");
        let prev_id = match full {
            SemanticTokensResult::Tokens(t) => t.result_id.unwrap(),
            _ => panic!(),
        };
        // Mutate the underlying document — append a new top-level decl so
        // the token list grows. Reach in directly since this is a unit
        // test; in production a didChange notification triggers
        // re-analysis.
        if let Some(doc) = ws.state.documents.get_mut(&uri) {
            doc.source.push_str("y = 42\n");
        }
        ws.reanalyze(&uri);
        let delta = handle_semantic_tokens_full_delta(&mut ws.state, &delta_params(&uri, &prev_id))
            .expect("delta");
        match delta {
            SemanticTokensFullDeltaResult::TokensDelta(d) => {
                assert!(!d.edits.is_empty(), "expected edits for token change");
            }
            other => panic!("expected TokensDelta, got: {other:?}"),
        }
    }
}

fn delta_encode_tokens(tokens: &[RawToken], source: &str) -> Vec<SemanticToken> {
    // Tokens arrive in source order. A naive implementation calls
    // `offset_to_position` per token, each O(N) — yielding O(M·N) overall.
    // Instead, do a single forward sweep over the source, tracking cumulative
    // line/UTF-16-column, and convert each token's byte offset by advancing
    // a cursor.
    let mut result = Vec::with_capacity(tokens.len());
    let mut prev_line = 0u32;
    let mut prev_char = 0u32;

    let mut byte_cursor = 0usize;
    let mut line = 0u32;
    let mut line_start_byte = 0usize;
    let mut col_utf16 = 0u32;
    let bytes = source.as_bytes();

    for token in tokens {
        // Tokens may not be sorted in pathological cases; reset and rescan
        // from the start for any token before the cursor.
        if token.start < byte_cursor {
            byte_cursor = 0;
            line = 0;
            line_start_byte = 0;
            col_utf16 = 0;
        }

        // Advance to token.start, updating line and column as we go.
        let target = token.start.min(source.len());
        while byte_cursor < target {
            // Find the next char boundary so we can decode one codepoint.
            if bytes[byte_cursor] == b'\n' {
                line += 1;
                byte_cursor += 1;
                line_start_byte = byte_cursor;
                col_utf16 = 0;
            } else if bytes[byte_cursor] == b'\r' {
                // Skip \r in CRLF — it doesn't contribute a UTF-16 column.
                byte_cursor += 1;
            } else {
                let mut next = byte_cursor + 1;
                while next < source.len() && !source.is_char_boundary(next) {
                    next += 1;
                }
                if let Some(s) = source.get(byte_cursor..next) {
                    if let Some(c) = s.chars().next() {
                        col_utf16 += c.len_utf16() as u32;
                    }
                }
                byte_cursor = next;
            }
        }
        let _ = line_start_byte; // explicit: line_start_byte is computed for clarity, not used downstream.

        // Token length must be expressed in UTF-16 code units (LSP spec
        // for the default UTF-16 position encoding). Compute it from the
        // token's source slice rather than passing through byte length —
        // otherwise non-ASCII tokens render with the wrong width.
        let token_end = (token.start + token.length).min(source.len());
        let utf16_length: u32 = source
            .get(token.start..token_end)
            .map(|slice| slice.chars().map(|c| c.len_utf16() as u32).sum())
            .unwrap_or(token.length as u32);

        // Saturating arithmetic guards the out-of-order reset above: when a
        // token's `start` is before the cursor we rewind line/col to 0, but
        // `prev_line`/`prev_char` still reflect the previous (later) token.
        // A naive subtraction would underflow into a huge u32 and feed the
        // editor a nonsense delta. Clamping to 0 produces a valid (if
        // suboptimal) encoding instead of a panic in debug builds.
        let delta_line = line.saturating_sub(prev_line);
        let delta_start = if delta_line == 0 {
            col_utf16.saturating_sub(prev_char)
        } else {
            col_utf16
        };

        result.push(SemanticToken {
            delta_line,
            delta_start,
            length: utf16_length,
            token_type: token.token_type,
            token_modifiers_bitset: token.modifiers,
        });

        prev_line = line;
        prev_char = col_utf16;
    }

    result
}
