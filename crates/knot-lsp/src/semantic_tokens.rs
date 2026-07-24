//! `textDocument/semanticTokens/full` handler. Walks the AST to label
//! identifiers, literals, and operator tokens with their semantic-tokens types.

use std::collections::HashSet;

use lsp_types::*;

use knot::ast::{self, Span};

use crate::builtins::EFFECTFUL_BUILTINS;
use crate::legend::{
    MOD_DECLARATION, MOD_EFFECTFUL, MOD_MUTATION, MOD_READONLY, TOK_ENUM_MEMBER, TOK_FUNCTION,
    TOK_KEYWORD, TOK_NAMESPACE, TOK_NUMBER, TOK_PARAMETER, TOK_PROPERTY, TOK_STRING, TOK_STRUCT,
    TOK_TYPE, TOK_VARIABLE,
};
use crate::state::ServerState;
use crate::utils::{find_word_in_source, position_to_offset, top_fields};

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
    // Per the LSP 3.16+ spec, `start` and `deleteCount` are offsets/counts
    // into the **flat `uinteger[]` data array** (5 integers per token tuple),
    // NOT into the logical token array. Multiply by 5 to convert from token
    // units to flat-integer units.
    vec![SemanticTokensEdit {
        start: (start * 5) as u32,
        delete_count: (prev_end - start) as u32 * 5,
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

    for decl in top_fields(&doc.module) {
        // Coarse pre-filter: skip decls whose span doesn't overlap the
        // requested range. Fine-grained per-token filtering still happens
        // below, but this avoids walking unrelated subtrees.
        if let Some((rs, re)) = range
            && (decl.value.span.end < rs || decl.value.span.start > re) {
                continue;
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
                // Split multi-line tokens into per-line tokens. CRLF files
                // leave a trailing '\r' on every non-final segment — strip it
                // from the emitted token length (it's part of the line break,
                // not a visible column), but keep it in the offset advance.
                let mut offset = span.start;
                for raw_line in text.split('\n') {
                    let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
                    if !line.is_empty() {
                        self.tokens.push(RawToken {
                            start: offset,
                            length: line.len(),
                            token_type,
                            modifiers,
                        });
                    }
                    offset += raw_line.len() + 1; // +1 for the '\n'
                }
            }
        }
    }

    fn visit_decl(&mut self, decl: &ast::RecordField) {
        let dspan = decl.value.span;
        // A named function field's own signature.
        if let Some(scheme) = &decl.sig {
            self.visit_type(&scheme.ty);
        }
        match &decl.value.node {
            ast::ExprKind::DataCtor {
                name, constructors, ..
            } => {
                let name_span =
                    find_word_in_source(self.source, name, dspan.start, dspan.end);
                if let Some(s) = name_span {
                    self.add(s, TOK_STRUCT, MOD_DECLARATION);
                }
                // Constructors appear after the `=`. Searching from the decl
                // start would match the TYPE name first for self-named
                // constructors (`data Person = Person {…}`), emitting an
                // overlapping token on the type name and none on the actual
                // constructor. Advance past each hit so a constructor name
                // appearing in a previous constructor's field types doesn't
                // steal a later constructor's token either.
                let mut search_from = self
                    .source
                    .get(dspan.start..dspan.end.min(self.source.len()))
                    .and_then(|t| t.find('='))
                    .map(|p| dspan.start + p + 1)
                    .or_else(|| name_span.map(|s| s.end))
                    .unwrap_or(dspan.start);
                for ctor in constructors {
                    // Bound the name search to the window *before* this
                    // constructor's first field type. Otherwise a later
                    // constructor whose name also appears as a field type in an
                    // earlier constructor (`data T = A {x: B} | B {y: Int}`)
                    // would match that field-type occurrence instead of the
                    // real constructor. The name always precedes the fields.
                    let search_end = ctor
                        .fields
                        .first()
                        .map(|f| f.value.span.start)
                        .unwrap_or(dspan.end);
                    let found = find_word_in_source(self.source, &ctor.name, search_from, search_end);
                    if let Some(s) = found {
                        self.add(s, TOK_ENUM_MEMBER, MOD_DECLARATION);
                    }
                    for f in &ctor.fields {
                        self.visit_type(&f.value);
                    }
                    // Advance past this constructor's last field type (or its
                    // name, if nullary) so its field types can never be matched
                    // as the next constructor's name.
                    if let Some(last) = ctor.fields.last() {
                        search_from = last.value.span.end;
                    } else if let Some(s) = found {
                        search_from = s.end;
                    }
                }
            }
            ast::ExprKind::TypeCtor { ty, .. } => {
                // Refined-type aliases (`type Nat = Int 1 where \x -> x >= 0`)
                // contain expression-bodied predicates; walk the type so the
                // predicate's tokens get highlighted alongside the rest of
                // the file.
                self.visit_type(ty);
            }
            ast::ExprKind::SourceDecl { name, ty, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, dspan.start, dspan.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
                self.visit_type(ty);
            }
            ast::ExprKind::ViewDecl { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, dspan.start, dspan.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
                self.visit_expr(body);
            }
            ast::ExprKind::DerivedDecl { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, dspan.start, dspan.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION | MOD_READONLY);
                }
                self.visit_expr(body);
            }
            ast::ExprKind::RouteDecl { name, entries } => {
                if let Some(s) = find_word_in_source(self.source, name, dspan.start, dspan.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
                for entry in entries {
                    for f in entry
                        .body_fields
                        .iter()
                        .chain(&entry.query_params)
                        .chain(&entry.request_headers)
                        .chain(&entry.response_headers)
                    {
                        self.visit_type(&f.value);
                    }
                    if let Some(resp) = &entry.response_ty {
                        self.visit_type(resp);
                    }
                    for seg in &entry.path {
                        if let ast::PathSegment::Param { ty, .. } = seg {
                            self.visit_type(ty);
                        }
                    }
                    // The `rateLimit <expr>` clause is user-edited code; walk it
                    // so its tokens are highlighted like any other expression
                    // (defs.rs already resolves references inside it).
                    if let Some(rl) = &entry.rate_limit {
                        self.visit_expr(rl);
                    }
                }
            }
            ast::ExprKind::RouteCompositeDecl { name, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, dspan.start, dspan.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
            }
            ast::ExprKind::SubsetConstraint { .. } => {}
            _ => {
                // A named function field. Search the whole declaration span
                // (rather than an arbitrary `name.len() + 20` byte window that
                // could end mid-codepoint, e.g. `f : Café -> Int`).
                if let Some(s) = find_word_in_source(self.source, &decl.name, dspan.start, dspan.end) {
                    self.add(s, TOK_FUNCTION, MOD_DECLARATION);
                }
                self.visit_expr(&decl.value);
            }
        }
    }

    /// Strip surrounding parentheses (and interior whitespace) from a span.
    /// The parser folds `( … )` into the wrapped node's span — `(x)` becomes a
    /// `Var` whose span covers the parens — so leaf tokens must trim them back
    /// or they'd color the parentheses (and `(r.total)` would miscompute the
    /// field-name suffix). Sigils (`*`/`&`) are preserved. A no-op for spans
    /// that aren't parenthesized. Handles nested parens (`((x))`).
    fn strip_parens(&self, mut span: Span) -> Span {
        let bytes = self.source.as_bytes();
        loop {
            while span.start < span.end
                && bytes.get(span.start).is_some_and(|b| b.is_ascii_whitespace())
            {
                span.start += 1;
            }
            while span.end > span.start
                && bytes.get(span.end - 1).is_some_and(|b| b.is_ascii_whitespace())
            {
                span.end -= 1;
            }
            if span.end - span.start >= 2
                && bytes.get(span.start) == Some(&b'(')
                && bytes.get(span.end - 1) == Some(&b')')
            {
                span.start += 1;
                span.end -= 1;
            } else {
                return span;
            }
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
                self.add(self.strip_parens(expr.span), TOK_VARIABLE, modifier);
            }
            ast::ExprKind::Constructor(_) => {
                self.add(self.strip_parens(expr.span), TOK_ENUM_MEMBER, 0);
            }
            ast::ExprKind::SourceRef(_) => {
                self.add(self.strip_parens(expr.span), TOK_NAMESPACE, 0);
            }
            ast::ExprKind::DerivedRef(_) => {
                self.add(self.strip_parens(expr.span), TOK_NAMESPACE, MOD_READONLY);
            }
            ast::ExprKind::ImplicitRef(_) => {
                self.add(self.strip_parens(expr.span), TOK_VARIABLE, MOD_READONLY);
            }
            ast::ExprKind::With { record, body } => {
                self.visit_expr(record);
                self.visit_expr(body);
            }
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                self.visit_expr(inner);
                // Field name span: the part after the `.`. A parenthesized field
                // access (`(r.total)`) widens `expr.span` to include the `)`, so
                // strip parens first, then take the field as the suffix of the
                // trimmed end. Guard the subtraction against underflow and
                // confirm the suffix actually spells the field, mirroring
                // rename.rs/linked_editing.rs — a stale or malformed span with
                // `end < field.len()` would otherwise panic (debug) or wrap to a
                // bogus span (release).
                let field_end = self.strip_parens(expr.span).end;
                if field_end >= field.len() {
                    let field_start = field_end - field.len();
                    if field_start < field_end
                        && self.source.get(field_start..field_end) == Some(field.as_str())
                    {
                        self.add(Span::new(field_start, field_end), TOK_PROPERTY, 0);
                    }
                }
            }
            ast::ExprKind::Lit(ast::Literal::Int(_) | ast::Literal::Float(_)) => {
                self.add(self.strip_parens(expr.span), TOK_NUMBER, 0);
            }
            ast::ExprKind::Lit(ast::Literal::Text(_)) => {
                self.add(self.strip_parens(expr.span), TOK_STRING, 0);
            }
            ast::ExprKind::Lit(ast::Literal::Bool(_)) => {
                self.add(self.strip_parens(expr.span), TOK_KEYWORD, 0);
            }
            ast::ExprKind::Lit(ast::Literal::Bytes(_)) => {
                self.add(self.strip_parens(expr.span), TOK_STRING, 0);
            }
            ast::ExprKind::Lambda { params, body, .. } => {
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
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    // Emit a property token for the field name. The field name
                    // appears in source before the `:` that separates it from
                    // the value, so search backwards from the value's span start.
                    let val_start = self.strip_parens(f.value.span).start;
                    if val_start >= f.name.len() + 1 {
                        // Look for `name:` before the value — find the last
                        // occurrence of the field name before the value start.
                        let search_end = val_start;
                        let search_start = search_end.saturating_sub(f.name.len() + 1);
                        if let Some(name_start) = self.source[search_start..search_end].rfind(f.name.as_str()) {
                            let abs_start = search_start + name_start;
                            let abs_end = abs_start + f.name.len();
                            if self.source.get(abs_start..abs_end) == Some(f.name.as_str()) {
                                self.add(Span::new(abs_start, abs_end), TOK_PROPERTY, 0);
                            }
                        }
                    }
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
            ast::ExprKind::TimeUnitLit { value, .. } => {
                self.visit_expr(value);
            }
            ast::ExprKind::Annot { expr: inner, ty } => {
                self.visit_expr(inner);
                self.visit_type(ty);
            }
            ast::ExprKind::Serve { api_span, handlers, .. } => {
                // The API type name, each endpoint constructor, and every
                // handler body need tokens — otherwise a whole `serve` block
                // renders unhighlighted. (Mirrors defs/rename/folding, which
                // all recurse into serve handler bodies.)
                self.add(*api_span, TOK_TYPE, 0);
                for h in handlers {
                    self.add(h.endpoint_span, TOK_ENUM_MEMBER, 0);
                    self.visit_expr(&h.body);
                }
            }
            // Highlight the alias body's type (the alias name token itself is
            // emitted by the record-field path); no value exprs inside.
            ast::ExprKind::TypeCtor { ty, .. } => self.visit_type(ty),
            ast::ExprKind::DataCtor { constructors, .. } => {
                for c in constructors {
                    for f in &c.fields {
                        self.visit_type(&f.value);
                    }
                }
            }
            // A source-declaration field's type is highlighted (`*todos : [Todo]`
            // highlights `Todo`).
            ast::ExprKind::SourceDecl { ty, .. } => self.visit_type(ty),
            // A subset constraint carries no type or value tokens.
            ast::ExprKind::SubsetConstraint { .. } => {}
            // A route field's entries carry types to highlight; a composite
            // only names other routes.
            ast::ExprKind::RouteDecl { entries, .. } => {
                for entry in entries {
                    for seg in &entry.path {
                        if let ast::PathSegment::Param { ty, .. } = seg {
                            self.visit_type(ty);
                        }
                    }
                    for f in entry
                        .body_fields
                        .iter()
                        .chain(&entry.query_params)
                        .chain(&entry.request_headers)
                        .chain(&entry.response_headers)
                    {
                        self.visit_type(&f.value);
                    }
                    if let Some(resp) = &entry.response_ty {
                        self.visit_type(resp);
                    }
                }
            }
            ast::ExprKind::RouteCompositeDecl { .. } => {}
            // A view field's annotation and body are highlighted.
            ast::ExprKind::ViewDecl { ty, body, .. } | ast::ExprKind::DerivedDecl { ty, body, .. } => {
                if let Some(scheme) = ty {
                    self.visit_type(&scheme.ty);
                }
                self.visit_expr(body);
            }
        }
    }

    /// Walk a type expression and emit semantic tokens for refined-type
    /// predicate bodies. Refined predicates are arbitrary `Expr`s, so the
    /// existing expression visitor handles them; this method just locates
    /// and recurses through the type AST to find each `Refined` node.
    fn visit_type(&mut self, ty: &ast::Type) {
        match &ty.node {
            ast::TypeKind::Named(name) => {
                if let Some(s) = find_word_in_source(self.source, name, ty.span.start, ty.span.end) {
                    self.add(s, TOK_TYPE, 0);
                }
            }
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
            ast::TypeKind::Unit(_) => {},
            ast::TypeKind::UnitAnnotated { base, .. } => {
                self.visit_type(base);
            }
            // Leaves with no named sub-types. Listed explicitly (rather than a
            // `_` wildcard) so a newly added `TypeKind` variant surfaces as a
            // non-exhaustive-match error here instead of being silently skipped.
            ast::TypeKind::Var(_) | ast::TypeKind::Hole => {}
        }
    }

    fn visit_pat(&mut self, pat: &ast::Pat, is_param: bool) {
        match &pat.node {
            ast::PatKind::Var(_) => {
                let tok = if is_param { TOK_PARAMETER } else { TOK_VARIABLE };
                self.add(pat.span, tok, MOD_DECLARATION);
            }
            ast::PatKind::Constructor { name, payload, .. } => {
                // Emit an ENUM_MEMBER token for the constructor name itself,
                // mirroring the expression-side `ExprKind::Constructor` arm so
                // `Circle c` in a pattern highlights like `Circle {..}` in
                // expression position. Locate the actual name token via
                // `find_word_in_source` — the name does NOT lead `pat.span`
                // for parenthesized patterns `(Circle c)`, where `pat.span`
                // starts at `(`.
                let name_span = find_word_in_source(self.source, name, pat.span.start, pat.span.end)
                    .unwrap_or_else(|| Span::new(pat.span.start, pat.span.start + name.len()));
                // Guard against mid-codepoint slice: if the fallback span
                // doesn't align to a char boundary, skip emitting the token
                // rather than panicking on `&source[start..end]`.
                if self.source.get(name_span.start..name_span.end) == Some(name) {
                    self.add(name_span, TOK_ENUM_MEMBER, 0);
                }
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
            ast::PatKind::Cons { head, tail } => {
                self.visit_pat(head, false);
                self.visit_pat(tail, false);
            }
            _ => {}
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
                if bytes.get(byte_cursor + 1) == Some(&b'\n') {
                    // The \r of a CRLF pair is part of the line break and
                    // doesn't contribute a UTF-16 column; the following \n
                    // advances the line.
                    byte_cursor += 1;
                } else {
                    // A lone \r (classic-Mac line ending) is its own line
                    // break, matching `utils::offset_to_position` and the
                    // lexer — so semantic-token positions stay in sync with the
                    // rest of the LSP for `\r`-only files.
                    line += 1;
                    byte_cursor += 1;
                    line_start_byte = byte_cursor;
                    col_utf16 = 0;
                }
            } else {
                let mut next = byte_cursor + 1;
                while next < source.len() && !source.is_char_boundary(next) {
                    next += 1;
                }
                if let Some(s) = source.get(byte_cursor..next)
                    && let Some(c) = s.chars().next() {
                        col_utf16 += c.len_utf16() as u32;
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

// Regression tests for the 2026-06 LSP bug-fix batch (semantic tokens).

