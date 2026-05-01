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
use crate::utils::find_word_in_source;

// ── Semantic Tokens ─────────────────────────────────────────────────

pub(crate) fn handle_semantic_tokens_full(
    state: &ServerState,
    params: &SemanticTokensParams,
) -> Option<SemanticTokensResult> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut raw_tokens = Vec::new();
    let mut collector = TokenCollector {
        tokens: &mut raw_tokens,
        source: &doc.source,
    };

    for decl in &doc.module.decls {
        collector.visit_decl(decl);
    }

    // Add keyword and operator tokens from lexer
    let ast_token_starts: HashSet<usize> = raw_tokens.iter().map(|t| t.start).collect();
    for (span, tok_type) in &doc.keyword_tokens {
        // Only add if no AST-based token already covers this position
        if !ast_token_starts.contains(&span.start) {
            raw_tokens.push(RawToken {
                start: span.start,
                length: span.end - span.start,
                token_type: *tok_type,
                modifiers: 0,
            });
        }
    }

    raw_tokens.sort_by_key(|t| (t.start, t.length));

    // Delta encode
    let encoded = delta_encode_tokens(&raw_tokens, &doc.source);

    Some(SemanticTokensResult::Tokens(SemanticTokens {
        result_id: None,
        data: encoded,
    }))
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
            DeclKind::Fun { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.start + name.len() + 20) {
                    self.add(s, TOK_FUNCTION, MOD_DECLARATION);
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
                }
            }
            DeclKind::Source { name, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
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
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
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

        let delta_line = line - prev_line;
        let delta_start = if delta_line == 0 { col_utf16 - prev_char } else { col_utf16 };

        result.push(SemanticToken {
            delta_line,
            delta_start,
            length: token.length as u32,
            token_type: token.token_type,
            token_modifiers_bitset: token.modifiers,
        });

        prev_line = line;
        prev_char = col_utf16;
    }

    result
}
