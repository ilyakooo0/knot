//! Position, span, URI, word, and edit-distance helpers shared across LSP
//! handlers. Plus doc-comment extraction and keyword-token collection — both
//! sit on the boundary between AST and presentation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use lsp_types::{Position, Range, Uri};

use knot::ast::{self, DeclKind, Module, Span};

use crate::legend::{TOK_KEYWORD, TOK_OPERATOR};

// ── Position ↔ Span ─────────────────────────────────────────────────

pub fn span_to_range(span: Span, source: &str) -> Range {
    Range {
        start: offset_to_position(source, span.start),
        end: offset_to_position(source, span.end),
    }
}

pub fn offset_to_position(source: &str, offset: usize) -> Position {
    // LSP positions use UTF-16 code units by default (we don't negotiate
    // `positionEncodingKind`), so count code units from the start of the
    // line up to `offset`, not bytes or codepoints.
    let clamped = offset.min(source.len());
    let bytes = source.as_bytes();
    let mut line: u32 = 0;
    let mut line_start: usize = 0;
    for i in 0..clamped {
        if bytes[i] == b'\n' {
            line += 1;
            line_start = i + 1;
        }
    }
    let mut safe_offset = clamped;
    while safe_offset > line_start && !source.is_char_boundary(safe_offset) {
        safe_offset -= 1;
    }
    // \r immediately before \n is part of the CRLF line break (LSP spec
    // says it counts as one character together), so strip it from the
    // column count. Stray \r in the middle of a line still counts — the
    // matching `position_to_offset` only strips the *trailing* \r too,
    // and the round-trip needs to be symmetric.
    let line_slice = &source[line_start..safe_offset];
    let line_slice = line_slice.strip_suffix('\r').unwrap_or(line_slice);
    let character: u32 = line_slice.chars().map(|c| c.len_utf16() as u32).sum();
    Position::new(line, character)
}

pub fn position_to_offset(source: &str, pos: Position) -> usize {
    let mut offset = 0;
    for (i, line) in source.split('\n').enumerate() {
        if i == pos.line as usize {
            // Strip trailing \r so CRLF line endings don't contribute a phantom
            // UTF-16 column. The LSP spec says the line break is one character.
            let line = line.strip_suffix('\r').unwrap_or(line);
            let mut utf16_count: u32 = 0;
            let mut byte_pos: usize = 0;
            for c in line.chars() {
                if utf16_count >= pos.character {
                    break;
                }
                utf16_count += c.len_utf16() as u32;
                byte_pos += c.len_utf8();
            }
            return offset + byte_pos;
        }
        offset += line.len() + 1;
    }
    source.len()
}

pub fn word_at_position<'a>(source: &'a str, pos: Position) -> Option<&'a str> {
    let offset = position_to_offset(source, pos);
    let (start, end) = word_bounds_at_offset(source, offset)?;
    Some(&source[start..end])
}

/// Like `word_at_position`, but returns the (start, end) byte span of the word
/// covering `offset`. Used by hover to populate the response range so editors
/// can highlight the hovered identifier.
pub fn word_span_at_offset(source: &str, offset: usize) -> Option<Span> {
    let (start, end) = word_bounds_at_offset(source, offset)?;
    Some(Span::new(start, end))
}

fn word_bounds_at_offset(source: &str, offset: usize) -> Option<(usize, usize)> {
    let bytes = source.as_bytes();
    if offset >= bytes.len() {
        return None;
    }

    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    if !is_ident(bytes[offset]) {
        return None;
    }

    let start = (0..offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);

    let end = (offset..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    Some((start, end))
}

/// Slice `source` by `span` without panicking on stale or out-of-bounds spans.
///
/// Spans recorded during a previous analysis can outlive the source they
/// point into when an edit truncates the document. Clamping both endpoints
/// (and snapping to char boundaries) keeps such spans safe to read instead
/// of taking down the LSP.
pub fn safe_slice<'a>(source: &'a str, span: Span) -> &'a str {
    let len = source.len();
    let mut start = span.start.min(len);
    let mut end = span.end.min(len);
    if start > end {
        start = end;
    }
    while start < len && !source.is_char_boundary(start) {
        start += 1;
    }
    while end < len && !source.is_char_boundary(end) {
        end += 1;
    }
    &source[start..end]
}

// ── URIs ────────────────────────────────────────────────────────────

pub fn uri_to_path(uri: &Uri) -> Option<PathBuf> {
    let s = uri.as_str();
    let raw = s.strip_prefix("file://")?;
    let decoded = percent_decode(raw);
    // On Windows, file URIs look like `file:///C:/...` — strip the leading
    // slash before the drive letter so the path is absolute on the host.
    #[cfg(windows)]
    let decoded = if decoded.starts_with('/') {
        let bytes = decoded.as_bytes();
        if bytes.len() >= 3 && bytes[2] == b':' && bytes[1].is_ascii_alphabetic() {
            decoded[1..].to_string()
        } else {
            decoded
        }
    } else {
        decoded
    };
    Some(PathBuf::from(decoded))
}

pub fn path_to_uri(path: &Path) -> Option<Uri> {
    let path_str = path.to_str()?;
    // Path components on Unix start with `/`; on Windows `C:\foo` we prepend `/`.
    let mut encoded = String::from("file://");
    let needs_leading_slash = !path_str.starts_with('/');
    if needs_leading_slash {
        encoded.push('/');
    }
    for &b in path_str.as_bytes() {
        let c = b as char;
        // Replace Windows backslashes with forward slashes for URIs.
        let c = if c == '\\' { '/' } else { c };
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~' | '/' | ':') {
            encoded.push(c);
        } else {
            encoded.push_str(&format!("%{:02X}", b));
        }
    }
    encoded.parse::<Uri>().ok()
}

/// Decode `%xx` sequences in a string. Invalid escapes are left as-is.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    // The percent-decoded path is a sequence of bytes representing the OS
    // path. On UTF-8 systems it should be valid UTF-8; if it isn't we fall
    // back to a lossy conversion rather than dropping the path entirely.
    String::from_utf8(out.clone()).unwrap_or_else(|_| String::from_utf8_lossy(&out).into_owned())
}

// ── Edit distance ───────────────────────────────────────────────────

pub fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() {
        dp[i][0] = i;
    }
    for j in 0..=b.len() {
        dp[0][j] = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

// ── Word search ────────────────────────────────────────────────────

/// Find a whole-word occurrence of `name` in `source[start..end]`.
pub fn find_word_in_source(source: &str, name: &str, start: usize, end: usize) -> Option<Span> {
    let end = end.min(source.len());
    let text = source.get(start..end)?;
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    let mut search_start = 0;
    while search_start <= text.len() {
        let Some(rest) = text.get(search_start..) else {
            // search_start landed mid-codepoint; advance to the next char
            // boundary so the next iteration can slice safely.
            search_start += 1;
            continue;
        };
        let Some(pos) = rest.find(name) else { break };
        let abs_pos = start + search_start + pos;
        let abs_end = abs_pos + name.len();

        let left_ok = abs_pos == 0 || !is_ident(bytes[abs_pos - 1]);
        let right_ok = abs_end >= bytes.len() || !is_ident(bytes[abs_end]);

        if left_ok && right_ok {
            return Some(Span::new(abs_pos, abs_end));
        }
        search_start += pos + 1;
    }
    None
}

// ── Doc comments ────────────────────────────────────────────────────

/// Extract doc comments (lines starting with `-- `) above each declaration.
pub fn extract_doc_comments(source: &str, module: &Module) -> HashMap<String, String> {
    let mut comments = HashMap::new();
    let lines: Vec<&str> = source.split('\n').collect();

    for decl in &module.decls {
        let name = match &decl.node {
            DeclKind::Fun { name, .. }
            | DeclKind::Data { name, .. }
            | DeclKind::TypeAlias { name, .. }
            | DeclKind::Source { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. }
            | DeclKind::Trait { name, .. }
            | DeclKind::Route { name, .. }
            | DeclKind::RouteComposite { name, .. } => name.clone(),
            _ => continue,
        };

        let decl_line = offset_to_position(source, decl.span.start).line as usize;
        if decl_line == 0 {
            continue;
        }

        let mut comment_lines = Vec::new();
        let mut line_idx = decl_line;
        while line_idx > 0 {
            line_idx -= 1;
            let line = lines.get(line_idx).map(|l| l.trim()).unwrap_or("");
            if let Some(text) = line.strip_prefix("-- ") {
                comment_lines.push(text.to_string());
            } else if line == "--" {
                comment_lines.push(String::new());
            } else {
                break;
            }
        }

        if !comment_lines.is_empty() {
            comment_lines.reverse();
            comments.insert(name, comment_lines.join("\n"));
        }
    }

    comments
}

// ── Keyword/operator tokens ─────────────────────────────────────────

/// Collect keyword and operator token positions from the lexer token stream.
pub fn collect_keyword_operator_positions(tokens: &[knot::lexer::Token]) -> Vec<(Span, u32)> {
    use knot::lexer::TokenKind;
    let mut positions = Vec::new();
    for token in tokens {
        let tok_type = match &token.kind {
            TokenKind::Import
            | TokenKind::Data
            | TokenKind::Type
            | TokenKind::Trait
            | TokenKind::Impl
            | TokenKind::Route
            | TokenKind::Migrate
            | TokenKind::Where
            | TokenKind::Do
            | TokenKind::If
            | TokenKind::Then
            | TokenKind::Else
            | TokenKind::Case
            | TokenKind::Of
            | TokenKind::Let
            | TokenKind::In
            | TokenKind::Not
            | TokenKind::Replace
            | TokenKind::Atomic
            | TokenKind::Deriving
            | TokenKind::With
            | TokenKind::Export
            | TokenKind::Unit
            | TokenKind::Refine => Some(TOK_KEYWORD),
            TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::EqEq
            | TokenKind::BangEq
            | TokenKind::Lt
            | TokenKind::Gt
            | TokenKind::Le
            | TokenKind::Ge
            | TokenKind::PlusPlus
            | TokenKind::AndAnd
            | TokenKind::OrOr
            | TokenKind::PipeGt
            | TokenKind::Caret
            | TokenKind::Arrow
            | TokenKind::FatArrow
            | TokenKind::LArrow => Some(TOK_OPERATOR),
            _ => None,
        };
        if let Some(tt) = tok_type {
            positions.push((token.span, tt));
        }
    }
    positions
}

// ── AST utility ─────────────────────────────────────────────────────

/// Recurse into all sub-expressions of `expr`, calling `f` on each.
/// Lives here so multiple modules can share it without circular deps.
pub fn recurse_expr<F: FnMut(&ast::Expr)>(expr: &ast::Expr, mut f: F) {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            f(func);
            f(arg);
        }
        ast::ExprKind::Lambda { body, .. } => f(body),
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            f(lhs);
            f(rhs);
        }
        ast::ExprKind::UnaryOp { operand, .. } => f(operand),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            f(cond);
            f(then_branch);
            f(else_branch);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            f(scrutinee);
            for arm in arms {
                f(&arm.body);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. }
                    | ast::StmtKind::Let { expr, .. }
                    | ast::StmtKind::Expr(expr)
                    | ast::StmtKind::Where { cond: expr } => f(expr),
                    ast::StmtKind::GroupBy { key } => f(key),
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => f(e),
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            f(target);
            f(value);
        }
        ast::ExprKind::Record(fields) => {
            for fld in fields {
                f(&fld.value);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            f(base);
            for fld in fields {
                f(&fld.value);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                f(e);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => f(expr),
        ast::ExprKind::Annot { expr, .. } => f(expr),
        ast::ExprKind::UnitLit { value, .. } => f(value),
        _ => {}
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_to_offset_handles_ascii() {
        let src = "abc\ndef";
        assert_eq!(position_to_offset(src, Position::new(0, 0)), 0);
        assert_eq!(position_to_offset(src, Position::new(0, 3)), 3);
        assert_eq!(position_to_offset(src, Position::new(1, 0)), 4);
        assert_eq!(position_to_offset(src, Position::new(1, 3)), 7);
    }

    #[test]
    fn position_to_offset_treats_character_as_utf16_units() {
        // "é" is 2 bytes in UTF-8 but 1 UTF-16 code unit.
        let src = "éx";
        assert_eq!(position_to_offset(src, Position::new(0, 0)), 0);
        assert_eq!(position_to_offset(src, Position::new(0, 1)), 2); // after é
        assert_eq!(position_to_offset(src, Position::new(0, 2)), 3); // after x
    }

    #[test]
    fn position_to_offset_handles_surrogate_pairs() {
        // 😀 is 4 bytes in UTF-8 and 2 UTF-16 code units (surrogate pair).
        let src = "a😀b";
        assert_eq!(position_to_offset(src, Position::new(0, 0)), 0); // before a
        assert_eq!(position_to_offset(src, Position::new(0, 1)), 1); // after a
        assert_eq!(position_to_offset(src, Position::new(0, 3)), 5); // after 😀 (1 + 4)
        assert_eq!(position_to_offset(src, Position::new(0, 4)), 6); // after b
    }

    #[test]
    fn offset_to_position_round_trips_ascii() {
        let src = "hello\nworld";
        for offset in 0..=src.len() {
            let pos = offset_to_position(src, offset);
            assert_eq!(position_to_offset(src, pos), offset, "offset {}", offset);
        }
    }

    #[test]
    fn offset_to_position_round_trips_unicode() {
        let src = "x é\n😀 y";
        // Round-trip every char-boundary offset.
        for offset in 0..=src.len() {
            if !src.is_char_boundary(offset) {
                continue;
            }
            let pos = offset_to_position(src, offset);
            assert_eq!(position_to_offset(src, pos), offset, "offset {}", offset);
        }
    }

    #[test]
    fn offset_to_position_emits_utf16_columns_for_surrogate_pairs() {
        let src = "a😀b";
        // Byte offset 5 is just after 😀 — should be UTF-16 column 3.
        let pos = offset_to_position(src, 5);
        assert_eq!(pos, Position::new(0, 3));
    }
}
