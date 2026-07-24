//! Position, span, URI, word, and edit-distance helpers shared across LSP
//! handlers. Plus doc-comment extraction and keyword-token collection — both
//! sit on the boundary between AST and presentation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use lsp_types::{Position, Range, Uri};

use knot::ast::{self, Expr, ExprKind, RecordField, Span};

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
        match bytes[i] {
            b'\n' => {
                line += 1;
                line_start = i + 1;
            }
            // A lone `\r` (classic-Mac line ending) is its own line break,
            // matching the lexer and `diagnostic::line_col`. A `\r` that is the
            // first half of a CRLF pair is NOT counted here — the break is
            // attributed to the following `\n`, and the trailing `\r` is
            // stripped from the column count below.
            b'\r' if bytes.get(i + 1) != Some(&b'\n') => {
                line += 1;
                line_start = i + 1;
            }
            _ => {}
        }
    }
    let mut safe_offset = clamped;
    while safe_offset > line_start && !source.is_char_boundary(safe_offset) {
        safe_offset -= 1;
    }
    // \r immediately before \n is part of the CRLF line break (LSP spec
    // says it counts as one character together), so strip it from the
    // column count — but only when it actually terminates the line, i.e.
    // the byte at `safe_offset` is the '\n' of this CRLF pair. A stray \r
    // in the middle of a line is an ordinary character and must count as
    // a column, matching `position_to_offset`, which only strips the
    // line-*trailing* \r.
    let line_slice = &source[line_start..safe_offset];
    let line_slice = if bytes.get(safe_offset) == Some(&b'\n') {
        line_slice.strip_suffix('\r').unwrap_or(line_slice)
    } else {
        line_slice
    };
    let character: u32 = line_slice.chars().map(|c| c.len_utf16() as u32).sum();
    Position::new(line, character)
}

pub fn position_to_offset(source: &str, pos: Position) -> usize {
    let bytes = source.as_bytes();
    // Advance to the byte where the target line begins, treating `\n`, a lone
    // `\r`, and `\r\n` each as a single line break — matching the lexer and
    // `offset_to_position` (a `split('\n')` here would miss `\r`-only endings).
    let mut line: u32 = 0;
    let mut i = 0;
    let mut line_start = 0;
    while i < bytes.len() && line < pos.line {
        match bytes[i] {
            b'\n' => {
                i += 1;
                line += 1;
                line_start = i;
            }
            b'\r' => {
                i += if bytes.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
                line += 1;
                line_start = i;
            }
            _ => i += 1,
        }
    }
    if line < pos.line {
        // The requested line is past the end of the document.
        return source.len();
    }
    // Walk within the line up to `pos.character` (UTF-16 code units), stopping
    // at the line terminator (`\n`, or a `\r` beginning a lone-CR or CRLF break).
    let mut utf16_count: u32 = 0;
    let mut byte_pos = line_start;
    while byte_pos < bytes.len() {
        let b = bytes[byte_pos];
        if b == b'\n' || b == b'\r' {
            break;
        }
        if utf16_count >= pos.character {
            break;
        }
        // Decode one full character starting at the current byte.
        let ch = source[byte_pos..].chars().next().unwrap();
        utf16_count += ch.len_utf16() as u32;
        byte_pos += ch.len_utf8();
    }
    byte_pos
}

pub fn word_at_position(source: &str, pos: Position) -> Option<&str> {
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
    // `'` is an identifier-continue character in the lexer (`x'` is one
    // identifier), so word boundaries must treat it the same — otherwise
    // rename/hover on `x'` resolve only `x` and corrupt primed identifiers.
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';

    // The caret immediately after the last char of an identifier (the
    // standard post-typing cursor position) should still resolve that
    // identifier: when the byte at `offset` isn't an ident byte (or we're
    // at EOF) but the byte before it is, fall back to the word ending at
    // `offset`.
    let offset = if offset < bytes.len() && is_ident(bytes[offset]) {
        offset
    } else if offset > 0 && offset <= bytes.len() && is_ident(bytes[offset - 1]) {
        offset - 1
    } else {
        return None;
    };

    let mut start = (0..offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);
    // `'` cannot START an identifier — skip any leading primes (e.g. when
    // the scan landed inside a string literal like "don't").
    while start < bytes.len() && bytes[start] == b'\'' && start <= offset {
        start += 1;
    }

    let end = (offset.max(start)..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    if start >= end {
        return None;
    }
    Some((start, end))
}

/// Effective cursor offset for identifier span-containment lookups
/// (`usage.start <= offset && offset < usage.end`). When the caret sits
/// immediately after the last char of an identifier, nudge it back inside
/// the word so position-keyed resolution (rename/references/highlight)
/// matches the same identifier `word_at_position` reports.
pub fn ident_lookup_offset(source: &str, offset: usize) -> usize {
    match word_bounds_at_offset(source, offset) {
        Some((start, end)) if end > start => offset.clamp(start, end - 1),
        _ => offset,
    }
}

/// Slice `source` by `span` without panicking on stale or out-of-bounds spans.
///
/// Spans recorded during a previous analysis can outlive the source they
/// point into when an edit truncates the document. Clamping both endpoints
/// (and snapping to char boundaries) keeps such spans safe to read instead
/// of taking down the LSP.
pub fn safe_slice(source: &str, span: Span) -> &str {
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
    // DP-table init: the index is both the row/column position and the value.
    #[allow(clippy::needless_range_loop)]
    for i in 0..=a.len() {
        dp[i][0] = i;
    }
    #[allow(clippy::needless_range_loop)]
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
    // `'` continues identifiers in the lexer (`x'`), so it's a word char
    // here too — `x` must not whole-word-match the prefix of `x'`.
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';

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

/// Find a whole-word `name` in the range whose nearest preceding
/// non-whitespace character is a single `=` (assignment), advancing past any
/// earlier occurrences that aren't in that position. Used to locate a route
/// endpoint constructor's definition site (`… -> Response = GetUsers`), which
/// is spanless in the AST, without matching an identically-named response type
/// or path segment earlier in the same entry. Rejects `==`/`>=`/`<=`/`!=` so a
/// comparison operator is never mistaken for assignment.
pub fn find_word_after_eq(source: &str, name: &str, start: usize, end: usize) -> Option<Span> {
    let bytes = source.as_bytes();
    let mut from = start;
    while let Some(span) = find_word_in_source(source, name, from, end) {
        from = span.end;
        // Walk back over whitespace to the char preceding the name.
        let mut i = span.start;
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        if i > 0 && bytes[i - 1] == b'=' {
            // Ensure it's a lone `=`, not part of `==`/`>=`/`<=`/`!=`.
            let prev = if i >= 2 { Some(bytes[i - 2]) } else { None };
            if !matches!(prev, Some(b'=' | b'>' | b'<' | b'!')) {
                return Some(span);
            }
        }
    }
    None
}

/// Like [`find_word_in_source`] but returns the *last* whole-word match in the
/// range. Useful when a name's true site is the one closest to the end of the
/// window — e.g. a route field/param declaration `name: Type`, where the name
/// sits immediately before its type, but an identical word (a path literal like
/// `/name`) may appear earlier in the same window and must not be chosen.
pub fn find_word_last_in_source(source: &str, name: &str, start: usize, end: usize) -> Option<Span> {
    let mut last = None;
    let mut from = start;
    while let Some(span) = find_word_in_source(source, name, from, end) {
        from = span.end;
        last = Some(span);
    }
    last
}

// ── Doc comments ────────────────────────────────────────────────────

/// Extract doc comments (lines starting with `-- `) above each declaration.
pub fn extract_doc_comments(source: &str, program: &Expr) -> HashMap<String, String> {
    let mut comments = HashMap::new();
    let lines: Vec<&str> = source.split('\n').collect();

    for decl in top_fields(program) {
        let name = match &decl.value.node {
            ExprKind::SubsetConstraint { .. } => continue,
            _ => decl.name.clone(),
        };

        let decl_line = offset_to_position(source, decl.value.span.start).line as usize;
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
            TokenKind::Data
            | TokenKind::Type
            | TokenKind::Route
            | TokenKind::Migrate
            | TokenKind::Where
            | TokenKind::Do
            | TokenKind::If
            | TokenKind::Then
            | TokenKind::Else
            | TokenKind::Case
            | TokenKind::Of
            | TokenKind::Not
            | TokenKind::Replace
            | TokenKind::Atomic
            | TokenKind::Deriving
            | TokenKind::With
            | TokenKind::Refine
            | TokenKind::Serve
            | TokenKind::Forall => Some(TOK_KEYWORD),
            TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::Percent
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
/// The top-level record fields of the program (its declarations). The file
/// is a single expression — usually a `with`-record or a record literal —
/// whose fields are the declarations.
pub fn top_fields(program: &Expr) -> Vec<&RecordField> {
    match &program.node {
        ExprKind::Record(fields) => fields.iter().collect(),
        ExprKind::With { record, .. } => match &record.node {
            ExprKind::Record(fields) => fields.iter().collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

pub fn recurse_expr<F: FnMut(&ast::Expr)>(expr: &ast::Expr, mut f: F) {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            f(func);
            f(arg);
        }
        ast::ExprKind::With { record, body } => {
            f(record);
            f(body);
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
        ast::ExprKind::TimeUnitLit { value, .. } => f(value),
        ast::ExprKind::Serve { handlers, .. } => {
            // `api` is a Name (no sub-expression); handler bodies are the
            // only expression children.
            for h in handlers {
                f(&h.body);
            }
        }
        // Leaves: Lit, Var, Constructor, SourceRef, DerivedRef, TypeCtor.
        ast::ExprKind::ImplicitRef(_) => {}
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Var(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_)
        | ast::ExprKind::TypeCtor { .. }
        | ast::ExprKind::DataCtor { .. }
        | ast::ExprKind::SourceDecl { .. }
        | ast::ExprKind::SubsetConstraint { .. }
        | ast::ExprKind::RouteDecl { .. }
        | ast::ExprKind::RouteCompositeDecl { .. } => {}
        ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => f(body),
    }
}

// ── Tests ───────────────────────────────────────────────────────────




