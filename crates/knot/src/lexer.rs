//! Lexer for the Knot language.
//!
//! Converts source text into a flat sequence of [`Token`]s, collapsing
//! consecutive newlines and reporting unknown characters as diagnostics.

use crate::ast::Span;
use crate::diagnostic::Diagnostic;

// ── Token types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Literals
    Int(String),
    Float(f64),
    Text(String),

    // Identifiers
    Lower(String),
    Upper(String),

    // Keywords
    Serve,
    Where,
    Do,
    Match,
    Not,
    Full,
    Atomic,
    With,
    Refine,
    Forall,

    // Delimiters
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,

    // Operators
    Eq,
    EqEq,
    BangEq,
    Lt,
    Gt,
    Le,
    Ge,
    Plus,
    Minus,
    Star,
    /// `*name` — a source-relation identifier: `*` immediately followed by a
    /// lowercase letter, lexed as ONE token whose string INCLUDES the leading
    /// `*` (e.g. `"*todos"`). Distinguished from the `Star` multiplication
    /// operator by the no-space rule: binary operators require surrounding
    /// whitespace, so a `*` directly abutting a lowercase letter is always a
    /// source identifier, never multiplication.
    StarIdent(String),
    Slash,
    Percent,
    PlusPlus,
    AndAnd,
    OrOr,
    PipeGt,
    Caret,
    Collect,

    // Arrows
    Arrow,
    FatArrow,
    LArrow,

    // Punctuation
    Dot,
    Comma,
    Colon,
    Pipe,
    Backslash,
    Ampersand,
    At,
    Underscore,
    Question,
    Newline,
    /// Gap: a run of two or more spaces/tabs within a line (a newline is
    /// `Newline`, separately). This is the type-annotation separator —
    /// `Type  name` (a sig) vs `name value` (a binding, single spaces).
    /// Single whitespace collapses to nothing; a gap is significant.
    Gap,
    Semicolon,
    Eof,

    /// `---` documentation comment. Carries the markdown text of one doc line
    /// (`--- text`) or one whole doc block (`---`-only open … `---`-only
    /// close). Attached by the parser to the immediately-following declaration.
    Doc(String),
}

impl TokenKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            TokenKind::Int(_) => "integer literal",
            TokenKind::Float(_) => "float literal",
            TokenKind::Text(_) => "string literal",
            TokenKind::Lower(_) => "identifier",
            TokenKind::Upper(_) => "type name",
            TokenKind::Serve => "'serve'",
            TokenKind::Where => "'where'",
            TokenKind::Do => "'do'",
            TokenKind::Match => "'match'",
            TokenKind::Not => "'not'",
            TokenKind::Full => "'full'",
            TokenKind::Atomic => "'atomic'",
            TokenKind::With => "'with'",
            TokenKind::Refine => "'refine'",
            TokenKind::Forall => "'forall'",
            TokenKind::LParen => "'('",
            TokenKind::RParen => "')'",
            TokenKind::LBrace => "'{'",
            TokenKind::RBrace => "'}'",
            TokenKind::LBracket => "'['",
            TokenKind::RBracket => "']'",
            TokenKind::Eq => "'='",
            TokenKind::EqEq => "'=='",
            TokenKind::BangEq => "'!='",
            TokenKind::Lt => "'<'",
            TokenKind::Gt => "'>'",
            TokenKind::Le => "'<='",
            TokenKind::Ge => "'>='",
            TokenKind::Plus => "'+'",
            TokenKind::Minus => "'-'",
            TokenKind::Star => "'*'",
            TokenKind::StarIdent(_) => "source identifier",
            TokenKind::Slash => "'/'",
            TokenKind::Percent => "'%'",
            TokenKind::PlusPlus => "'++'",
            TokenKind::AndAnd => "'&&'",
            TokenKind::OrOr => "'||'",
            TokenKind::PipeGt => "'|>'",
            TokenKind::Caret => "'^'",
            TokenKind::Collect => "'<>'",
            TokenKind::Arrow => "'->'",
            TokenKind::FatArrow => "'=>'",
            TokenKind::LArrow => "'<-'",
            TokenKind::Dot => "'.'",
            TokenKind::Comma => "','",
            TokenKind::Colon => "':'",
            TokenKind::Pipe => "'|'",
            TokenKind::Backslash => "'\\'",
            TokenKind::Ampersand => "'&'",
            TokenKind::At => "'@'",
            TokenKind::Underscore => "'_'",
            TokenKind::Question => "'?'",
            TokenKind::Newline => "newline",
            TokenKind::Gap => "gap",
            TokenKind::Semicolon => "';'",
            TokenKind::Eof => "end of file",
            TokenKind::Doc(_) => "documentation comment",
        }
    }

    /// If this token is a keyword, return its string representation.
    pub fn keyword_str(&self) -> Option<&'static str> {
        match self {
            TokenKind::Serve => Some("serve"),
            TokenKind::Where => Some("where"),
            TokenKind::Do => Some("do"),
            TokenKind::Match => Some("match"),
            TokenKind::Not => Some("not"),
            TokenKind::Full => Some("full"),
            TokenKind::Atomic => Some("atomic"),
            TokenKind::With => Some("with"),
            TokenKind::Refine => Some("refine"),
            TokenKind::Forall => Some("forall"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// Digits of `i64::MIN` without its sign — the one magnitude that is only
/// valid under a prefix `-`.
const I64_MIN_MAGNITUDE: &str = "9223372036854775808";

/// Whether the token just before the one being lexed is a prefix (unary) `-`.
/// A `-` is prefix unless the token before it can end an expression, in which
/// case it is binary subtraction.
fn follows_prefix_minus(prev: &[Token]) -> bool {
    let mut back = prev.iter().rev().map(|t| &t.kind);
    if !matches!(back.next(), Some(TokenKind::Minus)) {
        return false;
    }
    !matches!(
        back.next(),
        Some(
            TokenKind::Int(_)
                | TokenKind::Float(_)
                | TokenKind::Text(_)
                | TokenKind::Lower(_)
                | TokenKind::Upper(_)
                | TokenKind::RParen
                | TokenKind::RBracket
                | TokenKind::RBrace
        )
    )
}

// ── Lexer ───────────────────────────────────────────────────────────

pub struct Lexer<'src> {
    source: &'src str,
    bytes: &'src [u8],
    pos: usize,
    diagnostics: Vec<Diagnostic>,
}

impl<'src> Lexer<'src> {
    pub fn new(source: &'src str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            pos: 0,
            diagnostics: Vec::new(),
        }
    }

    pub fn tokenize(mut self) -> (Vec<Token>, Vec<Diagnostic>) {
        let mut tokens = Vec::new();
        let mut last_was_newline = true; // suppress leading newlines

        // Skip a leading UTF-8 BOM (0xEF 0xBB 0xBF) if present — some Windows
        // editors prepend it, and without this check the BOM bytes would
        // produce a spurious "unexpected character" diagnostic.
        if self.bytes.starts_with(b"\xEF\xBB\xBF") {
            self.pos += 3;
        }

        // Doc-block toggle: a line holding ONLY a bare `---` (no text) opens or
        // closes a documentation block. `--` is always a line comment — a bare
        // `--` is just an empty comment line, never a block toggle.
        let mut in_doc_block = false;

        loop {
            // Whitespace: a run of 2+ spaces/tabs within a line is a gap — the
            // type-annotation separator (`Type  name`). A single space/tab is
            // insignificant and collapses away. Leading indentation (a gap right
            // after a newline / at file start) is layout, not an annotation
            // separator, so it is not emitted.
            {
                let ws_start = self.pos;
                let mut run = 0usize;
                while matches!(self.peek(), Some(b' ') | Some(b'\t')) {
                    self.advance();
                    run += 1;
                }
                if run >= 2 && !last_was_newline && !tokens.is_empty() {
                    tokens.push(Token {
                        kind: TokenKind::Gap,
                        span: self.span_from(ws_start),
                    });
                }
            }

            // Comments: `--` line, `---` doc line/block.
            if self.check(b'-') && self.peek_at(1) == Some(b'-') {
                let is_doc = self.peek_at(2) == Some(b'-');
                let marker_len = if is_doc { 3 } else { 2 };
                let after = self.pos + marker_len;
                // A bare `---` (only whitespace then a line end / EOF) toggles a
                // doc block. A bare `--` is just an empty line comment.
                if is_doc && self.only_blank_until_line_end(after) {
                    in_doc_block = !in_doc_block;
                    self.skip_line_comment();
                    continue;
                }
                if is_doc {
                    // `--- text`: a one-line doc comment; emit a Doc token.
                    let start = self.pos;
                    let text = self.take_line_comment_text(marker_len);
                    tokens.push(Token {
                        kind: TokenKind::Doc(text),
                        span: self.span_from(start),
                    });
                    continue;
                }
                // `-- text` or a bare `--`: an ordinary line comment — skip it.
                self.skip_line_comment();
                continue;
            }

            // Inside a doc block, accumulate lines into a single Doc token until
            // the closing bare `---` line.
            if in_doc_block {
                if self.at_end() {
                    self.diagnostics.push(
                        Diagnostic::error("unterminated documentation block").label(
                            self.span_from(self.pos),
                            "doc block opened here never closed",
                        ),
                    );
                    break;
                }
                let block_start = self.pos;
                let mut text = String::new();
                loop {
                    self.skip_whitespace();
                    if self.check(b'-')
                        && self.peek_at(1) == Some(b'-')
                        && self.peek_at(2) == Some(b'-')
                        && self.only_blank_until_line_end(self.pos + 3)
                    {
                        in_doc_block = false;
                        self.skip_line_comment();
                        break;
                    }
                    if self.at_end() {
                        in_doc_block = false;
                        break;
                    }
                    let line = self.take_line_comment_text(0);
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(&line);
                    // consume the newline (or EOF)
                    if matches!(self.peek(), Some(b'\n') | Some(b'\r')) {
                        self.advance();
                    }
                }
                let trimmed = text.trim_end().to_string();
                if !trimmed.is_empty() {
                    tokens.push(Token {
                        kind: TokenKind::Doc(trimmed),
                        span: self.span_from(block_start),
                    });
                }
                continue;
            }

            if self.at_end() {
                tokens.push(Token {
                    kind: TokenKind::Eof,
                    span: self.span_from(self.pos),
                });
                break;
            }

            let ch = self.bytes[self.pos];

            // Newlines — collapse consecutive, suppress leading. Accept `\n`,
            // `\r`, and `\r\n` so classic-Mac (`\r`-only) and Windows (`\r\n`)
            // line endings also produce layout newlines. (`\r` was previously
            // eaten by `skip_whitespace`, so `\r`-only files collapsed into a
            // single logical line and failed layout-sensitive parsing.)
            if ch == b'\n' || ch == b'\r' {
                if !last_was_newline {
                    let start = self.pos;
                    self.advance();
                    while matches!(self.peek(), Some(b'\n') | Some(b'\r')) {
                        self.advance();
                    }
                    tokens.push(Token {
                        kind: TokenKind::Newline,
                        span: self.span_from(start),
                    });
                    last_was_newline = true;
                } else {
                    self.advance();
                }
                continue;
            }

            let start = self.pos;
            let kind = self.lex_token();
            let span = self.span_from(start);

            if let Some(kind) = kind {
                if let TokenKind::Int(raw) = &kind {
                    let raw = raw.clone();
                    self.check_int_range(&raw, span, &tokens);
                }
                last_was_newline = matches!(kind, TokenKind::Newline);
                tokens.push(Token { kind, span });
            } else {
                // Unknown character was skipped — reset newline flag so
                // a subsequent newline is not incorrectly suppressed.
                last_was_newline = false;
            }
        }

        (tokens, self.diagnostics)
    }

    // ── Core helpers ────────────────────────────────────────────────

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn check(&self, expected: u8) -> bool {
        self.peek() == Some(expected)
    }

    fn advance(&mut self) -> u8 {
        let b = self.bytes[self.pos];
        self.pos += 1;
        b
    }

    fn eat(&mut self, expected: u8) -> bool {
        if self.check(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn span_from(&self, start: usize) -> Span {
        Span::new(start, self.pos)
    }

    fn slice(&self, start: usize, end: usize) -> &'src str {
        &self.source[start..end]
    }

    // ── Whitespace / comments ───────────────────────────────────────

    fn skip_whitespace(&mut self) {
        while let Some(b) = self.peek() {
            // Note: `\r` is intentionally *not* skipped here — it is handled by
            // the newline logic in `tokenize` so lone-`\r` line endings produce
            // layout newlines.
            if b == b' ' || b == b'\t' {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        // Skip the `--`
        self.advance();
        self.advance();
        // Skip until newline (leave the newline for the main loop). Treat both
        // `\n` and `\r` as line terminators so comments end on any line ending.
        while let Some(b) = self.peek() {
            if b == b'\n' || b == b'\r' {
                break;
            }
            self.advance();
        }
    }

    /// Whether the bytes from `pos` to the end of the current line are only
    /// spaces/tabs (i.e. the marker at `pos` is bare — nothing but blank space
    /// follows it before the line ends or the file does). Used to distinguish a
    /// block-comment toggle (`--` alone) from a line comment (`-- text`).
    fn only_blank_until_line_end(&self, pos: usize) -> bool {
        let mut i = pos;
        while i < self.bytes.len() {
            match self.bytes[i] {
                b' ' | b'\t' => i += 1,
                b'\n' | b'\r' => return true,
                _ => return false,
            }
        }
        true
    }

    /// Consume the rest of the current line starting `marker_len` bytes ahead
    /// (skipping one leading space after the marker) and return its text with
    /// surrounding whitespace trimmed. Leaves the terminating newline for the
    /// main loop. Used to capture doc-comment / doc-block line contents.
    fn take_line_comment_text(&mut self, marker_len: usize) -> String {
        for _ in 0..marker_len {
            self.advance();
        }
        // Skip a single leading space after the marker (markdown body starts
        // after `-- ` / `--- `), but preserve deeper indentation.
        if self.peek() == Some(b' ') {
            self.advance();
        }
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b'\n' || b == b'\r' {
                break;
            }
            self.advance();
        }
        self.source[start..self.pos].trim().to_string()
    }

    // ── Main dispatch ───────────────────────────────────────────────

    fn lex_token(&mut self) -> Option<TokenKind> {
        let ch = self.bytes[self.pos];

        // Identifiers and keywords. Accept non-ASCII Unicode letters as an
        // identifier start (`café`, `α`) too — the byte-level gate uses the
        // lead byte, and the full multi-byte character is then consumed by
        // `is_ident_continue`.
        if ch.is_ascii_alphabetic() || (ch >= 0x80 && char::from(ch).is_alphabetic()) || ch == b'_'
        {
            return Some(self.lex_identifier());
        }

        // Numbers
        if ch.is_ascii_digit() {
            return Some(self.lex_number());
        }

        // Strings
        if ch == b'"' {
            return Some(self.lex_string());
        }

        // Operators and punctuation
        self.lex_operator()
    }

    // ── Identifiers & keywords ──────────────────────────────────────

    fn lex_identifier(&mut self) -> TokenKind {
        let start = self.pos;
        let first = self.advance();

        // `_` alone is Underscore
        if first == b'_' && !self.is_ident_continue() {
            return TokenKind::Underscore;
        }

        while self.is_ident_continue() {
            self.advance();
        }

        let text = self.slice(start, self.pos);

        // Keywords (only lowercase identifiers can be keywords)
        if first.is_ascii_lowercase() || first == b'_' {
            match text {
                "serve" => return TokenKind::Serve,
                "where" => return TokenKind::Where,
                "do" => return TokenKind::Do,
                "match" => return TokenKind::Match,
                "not" => return TokenKind::Not,
                "full" => return TokenKind::Full,
                "atomic" => return TokenKind::Atomic,
                "with" => return TokenKind::With,
                "refine" => return TokenKind::Refine,
                "forall" => return TokenKind::Forall,
                _ => {}
            }
            TokenKind::Lower(text.to_owned())
        } else {
            TokenKind::Upper(text.to_owned())
        }
    }

    fn is_ident_continue(&self) -> bool {
        // Any non-ASCII byte (`b >= 0x80`) continues an identifier: this covers
        // every byte of a multi-byte UTF-8 letter — both the lead byte and its
        // continuation bytes, which individually are not alphanumeric — so a
        // Unicode identifier is consumed whole and `slice` stays on a char
        // boundary. `'` continues identifiers too (`x'`).
        matches!(
            self.peek(),
            Some(b) if b.is_ascii_alphanumeric() || b == b'_' || b == b'\'' || b >= 0x80
        )
    }

    // ── Numbers ─────────────────────────────────────────────────────

    /// Emit a diagnostic if a `_` digit separator is not flanked by digits on
    /// both sides (leading, trailing, doubled, or `_`-adjacent-to-`.`). The
    /// caller still strips underscores for the value, so recovery is exact —
    /// this only surfaces the malformed source. No-op when there are no `_`.
    fn check_digit_separators(&mut self, slice: &str, start: usize) {
        let bytes = slice.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'_' {
                let prev_ok = i > 0 && bytes[i - 1].is_ascii_digit();
                let next_ok = i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit();
                if !prev_ok || !next_ok {
                    let span = Span::new(start + i, start + i + 1);
                    self.diagnostics
                        .push(Diagnostic::error("misplaced digit separator").label(
                            span,
                            "`_` in a numeric literal must appear between two digits",
                        ));
                    return;
                }
            }
        }
    }

    /// Reject integer literals that do not fit in an `i64`.
    ///
    /// `i64::MIN` has no positive counterpart, so its magnitude
    /// (`9223372036854775808`) is accepted when it sits directly under a
    /// prefix `-`: the parser folds the two tokens into a single negative
    /// literal. Anywhere else — including as the right operand of a binary
    /// `-` — that magnitude overflows and is reported.
    fn check_int_range(&mut self, raw: &str, span: Span, prev: &[Token]) {
        if raw.parse::<i64>().is_ok() {
            return;
        }
        let magnitude = raw.trim_start_matches('0');
        if magnitude == I64_MIN_MAGNITUDE && follows_prefix_minus(prev) {
            return;
        }
        self.diagnostics
            .push(Diagnostic::error("integer literal is out of range").label(
                span,
                "does not fit in a 64-bit signed integer \
                 (-9223372036854775808 to 9223372036854775807)",
            ));
    }

    fn lex_number(&mut self) -> TokenKind {
        let start = self.pos;

        // Consume integer part (digits and underscores)
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() || b == b'_' {
                self.advance();
            } else {
                break;
            }
        }

        let mut is_float = false;

        // Check for fractional part: `.` followed by a digit
        if self.peek() == Some(b'.') && matches!(self.peek_at(1), Some(b'0'..=b'9')) {
            is_float = true;
            self.advance(); // consume '.'
            while let Some(b) = self.peek() {
                if b.is_ascii_digit() || b == b'_' {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // Check for exponent: `e`/`E`, optional `+`/`-`, at least one digit.
        // Scientific notation always produces a float literal.
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            let exp_digit_pos = match self.peek_at(1) {
                Some(b'+') | Some(b'-') => 2,
                _ => 1,
            };
            if matches!(self.peek_at(exp_digit_pos), Some(b'0'..=b'9')) {
                is_float = true;
                self.advance(); // consume 'e'/'E'
                if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                    self.advance(); // consume sign
                }
                while let Some(b) = self.peek() {
                    if b.is_ascii_digit() || b == b'_' {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
        }

        let slice = self.slice(start, self.pos);
        let raw = if slice.contains('_') {
            self.check_digit_separators(slice, start);
            slice.replace('_', "")
        } else {
            slice.to_string()
        };

        if is_float {
            let value = match raw.parse::<f64>() {
                Ok(v) if !v.is_finite() => {
                    // `parse::<f64>` saturates oversized literals to infinity;
                    // there is no literal syntax for non-finite floats, so
                    // report the overflow and recover with the largest finite
                    // value (keeps downstream output parseable).
                    let span = self.span_from(start);
                    self.diagnostics.push(
                        Diagnostic::error("float literal is too large")
                            .label(span, "overflows the range of a 64-bit float"),
                    );
                    f64::MAX
                }
                Ok(v) => v,
                Err(_) => {
                    let span = self.span_from(start);
                    self.diagnostics.push(
                        Diagnostic::error("invalid float literal")
                            .label(span, "could not parse as a floating-point number"),
                    );
                    0.0
                }
            };
            TokenKind::Float(value)
        } else {
            TokenKind::Int(raw)
        }
    }

    // ── Strings ─────────────────────────────────────────────────────

    fn lex_string(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // opening `"`

        let mut value = String::new();

        loop {
            match self.peek() {
                None | Some(b'\n') | Some(b'\r') => {
                    // Unterminated string. CR (alone or as part of CRLF) is
                    // also a line break — without this branch a CRLF inside
                    // an unterminated string would embed the CR in the value
                    // before the LF tripped the diagnostic.
                    let span = self.span_from(start);
                    self.diagnostics.push(
                        Diagnostic::error("unterminated string literal")
                            .label(span, "string starts here"),
                    );
                    return TokenKind::Text(value);
                }
                Some(b'"') => {
                    self.advance(); // closing `"`
                    return TokenKind::Text(value);
                }
                Some(b'\\') => {
                    self.advance(); // consume `\`
                    match self.peek() {
                        Some(b'\\') => {
                            self.advance();
                            value.push('\\');
                        }
                        Some(b'"') => {
                            self.advance();
                            value.push('"');
                        }
                        Some(b'n') => {
                            self.advance();
                            value.push('\n');
                        }
                        Some(b't') => {
                            self.advance();
                            value.push('\t');
                        }
                        Some(b'r') => {
                            self.advance();
                            value.push('\r');
                        }
                        Some(b'0') => {
                            // Match the byte-string lexer's vocabulary so
                            // `\0` works in both literal kinds.
                            self.advance();
                            value.push('\0');
                        }
                        Some(b'x') => {
                            self.advance();
                            // Hex escape: \xHH — mirrors the byte-string
                            // lexer so control characters round-trip through
                            // the formatter. The value is the Unicode code
                            // point U+00HH (Latin-1 mapping).
                            let h1 = self.peek().and_then(|b| (b as char).to_digit(16));
                            if let Some(d1) = h1 {
                                let first_hex_byte = self.bytes[self.pos];
                                self.advance();
                                let h2 = self.peek().and_then(|b| (b as char).to_digit(16));
                                if let Some(d2) = h2 {
                                    self.advance();
                                    value.push(((d1 * 16 + d2) as u8) as char);
                                } else {
                                    let span = Span::new(self.pos - 3, self.pos);
                                    self.diagnostics.push(
                                        Diagnostic::error("invalid hex escape in string")
                                            .label(span, "expected two hex digits after \\x"),
                                    );
                                    // Error recovery: emit the literal hex char
                                    // (e.g. '5' for `\x5`) rather than the digit
                                    // *value* so the recovered text resembles
                                    // what the user typed.
                                    value.push(first_hex_byte as char);
                                }
                            } else {
                                // Span only the `\x`, not a trailing terminator
                                // (`"`/newline/EOF) — recovery below never
                                // consumes those, so they must not be underlined.
                                let bad_end = match self.peek() {
                                    Some(b'"') | Some(b'\n') | Some(b'\r') | None => self.pos,
                                    Some(_) => {
                                        self.pos
                                            + self.source[self.pos..]
                                                .chars()
                                                .next()
                                                .map_or(1, |c| c.len_utf8())
                                    }
                                };
                                let span = Span::new(self.pos - 2, bad_end);
                                self.diagnostics.push(
                                    Diagnostic::error("invalid hex escape in string")
                                        .label(span, "expected two hex digits after \\x"),
                                );
                                // Error recovery: emit the bad character as a
                                // literal so the string isn't silently
                                // shortened — but never consume a closing `"`
                                // or a line break; those must terminate the
                                // string via the normal branches.
                                match self.peek() {
                                    Some(b'"') | Some(b'\n') | Some(b'\r') | None => {}
                                    Some(_) => {
                                        let ch = self.source[self.pos..].chars().next().unwrap();
                                        self.pos += ch.len_utf8();
                                        value.push(ch);
                                    }
                                }
                            }
                        }
                        Some(b'\n') | Some(b'\r') => {
                            // Backslash at end of line — never consume the
                            // line break as an "unknown escape" (that would
                            // swallow the whole next line into the string).
                            // Leave it for the unterminated-string branch.
                        }
                        Some(_) => {
                            let esc_start = self.pos - 1;
                            // Advance by one full UTF-8 character (not just one byte)
                            let ch = self.source[self.pos..].chars().next().unwrap();
                            self.pos += ch.len_utf8();
                            let span = Span::new(esc_start, self.pos);
                            self.diagnostics.push(
                                Diagnostic::error("unknown escape sequence")
                                    .label(span, "unknown escape"),
                            );
                            // Error recovery: emit the escaped character so the
                            // string value isn't silently shortened.
                            value.push(ch);
                        }
                        None => {
                            // Backslash at EOF — caught as unterminated
                            // on the next iteration.
                        }
                    }
                }
                Some(_) => {
                    // Normal character — advance by one full char (UTF-8 safe).
                    let ch = self.source[self.pos..].chars().next().unwrap();
                    self.pos += ch.len_utf8();
                    value.push(ch);
                }
            }
        }
    }

    // ── Operators & punctuation ─────────────────────────────────────

    fn lex_operator(&mut self) -> Option<TokenKind> {
        let ch = self.advance();

        let kind = match ch {
            b'<' => {
                if self.eat(b'>') {
                    TokenKind::Collect
                } else if self.eat(b'-') {
                    TokenKind::LArrow
                } else if self.eat(b'=') {
                    TokenKind::Le
                } else {
                    TokenKind::Lt
                }
            }
            b'-' => {
                if self.eat(b'>') {
                    TokenKind::Arrow
                } else {
                    TokenKind::Minus
                }
            }
            b'>' => {
                if self.eat(b'=') {
                    TokenKind::Ge
                } else {
                    TokenKind::Gt
                }
            }
            b'=' => {
                if self.eat(b'>') {
                    TokenKind::FatArrow
                } else if self.eat(b'=') {
                    TokenKind::EqEq
                } else {
                    TokenKind::Eq
                }
            }
            b'!' => {
                if self.eat(b'=') {
                    TokenKind::BangEq
                } else {
                    let span = Span::new(self.pos - 1, self.pos);
                    self.diagnostics.push(
                        Diagnostic::error("unexpected character '!'").label(span, "unexpected"),
                    );
                    return None;
                }
            }
            b'+' => {
                if self.eat(b'+') {
                    TokenKind::PlusPlus
                } else {
                    TokenKind::Plus
                }
            }
            b'*' => {
                // `*name` (no space, lowercase letter immediately after) is a
                // source-relation identifier — a single StarIdent token whose
                // string includes the `*`. Binary operators require surrounding
                // whitespace, so a spaced `*` (or `*` before a non-letter, e.g.
                // `x * 2`, `a * (b)`) stays the multiplication operator. The
                // `*` was already consumed by `lex_operator`'s `advance()`, so
                // `start` is one byte back and `peek()` is the char after `*`.
                if matches!(self.peek(), Some(b) if b.is_ascii_lowercase()) {
                    let start = self.pos - 1; // include the consumed `*`
                    while self.is_ident_continue() {
                        self.advance();
                    }
                    TokenKind::StarIdent(self.slice(start, self.pos).to_owned())
                } else {
                    TokenKind::Star
                }
            }
            b'/' => TokenKind::Slash,
            b'%' => TokenKind::Percent,
            b'&' => {
                if self.eat(b'&') {
                    TokenKind::AndAnd
                } else {
                    TokenKind::Ampersand
                }
            }
            b'|' => {
                if self.eat(b'|') {
                    TokenKind::OrOr
                } else if self.eat(b'>') {
                    TokenKind::PipeGt
                } else {
                    TokenKind::Pipe
                }
            }
            b'.' => TokenKind::Dot,
            b',' => TokenKind::Comma,
            b':' => TokenKind::Colon,
            b'\\' => TokenKind::Backslash,
            b'@' => TokenKind::At,
            b';' => TokenKind::Semicolon,
            b'?' => TokenKind::Question,
            b'^' => TokenKind::Caret,
            b'(' => TokenKind::LParen,
            b')' => TokenKind::RParen,
            b'{' => TokenKind::LBrace,
            b'}' => TokenKind::RBrace,
            b'[' => TokenKind::LBracket,
            b']' => TokenKind::RBracket,
            _ => {
                let char_start = self.pos - 1;
                // Skip remaining bytes of multi-byte UTF-8 character
                while self.pos < self.bytes.len() && (self.bytes[self.pos] & 0xC0) == 0x80 {
                    self.pos += 1;
                }
                let span = Span::new(char_start, self.pos);
                let c = self.source[char_start..self.pos]
                    .chars()
                    .next()
                    .unwrap_or('?');
                self.diagnostics.push(
                    Diagnostic::error(format!("unexpected character '{c}'"))
                        .label(span, "unexpected"),
                );
                return None;
            }
        };

        Some(kind)
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        Lexer::new(src)
            .tokenize()
            .0
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    fn has_doc(kinds: &[TokenKind], needle: &str) -> bool {
        kinds
            .iter()
            .any(|k| matches!(k, TokenKind::Doc(d) if d.contains(needle)))
    }

    #[test]
    fn line_comment_skipped() {
        let k = kinds("x -- a comment\ny");
        assert!(k.iter().all(|t| !matches!(t, TokenKind::Doc(_))));
        assert!(
            kinds("x -- a comment\ny")
                .iter()
                .any(|t| matches!(t, TokenKind::Lower(n) if n == "y"))
        );
    }

    #[test]
    fn doc_line_comment() {
        let k = kinds("--- adds two numbers\nadd");
        assert!(has_doc(&k, "adds two numbers"));
    }

    #[test]
    fn doc_block_toggle() {
        let k = kinds("---\n# Title\n\nSome *markdown* docs.\n---\nadd");
        assert!(has_doc(&k, "# Title"));
        assert!(has_doc(&k, "Some *markdown* docs."));
    }

    #[test]
    fn bare_dashes_are_empty_line_comments() {
        // A bare `--` is an empty line comment, NOT a block-comment toggle —
        // content on the following lines is still lexed.
        let k = kinds("a\n--\nb\n--\nc");
        for name in ["a", "b", "c"] {
            assert!(
                k.iter().any(|t| matches!(t, TokenKind::Lower(n) if n == name)),
                "expected `{name}` to be lexed"
            );
        }
    }

    #[test]
    fn doc_block_multiline_single_token() {
        let k = kinds("---\nline one\nline two\n---\nadd");
        // One merged Doc token containing both lines.
        let docs: Vec<_> = k
            .iter()
            .filter_map(|t| match t {
                TokenKind::Doc(d) => Some(d.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(docs.len(), 1);
        assert!(docs[0].contains("line one") && docs[0].contains("line two"));
    }

    #[test]
    fn consecutive_doc_lines_separate_tokens() {
        // The parser merges consecutive Doc tokens; the lexer emits one per line.
        let k = kinds("--- first\n--- second\nadd");
        let n = k.iter().filter(|t| matches!(t, TokenKind::Doc(_))).count();
        assert_eq!(n, 2);
    }

    #[test]
    fn marker_with_text_is_not_a_toggle() {
        // `-- text` is a line comment, NOT a block opener.
        let k = kinds("-- just a comment\nfoo");
        assert!(
            k.iter()
                .any(|t| matches!(t, TokenKind::Lower(n) if n == "foo"))
        );
        // `--- text` is a doc line, NOT a doc-block opener.
        let k2 = kinds("--- a doc line\nbar");
        assert!(
            k2.iter()
                .any(|t| matches!(t, TokenKind::Lower(n) if n == "bar"))
        );
    }
}
