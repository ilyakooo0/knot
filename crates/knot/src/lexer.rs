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
    Bytes(Vec<u8>),
    Bool(bool),

    // Identifiers
    Lower(String),
    Upper(String),

    // Keywords
    Import,
    Data,
    Type,
    Trait,
    Impl,
    Route,
    Migrate,
    Where,
    Do,
    Set,
    If,
    Then,
    Else,
    Case,
    Of,
    Let,
    In,
    Not,
    Full,
    Atomic,
    Deriving,
    With,
    Export,
    Unit,
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
    Slash,
    PlusPlus,
    AndAnd,
    OrOr,
    PipeGt,
    Caret,

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

    // Layout
    Newline,
    Semicolon,

    // End
    Eof,
}

impl TokenKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            TokenKind::Int(_) => "integer literal",
            TokenKind::Float(_) => "float literal",
            TokenKind::Text(_) => "string literal",
            TokenKind::Bytes(_) => "byte string literal",
            TokenKind::Bool(true) => "'true'",
            TokenKind::Bool(false) => "'false'",
            TokenKind::Lower(_) => "identifier",
            TokenKind::Upper(_) => "type name",
            TokenKind::Import => "'import'",
            TokenKind::Data => "'data'",
            TokenKind::Type => "'type'",
            TokenKind::Trait => "'trait'",
            TokenKind::Impl => "'impl'",
            TokenKind::Route => "'route'",
            TokenKind::Migrate => "'migrate'",
            TokenKind::Where => "'where'",
            TokenKind::Do => "'do'",
            TokenKind::Set => "'set'",
            TokenKind::If => "'if'",
            TokenKind::Then => "'then'",
            TokenKind::Else => "'else'",
            TokenKind::Case => "'case'",
            TokenKind::Of => "'of'",
            TokenKind::Let => "'let'",
            TokenKind::In => "'in'",
            TokenKind::Not => "'not'",
            TokenKind::Full => "'full'",
            TokenKind::Atomic => "'atomic'",
            TokenKind::Deriving => "'deriving'",
            TokenKind::With => "'with'",
            TokenKind::Export => "'export'",
            TokenKind::Unit => "'unit'",
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
            TokenKind::Slash => "'/'",
            TokenKind::PlusPlus => "'++'",
            TokenKind::AndAnd => "'&&'",
            TokenKind::OrOr => "'||'",
            TokenKind::PipeGt => "'|>'",
            TokenKind::Caret => "'^'",
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
            TokenKind::Semicolon => "';'",
            TokenKind::Eof => "end of file",
        }
    }

    /// If this token is a keyword, return its string representation.
    pub fn keyword_str(&self) -> Option<&'static str> {
        match self {
            TokenKind::Import => Some("import"),
            TokenKind::Data => Some("data"),
            TokenKind::Type => Some("type"),
            TokenKind::Trait => Some("trait"),
            TokenKind::Impl => Some("impl"),
            TokenKind::Route => Some("route"),
            TokenKind::Migrate => Some("migrate"),
            TokenKind::Where => Some("where"),
            TokenKind::Do => Some("do"),
            TokenKind::Set => Some("set"),
            TokenKind::If => Some("if"),
            TokenKind::Then => Some("then"),
            TokenKind::Else => Some("else"),
            TokenKind::Case => Some("case"),
            TokenKind::Of => Some("of"),
            TokenKind::Let => Some("let"),
            TokenKind::In => Some("in"),
            TokenKind::Not => Some("not"),
            TokenKind::Full => Some("full"),
            TokenKind::Atomic => Some("atomic"),
            TokenKind::Deriving => Some("deriving"),
            TokenKind::With => Some("with"),
            TokenKind::Export => Some("export"),
            TokenKind::Unit => Some("unit"),
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

        loop {
            self.skip_whitespace();

            // Comments: `--` to end of line
            if self.check(b'-') && self.peek_at(1) == Some(b'-') {
                self.skip_line_comment();
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

            // Newlines — collapse consecutive, suppress leading
            if ch == b'\n' {
                if !last_was_newline {
                    let start = self.pos;
                    self.advance();
                    while self.peek() == Some(b'\n') {
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
            if b == b' ' || b == b'\t' || b == b'\r' {
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
        // Skip until newline (leave the newline for the main loop)
        while let Some(b) = self.peek() {
            if b == b'\n' {
                break;
            }
            self.advance();
        }
    }

    // ── Main dispatch ───────────────────────────────────────────────

    fn lex_token(&mut self) -> Option<TokenKind> {
        let ch = self.bytes[self.pos];

        // Byte string literal: b"..."
        if ch == b'b' && self.peek_at(1) == Some(b'"') {
            return Some(self.lex_byte_string());
        }

        // Identifiers and keywords
        if ch.is_ascii_alphabetic() || ch == b'_' {
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
                "import" => return TokenKind::Import,
                "data" => return TokenKind::Data,
                "type" => return TokenKind::Type,
                "trait" => return TokenKind::Trait,
                "impl" => return TokenKind::Impl,
                "route" => return TokenKind::Route,
                "migrate" => return TokenKind::Migrate,
                "where" => return TokenKind::Where,
                "do" => return TokenKind::Do,
                "set" => return TokenKind::Set,
                "if" => return TokenKind::If,
                "then" => return TokenKind::Then,
                "else" => return TokenKind::Else,
                "case" => return TokenKind::Case,
                "of" => return TokenKind::Of,
                "let" => return TokenKind::Let,
                "in" => return TokenKind::In,
                "not" => return TokenKind::Not,
                "full" => return TokenKind::Full,
                "atomic" => return TokenKind::Atomic,
                "deriving" => return TokenKind::Deriving,
                "with" => return TokenKind::With,
                "export" => return TokenKind::Export,
                "unit" => return TokenKind::Unit,
                "refine" => return TokenKind::Refine,
                "forall" => return TokenKind::Forall,
                "true" => return TokenKind::Bool(true),
                "false" => return TokenKind::Bool(false),
                _ => {}
            }
            TokenKind::Lower(text.to_owned())
        } else {
            TokenKind::Upper(text.to_owned())
        }
    }

    fn is_ident_continue(&self) -> bool {
        matches!(self.peek(), Some(b) if b.is_ascii_alphanumeric() || b == b'_' || b == b'\'')
    }

    // ── Numbers ─────────────────────────────────────────────────────

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

        // Check for float: `.` followed by a digit
        if self.peek() == Some(b'.') && matches!(self.peek_at(1), Some(b'0'..=b'9')) {
            self.advance(); // consume '.'
            while let Some(b) = self.peek() {
                if b.is_ascii_digit() || b == b'_' {
                    self.advance();
                } else {
                    break;
                }
            }
            let slice = self.slice(start, self.pos);
            let raw = if slice.contains('_') { slice.replace('_', "") } else { slice.to_string() };
            let value = match raw.parse::<f64>() {
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
            let slice = self.slice(start, self.pos);
            let raw = if slice.contains('_') { slice.replace('_', "") } else { slice.to_string() };
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

    // ── Byte strings ─────────────────────────────────────────────────

    fn lex_byte_string(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance(); // skip 'b'
        self.advance(); // skip opening '"'

        let mut value = Vec::new();

        loop {
            match self.peek() {
                None | Some(b'\n') | Some(b'\r') => {
                    // CR (alone or as part of CRLF) terminates the byte
                    // string just like \n — matches the regular-string lexer
                    // (line 507) so byte-string literals can't silently eat a
                    // line ending.
                    let span = self.span_from(start);
                    self.diagnostics.push(
                        Diagnostic::error("unterminated byte string literal")
                            .label(span, "byte string starts here"),
                    );
                    return TokenKind::Bytes(value);
                }
                Some(b'"') => {
                    self.advance();
                    return TokenKind::Bytes(value);
                }
                Some(b'\\') => {
                    self.advance();
                    match self.peek() {
                        Some(b'\\') => {
                            self.advance();
                            value.push(b'\\');
                        }
                        Some(b'"') => {
                            self.advance();
                            value.push(b'"');
                        }
                        Some(b'n') => {
                            self.advance();
                            value.push(b'\n');
                        }
                        Some(b't') => {
                            self.advance();
                            value.push(b'\t');
                        }
                        Some(b'r') => {
                            self.advance();
                            value.push(b'\r');
                        }
                        Some(b'0') => {
                            self.advance();
                            value.push(0);
                        }
                        Some(b'x') => {
                            self.advance();
                            // Hex escape: \xHH
                            let h1 = self.peek().and_then(|b| (b as char).to_digit(16));
                            if let Some(d1) = h1 {
                                let first_hex_byte = self.bytes[self.pos];
                                self.advance();
                                let h2 = self.peek().and_then(|b| (b as char).to_digit(16));
                                if let Some(d2) = h2 {
                                    self.advance();
                                    value.push((d1 * 16 + d2) as u8);
                                } else {
                                    let span = Span::new(self.pos - 3, self.pos);
                                    self.diagnostics.push(
                                        Diagnostic::error("invalid hex escape in byte string")
                                            .label(span, "expected two hex digits after \\x"),
                                    );
                                    // Error recovery: emit the literal hex char (e.g.
                                    // `b'5'` for `\x5`) rather than the digit *value*
                                    // (`0x05`) so the recovered bytes resemble what
                                    // the user typed.
                                    let _ = d1;
                                    value.push(first_hex_byte);
                                }
                            } else {
                                let bad_end = if self.pos < self.source.len() {
                                    self.pos + self.source[self.pos..].chars().next().map_or(1, |c| c.len_utf8())
                                } else {
                                    self.pos
                                };
                                let span = Span::new(self.pos - 2, bad_end);
                                self.diagnostics.push(
                                    Diagnostic::error("invalid hex escape in byte string")
                                        .label(span, "expected two hex digits after \\x"),
                                );
                                // Error recovery: advance past the bad character
                                // and emit it as a literal byte so the byte string
                                // isn't silently shortened (consistent with other
                                // escape error recovery paths).
                                if self.pos < self.source.len() {
                                    let b = self.source.as_bytes()[self.pos];
                                    self.advance();
                                    value.push(b);
                                }
                            }
                        }
                        Some(_) => {
                            let esc_start = self.pos - 1;
                            // Advance by one full UTF-8 character (not just one byte)
                            let ch = self.source[self.pos..].chars().next().unwrap();
                            self.pos += ch.len_utf8();
                            let span = Span::new(esc_start, self.pos);
                            self.diagnostics.push(
                                Diagnostic::error("unknown escape sequence in byte string")
                                    .label(span, "unknown escape"),
                            );
                            // Error recovery: emit the escaped character's bytes
                            // so the byte string isn't silently shortened.
                            let mut buf = [0u8; 4];
                            let encoded = ch.encode_utf8(&mut buf);
                            value.extend_from_slice(encoded.as_bytes());
                        }
                        None => {}
                    }
                }
                Some(b) => {
                    self.advance();
                    value.push(b);
                }
            }
        }
    }

    // ── Operators & punctuation ─────────────────────────────────────

    fn lex_operator(&mut self) -> Option<TokenKind> {
        let ch = self.advance();

        let kind = match ch {
            b'<' => {
                if self.eat(b'-') {
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
                        Diagnostic::error("unexpected character '!'")
                            .label(span, "unexpected"),
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
            b'*' => TokenKind::Star,
            b'/' => TokenKind::Slash,
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
                let c = self.source[char_start..self.pos].chars().next().unwrap_or('?');
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
        let (tokens, diags) = Lexer::new(src).tokenize();
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:?}");
        tokens.into_iter().map(|t| t.kind).collect()
    }

    fn kinds_with_diags(src: &str) -> (Vec<TokenKind>, Vec<Diagnostic>) {
        let (tokens, diags) = Lexer::new(src).tokenize();
        (tokens.into_iter().map(|t| t.kind).collect(), diags)
    }

    #[test]
    fn empty_source() {
        assert_eq!(kinds(""), vec![TokenKind::Eof]);
    }

    #[test]
    fn integers() {
        assert_eq!(kinds("42"), vec![TokenKind::Int("42".into()), TokenKind::Eof]);
        assert_eq!(
            kinds("1_000_000"),
            vec![TokenKind::Int("1000000".into()), TokenKind::Eof],
        );
    }

    #[test]
    fn floats() {
        assert_eq!(kinds("3.14"), vec![TokenKind::Float(3.14), TokenKind::Eof]);
    }

    #[test]
    fn dot_not_float() {
        assert_eq!(
            kinds("x.y"),
            vec![
                TokenKind::Lower("x".into()),
                TokenKind::Dot,
                TokenKind::Lower("y".into()),
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn strings() {
        assert_eq!(
            kinds(r#""hello""#),
            vec![TokenKind::Text("hello".into()), TokenKind::Eof],
        );
        assert_eq!(
            kinds(r#""a\nb""#),
            vec![TokenKind::Text("a\nb".into()), TokenKind::Eof],
        );
    }

    #[test]
    fn unterminated_string() {
        let (toks, diags) = kinds_with_diags("\"oops\n");
        assert_eq!(toks[0], TokenKind::Text("oops".into()));
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unterminated"));
    }

    #[test]
    fn keywords() {
        assert_eq!(
            kinds("if then else let in"),
            vec![
                TokenKind::If,
                TokenKind::Then,
                TokenKind::Else,
                TokenKind::Let,
                TokenKind::In,
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn identifiers() {
        assert_eq!(
            kinds("foo Bar _x _"),
            vec![
                TokenKind::Lower("foo".into()),
                TokenKind::Upper("Bar".into()),
                TokenKind::Lower("_x".into()),
                TokenKind::Underscore,
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn primed_identifiers() {
        assert_eq!(
            kinds("x'"),
            vec![TokenKind::Lower("x'".into()), TokenKind::Eof],
        );
    }

    #[test]
    fn operators() {
        assert_eq!(
            kinds("-> => <- == != <= >="),
            vec![
                TokenKind::Arrow,
                TokenKind::FatArrow,
                TokenKind::LArrow,
                TokenKind::EqEq,
                TokenKind::BangEq,
                TokenKind::Le,
                TokenKind::Ge,
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn pipe_operators() {
        assert_eq!(
            kinds("| || |>"),
            vec![
                TokenKind::Pipe,
                TokenKind::OrOr,
                TokenKind::PipeGt,
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn newlines_collapse() {
        assert_eq!(
            kinds("a\n\n\nb"),
            vec![
                TokenKind::Lower("a".into()),
                TokenKind::Newline,
                TokenKind::Lower("b".into()),
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn leading_newlines_suppressed() {
        assert_eq!(
            kinds("\n\na\n\n"),
            vec![
                TokenKind::Lower("a".into()),
                TokenKind::Newline,
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn comments() {
        assert_eq!(
            kinds("a -- comment\nb"),
            vec![
                TokenKind::Lower("a".into()),
                TokenKind::Newline,
                TokenKind::Lower("b".into()),
                TokenKind::Eof,
            ],
        );
    }

    #[test]
    fn unknown_character() {
        let (toks, diags) = kinds_with_diags("a ~ b");
        assert_eq!(
            toks,
            vec![
                TokenKind::Lower("a".into()),
                TokenKind::Lower("b".into()),
                TokenKind::Eof,
            ],
        );
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unexpected character '~'"));
    }

    #[test]
    fn spans_are_correct() {
        let (tokens, _) = Lexer::new("let x = 42").tokenize();
        assert_eq!(tokens[0].span, Span::new(0, 3)); // "let"
        assert_eq!(tokens[1].span, Span::new(4, 5)); // "x"
        assert_eq!(tokens[2].span, Span::new(6, 7)); // "="
        assert_eq!(tokens[3].span, Span::new(8, 10)); // "42"
    }

    #[test]
    fn display_names() {
        assert_eq!(TokenKind::Int("0".into()).display_name(), "integer literal");
        assert_eq!(TokenKind::Eof.display_name(), "end of file");
        assert_eq!(TokenKind::Plus.display_name(), "'+'");
        assert_eq!(TokenKind::If.display_name(), "'if'");
    }
}
