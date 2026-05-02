//! Recursive-descent parser with Pratt expression parsing for the Knot language.

use crate::ast::*;
use crate::diagnostic::Diagnostic;
use crate::lexer::{Token, TokenKind};

// ── Parser state ────────────────────────────────────────────────────

pub struct Parser {
    source: String,
    tokens: Vec<Token>,
    pos: usize,
    diagnostics: Vec<Diagnostic>,
    context: Vec<(&'static str, Span)>,
    /// When true, `can_start_type_atom` returns false for `Lower("headers")`.
    /// Used in route entry parsing to prevent the type parser from consuming
    /// the `headers` keyword.
    stop_type_at_headers: bool,
    /// Indentation level of the current block (set by `parse_block`).
    /// Used by `parse_application` to allow multi-line function application
    /// when continuation lines are indented past the block indent.
    block_indent: usize,
    /// Nesting depth inside delimiters (parens, brackets, braces).
    /// When > 0, the column-0 check in `parse_expr_bp` is suppressed so that
    /// operators at column 0 inside grouped expressions are not mistaken for
    /// new top-level declarations.
    delimiter_depth: usize,
    /// Tracks recursion depth for unbounded recursive-descent paths
    /// (unary operators, constructor chaining, type arrows) to prevent
    /// stack overflow on pathological input.
    recursion_depth: usize,
}

// ── Public API ──────────────────────────────────────────────────────

impl Parser {
    pub fn new(source: String, tokens: Vec<Token>) -> Self {
        Self {
            source,
            tokens,
            pos: 0,
            diagnostics: Vec::new(),
            context: Vec::new(),
            stop_type_at_headers: false,
            block_indent: usize::MAX,
            delimiter_depth: 0,
            recursion_depth: 0,
        }
    }

    pub fn parse_module(mut self) -> (Module, Vec<Diagnostic>) {
        self.skip_newlines();

        // Parse imports (must come before other declarations)
        let mut imports = Vec::new();
        while self.at(&TokenKind::Import) {
            if let Some(imp) = self.parse_import() {
                imports.push(imp);
            }
            self.skip_newlines();
        }

        // Set block_indent so that multiline expressions inside declarations
        // can continue across newlines (parse_application checks column > block_indent).
        self.block_indent = 0;
        let mut decls = Vec::new();
        while !self.at_eof() {
            self.skip_newlines();
            if self.at_eof() {
                break;
            }
            let exported = self.eat(&TokenKind::Export);
            self.skip_newlines();
            match self.parse_decl() {
                Some(mut d) => {
                    d.exported = exported;
                    decls.push(d);
                }
                None => {
                    // Error recovery: clear stale parser context entries
                    // left by early returns via `?` in parse functions.
                    self.context.clear();
                    // Skip to next declaration boundary.
                    if !self.at_eof() {
                        self.advance();
                        self.skip_to_decl_boundary();
                    }
                }
            }
            self.skip_newlines();
        }

        (Module { imports, decls }, self.diagnostics)
    }
}

// ── Recursion depth guard ────────────────────────────────────────────

const MAX_RECURSION_DEPTH: usize = 256;

impl Parser {
    /// Increment recursion depth and return `true`, or emit an error and
    /// return `false` if the limit is exceeded.  Callers must decrement
    /// `self.recursion_depth` when the recursive call returns.
    fn enter_recursion(&mut self) -> bool {
        self.recursion_depth += 1;
        if self.recursion_depth > MAX_RECURSION_DEPTH {
            self.recursion_depth -= 1;
            let span = self.span();
            self.diagnostics.push(
                Diagnostic::error("nesting depth limit exceeded")
                    .label(span, "expression is too deeply nested"),
            );
            return false;
        }
        true
    }
}

// ── Token navigation ────────────────────────────────────────────────

impl Parser {
    fn peek(&self) -> &TokenKind {
        self.tokens
            .get(self.pos)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
    }

    fn peek_token(&self) -> Token {
        self.tokens.get(self.pos).cloned().unwrap_or(Token {
            kind: TokenKind::Eof,
            span: self.eof_span(),
        })
    }

    fn peek_ahead(&self, offset: usize) -> &TokenKind {
        self.tokens
            .get(self.pos + offset)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
    }

    fn span(&self) -> Span {
        self.peek_token().span
    }

    /// Check whether the current token has the same discriminant as `kind`.
    fn at(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(self.peek()) == std::mem::discriminant(kind)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token {
            kind: TokenKind::Eof,
            span: self.eof_span(),
        });
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, kind: &TokenKind, msg: &str) -> Result<Token, ()> {
        if self.at(kind) {
            Ok(self.advance())
        } else {
            self.error(msg);
            Err(())
        }
    }

    fn eat(&mut self, kind: &TokenKind) -> bool {
        if self.at(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn skip_newlines(&mut self) {
        while self.at(&TokenKind::Newline) {
            self.advance();
        }
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), TokenKind::Eof)
    }

    fn eof_span(&self) -> Span {
        Span::new(self.source.len(), self.source.len())
    }

    fn save(&self) -> (usize, usize, usize) {
        (self.pos, self.delimiter_depth, self.recursion_depth)
    }

    fn restore(&mut self, saved: (usize, usize, usize)) {
        self.pos = saved.0;
        self.delimiter_depth = saved.1;
        self.recursion_depth = saved.2;
    }

    fn column_of(&self, span: &Span) -> usize {
        let offset = span.start.min(self.source.len());
        let before = &self.source[..offset];
        let line_start = match before.rfind('\n') {
            Some(nl) => nl + 1,
            None => 0,
        };
        before[line_start..].chars().count()
    }
}

// ── Context & error helpers ─────────────────────────────────────────

impl Parser {
    fn push_context(&mut self, ctx: &'static str) {
        let sp = self.span();
        self.context.push((ctx, sp));
    }

    fn pop_context(&mut self) {
        self.context.pop();
    }

    /// Push a parser context, run `f`, and always pop the context — even on
    /// early `?` returns inside `f`.  This prevents stale "while parsing …"
    /// notes from leaking into later diagnostics.
    fn in_context<T>(&mut self, ctx: &'static str, f: impl FnOnce(&mut Self) -> Option<T>) -> Option<T> {
        self.push_context(ctx);
        let result = f(self);
        self.pop_context();
        result
    }

    fn error(&mut self, msg: impl Into<String>) {
        self.error_at(self.span(), msg);
    }

    fn error_at(&mut self, span: Span, msg: impl Into<String>) {
        let mut diag = Diagnostic::error(msg).label(span, "here");
        // Add context notes from the stack.
        for &(ctx, ctx_span) in self.context.iter().rev() {
            let (line, _) = crate::diagnostic::line_col(&self.source, ctx_span.start);
            diag = diag.note(format!("while parsing {ctx} starting at line {line}"));
        }
        self.diagnostics.push(diag);
    }

    /// Skip tokens until we reach a comma or `}` — the boundary between two
    /// record fields, or the end of the record. Used to recover from a bad
    /// field value without aborting the whole record literal. Stops at the
    /// boundary token *without consuming it* so the surrounding loop can
    /// inspect and react (continue on `,`, exit on `}`).
    fn skip_to_record_field_boundary(&mut self) {
        let mut depth: usize = 0;
        loop {
            if self.at_eof() {
                break;
            }
            match self.peek() {
                TokenKind::LBrace | TokenKind::LParen | TokenKind::LBracket => {
                    depth += 1;
                    self.advance();
                }
                TokenKind::RBrace if depth == 0 => break,
                TokenKind::RBrace => {
                    depth = depth.saturating_sub(1);
                    self.advance();
                }
                TokenKind::RParen | TokenKind::RBracket => {
                    depth = depth.saturating_sub(1);
                    self.advance();
                }
                TokenKind::Comma if depth == 0 => break,
                _ => {
                    self.advance();
                }
            }
        }
    }

    /// Skip tokens until we reach what looks like a new declaration boundary.
    fn skip_to_decl_boundary(&mut self) {
        loop {
            if self.at_eof() {
                break;
            }
            let col = self.column_of(&self.span());
            if col == 0 {
                match self.peek() {
                    TokenKind::Export
                    | TokenKind::Data
                    | TokenKind::Type
                    | TokenKind::Trait
                    | TokenKind::Impl
                    | TokenKind::Route
                    | TokenKind::Migrate
                    | TokenKind::Unit
                    | TokenKind::Star
                    | TokenKind::Ampersand
                    | TokenKind::Lower(_)
                    | TokenKind::Upper(_) => break,
                    _ => {}
                }
            }
            self.advance();
        }
    }

    /// Expect a lower-case identifier, returning the name.
    fn expect_lower(&mut self, msg: &str) -> Result<(Name, Span), ()> {
        match self.peek() {
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(n) = tok.kind else { unreachable!() };
                Ok((n, tok.span))
            }
            TokenKind::Where
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
            | TokenKind::Import
            | TokenKind::Data
            | TokenKind::Type
            | TokenKind::Trait
            | TokenKind::Impl
            | TokenKind::Route
            | TokenKind::Migrate
            | TokenKind::Export => {
                let kw = format!("{:?}", self.peek()).to_lowercase();
                self.error(format!(
                    "'{kw}' is a keyword and cannot be used as a variable name"
                ));
                Err(())
            }
            _ => {
                self.error(msg);
                Err(())
            }
        }
    }

    /// Expect an upper-case identifier, returning the name.
    fn expect_upper(&mut self, msg: &str) -> Result<(Name, Span), ()> {
        match self.peek() {
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(n) = tok.kind else { unreachable!() };
                Ok((n, tok.span))
            }
            _ => {
                self.error(msg);
                Err(())
            }
        }
    }
}

// ── Imports ─────────────────────────────────────────────────────────

impl Parser {
    /// Parse `import ./path` or `import ./path (A, b)`.
    /// Path is assembled from Dot, Slash, and identifier tokens.
    fn parse_import(&mut self) -> Option<Import> {
        let start = self.span();
        self.advance(); // consume `import`

        // Parse the relative path: ./foo, ../bar/baz, etc.
        let mut path = String::new();

        // Must start with `.`
        if !self.at(&TokenKind::Dot) {
            self.error("expected relative path starting with '.' after 'import'");
            return None;
        }
        self.advance();
        path.push('.');

        // Could be `..` (parent directory)
        if self.at(&TokenKind::Dot) {
            self.advance();
            path.push('.');
            if self.at(&TokenKind::Dot) {
                self.error("invalid import path: too many leading dots (use '.' or '..')");
                return None;
            }
        }

        // Consume `/segment` pairs (segment can be an identifier or `..`)
        loop {
            if !self.at(&TokenKind::Slash) {
                break;
            }
            self.advance();
            path.push('/');

            if self.at(&TokenKind::Dot) {
                // `..` parent directory segment within path
                self.advance();
                path.push('.');
                if self.at(&TokenKind::Dot) {
                    self.advance();
                    path.push('.');
                }
            } else {
                match self.peek() {
                    TokenKind::Lower(_) | TokenKind::Upper(_) => {
                        let tok = self.advance();
                        let name = match tok.kind {
                            TokenKind::Lower(n) | TokenKind::Upper(n) => n,
                            _ => unreachable!(),
                        };
                        path.push_str(&name);
                    }
                    ref tok if tok.keyword_str().is_some() => {
                        let tok = self.advance();
                        path.push_str(tok.kind.keyword_str().unwrap());
                    }
                    _ => {
                        self.error("expected path segment after '/'");
                        return None;
                    }
                }
            }
        }

        // Optional selective import list: (A, b, C)
        let items = if self.at(&TokenKind::LParen) {
            self.advance();
            let mut items = Vec::new();
            loop {
                if self.at(&TokenKind::RParen) {
                    self.advance();
                    break;
                }
                let item_span = self.span();
                let name = match self.peek() {
                    TokenKind::Upper(_) | TokenKind::Lower(_) => {
                        let tok = self.advance();
                        match tok.kind {
                            TokenKind::Upper(n) | TokenKind::Lower(n) => n,
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        self.error("expected name in import list");
                        return None;
                    }
                };
                items.push(ImportItem {
                    name,
                    span: item_span,
                });
                if !self.eat(&TokenKind::Comma) {
                    self.expect(&TokenKind::RParen, "expected ',' or ')' in import list")
                        .ok()?;
                    break;
                }
            }
            Some(items)
        } else {
            None
        };

        let end = self.prev_span();
        let span = Span::new(start.start, end.end);
        Some(Import { path, items, span })
    }
}

// ── Layout block helper ─────────────────────────────────────────────

impl Parser {
    fn parse_block<T>(&mut self, mut parse_item: impl FnMut(&mut Self) -> Option<T>) -> Vec<T> {
        self.skip_newlines();
        if self.at_eof() {
            return vec![];
        }
        let indent = self.column_of(&self.span());
        let prev_block_indent = self.block_indent;
        self.block_indent = indent;
        let mut items = vec![];
        loop {
            if self.at_eof() {
                break;
            }
            let col = self.column_of(&self.span());
            if col < indent {
                break;
            }
            let before = self.pos;
            match parse_item(self) {
                Some(item) => items.push(item),
                None => {
                    // No progress means parse_item bailed without consuming a
                    // token — break to avoid an infinite loop.
                    if self.pos == before {
                        break;
                    }
                    // The item parser hit an error after consuming tokens.
                    // Recover by skipping to the next line so subsequent
                    // items in this block still get parsed; without this
                    // one bad declaration silently truncates the block.
                    // parse_item already emitted a diagnostic.
                    while !self.at_eof() && !self.at(&TokenKind::Newline) {
                        self.advance();
                    }
                    self.skip_newlines();
                    continue;
                }
            }
            // Semicolons act as explicit item separators within a block,
            // allowing e.g. `case x of A {} -> 1; B {} -> 2` on one line.
            if self.at(&TokenKind::Semicolon) {
                self.advance();
                continue;
            }
            // When inside delimiters (parens, brackets, braces), a closing
            // delimiter ends the block — it belongs to an outer scope.
            // Without this, `(case x of A -> 1; B -> 2)` would try to
            // parse `)` as a case arm pattern.
            if self.delimiter_depth > 0
                && matches!(
                    self.peek(),
                    TokenKind::RParen | TokenKind::RBracket | TokenKind::RBrace
                )
            {
                break;
            }
            // Keywords that cannot start a new block item terminate the block.
            // For example, `in` after `let active = do ...; yield x in ...`
            // belongs to the enclosing `let...in`, not to the do block.
            if matches!(self.peek(), TokenKind::In | TokenKind::Then | TokenKind::Else | TokenKind::Of) {
                break;
            }
            // Peek past newlines to check if the next item is still in
            // this block. If not, DON'T consume the newlines — they act
            // as separators for the outer parser (e.g. parse_application
            // uses newlines to distinguish same-line args from multi-line
            // continuation).
            let saved = self.save();
            self.skip_newlines();
            if self.at_eof()
                || self.column_of(&self.span()) < indent
                || (self.delimiter_depth > 0
                    && matches!(
                        self.peek(),
                        TokenKind::RParen | TokenKind::RBracket | TokenKind::RBrace
                    ))
            {
                self.restore(saved);
                break;
            }
        }
        self.block_indent = prev_block_indent;
        items
    }
}

// ── Declarations ────────────────────────────────────────────────────

impl Parser {
    fn parse_decl(&mut self) -> Option<Decl> {
        let start = self.span();
        match self.peek() {
            TokenKind::Data => self.parse_data(),
            TokenKind::Type => self.parse_type_alias(),
            TokenKind::Star => self.parse_source_or_view(),
            TokenKind::Ampersand => self.parse_derived(),
            TokenKind::Lower(_) => self.parse_fun(),
            TokenKind::Trait => self.parse_trait_decl(),
            TokenKind::Impl => self.parse_impl_decl(),
            TokenKind::Route => self.parse_route_decl(),
            TokenKind::Migrate => self.parse_migrate(),
            TokenKind::Unit => self.parse_unit_decl(),
            _ => {
                self.error_at(start, "expected declaration");
                None
            }
        }
    }

    // ── unit ─────────────────────────────────────────────────────────

    fn parse_unit_decl(&mut self) -> Option<Decl> {
        let start = self.span();
        self.advance(); // consume `unit`

        let (name, _) = self.expect_upper("expected unit name after 'unit' (units must start with uppercase)").ok()?;

        let definition = if self.eat(&TokenKind::Eq) {
            Some(self.parse_unit_expr()?)
        } else {
            None
        };

        let span = Span::new(start.start, self.prev_span().end);
        Some(Decl {
            node: DeclKind::UnitDecl { name, definition },
            span,
            exported: false,
        })
    }

    /// Parse a unit expression: products, quotients, powers of named units.
    /// Grammar:
    ///   unit_expr    = unit_mul_div
    ///   unit_mul_div = unit_power (('*' | '/') unit_power)*
    ///   unit_power   = unit_atom ('^' integer)?
    ///   unit_atom    = lower_ident | '1' | '(' unit_expr ')'
    fn parse_unit_expr(&mut self) -> Option<UnitExpr> {
        let mut lhs = self.parse_unit_power()?;
        loop {
            if self.eat(&TokenKind::Star) {
                let rhs = self.parse_unit_power()?;
                lhs = UnitExpr::Mul(Box::new(lhs), Box::new(rhs));
            } else if self.eat(&TokenKind::Slash) {
                let rhs = self.parse_unit_power()?;
                lhs = UnitExpr::Div(Box::new(lhs), Box::new(rhs));
            } else {
                break;
            }
        }
        Some(lhs)
    }

    fn parse_unit_power(&mut self) -> Option<UnitExpr> {
        let base = self.parse_unit_atom()?;
        if self.eat(&TokenKind::Caret) {
            // Parse integer exponent (possibly negative)
            let neg = self.eat(&TokenKind::Minus);
            match self.peek() {
                TokenKind::Int(_) => {
                    let tok = self.advance();
                    let TokenKind::Int(n) = tok.kind else { unreachable!() };
                    let exp: i32 = match n.parse() {
                        Ok(e) => e,
                        Err(_) => {
                            self.error("unit exponent out of range (must fit in i32)");
                            return None;
                        }
                    };
                    Some(UnitExpr::Pow(Box::new(base), if neg { -exp } else { exp }))
                }
                _ => {
                    self.error("expected integer exponent after '^'");
                    None
                }
            }
        } else {
            Some(base)
        }
    }

    fn parse_unit_atom(&mut self) -> Option<UnitExpr> {
        match self.peek() {
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(name) = tok.kind else { unreachable!() };
                Some(UnitExpr::Named(name))
            }
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                Some(UnitExpr::Named(name))
            }
            TokenKind::Int(n) if n == "1" => {
                self.advance();
                Some(UnitExpr::Dimensionless)
            }
            TokenKind::LParen => {
                self.advance();
                let inner = self.parse_unit_expr()?;
                self.expect(&TokenKind::RParen, "expected ')' in unit expression").ok()?;
                Some(inner)
            }
            _ => {
                self.error("expected unit name, '1', or '(' in unit expression");
                None
            }
        }
    }

    /// Try to parse `<unit_expr>` after a numeric literal or type name.
    /// Returns `None` if the `<` doesn't start a unit annotation (falls through to comparison).
    fn try_parse_unit_annotation(&mut self) -> Option<UnitExpr> {
        if !matches!(self.peek(), TokenKind::Lt) {
            return None;
        }
        // Check adjacency: no whitespace between previous token and `<`
        let lt_span = self.span();
        let prev_end = self.prev_span().end;
        if lt_span.start != prev_end {
            return None;
        }

        let saved = self.save();
        let diag_count = self.diagnostics.len();
        self.advance(); // consume `<`
        if let Some(unit) = self.parse_unit_expr() {
            if matches!(self.peek(), TokenKind::Gt) {
                self.advance(); // consume `>`
                return Some(unit);
            }
        }
        // Not a unit annotation — restore
        self.diagnostics.truncate(diag_count);
        self.restore(saved);
        None
    }

    // ── data ─────────────────────────────────────────────────────────

    fn parse_data(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("data declaration", |this| {
            this.advance(); // consume `data`

            let (name, _) = this.expect_upper("expected type name after 'data'").ok()?;

            // Parse type parameters (lowercase identifiers before `=`).
            let mut params = Vec::new();
            while matches!(this.peek(), TokenKind::Lower(_)) {
                let tok = this.advance();
                let TokenKind::Lower(p) = tok.kind else { unreachable!() };
                params.push(p);
            }

            this.skip_newlines();
            this.expect(&TokenKind::Eq, "expected '=' in data declaration").ok()?;
            this.skip_newlines();

            let mut constructors = vec![this.parse_constructor_def()?];
            loop {
                this.skip_newlines();
                if !this.eat(&TokenKind::Pipe) {
                    break;
                }
                this.skip_newlines();
                constructors.push(this.parse_constructor_def()?);
            }

            // Optional deriving clause.
            this.skip_newlines();
            let mut deriving = Vec::new();
            if this.eat(&TokenKind::Deriving) {
                this.expect(&TokenKind::LParen, "expected '(' after 'deriving'").ok()?;
                loop {
                    if matches!(this.peek(), TokenKind::Upper(_)) {
                        let tok = this.advance();
                        let TokenKind::Upper(n) = tok.kind else { unreachable!() };
                        deriving.push(n);
                    } else {
                        break;
                    }
                    if !this.eat(&TokenKind::Comma) {
                        break;
                    }
                }
                this.expect(&TokenKind::RParen, "expected ')' to close deriving list")
                    .ok()?;
            }

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Data {
                    name,
                    params,
                    constructors,
                    deriving,
                },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    fn parse_constructor_def(&mut self) -> Option<ConstructorDef> {
        let (name, _) = self.expect_upper("expected constructor name").ok()?;
        let mut fields = Vec::new();
        if self.eat(&TokenKind::LBrace) {
            if !self.at(&TokenKind::RBrace) {
                loop {
                    self.skip_newlines();
                    let (fname, _) = self.expect_lower("expected field name in constructor").ok()?;
                    self.expect(&TokenKind::Colon, "expected ':' after field name in constructor")
                        .ok()?;
                    let ty = self.parse_type()?;
                    fields.push(Field {
                        name: fname,
                        value: ty,
                    });
                    self.skip_newlines();
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(&TokenKind::RBrace, "expected '}' to close constructor fields")
                .ok()?;
        }
        Some(ConstructorDef { name, fields })
    }

    // ── type alias ───────────────────────────────────────────────────

    fn parse_type_alias(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("type alias", |this| {
            this.advance(); // consume `type`

            let (name, _) = this.expect_upper("expected type name after 'type'").ok()?;

            let mut params = Vec::new();
            while matches!(this.peek(), TokenKind::Lower(_)) {
                let tok = this.advance();
                let TokenKind::Lower(p) = tok.kind else { unreachable!() };
                params.push(p);
            }

            this.expect(&TokenKind::Eq, "expected '=' in type alias").ok()?;
            let ty = this.parse_type()?;

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::TypeAlias { name, params, ty },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    // ── source / view ────────────────────────────────────────────────

    fn parse_source_or_view(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("source/view declaration", |this| {
            this.advance(); // consume `*`

            let (name, _) = this.expect_lower("expected name after '*'").ok()?;

            // Subset constraint: *name.field <= ... or *name <= ...
            if this.at(&TokenKind::Dot) || this.at(&TokenKind::Le) {
                return this.parse_subset_constraint_rest(start, name);
            }

            // Peek: if `:` → source declaration, if `=` → view declaration.
            if this.eat(&TokenKind::Colon) {
                // Source declaration: *name : type
                // Or annotated view: *name : type = body
                let ty = this.parse_type()?;

                // Inline annotated view: *name : Type = body
                if this.at(&TokenKind::Eq) {
                    this.advance();
                    let body = this.parse_expr()?;
                    let end = this.prev_span();
                    let scheme = TypeScheme {
                        constraints: vec![],
                        ty,
                    };
                    return Some(Decl {
                        node: DeclKind::View {
                            name,
                            ty: Some(scheme),
                            body,
                        },
                        span: Span::new(start.start, end.end),
                        exported: false,
                    });
                }

                // Optional `with history` (may be on the next line)
                let mut history = false;
                this.skip_newlines();
                if this.eat(&TokenKind::With) {
                    if matches!(this.peek(), TokenKind::Lower(s) if s == "history") {
                        this.advance();
                        history = true;
                    } else {
                        this.error("expected 'history' after 'with'");
                        return None;
                    }
                }
                let end = this.prev_span();
                Some(Decl {
                    node: DeclKind::Source { name, ty, history },
                    span: Span::new(start.start, end.end),
                    exported: false,
                })
            } else if this.eat(&TokenKind::Eq) {
                // View declaration: *name = expr
                let body = this.parse_expr()?;
                let end = this.prev_span();
                Some(Decl {
                    node: DeclKind::View {
                        name,
                        ty: None,
                        body,
                    },
                    span: Span::new(start.start, end.end),
                    exported: false,
                })
            } else {
                this.error("expected ':', '=', or '<=' after source/view name");
                None
            }
        })
    }

    // ── subset constraint ────────────────────────────────────────────

    /// Parse the rest of a subset constraint after `*name` has been consumed.
    /// Handles: `*name.field <= *other.field` and `*name <= *other.field`.
    fn parse_subset_constraint_rest(&mut self, start: Span, left_relation: Name) -> Option<Decl> {
        self.in_context("subset constraint", |this| {
            let left_field = if this.eat(&TokenKind::Dot) {
                let (field, _) = this.expect_lower("expected field name after '.'").ok()?;
                Some(field)
            } else {
                None
            };

            this.expect(&TokenKind::Le, "expected '<=' in subset constraint").ok()?;

            // Parse right side: *relation.field or *relation
            this.expect(&TokenKind::Star, "expected '*' before relation name in subset constraint")
                .ok()?;
            let (right_relation, _) = this
                .expect_lower("expected relation name after '*' in subset constraint")
                .ok()?;

            let right_field = if this.eat(&TokenKind::Dot) {
                let (field, _) = this.expect_lower("expected field name after '.'").ok()?;
                Some(field)
            } else {
                None
            };

            let end = this.prev_span();
        Some(Decl {
            node: DeclKind::SubsetConstraint {
                sub: RelationPath {
                    relation: left_relation,
                    field: left_field,
                },
                sup: RelationPath {
                    relation: right_relation,
                    field: right_field,
                },
            },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    // ── derived ──────────────────────────────────────────────────────

    fn parse_derived(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("derived declaration", |this| {
            this.advance(); // consume `&`

            let (name, _) = this.expect_lower("expected name after '&'").ok()?;

            // Optional inline type annotation: `&name : Type = body`
            let ty = if this.eat(&TokenKind::Colon) {
                let scheme = this.parse_type_scheme()?;
                Some(scheme)
            } else {
                None
            };

            this.expect(&TokenKind::Eq, "expected '=' in derived declaration")
                .ok()?;
            let body = this.parse_expr()?;

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Derived { name, ty, body },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    // ── function / constant ──────────────────────────────────────────

    fn parse_fun(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("function declaration", |this| {
            let (name, _) = this.expect_lower("expected function name").ok()?;

            // Check: is this a type signature (name : type) or a definition?
            if this.at(&TokenKind::Colon) {
                // Type signature — parse it and try to attach to next definition.
                this.advance(); // consume `:`
                let ts = this.parse_type_scheme();

                // Inline form: `name : Type = body` (no newline, no name repeat).
                if this.at(&TokenKind::Eq) {
                    this.advance(); // consume `=`
                    if let Some(body) = this.parse_expr() {
                        let end = this.prev_span();
                        return Some(Decl {
                            node: DeclKind::Fun {
                                name,
                                ty: ts,
                                body: Some(body),
                            },
                            span: Span::new(start.start, end.end),
                            exported: false,
                        });
                    }
                }

                this.skip_newlines();

                // Now check if the next line is the definition body.
                if matches!(this.peek(), TokenKind::Lower(n) if *n == name) {
                    let saved = this.save();
                    let diag_len = this.diagnostics.len();
                    this.advance(); // consume name again

                    if this.eat(&TokenKind::Eq) {
                        if let Some(body) = this.parse_expr() {
                            let end = this.prev_span();
                            return Some(Decl {
                                node: DeclKind::Fun {
                                    name,
                                    ty: ts,
                                    body: Some(body),
                                },
                                span: Span::new(start.start, end.end),
                                exported: false,
                            });
                        } else {
                            // parse_expr failed — restore to before the name
                            // so the tokens can be re-parsed as a separate decl.
                            this.restore(saved);
                            this.diagnostics.truncate(diag_len);
                        }
                    } else {
                        // Not a definition after the signature — restore.
                        this.restore(saved);
                    }
                }

                // Return a Fun with just a type signature and no body.
                let end = this.prev_span();
                return Some(Decl {
                    node: DeclKind::Fun {
                        name,
                        ty: ts,
                        body: None,
                    },
                    span: Span::new(start.start, end.end),
                    exported: false,
                });
            }

            this.expect(&TokenKind::Eq, "expected '=' in definition")
                .ok()?;
            let body = this.parse_expr()?;

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Fun {
                    name,
                    ty: None,
                    body: Some(body),
                },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    // ── trait ─────────────────────────────────────────────────────────

    fn parse_trait_decl(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("trait declaration", |this| {
            this.advance(); // consume `trait`

            let mut supertraits = Vec::new();

            let saved = this.save();
            if let Some(constraints) = this.try_parse_constraints() {
                supertraits = constraints;
            } else {
                this.restore(saved);
            }

            let (name, _) = this.expect_upper("expected trait name").ok()?;

            // Parse trait parameters: (name : kind?) or just lowercase name
            let mut params = Vec::new();
            loop {
                if this.eat(&TokenKind::LParen) {
                    let (pname, _) = this
                        .expect_lower("expected type parameter name in trait declaration")
                        .ok()?;
                    let kind = if this.eat(&TokenKind::Colon) {
                        Some(this.parse_type()?)
                    } else {
                        None
                    };
                    this.expect(&TokenKind::RParen, "expected ')' after trait parameter")
                        .ok()?;
                    params.push(TraitParam { name: pname, kind });
                } else if matches!(this.peek(), TokenKind::Lower(_)) {
                    let tok = this.advance();
                    let TokenKind::Lower(pname) = tok.kind else { unreachable!() };
                    params.push(TraitParam {
                        name: pname,
                        kind: None,
                    });
                } else {
                    break;
                }
            }

            this.expect(&TokenKind::Where, "expected 'where' in trait declaration")
                .ok()?;

            let items = this.parse_block(|p| p.parse_trait_item());

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Trait {
                    name,
                    params,
                    supertraits,
                    items,
                },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    fn parse_trait_item(&mut self) -> Option<TraitItem> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }

        // `type Name params*` — associated type
        if self.at(&TokenKind::Type) {
            self.advance();
            let (name, _) = self.expect_upper("expected associated type name").ok()?;
            let mut assoc_params = Vec::new();
            while matches!(self.peek(), TokenKind::Lower(_)) {
                let tok = self.advance();
                let TokenKind::Lower(p) = tok.kind else { unreachable!() };
                assoc_params.push(p);
            }
            return Some(TraitItem::AssociatedType {
                name,
                params: assoc_params,
            });
        }

        // Method: name : type_scheme  (or name params = expr for default)
        let method_name = match self.peek() {
            TokenKind::Lower(_) => Some(self.expect_lower("expected method name").ok()?),
            _ => {
                self.error("expected method name or 'type' in trait definition");
                return None;
            }
        };
        if let Some((name, name_span)) = method_name {

            if self.at(&TokenKind::Colon) {
                self.advance();
                let ts = self.parse_type_scheme()?;

                // Check for default body on next line.
                // For simplicity, don't handle default bodies in this pass.
                return Some(TraitItem::Method {
                    name,
                    name_span,
                    ty: ts,
                    default_params: Vec::new(),
                    default_body: None,
                });
            }

            // Default implementation: name params = expr
            let mut params = Vec::new();
            while self.can_start_pat() && !self.at(&TokenKind::Eq) {
                if let Some(p) = self.try_parse_pat() {
                    params.push(p);
                } else {
                    break;
                }
            }

            if self.eat(&TokenKind::Eq) {
                let body = self.parse_expr()?;
                // We need a type for the trait item — use a hole for inference.
                return Some(TraitItem::Method {
                    name,
                    name_span,
                    ty: TypeScheme {
                        constraints: vec![],
                        ty: Spanned::new(TypeKind::Hole, self.span()),
                    },
                    default_params: params,
                    default_body: Some(body),
                });
            }

            // Method name consumed but no ':' (type signature) or '=' (default body) found
            self.error(format!("expected ':' or '=' after method name '{}'", name));
        }

        None
    }

    // ── impl ─────────────────────────────────────────────────────────

    fn parse_impl_decl(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("impl declaration", |this| {
            this.advance(); // consume `impl`

            // Parse optional constraints: (Constraint =>)*
            let mut constraints = Vec::new();
            let saved = this.save();
            if let Some(cs) = this.try_parse_constraints() {
                constraints = cs;
            } else {
                this.restore(saved);
            }

            let (trait_name, _) = this.expect_upper("expected trait name in impl").ok()?;

            // Parse type arguments.
            let mut args = Vec::new();
            while this.can_start_type_atom()
                && !this.at(&TokenKind::Where)
                && !this.at(&TokenKind::Newline)
                && !this.at_eof()
            {
                if let Some(ty) = this.try_parse_type_atom() {
                    args.push(ty);
                } else {
                    break;
                }
            }

            this.expect(&TokenKind::Where, "expected 'where' in impl declaration")
                .ok()?;

            let items = this.parse_block(|p| p.parse_impl_item());

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Impl {
                    trait_name,
                    args,
                    constraints,
                    items,
                },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    fn parse_impl_item(&mut self) -> Option<ImplItem> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }

        // Associated type: `type Name args* = type`
        if self.at(&TokenKind::Type) {
            self.advance();
            let (name, _) = self.expect_upper("expected associated type name").ok()?;
            let mut assoc_args = Vec::new();
            while self.can_start_type_atom() && !self.at(&TokenKind::Eq) {
                if let Some(ty) = self.try_parse_type_atom() {
                    assoc_args.push(ty);
                } else {
                    break;
                }
            }
            self.expect(&TokenKind::Eq, "expected '=' in associated type definition")
                .ok()?;
            let ty = self.parse_type()?;
            return Some(ImplItem::AssociatedType {
                name,
                args: assoc_args,
                ty,
            });
        }

        // Method: name params* = expr
        let method_name = match self.peek() {
            TokenKind::Lower(_) => Some(self.expect_lower("expected method name in impl").ok()?),
            _ => {
                self.error("expected method name or 'type' in impl definition");
                return None;
            }
        };
        if let Some((name, name_span)) = method_name {
            let mut params = Vec::new();
            while self.can_start_pat() && !self.at(&TokenKind::Eq) {
                if let Some(p) = self.try_parse_pat() {
                    params.push(p);
                } else {
                    break;
                }
            }
            self.expect(&TokenKind::Eq, "expected '=' in method definition")
                .ok()?;
            let body = self.parse_expr()?;
            return Some(ImplItem::Method { name, name_span, params, body });
        }

        None
    }

    // ── route ────────────────────────────────────────────────────────

    fn parse_route_decl(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("route declaration", |this| {
            this.advance(); // consume `route`

            let (name, _) = this.expect_upper("expected route name").ok()?;

            // Composite: `route Api = TodoApi | AdminApi`
            if this.eat(&TokenKind::Eq) {
                let mut components = Vec::new();
                let (first, _) = this.expect_upper("expected route name in composite").ok()?;
                components.push(first);
                while this.eat(&TokenKind::Pipe) {
                    let (comp, _) = this.expect_upper("expected route name after '|'").ok()?;
                    components.push(comp);
                }
                let end = this.prev_span();
                return Some(Decl {
                    node: DeclKind::RouteComposite { name, components },
                    span: Span::new(start.start, end.end),
                    exported: false,
                });
            }

            this.expect(&TokenKind::Where, "expected 'where' or '=' after route name")
                .ok()?;

            let no_prefix: Vec<PathSegment> = vec![];
            let entries = this.parse_route_entries_with_prefix(&no_prefix);

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Route { name, entries },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    /// Parse route entries, supporting path prefix nesting.
    /// A line starting with `/` (no HTTP method) introduces a prefix group;
    /// nested entries under it have the prefix prepended to their paths.
    fn parse_route_entries_with_prefix(&mut self, prefix: &[PathSegment]) -> Vec<RouteEntry> {
        self.skip_newlines();
        if self.at_eof() {
            return vec![];
        }
        let indent = self.column_of(&self.span());
        let mut entries = vec![];
        loop {
            self.skip_newlines();
            if self.at_eof() {
                break;
            }
            let col = self.column_of(&self.span());
            if col < indent {
                break;
            }
            if self.at(&TokenKind::Slash) {
                // Path prefix group: `/prefix` followed by nested entries
                let prefix_path = self.parse_route_path();
                let mut combined = prefix.to_vec();
                combined.extend(prefix_path);
                let nested = self.parse_route_entries_with_prefix(&combined);
                entries.extend(nested);
            } else {
                // Route entry (starts with HTTP method)
                match self.parse_route_entry() {
                    Some(mut entry) => {
                        let mut full_path = prefix.to_vec();
                        full_path.extend(entry.path);
                        entry.path = full_path;
                        entries.push(entry);
                    }
                    None => break,
                }
            }
        }
        entries
    }

    fn parse_route_entry(&mut self) -> Option<RouteEntry> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }

        // Expect HTTP method (GET, POST, PUT, DELETE, PATCH) as Upper identifier.
        let method = match self.peek() {
            TokenKind::Upper(m) => match m.as_str() {
                "GET" => Some(HttpMethod::Get),
                "POST" => Some(HttpMethod::Post),
                "PUT" => Some(HttpMethod::Put),
                "DELETE" => Some(HttpMethod::Delete),
                "PATCH" => Some(HttpMethod::Patch),
                _ => {
                    self.error(format!(
                        "expected HTTP method (GET, POST, PUT, DELETE, PATCH), found '{}'", m
                    ));
                    None
                }
            },
            _ => {
                self.error(format!(
                    "expected HTTP method (GET, POST, PUT, DELETE, PATCH), found '{:?}'",
                    self.peek()
                ));
                None
            }
        };

        let method = method?;
        self.advance();

        // Optional body fields: `{name: Type, ...}`
        let mut body_fields = Vec::new();
        if self.at(&TokenKind::LBrace) {
            self.advance();
            if !self.at(&TokenKind::RBrace) {
                loop {
                    self.skip_newlines();
                    let (fname, _) = self.expect_lower("expected field name in route body").ok()?;
                    self.expect(&TokenKind::Colon, "expected ':' after field name").ok()?;
                    let ty = self.parse_type()?;
                    body_fields.push(Field {
                        name: fname,
                        value: ty,
                    });
                    self.skip_newlines();
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(&TokenKind::RBrace, "expected '}' to close route body fields")
                .ok()?;
        }

        // Parse path: /segment/{param: Type}/...
        let path = self.parse_route_path();

        // Optional query params: ?{name: Type, ...}
        self.skip_newlines();
        let mut query_params = Vec::new();
        if self.eat(&TokenKind::Question) {
            if self.eat(&TokenKind::LBrace) {
                if !self.at(&TokenKind::RBrace) {
                    loop {
                        self.skip_newlines();
                        let (qname, _) =
                            self.expect_lower("expected query param name").ok()?;
                        self.expect(&TokenKind::Colon, "expected ':' after query param name")
                            .ok()?;
                        let ty = self.parse_type()?;
                        query_params.push(Field {
                            name: qname,
                            value: ty,
                        });
                        self.skip_newlines();
                        if !self.eat(&TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(&TokenKind::RBrace, "expected '}' to close query params")
                    .ok()?;
            }
        }

        // Optional request headers: `headers {name: Type, ...}`
        self.skip_newlines();
        let request_headers = self.parse_route_headers();

        // Optional response type: `-> Type`
        self.skip_newlines();
        // Set stop_type_at_headers so parse_type won't consume `headers` as a type variable.
        let response_ty = if self.eat(&TokenKind::Arrow) {
            self.stop_type_at_headers = true;
            let ty = self.parse_type();
            self.stop_type_at_headers = false;
            let ty = ty?;
            Some(ty)
        } else {
            None
        };

        // Optional response headers: `headers {name: Type, ...}`
        self.skip_newlines();
        let response_headers = self.parse_route_headers();

        // `= ConstructorName`
        self.skip_newlines();
        self.expect(&TokenKind::Eq, "expected '=' before route constructor name")
            .ok()?;
        let (constructor, _) = self
            .expect_upper("expected constructor name in route entry")
            .ok()?;

        Some(RouteEntry {
            method,
            path,
            body_fields,
            query_params,
            request_headers,
            response_ty,
            response_headers,
            constructor,
        })
    }

    fn parse_route_path(&mut self) -> Vec<PathSegment> {
        let mut segments = Vec::new();
        // A path starts with `/` which the lexer tokenizes as `Slash`.
        while self.at(&TokenKind::Slash) {
            self.advance(); // consume `/`
            if self.at(&TokenKind::LBrace) {
                // Path parameter: {name: Type}
                self.advance();
                if let Ok((pname, _)) = self.expect_lower("expected parameter name in path") {
                    if self.eat(&TokenKind::Colon) {
                        if let Some(ty) = self.parse_type() {
                            segments.push(PathSegment::Param { name: pname, ty });
                        }
                    } else {
                        self.error(format!(
                            "expected ':' and type after path parameter '{}' (e.g., {{{}: Int}})",
                            pname, pname
                        ));
                    }
                }
                let _ = self.expect(&TokenKind::RBrace, "expected '}' to close path parameter");
            } else if matches!(self.peek(), TokenKind::Lower(s) if s != "headers")
                || (matches!(self.peek(), TokenKind::Lower(s) if s == "headers")
                    && !matches!(self.peek_ahead(1), TokenKind::LBrace))
            {
                let tok = self.advance();
                let TokenKind::Lower(s) = tok.kind else { unreachable!() };
                let mut seg = s;
                while self.at(&TokenKind::Minus) && matches!(self.peek_ahead(1), TokenKind::Lower(_) | TokenKind::Upper(_)) {
                    self.advance(); // consume `-`
                    let next = self.advance();
                    match next.kind {
                        TokenKind::Lower(s) | TokenKind::Upper(s) => {
                            seg.push('-');
                            seg.push_str(&s);
                        }
                        _ => unreachable!(),
                    }
                }
                segments.push(PathSegment::Literal(seg));
            } else if matches!(self.peek(), TokenKind::Upper(_)) {
                // uppercase segment like /api/v1 — unlikely but handle
                let tok = self.advance();
                let TokenKind::Upper(s) = tok.kind else { unreachable!() };
                let mut seg = s;
                while self.at(&TokenKind::Minus) && matches!(self.peek_ahead(1), TokenKind::Lower(_) | TokenKind::Upper(_)) {
                    self.advance(); // consume `-`
                    let next = self.advance();
                    match next.kind {
                        TokenKind::Lower(s) | TokenKind::Upper(s) => {
                            seg.push('-');
                            seg.push_str(&s);
                        }
                        _ => unreachable!(),
                    }
                }
                segments.push(PathSegment::Literal(seg));
            } else if self.peek().keyword_str().is_some() {
                let tok = self.advance();
                let mut seg = tok.kind.keyword_str().unwrap().to_string();
                while self.at(&TokenKind::Minus) && matches!(self.peek_ahead(1), TokenKind::Lower(_) | TokenKind::Upper(_)) {
                    self.advance(); // consume `-`
                    let next = self.advance();
                    match next.kind {
                        TokenKind::Lower(s) | TokenKind::Upper(s) => {
                            seg.push('-');
                            seg.push_str(&s);
                        }
                        _ => unreachable!(),
                    }
                }
                segments.push(PathSegment::Literal(seg));
            } else {
                // Just a trailing `/`
            }
        }
        segments
    }

    /// Parse an optional `headers {name: Type, ...}` block in a route entry.
    /// Returns empty vec if no `headers` keyword is present.
    fn parse_route_headers(&mut self) -> Vec<Field<Type>> {
        if !matches!(self.peek(), TokenKind::Lower(s) if s == "headers") {
            return Vec::new();
        }
        self.advance(); // consume `headers`
        self.parse_route_header_fields()
    }

    /// Parse `{name: Type, ...}` header fields (the `headers` keyword already consumed).
    fn parse_route_header_fields(&mut self) -> Vec<Field<Type>> {
        let mut fields = Vec::new();
        if self.eat(&TokenKind::LBrace) {
            if !self.at(&TokenKind::RBrace) {
                loop {
                    self.skip_newlines();
                    let (fname, _) = match self.expect_lower("expected header field name") {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    if self.expect(&TokenKind::Colon, "expected ':' after header field name").is_err() {
                        break;
                    }
                    let ty = match self.parse_type() {
                        Some(t) => t,
                        None => break,
                    };
                    fields.push(Field {
                        name: fname,
                        value: ty,
                    });
                    self.skip_newlines();
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            let _ = self.expect(&TokenKind::RBrace, "expected '}' to close headers");
        }
        fields
    }

    // ── migrate ──────────────────────────────────────────────────────

    fn parse_migrate(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("migrate declaration", |this| {
            this.advance(); // consume `migrate`

            // Expect `*name`
            this.expect(&TokenKind::Star, "expected '*' before relation name in migrate")
                .ok()?;
            let (relation, _) = this
                .expect_lower("expected relation name after '*' in migrate")
                .ok()?;

            this.skip_newlines();
            // `from`/`to`/`using` clause keywords sit at one indent inside
            // the migrate header. Set `block_indent` to the column of the
            // first clause so that multi-line type continuations (in
            // `parse_type_app`) only fire when the next line is indented
            // *past* the sibling clause keywords, not at their column.
            let prev_block_indent = this.block_indent;
            this.block_indent = this.column_of(&this.span());

            if !matches!(this.peek(), TokenKind::Lower(s) if s == "from") {
                this.error("expected 'from' in migrate declaration");
                this.block_indent = prev_block_indent;
                return None;
            }
            this.advance();

            let from_ty = this.parse_type()?;

            this.skip_newlines();
            // `to`
            if !matches!(this.peek(), TokenKind::Lower(s) if s == "to") {
                this.error("expected 'to' in migrate declaration");
                this.block_indent = prev_block_indent;
                return None;
            }
            this.advance();

            let to_ty = this.parse_type()?;

            this.skip_newlines();
            // `using`
            if !matches!(this.peek(), TokenKind::Lower(s) if s == "using") {
                this.error("expected 'using' in migrate declaration");
                this.block_indent = prev_block_indent;
                return None;
            }
            this.advance();
            this.block_indent = prev_block_indent;

            let using_fn = this.parse_expr()?;

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Migrate {
                    relation,
                    from_ty,
                    to_ty,
                    using_fn,
                },
                span: Span::new(start.start, end.end),
                exported: false,
            })
        })
    }

    /// Try to parse `(Constraint =>)+`. Returns None if it doesn't look like constraints.
    fn try_parse_constraints(&mut self) -> Option<Vec<Constraint>> {
        let mut constraints = Vec::new();
        loop {
            let saved = self.save();
            // Allow newlines between constraints (e.g. after a previous `=>`).
            self.skip_newlines();
            if matches!(self.peek(), TokenKind::Upper(_)) {
                let tok = self.advance();
                let TokenKind::Upper(trait_name) = tok.kind else { unreachable!() };
                let mut args = Vec::new();
                while self.can_start_type_atom()
                    && !self.at(&TokenKind::FatArrow)
                    && !self.at(&TokenKind::Where)
                    && !self.at(&TokenKind::Newline)
                    && !self.at_eof()
                {
                    if let Some(ty) = self.try_parse_type_atom() {
                        args.push(ty);
                    } else {
                        break;
                    }
                }
                // Allow `=>` on the next line.
                let pre_arrow = self.save();
                self.skip_newlines();
                if self.eat(&TokenKind::FatArrow) {
                    constraints.push(Constraint {
                        trait_name,
                        args,
                    });
                    continue;
                }
                self.restore(pre_arrow);
            }
            self.restore(saved);
            break;
        }
        if constraints.is_empty() {
            None
        } else {
            Some(constraints)
        }
    }

    fn prev_span(&self) -> Span {
        if self.pos > 0 {
            self.tokens[self.pos - 1].span
        } else {
            Span::new(0, 0)
        }
    }
}

// ── Expressions ─────────────────────────────────────────────────────

impl Parser {
    fn parse_expr(&mut self) -> Option<Expr> {
        self.skip_newlines();
        match self.peek() {
            TokenKind::Backslash => self.parse_lambda(),
            TokenKind::If => self.parse_if(),
            TokenKind::Case => self.parse_case(),
            TokenKind::Do => self.parse_do_expr(),
            TokenKind::Star => {
                // `*name = expr` is a set expression; otherwise just an
                // ordinary source-ref expression handled by Pratt parsing.
                let start = self.span();
                let target = self.parse_expr_bp(0)?;
                if self.eat(&TokenKind::Eq) {
                    let value = self.parse_expr()?;
                    let end_sp = value.span;
                    Some(Spanned::new(
                        ExprKind::Set {
                            target: Box::new(target),
                            value: Box::new(value),
                        },
                        Span::new(start.start, end_sp.end),
                    ))
                } else {
                    Some(target)
                }
            }
            TokenKind::Replace => {
                // `replace *rel = expr` is a replace-set expression. Otherwise
                // `replace` is treated as a regular identifier.
                let mut offset = 1;
                while self.peek_ahead(offset) == &TokenKind::Newline {
                    offset += 1;
                }
                if self.peek_ahead(offset) == &TokenKind::Star {
                    let replace_start = self.span();
                    self.advance(); // consume `replace`
                    self.skip_newlines();
                    self.parse_set_with_start(true, replace_start)
                } else {
                    // `replace` used as a regular identifier — fall through to
                    // Pratt parsing so binary operators and application work.
                    self.parse_expr_bp(0)
                }
            }
            TokenKind::Atomic => {
                let start = self.span();
                self.advance();
                let e = self.parse_expr()?;
                let end_sp = e.span;
                Some(Spanned::new(
                    ExprKind::Atomic(Box::new(e)),
                    Span::new(start.start, end_sp.end),
                ))
            }
            TokenKind::Refine => {
                let start = self.span();
                self.advance();
                let e = self.parse_expr()?;
                let end_sp = e.span;
                Some(Spanned::new(
                    ExprKind::Refine(Box::new(e)),
                    Span::new(start.start, end_sp.end),
                ))
            }
            TokenKind::Let => self.parse_let_in_expr(),
            _ => self.parse_expr_bp(0),
        }
    }

    /// Pratt parsing entry point.
    fn parse_expr_bp(&mut self, min_bp: u8) -> Option<Expr> {
        let mut lhs = self.parse_unary()?;

        loop {
            // Skip newlines in certain contexts to allow multiline expressions.
            // But be careful: a newline at column 0 might be a new declaration.
            let saved_pos = self.save();
            self.skip_newlines();

            // If the next token is at column 0 and we're NOT inside delimiters,
            // it's a new declaration — don't consume it as a binary operator.
            if self.delimiter_depth == 0 && self.column_of(&self.span()) == 0 {
                self.restore(saved_pos);
                break;
            }

            // If `*` is immediately adjacent to a lowercase identifier with no
            // whitespace, it's a source reference (`*name`), not multiplication.
            // This matches `can_start_atom`'s rule and prevents the binop loop
            // from gobbling a `*relation = ...` statement on the next line.
            if matches!(self.peek(), TokenKind::Star) {
                if let Some(next) = self.tokens.get(self.pos + 1) {
                    let cur_end = self.peek_token().span.end;
                    if matches!(next.kind, TokenKind::Lower(_))
                        && next.span.start == cur_end
                    {
                        self.restore(saved_pos);
                        break;
                    }
                }
            }

            let (op, l_bp, r_bp) = match self.peek() {
                TokenKind::PipeGt => (BinOp::Pipe, 1, 2),
                TokenKind::OrOr => (BinOp::Or, 3, 4),
                TokenKind::AndAnd => (BinOp::And, 5, 6),
                TokenKind::EqEq => (BinOp::Eq, 7, 8),
                TokenKind::BangEq => (BinOp::Neq, 7, 8),
                TokenKind::Lt => (BinOp::Lt, 9, 10),
                TokenKind::Gt => (BinOp::Gt, 9, 10),
                TokenKind::Le => (BinOp::Le, 9, 10),
                TokenKind::Ge => (BinOp::Ge, 9, 10),
                TokenKind::PlusPlus => (BinOp::Concat, 11, 11), // right-assoc: use same bp
                TokenKind::Plus => (BinOp::Add, 13, 14),
                TokenKind::Minus => (BinOp::Sub, 13, 14),
                TokenKind::Star => (BinOp::Mul, 15, 16),
                TokenKind::Slash => (BinOp::Div, 15, 16),
                _ => {
                    self.restore(saved_pos);
                    break;
                }
            };

            if l_bp < min_bp {
                self.restore(saved_pos);
                break;
            }

            self.advance(); // consume operator
            self.skip_newlines();
            // Allow let/if/case/do/lambda/atomic/refine on the RHS of
            // binary operators.  These are handled by `parse_expr` but not by
            // the Pratt sub-parser, so we delegate to `parse_expr` when we see
            // one of these keyword tokens.
            let rhs = if matches!(
                self.peek(),
                TokenKind::Let
                    | TokenKind::If
                    | TokenKind::Case
                    | TokenKind::Do
                    | TokenKind::Backslash
                    | TokenKind::Atomic
                    | TokenKind::Refine
            ) {
                self.parse_expr()?
            } else {
                self.parse_expr_bp(r_bp)?
            };

            let span = Span::new(lhs.span.start, rhs.span.end);
            lhs = Spanned::new(
                ExprKind::BinOp {
                    op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                },
                span,
            );
        }

        Some(lhs)
    }

    fn parse_unary(&mut self) -> Option<Expr> {
        match self.peek() {
            TokenKind::Minus => {
                if !self.enter_recursion() { return None; }
                let start = self.span();
                self.advance();
                let operand = self.parse_unary();
                self.recursion_depth -= 1;
                let operand = operand?;
                let span = Span::new(start.start, operand.span.end);
                Some(Spanned::new(
                    ExprKind::UnaryOp {
                        op: UnaryOp::Neg,
                        operand: Box::new(operand),
                    },
                    span,
                ))
            }
            TokenKind::Not => {
                if !self.enter_recursion() { return None; }
                let start = self.span();
                self.advance();
                let operand = self.parse_unary();
                self.recursion_depth -= 1;
                let operand = operand?;
                let span = Span::new(start.start, operand.span.end);
                Some(Spanned::new(
                    ExprKind::UnaryOp {
                        op: UnaryOp::Not,
                        operand: Box::new(operand),
                    },
                    span,
                ))
            }
            _ => self.parse_application(),
        }
    }

    fn parse_application(&mut self) -> Option<Expr> {
        let mut func = self.parse_postfix()?;

        loop {
            if self.can_start_atom() {
                let arg = self.parse_postfix()?;
                let span = Span::new(func.span.start, arg.span.end);
                func = Spanned::new(
                    ExprKind::App {
                        func: Box::new(func),
                        arg: Box::new(arg),
                    },
                    span,
                );
                continue;
            }

            // Try to continue across newlines: if the next non-newline token
            // is indented past the current block indent, treat it as a
            // continuation of this application (like multi-line fn args).
            let saved = self.save();
            self.skip_newlines();
            if !self.at_eof()
                && self.column_of(&self.span()) > self.block_indent
                && self.can_start_atom()
            {
                let arg = self.parse_postfix()?;
                let span = Span::new(func.span.start, arg.span.end);
                func = Spanned::new(
                    ExprKind::App {
                        func: Box::new(func),
                        arg: Box::new(arg),
                    },
                    span,
                );
            } else {
                self.restore(saved);
                break;
            }
        }

        Some(func)
    }

    /// Check if the current token can start an atom in application position.
    fn can_start_atom(&self) -> bool {
        match self.peek() {
            TokenKind::Int(_)
            | TokenKind::Float(_)
            | TokenKind::Text(_)
            | TokenKind::Bytes(_)
            | TokenKind::Bool(_)
            | TokenKind::Upper(_)
            | TokenKind::LParen
            | TokenKind::LBrace
            | TokenKind::LBracket
            | TokenKind::Replace
            | TokenKind::Do => true,
            // `yield` is not a keyword but should not start application atoms
            // (like keywords), to prevent `f; yield x` from parsing as `f yield x`
            // in inline do-blocks where `;` is lexed as Newline.
            TokenKind::Lower(n) => n != "yield",
            TokenKind::Star => {
                // Source ref `*name` only when `*` is immediately adjacent to a Lower token
                // (no whitespace). This avoids ambiguity with the `*` multiplication operator.
                if let Some(next) = self.tokens.get(self.pos + 1) {
                    let cur_end = self.peek_token().span.end;
                    matches!(next.kind, TokenKind::Lower(_)) && next.span.start == cur_end
                } else {
                    false
                }
            }
            TokenKind::Ampersand => {
                // Derived ref `&name` only when `&` is immediately adjacent to a Lower token.
                if let Some(next) = self.tokens.get(self.pos + 1) {
                    let cur_end = self.peek_token().span.end;
                    matches!(next.kind, TokenKind::Lower(_)) && next.span.start == cur_end
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Parse atom with constructor greedy binding.
    /// A bare constructor greedily binds with the next atom, so
    /// `f Circle {radius: 5}` parses as `f (Circle {radius: 5})`
    /// rather than `(f Circle) {radius: 5}`.
    /// Nested constructors are handled recursively:
    /// `Just Nothing {}` parses as `Just (Nothing {})`.
    fn parse_constructor_or_atom(&mut self) -> Option<Expr> {
        let expr = self.parse_atom()?;
        if matches!(expr.node, ExprKind::Constructor(_)) && self.can_start_atom() {
            if !self.enter_recursion() { return None; }
            let arg = self.parse_constructor_or_atom();
            self.recursion_depth -= 1;
            let arg = arg?;
            let span = Span::new(expr.span.start, arg.span.end);
            Some(Spanned::new(
                ExprKind::App {
                    func: Box::new(expr),
                    arg: Box::new(arg),
                },
                span,
            ))
        } else {
            Some(expr)
        }
    }

    fn parse_postfix(&mut self) -> Option<Expr> {
        let mut expr = self.parse_constructor_or_atom()?;

        loop {
            if self.at(&TokenKind::Dot) {
                self.advance();
                let (field, field_span) =
                    self.expect_lower("expected field name after '.'").ok()?;
                let span = Span::new(expr.span.start, field_span.end);
                expr = Spanned::new(
                    ExprKind::FieldAccess {
                        expr: Box::new(expr),
                        field,
                    },
                    span,
                );
            } else if self.at(&TokenKind::At) && matches!(self.peek_ahead(1), TokenKind::LParen) {
                self.advance(); // consume `@`
                self.advance(); // consume `(`
                self.delimiter_depth += 1;
                let time = self.parse_expr();
                if time.is_none() {
                    self.delimiter_depth -= 1;
                    return None;
                }
                let time = time.unwrap();
                self.delimiter_depth -= 1;
                let end_tok = self
                    .expect(&TokenKind::RParen, "expected ')' to close temporal query '@(...)'")
                    .ok()?;
                let span = Span::new(expr.span.start, end_tok.span.end);
                expr = Spanned::new(
                    ExprKind::At {
                        relation: Box::new(expr),
                        time: Box::new(time),
                    },
                    span,
                );
            } else {
                break;
            }
        }

        Some(expr)
    }

    /// If the next token is a time-unit identifier (`ms`, `seconds`, `minutes`,
    /// `hours`, `days`, `weeks`), consume it and desugar `n unit` into `n * factor`
    /// where factor is the millisecond equivalent.
    fn maybe_time_unit(&mut self, lit: Expr) -> Option<Expr> {
        let factor: Option<&str> = match self.peek() {
            TokenKind::Lower(u) => match u.as_str() {
                "ms" => Some("1"),
                "seconds" => Some("1000"),
                "minutes" => Some("60000"),
                "hours" => Some("3600000"),
                "days" => Some("86400000"),
                "weeks" => Some("604800000"),
                _ => None,
            },
            _ => None,
        };
        match factor {
            Some(f) => {
                let unit_tok = self.advance();
                let span = Span::new(lit.span.start, unit_tok.span.end);
                Some(Spanned::new(
                    ExprKind::BinOp {
                        op: BinOp::Mul,
                        lhs: Box::new(lit),
                        rhs: Box::new(Spanned::new(
                            ExprKind::Lit(Literal::Int(f.to_string())),
                            unit_tok.span,
                        )),
                    },
                    span,
                ))
            }
            None => {
                // Try unit annotation: `42.0<m>`, `999<usd>`
                if let Some(unit) = self.try_parse_unit_annotation() {
                    let span = Span::new(lit.span.start, self.prev_span().end);
                    Some(Spanned::new(
                        ExprKind::UnitLit {
                            value: Box::new(lit),
                            unit,
                        },
                        span,
                    ))
                } else {
                    Some(lit)
                }
            }
        }
    }

    fn parse_atom(&mut self) -> Option<Expr> {
        let start = self.span();
        match self.peek() {
            TokenKind::Int(_) => {
                let tok = self.advance();
                let TokenKind::Int(n) = tok.kind else { unreachable!() };
                let lit = Spanned::new(ExprKind::Lit(Literal::Int(n)), tok.span);
                self.maybe_time_unit(lit)
            }
            TokenKind::Float(_) => {
                let tok = self.advance();
                let TokenKind::Float(f) = tok.kind else { unreachable!() };
                let lit = Spanned::new(ExprKind::Lit(Literal::Float(f)), tok.span);
                self.maybe_time_unit(lit)
            }
            TokenKind::Text(_) => {
                let tok = self.advance();
                let TokenKind::Text(s) = tok.kind else { unreachable!() };
                Some(Spanned::new(ExprKind::Lit(Literal::Text(s)), tok.span))
            }
            TokenKind::Bytes(_) => {
                let tok = self.advance();
                let TokenKind::Bytes(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(ExprKind::Lit(Literal::Bytes(b)), tok.span))
            }
            TokenKind::Bool(_) => {
                let tok = self.advance();
                let TokenKind::Bool(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(ExprKind::Lit(Literal::Bool(b)), tok.span))
            }
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                Some(Spanned::new(ExprKind::Var(name), tok.span))
            }
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(name) = tok.kind else { unreachable!() };
                Some(Spanned::new(ExprKind::Constructor(name), tok.span))
            }
            TokenKind::Replace => {
                let tok = self.advance();
                Some(Spanned::new(ExprKind::Var("replace".into()), tok.span))
            }
            TokenKind::Star => {
                // *name — source reference
                self.advance();
                match self.peek() {
                    TokenKind::Lower(_) => {
                        let tok = self.advance();
                        let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                        Some(Spanned::new(
                            ExprKind::SourceRef(name),
                            Span::new(start.start, tok.span.end),
                        ))
                    }
                    _ => {
                        self.error("expected identifier after '*' for source reference");
                        None
                    }
                }
            }
            TokenKind::Ampersand => {
                // &name — derived reference
                self.advance();
                match self.peek() {
                    TokenKind::Lower(_) => {
                        let tok = self.advance();
                        let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                        Some(Spanned::new(
                            ExprKind::DerivedRef(name),
                            Span::new(start.start, tok.span.end),
                        ))
                    }
                    _ => {
                        self.error("expected identifier after '&' for derived reference");
                        None
                    }
                }
            }
            TokenKind::LParen => {
                self.advance();
                self.delimiter_depth += 1;
                // Check for empty parens `()` as unit.
                if self.eat(&TokenKind::RParen) {
                    self.delimiter_depth -= 1;
                    return Some(Spanned::new(
                        ExprKind::Record(vec![]),
                        Span::new(start.start, self.prev_span().end),
                    ));
                }
                let inner = self.parse_expr();
                if inner.is_none() {
                    self.delimiter_depth -= 1;
                    return None;
                }
                let inner = inner.unwrap();
                self.skip_newlines();
                // Check for type annotation: `(expr : Type)`
                if self.eat(&TokenKind::Colon) {
                    let ty = match self.parse_type() {
                        Some(t) => t,
                        None => {
                            self.delimiter_depth -= 1;
                            return None;
                        }
                    };
                    let end_tok = self
                        .expect(
                            &TokenKind::RParen,
                            "unclosed '(' — expected matching ')' after type annotation",
                        );
                    self.delimiter_depth -= 1;
                    let end_tok = end_tok.ok()?;
                    let span = Span::new(start.start, end_tok.span.end);
                    return Some(Spanned::new(
                        ExprKind::Annot {
                            expr: Box::new(inner),
                            ty,
                        },
                        span,
                    ));
                }
                let end_tok = self
                    .expect(
                        &TokenKind::RParen,
                        "unclosed '(' — expected matching ')'",
                    );
                self.delimiter_depth -= 1;
                let end_tok = end_tok.ok()?;
                // Keep the inner expression but update span to include parens.
                Some(Spanned::new(
                    inner.node,
                    Span::new(start.start, end_tok.span.end),
                ))
            }
            TokenKind::Do => self.parse_do_expr(),
            TokenKind::LBrace => {
                self.advance();
                self.delimiter_depth += 1;
                let result = self.parse_record_or_update(start);
                self.delimiter_depth -= 1;
                result
            }
            TokenKind::LBracket => {
                self.advance();
                self.delimiter_depth += 1;
                let result = self.parse_list_expr(start);
                self.delimiter_depth -= 1;
                result
            }
            TokenKind::Underscore => {
                self.error("unexpected '_' in expression — wildcards are only for patterns");
                self.advance();
                None
            }
            _ => {
                self.error("expected expression");
                None
            }
        }
    }

    fn parse_record_or_update(&mut self, start: Span) -> Option<Expr> {
        // Already consumed `{`.
        self.skip_newlines();

        // Empty record `{}`
        if self.eat(&TokenKind::RBrace) {
            return Some(Spanned::new(
                ExprKind::Record(vec![]),
                Span::new(start.start, self.prev_span().end),
            ));
        }

        // We need to distinguish:
        // 1. Record literal: {name: expr, ...}
        // 2. Record update: {base | name: expr, ...}
        // 3. Punned fields: {name, age} (shorthand for {name: name, age: age})
        //    or {expr.field, ...} (shorthand for {field: expr.field})

        // Parse first element to decide.
        let saved = self.save();
        let diag_count = self.diagnostics.len();

        // Try to detect if this is a record update: expr `|` fields
        // The base expression in a record update is followed by `|`.
        // We parse an expression optimistically; if we see `|` after, it's an update.
        if let Some(first_expr) = self.parse_expr() {
            self.skip_newlines();
            if self.eat(&TokenKind::Pipe) {
                // Record update: {base | field: expr, ...}
                let mut fields = Vec::new();
                self.skip_newlines();
                if !self.at(&TokenKind::RBrace) {
                    loop {
                        self.skip_newlines();
                        let (fname, _) = self
                            .expect_lower("expected field name in record update")
                            .ok()?;
                        self.expect(
                            &TokenKind::Colon,
                            "expected ':' after field name in record update",
                        )
                        .ok()?;
                        let val = self.parse_expr()?;
                        fields.push(Field {
                            name: fname,
                            value: val,
                        });
                        self.skip_newlines();
                        if !self.eat(&TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.skip_newlines();
                let end_tok = self
                    .expect(&TokenKind::RBrace, "expected '}' to close record update")
                    .ok()?;
                return Some(Spanned::new(
                    ExprKind::RecordUpdate {
                        base: Box::new(first_expr),
                        fields,
                    },
                    Span::new(start.start, end_tok.span.end),
                ));
            }

            // Not an update. Could be:
            // - Comma-separated punned fields: {name, age}
            // - A record literal where the first element is name: expr
            // Check if the first element was parsed as something that looks like field punning.
            // We need to restart.
            self.restore(saved);
            self.diagnostics.truncate(diag_count);
        } else {
            self.restore(saved);
            self.diagnostics.truncate(diag_count);
        }

        // Now try to parse as record literal or punned fields.
        // If we see `lower:` it's a record literal.
        // If we see `lower,` or `lower}` it's punned fields.
        // If we see an expression followed by `,` or `}` it's punned fields.
        //
        // Field-level error recovery: when a field's value or punned
        // expression fails to parse, skip to the next `,` or `}` instead of
        // bailing on the whole record. This way a single malformed field
        // surfaces one diagnostic but doesn't suppress the rest of the
        // record (and any cascading errors from later code that depends on
        // the record having parsed).
        let mut fields: Vec<Field<Expr>> = Vec::new();
        loop {
            self.skip_newlines();
            if self.at(&TokenKind::RBrace) {
                break;
            }

            let progress_before = self.pos;
            if matches!(self.peek(), TokenKind::Lower(_)) {
                // Check if next token after the identifier is `:` (record literal field)
                if matches!(self.peek_ahead(1), TokenKind::Colon) {
                    // Record literal field: name: expr
                    let tok = self.advance(); // consume name
                    let TokenKind::Lower(fname) = tok.kind else { unreachable!() };
                    self.advance(); // consume `:`
                    match self.parse_expr() {
                        Some(val) => fields.push(Field {
                            name: fname,
                            value: val,
                        }),
                        None => self.skip_to_record_field_boundary(),
                    }
                } else {
                    // Punned field: {name} means {name: name}
                    // Or it could be an expression like {expr.field}
                    match self.parse_expr() {
                        Some(expr) => {
                            let field_name = self.extract_pun_name(&expr).unwrap_or_else(|| {
                                self.error_at(
                                    expr.span,
                                    "cannot determine field name for punned record field",
                                );
                                "?".into()
                            });
                            fields.push(Field {
                                name: field_name,
                                value: expr,
                            });
                        }
                        None => self.skip_to_record_field_boundary(),
                    }
                }
            } else {
                // Expression-based pun: {expr.field}
                match self.parse_expr() {
                    Some(expr) => {
                        let field_name = self.extract_pun_name(&expr).unwrap_or_else(|| {
                            self.error_at(
                                expr.span,
                                "cannot determine field name for punned record field",
                            );
                            "?".into()
                        });
                        fields.push(Field {
                            name: field_name,
                            value: expr,
                        });
                    }
                    None => self.skip_to_record_field_boundary(),
                }
            }

            self.skip_newlines();
            if !self.eat(&TokenKind::Comma) {
                break;
            }
            // Defensive: if we made no progress on this iteration AND didn't
            // hit a comma, bail out to avoid an infinite loop on pathological
            // inputs (every recovery path advances at least one token, so
            // this should be unreachable in practice).
            if self.pos == progress_before {
                break;
            }
        }

        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBrace, "expected '}' to close record")
            .ok()?;

        Some(Spanned::new(
            ExprKind::Record(fields),
            Span::new(start.start, end_tok.span.end),
        ))
    }

    /// Extract the field name for a punned record field.
    /// `x` => "x", `t.name` => "name"
    fn extract_pun_name(&self, expr: &Expr) -> Option<Name> {
        match &expr.node {
            ExprKind::Var(name) => Some(name.clone()),
            ExprKind::FieldAccess { field, .. } => Some(field.clone()),
            _ => None,
        }
    }

    fn parse_list_expr(&mut self, start: Span) -> Option<Expr> {
        // Already consumed `[`.
        self.skip_newlines();
        let mut elems = Vec::new();
        if !self.at(&TokenKind::RBracket) {
            loop {
                self.skip_newlines();
                let e = self.parse_expr()?;
                elems.push(e);
                self.skip_newlines();
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBracket, "expected ']' to close list")
            .ok()?;
        Some(Spanned::new(
            ExprKind::List(elems),
            Span::new(start.start, end_tok.span.end),
        ))
    }

    fn parse_lambda(&mut self) -> Option<Expr> {
        let start = self.span();
        self.in_context("lambda expression", |this| {
            this.advance(); // consume `\`

            let mut params = Vec::new();
            while !this.at(&TokenKind::Arrow) && !this.at_eof() {
                this.skip_newlines();
                if this.at(&TokenKind::Arrow) { break; }
                // Stop consuming params if we've crossed back to column 0 outside
                // any delimiter — this prevents eating into the next declaration
                // when `->` is missing.
                if this.delimiter_depth == 0 && this.column_of(&this.span()) == 0 {
                    break;
                }
                let p = this.parse_pat()?;
                params.push(p);
            }

            this.expect(&TokenKind::Arrow, "expected '->' in lambda expression")
                .ok()?;
            let body = this.parse_expr()?;

            let end_sp = body.span;
            Some(Spanned::new(
                ExprKind::Lambda {
                    params,
                    body: Box::new(body),
                },
                Span::new(start.start, end_sp.end),
            ))
        })
    }

    fn parse_if(&mut self) -> Option<Expr> {
        let start = self.span();
        self.in_context("if expression", |this| {
            this.advance(); // consume `if`

            let cond = this.parse_expr()?;
            this.skip_newlines();
            this.expect(
                &TokenKind::Then,
                "expected 'then' after condition in 'if' expression",
            )
            .ok()?;
            let then_branch = this.parse_expr()?;
            this.skip_newlines();
            this.expect(
                &TokenKind::Else,
                "expected 'else' after 'then' branch in 'if' expression",
            )
            .ok()?;
            let else_branch = this.parse_expr()?;

            let end_sp = else_branch.span;
            Some(Spanned::new(
                ExprKind::If {
                    cond: Box::new(cond),
                    then_branch: Box::new(then_branch),
                    else_branch: Box::new(else_branch),
                },
                Span::new(start.start, end_sp.end),
            ))
        })
    }

    fn parse_case(&mut self) -> Option<Expr> {
        let start = self.span();
        self.in_context("case expression", |this| {
            this.advance(); // consume `case`

            let scrutinee = this.parse_expr()?;
            this.skip_newlines();
            this.expect(&TokenKind::Of, "expected 'of' after scrutinee in 'case' expression")
                .ok()?;

            let arms = this.parse_block(|p| p.parse_case_arm());

            let end = this.prev_span();
            Some(Spanned::new(
                ExprKind::Case {
                    scrutinee: Box::new(scrutinee),
                    arms,
                },
                Span::new(start.start, end.end),
            ))
        })
    }

    fn parse_case_arm(&mut self) -> Option<CaseArm> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }
        let pat = self.parse_pat()?;
        self.expect(
            &TokenKind::Arrow,
            "expected '->' after pattern in case arm",
        )
        .ok()?;
        let body = self.parse_expr()?;
        Some(CaseArm { pat, body })
    }

    fn parse_do_expr(&mut self) -> Option<Expr> {
        let start = self.span();
        self.in_context("do expression", |this| {
            this.advance(); // consume `do`

            let stmts = this.parse_block(|p| p.parse_stmt());

            let end = this.prev_span();
            Some(Spanned::new(
                ExprKind::Do(stmts),
                Span::new(start.start, end.end),
            ))
        })
    }

    fn parse_set_with_start(&mut self, replace: bool, start: Span) -> Option<Expr> {
        let ctx = if replace {
            "replace set expression"
        } else {
            "set expression"
        };
        self.in_context(ctx, |this| {
            // The caller has already positioned the parser at the target.
            let target = this.parse_expr_bp(0)?;

            this.expect(&TokenKind::Eq, "expected '=' after target")
                .ok()?;
            let value = this.parse_expr()?;

            let end_sp = value.span;
            let kind = if replace {
                ExprKind::ReplaceSet {
                    target: Box::new(target),
                    value: Box::new(value),
                }
            } else {
                ExprKind::Set {
                    target: Box::new(target),
                    value: Box::new(value),
                }
            };
            Some(Spanned::new(kind, Span::new(start.start, end_sp.end)))
        })
    }

    fn parse_let_in_expr(&mut self) -> Option<Expr> {
        let start = self.span();
        self.in_context("let expression", |this| {
            this.advance(); // consume `let`

            let pat = this.parse_pat()?;

            // Optional type annotation: `let x : Type = ...`
            let annot_ty = if this.at(&TokenKind::Colon) {
                this.advance();
                Some(this.parse_type()?)
            } else {
                None
            };

            this.expect(&TokenKind::Eq, "expected '=' in let binding").ok()?;
            let mut value = this.parse_expr()?;
            this.skip_newlines();
            this.expect(&TokenKind::In, "expected 'in' after let binding").ok()?;
            let body = this.parse_expr()?;

            // If there is a type annotation, wrap the value as `(value : Type)`
            // so that inference sees the constraint.
            if let Some(ty) = annot_ty {
                let sp = value.span;
                value = Spanned::new(
                    ExprKind::Annot {
                        expr: Box::new(value),
                        ty,
                    },
                    sp,
                );
            }

            // Desugar `let pat = value in body` as a lambda application.
            // `(\pat -> body) value`
            let end_sp = body.span;
            let lam = Spanned::new(
                ExprKind::Lambda {
                    params: vec![pat],
                    body: Box::new(body),
                },
                Span::new(start.start, end_sp.end),
            );
            Some(Spanned::new(
                ExprKind::App {
                    func: Box::new(lam),
                    arg: Box::new(value),
                },
                Span::new(start.start, end_sp.end),
            ))
        })
    }
}

// ── Statements ──────────────────────────────────────────────────────

impl Parser {
    fn parse_stmt(&mut self) -> Option<Stmt> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }

        // Closing delimiters from an enclosing expression end the do block
        // without an error (e.g. `(do ... )` or `[do ... ]`).
        if matches!(
            self.peek(),
            TokenKind::RParen | TokenKind::RBrace | TokenKind::RBracket
        ) {
            return None;
        }

        // `_` followed by `->` is a case arm wildcard, not a do statement.
        // Return None so the enclosing case block can claim it.
        // `_` followed by `<-` is a valid bind (`_ <- expr`), so allow that.
        if self.at(&TokenKind::Underscore) {
            let saved = self.save();
            self.advance(); // consume `_`
            let is_bind = self.at(&TokenKind::LArrow);
            self.restore(saved);
            if !is_bind {
                return None;
            }
        }

        let start = self.span();

        // `where cond`
        if self.eat(&TokenKind::Where) {
            let cond = self.parse_expr()?;
            let end_sp = cond.span;
            return Some(Spanned::new(
                StmtKind::Where { cond },
                Span::new(start.start, end_sp.end),
            ));
        }

        // `groupBy expr`
        if matches!(self.peek(), TokenKind::Lower(n) if n == "groupBy") {
            self.advance();
            let key = self.parse_expr()?;
            let end_sp = key.span;
            return Some(Spanned::new(
                StmtKind::GroupBy { key },
                Span::new(start.start, end_sp.end),
            ));
        }

        // `let pat = expr` or `let pat : Type = expr`
        if self.at(&TokenKind::Let) {
            self.advance();
            let pat = self.parse_pat()?;

            // Optional type annotation: `let x : Type = ...`
            let annot_ty = if self.at(&TokenKind::Colon) {
                self.advance();
                Some(self.parse_type()?)
            } else {
                None
            };

            self.expect(&TokenKind::Eq, "expected '=' in let statement").ok()?;
            let mut expr = self.parse_expr()?;

            // Wrap value with annotation so inference sees the constraint.
            if let Some(ty) = annot_ty {
                let sp = expr.span;
                expr = Spanned::new(
                    ExprKind::Annot {
                        expr: Box::new(expr),
                        ty,
                    },
                    sp,
                );
            }

            let end_sp = expr.span;
            return Some(Spanned::new(
                StmtKind::Let { pat, expr },
                Span::new(start.start, end_sp.end),
            ));
        }

        // Try to parse as a bind: `pat <- expr`
        // Use save/restore: parse pattern, check for `<-`.
        let saved = self.save();
        let diag_count = self.diagnostics.len();

        if let Some(pat) = self.try_parse_pat() {
            if self.eat(&TokenKind::LArrow) {
                // Committed to a bind statement — `<-` was consumed.
                // If the expression fails, return None without trying
                // to re-parse as an expression statement.
                let expr = match self.parse_expr() {
                    Some(expr) => expr,
                    None => return None,
                };
                let end_sp = expr.span;
                return Some(Spanned::new(
                    StmtKind::Bind { pat, expr },
                    Span::new(start.start, end_sp.end),
                ));
            }
        }

        // Not a bind — restore and parse as expression statement.
        self.restore(saved);
        self.diagnostics.truncate(diag_count);

        let expr = self.parse_expr()?;
        let end_sp = expr.span;
        Some(Spanned::new(
            StmtKind::Expr(expr),
            Span::new(start.start, end_sp.end),
        ))
    }
}

// ── Patterns ────────────────────────────────────────────────────────

impl Parser {
    /// Can the current token begin a pattern?
    fn can_start_pat(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::Lower(_)
                | TokenKind::Upper(_)
                | TokenKind::Underscore
                | TokenKind::LBrace
                | TokenKind::LBracket
                | TokenKind::LParen
                | TokenKind::Int(_)
                | TokenKind::Float(_)
                | TokenKind::Text(_)
                | TokenKind::Bytes(_)
                | TokenKind::Bool(_)
                | TokenKind::Minus
        )
    }

    /// Try to parse a pattern, returning None without emitting errors if it fails.
    fn try_parse_pat(&mut self) -> Option<Pat> {
        if !self.can_start_pat() {
            return None;
        }
        let saved = self.save();
        let diag_count = self.diagnostics.len();
        match self.parse_pat() {
            Some(pat) => Some(pat),
            None => {
                self.restore(saved);
                self.diagnostics.truncate(diag_count);
                None
            }
        }
    }

    fn parse_pat(&mut self) -> Option<Pat> {
        // Guard recursion here: every pattern-side recursive path
        // (parens, record fields, list elements) flows through this entry
        // point, so guarding it alone prevents stack overflow on pathological
        // input like `((((x))))`.
        if !self.enter_recursion() { return None; }
        let result = self.parse_pat_inner();
        self.recursion_depth -= 1;
        result
    }

    fn parse_pat_inner(&mut self) -> Option<Pat> {
        let start = self.span();
        match self.peek() {
            TokenKind::Underscore => {
                let tok = self.advance();
                Some(Spanned::new(PatKind::Wildcard, tok.span))
            }
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Var(name), tok.span))
            }
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(name) = tok.kind else { unreachable!() };
                // Constructor pattern: Upper payload
                // Payload can be a record pattern `{...}` or a variable.
                let payload = if self.can_start_pat_atom() {
                    self.parse_pat_atom()?
                } else {
                    // No payload — use empty record pattern.
                    Spanned::new(PatKind::Record(vec![]), tok.span)
                };
                let span = Span::new(start.start, payload.span.end);
                Some(Spanned::new(
                    PatKind::Constructor {
                        name,
                        payload: Box::new(payload),
                    },
                    span,
                ))
            }
            TokenKind::LBrace => {
                self.advance();
                self.parse_record_pat(start)
            }
            TokenKind::LBracket => {
                self.advance();
                self.parse_list_pat(start)
            }
            TokenKind::LParen => {
                self.advance();
                if self.eat(&TokenKind::RParen) {
                    return Some(Spanned::new(
                        PatKind::Record(vec![]),
                        Span::new(start.start, self.prev_span().end),
                    ));
                }
                let inner = self.parse_pat()?;
                let end_tok = self
                    .expect(&TokenKind::RParen, "expected ')' to close pattern group")
                    .ok()?;
                Some(Spanned::new(inner.node, Span::new(start.start, end_tok.span.end)))
            }
            TokenKind::Minus => {
                let minus_tok = self.advance();
                match self.peek() {
                    TokenKind::Int(_) => {
                        let tok = self.advance();
                        let TokenKind::Int(n) = tok.kind else { unreachable!() };
                        let neg = format!("-{}", n);
                        let span = Span::new(minus_tok.span.start, tok.span.end);
                        Some(Spanned::new(PatKind::Lit(Literal::Int(neg)), span))
                    }
                    TokenKind::Float(_) => {
                        let tok = self.advance();
                        let TokenKind::Float(f) = tok.kind else { unreachable!() };
                        let span = Span::new(minus_tok.span.start, tok.span.end);
                        Some(Spanned::new(PatKind::Lit(Literal::Float(-f)), span))
                    }
                    _ => {
                        self.error("expected number after '-' in pattern");
                        None
                    }
                }
            }
            TokenKind::Int(_) => {
                let tok = self.advance();
                let TokenKind::Int(n) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Int(n)), tok.span))
            }
            TokenKind::Float(_) => {
                let tok = self.advance();
                let TokenKind::Float(f) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Float(f)), tok.span))
            }
            TokenKind::Text(_) => {
                let tok = self.advance();
                let TokenKind::Text(s) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Text(s)), tok.span))
            }
            TokenKind::Bytes(_) => {
                let tok = self.advance();
                let TokenKind::Bytes(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Bytes(b)), tok.span))
            }
            TokenKind::Bool(_) => {
                let tok = self.advance();
                let TokenKind::Bool(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Bool(b)), tok.span))
            }
            _ => {
                self.error("expected pattern");
                None
            }
        }
    }

    /// Can start a "small" pattern atom (used for constructor payloads)?
    fn can_start_pat_atom(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::Lower(_)
                | TokenKind::Underscore
                | TokenKind::LBrace
                | TokenKind::LBracket
                | TokenKind::LParen
                | TokenKind::Minus
                | TokenKind::Int(_)
                | TokenKind::Float(_)
                | TokenKind::Text(_)
                | TokenKind::Bytes(_)
                | TokenKind::Bool(_)
        )
    }

    /// Parse a pattern atom (non-constructor patterns, for use as constructor payloads).
    fn parse_pat_atom(&mut self) -> Option<Pat> {
        let start = self.span();
        match self.peek() {
            TokenKind::Underscore => {
                let tok = self.advance();
                Some(Spanned::new(PatKind::Wildcard, tok.span))
            }
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Var(name), tok.span))
            }
            TokenKind::LBrace => {
                self.advance();
                self.parse_record_pat(start)
            }
            TokenKind::LBracket => {
                self.advance();
                self.parse_list_pat(start)
            }
            TokenKind::LParen => {
                self.advance();
                if self.eat(&TokenKind::RParen) {
                    return Some(Spanned::new(
                        PatKind::Record(vec![]),
                        Span::new(start.start, self.prev_span().end),
                    ));
                }
                let inner = self.parse_pat()?;
                let end_tok = self
                    .expect(&TokenKind::RParen, "expected ')' to close pattern group")
                    .ok()?;
                Some(Spanned::new(inner.node, Span::new(start.start, end_tok.span.end)))
            }
            TokenKind::Minus => {
                let minus_tok = self.advance();
                match self.peek() {
                    TokenKind::Int(_) => {
                        let tok = self.advance();
                        let TokenKind::Int(n) = tok.kind else { unreachable!() };
                        let neg = format!("-{}", n);
                        let span = Span::new(minus_tok.span.start, tok.span.end);
                        Some(Spanned::new(PatKind::Lit(Literal::Int(neg)), span))
                    }
                    TokenKind::Float(_) => {
                        let tok = self.advance();
                        let TokenKind::Float(f) = tok.kind else { unreachable!() };
                        let span = Span::new(minus_tok.span.start, tok.span.end);
                        Some(Spanned::new(PatKind::Lit(Literal::Float(-f)), span))
                    }
                    _ => {
                        self.error("expected number after '-' in pattern");
                        None
                    }
                }
            }
            TokenKind::Int(_) => {
                let tok = self.advance();
                let TokenKind::Int(n) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Int(n)), tok.span))
            }
            TokenKind::Float(_) => {
                let tok = self.advance();
                let TokenKind::Float(f) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Float(f)), tok.span))
            }
            TokenKind::Text(_) => {
                let tok = self.advance();
                let TokenKind::Text(s) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Text(s)), tok.span))
            }
            TokenKind::Bytes(_) => {
                let tok = self.advance();
                let TokenKind::Bytes(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Bytes(b)), tok.span))
            }
            TokenKind::Bool(_) => {
                let tok = self.advance();
                let TokenKind::Bool(b) = tok.kind else { unreachable!() };
                Some(Spanned::new(PatKind::Lit(Literal::Bool(b)), tok.span))
            }
            _ => {
                self.error("expected pattern atom");
                None
            }
        }
    }

    fn parse_record_pat(&mut self, start: Span) -> Option<Pat> {
        // Already consumed `{`.
        self.skip_newlines();
        let mut fields = Vec::new();
        if !self.at(&TokenKind::RBrace) {
            loop {
                self.skip_newlines();
                let (fname, _) = self.expect_lower("expected field name in record pattern").ok()?;
                let pattern = if self.eat(&TokenKind::Colon) {
                    self.skip_newlines();
                    Some(self.parse_pat()?)
                } else {
                    None // punned: {name} means {name: name}
                };
                fields.push(FieldPat {
                    name: fname,
                    pattern,
                });
                self.skip_newlines();
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBrace, "expected '}' to close record pattern")
            .ok()?;
        Some(Spanned::new(
            PatKind::Record(fields),
            Span::new(start.start, end_tok.span.end),
        ))
    }

    fn parse_list_pat(&mut self, start: Span) -> Option<Pat> {
        // Already consumed `[`.
        self.skip_newlines();
        let mut pats = Vec::new();
        if !self.at(&TokenKind::RBracket) {
            loop {
                self.skip_newlines();
                let p = self.parse_pat()?;
                pats.push(p);
                self.skip_newlines();
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBracket, "expected ']' to close list pattern")
            .ok()?;
        Some(Spanned::new(
            PatKind::List(pats),
            Span::new(start.start, end_tok.span.end),
        ))
    }
}

// ── Types ───────────────────────────────────────────────────────────

impl Parser {
    fn parse_type(&mut self) -> Option<Type> {
        if self.at(&TokenKind::Forall) {
            let start = self.span();
            self.advance(); // consume 'forall'
            let mut vars = Vec::new();
            loop {
                match self.peek().clone() {
                    TokenKind::Lower(name) => {
                        self.advance();
                        vars.push(name);
                    }
                    _ => break,
                }
            }
            if vars.is_empty() {
                self.error("expected one or more type variables after 'forall'");
                return None;
            }
            self.expect(&TokenKind::Dot, "expected '.' after forall variables").ok()?;
            self.skip_newlines();
            let body = self.parse_type()?;
            let span = Span::new(start.start, body.span.end);
            return Some(Spanned::new(
                TypeKind::Forall { vars, ty: Box::new(body) },
                span,
            ));
        }
        self.parse_type_function()
    }

    fn parse_type_function(&mut self) -> Option<Type> {
        let lhs = self.parse_type_refined()?;
        // Allow `->` on the next line by peeking past newlines.
        let saved = self.save();
        self.skip_newlines();
        if self.eat(&TokenKind::Arrow) {
            if !self.enter_recursion() { return None; }
            self.skip_newlines();
            let rhs = self.parse_type_function(); // right-associative
            self.recursion_depth -= 1;
            let rhs = rhs?;
            let span = Span::new(lhs.span.start, rhs.span.end);
            Some(Spanned::new(
                TypeKind::Function {
                    param: Box::new(lhs),
                    result: Box::new(rhs),
                },
                span,
            ))
        } else {
            self.restore(saved);
            Some(lhs)
        }
    }

    fn parse_type_refined(&mut self) -> Option<Type> {
        let base = self.parse_type_app()?;
        if self.eat(&TokenKind::Where) {
            let predicate = self.parse_expr()?;
            let span = Span::new(base.span.start, predicate.span.end);
            Some(Spanned::new(
                TypeKind::Refined {
                    base: Box::new(base),
                    predicate: Box::new(predicate),
                },
                span,
            ))
        } else {
            Some(base)
        }
    }

    fn parse_type_app(&mut self) -> Option<Type> {
        let mut func = self.parse_type_atom()?;
        loop {
            if self.can_start_type_atom() {
                let arg = self.parse_type_atom()?;
                let span = Span::new(func.span.start, arg.span.end);
                func = Spanned::new(
                    TypeKind::App {
                        func: Box::new(func),
                        arg: Box::new(arg),
                    },
                    span,
                );
                continue;
            }
            // Continue across newlines if the next non-newline token is
            // indented past the current block indent — mirrors the multi-line
            // continuation rule for expression application.
            let saved = self.save();
            self.skip_newlines();
            if !self.at_eof()
                && self.column_of(&self.span()) > self.block_indent
                && self.can_start_type_atom()
            {
                let arg = self.parse_type_atom()?;
                let span = Span::new(func.span.start, arg.span.end);
                func = Spanned::new(
                    TypeKind::App {
                        func: Box::new(func),
                        arg: Box::new(arg),
                    },
                    span,
                );
            } else {
                self.restore(saved);
                break;
            }
        }
        Some(func)
    }

    fn can_start_type_atom(&self) -> bool {
        if self.stop_type_at_headers {
            if matches!(self.peek(), TokenKind::Lower(s) if s == "headers") {
                return false;
            }
        }
        matches!(
            self.peek(),
            TokenKind::Upper(_)
                | TokenKind::Lower(_)
                | TokenKind::Underscore
                | TokenKind::LBrace
                | TokenKind::LBracket
                | TokenKind::LParen
                | TokenKind::Lt
        )
    }

    fn parse_type_atom(&mut self) -> Option<Type> {
        let start = self.span();
        match self.peek() {
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(name) = tok.kind else { unreachable!() };
                if name == "IO" && matches!(self.peek(), TokenKind::LBrace) {
                    // Parse IO {effects} Type or IO {effects | r} Type or IO {| r} Type
                    self.advance(); // consume '{'
                    self.skip_newlines();
                    let effects = if matches!(self.peek(), TokenKind::RBrace | TokenKind::Pipe) {
                        Vec::new()
                    } else {
                        self.try_parse_effects().unwrap_or_default()
                    };
                    self.skip_newlines();
                    let rest = if self.eat(&TokenKind::Pipe) {
                        self.skip_newlines();
                        match self.peek() {
                            TokenKind::Lower(_) => {
                                let tok = self.advance();
                                let TokenKind::Lower(n) = tok.kind else { unreachable!() };
                                Some(n)
                            }
                            TokenKind::Underscore => {
                                self.advance();
                                Some("_".to_string())
                            }
                            _ => {
                                self.error("expected effect row variable name or '_' after '|'");
                                None
                            }
                        }
                    } else {
                        None
                    };
                    self.skip_newlines();
                    self.expect(&TokenKind::RBrace, "expected '}' to close IO effect set")
                        .ok()?;
                    if !self.enter_recursion() { return None; }
                    let inner = self.parse_type_atom();
                    self.recursion_depth -= 1;
                    let inner = inner?;
                    let span = Span::new(tok.span.start, inner.span.end);
                    Some(Spanned::new(TypeKind::IO { effects, rest, ty: Box::new(inner) }, span))
                } else if name == "IO" && matches!(self.peek(), TokenKind::Lower(_) | TokenKind::Underscore) {
                    // Shorthand: `IO e Type` desugars to `IO {| e} Type`.
                    // `IO _ Type` is the wildcard form — effects are inferred.
                    let row_tok = self.advance();
                    let row_name = match row_tok.kind {
                        TokenKind::Lower(n) => n,
                        TokenKind::Underscore => "_".to_string(),
                        _ => unreachable!(),
                    };
                    if !self.enter_recursion() { return None; }
                    let inner = self.parse_type_atom();
                    self.recursion_depth -= 1;
                    let inner = inner?;
                    let span = Span::new(tok.span.start, inner.span.end);
                    Some(Spanned::new(
                        TypeKind::IO {
                            effects: Vec::new(),
                            rest: Some(row_name),
                            ty: Box::new(inner),
                        },
                        span,
                    ))
                } else if (name == "Float" || name == "Int") && matches!(self.peek(), TokenKind::Lt) {
                    // Try Float<unit> or Int<unit> — no adjacency check in type context
                    let saved = self.save();
                    let diag_count = self.diagnostics.len();
                    self.advance(); // consume `<`
                    if let Some(unit) = self.parse_unit_expr() {
                        if matches!(self.peek(), TokenKind::Gt) {
                            self.advance(); // consume `>`
                            let span = Span::new(tok.span.start, self.prev_span().end);
                            let base = Box::new(Spanned::new(TypeKind::Named(name), tok.span));
                            return Some(Spanned::new(TypeKind::UnitAnnotated { base, unit }, span));
                        }
                    }
                    self.diagnostics.truncate(diag_count);
                    self.restore(saved);
                    Some(Spanned::new(TypeKind::Named(name), tok.span))
                } else {
                    Some(Spanned::new(TypeKind::Named(name), tok.span))
                }
            }
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                Some(Spanned::new(TypeKind::Var(name), tok.span))
            }
            TokenKind::Underscore => {
                let tok = self.advance();
                Some(Spanned::new(TypeKind::Hole, tok.span))
            }
            TokenKind::LBrace => {
                self.advance();
                self.parse_record_type(start)
            }
            TokenKind::LBracket => {
                self.advance();
                self.parse_relation_type(start)
            }
            TokenKind::LParen => {
                self.advance();
                if self.eat(&TokenKind::RParen) {
                    // Unit type ()
                    return Some(Spanned::new(
                        TypeKind::Record {
                            fields: vec![],
                            rest: None,
                        },
                        Span::new(start.start, self.prev_span().end),
                    ));
                }
                let inner = self.parse_type()?;
                let end_tok = self
                    .expect(
                        &TokenKind::RParen,
                        "unclosed '(' — expected matching ')' in type",
                    )
                    .ok()?;
                // Return inner type with paren span.
                Some(Spanned::new(
                    inner.node,
                    Span::new(start.start, end_tok.span.end),
                ))
            }
            TokenKind::Lt => {
                self.advance();
                self.parse_variant_type(start)
            }
            _ => {
                self.error("expected type");
                None
            }
        }
    }

    /// Try parse a type atom, returning None without diagnostics on failure.
    fn try_parse_type_atom(&mut self) -> Option<Type> {
        if !self.can_start_type_atom() {
            return None;
        }
        let saved = self.save();
        let diag_count = self.diagnostics.len();
        match self.parse_type_atom() {
            Some(ty) => Some(ty),
            None => {
                self.restore(saved);
                self.diagnostics.truncate(diag_count);
                None
            }
        }
    }

    fn parse_record_type(&mut self, start: Span) -> Option<Type> {
        // Already consumed `{`.
        self.skip_newlines();

        // Check for effectful type: {reads *rel, writes *rel, ...} Type
        // Effects have special keyword-like identifiers.
        let saved = self.save();
        let diag_count = self.diagnostics.len();
        if let Some(effects) = self.try_parse_effects() {
            self.expect(&TokenKind::RBrace, "expected '}' to close effect set")
                .ok()?;
            // Now parse the effectful type body.
            let ty = self.parse_type()?;
            let span = Span::new(start.start, ty.span.end);
            return Some(Spanned::new(
                TypeKind::Effectful {
                    effects,
                    ty: Box::new(ty),
                },
                span,
            ));
        }
        self.restore(saved);
        self.diagnostics.truncate(diag_count);

        // Empty record type `{}`
        if self.eat(&TokenKind::RBrace) {
            return Some(Spanned::new(
                TypeKind::Record {
                    fields: vec![],
                    rest: None,
                },
                Span::new(start.start, self.prev_span().end),
            ));
        }

        // Record type: {field: Type, ... | rest?}
        let mut fields = Vec::new();
        loop {
            self.skip_newlines();
            if self.at(&TokenKind::RBrace) || self.at(&TokenKind::Pipe) {
                break;
            }
            let (fname, _) = self.expect_lower("expected field name in record type").ok()?;
            self.expect(&TokenKind::Colon, "expected ':' after field name in record type")
                .ok()?;
            self.skip_newlines();
            let ty = self.parse_type()?;
            fields.push(Field {
                name: fname,
                value: ty,
            });
            self.skip_newlines();
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }

        self.skip_newlines();
        let rest = if self.eat(&TokenKind::Pipe) {
            let (rname, _) = self.expect_lower("expected row variable after '|' in record type").ok()?;
            Some(rname)
        } else {
            None
        };

        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBrace, "expected '}' to close record type")
            .ok()?;
        Some(Spanned::new(
            TypeKind::Record { fields, rest },
            Span::new(start.start, end_tok.span.end),
        ))
    }

    fn try_parse_effects(&mut self) -> Option<Vec<Effect>> {
        let mut effects = Vec::new();
        loop {
            match self.peek() {
                TokenKind::Lower(s) if s == "reads" => {
                    self.advance();
                    self.expect(&TokenKind::Star, "expected '*' after 'reads'").ok()?;
                    let (name, _) = self
                        .expect_lower("expected relation name after 'reads *'")
                        .ok()?;
                    effects.push(Effect::Reads(name));
                }
                TokenKind::Lower(s) if s == "writes" => {
                    self.advance();
                    self.expect(&TokenKind::Star, "expected '*' after 'writes'").ok()?;
                    let (name, _) = self
                        .expect_lower("expected relation name after 'writes *'")
                        .ok()?;
                    effects.push(Effect::Writes(name));
                }
                TokenKind::Lower(s) if s == "console" => {
                    self.advance();
                    effects.push(Effect::Console);
                }
                TokenKind::Lower(s) if s == "network" => {
                    self.advance();
                    effects.push(Effect::Network);
                }
                TokenKind::Lower(s) if s == "fs" => {
                    self.advance();
                    effects.push(Effect::Fs);
                }
                TokenKind::Lower(s) if s == "clock" => {
                    self.advance();
                    effects.push(Effect::Clock);
                }
                TokenKind::Lower(s) if s == "random" => {
                    self.advance();
                    effects.push(Effect::Random);
                }
                _ => break,
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        if effects.is_empty() {
            None
        } else {
            Some(effects)
        }
    }

    fn parse_relation_type(&mut self, start: Span) -> Option<Type> {
        // Already consumed `[`.
        self.skip_newlines();
        if self.eat(&TokenKind::RBracket) {
            // `[]` as a type — the list/relation type constructor with no argument.
            // This represents the `[]` type constructor used in `impl Functor []`.
            return Some(Spanned::new(
                TypeKind::Named("[]".into()),
                Span::new(start.start, self.prev_span().end),
            ));
        }

        let inner = self.parse_type()?;
        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBracket, "expected ']' to close relation type")
            .ok()?;
        Some(Spanned::new(
            TypeKind::Relation(Box::new(inner)),
            Span::new(start.start, end_tok.span.end),
        ))
    }

    fn parse_variant_type(&mut self, start: Span) -> Option<Type> {
        // Already consumed `<`.
        self.skip_newlines();

        let mut constructors = Vec::new();
        let mut rest = None;

        loop {
            self.skip_newlines();
            if self.at(&TokenKind::Gt) {
                break;
            }
            if self.at(&TokenKind::Pipe) {
                self.advance();
                self.skip_newlines();
                // Could be a rest variable or another constructor.
                if matches!(self.peek(), TokenKind::Lower(_)) {
                    // Check if this is followed by `>` — if so, it's a rest variable.
                    if matches!(self.peek_ahead(1), TokenKind::Gt) {
                        let tok = self.advance();
                        let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                        rest = Some(name);
                        break;
                    }
                }
                // Fall through to parse as constructor.
            }
            if let TokenKind::Upper(_) = self.peek() {
                constructors.push(self.parse_constructor_def()?);
            } else if matches!(self.peek(), TokenKind::Lower(_)) {
                // Rest variable.
                let tok = self.advance();
                let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                rest = Some(name);
                break;
            } else {
                break;
            }
            self.skip_newlines();
            if !self.at(&TokenKind::Pipe) && !self.at(&TokenKind::Gt) {
                break;
            }
        }

        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::Gt, "expected '>' to close variant type")
            .ok()?;
        Some(Spanned::new(
            TypeKind::Variant { constructors, rest },
            Span::new(start.start, end_tok.span.end),
        ))
    }

    fn parse_type_scheme(&mut self) -> Option<TypeScheme> {
        // Parse optional constraints: (TraitName args* =>)*
        let saved = self.save();
        let diag_count = self.diagnostics.len();
        let constraints = if let Some(cs) = self.try_parse_constraints() {
            cs
        } else {
            self.restore(saved);
            self.diagnostics.truncate(diag_count);
            vec![]
        };
        // Allow the type body to begin on a new line (after `:` or after `=>`).
        self.skip_newlines();
        let ty = self.parse_type()?;
        Some(TypeScheme { constraints, ty })
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a token list from (kind, start, end) triples.
    fn toks(items: Vec<(TokenKind, usize, usize)>) -> Vec<Token> {
        items
            .into_iter()
            .map(|(kind, start, end)| Token {
                kind,
                span: Span::new(start, end),
            })
            .collect()
    }

    #[test]
    fn parse_empty_module() {
        let tokens = toks(vec![(TokenKind::Eof, 0, 0)]);
        let (module, diags) = Parser::new(String::new(), tokens).parse_module();
        assert!(diags.is_empty());
        assert!(module.decls.is_empty());
    }

    #[test]
    fn parse_simple_fun() {
        // x = 42
        let source = "x = 42".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("x".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Int("42".into()), 4, 6),
            (TokenKind::Eof, 6, 6),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        assert_eq!(module.decls.len(), 1);
        match &module.decls[0].node {
            DeclKind::Fun { name, body: Some(body), .. } => {
                assert_eq!(name, "x");
                assert!(matches!(&body.node, ExprKind::Lit(Literal::Int(n)) if n == "42"));
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_binop() {
        // a + b * c
        let source = "x = a + b * c".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("x".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Lower("a".into()), 4, 5),
            (TokenKind::Plus, 6, 7),
            (TokenKind::Lower("b".into()), 8, 9),
            (TokenKind::Star, 10, 11),
            (TokenKind::Lower("c".into()), 12, 13),
            (TokenKind::Eof, 13, 13),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        // Should parse as a + (b * c) due to precedence.
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => match &body.node {
                ExprKind::BinOp {
                    op: BinOp::Add,
                    lhs,
                    rhs,
                } => {
                    assert!(matches!(&lhs.node, ExprKind::Var(n) if n == "a"));
                    assert!(matches!(&rhs.node, ExprKind::BinOp { op: BinOp::Mul, .. }));
                }
                other => panic!("expected BinOp Add, got {:?}", other),
            },
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_if_expr() {
        // f = \x -> if x then 1 else 2
        let source = r"f = \x -> if x then 1 else 2".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Backslash, 4, 5),
            (TokenKind::Lower("x".into()), 5, 6),
            (TokenKind::Arrow, 7, 9),
            (TokenKind::If, 10, 12),
            (TokenKind::Lower("x".into()), 13, 14),
            (TokenKind::Then, 15, 19),
            (TokenKind::Int("1".into()), 20, 21),
            (TokenKind::Else, 22, 26),
            (TokenKind::Int("2".into()), 27, 28),
            (TokenKind::Eof, 28, 28),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                assert!(matches!(&body.node, ExprKind::Lambda { .. }));
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_data_decl() {
        // data Bool = True {} | False {}
        let source = "data Bool = True {} | False {}".to_string();
        let tokens = toks(vec![
            (TokenKind::Data, 0, 4),
            (TokenKind::Upper("Bool".into()), 5, 9),
            (TokenKind::Eq, 10, 11),
            (TokenKind::Upper("True".into()), 12, 16),
            (TokenKind::LBrace, 17, 18),
            (TokenKind::RBrace, 18, 19),
            (TokenKind::Pipe, 20, 21),
            (TokenKind::Upper("False".into()), 22, 27),
            (TokenKind::LBrace, 28, 29),
            (TokenKind::RBrace, 29, 30),
            (TokenKind::Eof, 30, 30),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Data {
                name,
                constructors,
                ..
            } => {
                assert_eq!(name, "Bool");
                assert_eq!(constructors.len(), 2);
                assert_eq!(constructors[0].name, "True");
                assert_eq!(constructors[1].name, "False");
            }
            other => panic!("expected Data, got {:?}", other),
        }
    }

    #[test]
    fn parse_source_decl() {
        // *people : [Person]
        let source = "*people : [Person]".to_string();
        let tokens = toks(vec![
            (TokenKind::Star, 0, 1),
            (TokenKind::Lower("people".into()), 1, 7),
            (TokenKind::Colon, 8, 9),
            (TokenKind::LBracket, 10, 11),
            (TokenKind::Upper("Person".into()), 11, 17),
            (TokenKind::RBracket, 17, 18),
            (TokenKind::Eof, 18, 18),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Source {
                name,
                ty,
                history,
            } => {
                assert_eq!(name, "people");
                assert!(!history);
                assert!(matches!(&ty.node, TypeKind::Relation(_)));
            }
            other => panic!("expected Source, got {:?}", other),
        }
    }

    #[test]
    fn parse_lambda() {
        // f = \x -> x
        let source = "f = \\x -> x".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Backslash, 4, 5),
            (TokenKind::Lower("x".into()), 5, 6),
            (TokenKind::Arrow, 7, 9),
            (TokenKind::Lower("x".into()), 10, 11),
            (TokenKind::Eof, 11, 11),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                assert!(matches!(&body.node, ExprKind::Lambda { .. }));
            }
            other => panic!("expected Fun with lambda body, got {:?}", other),
        }
    }

    #[test]
    fn parse_record_expr() {
        // r = {name: "Alice", age: 30}
        let source = r#"r = {name: "Alice", age: 30}"#.to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("r".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::LBrace, 4, 5),
            (TokenKind::Lower("name".into()), 5, 9),
            (TokenKind::Colon, 9, 10),
            (TokenKind::Text("Alice".into()), 11, 18),
            (TokenKind::Comma, 18, 19),
            (TokenKind::Lower("age".into()), 20, 23),
            (TokenKind::Colon, 23, 24),
            (TokenKind::Int("30".into()), 25, 27),
            (TokenKind::RBrace, 27, 28),
            (TokenKind::Eof, 28, 28),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => match &body.node {
                ExprKind::Record(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name, "name");
                    assert_eq!(fields[1].name, "age");
                }
                other => panic!("expected Record, got {:?}", other),
            },
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_type_alias() {
        // type Person = {name: Text, age: Int}
        let source = "type Person = {name: Text, age: Int}".to_string();
        let tokens = toks(vec![
            (TokenKind::Type, 0, 4),
            (TokenKind::Upper("Person".into()), 5, 11),
            (TokenKind::Eq, 12, 13),
            (TokenKind::LBrace, 14, 15),
            (TokenKind::Lower("name".into()), 15, 19),
            (TokenKind::Colon, 19, 20),
            (TokenKind::Upper("Text".into()), 21, 25),
            (TokenKind::Comma, 25, 26),
            (TokenKind::Lower("age".into()), 27, 30),
            (TokenKind::Colon, 30, 31),
            (TokenKind::Upper("Int".into()), 32, 35),
            (TokenKind::RBrace, 35, 36),
            (TokenKind::Eof, 36, 36),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::TypeAlias { name, ty, .. } => {
                assert_eq!(name, "Person");
                assert!(matches!(&ty.node, TypeKind::Record { .. }));
            }
            other => panic!("expected TypeAlias, got {:?}", other),
        }
    }

    #[test]
    fn parse_application_expr() {
        // f = g x y
        let source = "f = g x y".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Lower("g".into()), 4, 5),
            (TokenKind::Lower("x".into()), 6, 7),
            (TokenKind::Lower("y".into()), 8, 9),
            (TokenKind::Eof, 9, 9),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                // g x y => App(App(g, x), y)
                match &body.node {
                    ExprKind::App { func, arg } => {
                        assert!(matches!(&arg.node, ExprKind::Var(n) if n == "y"));
                        assert!(matches!(&func.node, ExprKind::App { .. }));
                    }
                    other => panic!("expected App, got {:?}", other),
                }
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_field_access() {
        // f x = x.name
        let source = "f = x.name".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Lower("x".into()), 4, 5),
            (TokenKind::Dot, 5, 6),
            (TokenKind::Lower("name".into()), 6, 10),
            (TokenKind::Eof, 10, 10),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                assert!(matches!(&body.node, ExprKind::FieldAccess { field, .. } if field == "name"));
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn expect_lower_rejects_keywords() {
        let source = "where".to_string();
        let tokens = toks(vec![
            (TokenKind::Where, 0, 5),
            (TokenKind::Eof, 5, 5),
        ]);
        let mut parser = Parser::new(source, tokens);
        let result = parser.expect_lower("expected identifier");
        assert!(result.is_err());
        assert!(!parser.diagnostics.is_empty());
        assert!(parser.diagnostics[0]
            .message
            .contains("keyword"));
    }

    #[test]
    fn parse_derived_decl() {
        // &seniors = x
        let source = "&seniors = x".to_string();
        let tokens = toks(vec![
            (TokenKind::Ampersand, 0, 1),
            (TokenKind::Lower("seniors".into()), 1, 8),
            (TokenKind::Eq, 9, 10),
            (TokenKind::Lower("x".into()), 11, 12),
            (TokenKind::Eof, 12, 12),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Derived { name, body, .. } => {
                assert_eq!(name, "seniors");
                assert!(matches!(&body.node, ExprKind::Var(n) if n == "x"));
            }
            other => panic!("expected Derived, got {:?}", other),
        }
    }

    #[test]
    fn parse_list_expr() {
        // xs = [1, 2, 3]
        let source = "xs = [1, 2, 3]".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("xs".into()), 0, 2),
            (TokenKind::Eq, 3, 4),
            (TokenKind::LBracket, 5, 6),
            (TokenKind::Int("1".into()), 6, 7),
            (TokenKind::Comma, 7, 8),
            (TokenKind::Int("2".into()), 9, 10),
            (TokenKind::Comma, 10, 11),
            (TokenKind::Int("3".into()), 12, 13),
            (TokenKind::RBracket, 13, 14),
            (TokenKind::Eof, 14, 14),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => match &body.node {
                ExprKind::List(elems) => {
                    assert_eq!(elems.len(), 3);
                }
                other => panic!("expected List, got {:?}", other),
            },
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_unary_neg() {
        // f = -x
        let source = "f = -x".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Minus, 4, 5),
            (TokenKind::Lower("x".into()), 5, 6),
            (TokenKind::Eof, 6, 6),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                assert!(matches!(
                    &body.node,
                    ExprKind::UnaryOp {
                        op: UnaryOp::Neg,
                        ..
                    }
                ));
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_source_ref_and_derived_ref() {
        // f = *people
        let source = "f = *people".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::Star, 4, 5),
            (TokenKind::Lower("people".into()), 5, 11),
            (TokenKind::Eof, 11, 11),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => {
                assert!(matches!(&body.node, ExprKind::SourceRef(n) if n == "people"));
            }
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_record_update() {
        // f = {t | age: 30}
        let source = "f = {t | age: 30}".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::LBrace, 4, 5),
            (TokenKind::Lower("t".into()), 5, 6),
            (TokenKind::Pipe, 7, 8),
            (TokenKind::Lower("age".into()), 9, 12),
            (TokenKind::Colon, 12, 13),
            (TokenKind::Int("30".into()), 14, 16),
            (TokenKind::RBrace, 16, 17),
            (TokenKind::Eof, 17, 17),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        assert!(diags.is_empty(), "diags: {:?}", diags);
        match &module.decls[0].node {
            DeclKind::Fun { body: Some(body), .. } => match &body.node {
                ExprKind::RecordUpdate { base, fields } => {
                    assert!(matches!(&base.node, ExprKind::Var(n) if n == "t"));
                    assert_eq!(fields.len(), 1);
                    assert_eq!(fields[0].name, "age");
                }
                other => panic!("expected RecordUpdate, got {:?}", other),
            },
            other => panic!("expected Fun, got {:?}", other),
        }
    }

    #[test]
    fn parse_error_recovery() {
        // First decl has an error, second should still parse.
        let source = "bad !!! stuff\nx = 1".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("bad".into()), 0, 3),
            // Some junk tokens that won't form a valid declaration.
            (TokenKind::Eq, 4, 5),
            (TokenKind::Eq, 5, 6), // double `=` — error
            (TokenKind::Eq, 6, 7),
            (TokenKind::Newline, 13, 14),
            // Second declaration at column 0.
            (TokenKind::Lower("x".into()), 14, 15),
            (TokenKind::Eq, 16, 17),
            (TokenKind::Int("1".into()), 18, 19),
            (TokenKind::Eof, 19, 19),
        ]);
        let (module, diags) = Parser::new(source, tokens).parse_module();
        // Should have at least one error from the first decl.
        assert!(!diags.is_empty());
        // But should still parse the second decl.
        let fun_count = module
            .decls
            .iter()
            .filter(|d| matches!(&d.node, DeclKind::Fun { name, .. } if name == "x"))
            .count();
        assert_eq!(fun_count, 1, "should recover and parse 'x = 1'");
    }
}
