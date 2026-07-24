//! Recursive-descent parser with Pratt expression parsing for the Knot language.

use crate::ast::*;
use crate::diagnostic::Diagnostic;
use crate::lexer::{Token, TokenKind};
use std::collections::HashSet;

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
    /// When true, `can_start_type_atom` returns false for `Lower("to")` and
    /// `Lower("using")`. Used while parsing the `from`/`to` types of a
    /// `migrate` declaration so a single-line migrate doesn't have its clause
    /// keywords consumed as type-variable applications.
    stop_type_at_migrate_clauses: bool,
    /// When true, the cross-newline type-application continuation stops at a
    /// `Lower` token on the following line. Set while parsing the `name : Type`
    /// signature line of a record VALUE literal, where a lowercase identifier
    /// on the next line is always the field's value (`name value`), never a
    /// type-variable argument. (Contrast record TYPES, where the next field is
    /// `Lower :` and the existing `next_starts_decl` guard already stops.)
    record_value_sig_type: bool,
    /// Indentation level of the current block (set by `parse_block`).
    /// Used by `parse_application` to allow multi-line function application
    /// when continuation lines are indented past the block indent.
    block_indent: usize,
    /// The `delimiter_depth` at which the current `block_indent` was
    /// established. A do/case block establishes a layout context whose
    /// statement boundaries stay layout-sensitive even inside parens — but
    /// only while that block is the innermost active one at the *current*
    /// delimiter depth. When an outer block's indent merely leaks into a
    /// deeper paren scope (`foo = (a\n+ b)`), `block_delim < delimiter_depth`,
    /// and the layout boundary check is skipped so a parenthesized multi-line
    /// expression still continues freely across newlines.
    block_delim: usize,
    /// Nesting depth inside delimiters (parens, brackets, braces).
    /// When > 0, the column-0 check in `parse_expr_bp` is suppressed so that
    /// operators at column 0 inside grouped expressions are not mistaken for
    /// new top-level declarations.
    delimiter_depth: usize,
    /// Tracks recursion depth for unbounded recursive-descent paths
    /// (unary operators, constructor chaining, type arrows) to prevent
    /// stack overflow on pathological input.
    recursion_depth: usize,
    /// Stack of locally-bound identifiers (lambda params, do-bind names,
    /// case pattern binders). Used by
    /// `maybe_time_unit` to suppress the `2 ms`/`5 seconds` literal sugar
    /// when the would-be unit name is actually a bound variable, so
    /// `\ms -> g 2 ms` applies `g` to `2` and `ms` rather than desugaring
    /// to `g (2 * 1)`.
    bound_vars: Vec<Name>,
    /// Top-level declaration names that collide with time-unit words
    /// (`ms`/`seconds`/...). Populated by a pre-scan in `parse_module` so
    /// that `maybe_time_unit` can suppress sugar for `ms = 5; ... 2 ms`
    /// (where `ms` is a user-defined top-level value, not the unit).
    top_level_names: HashSet<String>,
    /// Display column (chars from the start of its line) of each token,
    /// indexed by token position. Precomputed in a single O(source) pass so
    /// layout queries during parsing are O(1) instead of O(line-length) —
    /// without this, the per-item `column_of` calls in `parse_block` and the
    /// error-recovery loop are O(n²) on a long single line (e.g. a minified
    /// or generated `do` block with `;` separators), causing a parser hang.
    token_cols: Vec<usize>,
    /// Display column at end-of-input, used when `pos` is past the last token.
    eof_col: usize,
}

// ── Public API ──────────────────────────────────────────────────────

impl Parser {
    pub fn new(source: String, tokens: Vec<Token>) -> Self {
        let (token_cols, eof_col) = Self::precompute_columns(&source, &tokens);
        Self {
            source,
            tokens,
            pos: 0,
            diagnostics: Vec::new(),
            context: Vec::new(),
            stop_type_at_headers: false,
            stop_type_at_migrate_clauses: false,
            record_value_sig_type: false,
            block_indent: usize::MAX,
            block_delim: 0,
            delimiter_depth: 0,
            recursion_depth: 0,
            bound_vars: Vec::new(),
            top_level_names: HashSet::new(),
            token_cols,
            eof_col,
        }
    }

    /// Compute the display column of every token in one pass over the source.
    /// A column is the number of Unicode scalar values between the start of the
    /// token's line and the token. Line boundaries are any of `\n`/`\r`/`\r\n`,
    /// matching the lexer's layout-newline handling and the legacy `column_of`.
    fn precompute_columns(source: &str, tokens: &[Token]) -> (Vec<usize>, usize) {
        let mut cols = Vec::with_capacity(tokens.len());
        let mut chars = source.char_indices();
        let mut byte = 0usize; // byte offset of the next unconsumed char
        let mut col = 0usize; // column at `byte`
        // Skip a leading UTF-8 BOM (0xEF 0xBB 0xBF) to match the lexer, which
        // advances past it without counting it as a column; otherwise every
        // token on line 1 would report a column one too high.
        if source.as_bytes().starts_with(b"\xEF\xBB\xBF") {
            chars.next();
            byte += 3;
        }
        for tok in tokens {
            let target = tok.span.start.min(source.len());
            while byte < target {
                match chars.next() {
                    Some((_, c)) => {
                        byte += c.len_utf8();
                        if c == '\n' || c == '\r' {
                            col = 0;
                        } else {
                            col += 1;
                        }
                    }
                    None => break,
                }
            }
            cols.push(col);
        }
        // Continue to the end of input for the EOF column.
        for (_, c) in chars {
            if c == '\n' || c == '\r' {
                col = 0;
            } else {
                col += 1;
            }
        }
        (cols, col)
    }

    /// Display column of the current token (O(1); see `token_cols`).
    fn cur_column(&self) -> usize {
        self.token_cols.get(self.pos).copied().unwrap_or(self.eof_col)
    }

    /// After a newline was crossed mid-expression, decide whether the current
    /// token begins a new top-level declaration or a new statement/item of an
    /// enclosing layout block — in which case it does NOT continue the current
    /// expression (operator or application continuation stops here).
    ///
    /// Two independent rules:
    ///
    /// * A token at column 0 begins a new top-level declaration — but only
    ///   outside delimiters, where a closing delimiter (not layout) would
    ///   otherwise terminate the expression. Inside parens a column-0 operator
    ///   is a legitimate continuation (`foo = (a\n+ b)`), so this is gated on
    ///   `delimiter_depth == 0`.
    ///
    /// * A token at or under the enclosing block's indent starts a new block
    ///   item (a do statement, a case arm, ...). This is layout-sensitive and
    ///   holds *regardless of paren nesting* — statement boundaries inside a
    ///   parenthesized `do`/`case` block must still be respected, otherwise the
    ///   formatter's parenthesized rendering of e.g. `do\n  let a = 1\n  -2`
    ///   reparses with the statements glued into `let a = 1 - 2`. It applies
    ///   only when the block was opened at the *current* delimiter depth
    ///   (`block_delim == delimiter_depth`); an outer block whose indent merely
    ///   leaked into a deeper paren scope must not terminate a legitimate
    ///   parenthesized continuation.
    fn at_layout_boundary(&self) -> bool {
        let col = self.cur_column();
        if self.delimiter_depth == 0 && col == 0 {
            return true;
        }
        self.block_indent != usize::MAX
            && self.block_delim == self.delimiter_depth
            && col <= self.block_indent
    }

    pub fn parse_module(mut self) -> (Module, Vec<Diagnostic>) {
        self.skip_newlines();

        // A `.knot` file is a single expression; its value is the program's
        // result. There are no top-level declarations — where declarations
        // are needed they live as fields inside a record literal (typically
        // via `with { ...decls... } body`).
        //
        // The parser parses the whole file as ONE expression, then lowers it
        // to the internal `Module { decls }` IR that the rest of the compiler
        // still consumes (Phase 1 bridge). The lowering recognises three
        // shapes:
        //   * `with {record} body`  -> record fields become decls, `body` -> main
        //   * a bare record literal -> fields become decls, `main` = unit record
        //   * any other expression  -> that expression becomes `main`
        self.block_indent = 0;
        self.block_delim = 0;
        self.top_level_names = self.scan_top_level_names();

        let decls = if self.at_eof() {
            Vec::new()
        } else {
            match self.parse_expr() {
                Some(expr) => {
                    // Trailing tokens after the single expression are an error.
                    self.skip_newlines();
                    if !self.at_eof() {
                        let span = self.span();
                        self.diagnostics.push(
                            Diagnostic::error(
                                "unexpected tokens after the file's expression",
                            )
                            .label(span, "a .knot file is a single expression"),
                        );
                    }
                    Self::lower_file_expr(expr)
                }
                None => {
                    self.context.clear();
                    Vec::new()
                }
            }
        };

        (Module { decls }, self.diagnostics)
    }

    /// Lower the file's single expression into the internal declaration IR.
    ///
    /// See `parse_module` for the three recognised shapes. Each record field
    /// that names a declaration becomes a top-level `Decl`; the program body
    /// becomes a `main` declaration.
    fn lower_file_expr(expr: Expr) -> Vec<Decl> {
        let span = expr.span;
        match expr.node {
            // `with {record} body`
            ExprKind::With { record, body } => {
                let mut decls = match record.node {
                    ExprKind::Record(fields) => Self::record_fields_to_decls(fields),
                    _ => Vec::new(),
                };
                let body_span = body.span;
                decls.push(Decl {
                    node: DeclKind::Fun {
                        name: "main".to_string(),
                        ty: None,
                        body: Some(*body),
                    },
                    span: body_span,
                });
                decls
            }
            // A bare record literal: fields are decls, body is unit.
            ExprKind::Record(fields) => {
                let mut decls = Self::record_fields_to_decls(fields);
                decls.push(Decl {
                    node: DeclKind::Fun {
                        name: "main".to_string(),
                        ty: None,
                        body: Some(Spanned::new(ExprKind::Record(Vec::new()), span)),
                    },
                    span,
                });
                decls
            }
            // Any other expression is the program body itself.
            _ => vec![Decl {
                node: DeclKind::Fun {
                    name: "main".to_string(),
                    ty: None,
                    body: Some(expr),
                },
                span,
            }],
        }
    }

    /// Convert record-literal fields that name declarations into `Decl`s.
    /// Plain value fields (`name = expr`) become `Fun` declarations.
    fn record_fields_to_decls(fields: Vec<crate::ast::RecordField>) -> Vec<Decl> {
        fields
            .into_iter()
            .map(|f| {
                let span = f.value.span;
                let node = match f.value.node {
                    ExprKind::DataCtor { name, params, constructors } => DeclKind::Data {
                        name,
                        params,
                        constructors,
                        deriving: Vec::new(),
                    },
                    ExprKind::TypeCtor { name, params, ty } => DeclKind::TypeAlias { name, params, ty },
                    ExprKind::SourceDecl { name, ty, .. } => DeclKind::Source { name, ty },
                    ExprKind::ViewDecl { name, ty, body } => DeclKind::View {
                        name,
                        ty,
                        body: *body,
                    },
                    ExprKind::DerivedDecl { name, ty, body } => DeclKind::Derived {
                        name,
                        ty,
                        body: *body,
                    },
                    ExprKind::RouteDecl { name, entries } => DeclKind::Route { name, entries },
                    ExprKind::RouteCompositeDecl { name, components } => {
                        DeclKind::RouteComposite { name, components }
                    }
                    ExprKind::SubsetConstraint { sub, sup } => {
                        DeclKind::SubsetConstraint { sub, sup }
                    }
                    // Signature-only field (`name : Type`, no value): the record
                    // parser emits an empty-record placeholder value. Lower to a
                    // body-less `Fun` — a required CLI constant.
                    ExprKind::Record(ref fs) if fs.is_empty() && f.sig.is_some() => {
                        DeclKind::Fun {
                            name: f.name,
                            ty: f.sig,
                            body: None,
                        }
                    }
                    // A plain value field: `name = expr` (functions are lambdas).
                    value => DeclKind::Fun {
                        name: f.name,
                        ty: f.sig,
                        body: Some(Spanned::new(value, span)),
                    },
                };
                Decl { node, span }
            })
            .collect()
    }
}

// ── Recursion depth guard ────────────────────────────────────────────

const MAX_RECURSION_DEPTH: usize = 256;

/// Cost charged by `parse_atom`/`parse_type_atom` per nesting level. The
/// expression delimiter cycle (`parse_atom` → `parse_expr` → … → `parse_atom`)
/// burns ~10 stack frames per level — far more than the cheap cycles (unary
/// chains, type arrows) — so it is charged more of the shared budget to stay
/// well within thread stack limits before the guard fires.
const DELIMITER_RECURSION_COST: usize = 4;

impl Parser {
    /// Increment recursion depth and return `true`, or emit an error and
    /// return `false` if the limit is exceeded.  Callers must decrement
    /// `self.recursion_depth` when the recursive call returns.
    fn enter_recursion(&mut self) -> bool {
        self.enter_recursion_cost(1)
    }

    /// Like `enter_recursion`, but charges `cost` units of the depth budget.
    /// Callers must subtract the same `cost` when the recursive call returns.
    fn enter_recursion_cost(&mut self, cost: usize) -> bool {
        self.recursion_depth += cost;
        if self.recursion_depth > MAX_RECURSION_DEPTH {
            self.recursion_depth -= cost;
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

// ── Local binding scope tracking ────────────────────────────────────

impl Parser {
    /// Push every variable bound by `pat` onto the `bound_vars` stack.
    /// Callers record `bound_vars.len()` before pushing and truncate back
    /// to it when the binder's scope ends.
    fn push_pat_vars(&mut self, pat: &Pat) {
        match &pat.node {
            PatKind::Var(n) => self.bound_vars.push(n.clone()),
            PatKind::Wildcard | PatKind::Lit(_) => {}
            PatKind::Constructor { payload, .. } => self.push_pat_vars(payload),
            PatKind::Record(fields) => {
                for f in fields {
                    match &f.pattern {
                        Some(sub) => self.push_pat_vars(sub),
                        // Punned field `{name}` binds the field name itself.
                        None => self.bound_vars.push(f.name.clone()),
                    }
                }
            }
            PatKind::List(items) => {
                for it in items {
                    self.push_pat_vars(it);
                }
            }
            PatKind::Cons { head, tail } => {
                self.push_pat_vars(head);
                self.push_pat_vars(tail);
            }
            PatKind::Annot { pat, .. } => self.push_pat_vars(pat),
        }
    }

    fn is_bound_var(&self, name: &str) -> bool {
        self.bound_vars.iter().any(|v| v == name)
    }

    /// Pre-scan the token stream for names that collide with time-unit words
    /// (`ms`, `seconds`, `minutes`, `hours`, `days`, `weeks`) and are actually
    /// user-defined values rather than the built-in unit. This lets
    /// `maybe_time_unit` suppress unit sugar for those names. Covers
    /// top-level declarations (column-0 `name =`/`name :`, optionally
    /// behind a `*`/`&`/`export` sigil).
    fn scan_top_level_names(&self) -> HashSet<String> {
        const TIME_UNITS: &[&str] =
            &["ms", "seconds", "minutes", "hours", "days", "weeks"];
        let mut names = HashSet::new();

        let n = self.tokens.len();
        for i in 0..n {
            let TokenKind::Lower(s) = &self.tokens[i].kind else {
                continue;
            };
            if !TIME_UNITS.contains(&s.as_str()) {
                continue;
            }
            // Must be followed by `=` or `:` to be a declaration name.
            if i + 1 >= n {
                continue;
            }
            if !matches!(self.tokens[i + 1].kind, TokenKind::Eq | TokenKind::Colon) {
                continue;
            }
            // A top-level declaration name is at column 0, or immediately
            // preceded by a `*`/`&` sigil token that is itself at column 0.
            let preceded_by_sigil_at_col0 = i >= 1
                && matches!(
                    self.tokens[i - 1].kind,
                    TokenKind::Star | TokenKind::Ampersand
                )
                && self.token_cols[i - 1] == 0;
            if self.token_cols[i] == 0 || preceded_by_sigil_at_col0 {
                names.insert(s.clone());
            }
        }
        names
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

    /// Is the cursor at the start of a `field:` signature (a lowercase
    /// identifier immediately followed by a colon)? Used to tell a new record
    /// field apart from a wrapped field type when scanning a multiline record.
    fn at_field_signature(&self) -> bool {
        matches!(self.peek(), TokenKind::Lower(_))
            && matches!(self.peek_ahead(1), TokenKind::Colon)
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
    /// Skip tokens until we reach what looks like a new declaration boundary.
    fn skip_to_decl_boundary(&mut self) {
        loop {
            if self.at_eof() {
                break;
            }
            let col = self.cur_column();
            if col == 0 {
                match self.peek() {
                    TokenKind::Data
                    | TokenKind::Type
                    | TokenKind::Route
                    | TokenKind::Migrate
                    | TokenKind::Star
                    | TokenKind::StarIdent(_)
                    | TokenKind::Ampersand
                    | TokenKind::AmpersandIdent(_)
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
            | TokenKind::Not
            | TokenKind::Replace
            | TokenKind::Atomic
            | TokenKind::Deriving
            | TokenKind::With
            | TokenKind::Data
            | TokenKind::Type
            | TokenKind::Route
            | TokenKind::Serve
            | TokenKind::Migrate
            | TokenKind::Refine
            | TokenKind::Forall => {
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

impl Parser {
    /// Extend a route path literal segment with `-`-joined parts, but only
    /// when the `-` and the following identifier are span-adjacent (no
    /// intervening whitespace). Without this, `/foo - bar` (a spaced,
    /// binary-minus-looking sequence) would be glued into the single literal
    /// `foo-bar`, silently parsing a different path than written.
    fn consume_route_dashed_suffix(&mut self, seg: &mut String) {
        loop {
            if !self.at(&TokenKind::Minus) {
                break;
            }
            let minus_span = self.span();
            if minus_span.start != self.prev_span().end {
                break;
            }
            let Some(next) = self.tokens.get(self.pos + 1) else {
                break;
            };
            if next.span.start != minus_span.end {
                break;
            }
            let part: Option<String> = match &next.kind {
                TokenKind::Lower(n) | TokenKind::Upper(n) => Some(n.clone()),
                k => k.keyword_str().map(|s| s.to_string()),
            };
            let Some(part) = part else {
                break;
            };
            self.advance(); // consume `-`
            self.advance(); // consume the segment part
            seg.push('-');
            seg.push_str(&part);
        }
    }
}

// ── Layout block helper ─────────────────────────────────────────────

impl Parser {
    fn parse_block<T>(&mut self, mut parse_item: impl FnMut(&mut Self) -> Option<T>) -> Vec<T> {
        self.skip_newlines();
        if self.at_eof() {
            return vec![];
        }
        let indent = self.cur_column();
        // Layout rule: a nested block's items must be indented strictly past
        // the enclosing block's indent. Without this, an empty block (e.g.
        // `trait Foo a where` followed by a blank line) silently captures the
        // next declaration at column 0 as a block item. Only enforced outside
        // delimiters — inside parens/brackets the closing delimiter already
        // terminates the block, and column positions are free-form there.
        if self.delimiter_depth == 0
            && self.block_indent != usize::MAX
            && indent <= self.block_indent
        {
            return vec![];
        }
        let prev_block_indent = self.block_indent;
        let prev_block_delim = self.block_delim;
        self.block_indent = indent;
        self.block_delim = self.delimiter_depth;
        let mut items = vec![];
        loop {
            if self.at_eof() {
                break;
            }
            let col = self.cur_column();
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
            // delimiter — or a comma separating outer list/record elements —
            // ends the block; it belongs to an outer scope. Without this,
            // `(case x of A -> 1; B -> 2)` would try to parse `)` as a case
            // arm pattern, and `[do ...; yield x, 2]` would swallow the `, 2`
            // into the do block instead of ending it at the comma.
            if self.delimiter_depth > 0
                && matches!(
                    self.peek(),
                    TokenKind::RParen
                        | TokenKind::RBracket
                        | TokenKind::RBrace
                        | TokenKind::Comma
                )
            {
                break;
            }
            // Keywords that cannot start a new block item terminate the block.
            if matches!(self.peek(), TokenKind::Then | TokenKind::Else | TokenKind::Of) {
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
                || self.cur_column() < indent
                || (self.delimiter_depth > 0
                    && matches!(
                        self.peek(),
                        TokenKind::RParen
                            | TokenKind::RBracket
                            | TokenKind::RBrace
                            | TokenKind::Comma
                    ))
            {
                self.restore(saved);
                break;
            }
        }
        self.block_indent = prev_block_indent;
        self.block_delim = prev_block_delim;
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
            TokenKind::StarIdent(_) => self.parse_source_or_view(),
            TokenKind::Ampersand => self.parse_derived(),
            TokenKind::AmpersandIdent(_) => self.parse_derived(),
            TokenKind::Lower(_) => self.parse_fun(),
            TokenKind::Route => self.parse_route_decl(),
            _ => {
                self.error_at(start, "expected declaration");
                None
            }
        }
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
            TokenKind::Underscore => {
                self.advance();
                Some(UnitExpr::Hole)
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

    /// Whether the next token can start a unit type argument: a bare unit
    /// name (`M`, `u`), `1` (dimensionless), or `(` for a compound unit
    /// expression (`M / S^2`). Used after `Float`/`Int` to decide whether to
    /// parse a postfix unit argument.
    fn can_start_unit_type_arg(&self) -> bool {
        // Don't consume a unit arg when the next token is a migrate clause
        // keyword (`to`/`using`) — `migrate *r from Int to Float ...` must
        // not parse `Int to` as `Int` with unit `to`.
        if self.stop_type_at_migrate_clauses
            && matches!(self.peek(), TokenKind::Lower(s) if s == "to" || s == "using")
        {
            return false;
        }
        match self.peek() {
            TokenKind::Upper(_) | TokenKind::Lower(_) | TokenKind::LParen => true,
            TokenKind::Underscore => true,
            TokenKind::Int(n) => n == "1",
            _ => false,
        }
    }

    /// Parse a unit argument in type position: `M`, `u`, `1`, or
    /// `(M / S^2)`. A bare identifier is a single unit atom; a parenthesized
    /// form allows the full unit algebra (`* / ^`).
    fn parse_unit_type_arg(&mut self) -> Option<UnitExpr> {
        match self.peek() {
            TokenKind::LParen => {
                self.advance();
                let inner = self.parse_unit_expr()?;
                self.expect(&TokenKind::RParen, "expected ')' in unit argument").ok()?;
                Some(inner)
            }
            _ => self.parse_unit_atom(),
        }
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
                // Probe past newlines for a continuation `|`. If there isn't
                // one, restore so the cursor stays right after the last
                // constructor — otherwise the decl's span would swallow the
                // trailing newline and any same-line comment.
                let saved = this.save();
                this.skip_newlines();
                if !this.eat(&TokenKind::Pipe) {
                    this.restore(saved);
                    break;
                }
                this.skip_newlines();
                constructors.push(this.parse_constructor_def()?);
            }

            // End of the constructor list. Capture it before skipping
            // newlines to probe for an optional `deriving` clause, so that
            // when there is no `deriving`, the decl's span doesn't swallow the
            // trailing newline (and any same-line trailing comment) — which
            // would otherwise make the formatter treat that comment as
            // internal and fall back to verbatim copying.
            let mut end = this.prev_span();

            // Optional deriving clause (possibly on a following line).
            let saved = this.save();
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
                end = this.prev_span();
            } else {
                this.restore(saved);
            }
            Some(Decl {
                node: DeclKind::Data {
                    name,
                    params,
                    constructors,
                    deriving,
                },
                span: Span::new(start.start, end.end),
            })
        })
    }

    fn parse_constructor_def(&mut self) -> Option<ConstructorDef> {
        let (name, _) = self.expect_upper("expected constructor name").ok()?;
        let mut fields = Vec::new();
        if self.eat(&TokenKind::LBrace) {
            self.skip_newlines();
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
                    self.skip_newlines();
                    if self.at(&TokenKind::RBrace) {
                        break; // trailing comma
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
            })
        })
    }

    // ── source / view ────────────────────────────────────────────────

    fn parse_source_or_view(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("source/view declaration", |this| {
            // Consume the source/view name. `*name` lexes as a single StarIdent
            // token (name includes the `*`, which we strip); fall back to the
            // legacy `Star` + `Lower` form for robustness.
            let name = match this.peek() {
                TokenKind::StarIdent(_) => {
                    let tok = this.advance();
                    let TokenKind::StarIdent(n) = tok.kind else { unreachable!() };
                    n.trim_start_matches('*').to_string()
                }
                TokenKind::Star => {
                    this.advance(); // consume `*`
                    let (n, _) = this.expect_lower("expected name after '*'").ok()?;
                    n
                }
                _ => {
                    this.error("expected source/view name (e.g. *name)");
                    return None;
                }
            };

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
                    });
                }

                let end = this.prev_span();
                Some(Decl {
                    node: DeclKind::Source { name, ty },
                    span: Span::new(start.start, end.end),
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

            // Parse right side: *relation.field or *relation. The `*name` may
            // arrive as a single StarIdent token or legacy Star + Lower.
            let right_relation = match this.peek() {
                TokenKind::StarIdent(_) => {
                    let tok = this.advance();
                    let TokenKind::StarIdent(n) = tok.kind else { unreachable!() };
                    n.trim_start_matches('*').to_string()
                }
                TokenKind::Star => {
                    this.advance();
                    let (n, _) = this
                        .expect_lower("expected relation name after '*' in subset constraint")
                        .ok()?;
                    n
                }
                _ => {
                    this.error("expected '*' before relation name in subset constraint");
                    return None;
                }
            };

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
            })
        })
    }

    // ── derived ──────────────────────────────────────────────────────

    fn parse_derived(&mut self) -> Option<Decl> {
        let start = self.span();
        self.in_context("derived declaration", |this| {
            // `&name` lexes as a single AmpersandIdent (name includes the `&`);
            // fall back to the legacy `Ampersand` + `Lower` form for robustness.
            let name = match this.peek() {
                TokenKind::AmpersandIdent(_) => {
                    let tok = this.advance();
                    let TokenKind::AmpersandIdent(n) = tok.kind else { unreachable!() };
                    n.trim_start_matches('&').to_string()
                }
                _ => {
                    this.advance(); // consume `&`
                    let (n, _) = this.expect_lower("expected name after '&'").ok()?;
                    n
                }
            };

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
            })
        })
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
                });
            }

            this.expect(&TokenKind::Where, "expected 'where' or '=' after route name")
                .ok()?;

            let no_prefix: Vec<PathSegment> = vec![];
            let entries = this.parse_route_entries_with_prefix(&no_prefix, 0);

            let end = this.prev_span();
            Some(Decl {
                node: DeclKind::Route { name, entries },
                span: Span::new(start.start, end.end),
            })
        })
    }

    /// Parse a route reference: a bare route name (`Api`) or a dotted field
    /// path to a record-embedded route (`rec.Api`, `a.b.TodoApi`). Used by
    /// composite components (`route X = A | rec.B`) and by `serve`'s API head.
    fn parse_route_component_path(&mut self) -> Option<String> {
        // First segment may be a record field (lowercase) or a route name
        // (uppercase); every later segment is a route name (uppercase) since
        // routes are always declared with an uppercase field name.
        let mut path = match self.peek().clone() {
            TokenKind::Lower(_) | TokenKind::Upper(_) => {
                let tok = self.advance();
                match tok.kind {
                    TokenKind::Lower(s) | TokenKind::Upper(s) => s,
                    _ => unreachable!(),
                }
            }
            _ => {
                self.error("expected route name");
                return None;
            }
        };
        while self.eat(&TokenKind::Dot) {
            let (seg, _) = self
                .expect_upper("expected route name after '.' in route path")
                .ok()?;
            path.push('.');
            path.push_str(&seg);
        }
        Some(path)
    }

    /// Parse route entries, supporting path prefix nesting.
    /// A line starting with `/` (no HTTP method) introduces a prefix group;
    /// nested entries under it have the prefix prepended to their paths.
    ///
    /// Each `/`-prefixed group recurses, so a long run of `/...` lines would
    /// otherwise grow the native call stack without bound and abort the
    /// process. Charge the shared recursion budget so pathological input
    /// surfaces a "nesting depth limit exceeded" diagnostic instead.
    /// `floor` is the column of the `/prefix` line that introduced this group
    /// (0 at the top level). Nested entries must be strictly more indented than
    /// it, so a same-indent sibling is not absorbed into the group.
    fn parse_route_entries_with_prefix(
        &mut self,
        prefix: &[PathSegment],
        floor: usize,
    ) -> Vec<RouteEntry> {
        if !self.enter_recursion() {
            return vec![];
        }
        let entries = self.parse_route_entries_inner(prefix, floor);
        self.recursion_depth -= 1;
        entries
    }

    fn parse_route_entries_inner(
        &mut self,
        prefix: &[PathSegment],
        floor: usize,
    ) -> Vec<RouteEntry> {
        self.skip_newlines();
        if self.at_eof() {
            return vec![];
        }
        let indent = self.cur_column();
        // A nested group's entries must be strictly more indented than the
        // `/prefix` line that introduced them. Otherwise a same-indent sibling
        // would be wrongly absorbed as a child and get the prefix prepended.
        if indent <= floor {
            return vec![];
        }
        let mut entries = vec![];
        loop {
            self.skip_newlines();
            if self.at_eof() {
                break;
            }
            let col = self.cur_column();
            if col < indent {
                break;
            }
            if self.at(&TokenKind::Slash) {
                // Path prefix group: `/prefix` followed by nested entries
                let group_col = col;
                let prefix_path = self.parse_route_path();
                let mut combined = prefix.to_vec();
                combined.extend(prefix_path);
                let nested = self.parse_route_entries_with_prefix(&combined, group_col);
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
            self.skip_newlines();
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
                    self.skip_newlines();
                    if self.at(&TokenKind::RBrace) {
                        break; // trailing comma
                    }
                }
            }
            self.expect(&TokenKind::RBrace, "expected '}' to close route body fields")
                .ok()?;
        }

        // Parse path: /segment/{param: Type}/...
        self.skip_newlines();
        let path = self.parse_route_path();

        // Optional query params: ?{name: Type, ...}
        self.skip_newlines();
        let mut query_params = Vec::new();
        if self.eat(&TokenKind::Question) {
            self.expect(&TokenKind::LBrace, "expected '{' after '?'").ok()?;
            self.skip_newlines();
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
                    self.skip_newlines();
                    if self.at(&TokenKind::RBrace) {
                        break; // trailing comma
                    }
                }
            }
            self.expect(&TokenKind::RBrace, "expected '}' to close query params")
                .ok()?;
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

        // Optional rate limit: `rateLimit <expr>`
        self.skip_newlines();
        let rate_limit = if matches!(self.peek(), TokenKind::Lower(s) if s == "rateLimit") {
            self.advance();
            self.parse_expr()
        } else {
            None
        };

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
            rate_limit,
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
                self.consume_route_dashed_suffix(&mut seg);
                segments.push(PathSegment::Literal(seg));
            } else if matches!(self.peek(), TokenKind::Upper(_)) {
                // uppercase segment like /api/v1 — unlikely but handle
                let tok = self.advance();
                let TokenKind::Upper(s) = tok.kind else { unreachable!() };
                let mut seg = s;
                self.consume_route_dashed_suffix(&mut seg);
                segments.push(PathSegment::Literal(seg));
            } else if self.peek().keyword_str().is_some() {
                let tok = self.advance();
                let Some(kw) = tok.kind.keyword_str() else {
                    // Token disappeared between peek and advance; bail
                    // out of segment collection rather than panicking.
                    break;
                };
                let mut seg = kw.to_string();
                self.consume_route_dashed_suffix(&mut seg);
                segments.push(PathSegment::Literal(seg));
            } else if matches!(
                self.peek(),
                TokenKind::Int(_)
                    | TokenKind::Float(_)
                    | TokenKind::Text(_)
                    | TokenKind::Bytes(_)
                    | TokenKind::Bool(_)
            ) {
                // A literal can't be a path segment. Report and consume it so
                // the segment isn't silently dropped (the leading `/` was
                // already eaten) and the loop still makes progress.
                let name = self.peek().display_name();
                self.error(format!(
                    "invalid path segment: expected an identifier after '/', found {name}"
                ));
                self.advance();
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
            self.skip_newlines();
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
                    self.skip_newlines();
                    if self.at(&TokenKind::RBrace) {
                        break; // trailing comma
                    }
                }
            }
            let _ = self.expect(&TokenKind::RBrace, "expected '}' to close headers");
        }
        fields
    }

    // ── migrate ──────────────────────────────────────────────────────

    /// Try to parse `(Constraint =>)+`. Returns None if it doesn't look like constraints.
    fn try_parse_constraints(&mut self) -> Option<Vec<Constraint>> {
        let mut constraints = Vec::new();
        loop {
            let saved = self.save();
            // Allow newlines between constraints (e.g. after a previous `=>`).
            self.skip_newlines();
            // Implicit-field constraint: `(^field : Type) =>`.
            if matches!(self.peek(), TokenKind::LParen) {
                let after_lparen = self.save();
                self.advance(); // `(`
                if matches!(self.peek(), TokenKind::Caret) {
                    self.advance(); // `^`
                    if let TokenKind::Lower(field) = self.peek().clone() {
                        self.advance();
                        self.skip_newlines();
                        if self.eat(&TokenKind::Colon) {
                            self.skip_newlines();
                            if let Some(ty) = self.parse_type() {
                                self.skip_newlines();
                                if self.eat(&TokenKind::RParen) {
                                    let pre_arrow = self.save();
                                    self.skip_newlines();
                                    if self.eat(&TokenKind::FatArrow) {
                                        constraints.push(Constraint::ImplicitField { field, ty });
                                        continue;
                                    }
                                    self.restore(pre_arrow);
                                }
                            }
                        }
                    }
                }
                self.restore(after_lparen);
                self.restore(saved);
                break;
            }
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
                    constraints.push(Constraint::Trait { trait_name, args });
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
        let expr = self.parse_expr_head()?;
        // Postfix type annotation: `expr : Type` (without the surrounding
        // parens that `(expr : Type)` requires). Consumed greedily so a
        // trailing annotation binds to whatever expression just parsed.
        if self.at(&TokenKind::Colon) {
            self.advance();
            let ty = self.parse_type()?;
            let span = Span::new(expr.span.start, ty.span.end);
            return Some(Spanned::new(
                ExprKind::Annot {
                    expr: Box::new(expr),
                    ty,
                },
                span,
            ));
        }
        Some(expr)
    }

    fn parse_expr_head(&mut self) -> Option<Expr> {
        let saved = self.save();
        self.skip_newlines();
        // If skipping newlines crossed a line break and landed at column 0 (a
        // new declaration) or at/under the enclosing block indent (a new block
        // item), there is no expression here — the operand is missing (e.g. a
        // declaration with an empty RHS: `greet =` followed by `main = …`).
        // Report the missing expression and restore to the newline so the outer
        // block recovery resumes cleanly at the next item, instead of reading
        // the following declaration as this expression's head and dropping it.
        // Mirrors the operator-RHS and application-continuation guards, which
        // use the same column rule for mid-expression continuation.
        if self.pos != saved.0 && self.at_layout_boundary() {
            self.error("expected expression");
            self.restore(saved);
            return None;
        }
        match self.peek() {
            TokenKind::Backslash => self.parse_lambda(),
            TokenKind::If => self.parse_if(),
            TokenKind::Case => self.parse_case(),
            TokenKind::Do => self.parse_do_expr(),
            TokenKind::Serve => self.parse_serve_expr(),
            TokenKind::StarIdent(_) => {
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
                // `replace *rel = expr` is a replace-set expression. So is
                // `replace db.*rel = expr` (a source-field on a record-var:
                // `Lower` `.` `StarIdent`). Otherwise `replace` is treated as
                // a regular identifier.
                let mut offset = 1;
                while self.peek_ahead(offset) == &TokenKind::Newline {
                    offset += 1;
                }
                let next = self.peek_ahead(offset);
                let is_source_target = next == &TokenKind::Star || matches!(next, TokenKind::StarIdent(_));
                // `db.*rel` — a record-var target whose field is a source.
                let is_record_source_target = matches!(next, TokenKind::Lower(_))
                    && self.peek_ahead(offset + 1) == &TokenKind::Dot
                    && matches!(self.peek_ahead(offset + 2), TokenKind::StarIdent(_));
                if is_source_target || is_record_source_target {
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
                if !self.enter_recursion() { return None; }
                let start = self.span();
                self.advance();
                let e = self.parse_expr();
                self.recursion_depth -= 1;
                let e = e?;
                let end_sp = e.span;
                Some(Spanned::new(
                    ExprKind::Atomic(Box::new(e)),
                    Span::new(start.start, end_sp.end),
                ))
            }
            TokenKind::Refine => {
                if !self.enter_recursion() { return None; }
                let start = self.span();
                self.advance();
                let e = self.parse_expr();
                self.recursion_depth -= 1;
                let e = e?;
                let end_sp = e.span;
                Some(Spanned::new(
                    ExprKind::Refine(Box::new(e)),
                    Span::new(start.start, end_sp.end),
                ))
            }
            TokenKind::With => self.parse_with_expr(),
            _ => self.parse_expr_bp(0),
        }
    }

    /// Pratt parsing entry point.
    fn parse_expr_bp(&mut self, min_bp: u8) -> Option<Expr> {
        let mut lhs = self.parse_unary()?;

        // Each spine node accumulated by this loop adds one level of nesting to
        // the returned AST. Left-associative chains (`a+b+c+…`) are built
        // iteratively — the RHS-descent guard below returns immediately for
        // them, so it never accumulates depth. Charge the depth budget per
        // spine node and hold it until return so a pathological flat chain
        // (`1+1+…` with tens of thousands of terms) hits the nesting limit and
        // reports a diagnostic, instead of building an AST whose first recursive
        // traversal (Drop, inference, codegen) overflows the native stack.
        let mut spine_charged = 0usize;

        loop {
            // Skip newlines in certain contexts to allow multiline expressions.
            // But be careful: a newline at column 0 might be a new declaration.
            let saved_pos = self.save();
            self.skip_newlines();

            // If the next token starts a new line, it only continues this
            // expression as a binary operator when it stays past the enclosing
            // block's indent — the same rule parse_application uses for
            // multi-line continuation. A token at column 0 (outside delimiters)
            // is a new declaration; a token at (or before) the block indent
            // starts a new block item (a do statement like `-1`, a case arm
            // like `-1 -> ...`, etc.). The block-item rule holds even inside
            // parens for a `do`/`case` block opened at this delimiter depth, so
            // a parenthesized `do\n  let a = 1\n  -2` does not glue its
            // statements into `let a = 1 - 2` on reparse.
            if self.pos != saved_pos.0 && self.at_layout_boundary() {
                self.restore(saved_pos);
                break;
            }

            // `*name` is a source reference, not multiplication — but only when
            // `*name` now lexes as a single `StarIdent` token, so a `Star`
            // reaching the binop loop is ALWAYS the multiplication operator —
            // the old left/right-adjacency heuristic is obsolete (it existed
            // only to disambiguate `*` as a separate token). No special-casing
            // needed here.

            // `^name` begins a fresh implicit-field-projection term (an
            // application argument), not a binary operator — break out so the
            // application machinery picks it up. Mirrors `*name` above.
            if matches!(self.peek(), TokenKind::Caret) {
                let caret_span = self.peek_token().span;
                let right_adjacent = match self.tokens.get(self.pos + 1) {
                    Some(next) => {
                        matches!(next.kind, TokenKind::Lower(_))
                            && next.span.start == caret_span.end
                    }
                    None => false,
                };
                if right_adjacent {
                    self.restore(saved_pos);
                    break;
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
                TokenKind::Percent => (BinOp::Mod, 15, 16),
                TokenKind::Caret => {
                    // `^` is only meaningful as a unit-of-measure exponent
                    // (e.g. `M^2`), which is parsed in unit context — it has no
                    // meaning in an ordinary expression. Report it clearly and
                    // consume the operator + RHS so the stray `^ b` doesn't
                    // cascade into a misleading "expected declaration" error.
                    self.error(
                        "`^` is not an expression operator — it is only valid \
                         in unit-of-measure exponents like `M^2`",
                    );
                    self.advance(); // consume `^`
                    self.skip_newlines();
                    if self.enter_recursion() {
                        let _ = self.parse_expr_bp(0); // parse and discard RHS
                        self.recursion_depth -= 1;
                    }
                    break;
                }
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
            let pos_before_rhs = self.pos;
            self.skip_newlines();
            // A dangling operator at end of line: if the next token begins a
            // new declaration (column 0) or a new block item (at/under the
            // enclosing block indent), it is NOT the operator's RHS. Report the
            // missing operand and stop, leaving the token for the outer parser
            // to recover on, rather than gobbling the following declaration.
            // Mirrors the pre-operator guard above for the symmetric case.
            //
            // Only relevant when a newline was actually crossed: if the RHS
            // token is on the same line as the operator it can't be a
            // line-leading declaration/block item. Gating on that also avoids
            // calling the O(line-length) `column_of` once per operator, which
            // made a long single-line `1+1+…+1` chain parse in O(n²).
            if self.pos != pos_before_rhs && self.at_layout_boundary() {
                self.error("expected expression after binary operator");
                break;
            }
            // Allow if/case/do/lambda/atomic/refine on the RHS of
            // binary operators.  These are handled by `parse_expr` but not by
            // the Pratt sub-parser, so we delegate to `parse_expr` when we see
            // one of these keyword tokens.
            // Guard the recursive RHS descent. Right-associative operators
            // (`++`, l_bp == r_bp) parse their RHS by re-entering at the same
            // binding power, so a long right-associative chain recurses one
            // native frame per operator. Charge recursion across the call —
            // mirroring `parse_type_function`'s right-recursive `->` — so a
            // pathological chain hits the depth limit and reports an error
            // instead of overflowing the stack. The `parse_atom` cost is
            // released before this point, so without this guard the depth
            // counter never accumulates here.
            if !self.enter_recursion() { self.recursion_depth -= spine_charged; return None; }
            let rhs = if matches!(
                self.peek(),
                TokenKind::If
                    | TokenKind::Case
                    | TokenKind::Do
                    | TokenKind::Backslash
                    | TokenKind::Atomic
                    | TokenKind::Refine
            ) {
                self.parse_expr()
            } else {
                self.parse_expr_bp(r_bp)
            };
            self.recursion_depth -= 1;
            let rhs = match rhs {
                Some(rhs) => rhs,
                None => { self.recursion_depth -= spine_charged; return None; }
            };

            // Charge one depth unit for the spine node we're about to build and
            // hold it until return (see the comment at the top of this fn).
            if !self.enter_recursion() { self.recursion_depth -= spine_charged; return None; }
            spine_charged += 1;

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

        self.recursion_depth -= spine_charged;
        Some(lhs)
    }

    fn parse_unary(&mut self) -> Option<Expr> {
        match self.peek() {
            // Fold a prefix `-` over an integer literal into a single negative
            // literal. `i64::MIN` has no positive counterpart, so
            // `-9223372036854775808` is only expressible if the two tokens
            // become one literal before the value is parsed.
            TokenKind::Minus if matches!(self.peek_ahead(1), TokenKind::Int(_)) => {
                let minus_tok = self.advance();
                let tok = self.advance();
                let TokenKind::Int(n) = tok.kind else { unreachable!() };
                let span = Span::new(minus_tok.span.start, tok.span.end);
                let lit = Spanned::new(ExprKind::Lit(Literal::Int(format!("-{}", n))), span);
                // Feed the folded literal through the postfix and application
                // layers so `-5 x` parses as `App(-5, x)` like `5 x`, instead
                // of short-circuiting to just `-5`.
                let lit = self.maybe_time_unit(lit)?;
                let lit = self.parse_postfix_from(lit)?;
                self.parse_application_from(lit)
            }
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
        let func = self.parse_postfix()?;
        self.parse_application_from(func)
    }

    /// Continue an application chain from an already-parsed head expression.
    /// Used both by `parse_application` and by the prefix-minus integer fold,
    /// so `-5 x` parses as `App(-5, x)` just like `5 x`.
    fn parse_application_from(&mut self, mut func: Expr) -> Option<Expr> {
        // Application chains (`f a b c …`) are built iteratively into a
        // left-spine, so — like the binop loop — they must charge the depth
        // budget per node and hold it until return, otherwise a pathological
        // chain produces an AST whose first recursive traversal overflows the
        // stack. See `parse_expr_bp` for the full rationale.
        let mut spine_charged = 0usize;

        macro_rules! fail {
            () => {{
                self.recursion_depth -= spine_charged;
                return None;
            }};
        }

        loop {
            if self.can_start_atom() {
                let arg = match self.parse_postfix() {
                    Some(arg) => arg,
                    None => fail!(),
                };
                if !self.enter_recursion() { fail!() }
                spine_charged += 1;
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
            //
            // Only worth attempting when `skip_newlines` actually crossed a
            // newline: if it didn't, the position is unchanged from the
            // `can_start_atom()` check above (which already returned false), so
            // nothing new can continue the application. Gating on that — and
            // testing the O(1) `can_start_atom` before the O(line-length)
            // `column_of` — keeps a long single-line application chain linear
            // instead of O(n²).
            let saved = self.save();
            self.skip_newlines();
            if self.pos != saved.0
                && !self.at_eof()
                && self.can_start_atom()
                && self.cur_column() > self.block_indent
            {
                let arg = match self.parse_postfix() {
                    Some(arg) => arg,
                    None => fail!(),
                };
                if !self.enter_recursion() { fail!() }
                spine_charged += 1;
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

        self.recursion_depth -= spine_charged;
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
            // While parsing a route's response type (including a refined
            // type's `where` predicate), `headers`/`rateLimit` are clause
            // keywords and must not be consumed as application arguments.
            // Likewise, while parsing a migrate `from`/`to` type (including a
            // refined type's `where` predicate expression), `to`/`using` are
            // clause keywords and must not be eaten as application arguments.
            TokenKind::Lower(n) => {
                n != "yield"
                    && !(self.stop_type_at_headers && (n == "headers" || n == "rateLimit"))
                    && !(self.stop_type_at_migrate_clauses && (n == "to" || n == "using"))
            }
            TokenKind::StarIdent(_) => {
                // `*name` is a single source-reference token — always a valid
                // application argument. (The `Star` operator never reaches here
                // as an atom starter; it's handled by the binop loop.)
                true
            }
            TokenKind::AmpersandIdent(_) => {
                // `&name` is a single derived-reference token — always a valid
                // application argument (mirrors StarIdent).
                true
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
            TokenKind::Caret => {
                // Implicit field projection `^name` only when `^` is immediately
                // adjacent to a Lower token (mirrors `&name`).
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
    /// The payload is parsed with the same postfix handling as
    /// function-application arguments, so `Just x.y` parses as
    /// `Just (x.y)` — consistent with `f x.y` parsing as `f (x.y)`.
    fn parse_constructor_or_atom(&mut self) -> Option<Expr> {
        let expr = self.parse_atom()?;
        if matches!(expr.node, ExprKind::Constructor(_)) && self.can_start_atom() {
            if !self.enter_recursion() { return None; }
            let arg = self.parse_postfix();
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
        let expr = self.parse_constructor_or_atom()?;
        self.parse_postfix_from(expr)
    }

    /// Continue a field-access chain from an already-parsed head expression.
    fn parse_postfix_from(&mut self, mut expr: Expr) -> Option<Expr> {
        // Field-access chains (`x.a.b.c…`) build a left-spine iteratively, so —
        // like the binop and application loops — charge the depth budget per
        // node and hold it until return to bound the resulting AST depth (see
        // `parse_expr_bp`).
        let mut spine_charged = 0usize;

        loop {
            if self.at(&TokenKind::Dot) {
                self.advance();
                // Field names are normally lowercase; a record may also carry a
                // first-class type-constructor field named after a `type` alias
                // (uppercase), accessed the same way (`r.Pair`). A
                // source-relation field is literally named `*name` (a
                // StarIdent), accessed as `db.*todos` — the field name KEEPS
                // the leading `*`.
                if matches!(self.peek(), TokenKind::StarIdent(_)) {
                    let tok = self.advance();
                    let TokenKind::StarIdent(field) = tok.kind else { unreachable!() };
                    let field_span = tok.span;
                    if !self.enter_recursion() { self.recursion_depth -= spine_charged; return None; }
                    spine_charged += 1;
                    let span = Span::new(expr.span.start, field_span.end);
                    expr = Spanned::new(
                        ExprKind::FieldAccess {
                            expr: Box::new(expr),
                            field,
                        },
                        span,
                    );
                    continue;
                }
                // A derived-relation field is literally named `&name` (an
                // AmpersandIdent), accessed as `db.&seniors` — the field name
                // KEEPS the leading `&`.
                if matches!(self.peek(), TokenKind::AmpersandIdent(_)) {
                    let tok = self.advance();
                    let TokenKind::AmpersandIdent(field) = tok.kind else { unreachable!() };
                    let field_span = tok.span;
                    if !self.enter_recursion() { self.recursion_depth -= spine_charged; return None; }
                    spine_charged += 1;
                    let span = Span::new(expr.span.start, field_span.end);
                    expr = Spanned::new(
                        ExprKind::FieldAccess {
                            expr: Box::new(expr),
                            field,
                        },
                        span,
                    );
                    continue;
                }
                let field_result = if matches!(self.peek(), TokenKind::Upper(_)) {
                    self.expect_upper("expected field name after '.'")
                } else {
                    self.expect_lower("expected field name after '.'")
                };
                let (field, field_span) = match field_result {
                    Ok(pair) => pair,
                    Err(_) => { self.recursion_depth -= spine_charged; return None; }
                };
                if !self.enter_recursion() { self.recursion_depth -= spine_charged; return None; }
                spine_charged += 1;
                let span = Span::new(expr.span.start, field_span.end);
                expr = Spanned::new(
                    ExprKind::FieldAccess {
                        expr: Box::new(expr),
                        field,
                    },
                    span,
                );
            } else {
                break;
            }
        }

        self.recursion_depth -= spine_charged;
        Some(expr)
    }

    /// If the next token is a time-unit identifier (`ms`, `seconds`, `minutes`,
    /// `hours`, `days`, `weeks`), consume it and desugar `n unit` into `n * factor`
    /// where factor is the millisecond equivalent. The multiplication is wrapped
    /// in an `ExprKind::TimeUnitLit` that also records the original unit word, so
    /// the formatter can re-render `n unit` instead of the raw `n * factor`
    /// (inference and codegen unwrap the wrapper and see only the multiplication).
    fn maybe_time_unit(&mut self, lit: Expr) -> Option<Expr> {
        let factor: Option<&str> = match self.peek() {
            // A locally-bound variable named like a time unit is NOT unit
            // sugar: `\ms -> g 2 ms` must apply `g` to `2` and `ms`.
            TokenKind::Lower(u) if self.is_bound_var(u) || self.top_level_names.contains(u) => None,
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
                let unit_span = unit_tok.span;
                let unit_name = match unit_tok.kind {
                    TokenKind::Lower(u) => u,
                    _ => unreachable!("time-unit suffix is always a Lower token"),
                };
                let span = Span::new(lit.span.start, unit_span.end);
                // Match the factor literal's kind to the operand's so the
                // multiplication stays homogeneous. A Float operand
                // (`2.5 seconds`) with an Int factor would produce a
                // `Float * Int` node that `Num`'s same-type `mul` rejects;
                // emit `Float` (`1000.0`) instead so `2.5 seconds` is `2500.0`.
                let factor_lit = if matches!(&lit.node, ExprKind::Lit(Literal::Float(_))) {
                    Literal::Float(f.parse::<f64>().unwrap_or(0.0))
                } else {
                    Literal::Int(f.to_string())
                };
                // Keep the desugared multiplication as `value` so inference and
                // codegen treat `2 seconds` exactly like `2 * 1000`, but wrap it
                // in `TimeUnitLit` so the formatter can recover the surface form
                // (`2 seconds`) instead of rewriting it to raw multiplication.
                let mul = Spanned::new(
                    ExprKind::BinOp {
                        op: BinOp::Mul,
                        lhs: Box::new(lit),
                        rhs: Box::new(Spanned::new(
                            ExprKind::Lit(factor_lit),
                            unit_span,
                        )),
                    },
                    span,
                );
                Some(Spanned::new(
                    ExprKind::TimeUnitLit {
                        value: Box::new(mul),
                        unit_name,
                    },
                    span,
                ))
            }
            None => Some(lit),
        }
    }

    fn parse_atom(&mut self) -> Option<Expr> {
        // Guard recursion here: every expression-side delimiter cycle
        // (parens, records, lists → parse_expr → ... → parse_atom) flows
        // through this entry point, so guarding it prevents stack overflow
        // on pathological input like `((((…))))`.
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.parse_atom_inner();
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    fn parse_atom_inner(&mut self) -> Option<Expr> {
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
            TokenKind::StarIdent(_) => {
                // `*name` lexed as a single token — source reference.
                let tok = self.advance();
                let TokenKind::StarIdent(n) = tok.kind else { unreachable!() };
                Some(Spanned::new(
                    ExprKind::SourceRef(n.trim_start_matches('*').to_string()),
                    Span::new(start.start, tok.span.end),
                ))
            }
            TokenKind::Star => {
                // *name — source reference (legacy Star + Lower form)
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
            TokenKind::Caret => {
                // ^name — implicit field projection
                self.advance();
                match self.peek() {
                    TokenKind::Lower(_) => {
                        let tok = self.advance();
                        let TokenKind::Lower(name) = tok.kind else { unreachable!() };
                        Some(Spanned::new(
                            ExprKind::ImplicitRef(name),
                            Span::new(start.start, tok.span.end),
                        ))
                    }
                    _ => {
                        self.error(
                            "expected identifier after '^' for implicit field projection",
                        );
                        None
                    }
                }
            }
            TokenKind::AmpersandIdent(_) => {
                // `&name` lexed as a single token — derived reference.
                let tok = self.advance();
                let TokenKind::AmpersandIdent(n) = tok.kind else { unreachable!() };
                Some(Spanned::new(
                    ExprKind::DerivedRef(n.trim_start_matches('&').to_string()),
                    Span::new(start.start, tok.span.end),
                ))
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
                let Some(inner) = self.parse_expr() else {
                    self.delimiter_depth -= 1;
                    return None;
                };
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
            TokenKind::Serve => self.parse_serve_expr(),
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

        // New record syntax: whitespace-separated `name value` pairs, no `:` or
        // `,`. A field value is a single atom / postfix chain / parenthesized
        // (or list / nested-record) compound — i.e. `parse_postfix`, which never
        // consumes a following bare identifier as an application argument. A bare
        // `Lower` after a value therefore always opens the next field.
        //
        // A record UPDATE `{base | name value, ...}` is detected by speculatively
        // parsing a leading postfix expression and checking for a top-level `|`.
        // A leading `Lower` is always a field name, never an update base, so we
        // only speculate when the first token is NOT a plain identifier.
        let first_is_lower = matches!(self.peek(), TokenKind::Lower(_));
        if !first_is_lower {
            // Speculative base parse for `{base | …}` (e.g. base is a parenthesized
            // expr, field-access chain, etc.). A bare-identifier base is handled by
            // the named-field path below and disambiguated there.
            let saved = self.save();
            let diag_count = self.diagnostics.len();
            if let Some(first_expr) = self.parse_postfix() {
                self.skip_newlines();
                if self.eat(&TokenKind::Pipe) {
                    let update_fields = self.parse_record_update_fields()?;
                    let end_tok = self
                        .expect(&TokenKind::RBrace, "expected '}' to close record update")
                        .ok()?;
                    return Some(Spanned::new(
                        ExprKind::RecordUpdate {
                            base: Box::new(first_expr),
                            fields: update_fields,
                        },
                        Span::new(start.start, end_tok.span.end),
                    ));
                }
            }
            self.restore(saved);
            self.diagnostics.truncate(diag_count);
        } else {
            // Leading identifier: could still be an update `{base | …}` where base
            // is a plain var or field-access chain. Parse the postfix, check `|`.
            let saved = self.save();
            let diag_count = self.diagnostics.len();
            if let Some(head) = self.parse_postfix() {
                self.skip_newlines();
                if self.eat(&TokenKind::Pipe) {
                    let update_fields = self.parse_record_update_fields()?;
                    let end_tok = self
                        .expect(&TokenKind::RBrace, "expected '}' to close record update")
                        .ok()?;
                    return Some(Spanned::new(
                        ExprKind::RecordUpdate {
                            base: Box::new(head),
                            fields: update_fields,
                        },
                        Span::new(start.start, end_tok.span.end),
                    ));
                }
            }
            self.restore(saved);
            self.diagnostics.truncate(diag_count);
        }

        // Named fields: `name value name2 value2 …`
        //
        // A field may carry a standalone type-signature line, written like a
        // record-type field, immediately before its value field:
        //   {name : Text
        //    name "a"
        //    age  : Int 1
        //    age  30}
        // The sig is attached to its value field and enforced by the checker;
        // the sig-line layout is preserved through the formatter.
        let mut fields: Vec<RecordField> = Vec::new();
        let mut pending_sigs: Vec<(Name, TypeScheme)> = Vec::new();
        loop {
            self.skip_newlines();
            if self.at(&TokenKind::RBrace) {
                break;
            }
            // A nested record value used as a field value (e.g. a migration's
            // `using` fn `\r -> {title r.title …}`) must terminate when the
            // next field sits at/below the enclosing block indent — otherwise
            // it would greedily absorb an OUTER record's following field.
            if self.at_layout_boundary() {
                break;
            }
            // `type Name p1 p2 … = <type>` — an embedded type-alias line. It
            // contributes a field named `Name` whose value is the (erased) type
            // constructor itself; the alias is also brought into type scope.
            if self.at(&TokenKind::Type) {
                self.advance(); // consume `type`
                let (tname, tspan) = self
                    .expect_upper("expected type name after 'type'")
                    .ok()?;
                let mut params = Vec::new();
                while matches!(self.peek(), TokenKind::Lower(_)) {
                    let tok = self.advance();
                    let TokenKind::Lower(p) = tok.kind else { unreachable!() };
                    params.push(p);
                }
                self.expect(&TokenKind::Eq, "expected '=' in type alias").ok()?;
                // Parse the alias body with `record_value_sig_type` set so a
                // `Lower` on the next line (a following record field) is not
                // absorbed as a type argument of the alias body.
                let saved_flag = self.record_value_sig_type;
                self.record_value_sig_type = true;
                let ty = self.parse_type();
                self.record_value_sig_type = saved_flag;
                let Some(ty) = ty else {
                    self.error("expected type after '=' in record type alias");
                    return None;
                };
                fields.push(RecordField {
                    name: tname.clone(),
                    value: Spanned::new(
                        ExprKind::TypeCtor {
                            name: tname,
                            params,
                            ty,
                        },
                        tspan,
                    ),
                    sig: None,
                });
                continue;
            }
            // `data Name p1 … = Ctor {…} | …` — an embedded data declaration.
            // Contributes a field named `Name` whose value is the (erased)
            // data-constructor namespace: the ctors become reachable as
            // `rec.Name.<Ctor>` and the type `Name` enters type scope.
            if self.at(&TokenKind::Data) {
                self.advance(); // consume `data`
                let (dname, dspan) = self
                    .expect_upper("expected type name after 'data'")
                    .ok()?;
                let mut params = Vec::new();
                while matches!(self.peek(), TokenKind::Lower(_)) {
                    let tok = self.advance();
                    let TokenKind::Lower(p) = tok.kind else { unreachable!() };
                    params.push(p);
                }
                self.skip_newlines();
                self.expect(&TokenKind::Eq, "expected '=' in data declaration").ok()?;
                self.skip_newlines();
                let mut constructors = vec![self.parse_constructor_def()?];
                loop {
                    let saved = self.save();
                    self.skip_newlines();
                    if !self.eat(&TokenKind::Pipe) {
                        self.restore(saved);
                        break;
                    }
                    self.skip_newlines();
                    constructors.push(self.parse_constructor_def()?);
                }
                fields.push(RecordField {
                    name: dname.clone(),
                    value: Spanned::new(
                        ExprKind::DataCtor {
                            name: dname,
                            params,
                            constructors,
                        },
                        dspan,
                    ),
                    sig: None,
                });
                continue;
            }
            // `route Name where …` / `route Name = A | B` — an embedded route
            // declaration. Contributes a field named `Name` whose value is a
            // pure marker (erased like a data decl): the route's entries are
            // registered statically under the record path (`rec.Name`) and
            // resolved by path at `serve rec.Name` / `fetch url (rec.Name.Ctor
            // …)` call sites. Composite components may themselves be field
            // paths (`other.TodoApi`).
            if self.at(&TokenKind::Route) {
                let rspan = self.span();
                let route_col = self.cur_column();
                self.advance(); // consume `route`
                let (rname, _) = self
                    .expect_upper("expected route name after 'route'")
                    .ok()?;

                // Composite: `route Name = A | B | other.C`
                if self.eat(&TokenKind::Eq) {
                    let mut components = Vec::new();
                    components.push(self.parse_route_component_path()?);
                    while self.eat(&TokenKind::Pipe) {
                        components.push(self.parse_route_component_path()?);
                    }
                    fields.push(RecordField {
                        name: rname.clone(),
                        value: Spanned::new(
                            ExprKind::RouteCompositeDecl {
                                name: rname,
                                components,
                            },
                            rspan,
                        ),
                        sig: None,
                    });
                    continue;
                }

                self.expect(&TokenKind::Where, "expected 'where' or '=' after route name")
                    .ok()?;
                let no_prefix: Vec<PathSegment> = vec![];
                // Route entries are indented one level deeper than the `route`
                // line inside the record, so the group's floor is the `route`
                // keyword's column.
                let entries =
                    self.parse_route_entries_with_prefix(&no_prefix, route_col);
                fields.push(RecordField {
                    name: rname.clone(),
                    value: Spanned::new(
                        ExprKind::RouteDecl {
                            name: rname,
                            entries,
                        },
                        rspan,
                    ),
                    sig: None,
                });
                continue;
            }
            // `*name : Type` — an embedded source-relation declaration; or
            // `*name = expr` / `*name : Type = expr` — an embedded view. The
            // field is literally named `*name`; its value is a marker (the
            // relation is registered statically and resolved by path). Parses
            // the type with `record_value_sig_type` set so a following field
            // on the next line is not absorbed as a type arg. Mirrors the
            // top-level `:`-vs-`=` disambiguation in `parse_source_or_view`.
            if matches!(self.peek(), TokenKind::StarIdent(_))
            {
                let tok = self.advance();
                let TokenKind::StarIdent(sname) = tok.kind else { unreachable!() };
                let sspan = tok.span;
                let bare_name = sname.trim_start_matches('*').to_string();

                // Subset constraint: `*rel.field <= *rel.field` / `*a <= *b`.
                // A marker field — no runtime value.
                if self.at(&TokenKind::Dot) || self.at(&TokenKind::Le) {
                    let left_field = if self.eat(&TokenKind::Dot) {
                        let (fld, _) = self.expect_lower("expected field name after '.'").ok()?;
                        Some(fld)
                    } else {
                        None
                    };
                    self.expect(&TokenKind::Le, "expected '<=' in subset constraint").ok()?;
                    let right_relation = match self.peek() {
                        TokenKind::StarIdent(_) => {
                            let t = self.advance();
                            let TokenKind::StarIdent(n) = t.kind else { unreachable!() };
                            n.trim_start_matches('*').to_string()
                        }
                        TokenKind::Star => {
                            self.advance();
                            let (n, _) = self
                                .expect_lower("expected relation name after '*' in subset constraint")
                                .ok()?;
                            n
                        }
                        _ => {
                            self.error("expected '*' before relation name in subset constraint");
                            return None;
                        }
                    };
                    let right_field = if self.eat(&TokenKind::Dot) {
                        let (fld, _) = self.expect_lower("expected field name after '.'").ok()?;
                        Some(fld)
                    } else {
                        None
                    };
                    let end = self.prev_span();
                    // A subset constraint is a marker, not a value field. Give it
                    // a synthetic name that can never collide with a real source
                    // (`*orders`) or with another constraint on the same
                    // relation — it is registered statically, not exposed as
                    // `db.*name`. `fields.len()` guarantees uniqueness.
                    let marker_name = format!("*{}#subset{}", bare_name, fields.len());
                    fields.push(RecordField {
                        name: marker_name,
                        value: Spanned::new(
                            ExprKind::SubsetConstraint {
                                sub: crate::ast::RelationPath {
                                    relation: bare_name,
                                    field: left_field,
                                },
                                sup: crate::ast::RelationPath {
                                    relation: right_relation,
                                    field: right_field,
                                },
                            },
                            Span::new(sspan.start, end.end),
                        ),
                        sig: None,
                    });
                    continue;
                }

                if self.eat(&TokenKind::Colon) {
                    self.skip_newlines();
                    let saved_flag = self.record_value_sig_type;
                    self.record_value_sig_type = true;
                    let sty = self.parse_type();
                    self.record_value_sig_type = saved_flag;
                    let Some(sty) = sty else {
                        self.error("expected type after ':' in record source declaration");
                        return None;
                    };
                    // Annotated view: `*name : Type = body`.
                    if self.eat(&TokenKind::Eq) {
                        self.skip_newlines();
                        let Some(body) = self.parse_expr() else {
                            self.error("expected view body after '=' in record view declaration");
                            return None;
                        };
                        fields.push(RecordField {
                            name: sname.clone(),
                            value: Spanned::new(
                                ExprKind::ViewDecl {
                                    name: bare_name,
                                    ty: Some(crate::ast::TypeScheme {
                                        constraints: vec![],
                                        ty: sty,
                                    }),
                                    body: Box::new(body),
                                },
                                sspan,
                            ),
                            sig: None,
                        });
                        continue;
                    }
                    // Source: optional migration clauses hanging off the field:
                    // `*todos : [Todo] migrate from A to B using f …`. Mirrors
                    // top-level `migrate` decls (cumulative).
                    let mut migrations = Vec::new();
                    while let Some(m) = self.parse_source_field_migration() {
                        migrations.push(m);
                    }
                    fields.push(RecordField {
                        name: sname.clone(),
                        value: Spanned::new(
                            ExprKind::SourceDecl {
                                name: bare_name,
                                ty: sty,
                                migrations,
                            },
                            sspan,
                        ),
                        sig: None,
                    });
                    continue;
                }

                // Unannotated view: `*name = body`.
                if self.eat(&TokenKind::Eq) {
                    self.skip_newlines();
                    let Some(body) = self.parse_expr() else {
                        self.error("expected view body after '=' in record view declaration");
                        return None;
                    };
                    fields.push(RecordField {
                        name: sname.clone(),
                        value: Spanned::new(
                            ExprKind::ViewDecl {
                                name: bare_name,
                                ty: None,
                                body: Box::new(body),
                            },
                            sspan,
                        ),
                        sig: None,
                    });
                    continue;
                }

                self.error("expected ':' or '=' after record source/view field name");
                return None;
            }
            // `&name = expr` / `&name : Type = expr` — an embedded derived
            // declaration. The field is literally named `&name`; its value is a
            // marker (the relation is registered statically and resolved by
            // path). Mirrors the top-level `parse_derived`.
            if matches!(self.peek(), TokenKind::AmpersandIdent(_)) {
                let tok = self.advance();
                let TokenKind::AmpersandIdent(sname) = tok.kind else { unreachable!() };
                let sspan = tok.span;
                let bare_name = sname.trim_start_matches('&').to_string();

                // Optional inline annotation: `&name : Type = body`.
                let ty = if self.eat(&TokenKind::Colon) {
                    self.skip_newlines();
                    let saved_flag = self.record_value_sig_type;
                    self.record_value_sig_type = true;
                    let sty = self.parse_type();
                    self.record_value_sig_type = saved_flag;
                    let Some(sty) = sty else {
                        self.error("expected type after ':' in record derived declaration");
                        return None;
                    };
                    Some(crate::ast::TypeScheme { constraints: vec![], ty: sty })
                } else {
                    None
                };

                if self.eat(&TokenKind::Eq) {
                    self.skip_newlines();
                    let Some(body) = self.parse_expr() else {
                        self.error("expected derived body after '=' in record derived declaration");
                        return None;
                    };
                    fields.push(RecordField {
                        name: sname.clone(),
                        value: Spanned::new(
                            ExprKind::DerivedDecl {
                                name: bare_name,
                                ty,
                                body: Box::new(body),
                            },
                            sspan,
                        ),
                        sig: None,
                    });
                    continue;
                }

                self.error("expected '=' after record derived field name");
                return None;
            }
            // Signature line: `name : Type`. The value for `name` is supplied by
            // a later `name value` field. Parse the type with
            // `record_value_sig_type` set so a `Lower` on the next line (the
            // field's value) is not absorbed as a type argument.
            if self.at_field_signature() {
                let (sname, _) = self.expect_lower("expected field name in record").ok()?;
                self.expect(&TokenKind::Colon, "expected ':' after field name").ok()?;
                self.skip_newlines();
                let saved_flag = self.record_value_sig_type;
                self.record_value_sig_type = true;
                let sty = self.parse_type_scheme();
                self.record_value_sig_type = saved_flag;
                let Some(sty) = sty else {
                    self.error("expected type after ':' in record field signature");
                    return None;
                };
                pending_sigs.push((sname, sty));
                continue;
            }
            let field_col = self.cur_column();
            let (fname, _) = self
                .expect_lower("expected field name in record")
                .ok()?;
            self.skip_newlines();
            // A bare lambda (`greet \name -> …`) or do-block (`run do …`) field
            // value. Field values normally use `parse_postfix` so a value can't
            // greedily absorb a following field as a function application — but
            // neither `\` nor `do` is a postfix head, so they'd be rejected.
            // Route them to their parsers, and pin `block_indent`/`block_delim`
            // to the field's column so the value's body terminates at the next
            // field (same column) via `at_layout_boundary` instead of absorbing
            // it. Mirrors the `using` clause in `parse_source_migration`.
            let Some(value) = (if self.at(&TokenKind::Backslash) || self.at(&TokenKind::Do) {
                let prev_bi = self.block_indent;
                let prev_bd = self.block_delim;
                self.block_indent = field_col;
                self.block_delim = self.delimiter_depth;
                let v = if self.at(&TokenKind::Backslash) {
                    self.parse_lambda()
                } else {
                    self.parse_do_expr()
                };
                self.block_indent = prev_bi;
                self.block_delim = prev_bd;
                v
            } else {
                self.parse_postfix()
            }) else {
                self.error("expected field value after field name in record");
                return None;
            };
            // Attach any pending sig for this field name.
            let sig = pending_sigs
                .iter()
                .position(|(n, _)| *n == fname)
                .map(|i| pending_sigs.remove(i).1);
            fields.push(RecordField {
                name: fname,
                value,
                sig,
            });
        }

        // Leftover sigs name signature-only fields: `name : Type` with no
        // value. These are required CLI constants — body-less `Fun` decls the
        // codegen registers as startup `--name=value` lookups. Emit each as a
        // field with an empty-record placeholder value; the lowering recognises
        // sig-present + empty-record as "no body" and produces `Fun{body:None}`.
        for (sname, sty) in pending_sigs {
            fields.push(RecordField {
                name: sname,
                value: Spanned::new(ExprKind::Record(Vec::new()), start),
                sig: Some(sty),
            });
        }

        self.skip_newlines();
        let end_tok = self
            .expect(&TokenKind::RBrace, "expected '}' to close record")
            .ok()?;
        let full_span = Span::new(start.start, end_tok.span.end);
        Some(Spanned::new(ExprKind::Record(fields), full_span))
    }

    /// Parse an optional `migrate from T to U using f` clause hanging off a
    /// record-embedded source field. Returns `None` when the next token is not
    /// `migrate`. Mirrors `parse_migrate`'s clause handling but with no
    /// relation name (the source field supplies it).
    fn parse_source_field_migration(&mut self) -> Option<crate::ast::SourceMigration> {
        self.skip_newlines();
        if !self.at(&TokenKind::Migrate) {
            return None;
        }
        self.advance(); // consume `migrate`

        let prev_block_indent = self.block_indent;
        self.block_indent = self.cur_column();

        if !matches!(self.peek(), TokenKind::Lower(s) if s == "from") {
            self.error("expected 'from' in source migration");
            self.block_indent = prev_block_indent;
            return None;
        }
        self.advance();

        self.stop_type_at_migrate_clauses = true;
        let from_ty = match self.parse_type() {
            Some(t) => t,
            None => {
                self.stop_type_at_migrate_clauses = false;
                self.block_indent = prev_block_indent;
                return None;
            }
        };

        self.skip_newlines();
        if !matches!(self.peek(), TokenKind::Lower(s) if s == "to") {
            self.error("expected 'to' in source migration");
            self.stop_type_at_migrate_clauses = false;
            self.block_indent = prev_block_indent;
            return None;
        }
        self.advance();

        let to_ty = match self.parse_type() {
            Some(t) => t,
            None => {
                self.stop_type_at_migrate_clauses = false;
                self.block_indent = prev_block_indent;
                return None;
            }
        };
        self.stop_type_at_migrate_clauses = false;

        self.skip_newlines();
        if !matches!(self.peek(), TokenKind::Lower(s) if s == "using") {
            self.error("expected 'using' in source migration");
            self.block_indent = prev_block_indent;
            return None;
        }
        self.advance();
        // Keep `block_indent` at the migrate-clause column while parsing
        // `using_fn` so a following record field at the outer indent terminates
        // the using-fn's record literal via `at_layout_boundary` instead of
        // being absorbed as one of its fields. Restore after.
        let using_fn = self.parse_expr()?;
        self.block_indent = prev_block_indent;
        Some(crate::ast::SourceMigration { from_ty, to_ty, using_fn })
    }

    /// Parse the `name value …` fields after the `|` in a record update
    /// `{base | name value, …}`. Stops before the closing `}`.
    fn parse_record_update_fields(&mut self) -> Option<Vec<Field<Expr>>> {
        let mut update_fields = Vec::new();
        self.skip_newlines();
        if !self.at(&TokenKind::RBrace) {
            loop {
                self.skip_newlines();
                if self.at(&TokenKind::RBrace) {
                    break;
                }
                let (fname, _) = self
                    .expect_lower("expected field name in record update")
                    .ok()?;
                self.skip_newlines();
                let Some(val) = self.parse_postfix() else {
                    self.error("expected field value after field name in record update");
                    return None;
                };
                update_fields.push(Field {
                    name: fname,
                    value: val,
                });
            }
        }
        self.skip_newlines();
        Some(update_fields)
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
                self.skip_newlines();
                if self.at(&TokenKind::RBracket) {
                    break; // trailing comma
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

    /// Lookahead: is the cursor at a type-witness parameter `(T : Type)`?
    /// Matches `LParen Upper Colon Upper("Type") RParen`. `Type` is an ordinary
    /// uppercase identifier (not a keyword), so compare by name.
    fn at_ty_param(&self) -> bool {
        if !matches!(self.peek(), TokenKind::LParen) {
            return false;
        }
        let is_upper = |k: &TokenKind| matches!(k, TokenKind::Upper(_));
        let name_is = |k: &TokenKind, want: &str| matches!(k, TokenKind::Upper(n) if n == want);
        is_upper(self.peek_ahead(1))
            && matches!(self.peek_ahead(2), TokenKind::Colon)
            && name_is(self.peek_ahead(3), "Type")
            && matches!(self.peek_ahead(4), TokenKind::RParen)
    }

    /// Parse a type-witness parameter `(T : Type)` — assumes `at_ty_param()`.
    fn parse_ty_param(&mut self) -> Option<crate::ast::TyParam> {
        let start = self.span();
        self.advance(); // (
        let name = match self.advance().kind {
            TokenKind::Upper(n) => n,
            _ => return None,
        };
        self.advance(); // :
        self.advance(); // Type
        let end = self.span();
        self.advance(); // )
        Some(crate::ast::TyParam {
            name,
            span: Span::new(start.start, end.end),
        })
    }

    fn parse_lambda(&mut self) -> Option<Expr> {
        let start = self.span();
        // Deeply nested lambdas (`\x -> \x -> … x`) would otherwise grow the
        // native call stack without bound and abort the process. `parse_lambda`
        // is dispatched from `parse_expr_head`, NOT `parse_atom`, so it never
        // gets the delimiter charge that guards `((((…))))`. Charge the budget
        // here so pathological nesting diagnoses instead of overflowing.
        // (Mirrors `parse_do_expr`. Same applies to `parse_if`/`parse_case`.)
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("lambda expression", |this| {
            this.advance(); // consume `\`

            let mut params = Vec::new();
            let mut ty_params = Vec::new();
            while !this.at(&TokenKind::Arrow) && !this.at_eof() {
                this.skip_newlines();
                if this.at(&TokenKind::Arrow) { break; }
                // Stop consuming params if we've crossed back to column 0 outside
                // any delimiter — this prevents eating into the next declaration
                // when `->` is missing.
                if this.delimiter_depth == 0 && this.cur_column() == 0 {
                    break;
                }
                // Type-witness parameter `\(T : Type)`. Lookahead: LParen Upper
                // Colon Upper("Type") RParen. Only valid in leading position
                // (before any value param). Erased at runtime.
                if params.is_empty() && this.at_ty_param() {
                    ty_params.push(this.parse_ty_param()?);
                    continue;
                }
                let p = this.parse_pat()?;
                params.push(p);
            }

            this.expect(&TokenKind::Arrow, "expected '->' in lambda expression")
                .ok()?;
            let scope_mark = this.bound_vars.len();
            for p in &params {
                this.push_pat_vars(p);
            }
            let body = this.parse_expr();
            this.bound_vars.truncate(scope_mark);
            let body = body?;

            let end_sp = body.span;
            Some(Spanned::new(
                ExprKind::Lambda {
                    params,
                    ty_params,
                    body: Box::new(body),
                },
                Span::new(start.start, end_sp.end),
            ))
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    fn parse_if(&mut self) -> Option<Expr> {
        let start = self.span();
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("if expression", |this| {
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
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    fn parse_case(&mut self) -> Option<Expr> {
        let start = self.span();
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("case expression", |this| {
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
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
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
        let scope_mark = self.bound_vars.len();
        self.push_pat_vars(&pat);
        let body = self.parse_expr();
        self.bound_vars.truncate(scope_mark);
        let body = body?;
        Some(CaseArm { pat, body })
    }

    fn parse_do_expr(&mut self) -> Option<Expr> {
        let start = self.span();
        // Deeply nested do-blocks (`do do do … 1`) would otherwise grow the
        // native call stack without bound and abort the process. `do` is
        // dispatched from `parse_expr_head`, NOT `parse_atom`, so it never gets
        // the delimiter charge that guards `((((…))))` — without a charge here
        // the budget never accumulates across `do` levels and a pathological
        // stack overflows the native stack instead of diagnosing. Charge the
        // same `DELIMITER_RECURSION_COST` as parens/records: each `do` level's
        // parse spine (parse_do_expr → parse_block → parse_stmt → parse_expr →
        // parse_expr_head → parse_do_expr) is at least as deep as a delimiter
        // cycle, so the limit must trip at the same shallow depth to stay
        // within the smaller stacks worker threads (e.g. the LSP) run on.
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("do expression", |this| {
            this.advance(); // consume `do`

            // `parse_stmt` pushes bind/let names so later statements see
            // them as bound; the whole do-block scope ends here.
            let scope_mark = this.bound_vars.len();
            let stmts = this.parse_block(|p| p.parse_stmt());
            this.bound_vars.truncate(scope_mark);

            let end = this.prev_span();
            Some(Spanned::new(
                ExprKind::Do(stmts),
                Span::new(start.start, end.end),
            ))
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    /// Parse `serve Api where E1 = expr1; E2 = expr2; ...`
    fn parse_serve_expr(&mut self) -> Option<Expr> {
        let start = self.span();
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("serve expression", |this| {
            this.advance(); // consume `serve`
            let api_span = this.span();
            let api = this.parse_route_component_path()?;
            this.skip_newlines();
            this.expect(&TokenKind::Where, "expected 'where' after API name in 'serve'")
                .ok()?;
            let handlers = this.parse_block(|p| p.parse_serve_handler());
            let end = this.prev_span();
            Some(Spanned::new(
                ExprKind::Serve {
                    api,
                    api_span,
                    handlers,
                },
                Span::new(start.start, end.end),
            ))
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    fn parse_serve_handler(&mut self) -> Option<ServeHandler> {
        self.skip_newlines();
        if self.at_eof() {
            return None;
        }
        let (endpoint, endpoint_span) =
            self.expect_upper("expected endpoint constructor name").ok()?;
        self.expect(&TokenKind::Eq, "expected '=' after endpoint name").ok()?;
        let body = self.parse_expr()?;
        Some(ServeHandler {
            endpoint,
            endpoint_span,
            body,
        })
    }

    fn parse_set_with_start(&mut self, replace: bool, start: Span) -> Option<Expr> {
        let ctx = if replace {
            "replace set expression"
        } else {
            "set expression"
        };
        self.in_context(ctx, |this| {
            // The caller has already positioned the parser at the target. A
            // set/replace target is a field-access chain (`*rel`, `x`,
            // `rec.field`, or `db.*rel` on a source-record) — never an
            // application or binary op — so parse it with `parse_postfix`,
            // which handles the `.`-chain including the `*name` source-field
            // form. (`parse_expr_bp` would stop at `db` and leave `.*todos`
            // dangling.)
            let target = this.parse_postfix()?;

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

    /// `with record body` — `record` is an atom (record literal, variable, or
    /// parenthesized expression, optionally with field-access chains); `body`
    /// is the rest of the expression. Every field of the record's type is in
    /// scope as a variable inside `body`; the result is `body`. Field names
    /// are only known once the record's type is inferred, so the parser binds
    /// nothing — it parses the two subexpressions and leaves scoping to
    /// inference (field references parse as ordinary `Var`s).
    fn parse_with_expr(&mut self) -> Option<Expr> {
        let start = self.span();
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.in_context("with expression", |this| {
            this.advance(); // consume `with`
            let record = this.parse_postfix()?;
            // Bind the record's field names for the body so `maybe_time_unit`
            // suppresses unit sugar on collisions (`with {ms: 5} g 2 ms`
            // applies `g` to `2` and `ms`, not `g (2 ms)`).
            let pushed = if let ExprKind::Record(fields) = &record.node {
                for f in fields {
                    this.bound_vars.push(f.name.clone());
                }
                fields.len()
            } else {
                0
            };
            this.skip_newlines();
            let body = this.parse_expr()?;
            this.bound_vars.truncate(this.bound_vars.len() - pushed);
            let end_sp = body.span;
            Some(Spanned::new(
                ExprKind::With {
                    record: Box::new(record),
                    body: Box::new(body),
                },
                Span::new(start.start, end_sp.end),
            ))
        });
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
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

        // Try to parse as a bind: `pat <- expr`
        // Use save/restore: parse pattern, check for `<-`.
        let saved = self.save();
        let diag_count = self.diagnostics.len();

        if let Some(pat) = self.try_parse_pat()
            && self.eat(&TokenKind::LArrow) {
                // Committed to a bind statement — `<-` was consumed.
                // If the expression fails, return None without trying
                // to re-parse as an expression statement.
                let expr = self.parse_expr()?;
                let end_sp = expr.span;
                // Names bound by this bind are in scope for the rest of the
                // do-block (popped by `parse_do_expr`).
                self.push_pat_vars(&pat);
                return Some(Spanned::new(
                    StmtKind::Bind { pat, expr },
                    Span::new(start.start, end_sp.end),
                ));
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
                let TokenKind::Upper(mut name) = tok.kind else { unreachable!() };
                // Qualified constructor: `Type.Ctor`. The leading `Type`
                // segment(s) form the qualifier (resolution/confinement
                // context); `name` is the final ctor tag. Loops to allow
                // deeper qualification (`a.b.Ctor` → qualifier `a.b`).
                let mut qualifier: Option<Name> = None;
                while self.at(&TokenKind::Dot)
                    && matches!(self.peek_ahead(1), TokenKind::Upper(_))
                {
                    self.advance(); // consume `.`
                    let ctor_tok = self.advance();
                    let TokenKind::Upper(seg) = ctor_tok.kind else { unreachable!() };
                    qualifier = Some(match qualifier {
                        None => name.clone(),
                        Some(q) => format!("{q}.{name}"),
                    });
                    name = seg;
                }
                // `Cons head tail` — non-empty relation pattern (reserved name).
                // The built-in form has exactly TWO atom sub-patterns; a single
                // atom after `Cons` (e.g. `Cons {head: h, tail: t}` or `Cons c`)
                // is a user-defined constructor named `Cons` with one record/var
                // payload, so fall through to a normal constructor pattern
                // instead of erroring — otherwise a `data … = … | Cons {…}` type
                // is constructable but impossible to pattern-match.
                if name == "Cons" && qualifier.is_none() && self.can_start_pat_atom() {
                    let head = self.parse_pat_atom()?;
                    if self.can_start_pat_atom() {
                        let tail = self.parse_pat_atom()?;
                        let span = Span::new(start.start, tail.span.end);
                        return Some(Spanned::new(
                            PatKind::Cons {
                                head: Box::new(head),
                                tail: Box::new(tail),
                            },
                            span,
                        ));
                    }
                    let span = Span::new(start.start, head.span.end);
                    return Some(Spanned::new(
                        PatKind::Constructor {
                            name,
                            payload: Box::new(head),
                            qualifier,
                        },
                        span,
                    ));
                }
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
                        qualifier,
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
                // Optional type annotation: `(pat : Type)`. Enables rank-N
                // lambda params like `\(f : (forall a. a -> a)) -> …`.
                if self.eat(&TokenKind::Colon) {
                    let ty = self.parse_type()?;
                    let end_tok = self
                        .expect(&TokenKind::RParen, "expected ')' after pattern type annotation")
                        .ok()?;
                    return Some(Spanned::new(
                        PatKind::Annot {
                            pat: Box::new(inner),
                            ty: Box::new(ty),
                        },
                        Span::new(start.start, end_tok.span.end),
                    ));
                }
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
                | TokenKind::Upper(_)
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
            TokenKind::Upper(_) => {
                // A bare constructor used as a payload: `Just True`, `Ok Nothing`.
                // Treat it as a nullary constructor (empty record payload) — do
                // NOT consume a further payload atom here, so a constructor with
                // arguments still requires parentheses. May be qualified
                // (`Just Color.Red`): capture the qualifier as above.
                let tok = self.advance();
                let TokenKind::Upper(mut name) = tok.kind else { unreachable!() };
                let mut qualifier: Option<Name> = None;
                while self.at(&TokenKind::Dot)
                    && matches!(self.peek_ahead(1), TokenKind::Upper(_))
                {
                    self.advance();
                    let ctor_tok = self.advance();
                    let TokenKind::Upper(seg) = ctor_tok.kind else { unreachable!() };
                    qualifier = Some(match qualifier {
                        None => name.clone(),
                        Some(q) => format!("{q}.{name}"),
                    });
                    name = seg;
                }
                Some(Spanned::new(
                    PatKind::Constructor {
                        name,
                        payload: Box::new(Spanned::new(PatKind::Record(vec![]), tok.span)),
                        qualifier,
                    },
                    tok.span,
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
                // Optional type annotation: `(pat : Type)`. Enables rank-N
                // lambda params like `\(f : (forall a. a -> a)) -> …`.
                if self.eat(&TokenKind::Colon) {
                    let ty = self.parse_type()?;
                    let end_tok = self
                        .expect(&TokenKind::RParen, "expected ')' after pattern type annotation")
                        .ok()?;
                    return Some(Spanned::new(
                        PatKind::Annot {
                            pat: Box::new(inner),
                            ty: Box::new(ty),
                        },
                        Span::new(start.start, end_tok.span.end),
                    ));
                }
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
                if self.at(&TokenKind::RBrace) {
                    break;
                }
                let (fname, fname_span) =
                    self.expect_lower("expected field name in record pattern").ok()?;
                self.skip_newlines();
                // Field pattern value: `name pattern` (whitespace-separated, no
                // `:`). No punning — every field binds an explicit pattern.
                let pattern = Some(self.parse_pat()?);
                fields.push(FieldPat {
                    name: fname,
                    name_span: fname_span,
                    pattern,
                });
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
                self.skip_newlines();
                if self.at(&TokenKind::RBracket) {
                    break; // trailing comma
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
            while let TokenKind::Lower(name) = self.peek().clone() {
                self.advance();
                vars.push(name);
            }
            if vars.is_empty() {
                self.error("expected one or more type variables after 'forall'");
                return None;
            }
            self.expect(&TokenKind::Dot, "expected '.' after forall variables").ok()?;
            self.skip_newlines();
            // Guard the recursive body parse: `forall a. forall a. …` would
            // otherwise recurse unbounded and overflow the stack (every other
            // recursive type path is charged against MAX_RECURSION_DEPTH).
            if !self.enter_recursion() {
                return None;
            }
            let body = self.parse_type();
            self.recursion_depth -= 1;
            let body = body?;
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
            if !self.enter_recursion() { self.restore(saved); return None; }
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
            // The predicate re-enters the expression grammar, which can loop
            // back here via a postfix `: Type` annotation. Charge the recursion
            // budget across the predicate so deeply chained `where` clauses trip
            // MAX_RECURSION_DEPTH instead of overflowing the native stack.
            if !self.enter_recursion() {
                return None;
            }
            let predicate = self.parse_expr();
            self.recursion_depth -= 1;
            let predicate = predicate?;
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

        // The type-application chain (`T a b c …`) is built iteratively into a
        // left-spine, so — like the expression application loop — it must charge
        // the depth budget per node and hold it until return, otherwise a
        // pathological chain produces an AST whose first recursive traversal
        // overflows the stack. See `parse_application` for the full rationale.
        let mut spine_charged = 0usize;

        macro_rules! fail {
            () => {{
                self.recursion_depth -= spine_charged;
                return None;
            }};
        }

        loop {
            if self.can_start_type_atom() {
                let arg = match self.parse_type_atom() {
                    Some(arg) => arg,
                    None => fail!(),
                };
                if !self.enter_recursion() { fail!() }
                spine_charged += 1;
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
            // Don't absorb what is really the start of a new declaration: a
            // lowercase identifier immediately followed by `=` or `:`. At top
            // level `block_indent` is 0, so any indented line would otherwise
            // extend the type application and silently swallow the next decl.
            let next_starts_decl = matches!(self.peek(), TokenKind::Lower(_))
                && matches!(self.peek_ahead(1), TokenKind::Eq | TokenKind::Colon);
            // While parsing a record VALUE literal's `name : Type` sig line, a
            // lowercase token on the next line is the field's value
            // (`name value`), never a type argument — stop regardless of what
            // follows it.
            let next_is_value_field = self.record_value_sig_type
                && matches!(self.peek(), TokenKind::Lower(_));
            if !self.at_eof()
                && !next_starts_decl
                && !next_is_value_field
                && self.cur_column() > self.block_indent
                && self.can_start_type_atom()
            {
                let arg = match self.parse_type_atom() {
                    Some(arg) => arg,
                    None => fail!(),
                };
                if !self.enter_recursion() { fail!() }
                spine_charged += 1;
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
        self.recursion_depth -= spine_charged;
        Some(func)
    }

    fn can_start_type_atom(&self) -> bool {
        if self.stop_type_at_headers
            && matches!(self.peek(), TokenKind::Lower(s) if s == "headers" || s == "rateLimit") {
                return false;
            }
        if self.stop_type_at_migrate_clauses
            && matches!(self.peek(), TokenKind::Lower(s) if s == "to" || s == "using") {
                return false;
            }
        matches!(
            self.peek(),
            TokenKind::Upper(_)
                | TokenKind::Lower(_)
                | TokenKind::Underscore
                | TokenKind::LBrace
                | TokenKind::LBracket
                | TokenKind::LParen
        )
    }

    fn parse_type_atom(&mut self) -> Option<Type> {
        // Guard recursion here: every type-side delimiter cycle (parens,
        // record types, relation types, variant types → parse_type → ... →
        // parse_type_atom) flows through this entry point, preventing stack
        // overflow on pathological input like `[[[[…]]]]`.
        if !self.enter_recursion_cost(DELIMITER_RECURSION_COST) {
            return None;
        }
        let result = self.parse_type_atom_inner();
        self.recursion_depth -= DELIMITER_RECURSION_COST;
        result
    }

    fn parse_type_atom_inner(&mut self) -> Option<Type> {
        let start = self.span();
        match self.peek() {
            TokenKind::Upper(_) => {
                let tok = self.advance();
                let TokenKind::Upper(name) = tok.kind else { unreachable!() };
                if name == "IO" && matches!(self.peek(), TokenKind::LBrace) {
                    // Parse `IO {effects} Type`, `IO {effects | r} Type`,
                    // `IO {| r} Type`, or `IO {effects | r1 \/ r2} Type`.
                    // After `|`, accept one row-variable name (or `_`),
                    // optionally chained with `\/` to form a row union.
                    self.advance(); // consume '{'
                    self.skip_newlines();
                    let effects = if matches!(self.peek(), TokenKind::RBrace | TokenKind::Pipe) {
                        Vec::new()
                    } else {
                        self.try_parse_effects().unwrap_or_default()
                    };
                    self.skip_newlines();
                    let rest: Vec<Name> = if self.eat(&TokenKind::Pipe) {
                        self.parse_effect_row_tail()
                    } else {
                        Vec::new()
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
                    // Also support `IO r1 \/ r2 Type` as shorthand for
                    // `IO {| r1 \/ r2} Type`.
                    let row_tok = self.advance();
                    let first_name = match row_tok.kind {
                        TokenKind::Lower(n) => n,
                        TokenKind::Underscore => "_".to_string(),
                        _ => unreachable!(),
                    };
                    let mut rest: Vec<Name> = vec![first_name];
                    while self.eat(&TokenKind::BackslashSlash) {
                        self.skip_newlines();
                        match self.peek() {
                            TokenKind::Lower(_) => {
                                let tok = self.advance();
                                let TokenKind::Lower(n) = tok.kind else { unreachable!() };
                                rest.push(n);
                            }
                            TokenKind::Underscore => {
                                self.advance();
                                rest.push("_".to_string());
                            }
                            _ => {
                                self.error("expected effect row variable name or '_' after '\\/'");
                                break;
                            }
                        }
                    }
                    if !self.enter_recursion() { return None; }
                    let inner = self.parse_type_atom();
                    self.recursion_depth -= 1;
                    let inner = inner?;
                    let span = Span::new(tok.span.start, inner.span.end);
                    Some(Spanned::new(
                        TypeKind::IO {
                            effects: Vec::new(),
                            rest,
                            ty: Box::new(inner),
                        },
                        span,
                    ))
                } else if (name == "Float" || name == "Int")
                    && self.can_start_unit_type_arg()
                {
                    // `Float M`, `Float u`, `Float (M / S^2)` — the unit is a
                    // regular type-argument position parsed as a unit
                    // expression. A bare Upper/Lower identifier is a unit
                    // (`M`, `u`); a parenthesized form carries the algebraic
                    // operators `* / ^`. A `(` could also start a parenthesized
                    // type (`Float (Int -> Text)` is application, not a unit),
                    // so save/restore on failure.
                    let saved = self.save();
                    let diag_count = self.diagnostics.len();
                    let unit = self.parse_unit_type_arg();
                    if let Some(unit) = unit {
                        let span = Span::new(tok.span.start, self.prev_span().end);
                        let base = Box::new(Spanned::new(TypeKind::Named(name), tok.span));
                        return Some(Spanned::new(TypeKind::UnitAnnotated { base, unit }, span));
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

        // Check for effectful type: {r *rel, w *rel, ...} Type
        // Effects have special keyword-like identifiers.
        let saved = self.save();
        let diag_count = self.diagnostics.len();
        if let Some(effects) = self.try_parse_effects() {
            let close = self
                .expect(&TokenKind::RBrace, "expected '}' to close effect set")
                .ok()?;
            // If a type atom follows, parse it as the effectful body (e.g.
            // `{console} Int`). Otherwise `{effects}` is terminal — before a
            // closing paren, `->`, end of type, or newline — so treat it as a
            // complete effectful type with an empty (Unit) body. This is the
            // form written in `Server Api {console}` type annotations.
            let ty = if self.can_start_type_atom() {
                self.parse_type()?
            } else {
                Spanned::new(
                    TypeKind::Record {
                        fields: vec![],
                        rest: None,
                    },
                    close.span,
                )
            };
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
        //
        // Fields are separated by a comma OR by a newline — so a record type may
        // be written one field per line without commas:
        //   {name: Text
        //    age: Int 1}
        // A same-line field still requires a comma (`{name: Text, age: Int 1}`).
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

            // Field separator: an explicit comma, or a newline followed by
            // another `field:` signature. Otherwise the record is done.
            if self.eat(&TokenKind::Comma) {
                continue;
            }
            if self.at(&TokenKind::Newline) {
                let saved = self.save();
                self.skip_newlines();
                if self.at_field_signature() {
                    continue;
                }
                self.restore(saved);
            }
            break;
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

    /// Parse the row-variable tail of an IO type after `|`:
    /// `r1`, `_`, or `r1 \/ r2 \/ r3`. Returns an empty Vec on parse error.
    fn parse_effect_row_tail(&mut self) -> Vec<Name> {
        let mut rest: Vec<Name> = Vec::new();
        self.skip_newlines();
        match self.peek() {
            TokenKind::Lower(_) => {
                let tok = self.advance();
                let TokenKind::Lower(n) = tok.kind else { unreachable!() };
                rest.push(n);
            }
            TokenKind::Underscore => {
                self.advance();
                rest.push("_".to_string());
            }
            _ => {
                self.error("expected effect row variable name or '_' after '|'");
                return rest;
            }
        }
        while self.eat(&TokenKind::BackslashSlash) {
            self.skip_newlines();
            match self.peek() {
                TokenKind::Lower(_) => {
                    let tok = self.advance();
                    let TokenKind::Lower(n) = tok.kind else { unreachable!() };
                    rest.push(n);
                }
                TokenKind::Underscore => {
                    self.advance();
                    rest.push("_".to_string());
                }
                _ => {
                    self.error("expected effect row variable name or '_' after '\\/'");
                    break;
                }
            }
        }
        rest
    }

    /// Parse the relation name in an effect row after `r`/`w`/`rw`. Accepts a
    /// single `StarIdent` token (`r *name`) or the legacy `Star` + `Lower`
    /// form. Returns the bare relation name (no `*`).
    fn parse_effect_relation_name(&mut self, kw: &str) -> Option<Name> {
        match self.peek() {
            TokenKind::StarIdent(_) => {
                let tok = self.advance();
                let TokenKind::StarIdent(n) = tok.kind else { unreachable!() };
                Some(n.trim_start_matches('*').to_string())
            }
            TokenKind::Star => {
                if self.expect(&TokenKind::Star, "expected '*' after effect keyword").is_err() {
                    return None;
                }
                let (n, _) = self
                    .expect_lower(&format!("expected relation name after '{} *'", kw))
                    .ok()?;
                Some(n)
            }
            _ => {
                self.error(&format!("expected '*' after '{}'", kw));
                None
            }
        }
    }

    fn try_parse_effects(&mut self) -> Option<Vec<Effect>> {
        let mut effects = Vec::new();
        loop {
            match self.peek() {
                // For the `r`/`w`/`rw` forms, once the keyword is consumed we are
                // committed. If the following `*`/name is malformed, `expect`
                // already emits a diagnostic — `break` out (keeping any effects
                // parsed so far) rather than `?`-propagating `None`, which would
                // discard the partial parse AND leave the caller unable to tell a
                // parse error from a legitimately empty effect set, desyncing the
                // surrounding type-row parse.
                TokenKind::Lower(s) if s == "r" => {
                    self.advance();
                    match self.parse_effect_relation_name("r") {
                        Some(name) => effects.push(Effect::Reads(name)),
                        None => break,
                    }
                }
                TokenKind::Lower(s) if s == "w" => {
                    self.advance();
                    match self.parse_effect_relation_name("w") {
                        Some(name) => effects.push(Effect::Writes(name)),
                        None => break,
                    }
                }
                TokenKind::Lower(s) if s == "rw" => {
                    self.advance();
                    match self.parse_effect_relation_name("rw") {
                        Some(name) => {
                            effects.push(Effect::Reads(name.clone()));
                            effects.push(Effect::Writes(name));
                        }
                        None => break,
                    }
                }
                // Bare effect keywords must not be a record field name: if the
                // next token is `:`, this is `{console: Type}` (a record), not an
                // effect set, so we bail and let `parse_record_type` fall back.
                TokenKind::Lower(s)
                    if s == "console" && self.peek_ahead(1) != &TokenKind::Colon =>
                {
                    self.advance();
                    effects.push(Effect::Console);
                }
                TokenKind::Lower(s)
                    if s == "network" && self.peek_ahead(1) != &TokenKind::Colon =>
                {
                    self.advance();
                    effects.push(Effect::Network);
                }
                TokenKind::Lower(s) if s == "fs" && self.peek_ahead(1) != &TokenKind::Colon => {
                    self.advance();
                    effects.push(Effect::Fs);
                }
                TokenKind::Lower(s) if s == "clock" && self.peek_ahead(1) != &TokenKind::Colon => {
                    self.advance();
                    effects.push(Effect::Clock);
                }
                TokenKind::Lower(s) if s == "random" && self.peek_ahead(1) != &TokenKind::Colon => {
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
            DeclKind::Source { name, ty } => {
                assert_eq!(name, "people");
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
        // r = {name "Alice" age 30}
        let source = r#"r = {name "Alice" age 30}"#.to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("r".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::LBrace, 4, 5),
            (TokenKind::Lower("name".into()), 5, 9),
            (TokenKind::Text("Alice".into()), 10, 17),
            (TokenKind::Lower("age".into()), 18, 21),
            (TokenKind::Int("30".into()), 22, 24),
            (TokenKind::RBrace, 24, 25),
            (TokenKind::Eof, 25, 25),
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
        // f = {t | age 30}
        let source = "f = {t | age 30}".to_string();
        let tokens = toks(vec![
            (TokenKind::Lower("f".into()), 0, 1),
            (TokenKind::Eq, 2, 3),
            (TokenKind::LBrace, 4, 5),
            (TokenKind::Lower("t".into()), 5, 6),
            (TokenKind::Pipe, 7, 8),
            (TokenKind::Lower("age".into()), 9, 12),
            (TokenKind::Int("30".into()), 13, 15),
            (TokenKind::RBrace, 15, 16),
            (TokenKind::Eof, 16, 16),
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
