//! AST-based pretty printer for Knot source.
//!
//! [`format_module`] is the single public entry point used by both `knot fmt`
//! and the language server's formatting handler. It walks the parsed
//! [`Module`] in source order, emits each declaration with consistent
//! indentation and layout, and re-inserts standalone comments that the lexer
//! discarded by scanning the original source between declaration spans.
//!
//! Declarations whose source span contains an inline comment are emitted as a
//! whitespace-normalized copy of the original text — the AST printer can't
//! place inline comments without losing them, and a verbatim copy is the
//! safest fallback.

use crate::ast::*;

const INDENT: usize = 2;
const TARGET_WIDTH: usize = 100;

// ── Public entry point ─────────────────────────────────────────────

pub fn format_module(source: &str, module: &Module) -> String {
    let out = format_module_inner(source, module);
    if out == source {
        return out;
    }
    // Safety net: the formatted output must reparse cleanly to the same AST
    // (modulo spans). If it doesn't — a formatter bug, e.g. layout-sensitive
    // indentation drift — return the original source unchanged rather than
    // ever producing output that no longer parses or means something else.
    // All callers (`knot fmt`, the LSP) go through this entry point.
    if reparses_to_same_ast(module, &out) {
        out
    } else {
        source.to_string()
    }
}

/// Parse `formatted` and check it produces the same AST as `module`,
/// ignoring source spans. Any lex/parse error counts as a mismatch.
fn reparses_to_same_ast(module: &Module, formatted: &str) -> bool {
    let (tokens, lex_diags) = crate::lexer::Lexer::new(formatted).tokenize();
    if lex_diags
        .iter()
        .any(|d| d.severity == crate::diagnostic::Severity::Error)
    {
        return false;
    }
    let parser = crate::parser::Parser::new(formatted.to_string(), tokens);
    let (reparsed, parse_diags) = parser.parse_module();
    if parse_diags
        .iter()
        .any(|d| d.severity == crate::diagnostic::Severity::Error)
    {
        return false;
    }
    strip_spans(&format!("{:?}", module)) == strip_spans(&format!("{:?}", reparsed))
}

/// Remove every `span: Span { ... }` payload from a `Debug` rendering so AST
/// comparison tolerates the byte-position drift formatting introduces.
///
/// Matches the *full* `Span { start: N, end: N }` shape (not a bare `Span {`
/// prefix + seek-to-`}`) and tracks whether we're inside a string literal's
/// Debug rendering. A user string whose contents happen to spell
/// `Span { start: N, end: N }` must NOT be scrubbed — otherwise edits
/// confined to those digits would compare equal, a false negative that
/// silently makes the formatter fall back to the original source. Derived
/// `Debug` always quotes string contents and escapes inner quotes as `\"`,
/// so a simple in-string flag (with backslash escape handling) separates
/// real span markers (rendered outside quotes) from look-alike string
/// contents. Mirrors `incremental.rs::strip_spans`.
fn strip_spans(debug: &str) -> String {
    let bytes = debug.as_bytes();
    let mut out = String::with_capacity(debug.len());
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        if in_string {
            // Inside a string: copy verbatim, honoring `\`-escapes (so an
            // escaped `\"` does not prematurely close the string), and exit
            // on the closing unescaped quote.
            let step = utf8_char_len(bytes[i]);
            let end = (i + step).min(bytes.len());
            out.push_str(&debug[i..end]);
            if bytes[i] == b'\\' {
                // Copy the escaped char too, so it can't be misread as a quote.
                if end < bytes.len() {
                    let step2 = utf8_char_len(bytes[end]);
                    let end2 = (end + step2).min(bytes.len());
                    out.push_str(&debug[end..end2]);
                    i = end2;
                    continue;
                }
            } else if bytes[i] == b'"' {
                in_string = false;
            }
            i = end;
            continue;
        }
        if let Some(len) = span_marker_len(&bytes[i..]) {
            i += len;
            // Swallow a following `, ` separator if present.
            if debug[i..].starts_with(", ") {
                i += 2;
            }
            continue;
        }
        // Copy one whole UTF-8 character verbatim. `i` is always on a char
        // boundary: it starts at 0 and advances either by a full char width
        // here or past an all-ASCII span marker above.
        let step = utf8_char_len(bytes[i]);
        let end = (i + step).min(bytes.len());
        out.push_str(&debug[i..end]);
        if bytes[i] == b'"' {
            in_string = true;
        }
        i = end;
    }
    out
}

/// If `b` begins with a complete derived-`Debug` span marker
/// (`Span { start: <digits>, end: <digits> }`), return its byte length;
/// otherwise `None`. Pure ASCII, so the returned length keeps `i` on a
/// UTF-8 char boundary.
fn span_marker_len(b: &[u8]) -> Option<usize> {
    let lit_at = |i: usize, lit: &[u8]| -> Option<usize> {
        if b.len() >= i + lit.len() && b[i..i + lit.len()] == *lit {
            Some(i + lit.len())
        } else {
            None
        }
    };
    let digits_at = |i: usize| -> Option<usize> {
        let mut j = i;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j > i { Some(j) } else { None }
    };
    let i = lit_at(0, b"Span { start: ".as_slice())?;
    let i = digits_at(i)?;
    let i = lit_at(i, b", end: ".as_slice())?;
    let i = digits_at(i)?;
    lit_at(i, b" }".as_slice())
}

/// Byte width of the UTF-8 character beginning with `first`.
fn utf8_char_len(first: u8) -> usize {
    match first {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1, // stray continuation byte: advance one to stay live
    }
}

fn format_module_inner(source: &str, module: &Module) -> String {
    let comments = collect_comments(source);

    enum Block<'a> {
        Import(&'a Import),
        Decl(&'a Decl),
    }
    impl<'a> Block<'a> {
        fn span(&self) -> Span {
            match self {
                Block::Import(i) => i.span,
                Block::Decl(d) => d.span,
            }
        }
    }

    let mut blocks: Vec<Block> = Vec::new();
    for imp in &module.imports {
        blocks.push(Block::Import(imp));
    }
    for d in &module.decls {
        blocks.push(Block::Decl(d));
    }
    blocks.sort_by_key(|b| b.span().start);

    let mut out = String::new();
    let mut prev_end: usize = 0;

    // Leading section: comments at the top of the file before the first block.
    let first_start = blocks
        .first()
        .map(|b| b.span().start)
        .unwrap_or(source.len());
    // An exported declaration's `export` keyword sits in the gap before the
    // decl's span, so a comment on the `export` line (`export -- keep me`) is
    // non-standalone yet must still be emitted above the decl. Include such
    // comments alongside the standalone ones and emit them in source order, so
    // they are neither dropped nor reordered relative to the standalone
    // comments in the same gap.
    let first_exported = matches!(blocks.first(), Some(Block::Decl(d)) if d.exported);
    let mut leading_comments: Vec<&Comment> = comments
        .iter()
        .filter(|c| (c.standalone || first_exported) && c.span.end <= first_start)
        .collect();
    leading_comments.sort_by_key(|c| c.span.start);
    if !leading_comments.is_empty() {
        for (j, c) in leading_comments.iter().enumerate() {
            if j > 0 {
                out.push('\n');
                if has_blank_lines_between(
                    source,
                    leading_comments[j - 1].span.end,
                    c.span.start,
                ) {
                    out.push('\n');
                }
            }
            out.push_str(c.text);
        }
        out.push('\n');
        if !blocks.is_empty()
            && has_blank_lines_between(
                source,
                leading_comments.last().unwrap().span.end,
                first_start,
            )
        {
            out.push('\n');
        }
        prev_end = leading_comments.last().unwrap().span.end;
    }

    for (i, block) in blocks.iter().enumerate() {
        let span = block.span();
        let block_start = span.start;
        let block_visible_end = visible_end(source, span);

        if i > 0 {
            // Always start a fresh line for the next block.
            out.push('\n');

            // Comments between the previous block and this one, in source
            // order. Standalone comments always qualify; when this block is an
            // exported declaration, a comment on its `export` line is
            // non-standalone but belongs here too (see the leading section).
            // The previous block's own trailing comment (on `prev_line`) is
            // emitted with that block, so exclude it.
            let this_exported = matches!(block, Block::Decl(d) if d.exported);
            let prev_line = line_of(source, prev_end);
            let mut between: Vec<&Comment> = comments
                .iter()
                .filter(|c| {
                    (c.standalone || (this_exported && c.line != prev_line))
                        && c.span.start >= prev_end
                        && c.span.end <= block_start
                })
                .collect();
            between.sort_by_key(|c| c.span.start);

            if !between.is_empty() {
                if has_blank_lines_between(source, prev_end, between[0].span.start) {
                    out.push('\n');
                }
                for (j, c) in between.iter().enumerate() {
                    if j > 0 {
                        out.push('\n');
                        if has_blank_lines_between(
                            source,
                            between[j - 1].span.end,
                            c.span.start,
                        ) {
                            out.push('\n');
                        }
                    }
                    out.push_str(c.text);
                }
                out.push('\n');
                if has_blank_lines_between(
                    source,
                    between.last().unwrap().span.end,
                    block_start,
                ) {
                    out.push('\n');
                }
            } else if has_blank_lines_between(source, prev_end, block_start) {
                out.push('\n');
            }
        }

        let rendered = match block {
            Block::Import(i) => render_import(i),
            Block::Decl(d) => render_decl_with_fallback(source, d, &comments),
        };
        out.push_str(rendered.trim_end());

        // Trailing line comment on the same line as the last visible token.
        if let Some(c) = trailing_line_comment(source, &comments, block_visible_end) {
            out.push(' ');
            out.push_str(c.text);
        }

        prev_end = block_visible_end;
    }

    // Trailer: standalone comments after the last block.
    let trailing: Vec<&Comment> = comments
        .iter()
        .filter(|c| c.standalone && c.span.start >= prev_end)
        .collect();
    if !trailing.is_empty() {
        out.push('\n');
        if has_blank_lines_between(source, prev_end, trailing[0].span.start) {
            out.push('\n');
        }
        for (j, c) in trailing.iter().enumerate() {
            if j > 0 {
                out.push('\n');
                if has_blank_lines_between(source, trailing[j - 1].span.end, c.span.start) {
                    out.push('\n');
                }
            }
            out.push_str(c.text);
        }
    }

    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

fn visible_end(source: &str, span: Span) -> usize {
    let slice = &source[span.start..span.end.min(source.len())];
    span.start + slice.trim_end().len()
}

fn has_blank_lines_between(source: &str, from: usize, to: usize) -> bool {
    if from >= to || to > source.len() {
        return false;
    }
    // Count line-break events, treating `\n`, a lone `\r`, and `\r\n` each as a
    // single break — mirroring `line_of`/`diagnostic::line_col`. Counting only
    // `\n` would miss a blank line in a lone-CR (classic-Mac) file, where a
    // blank line is `\r\r` and contains no `\n`, silently collapsing it.
    let bytes = source.as_bytes();
    let mut breaks = 0usize;
    let mut i = from;
    while i < to {
        match bytes[i] {
            b'\n' => {
                breaks += 1;
                i += 1;
            }
            b'\r' => {
                breaks += 1;
                i += if bytes.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
            }
            _ => i += 1,
        }
    }
    breaks >= 2
}

// ── Comment extraction ─────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Comment<'a> {
    /// Byte span covering the `--` through end-of-line (excluding the newline).
    span: Span,
    /// Original text including the leading `--`.
    text: &'a str,
    /// Line in the original source (0-indexed).
    line: usize,
    /// `true` if the line contains only whitespace before the `--`.
    standalone: bool,
}

fn collect_comments(source: &str) -> Vec<Comment<'_>> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 0usize;
    let mut line_start = 0usize;
    let mut in_text = false;
    let mut in_bytes = false;

    // Treat `\r` (lone CR) and `\r\n` as a single line break, matching
    // `diagnostic::line_col` and the lexer. Counting only `\n` here would
    // report every comment in a `\r`-only file as line 0 and misplace it.
    // Returns the byte advance for a line break, or `None` for non-breaks.
    let line_break_adv = |b: u8, next: Option<u8>| -> Option<usize> {
        match b {
            b'\n' => Some(1),
            b'\r' => Some(if next == Some(b'\n') { 2 } else { 1 }),
            _ => None,
        }
    };

    while i < bytes.len() {
        let b = bytes[i];

        if in_text {
            if b == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_text = false;
            } else if let Some(adv) = line_break_adv(b, bytes.get(i + 1).copied()) {
                line += 1;
                line_start = i + adv;
            }
            i += 1;
            continue;
        }
        if in_bytes {
            if b == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_bytes = false;
            } else if let Some(adv) = line_break_adv(b, bytes.get(i + 1).copied()) {
                line += 1;
                line_start = i + adv;
            }
            i += 1;
            continue;
        }

        if b == b'"' {
            in_text = true;
            i += 1;
            continue;
        }
        if b == b'b'
            && i + 1 < bytes.len()
            && bytes[i + 1] == b'"'
            // Only a `b` that *begins* a token starts a byte-string literal.
            // A `b` preceded by an identifier character is the tail of an
            // identifier (e.g. `foob"…"`), not a `b"…"` prefix — treating it
            // as a byte string would swallow a following `--` comment.
            && (i == 0 || {
                let p = bytes[i - 1];
                !(p.is_ascii_alphanumeric() || p == b'_' || p == b'\'')
            })
        {
            in_bytes = true;
            i += 2;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Comment to end of line.
            let standalone = source[line_start..i].chars().all(|c| c == ' ' || c == '\t');
            let comment_start = i;
            while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                i += 1;
            }
            let span = Span::new(comment_start, i);
            let text = &source[comment_start..i];
            out.push(Comment {
                span,
                text: text.trim_end(),
                line,
                standalone,
            });
            continue;
        }
        if let Some(adv) = line_break_adv(b, bytes.get(i + 1).copied()) {
            line += 1;
            i += adv;
            line_start = i;
            continue;
        }
        i += 1;
    }
    out
}

fn line_of(source: &str, byte: usize) -> usize {
    // Count both `\n` and lone `\r` as line breaks (treat `\r\n` as one),
    // mirroring `diagnostic::line_col` so comment placement is consistent
    // across line-ending styles.
    let bytes = source.as_bytes();
    let end = byte.min(source.len());
    let mut line = 0usize;
    let mut i = 0usize;
    while i < end {
        let b = bytes[i];
        if b == b'\n' {
            line += 1;
            i += 1;
        } else if b == b'\r' {
            line += 1;
            i += if bytes.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
        } else {
            i += 1;
        }
    }
    line
}

fn trailing_line_comment<'a>(
    source: &str,
    comments: &'a [Comment<'a>],
    after: usize,
) -> Option<&'a Comment<'a>> {
    // A comment is "trailing" if it's on the same line as `after` and not standalone.
    let line = line_of(source, after);
    comments
        .iter()
        .find(|c| c.line == line && !c.standalone && c.span.start >= after)
}


// ── Imports ─────────────────────────────────────────────────────────

fn render_import(i: &Import) -> String {
    let mut s = String::from("import ");
    s.push_str(&i.path);
    if let Some(items) = &i.items {
        s.push_str(" (");
        for (idx, it) in items.iter().enumerate() {
            if idx > 0 {
                s.push_str(", ");
            }
            s.push_str(&it.name);
        }
        s.push(')');
    }
    s
}

// ── Decl entry point with comment-preservation fallback ───────────

fn render_decl_with_fallback(source: &str, d: &Decl, comments: &[Comment<'_>]) -> String {
    let has_internal = comments
        .iter()
        .any(|c| c.span.start > d.span.start && c.span.end <= d.span.end);
    if has_internal {
        // `d.span` starts at the declaration keyword, not at a preceding
        // `export`, so the prefix must be re-attached to the verbatim copy.
        let verbatim = normalize_source_slice(&source[d.span.start..d.span.end]);
        if d.exported {
            return format!("export {}", verbatim);
        }
        return verbatim;
    }
    let mut p = Printer::new();
    render_decl(&mut p, d);
    p.finish()
}

/// Verbatim source with tabs → 1 space and trailing whitespace trimmed.
/// The parser counts a tab as ONE column (`column_of` uses `chars().count()`),
/// so a tab must be replaced by exactly one space — anything wider can change
/// the relative indentation of mixed tab/space sibling lines inside a layout
/// block, altering block structure on reparse.
///
/// Tab replacement is string-aware: tabs inside `"…"` string literals are left
/// untouched so that a raw tab in a string value survives the round-trip
/// (replacing it would change the string's value → reparse mismatch → the
/// whole-file fallback silently reverts all formatting). Escaped quotes (`\"`)
/// and other backslash escapes are handled so a `\"` inside a string does not
/// terminate the string-tracking state.
///
/// String tracking also skips `--` comments, mirroring the lexer: a lone `"` in
/// a comment must not flip the state, or every tab after it would be preserved
/// as if it were inside a string literal.
fn normalize_source_slice(s: &str) -> String {
    // Replace tabs outside of string literals with a single space, preserving
    // tabs that appear inside `"…"` string literals verbatim.
    let mut tab_normalized = String::with_capacity(s.len());
    let mut in_string = false;
    let mut in_comment = false;
    let mut prev_backslash = false;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_string {
            tab_normalized.push(ch);
            if prev_backslash {
                // Any character following a backslash is consumed as part of
                // the escape — it cannot end the string.
                prev_backslash = false;
            } else if ch == '\\' {
                prev_backslash = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if in_comment {
            // The lexer ends a line comment at `\n` or `\r`.
            if ch == '\n' || ch == '\r' {
                in_comment = false;
            }
            tab_normalized.push(if ch == '\t' { ' ' } else { ch });
        } else if ch == '-' && chars.peek() == Some(&'-') {
            in_comment = true;
            tab_normalized.push(ch);
        } else if ch == '"' {
            in_string = true;
            tab_normalized.push(ch);
        } else if ch == '\t' {
            tab_normalized.push(' ');
        } else {
            tab_normalized.push(ch);
        }
    }
    let s = tab_normalized;
    let mut out = String::with_capacity(s.len());
    for (i, line) in s.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(line.trim_end());
    }
    out
}

// ── Printer ────────────────────────────────────────────────────────

struct Printer {
    out: String,
    indent: usize,
    at_line_start: bool,
}

impl Printer {
    fn new() -> Self {
        Self {
            out: String::new(),
            indent: 0,
            at_line_start: true,
        }
    }

    fn finish(self) -> String {
        self.out
    }

    fn write(&mut self, s: &str) {
        if s.is_empty() {
            return;
        }
        if self.at_line_start {
            for _ in 0..self.indent {
                self.out.push(' ');
            }
            self.at_line_start = false;
        }
        self.out.push_str(s);
    }

    fn newline(&mut self) {
        // Avoid trailing whitespace from an unfinished line.
        while self.out.ends_with(' ') {
            self.out.pop();
        }
        self.out.push('\n');
        self.at_line_start = true;
    }

    fn with_indent<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        self.indent += INDENT;
        let r = f(self);
        self.indent -= INDENT;
        r
    }

    fn current_col(&self) -> usize {
        if self.at_line_start {
            self.indent
        } else {
            // Column = chars since last newline (Unicode-aware — the parser
            // counts columns in scalar values, so the formatter must match).
            match self.out.rfind('\n') {
                Some(i) => self.out[i + 1..].chars().count(),
                None => self.out.chars().count(),
            }
        }
    }
}

// ── Declarations ───────────────────────────────────────────────────

fn render_decl(p: &mut Printer, d: &Decl) {
    // The parser accepts `export` before any declaration form and records it
    // on the `Decl` (the decl's span starts *after* the keyword), so the
    // prefix must be re-emitted here for every declaration kind.
    if d.exported {
        p.write("export ");
    }
    match &d.node {
        DeclKind::Data { name, params, constructors, deriving } => {
            render_data(p, name, params, constructors, deriving);
        }
        DeclKind::TypeAlias { name, params, ty } => {
            render_type_alias(p, name, params, ty);
        }
        DeclKind::Source { name, ty } => {
            render_source(p, name, ty);
        }
        DeclKind::View { name, ty, body } => {
            render_view(p, name, ty.as_ref(), body);
        }
        DeclKind::Derived { name, ty, body } => {
            render_derived(p, name, ty.as_ref(), body);
        }
        DeclKind::Fun { name, ty, body } => {
            render_fun(p, name, ty.as_ref(), body.as_ref());
        }
        DeclKind::Trait { name, params, supertraits, items } => {
            render_trait(p, name, params, supertraits, items);
        }
        DeclKind::Impl { trait_name, args, constraints, items } => {
            render_impl(p, trait_name, args, constraints, items);
        }
        DeclKind::Route { name, entries } => {
            render_route(p, name, entries);
        }
        DeclKind::RouteComposite { name, components } => {
            p.write("route ");
            p.write(name);
            p.write(" = ");
            for (i, c) in components.iter().enumerate() {
                if i > 0 {
                    p.write(" | ");
                }
                p.write(c);
            }
        }
        DeclKind::Migrate { relation, from_ty, to_ty, using_fn } => {
            // Always use the multi-line layout: on a single line,
            // `parse_type_app` would greedily consume the `to`/`using` clause
            // keywords as type-variable applications. With each clause on its
            // own line at one indent, the parser's migrate `block_indent`
            // guard stops type continuation at the sibling clause keywords.
            p.write("migrate *");
            p.write(relation);
            p.newline();
            p.with_indent(|p| {
                p.write("from ");
                p.write(&render_type(from_ty));
                p.newline();
                p.write("to ");
                p.write(&render_type(to_ty));
                p.newline();
                p.write("using ");
                render_expr(p, using_fn, Prec::Lowest);
            });
        }
        DeclKind::SubsetConstraint { sub, sup } => {
            render_relpath(p, sub);
            p.write(" <= ");
            render_relpath(p, sup);
        }
        DeclKind::UnitDecl { name, definition } => {
            p.write("unit ");
            p.write(name);
            if let Some(def) = definition {
                p.write(" = ");
                p.write(&render_unit_expr(def));
            }
        }
    }
}

fn render_relpath(p: &mut Printer, r: &RelationPath) {
    p.write("*");
    p.write(&r.relation);
    if let Some(f) = &r.field {
        p.write(".");
        p.write(f);
    }
}

fn render_data(
    p: &mut Printer,
    name: &str,
    params: &[Name],
    constructors: &[ConstructorDef],
    deriving: &[Name],
) {
    p.write("data ");
    p.write(name);
    for prm in params {
        p.write(" ");
        p.write(prm);
    }

    // Single short constructor → single line.
    let single_line = constructors.len() == 1 && {
        let c = &constructors[0];
        let body = render_constructor(c);
        let pre_len = p.current_col() + " = ".len() + body.len();
        let total = pre_len + deriving_suffix_len(deriving);
        total <= TARGET_WIDTH
    };

    let multi_line_short = constructors.len() > 1 && {
        let mut total = p.current_col() + " = ".len();
        for (i, c) in constructors.iter().enumerate() {
            if i > 0 {
                total += " | ".len();
            }
            total += render_constructor(c).len();
        }
        total + deriving_suffix_len(deriving) <= TARGET_WIDTH
    };

    if single_line {
        p.write(" = ");
        p.write(&render_constructor(&constructors[0]));
    } else if multi_line_short {
        p.write(" = ");
        for (i, c) in constructors.iter().enumerate() {
            if i > 0 {
                p.write(" | ");
            }
            p.write(&render_constructor(c));
        }
    } else {
        p.newline();
        p.with_indent(|p| {
            for (i, c) in constructors.iter().enumerate() {
                let lead = if i == 0 { "= " } else { "| " };
                p.write(lead);
                p.write(&render_constructor(c));
                if i + 1 < constructors.len() {
                    p.newline();
                }
            }
        });
    }

    if !deriving.is_empty() {
        p.write(" deriving (");
        for (i, n) in deriving.iter().enumerate() {
            if i > 0 {
                p.write(", ");
            }
            p.write(n);
        }
        p.write(")");
    }
}

fn deriving_suffix_len(deriving: &[Name]) -> usize {
    if deriving.is_empty() {
        0
    } else {
        // " deriving (A, B, C)"
        let inner: usize = deriving.iter().map(|n| n.len()).sum::<usize>()
            + (deriving.len().saturating_sub(1) * 2);
        " deriving (".len() + inner + ")".len()
    }
}

fn render_constructor(c: &ConstructorDef) -> String {
    let mut s = c.name.clone();
    s.push_str(" {");
    for (i, f) in c.fields.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&f.name);
        s.push_str(": ");
        s.push_str(&render_type(&f.value));
    }
    s.push('}');
    s
}

fn render_type_alias(p: &mut Printer, name: &str, params: &[Name], ty: &Type) {
    p.write("type ");
    p.write(name);
    for prm in params {
        p.write(" ");
        p.write(prm);
    }
    p.write(" = ");
    let rendered = render_type(ty);
    if p.current_col() + rendered.len() <= TARGET_WIDTH {
        p.write(&rendered);
    } else {
        // Break record types onto multiple lines.
        match &ty.node {
            TypeKind::Record { fields, rest } if !fields.is_empty() => {
                p.write("{");
                p.newline();
                p.with_indent(|p| {
                    for (i, f) in fields.iter().enumerate() {
                        p.write(&f.name);
                        p.write(": ");
                        p.write(&render_type(&f.value));
                        if i + 1 < fields.len() || rest.is_some() {
                            p.write(",");
                        }
                        p.newline();
                    }
                    if let Some(r) = rest {
                        p.write("| ");
                        p.write(r);
                        p.newline();
                    }
                });
                p.write("}");
            }
            _ => p.write(&rendered),
        }
    }
}

fn render_source(p: &mut Printer, name: &str, ty: &Type) {
    p.write("*");
    p.write(name);
    p.write(" : ");
    p.write(&render_type(ty));
}

fn render_view(p: &mut Printer, name: &str, ty: Option<&TypeScheme>, body: &Expr) {
    p.write("*");
    p.write(name);
    if let Some(scheme) = ty {
        p.write(" : ");
        p.write(&render_type_scheme(scheme));
    }
    p.write(" = ");
    render_expr(p, body, Prec::Lowest);
}

fn render_derived(p: &mut Printer, name: &str, ty: Option<&TypeScheme>, body: &Expr) {
    p.write("&");
    p.write(name);
    if let Some(scheme) = ty {
        p.write(" : ");
        p.write(&render_type_scheme(scheme));
    }
    p.write(" = ");
    render_expr(p, body, Prec::Lowest);
}

fn render_fun(p: &mut Printer, name: &str, ty: Option<&TypeScheme>, body: Option<&Expr>) {
    // Top-level `name = body`. Knot's parser only supports `name = expr`
    // here — there is no curried-args form like `name x y = ...` at module
    // scope (that syntax is exclusive to impl methods and trait defaults).
    // So a body that is a lambda must be re-emitted as a lambda.
    if let Some(ts) = ty {
        p.write(name);
        p.write(" : ");
        p.write(&render_type_scheme(ts));
        if let Some(b) = body {
            p.newline();
            p.write(name);
            p.write(" = ");
            render_expr(p, b, Prec::Lowest);
        }
    } else if let Some(b) = body {
        p.write(name);
        p.write(" = ");
        render_expr(p, b, Prec::Lowest);
    } else {
        p.write(name);
    }
}

fn render_trait(
    p: &mut Printer,
    name: &str,
    params: &[TraitParam],
    supertraits: &[Constraint],
    items: &[TraitItem],
) {
    p.write("trait ");
    if !supertraits.is_empty() {
        for c in supertraits {
            p.write(&render_constraint(c));
            p.write(" => ");
        }
    }
    p.write(name);
    for prm in params {
        if let Some(k) = &prm.kind {
            p.write(" (");
            p.write(&prm.name);
            p.write(" : ");
            p.write(&render_type(k));
            p.write(")");
        } else {
            p.write(" ");
            p.write(&prm.name);
        }
    }
    p.write(" where");
    p.newline();
    p.with_indent(|p| {
        for (i, it) in items.iter().enumerate() {
            render_trait_item(p, it);
            if i + 1 < items.len() {
                p.newline();
            }
        }
    });
}

fn render_trait_item(p: &mut Printer, it: &TraitItem) {
    match it {
        TraitItem::Method { name, ty, default_params, default_body, .. } => {
            // The parser emits one TraitItem per syntactic line: a signature
            // (`describe : a -> Text`) is one item; a default body
            // (`describe x = ...`) is another with a Hole type. Render each
            // accordingly.
            let is_body_only = matches!(ty.ty.node, TypeKind::Hole) && default_body.is_some();
            if is_body_only {
                p.write(name);
                for prm in default_params {
                    p.write(" ");
                    p.write(&render_pat(prm));
                }
                p.write(" = ");
                render_expr(p, default_body.as_ref().unwrap(), Prec::Lowest);
            } else {
                p.write(name);
                p.write(" : ");
                p.write(&render_type_scheme(ty));
                if let Some(body) = default_body {
                    p.newline();
                    p.write(name);
                    for prm in default_params {
                        p.write(" ");
                        p.write(&render_pat(prm));
                    }
                    p.write(" = ");
                    render_expr(p, body, Prec::Lowest);
                }
            }
        }
        TraitItem::AssociatedType { name, params } => {
            p.write("type ");
            p.write(name);
            for pname in params {
                p.write(" ");
                p.write(pname);
            }
        }
    }
}

fn render_impl(
    p: &mut Printer,
    trait_name: &str,
    args: &[Type],
    constraints: &[Constraint],
    items: &[ImplItem],
) {
    p.write("impl ");
    if !constraints.is_empty() {
        for c in constraints {
            p.write(&render_constraint(c));
            p.write(" => ");
        }
    }
    p.write(trait_name);
    for a in args {
        p.write(" ");
        p.write(&render_type_atom(a));
    }
    p.write(" where");
    p.newline();
    p.with_indent(|p| {
        for (i, it) in items.iter().enumerate() {
            render_impl_item(p, it);
            if i + 1 < items.len() {
                p.newline();
            }
        }
    });
}

fn render_impl_item(p: &mut Printer, it: &ImplItem) {
    match it {
        ImplItem::Method { name, params, body, .. } => {
            p.write(name);
            for prm in params {
                p.write(" ");
                p.write(&render_pat(prm));
            }
            p.write(" = ");
            render_expr(p, body, Prec::Lowest);
        }
        ImplItem::AssociatedType { name, args, ty } => {
            p.write("type ");
            p.write(name);
            for a in args {
                p.write(" ");
                p.write(&render_type_atom(a));
            }
            p.write(" = ");
            p.write(&render_type(ty));
        }
    }
}

fn render_route(p: &mut Printer, name: &str, entries: &[RouteEntry]) {
    p.write("route ");
    p.write(name);
    p.write(" where");
    p.newline();
    p.with_indent(|p| {
        for (i, e) in entries.iter().enumerate() {
            render_route_entry(p, e);
            if i + 1 < entries.len() {
                p.newline();
            }
        }
    });
}

fn render_route_entry(p: &mut Printer, e: &RouteEntry) {
    p.write(method_str(e.method));
    if !e.body_fields.is_empty() {
        p.write(" {");
        for (i, f) in e.body_fields.iter().enumerate() {
            if i > 0 {
                p.write(", ");
            }
            p.write(&f.name);
            p.write(": ");
            p.write(&render_type(&f.value));
        }
        p.write("}");
    }
    p.write(" ");
    if e.path.is_empty() {
        p.write("/");
    } else {
        for seg in &e.path {
            p.write("/");
            match seg {
                PathSegment::Literal(s) => p.write(s),
                PathSegment::Param { name, ty } => {
                    p.write("{");
                    p.write(name);
                    p.write(": ");
                    p.write(&render_type(ty));
                    p.write("}");
                }
            }
        }
    }
    if !e.query_params.is_empty() {
        p.write("?{");
        for (i, f) in e.query_params.iter().enumerate() {
            if i > 0 {
                p.write(", ");
            }
            p.write(&f.name);
            p.write(": ");
            p.write(&render_type(&f.value));
        }
        p.write("}");
    }
    if !e.request_headers.is_empty() {
        p.write(" headers {");
        for (i, f) in e.request_headers.iter().enumerate() {
            if i > 0 {
                p.write(", ");
            }
            p.write(&f.name);
            p.write(": ");
            p.write(&render_type(&f.value));
        }
        p.write("}");
    }
    if let Some(rty) = &e.response_ty {
        p.write(" -> ");
        p.write(&render_type(rty));
    }
    if !e.response_headers.is_empty() {
        p.write(" headers {");
        for (i, f) in e.response_headers.iter().enumerate() {
            if i > 0 {
                p.write(", ");
            }
            p.write(&f.name);
            p.write(": ");
            p.write(&render_type(&f.value));
        }
        p.write("}");
    }
    if let Some(rl) = &e.rate_limit {
        p.write(" rateLimit ");
        render_expr(p, rl, Prec::App);
    }
    p.write(" = ");
    p.write(&e.constructor);
}

fn method_str(m: HttpMethod) -> &'static str {
    match m {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Delete => "DELETE",
        HttpMethod::Patch => "PATCH",
    }
}

// ── Type printing ───────────────────────────────────────────────────

fn render_type(t: &Type) -> String {
    render_type_prec(t, TyPrec::Function)
}

fn render_type_atom(t: &Type) -> String {
    render_type_prec(t, TyPrec::Atom)
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum TyPrec {
    Function,
    App,
    Atom,
}

fn render_type_prec(t: &Type, ctx: TyPrec) -> String {
    match &t.node {
        TypeKind::Named(n) => n.clone(),
        TypeKind::Var(n) => n.clone(),
        TypeKind::App { func, arg } => {
            let s = format!("{} {}", render_type_prec(func, TyPrec::App), render_type_atom(arg));
            if ctx > TyPrec::App {
                format!("({})", s)
            } else {
                s
            }
        }
        TypeKind::Record { fields, rest } => {
            let mut s = String::from("{");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&f.name);
                s.push_str(": ");
                s.push_str(&render_type(&f.value));
            }
            if let Some(r) = rest {
                if !fields.is_empty() {
                    s.push_str(" | ");
                } else {
                    s.push_str("| ");
                }
                s.push_str(r);
            }
            s.push('}');
            s
        }
        TypeKind::Relation(inner) => format!("[{}]", render_type(inner)),
        TypeKind::Function { param, result } => {
            let s = format!("{} -> {}", render_type_prec(param, TyPrec::App), render_type_prec(result, TyPrec::Function));
            if ctx > TyPrec::Function {
                format!("({})", s)
            } else {
                s
            }
        }
        TypeKind::Variant { constructors, rest } => {
            let mut s = String::from("<");
            for (i, c) in constructors.iter().enumerate() {
                if i > 0 {
                    s.push_str(" | ");
                }
                s.push_str(&render_constructor(c));
            }
            if let Some(r) = rest {
                if !constructors.is_empty() {
                    s.push_str(" | ");
                } else {
                    s.push_str("| ");
                }
                s.push_str(r);
            }
            s.push('>');
            s
        }
        TypeKind::Effectful { effects, ty } => {
            let mut s = String::from("{");
            let parts = render_effects_coalesced(effects);
            s.push_str(&parts.join(", "));
            s.push_str("} ");
            s.push_str(&render_type(ty));
            if ctx > TyPrec::Function {
                format!("({})", s)
            } else {
                s
            }
        }
        TypeKind::IO { effects, rest, ty } => {
            // `rest` joins one or more row-variable names with ` \/ `; an empty
            // list means a closed row. `IO {| r \/ s} T` collapses to the
            // shorthand `IO r \/ s T`.
            let rest_joined = rest.join(" \\/ ");
            let s = if effects.is_empty() {
                if rest.is_empty() {
                    format!("IO {{}} {}", render_type_atom(ty))
                } else {
                    format!("IO {} {}", rest_joined, render_type_atom(ty))
                }
            } else {
                let mut s = String::from("IO {");
                let parts = render_effects_coalesced(effects);
                s.push_str(&parts.join(", "));
                if !rest.is_empty() {
                    s.push_str(" | ");
                    s.push_str(&rest_joined);
                }
                s.push_str("} ");
                s.push_str(&render_type_atom(ty));
                s
            };
            if ctx > TyPrec::App {
                format!("({})", s)
            } else {
                s
            }
        }
        TypeKind::Hole => "_".into(),
        TypeKind::UnitAnnotated { base, unit } => {
            format!("{}<{}>", render_type_atom(base), render_unit_expr(unit))
        }
        TypeKind::Refined { base, predicate } => {
            // `T where \x -> ...` — predicate is always a lambda.
            let s = format!("{} where {}", render_type_prec(base, TyPrec::App), render_expr_inline(predicate, Prec::Lowest));
            if ctx > TyPrec::App {
                format!("({})", s)
            } else {
                s
            }
        }
        TypeKind::Forall { vars, ty } => {
            let mut s = String::from("forall");
            for v in vars {
                s.push(' ');
                s.push_str(v);
            }
            s.push_str(". ");
            s.push_str(&render_type(ty));
            if ctx > TyPrec::Function {
                format!("({})", s)
            } else {
                s
            }
        }
    }
}

fn render_effect(e: &Effect) -> String {
    match e {
        Effect::Reads(n) => format!("r *{}", n),
        Effect::Writes(n) => format!("w *{}", n),
        Effect::Console => "console".into(),
        Effect::Network => "network".into(),
        Effect::Fs => "fs".into(),
        Effect::Clock => "clock".into(),
        Effect::Random => "random".into(),
    }
}

/// Render an effect list, coalescing an *adjacent* `r *x` followed by `w *x`
/// into `rw *x`. The parser expands `rw *x` to exactly `[Reads(x), Writes(x)]`
/// in place, so only that pattern may be coalesced — anything looser (e.g.
/// merging `w *x, r *x` or a non-adjacent pair) would reorder the effect list
/// on reparse and break the formatter's AST round-trip invariant. Pairs in
/// any other order or position are printed uncoalesced.
fn render_effects_coalesced(effects: &[Effect]) -> Vec<String> {
    let mut out = Vec::with_capacity(effects.len());
    let mut i = 0;
    while i < effects.len() {
        if let Effect::Reads(n) = &effects[i]
            && matches!(effects.get(i + 1), Some(Effect::Writes(m)) if m == n) {
                out.push(format!("rw *{}", n));
                i += 2;
                continue;
            }
        out.push(render_effect(&effects[i]));
        i += 1;
    }
    out
}

fn render_type_scheme(ts: &TypeScheme) -> String {
    let mut s = String::new();
    for c in &ts.constraints {
        s.push_str(&render_constraint(c));
        s.push_str(" => ");
    }
    s.push_str(&render_type(&ts.ty));
    s
}

fn render_constraint(c: &Constraint) -> String {
    let mut s = c.trait_name.clone();
    for a in &c.args {
        s.push(' ');
        s.push_str(&render_type_atom(a));
    }
    s
}

fn render_unit_expr(u: &UnitExpr) -> String {
    render_unit_expr_prec(u, 0)
}

/// Returns true if the unit expression is a single bare lowercase identifier
/// (e.g. `usd`, `m`), which in argument position creates a syntactic ambiguity
/// with chained comparison (`f 999<usd> 6` reparses as `(f 999 < usd) > 6`).
/// Uppercase (`M`), compound (`m/s`), and power (`m^2`) units have no
/// comparison reading, so only the bare-lowercase-ident case needs protecting.
fn unit_is_bare_lower_ident(u: &UnitExpr) -> bool {
    match u {
        UnitExpr::Named(n) => n.chars().all(|c| c.is_ascii_lowercase()) && !n.is_empty(),
        _ => false,
    }
}

/// Contexts (`ctx`): 0 = top level, 1 = left operand of `*`/`/` (left-assoc,
/// so same-precedence children need no parens), 2 = right operand of `*`/`/`
/// (a nested `*`/`/` must keep its parens to preserve associativity), 3 =
/// base of `^` (the grammar's `parse_unit_power` allows one `^` per atom, so
/// any non-atom base must be parenthesized).
fn render_unit_expr_prec(u: &UnitExpr, ctx: u8) -> String {
    match u {
        UnitExpr::Dimensionless => "1".into(),
        UnitExpr::Named(n) => n.clone(),
        UnitExpr::Mul(a, b) => {
            let s = format!("{} * {}", render_unit_expr_prec(a, 1), render_unit_expr_prec(b, 2));
            if ctx > 1 { format!("({})", s) } else { s }
        }
        UnitExpr::Div(a, b) => {
            let s = format!("{} / {}", render_unit_expr_prec(a, 1), render_unit_expr_prec(b, 2));
            if ctx > 1 { format!("({})", s) } else { s }
        }
        UnitExpr::Pow(a, n) => {
            let s = format!("{}^{}", render_unit_expr_prec(a, 3), n);
            if ctx > 2 { format!("({})", s) } else { s }
        }
    }
}

// ── Expression precedence ───────────────────────────────────────────

/// Mirrors the parser's Pratt binding powers (`parse_expr_bp`): each binary
/// operator level has a left value (`X`) and a right value (`XRhs`, one
/// tighter). All operators are left-associative except `++`, which the
/// parser treats as right-associative (equal binding powers 11/11).
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Prec {
    Lowest = 0,
    Pipe = 1,
    PipeRhs = 2,
    Or = 3,
    OrRhs = 4,
    And = 5,
    AndRhs = 6,
    Cmp = 7,
    CmpRhs = 8,
    Rel = 9,
    RelRhs = 10,
    Concat = 11,
    ConcatLhs = 12,
    Add = 13,
    AddRhs = 14,
    Mul = 15,
    MulRhs = 16,
    Unary = 17,
    App = 18,
    Atom = 19,
}

fn binop_prec(op: BinOp) -> Prec {
    match op {
        BinOp::Pipe => Prec::Pipe,
        BinOp::Or => Prec::Or,
        BinOp::And => Prec::And,
        BinOp::Eq | BinOp::Neq => Prec::Cmp,
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => Prec::Rel,
        BinOp::Concat => Prec::Concat,
        BinOp::Add | BinOp::Sub => Prec::Add,
        BinOp::Mul | BinOp::Div | BinOp::Mod => Prec::Mul,
    }
}

/// Context to use when rendering the left operand of `op`. For
/// left-associative operators a same-precedence left child needs no parens
/// (`a - b - c` parses as `(a - b) - c`). For right-associative `++` a
/// same-precedence left child must be parenthesized so `(a ++ b) ++ c`
/// doesn't reparse as `a ++ (b ++ c)`.
fn binop_lhs_prec(op: BinOp) -> Prec {
    match op {
        BinOp::Concat => Prec::ConcatLhs,
        _ => binop_prec(op),
    }
}

/// Context to use when rendering the right operand of `op`. For
/// left-associative operators a same-precedence right child must be
/// parenthesized: `10 - (5 - 2)` would otherwise print as `10 - 5 - 2`,
/// silently changing semantics. Right-associative `++` keeps same-precedence
/// right children unparenthesized (`a ++ b ++ c` already parses right-nested).
fn binop_rhs_prec(op: BinOp) -> Prec {
    match op {
        BinOp::Pipe => Prec::PipeRhs,
        BinOp::Or => Prec::OrRhs,
        BinOp::And => Prec::AndRhs,
        BinOp::Eq | BinOp::Neq => Prec::CmpRhs,
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => Prec::RelRhs,
        BinOp::Concat => Prec::Concat,
        BinOp::Add | BinOp::Sub => Prec::AddRhs,
        BinOp::Mul | BinOp::Div | BinOp::Mod => Prec::MulRhs,
    }
}

fn binop_str(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "==",
        BinOp::Neq => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Le => "<=",
        BinOp::Ge => ">=",
        BinOp::And => "&&",
        BinOp::Or => "||",
        BinOp::Concat => "++",
        BinOp::Pipe => "|>",
    }
}

// ── Expression rendering ────────────────────────────────────────────

/// Detect the parser's `let pat = value in body` desugaring so the surface
/// syntax can be printed back faithfully.
///
/// `parse_let_in_expr` produces `App { func: Lambda { params: [pat], body },
/// arg: value }` where the value's span lies textually INSIDE the lambda's
/// span (the value sits between `=` and `in`, while the lambda spans the
/// whole `let ... body` range). A genuine application can never look like
/// this: its argument follows the head textually, so `arg.span.end` is
/// always past `func.span.end`. (Span identity with the App node itself is
/// not usable — a parenthesized `(let ... in ...)` atom is re-wrapped with
/// a widened span that includes the parens.)
///
/// Returns `(pat, annot_ty, value, body)`. When the let binding carried a
/// type annotation (`let x : T = v in b`) the parser wraps the value in
/// `Annot`; it is unwrapped here so the annotation prints back in binding
/// position (the unannotated `let x = (v : T) in b` parses to the same AST,
/// so either rendering reparses identically).
fn as_let_in(e: &Expr) -> Option<(&Pat, Option<&Type>, &Expr, &Expr)> {
    if let ExprKind::App { func, arg } = &e.node
        && arg.span.end < func.span.end && arg.span.start > func.span.start
            && let ExprKind::Lambda { params, body } = &func.node
                && params.len() == 1 {
                    if let ExprKind::Annot { expr, ty } = &arg.node {
                        return Some((&params[0], Some(ty), expr, body));
                    }
                    return Some((&params[0], None, arg, body));
                }
    None
}

fn render_expr(p: &mut Printer, e: &Expr, parent: Prec) {
    if forces_multiline(e) {
        render_expr_block(p, e, parent);
    } else {
        let inline = render_expr_inline(e, parent);
        if p.current_col() + inline.len() <= TARGET_WIDTH {
            p.write(&inline);
        } else {
            render_expr_block(p, e, parent);
        }
    }
}

/// Expressions that always render on multiple lines, regardless of length.
/// `do` and `case` are layout-sensitive in idiomatic Knot — collapsing them
/// onto a single line with `;` separators is legal but unreadable.
fn forces_multiline(e: &Expr) -> bool {
    match &e.node {
        ExprKind::Do(_) | ExprKind::Case { .. } => true,
        ExprKind::Lambda { body, .. } => forces_multiline(body),
        ExprKind::App { func, arg } => forces_multiline(func) || forces_multiline(arg),
        ExprKind::Set { value, .. } | ExprKind::ReplaceSet { value, .. } => forces_multiline(value),
        _ => false,
    }
}

/// Render an expression on a single line, with conservative parenthesization.
fn render_expr_inline(e: &Expr, parent: Prec) -> String {
    // `let pat = value in body` — preserve the surface syntax instead of
    // printing the parser's `(\pat -> body) value` desugaring.
    if let Some((pat, ty, value, body)) = as_let_in(e) {
        let mut s = format!("let {}", render_pat(pat));
        if let Some(t) = ty {
            s.push_str(" : ");
            s.push_str(&render_type(t));
        }
        s.push_str(" = ");
        s.push_str(&render_expr_inline(value, Prec::Lowest));
        s.push_str(" in ");
        s.push_str(&render_expr_inline(body, Prec::Lowest));
        return paren_if(parent > Prec::Lowest, s);
    }
    match &e.node {
        ExprKind::Lit(l) => render_literal(l),
        // `yield` is refused by the parser's `can_start_atom` in application
        // argument position (it would be ambiguous with do-block yields), so
        // a Var named `yield` must keep its parens there: `f (yield)`.
        // Head position (Prec::App) must stay bare — `yield x` do-statements
        // are represented as `App(Var("yield"), x)`.
        ExprKind::Var(n) if n == "yield" && parent == Prec::Atom => format!("({})", n),
        ExprKind::Var(n) => n.clone(),
        ExprKind::Constructor(n) => n.clone(),
        ExprKind::SourceRef(n) => format!("*{}", n),
        ExprKind::DerivedRef(n) => format!("&{}", n),
        ExprKind::Record(fields) => render_record_inline(fields),
        ExprKind::RecordUpdate { base, fields } => {
            let mut s = String::from("{");
            s.push_str(&render_expr_inline(base, Prec::Lowest));
            s.push_str(" | ");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                // NOTE: never pun record-update fields. The parser only
                // accepts punning in record LITERALS; update fields must
                // always be written `name: value` (`{t | age}` is a parse
                // error and `{t | u.name}` is unparseable).
                s.push_str(&f.name);
                s.push_str(": ");
                s.push_str(&render_expr_inline(&f.value, Prec::Lowest));
            }
            s.push('}');
            s
        }
        ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", render_expr_inline(expr, Prec::Atom), field)
        }
        ExprKind::List(items) => {
            let mut s = String::from("[");
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&render_expr_inline(it, Prec::Lowest));
            }
            s.push(']');
            s
        }
        ExprKind::Lambda { params, body } => {
            let mut s = String::from("\\");
            for (i, prm) in params.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(&render_pat(prm));
            }
            s.push_str(" -> ");
            s.push_str(&render_expr_inline(body, Prec::Lowest));
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::App { func, arg } => {
            let s = format!(
                "{} {}",
                render_expr_inline(func, Prec::App),
                render_expr_inline(arg, Prec::Atom)
            );
            paren_if(parent > Prec::App, s)
        }
        ExprKind::BinOp { op, lhs, rhs } => {
            let prec = binop_prec(*op);
            let l = render_expr_inline(lhs, binop_lhs_prec(*op));
            let r = render_expr_inline(rhs, binop_rhs_prec(*op));
            let s = format!("{} {} {}", l, binop_str(*op), r);
            paren_if(parent > prec, s)
        }
        ExprKind::UnaryOp { op, operand } => {
            let s = match op {
                UnaryOp::Neg => {
                    let inner = render_expr_inline(operand, Prec::Unary);
                    // A nested negation must be parenthesized: `--x` lexes
                    // as a line comment, not double negation.
                    if inner.starts_with('-') {
                        format!("-({})", inner)
                    } else {
                        format!("-{}", inner)
                    }
                }
                UnaryOp::Not => format!("not {}", render_expr_inline(operand, Prec::App)),
            };
            paren_if(parent > Prec::Unary, s)
        }
        ExprKind::If { cond, then_branch, else_branch } => {
            let s = format!(
                "if {} then {} else {}",
                render_expr_inline(cond, Prec::Lowest),
                render_expr_inline(then_branch, Prec::Lowest),
                render_expr_inline(else_branch, Prec::Lowest),
            );
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::Case { scrutinee, arms } => {
            let mut s = format!("case {} of", render_expr_inline(scrutinee, Prec::Lowest));
            // The first arm follows `of` directly; `;` only separates arms.
            // A leading `;` would make the output unparseable.
            for (i, arm) in arms.iter().enumerate() {
                s.push_str(if i == 0 { " " } else { "; " });
                s.push_str(&render_pat(&arm.pat));
                s.push_str(" -> ");
                s.push_str(&render_expr_inline(&arm.body, Prec::Lowest));
            }
            // Always parenthesize an inline case: in positions like list
            // elements or record fields the last arm would otherwise swallow
            // the following `,`/`]`/`}` tokens on reparse.
            format!("({})", s)
        }
        ExprKind::Do(stmts) => {
            // Inline `do {s1; s2}` form is rarely useful; keep it for one-liners.
            let mut s = String::from("do ");
            for (i, st) in stmts.iter().enumerate() {
                if i > 0 {
                    s.push_str("; ");
                }
                s.push_str(&render_stmt_inline(st));
            }
            // Always parenthesize an inline do-block: in positions like list
            // elements, record fields, or if-branches a bare `do` would
            // swallow the following `,`/`]`/`}` tokens on reparse
            // (e.g. `[do yield 1, 2]`).
            format!("({})", s)
        }
        ExprKind::Set { target, value } => {
            // A set expression only parses at expression-head position or
            // inside parens — parenthesize in any tighter context (function
            // argument, operand, ...), like other lowest-precedence forms.
            let s = format!(
                "{} = {}",
                render_expr_inline(target, Prec::App),
                render_expr_inline(value, Prec::Lowest)
            );
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::ReplaceSet { target, value } => {
            let s = format!(
                "replace {} = {}",
                render_expr_inline(target, Prec::App),
                render_expr_inline(value, Prec::Lowest)
            );
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::Atomic(inner) => {
            let s = format!("atomic {}", render_expr_inline(inner, Prec::App));
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::UnitLit { value, unit } => {
            let s = format!(
                "{}<{}>",
                render_expr_inline(value, Prec::Atom),
                render_unit_expr(unit)
            );
            // A unit literal whose unit is a single bare lowercase identifier
            // (`999<usd>`) is syntactically ambiguous with a chained
            // comparison: in argument position `f 999<usd> 6` reparses as
            // `(f 999 < usd) > 6`. The parser resolves this toward comparison
            // whenever another atom follows the `>`, even across whitespace.
            // Uppercase (`<M>`), compound (`<m/s>`), and power (`<m^2>`) units
            // have no comparison reading once space-separated, so only the
            // bare-lowercase case needs protecting. Parenthesize it in tight
            // (atom) positions — i.e. as a function argument — so the `<…>`
            // can never be mistaken for comparison operators.
            if parent == Prec::Atom && unit_is_bare_lower_ident(unit) {
                format!("({})", s)
            } else {
                s
            }
        }
        ExprKind::TimeUnitLit { value, unit_name } => {
            // Recover the original numeric literal from the desugared
            // `n * factor` and re-render the surface `n unit` form (e.g.
            // `2 seconds`). The `n unit` juxtaposition reads like an
            // application, so parenthesize in argument position the same way
            // — `sleep (2 seconds)`, not `sleep 2 seconds`.
            let num = match &value.node {
                ExprKind::BinOp { lhs, .. } => render_expr_inline(lhs, Prec::Atom),
                _ => render_expr_inline(value, Prec::Atom),
            };
            paren_if(parent > Prec::App, format!("{} {}", num, unit_name))
        }
        ExprKind::Annot { expr, ty } => {
            let mut inner = render_expr_inline(expr, Prec::Lowest);
            if annot_inner_needs_parens(expr, true) {
                inner = format!("({})", inner);
            }
            format!("({} : {})", inner, render_type(ty))
        }
        ExprKind::Refine(inner) => {
            let s = format!("refine {}", render_expr_inline(inner, Prec::App));
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::Serve { api, handlers, .. } => {
            let mut s = format!("serve {} where", api);
            // The first handler follows `where` directly; `;` only separates
            // handlers. A leading `;` would make the output unparseable.
            for (i, h) in handlers.iter().enumerate() {
                s.push_str(if i == 0 { " " } else { "; " });
                s.push_str(&h.endpoint);
                s.push_str(" = ");
                s.push_str(&render_expr_inline(&h.body, Prec::Lowest));
            }
            // Always parenthesize an inline serve, for the same reason as
            // inline case/do: the last handler would otherwise swallow
            // following `,`/`]`/`}` tokens on reparse.
            format!("({})", s)
        }
    }
}

fn render_stmt_inline(s: &Stmt) -> String {
    match &s.node {
        StmtKind::Bind { pat, expr } => {
            format!("{} <- {}", render_pat(pat), render_expr_inline(expr, Prec::Lowest))
        }
        StmtKind::Let { pat, expr } => {
            format!("let {} = {}", render_pat(pat), render_expr_inline(expr, Prec::Lowest))
        }
        StmtKind::Where { cond } => {
            format!("where {}", render_expr_inline(cond, Prec::Lowest))
        }
        StmtKind::GroupBy { key } => {
            format!("groupBy {}", render_expr_inline(key, Prec::Atom))
        }
        StmtKind::Expr(e) => render_expr_inline(e, Prec::Lowest),
    }
}

fn paren_if(cond: bool, s: String) -> String {
    if cond {
        format!("({})", s)
    } else {
        s
    }
}

fn render_literal(l: &Literal) -> String {
    match l {
        Literal::Int(s) => s.clone(),
        Literal::Float(f) => {
            // Knot has no literal syntax for non-finite floats (`inf.0` would
            // reparse as a field access on an identifier). The lexer rejects
            // overflowing literals, so this only arises from
            // programmatically-built ASTs — render the nearest finite value
            // so the output stays parseable.
            if !f.is_finite() {
                return if f.is_nan() {
                    "0.0".into()
                } else if *f > 0.0 {
                    format!("{}.0", f64::MAX)
                } else {
                    format!("-{}.0", f64::MAX)
                };
            }
            // Preserve `.0` for whole floats so we don't change them to integers.
            let s = format!("{}", f);
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{}.0", s)
            }
        }
        Literal::Text(s) => format!("\"{}\"", escape_text(s)),
        Literal::Bytes(bytes) => format!("b\"{}\"", escape_bytes(bytes)),
        Literal::Bool(b) => if *b { "true".into() } else { "false".into() },
    }
}

fn escape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn escape_bytes(bs: &[u8]) -> String {
    let mut out = String::with_capacity(bs.len());
    for &b in bs {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{:02x}", b)),
        }
    }
    out
}

fn render_record_inline(fields: &[Field<Expr>]) -> String {
    let mut s = String::from("{");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        if let Some(p) = punned_form(f) {
            s.push_str(&p);
        } else {
            s.push_str(&f.name);
            s.push_str(": ");
            s.push_str(&render_expr_inline(&f.value, Prec::Lowest));
        }
    }
    s.push('}');
    s
}

/// Field punning: `{name}` is sugar for `{name: name}`, and `{e.f}` is sugar
/// for `{f: e.f}`. The parser produces both sugared and explicit forms with
/// identical ASTs, so the printer prefers the shorter form when applicable.
fn punned_form(f: &Field<Expr>) -> Option<String> {
    match &f.value.node {
        ExprKind::Var(n) if n == &f.name => Some(f.name.clone()),
        ExprKind::FieldAccess { expr, field } if field == &f.name => {
            Some(format!("{}.{}", render_expr_inline(expr, Prec::Atom), field))
        }
        _ => None,
    }
}

/// Does an expression's parse end with a greedy `parse_expr` tail?
///
/// `parse_expr` greedily consumes a trailing `: Type` postfix annotation, so
/// when one of these expressions is the inner of an `Annot`, it must be
/// parenthesized — otherwise `(\x -> x) : Int -> Int` would reformat to
/// `(\x -> x : Int -> Int)` and the annotation would silently reattach to
/// the lambda body on reparse. `inline` distinguishes the single-line
/// renderers: inline `case`/`do`/`serve` always self-parenthesize, but their
/// multi-line renderings at `Prec::Lowest` do not.
fn annot_inner_needs_parens(e: &Expr, inline: bool) -> bool {
    // `let … in body` — the body is parsed with `parse_expr`, which would
    // greedily reattach a trailing `: Type` to the body on reparse.
    if as_let_in(e).is_some() {
        return true;
    }
    match &e.node {
        // Tail is `parse_expr`: lambda body, else-branch, atomic/refine
        // operand, set/replace value.
        ExprKind::Lambda { .. }
        | ExprKind::If { .. }
        | ExprKind::Atomic(_)
        | ExprKind::Refine(_)
        | ExprKind::Set { .. }
        | ExprKind::ReplaceSet { .. } => true,
        // Last case arm body / do statement / serve handler is also parsed
        // with `parse_expr`, but the inline renderers already wrap these in
        // parens unconditionally.
        ExprKind::Case { .. } | ExprKind::Do(_) | ExprKind::Serve { .. } => !inline,
        _ => false,
    }
}

// ── Multi-line expression rendering ─────────────────────────────────

fn render_expr_block(p: &mut Printer, e: &Expr, parent: Prec) {
    // `let pat = value in body` — preserve the surface syntax (see
    // `as_let_in`). The body may render multiline (do/case blocks manage
    // their own layout after `in `).
    if let Some((pat, ty, value, body)) = as_let_in(e) {
        let need_parens = parent > Prec::Lowest;
        if need_parens {
            p.write("(");
        }
        p.write("let ");
        p.write(&render_pat(pat));
        if let Some(t) = ty {
            p.write(" : ");
            p.write(&render_type(t));
        }
        p.write(" = ");
        render_expr(p, value, Prec::Lowest);
        p.write(" in ");
        render_expr(p, body, Prec::Lowest);
        if need_parens {
            p.write(")");
        }
        return;
    }
    match &e.node {
        ExprKind::Do(stmts) => render_do_block(p, stmts, parent),
        ExprKind::Case { scrutinee, arms } => render_case_block(p, scrutinee, arms, parent),
        ExprKind::If { cond, then_branch, else_branch } => {
            render_if_block(p, cond, then_branch, else_branch, parent)
        }
        ExprKind::Lambda { params, body } => {
            // `\x y -> body` where body is multiline
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("\\");
            for (i, prm) in params.iter().enumerate() {
                if i > 0 {
                    p.write(" ");
                }
                p.write(&render_pat(prm));
            }
            p.write(" -> ");
            render_expr(p, body, Prec::Lowest);
            if need_parens {
                p.write(")");
            }
        }
        ExprKind::List(items) => render_list_block(p, items),
        ExprKind::Record(fields) => render_record_block(p, fields),
        ExprKind::RecordUpdate { base, fields } => render_record_update_block(p, base, fields),
        ExprKind::App { .. } => render_app_block(p, e, parent),
        ExprKind::BinOp { op, lhs, rhs } => render_binop_block(p, *op, lhs, rhs, parent),
        ExprKind::Set { target, value } => {
            // Same parenthesization rule as the inline form: a set expression
            // only parses at expression-head position or inside parens.
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            render_expr(p, target, Prec::App);
            p.write(" = ");
            render_expr(p, value, Prec::Lowest);
            if need_parens {
                p.write(")");
            }
        }
        ExprKind::ReplaceSet { target, value } => {
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("replace ");
            render_expr(p, target, Prec::App);
            p.write(" = ");
            render_expr(p, value, Prec::Lowest);
            if need_parens {
                p.write(")");
            }
        }
        ExprKind::Atomic(inner) => {
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("atomic ");
            render_expr(p, inner, Prec::App);
            if need_parens {
                p.write(")");
            }
        }
        ExprKind::Refine(inner) => {
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("refine ");
            render_expr(p, inner, Prec::App);
            if need_parens {
                p.write(")");
            }
        }
        ExprKind::Annot { expr, ty } => {
            p.write("(");
            let inner_parens = annot_inner_needs_parens(expr, false);
            if inner_parens {
                p.write("(");
            }
            render_expr(p, expr, Prec::Lowest);
            if inner_parens {
                p.write(")");
            }
            p.write(" : ");
            p.write(&render_type(ty));
            p.write(")");
        }
        ExprKind::Serve { api, handlers, .. } => {
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("serve ");
            p.write(api);
            p.write(" where");
            p.newline();
            p.with_indent(|p| {
                for (i, h) in handlers.iter().enumerate() {
                    p.write(&h.endpoint);
                    p.write(" = ");
                    render_expr(p, &h.body, Prec::Lowest);
                    if i + 1 < handlers.len() {
                        p.newline();
                    }
                }
            });
            if need_parens {
                p.write(")");
            }
        }
        _ => p.write(&render_expr_inline(e, parent)),
    }
}

fn render_do_block(p: &mut Printer, stmts: &[Stmt], parent: Prec) {
    let need_parens = parent > Prec::Lowest;
    if need_parens {
        p.write("(");
    }
    p.write("do");
    p.newline();
    p.with_indent(|p| {
        for (i, s) in stmts.iter().enumerate() {
            render_stmt(p, s);
            if i + 1 < stmts.len() {
                p.newline();
            }
        }
    });
    if need_parens {
        p.write(")");
    }
}

fn render_stmt(p: &mut Printer, s: &Stmt) {
    match &s.node {
        StmtKind::Bind { pat, expr } => {
            p.write(&render_pat(pat));
            p.write(" <- ");
            render_expr(p, expr, Prec::Lowest);
        }
        StmtKind::Let { pat, expr } => {
            p.write("let ");
            p.write(&render_pat(pat));
            p.write(" = ");
            render_expr(p, expr, Prec::Lowest);
        }
        StmtKind::Where { cond } => {
            p.write("where ");
            render_expr(p, cond, Prec::Lowest);
        }
        StmtKind::GroupBy { key } => {
            p.write("groupBy ");
            render_expr(p, key, Prec::Atom);
        }
        StmtKind::Expr(e) => render_expr(p, e, Prec::Lowest),
    }
}

fn render_case_block(p: &mut Printer, scrut: &Expr, arms: &[CaseArm], parent: Prec) {
    let need_parens = parent > Prec::Lowest;
    if need_parens {
        p.write("(");
    }
    p.write("case ");
    render_expr(p, scrut, Prec::Lowest);
    p.write(" of");
    p.newline();
    p.with_indent(|p| {
        for (i, arm) in arms.iter().enumerate() {
            p.write(&render_pat(&arm.pat));
            p.write(" -> ");
            render_expr(p, &arm.body, Prec::Lowest);
            if i + 1 < arms.len() {
                p.newline();
            }
        }
    });
    if need_parens {
        p.write(")");
    }
}

fn render_if_block(p: &mut Printer, cond: &Expr, then_branch: &Expr, else_branch: &Expr, parent: Prec) {
    let need_parens = parent > Prec::Lowest;
    if need_parens {
        p.write("(");
    }
    p.write("if ");
    render_expr(p, cond, Prec::Lowest);
    // Indent `then`/`else` relative to the `if` (mirrors how `case` indents
    // its arms): the newline must precede the writes inside `with_indent` so
    // the branch keywords are padded with the deeper indent.
    p.with_indent(|p| {
        p.newline();
        p.write("then ");
        render_expr(p, then_branch, Prec::Lowest);
        p.newline();
        p.write("else ");
        render_expr(p, else_branch, Prec::Lowest);
    });
    if need_parens {
        p.write(")");
    }
}

fn render_list_block(p: &mut Printer, items: &[Expr]) {
    if items.is_empty() {
        p.write("[]");
        return;
    }
    p.write("[");
    p.newline();
    p.with_indent(|p| {
        for (i, it) in items.iter().enumerate() {
            render_expr(p, it, Prec::Lowest);
            if i + 1 < items.len() {
                p.write(",");
            }
            p.newline();
        }
    });
    p.write("]");
}

fn render_record_block(p: &mut Printer, fields: &[Field<Expr>]) {
    if fields.is_empty() {
        p.write("{}");
        return;
    }
    p.write("{");
    p.newline();
    p.with_indent(|p| {
        for (i, f) in fields.iter().enumerate() {
            if let Some(s) = punned_form(f) {
                p.write(&s);
            } else {
                p.write(&f.name);
                p.write(": ");
                render_expr(p, &f.value, Prec::Lowest);
            }
            if i + 1 < fields.len() {
                p.write(",");
            }
            p.newline();
        }
    });
    p.write("}");
}

fn render_record_update_block(p: &mut Printer, base: &Expr, fields: &[Field<Expr>]) {
    p.write("{");
    p.write(&render_expr_inline(base, Prec::Lowest));
    p.write(" |");
    p.newline();
    p.with_indent(|p| {
        for (i, f) in fields.iter().enumerate() {
            // NOTE: never pun record-update fields — the parser only accepts
            // punning in record literals (see render_expr_inline's
            // RecordUpdate arm).
            p.write(&f.name);
            p.write(": ");
            render_expr(p, &f.value, Prec::Lowest);
            if i + 1 < fields.len() {
                p.write(",");
            }
            p.newline();
        }
    });
    p.write("}");
}

fn render_app_block(p: &mut Printer, e: &Expr, parent: Prec) {
    // Flatten left-spine of applications. Stop at a `let … in` node — it is
    // an App in the AST but renders as a binding, not as head + args.
    let mut spine: Vec<&Expr> = Vec::new();
    let mut cur = e;
    while let ExprKind::App { func, arg } = &cur.node {
        if as_let_in(cur).is_some() {
            break;
        }
        spine.push(arg);
        cur = func;
    }
    spine.reverse();
    let head = cur;

    let need_parens = parent > Prec::App;
    if need_parens {
        p.write("(");
    }

    // Heuristic: place head + first arg on the line, indent the rest.
    let head_str = render_expr_inline(head, Prec::App);
    p.write(&head_str);

    if let Some((last, rest)) = spine.split_last() {
        for arg in rest {
            p.write(" ");
            let inline = render_expr_inline(arg, Prec::Atom);
            if p.current_col() + inline.len() <= TARGET_WIDTH {
                p.write(&inline);
            } else {
                render_expr(p, arg, Prec::Atom);
            }
        }
        // Last argument can be a multi-line do/case/list/record.
        p.write(" ");
        render_expr(p, last, Prec::Atom);
    }

    if need_parens {
        p.write(")");
    }
}

fn render_binop_block(p: &mut Printer, op: BinOp, lhs: &Expr, rhs: &Expr, parent: Prec) {
    let prec = binop_prec(op);
    let need_parens = parent > prec;
    if need_parens {
        p.write("(");
    }
    // Associativity-aware contexts: a same-precedence right child of a
    // left-associative operator (and the mirror case for right-associative
    // `++`) must keep its parentheses — see `binop_lhs_prec`/`binop_rhs_prec`.
    render_expr(p, lhs, binop_lhs_prec(op));
    p.write(" ");
    p.write(binop_str(op));
    p.write(" ");
    render_expr(p, rhs, binop_rhs_prec(op));
    if need_parens {
        p.write(")");
    }
}

// ── Patterns ───────────────────────────────────────────────────────

fn render_pat(p: &Pat) -> String {
    match &p.node {
        PatKind::Var(n) => n.clone(),
        PatKind::Wildcard => "_".into(),
        PatKind::Constructor { name, payload } => {
            // `Ctor {}` for empty record; otherwise `Ctor {fields}` or `Ctor pat`.
            match &payload.node {
                // A constructor named `Cons` must print WITHOUT a payload
                // atom: `Cons {}` would reparse via the reserved
                // `Cons head tail` path ('{}' becomes the head pattern and
                // the parse fails on a missing tail). A bare `Cons` reparses
                // to Constructor("Cons", Record([])) — exactly this AST.
                PatKind::Record(fields) if fields.is_empty() && name == "Cons" => name.clone(),
                PatKind::Record(fields) if fields.is_empty() => format!("{} {{}}", name),
                _ => format!("{} {}", name, render_pat_atom(payload)),
            }
        }
        PatKind::Record(fields) => {
            let mut s = String::from("{");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&f.name);
                if let Some(sub) = &f.pattern {
                    s.push_str(": ");
                    s.push_str(&render_pat(sub));
                }
            }
            s.push('}');
            s
        }
        PatKind::Lit(l) => render_literal(l),
        PatKind::List(items) => {
            let mut s = String::from("[");
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&render_pat(it));
            }
            s.push(']');
            s
        }
        PatKind::Cons { head, tail } => {
            format!("Cons {} {}", render_pat_atom(head), render_pat_atom(tail))
        }
    }
}

/// Render a pattern in atom position (constructor payloads, `Cons` head/tail).
/// The grammar's `parse_pat_atom` does not accept constructor or `Cons`
/// patterns — those only parse at atom position inside parens — so they must
/// be parenthesized here. All other pattern forms are atoms already.
fn render_pat_atom(p: &Pat) -> String {
    match &p.node {
        PatKind::Constructor { .. } | PatKind::Cons { .. } => {
            format!("({})", render_pat(p))
        }
        _ => render_pat(p),
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;
    use crate::parser::Parser;

    fn fmt(src: &str) -> String {
        let lexer = Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = Parser::new(src.to_string(), tokens);
        let (module, diags) = parser.parse_module();
        for d in &diags {
            if d.severity == crate::diagnostic::Severity::Error {
                panic!("parse error: {}", d.render(src, "<test>"));
            }
        }
        format_module(src, &module)
    }

    fn assert_fmt(input: &str, expected: &str) {
        let got = fmt(input);
        assert_eq!(got, expected, "\n--- got ---\n{}\n--- want ---\n{}\n", got, expected);
    }

    fn assert_idempotent(input: &str) {
        let once = fmt(input);
        let twice = fmt(&once);
        assert_eq!(once, twice, "formatter is not idempotent:\n--- once ---\n{}\n--- twice ---\n{}\n", once, twice);
    }

    // B50: A multi-line `do`/`case` block in argument position is rendered
    // parenthesized. A statement that begins with a prefix operator (`-2`)
    // must stay a separate statement on reparse — the parser's layout column
    // guard has to fire at the block-item boundary even inside parens, or the
    // `let a = 1` line glues to the `-2` line as `let a = 1 - 2`, the reparse
    // AST diverges, and the whole file silently reverts.
    #[test]
    fn b50_parenthesized_do_block_keeps_statement_boundary() {
        // The formatter parenthesizes the do-block (last app arg) and the
        // output must round-trip — no whole-file revert to the input.
        assert_fmt(
            "main = forEach xs do\n  let a = 1\n  -2\n",
            "main = forEach xs (do\n  let a = 1\n  -2)\n",
        );
        assert_idempotent("main = forEach xs do\n  let a = 1\n  -2\n");
    }

    #[test]
    fn b50_parenthesized_do_block_parses_two_statements() {
        // Parse the parenthesized rendering directly: the do-block must hold
        // two statements (`let a = 1` and `-2`), not one glued `let a = 1 - 2`.
        let src = "main = forEach xs (do\n  let a = 1\n  -2)\n";
        let lexer = Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let (module, diags) = Parser::new(src.to_string(), tokens).parse_module();
        assert!(
            !diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error),
            "unexpected parse errors: {:?}",
            diags
        );
        fn find_do(e: &Expr) -> Option<&[Stmt]> {
            match &e.node {
                ExprKind::Do(stmts) => Some(stmts),
                ExprKind::App { func, arg } => find_do(arg).or_else(|| find_do(func)),
                _ => None,
            }
        }
        let DeclKind::Fun { body: Some(body), .. } = &module.decls[0].node else {
            panic!("expected a function declaration");
        };
        let stmts = find_do(body).expect("expected a do-block argument");
        assert_eq!(stmts.len(), 2, "statements glued together: {:?}", stmts);
        assert!(matches!(&stmts[0].node, StmtKind::Let { .. }), "{:?}", stmts[0]);
    }

    #[test]
    fn b50_parenthesized_column0_continuation_still_works() {
        // The block-boundary rule must NOT break a legitimate column-0 operator
        // continuation inside parens: here no do/case block is active, so the
        // `+ b` on the next line continues the parenthesized expression.
        let src = "main = (a\n+ b)\n";
        let lexer = Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let (module, diags) = Parser::new(src.to_string(), tokens).parse_module();
        assert!(
            !diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error),
            "unexpected parse errors: {:?}",
            diags
        );
        let DeclKind::Fun { body: Some(body), .. } = &module.decls[0].node else {
            panic!("expected a function declaration");
        };
        assert!(
            matches!(&body.node, ExprKind::BinOp { op: BinOp::Add, .. }),
            "column-0 continuation inside parens broke: {:?}",
            body
        );
    }

    #[test]
    fn non_finite_float_literals_render_parseable() {
        // No literal syntax exists for inf/NaN; the renderer must fall back
        // to finite values that re-lex as plain float literals.
        let pos = render_literal(&Literal::Float(f64::INFINITY));
        assert!(!pos.contains("inf"), "rendered: {}", pos);
        assert_eq!(pos.parse::<f64>().unwrap(), f64::MAX);

        let neg = render_literal(&Literal::Float(f64::NEG_INFINITY));
        assert!(!neg.contains("inf"), "rendered: {}", neg);
        assert_eq!(neg.parse::<f64>().unwrap(), -f64::MAX);

        let nan = render_literal(&Literal::Float(f64::NAN));
        assert_eq!(nan, "0.0");
    }

    #[test]
    fn type_alias_short() {
        assert_fmt(
            "type   Person={ name :Text,age:Int  }",
            "type Person = {name: Text, age: Int}\n",
        );
    }

    #[test]
    fn source_decl() {
        assert_fmt("*people:[Person]", "*people : [Person]\n");
    }

    #[test]
    fn data_single_constructor() {
        assert_fmt(
            "data Box a = Box {value: a}",
            "data Box a = Box {value: a}\n",
        );
    }

    #[test]
    fn data_multi_constructors_short() {
        assert_fmt(
            "data Bool = True {} | False {}",
            "data Bool = True {} | False {}\n",
        );
    }

    #[test]
    fn function_with_lambda_body() {
        assert_fmt(
            "add = \\x y -> x + y",
            "add = \\x y -> x + y\n",
        );
    }

    #[test]
    fn record_punning_preserved() {
        assert_fmt(
            "main = {name: name, age: age}",
            "main = {name, age}\n",
        );
    }

    #[test]
    fn record_field_access_punning() {
        // {e.name} sugars to {name: e.name}; the formatter prefers the sugared form.
        let formatted = fmt("main = {name: e.name, value: e.salary}");
        assert!(
            formatted.contains("{e.name, value: e.salary}"),
            "expected pun for e.name; got:\n{}",
            formatted
        );
    }

    #[test]
    fn do_block_multiline() {
        assert_fmt(
            "main = do\n  x <- foo\n  yield x",
            "main = do\n  x <- foo\n  yield x\n",
        );
    }

    #[test]
    fn case_block_multiline() {
        let src = "f = \\x -> case x of\n  Just {value} -> value\n  Nothing {} -> 0";
        assert_idempotent(src);
        let out = fmt(src);
        assert!(out.contains("case x of"));
        assert!(out.contains("Just {value} -> value"));
    }

    #[test]
    fn impl_method_curried_args() {
        let src = "impl Functor Maybe where\n  map f m = case m of\n    Just {value} -> Just {value: f value}\n    Nothing {} -> Nothing {}";
        let out = fmt(src);
        assert!(out.contains("map f m = case m of"));
    }

    #[test]
    fn comments_preserved_between_decls() {
        let src = "-- top\ntype A = Int\n\n-- middle\ntype B = Text\n";
        let out = fmt(src);
        assert!(out.starts_with("-- top\n"));
        assert!(out.contains("-- middle"));
    }

    #[test]
    fn comments_inside_decl_uses_verbatim() {
        // Comments inside a declaration force the verbatim fallback —
        // the AST-based printer would lose them otherwise.
        let src = "main = do\n  -- inside\n  yield {}\n";
        let out = fmt(src);
        assert!(out.contains("-- inside"));
    }

    #[test]
    fn idempotent_examples() {
        let inputs = [
            "type Person = {name: Text, age: Int}\n",
            "main = do\n  println \"hi\"\n  yield {}\n",
            "data Maybe a = Nothing {} | Just {value: a}\n",
            "trait Eq a where\n  eq : a -> a -> Bool\n",
        ];
        for i in inputs {
            assert_idempotent(i);
        }
    }
}
