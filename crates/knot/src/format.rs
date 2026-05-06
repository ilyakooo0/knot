//! AST-based pretty printer for Knot source.
//!
//! [`format_module`] is the single public entry point used by both `knotc fmt`
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
    let leading_comments: Vec<&Comment> = comments
        .iter()
        .filter(|c| c.standalone && c.span.end <= first_start)
        .collect();
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

            // Standalone comments between the previous block and this one.
            let between: Vec<&Comment> = comments
                .iter()
                .filter(|c| c.standalone && c.span.start >= prev_end && c.span.end <= block_start)
                .collect();

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
    source[from..to].bytes().filter(|&b| b == b'\n').count() >= 2
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

    while i < bytes.len() {
        let b = bytes[i];

        if in_text {
            if b == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_text = false;
            } else if b == b'\n' {
                line += 1;
                line_start = i + 1;
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
            } else if b == b'\n' {
                line += 1;
                line_start = i + 1;
            }
            i += 1;
            continue;
        }

        if b == b'"' {
            in_text = true;
            i += 1;
            continue;
        }
        if b == b'b' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
            in_bytes = true;
            i += 2;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Comment to end of line.
            let standalone = source[line_start..i].chars().all(|c| c == ' ' || c == '\t');
            let comment_start = i;
            while i < bytes.len() && bytes[i] != b'\n' {
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
        if b == b'\n' {
            line += 1;
            line_start = i + 1;
        }
        i += 1;
    }
    out
}

fn line_of(source: &str, byte: usize) -> usize {
    source[..byte.min(source.len())].bytes().filter(|&b| b == b'\n').count()
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
        return normalize_source_slice(&source[d.span.start..d.span.end]);
    }
    let mut p = Printer::new();
    render_decl(&mut p, d);
    p.finish()
}

/// Verbatim source with tabs → 2 spaces and trailing whitespace trimmed.
fn normalize_source_slice(s: &str) -> String {
    let s = s.replace('\t', "  ");
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
            // Column = bytes since last newline (sufficient for ASCII layout).
            match self.out.rfind('\n') {
                Some(i) => self.out.len() - i - 1,
                None => self.out.len(),
            }
        }
    }
}

// ── Declarations ───────────────────────────────────────────────────

fn render_decl(p: &mut Printer, d: &Decl) {
    match &d.node {
        DeclKind::Data { name, params, constructors, deriving } => {
            render_data(p, name, params, constructors, deriving);
        }
        DeclKind::TypeAlias { name, params, ty } => {
            render_type_alias(p, name, params, ty);
        }
        DeclKind::Source { name, ty, history } => {
            render_source(p, name, ty, *history);
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
            p.write("migrate *");
            p.write(relation);
            p.write(" from ");
            p.write(&render_type(from_ty));
            p.write(" to ");
            p.write(&render_type(to_ty));
            p.write(" using ");
            render_expr(p, using_fn, Prec::App);
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

fn render_source(p: &mut Printer, name: &str, ty: &Type, history: bool) {
    p.write("*");
    p.write(name);
    p.write(" : ");
    p.write(&render_type(ty));
    if history {
        p.newline();
        p.with_indent(|p| {
            p.write("with history");
        });
    }
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
            let mut s = String::from("IO {");
            let parts = render_effects_coalesced(effects);
            s.push_str(&parts.join(", "));
            if let Some(r) = rest {
                if !effects.is_empty() {
                    s.push_str(" | ");
                } else {
                    s.push_str("| ");
                }
                s.push_str(r);
            }
            s.push_str("} ");
            s.push_str(&render_type_atom(ty));
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

/// Render an effect list, coalescing matching `r *x` and `w *x` pairs into
/// `rw *x`. Preserves the original ordering for non-coalesced effects.
fn render_effects_coalesced(effects: &[Effect]) -> Vec<String> {
    use std::collections::BTreeSet;
    let mut reads: BTreeSet<&str> = BTreeSet::new();
    let mut writes: BTreeSet<&str> = BTreeSet::new();
    for e in effects {
        match e {
            Effect::Reads(n) => {
                reads.insert(n.as_str());
            }
            Effect::Writes(n) => {
                writes.insert(n.as_str());
            }
            _ => {}
        }
    }
    let both: BTreeSet<&str> = reads.intersection(&writes).copied().collect();
    let mut emitted_rw: BTreeSet<&str> = BTreeSet::new();
    let mut out = Vec::with_capacity(effects.len());
    for e in effects {
        match e {
            Effect::Reads(n) if both.contains(n.as_str()) => {
                if emitted_rw.insert(n.as_str()) {
                    out.push(format!("rw *{}", n));
                }
            }
            Effect::Writes(n) if both.contains(n.as_str()) => {
                if emitted_rw.insert(n.as_str()) {
                    out.push(format!("rw *{}", n));
                }
            }
            _ => out.push(render_effect(e)),
        }
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

fn render_unit_expr_prec(u: &UnitExpr, ctx: u8) -> String {
    match u {
        UnitExpr::Dimensionless => "1".into(),
        UnitExpr::Named(n) => n.clone(),
        UnitExpr::Mul(a, b) => {
            let s = format!("{} * {}", render_unit_expr_prec(a, 1), render_unit_expr_prec(b, 1));
            if ctx > 1 { format!("({})", s) } else { s }
        }
        UnitExpr::Div(a, b) => {
            let s = format!("{} / {}", render_unit_expr_prec(a, 1), render_unit_expr_prec(b, 2));
            if ctx > 1 { format!("({})", s) } else { s }
        }
        UnitExpr::Pow(a, n) => format!("{}^{}", render_unit_expr_prec(a, 2), n),
    }
}

// ── Expression precedence ───────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Prec {
    Lowest = 0,
    Pipe = 1,
    Or = 3,
    And = 5,
    Cmp = 7,
    Concat = 11,
    Add = 13,
    Mul = 15,
    Unary = 17,
    App = 18,
    Atom = 19,
}

fn binop_prec(op: BinOp) -> Prec {
    match op {
        BinOp::Pipe => Prec::Pipe,
        BinOp::Or => Prec::Or,
        BinOp::And => Prec::And,
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => Prec::Cmp,
        BinOp::Concat => Prec::Concat,
        BinOp::Add | BinOp::Sub => Prec::Add,
        BinOp::Mul | BinOp::Div => Prec::Mul,
    }
}

fn binop_str(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
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
    match &e.node {
        ExprKind::Lit(l) => render_literal(l),
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
            // For right-assoc operators (`++`), keep parens off the right side
            // when same precedence.
            let l = render_expr_inline(lhs, if *op == BinOp::Concat { Prec::Concat } else { prec });
            let r = render_expr_inline(rhs, prec);
            let s = format!("{} {} {}", l, binop_str(*op), r);
            paren_if(parent > prec, s)
        }
        ExprKind::UnaryOp { op, operand } => {
            let s = match op {
                UnaryOp::Neg => format!("-{}", render_expr_inline(operand, Prec::Unary)),
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
            for arm in arms {
                s.push_str("; ");
                s.push_str(&render_pat(&arm.pat));
                s.push_str(" -> ");
                s.push_str(&render_expr_inline(&arm.body, Prec::Lowest));
            }
            paren_if(parent > Prec::Lowest, s)
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
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::Set { target, value } => {
            format!(
                "{} = {}",
                render_expr_inline(target, Prec::App),
                render_expr_inline(value, Prec::Lowest)
            )
        }
        ExprKind::ReplaceSet { target, value } => {
            format!(
                "replace {} = {}",
                render_expr_inline(target, Prec::App),
                render_expr_inline(value, Prec::Lowest)
            )
        }
        ExprKind::Atomic(inner) => {
            let s = format!("atomic {}", render_expr_inline(inner, Prec::App));
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::At { relation, time } => {
            format!(
                "{} @({})",
                render_expr_inline(relation, Prec::Atom),
                render_expr_inline(time, Prec::Lowest)
            )
        }
        ExprKind::UnitLit { value, unit } => {
            format!(
                "{}<{}>",
                render_expr_inline(value, Prec::Atom),
                render_unit_expr(unit)
            )
        }
        ExprKind::Annot { expr, ty } => {
            format!(
                "({} : {})",
                render_expr_inline(expr, Prec::Lowest),
                render_type(ty)
            )
        }
        ExprKind::Refine(inner) => {
            let s = format!("refine {}", render_expr_inline(inner, Prec::App));
            paren_if(parent > Prec::Lowest, s)
        }
        ExprKind::Serve { api, handlers, .. } => {
            let mut s = format!("serve {} where", api);
            for h in handlers {
                s.push_str("; ");
                s.push_str(&h.endpoint);
                s.push_str(" = ");
                s.push_str(&render_expr_inline(&h.body, Prec::Lowest));
            }
            paren_if(parent > Prec::Lowest, s)
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

// ── Multi-line expression rendering ─────────────────────────────────

fn render_expr_block(p: &mut Printer, e: &Expr, parent: Prec) {
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
            render_expr(p, target, Prec::App);
            p.write(" = ");
            render_expr(p, value, Prec::Lowest);
        }
        ExprKind::ReplaceSet { target, value } => {
            p.write("replace ");
            render_expr(p, target, Prec::App);
            p.write(" = ");
            render_expr(p, value, Prec::Lowest);
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
            render_expr(p, expr, Prec::Lowest);
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
    p.newline();
    p.write("then ");
    render_expr(p, then_branch, Prec::Lowest);
    p.newline();
    p.write("else ");
    render_expr(p, else_branch, Prec::Lowest);
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

fn render_app_block(p: &mut Printer, e: &Expr, parent: Prec) {
    // Flatten left-spine of applications.
    let mut spine: Vec<&Expr> = Vec::new();
    let mut cur = e;
    while let ExprKind::App { func, arg } = &cur.node {
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
    // Render left side with binop precedence.
    render_expr(p, lhs, prec);
    p.write(" ");
    p.write(binop_str(op));
    p.write(" ");
    render_expr(p, rhs, prec);
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
            let pl = render_pat(payload);
            // `Ctor {}` for empty record; otherwise `Ctor {fields}` or `Ctor pat`.
            match &payload.node {
                PatKind::Record(fields) if fields.is_empty() => format!("{} {{}}", name),
                _ => format!("{} {}", name, pl),
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
