//! AST-based pretty printer for Knot source.
//!
//! [`format_expr`] is the public entry point used by both `knot fmt` and the
//! language server's formatting handler. A `.knot` file is a single
//! expression, so formatting renders that one expression with consistent
//! indentation and layout.

use crate::ast::*;

const INDENT: usize = 2;
const TARGET_WIDTH: usize = 100;

// ── Public entry point ─────────────────────────────────────────────

/// Format the file's single expression. The result is the rendered
/// expression followed by a trailing newline.
pub fn format_expr(_source: &str, expr: &Expr) -> String {
    let mut p = Printer::new();
    render_expr(&mut p, expr, Prec::Lowest);
    let mut out = p.finish();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

// ── Printer ────────────────────────────────────────────────────────


// ── Type printing ───────────────────────────────────────────────────

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

fn method_str(m: HttpMethod) -> &'static str {
    match m {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Delete => "DELETE",
        HttpMethod::Patch => "PATCH",
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

/// Render a single route entry as a standalone string (used for the embedded
/// `route … where` marker inside a record literal, where the record renderer
/// manages indentation itself).
fn render_route_entry_inline(e: &RouteEntry) -> String {
    let mut p = Printer::new();
    render_route_entry(&mut p, e);
    p.finish()
}

/// Render a type back to Knot source syntax. Used by the formatter and by the
/// schema lockfile, which synthesizes `*name : <ty>` declarations for sources
/// embedded in record literals.
pub fn render_type(t: &Type) -> String {
    render_type_prec(t, TyPrec::Function)
}

/// Render an expression back to Knot source syntax (inline). Used by the
/// schema lockfile, which synthesizes `migrate *name … using <fn>` lines for
/// migrations attached to record-embedded source fields.
pub fn render_expr_source(e: &Expr) -> String {
    render_expr_inline(e, Prec::Lowest)
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
            // `Float M`, `Float (M / S^2)`, `Float u` — space-separated
            // application. Parenthesize compound units (those with operators)
            // so the algebraic precedence is explicit.
            let unit_str = render_unit_type_arg(unit);
            format!("{} {}", render_type_atom(base), unit_str)
        }
        TypeKind::Unit(u) => render_unit_type_arg(u),
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
    match c {
        Constraint::Trait { trait_name, args } => {
            let mut s = trait_name.clone();
            for a in args {
                s.push(' ');
                s.push_str(&render_type_atom(a));
            }
            s
        }
        Constraint::ImplicitField { field, ty } => {
            format!("(^{field} : {})", render_type(ty))
        }
    }
}

fn render_unit_expr(u: &UnitExpr) -> String {
    render_unit_expr_prec(u, 0)
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
        UnitExpr::Hole => "_".into(),
    }
}

/// Render a unit as a type argument: bare for a simple name or `1`,
/// parenthesized for compound expressions (`M / S^2`, `M^2`).
fn render_unit_type_arg(u: &UnitExpr) -> String {
    match u {
        UnitExpr::Named(n) => n.clone(),
        UnitExpr::Dimensionless => "1".into(),
        UnitExpr::Hole => "_".into(),
        _ => format!("({})", render_unit_expr(u)),
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
            && let ExprKind::Lambda { params, body, .. } = &func.node
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
        ExprKind::ImplicitRef(n) => format!("^{}", n),
        ExprKind::Record(fields) => render_record_inline(fields),
        ExprKind::RecordUpdate { base, fields } => {
            let mut s = String::from("{");
            s.push_str(&render_expr_inline(base, Prec::Lowest));
            s.push_str(" | ");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(&f.name);
                s.push(' ');
                s.push_str(&render_expr_inline(&f.value, Prec::Atom));
            }
            s.push('}');
            s
        }
        ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", render_expr_inline(expr, Prec::Atom), field)
        }
        ExprKind::With { record, body } => {
            // Never pun the `with` record: its field NAMES are the bindings,
            // so `{lo: p.lo}` must stay explicit — `{p.lo}` would bind nothing.
            let rec_s = match &record.node {
                ExprKind::Record(fields) => render_record_inline_no_pun(fields),
                _ => render_expr_inline(record, Prec::Atom),
            };
            let s = format!("with {} {}", rec_s, render_expr_inline(body, Prec::Lowest));
            paren_if(parent > Prec::Lowest, s)
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
        ExprKind::Lambda { params, ty_params, body } => {
            let mut s = String::from("\\");
            for tp in ty_params {
                s.push_str(&format!("({} : Type)", tp.name));
                // Space after the witness only when value params follow, so a
                // witness-only lambda renders `\(T : Type) -> …`, not `)  ->`.
                if !params.is_empty() {
                    s.push(' ');
                }
            }
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
        ExprKind::TypeCtor { name, params, ty } => {
            // Renders the embedded `type` alias line. When this is a record
            // field's value, the record renderers emit the field-name line
            // before it, so here we emit only the `type Name … = <type>` part.
            let params = if params.is_empty() {
                String::new()
            } else {
                format!(" {}", params.join(" "))
            };
            format!("type {}{} = {}", name, params, render_type(ty))
        }
        ExprKind::DataCtor { name, params, constructors } => {
            // Renders the embedded `data` line. As with `TypeCtor`, the record
            // renderers emit the field-name line first, so here we emit only
            // the `data Name … = Ctor {…} | …` part.
            let params = if params.is_empty() {
                String::new()
            } else {
                format!(" {}", params.join(" "))
            };
            let ctors = constructors
                .iter()
                .map(render_constructor)
                .collect::<Vec<_>>()
                .join(" | ");
            format!("data {}{} = {}", name, params, ctors)
        }
        ExprKind::SourceDecl { name, ty, migrations } => {
            // Renders the embedded source line. The field is literally named
            // `*name`; here we emit the `*name : Type` declaration, plus any
            // attached migration clauses.
            let mut s = format!("*{} : {}", name, render_type(ty));
            for m in migrations {
                s.push_str(&format!(
                    " migrate from {} to {} using {}",
                    render_type(&m.from_ty),
                    render_type(&m.to_ty),
                    render_expr_inline(&m.using_fn, Prec::Lowest)
                ));
            }
            s
        }
        ExprKind::ViewDecl { name, ty, body } => {
            // Renders the embedded view line: `*name = body` or
            // `*name : Type = body`. The field is literally named `*name`.
            match ty {
                Some(scheme) => format!(
                    "*{} : {} = {}",
                    name,
                    render_type(&scheme.ty),
                    render_expr_inline(body, Prec::Lowest)
                ),
                None => format!("*{} = {}", name, render_expr_inline(body, Prec::Lowest)),
            }
        }
        ExprKind::DerivedDecl { name, ty, body } => {
            // Renders the embedded derived line: `&name = body` or
            // `&name : Type = body`. The field is literally named `&name`.
            match ty {
                Some(scheme) => format!(
                    "&{} : {} = {}",
                    name,
                    render_type(&scheme.ty),
                    render_expr_inline(body, Prec::Lowest)
                ),
                None => format!("&{} = {}", name, render_expr_inline(body, Prec::Lowest)),
            }
        }
        ExprKind::SubsetConstraint { sub, sup } => {
            // Renders the embedded constraint: `*a.f <= *b.g` / `*a <= *b`.
            let path = |p: &crate::ast::RelationPath| match &p.field {
                Some(f) => format!("*{}.{}", p.relation, f),
                None => format!("*{}", p.relation),
            };
            format!("{} <= {}", path(sub), path(sup))
        }
        ExprKind::RouteDecl { name, entries } => {
            // Embedded route declaration. Renders `route Name where` followed
            // by the entries on their own lines (the record renderers indent
            // the whole block). Entries reuse the top-level entry renderer.
            let mut s = format!("route {} where", name);
            for e in entries {
                s.push('\n');
                s.push_str(&render_route_entry_inline(e));
            }
            s
        }
        ExprKind::RouteCompositeDecl { name, components } => {
            // `route Name = A | rec.B | …` — components may be field paths.
            format!("route {} = {}", name, components.join(" | "))
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

/// Record fields render as whitespace-separated `name value` pairs (no `:` or
/// `,`). The value is rendered at `Prec::Atom` so non-atomic values
/// (applications, operators, lambdas, …) are parenthesized and the field
/// boundary stays unambiguous on reparse. A field with an explicit type
/// signature renders as `name : Type name value` — the sig-line form
/// round-trips through the parser unchanged.
fn render_record_inline(fields: &[RecordField]) -> String {
    let mut s = String::from("{");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        // A type-constructor field renders as the bare embedded `type` alias
        // line — the field name is the alias name, so no separate `name value`.
        if let ExprKind::TypeCtor { .. } = &f.value.node {
            s.push_str(&render_expr_inline(&f.value, Prec::Atom));
            continue;
        }
        // A source-declaration field renders as the bare `*name : Type` line —
        // the field name is the source name (with `*`), so no separate
        // `name value`.
        if let ExprKind::SourceDecl { .. } = &f.value.node {
            s.push_str(&render_expr_inline(&f.value, Prec::Atom));
            continue;
        }
        if let Some(sig) = &f.sig {
            s.push_str(&f.name);
            s.push_str(" : ");
            s.push_str(&render_type_scheme(sig));
            s.push(' ');
        }
        s.push_str(&f.name);
        s.push(' ');
        s.push_str(&render_expr_inline(&f.value, Prec::Atom));
    }
    s.push('}');
    s
}

/// `with` records use the same `name value` rendering (no punning exists).
fn render_record_inline_no_pun(fields: &[RecordField]) -> String {
    render_record_inline(fields)
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
        ExprKind::Lambda { params, ty_params, body } => {
            // `\(T : Type) \x y -> body` where body is multiline
            let need_parens = parent > Prec::Lowest;
            if need_parens {
                p.write("(");
            }
            p.write("\\");
            for tp in ty_params {
                p.write(&format!("({} : Type)", tp.name));
                if !params.is_empty() {
                    p.write(" ");
                }
            }
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

fn render_record_block(p: &mut Printer, fields: &[RecordField]) {
    if fields.is_empty() {
        p.write("{}");
        return;
    }
    p.write("{");
    p.newline();
    p.with_indent(|p| {
        for f in fields.iter() {
            // A type-constructor field renders as the bare embedded `type`
            // alias line — the field name is the alias name.
            if let ExprKind::TypeCtor { .. } = &f.value.node {
                render_expr(p, &f.value, Prec::Atom);
                p.newline();
                continue;
            }
            // A source-declaration field renders as the bare `*name : Type`
            // line — the field name is the source name (with `*`).
            if let ExprKind::SourceDecl { .. } = &f.value.node {
                render_expr(p, &f.value, Prec::Atom);
                p.newline();
                continue;
            }
            // A field with an explicit type signature keeps its sig-line
            // layout: `name : Type` on its own line, then `name value`.
            if let Some(sig) = &f.sig {
                p.write(&f.name);
                p.write(" : ");
                p.write(&render_type_scheme(sig));
                p.newline();
            }
            p.write(&f.name);
            p.write(" ");
            render_expr(p, &f.value, Prec::Atom);
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
        for f in fields.iter() {
            p.write(&f.name);
            p.write(" ");
            render_expr(p, &f.value, Prec::Atom);
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
        PatKind::Constructor {
            name,
            payload,
            qualifier,
        } => {
            // Re-emit the qualifier for a qualified pattern (`Color.Red`).
            let head = match qualifier {
                Some(q) => format!("{q}.{name}"),
                None => name.clone(),
            };
            // `Ctor {}` for empty record; otherwise `Ctor {fields}` or `Ctor pat`.
            match &payload.node {
                // A constructor named `Cons` must print WITHOUT a payload
                // atom: `Cons {}` would reparse via the reserved
                // `Cons head tail` path ('{}' becomes the head pattern and
                // the parse fails on a missing tail). A bare `Cons` reparses
                // to Constructor("Cons", Record([])) — exactly this AST.
                PatKind::Record(fields) if fields.is_empty() && name == "Cons" => head,
                PatKind::Record(fields) if fields.is_empty() => format!("{} {{}}", head),
                _ => format!("{} {}", head, render_pat_atom(payload)),
            }
        }
        PatKind::Record(fields) => {
            let mut s = String::from("{");
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(&f.name);
                // The parser always produces `Some` here (no punning exists);
                // `None` is unreachable but handled defensively as a bare name.
                if let Some(sub) = &f.pattern {
                    s.push(' ');
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
        PatKind::Annot { pat, ty } => {
            format!("({} : {})", render_pat(pat), render_type(ty))
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


