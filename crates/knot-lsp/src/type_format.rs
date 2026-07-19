//! Pretty-printing for AST type/expr nodes used by hover, signature help, and
//! `build_details`. Pure formatting — no LSP types here.

use knot::ast::{self, TypeKind, TypeScheme};

/// Maximum nesting depth for type/expr/pattern/unit formatting. Inference can
/// produce pathologically nested types (deep curried functions, recursive ADT
/// expansions, refinement predicates with large expression trees) that would
/// otherwise blow the stack on hover. Past the cap, we emit `…` so the user
/// sees something rather than crashing the server.
const MAX_FORMAT_DEPTH: usize = 64;

pub fn format_type_scheme(ts: &TypeScheme) -> String {
    let mut s = String::new();
    for c in &ts.constraints {
        let args: Vec<String> = c.args.iter().map(|a| format_type_kind(&a.node)).collect();
        s.push_str(&format!("{} {} => ", c.trait_name, args.join(" ")));
    }
    s.push_str(&format_type_kind(&ts.ty.node));
    s
}

pub fn format_type_kind(ty: &TypeKind) -> String {
    format_type_kind_d(ty, 0)
}

fn format_type_kind_d(ty: &TypeKind, depth: usize) -> String {
    if depth >= MAX_FORMAT_DEPTH {
        return "…".into();
    }
    let d = depth + 1;
    match ty {
        TypeKind::Named(n) => n.clone(),
        TypeKind::Var(n) => n.clone(),
        TypeKind::App { func, arg } => {
            // Parenthesize non-atomic components so nesting stays
            // unambiguous: `Maybe (Maybe Int)` must not render as
            // `Maybe Maybe Int`, nor `Maybe (Int -> Text)` as
            // `Maybe Int -> Text` (mirrors the compiler's Ty::App display).
            let f = format_type_kind_d(&func.node, d);
            let f = if matches!(func.node, TypeKind::Function { .. } | TypeKind::Forall { .. }) {
                format!("({f})")
            } else {
                f
            };
            let a = format_type_kind_d(&arg.node, d);
            let a = if matches!(
                arg.node,
                TypeKind::App { .. }
                    | TypeKind::Function { .. }
                    | TypeKind::IO { .. }
                    | TypeKind::Effectful { .. }
                    | TypeKind::Refined { .. }
                    | TypeKind::Forall { .. }
                    | TypeKind::Unit(_)
            ) {
                format!("({a})")
            } else {
                a
            };
            format!("{f} {a}")
        }
        TypeKind::Record { fields, rest } => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, format_type_kind_d(&f.value.node, d)))
                .collect();
            match rest {
                Some(r) => format!("{{{} | {r}}}", fs.join(", ")),
                None => format!("{{{}}}", fs.join(", ")),
            }
        }
        TypeKind::Relation(inner) => format!("[{}]", format_type_kind_d(&inner.node, d)),
        TypeKind::Function { param, result } => {
            let p = format_type_kind_d(&param.node, d);
            let r = format_type_kind_d(&result.node, d);
            if matches!(param.node, TypeKind::Function { .. }) {
                format!("({p}) -> {r}")
            } else {
                format!("{p} -> {r}")
            }
        }
        TypeKind::Variant { constructors, rest } => {
            let cs: Vec<String> = constructors
                .iter()
                .map(|c| {
                    if c.fields.is_empty() {
                        c.name.clone()
                    } else {
                        let fs: Vec<String> = c
                            .fields
                            .iter()
                            .map(|f| format!("{}: {}", f.name, format_type_kind_d(&f.value.node, d)))
                            .collect();
                        format!("{} {{{}}}", c.name, fs.join(", "))
                    }
                })
                .collect();
            match rest {
                Some(r) => format!("<{} | {r}>", cs.join(" | ")),
                None => format!("<{}>", cs.join(" | ")),
            }
        }
        TypeKind::Effectful { effects, ty } => {
            let effs: Vec<String> = effects.iter().map(format_effect).collect();
            format!("{{{}}} {}", effs.join(", "), format_type_kind_d(&ty.node, d))
        }
        TypeKind::IO { effects, rest, ty } => {
            // Open rows render `{fs | r}` / `{| r}` — the row tail is
            // separated by `|`, never preceded by a comma.
            let effs: Vec<String> = effects.iter().map(format_effect).collect();
            let row = if rest.is_empty() {
                effs.join(", ")
            } else if effs.is_empty() {
                format!("| {}", rest.join(" \\/ "))
            } else {
                format!("{} | {}", effs.join(", "), rest.join(" \\/ "))
            };
            format!("IO {{{row}}} {}", format_type_kind_d(&ty.node, d))
        }
        TypeKind::Hole => "_".into(),
        TypeKind::UnitAnnotated { base, unit } => {
            // `Float M`, `Float (M / S^2)` — space-separated, parens for compound.
            let u = format_unit_expr_d(unit, d);
            if u.contains(' ') || u.contains('^') {
                format!("{} ({})", format_type_kind_d(&base.node, d), u)
            } else {
                format!("{} {}", format_type_kind_d(&base.node, d), u)
            }
        }
        TypeKind::Unit(u) => format_unit_expr_d(u, d),
        TypeKind::Refined { base, predicate } => {
            format!(
                "{} where {}",
                format_type_kind_d(&base.node, d),
                format_expr_brief_d(&predicate.node, d)
            )
        }
        TypeKind::Forall { vars, ty } => {
            format!("forall {}. {}", vars.join(" "), format_type_kind_d(&ty.node, d))
        }
    }
}

/// Brief structural rendering of an expression for display in type hovers.
fn format_expr_brief_d(expr: &ast::ExprKind, depth: usize) -> String {
    if depth >= MAX_FORMAT_DEPTH {
        return "…".into();
    }
    let d = depth + 1;
    match expr {
        ast::ExprKind::Var(name) => name.clone(),
        ast::ExprKind::Lit(ast::Literal::Int(n)) => n.to_string(),
        ast::ExprKind::Lit(ast::Literal::Float(f)) => f.to_string(),
        ast::ExprKind::Lit(ast::Literal::Text(s)) => format!("\"{}\"", s),
        ast::ExprKind::Lit(ast::Literal::Bool(b)) => if *b { "true" } else { "false" }.into(),
        ast::ExprKind::Lit(ast::Literal::Bytes(_)) => "b\"…\"".into(),
        ast::ExprKind::Lambda { params, body, .. } => {
            let ps: Vec<String> = params.iter().map(|p| format_pat_brief_d(&p.node, d)).collect();
            format!("\\{} -> {}", ps.join(" "), format_expr_brief_d(&body.node, d))
        }
        ast::ExprKind::App { func, arg } => {
            let f = format_expr_brief_d(&func.node, d);
            let a = format_expr_brief_d(&arg.node, d);
            if matches!(arg.node, ast::ExprKind::App { .. } | ast::ExprKind::BinOp { .. }) {
                format!("{f} ({a})")
            } else {
                format!("{f} {a}")
            }
        }
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let op_str = bin_op_str(*op);
            // Precedence-aware parens so nested operations round-trip:
            // `(x + 1) * 2 <= 10` must not display as `x + 1 * 2 <= 10`.
            // A child needs parens when it binds looser than its parent
            // (lower precedence), or equally on the right of a left-assoc
            // operator (`a - (b - c)`).
            let prec = bin_op_prec(*op);
            let l = format_binop_operand(&lhs.node, d, prec, false);
            let r = format_binop_operand(&rhs.node, d, prec, true);
            format!("{l} {op_str} {r}")
        }
        ast::ExprKind::UnaryOp { op, operand } => {
            let op_str = match op {
                ast::UnaryOp::Neg => "-",
                ast::UnaryOp::Not => "not ",
            };
            format!("{}{}", op_str, format_expr_brief_d(&operand.node, d))
        }
        ast::ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", format_expr_brief_d(&expr.node, d), field)
        }
        _ => "...".into(),
    }
}

fn bin_op_str(op: ast::BinOp) -> &'static str {
    match op {
        ast::BinOp::Add => "+", ast::BinOp::Sub => "-",
        ast::BinOp::Mul => "*", ast::BinOp::Div => "/", ast::BinOp::Mod => "%",
        ast::BinOp::Eq => "==", ast::BinOp::Neq => "!=",
        ast::BinOp::Lt => "<", ast::BinOp::Gt => ">",
        ast::BinOp::Le => "<=", ast::BinOp::Ge => ">=",
        ast::BinOp::And => "&&", ast::BinOp::Or => "||",
        ast::BinOp::Concat => "++", ast::BinOp::Pipe => "|>",
    }
}

/// Binding strength for display purposes (higher binds tighter).
fn bin_op_prec(op: ast::BinOp) -> u8 {
    match op {
        ast::BinOp::Pipe => 1,
        ast::BinOp::Or => 2,
        ast::BinOp::And => 3,
        ast::BinOp::Eq
        | ast::BinOp::Neq
        | ast::BinOp::Lt
        | ast::BinOp::Gt
        | ast::BinOp::Le
        | ast::BinOp::Ge => 4,
        ast::BinOp::Add | ast::BinOp::Sub | ast::BinOp::Concat => 5,
        ast::BinOp::Mul | ast::BinOp::Div | ast::BinOp::Mod => 6,
    }
}

/// Render a BinOp operand, adding parens when the child would re-associate
/// differently on re-parse.
fn format_binop_operand(expr: &ast::ExprKind, depth: usize, parent_prec: u8, is_rhs: bool) -> String {
    let rendered = format_expr_brief_d(expr, depth);
    let needs_parens = match expr {
        ast::ExprKind::BinOp { op, .. } => {
            let child_prec = bin_op_prec(*op);
            child_prec < parent_prec || (child_prec == parent_prec && is_rhs)
        }
        ast::ExprKind::Lambda { .. } | ast::ExprKind::If { .. } => true,
        _ => false,
    };
    if needs_parens {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn format_pat_brief_d(pat: &ast::PatKind, depth: usize) -> String {
    if depth >= MAX_FORMAT_DEPTH {
        return "…".into();
    }
    let d = depth + 1;
    match pat {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".into(),
        ast::PatKind::Lit(ast::Literal::Int(n)) => n.to_string(),
        ast::PatKind::Lit(ast::Literal::Float(f)) => f.to_string(),
        ast::PatKind::Lit(ast::Literal::Text(s)) => format!("\"{s}\""),
        ast::PatKind::Lit(ast::Literal::Bool(b)) => if *b { "true" } else { "false" }.into(),
        ast::PatKind::Lit(ast::Literal::Bytes(_)) => "<bytes>".into(),
        ast::PatKind::Constructor { name, payload } => match &payload.node {
            // `Open {}` — nullary constructor; drop the empty payload to keep
            // the brief rendering tight.
            ast::PatKind::Record(fields) if fields.is_empty() => name.clone(),
            other => format!("{name} {}", format_pat_brief_d(other, d)),
        },
        ast::PatKind::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| match &f.pattern {
                    None => f.name.clone(),
                    Some(p) => format!("{}: {}", f.name, format_pat_brief_d(&p.node, d)),
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ast::PatKind::List(pats) => {
            let parts: Vec<String> = pats.iter().map(|p| format_pat_brief_d(&p.node, d)).collect();
            format!("[{}]", parts.join(", "))
        }
        ast::PatKind::Cons { head, tail } => {
            format!(
                "Cons {} {}",
                format_pat_brief_d(&head.node, d),
                format_pat_brief_d(&tail.node, d)
            )
        }
        ast::PatKind::Annot { pat, .. } => format_pat_brief_d(&pat.node, d),
    }
}

fn format_unit_expr_d(u: &ast::UnitExpr, depth: usize) -> String {
    if depth >= MAX_FORMAT_DEPTH {
        return "…".into();
    }
    let d = depth + 1;
    match u {
        ast::UnitExpr::Dimensionless => "1".into(),
        ast::UnitExpr::Named(n) => n.clone(),
        ast::UnitExpr::Mul(a, b) => {
            format!("{}*{}", format_unit_expr_d(a, d), format_unit_expr_d(b, d))
        }
        ast::UnitExpr::Div(a, b) => {
            format!("{}/{}", format_unit_expr_d(a, d), format_unit_expr_d(b, d))
        }
        ast::UnitExpr::Pow(base, exp) => format!("{}^{}", format_unit_expr_d(base, d), exp),
        ast::UnitExpr::Hole => "_".into(),
    }
}

pub fn format_effect(eff: &ast::Effect) -> String {
    match eff {
        ast::Effect::Reads(name) => format!("r *{name}"),
        ast::Effect::Writes(name) => format!("w *{name}"),
        ast::Effect::Console => "console".into(),
        ast::Effect::Network => "network".into(),
        ast::Effect::Fs => "fs".into(),
        ast::Effect::Clock => "clock".into(),
        ast::Effect::Random => "random".into(),
    }
}

// Regression tests for the 2026-06 LSP bug-fix batch (type-format group).
#[cfg(test)]
mod regress_fixes_tests {
    use super::*;
    use knot::ast::{Span, Spanned};

    fn t(k: TypeKind) -> ast::Type {
        Spanned {
            node: k,
            span: Span::new(0, 0),
        }
    }

    fn named(n: &str) -> ast::Type {
        t(TypeKind::Named(n.to_string()))
    }

    fn app(f: ast::Type, a: ast::Type) -> TypeKind {
        TypeKind::App {
            func: Box::new(f),
            arg: Box::new(a),
        }
    }

    /// Item 14: non-atomic App arguments must be parenthesized.
    #[test]
    fn app_args_parenthesized() {
        // Maybe (Maybe Int)
        let inner = t(app(named("Maybe"), named("Int")));
        let outer = app(named("Maybe"), inner);
        assert_eq!(format_type_kind(&outer), "Maybe (Maybe Int)");

        // Maybe (Int -> Text)
        let f = t(TypeKind::Function {
            param: Box::new(named("Int")),
            result: Box::new(named("Text")),
        });
        let outer2 = app(named("Maybe"), f);
        assert_eq!(format_type_kind(&outer2), "Maybe (Int -> Text)");

        // Plain application unchanged.
        let plain = app(named("Maybe"), named("Int"));
        assert_eq!(format_type_kind(&plain), "Maybe Int");
    }

    /// Item 15: nested BinOps in refinement predicates keep their parens.
    #[test]
    fn refined_predicate_binop_parens_preserved() {
        use knot::ast::{BinOp, ExprKind, Literal};
        fn e(k: ExprKind) -> ast::Expr {
            Spanned {
                node: k,
                span: Span::new(0, 0),
            }
        }
        fn bin(op: BinOp, l: ast::Expr, r: ast::Expr) -> ast::Expr {
            e(ExprKind::BinOp {
                op,
                lhs: Box::new(l),
                rhs: Box::new(r),
            })
        }
        let var_x = || e(ExprKind::Var("x".into()));
        let int = |s: &str| e(ExprKind::Lit(Literal::Int(s.into())));
        // (x + 1) * 2 <= 10
        let expr = bin(
            BinOp::Le,
            bin(
                BinOp::Mul,
                bin(BinOp::Add, var_x(), int("1")),
                int("2"),
            ),
            int("10"),
        );
        assert_eq!(format_expr_brief_d(&expr.node, 0), "(x + 1) * 2 <= 10");
        // Right-associated subtraction keeps parens: a - (b - c).
        let expr2 = bin(
            BinOp::Sub,
            var_x(),
            bin(BinOp::Sub, int("1"), int("2")),
        );
        assert_eq!(format_expr_brief_d(&expr2.node, 0), "x - (1 - 2)");
        // Flat same-precedence chains don't gain parens on the left.
        let expr3 = bin(BinOp::Add, bin(BinOp::Add, var_x(), int("1")), int("2"));
        assert_eq!(format_expr_brief_d(&expr3.node, 0), "x + 1 + 2");
    }

    /// Item 16: open IO effect rows render `{fs | r}` / `{| r}`.
    #[test]
    fn io_open_row_renders_without_stray_comma() {
        let io = TypeKind::IO {
            effects: vec![ast::Effect::Fs],
            rest: vec!["r".to_string()],
            ty: Box::new(named("Text")),
        };
        assert_eq!(format_type_kind(&io), "IO {fs | r} Text");
        let io2 = TypeKind::IO {
            effects: vec![],
            rest: vec!["r".to_string()],
            ty: Box::new(named("Text")),
        };
        assert_eq!(format_type_kind(&io2), "IO {| r} Text");
        let io3 = TypeKind::IO {
            effects: vec![ast::Effect::Fs],
            rest: vec![],
            ty: Box::new(named("Text")),
        };
        assert_eq!(format_type_kind(&io3), "IO {fs} Text");
    }
}
