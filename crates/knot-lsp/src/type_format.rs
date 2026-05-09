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
            format!("{} {}", format_type_kind_d(&func.node, d), format_type_kind_d(&arg.node, d))
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
            let mut parts: Vec<String> = effects.iter().map(format_effect).collect();
            if let Some(name) = rest {
                parts.push(format!("| {}", name));
            }
            format!("IO {{{}}} {}", parts.join(", "), format_type_kind_d(&ty.node, d))
        }
        TypeKind::Hole => "_".into(),
        TypeKind::UnitAnnotated { base, unit } => {
            format!("{}<{}>", format_type_kind_d(&base.node, d), format_unit_expr_d(unit, d))
        }
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
        ast::ExprKind::Lambda { params, body } => {
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
            let op_str = match op {
                ast::BinOp::Add => "+", ast::BinOp::Sub => "-",
                ast::BinOp::Mul => "*", ast::BinOp::Div => "/",
                ast::BinOp::Eq => "==", ast::BinOp::Neq => "!=",
                ast::BinOp::Lt => "<", ast::BinOp::Gt => ">",
                ast::BinOp::Le => "<=", ast::BinOp::Ge => ">=",
                ast::BinOp::And => "&&", ast::BinOp::Or => "||",
                ast::BinOp::Concat => "++", ast::BinOp::Pipe => "|>",
            };
            format!(
                "{} {} {}",
                format_expr_brief_d(&lhs.node, d),
                op_str,
                format_expr_brief_d(&rhs.node, d)
            )
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
