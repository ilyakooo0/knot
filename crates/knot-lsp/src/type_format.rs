//! Pretty-printing for AST type/expr nodes used by hover, signature help, and
//! `build_details`. Pure formatting — no LSP types here.

use knot::ast::{self, TypeKind, TypeScheme};

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
    match ty {
        TypeKind::Named(n) => n.clone(),
        TypeKind::Var(n) => n.clone(),
        TypeKind::App { func, arg } => {
            format!("{} {}", format_type_kind(&func.node), format_type_kind(&arg.node))
        }
        TypeKind::Record { fields, rest } => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                .collect();
            match rest {
                Some(r) => format!("{{{} | {r}}}", fs.join(", ")),
                None => format!("{{{}}}", fs.join(", ")),
            }
        }
        TypeKind::Relation(inner) => format!("[{}]", format_type_kind(&inner.node)),
        TypeKind::Function { param, result } => {
            let p = format_type_kind(&param.node);
            let r = format_type_kind(&result.node);
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
                            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
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
            format!("{{{}}} {}", effs.join(", "), format_type_kind(&ty.node))
        }
        TypeKind::IO { effects, rest, ty } => {
            let mut parts: Vec<String> = effects.iter().map(format_effect).collect();
            if let Some(name) = rest {
                parts.push(format!("| {}", name));
            }
            format!("IO {{{}}} {}", parts.join(", "), format_type_kind(&ty.node))
        }
        TypeKind::Hole => "_".into(),
        TypeKind::UnitAnnotated { base, unit } => {
            format!("{}<{}>", format_type_kind(&base.node), format_unit_expr(unit))
        }
        TypeKind::Refined { base, predicate } => {
            format!("{} where {}", format_type_kind(&base.node), format_expr_brief(&predicate.node))
        }
        TypeKind::Forall { vars, ty } => {
            format!("forall {}. {}", vars.join(" "), format_type_kind(&ty.node))
        }
    }
}

/// Brief structural rendering of an expression for display in type hovers.
pub fn format_expr_brief(expr: &ast::ExprKind) -> String {
    match expr {
        ast::ExprKind::Var(name) => name.clone(),
        ast::ExprKind::Lit(ast::Literal::Int(n)) => n.to_string(),
        ast::ExprKind::Lit(ast::Literal::Float(f)) => f.to_string(),
        ast::ExprKind::Lit(ast::Literal::Text(s)) => format!("\"{}\"", s),
        ast::ExprKind::Lit(ast::Literal::Bool(b)) => if *b { "true" } else { "false" }.into(),
        ast::ExprKind::Lambda { params, body } => {
            let ps: Vec<String> = params.iter().map(|p| format_pat_brief(&p.node)).collect();
            format!("\\{} -> {}", ps.join(" "), format_expr_brief(&body.node))
        }
        ast::ExprKind::App { func, arg } => {
            let f = format_expr_brief(&func.node);
            let a = format_expr_brief(&arg.node);
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
            format!("{} {} {}", format_expr_brief(&lhs.node), op_str, format_expr_brief(&rhs.node))
        }
        ast::ExprKind::UnaryOp { op, operand } => {
            let op_str = match op {
                ast::UnaryOp::Neg => "-",
                ast::UnaryOp::Not => "not ",
            };
            format!("{}{}", op_str, format_expr_brief(&operand.node))
        }
        ast::ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", format_expr_brief(&expr.node), field)
        }
        _ => "...".into(),
    }
}

pub fn format_pat_brief(pat: &ast::PatKind) -> String {
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
            other => format!("{name} {}", format_pat_brief(other)),
        },
        ast::PatKind::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| match &f.pattern {
                    None => f.name.clone(),
                    Some(p) => format!("{}: {}", f.name, format_pat_brief(&p.node)),
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ast::PatKind::List(pats) => {
            let parts: Vec<String> = pats.iter().map(|p| format_pat_brief(&p.node)).collect();
            format!("[{}]", parts.join(", "))
        }
    }
}

pub fn format_unit_expr(u: &ast::UnitExpr) -> String {
    match u {
        ast::UnitExpr::Dimensionless => "1".into(),
        ast::UnitExpr::Named(n) => n.clone(),
        ast::UnitExpr::Mul(a, b) => format!("{}*{}", format_unit_expr(a), format_unit_expr(b)),
        ast::UnitExpr::Div(a, b) => format!("{}/{}", format_unit_expr(a), format_unit_expr(b)),
        ast::UnitExpr::Pow(base, exp) => format!("{}^{}", format_unit_expr(base), exp),
    }
}

pub fn format_effect(eff: &ast::Effect) -> String {
    match eff {
        ast::Effect::Reads(r) => format!("reads *{r}"),
        ast::Effect::Writes(r) => format!("writes *{r}"),
        ast::Effect::Console => "console".into(),
        ast::Effect::Network => "network".into(),
        ast::Effect::Fs => "fs".into(),
        ast::Effect::Clock => "clock".into(),
        ast::Effect::Random => "random".into(),
    }
}
