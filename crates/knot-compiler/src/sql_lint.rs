//! SQL optimization lint: detects constructs that could theoretically
//! be folded into SQL but will fall back to runtime evaluation.
//!
//! This analysis runs on the desugared AST and produces informational
//! diagnostics for the LSP.

use std::collections::{HashMap, HashSet};

use knot::ast::*;
use knot::diagnostic::Diagnostic;

use crate::types::TypeEnv;

/// Run the SQL lint analysis on a module and return informational diagnostics.
pub fn check(module: &Module, type_env: &TypeEnv) -> Vec<Diagnostic> {
    let views: HashSet<&str> = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            DeclKind::View { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();

    let mut diags = Vec::new();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &mut diags);
            }
            DeclKind::Derived { body, .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &mut diags);
            }
            DeclKind::View { body, .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &mut diags);
            }
            _ => {}
        }
    }

    diags
}

/// Walk an expression tree looking for SQL-lintable patterns.
fn lint_expr(
    expr: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    match &expr.node {
        ExprKind::Set { target, value } => {
            if let ExprKind::SourceRef(name) = &target.node {
                let schema = source_schemas.get(name).cloned();
                if let Some(ref schema) = schema {
                    lint_set_expr(name, schema, value, source_schemas, views, diags);
                }
                // Don't recurse into the value's do-block — lint_set_expr
                // already provides more specific diagnostics for missed SQL
                // optimizations. Recursing would double-report where clauses
                // both as missed DELETE WHERE and as missed filter pushdown.
                lint_expr(target, source_schemas, views, diags);
            } else {
                lint_expr(target, source_schemas, views, diags);
                lint_expr(value, source_schemas, views, diags);
            }
        }
        ExprKind::ReplaceSet { target, value } => {
            lint_expr(target, source_schemas, views, diags);
            lint_expr(value, source_schemas, views, diags);
        }
        ExprKind::Do(stmts) => {
            lint_do_block(stmts, source_schemas, views, diags);
            for stmt in stmts {
                lint_stmt(stmt, source_schemas, views, diags);
            }
        }
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            rhs,
        } => {
            lint_pipe_chain(expr, source_schemas, views, diags);
            // Still recurse into sub-expressions for nested patterns,
            // but avoid double-reporting the top-level pipe.
            lint_expr(lhs, source_schemas, views, diags);
            lint_expr(rhs, source_schemas, views, diags);
        }
        // Recurse into all sub-expressions
        ExprKind::BinOp { lhs, rhs, .. } => {
            lint_expr(lhs, source_schemas, views, diags);
            lint_expr(rhs, source_schemas, views, diags);
        }
        ExprKind::UnaryOp { operand, .. } => {
            lint_expr(operand, source_schemas, views, diags);
        }
        ExprKind::App { func, arg } => {
            lint_app_form(func, arg, source_schemas, views, diags);
            lint_expr(func, source_schemas, views, diags);
            lint_expr(arg, source_schemas, views, diags);
        }
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            lint_expr(cond, source_schemas, views, diags);
            lint_expr(then_branch, source_schemas, views, diags);
            lint_expr(else_branch, source_schemas, views, diags);
        }
        ExprKind::Lambda { body, .. } => {
            lint_expr(body, source_schemas, views, diags);
        }
        ExprKind::Case { scrutinee, arms } => {
            lint_expr(scrutinee, source_schemas, views, diags);
            for arm in arms {
                lint_expr(&arm.body, source_schemas, views, diags);
            }
        }
        ExprKind::Record(fields) => {
            for f in fields {
                lint_expr(&f.value, source_schemas, views, diags);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            lint_expr(base, source_schemas, views, diags);
            for f in fields {
                lint_expr(&f.value, source_schemas, views, diags);
            }
        }
        ExprKind::List(elems) => {
            for e in elems {
                lint_expr(e, source_schemas, views, diags);
            }
        }
        ExprKind::FieldAccess { expr: inner, .. } => {
            lint_expr(inner, source_schemas, views, diags);
        }
        ExprKind::Atomic(inner) => {
            lint_expr(inner, source_schemas, views, diags);
        }
        ExprKind::Annot { expr: inner, .. } => {
            lint_expr(inner, source_schemas, views, diags);
        }
        ExprKind::At { relation, time } => {
            lint_expr(relation, source_schemas, views, diags);
            lint_expr(time, source_schemas, views, diags);
        }
        ExprKind::UnitLit { value, .. } => {
            lint_expr(value, source_schemas, views, diags);
        }
        ExprKind::Refine(inner) => {
            lint_expr(inner, source_schemas, views, diags);
        }
        // Terminals — nothing to recurse into
        ExprKind::Lit(_)
        | ExprKind::Var(_)
        | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_)
        | ExprKind::DerivedRef(_) => {}
    }
}

fn lint_stmt(
    stmt: &Stmt,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    match &stmt.node {
        StmtKind::Bind { expr, .. } => lint_expr(expr, source_schemas, views, diags),
        StmtKind::Let { expr, .. } => lint_expr(expr, source_schemas, views, diags),
        StmtKind::Where { cond } => lint_expr(cond, source_schemas, views, diags),
        StmtKind::Expr(e) => lint_expr(e, source_schemas, views, diags),
        StmtKind::GroupBy { .. } => {}
    }
}

// ── Do-block where-clause pushdown lint ────────────────────────────

/// Check a do-block for where clauses after source binds that can't be
/// pushed down to SQL.
fn lint_do_block(
    stmts: &[Stmt],
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    for (i, stmt) in stmts.iter().enumerate() {
        if let StmtKind::Bind { pat, expr } = &stmt.node {
            let bind_var = match &pat.node {
                PatKind::Var(name) => name,
                _ => continue,
            };
            let source_name = match &expr.node {
                ExprKind::SourceRef(name) => name,
                _ => continue,
            };
            if views.contains(source_name.as_str()) {
                continue;
            }
            let schema = match source_schemas.get(source_name) {
                Some(s) => s,
                None => continue,
            };
            if schema.starts_with('#') || schema.contains('[') {
                continue;
            }

            // Look ahead for where clauses
            let search_end = stmts[i + 1..]
                .iter()
                .position(|s| {
                    matches!(
                        &s.node,
                        StmtKind::Bind { .. } | StmtKind::Let { .. } | StmtKind::GroupBy { .. }
                    )
                })
                .map_or(stmts.len(), |p| i + 1 + p);

            for wi in (i + 1)..search_end {
                if let StmtKind::Where { cond } = &stmts[wi].node {
                    if try_compile_sql_expr(bind_var, cond).is_none() {
                        diags.push(
                            Diagnostic::info("where clause will be evaluated at runtime")
                                .label(
                                    stmts[wi].span,
                                    "cannot be compiled to a SQL WHERE clause",
                                )
                                .note(
                                    "only simple comparisons (==, !=, <, >, <=, >=) on \
                                     source fields against literals or variables can be \
                                     pushed down to SQL",
                                ),
                        );
                    }
                }
            }
        }
    }
}

// ── Set-expression lint ────────────────────────────────────────────

/// Check `set *source = <value>` for SQL optimization opportunities
/// that will fall back to runtime.
fn lint_set_expr(
    source_name: &str,
    schema: &str,
    value: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    if schema.starts_with('#') || schema.contains('[') {
        return;
    }

    // Check if value references the source (self-referential update)
    if !references_source(value, source_name) {
        return; // Full replace — no SQL optimization needed
    }

    // Try conditional update pattern:
    //   do { t <- *rel; yield (if cond then {t | ...} else t) }
    if let Some((bind_var, cond, update_fields)) =
        match_conditional_update(source_name, value)
    {
        let where_ok = try_compile_sql_expr(&bind_var, cond).is_some();
        let set_ok = where_ok
            && update_fields.iter().all(|(_, val)| {
                matches!(val.node, ExprKind::Lit(_) | ExprKind::Var(_))
                    || try_sql_atom(&bind_var, val).is_some()
            });

        if !where_ok {
            diags.push(
                Diagnostic::info(
                    "conditional update will use full table rewrite instead of SQL UPDATE",
                )
                .label(cond.span, "condition cannot be compiled to SQL WHERE")
                .note(
                    "only simple comparisons on source fields against literals \
                     or variables can be compiled to SQL",
                ),
            );
        } else if !set_ok {
            diags.push(
                Diagnostic::info(
                    "conditional update will use full table rewrite instead of SQL UPDATE",
                )
                .label(
                    value.span,
                    "update values must be literals or variables for SQL SET",
                ),
            );
        }
        return;
    }

    // Try filter-only pattern:
    //   do { t <- *rel; where cond; yield t }
    if let Some((bind_var, conditions)) = match_filter_only(source_name, value) {
        let mut any_failed = false;
        for cond in &conditions {
            if try_compile_sql_expr(&bind_var, cond).is_none() {
                any_failed = true;
                diags.push(
                    Diagnostic::info(
                        "filter will use runtime diff instead of SQL DELETE WHERE",
                    )
                    .label(cond.span, "condition cannot be compiled to SQL WHERE"),
                );
            }
        }
        if any_failed {
            return;
        }
    }

    // Remaining patterns (map-no-filter, fallback) don't have a clearly
    // missed SQL optimization to report — they're inherently runtime.
    let _ = (source_schemas, views);
}

// ── Pipe chain lint ────────────────────────────────────────────────

/// Check pipe chains like `*source |> filter f |> map g` for operations
/// that will fall back to runtime.
fn lint_pipe_chain(
    expr: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    let (source, ops) = match flatten_pipe_chain(expr) {
        Some(v) => v,
        None => return,
    };
    if ops.is_empty() {
        return;
    }

    let source_name = match &source.node {
        ExprKind::SourceRef(name) => name,
        _ => return,
    };
    if views.contains(source_name.as_str()) {
        return;
    }
    let schema = match source_schemas.get(source_name) {
        Some(s) => s,
        None => return,
    };
    if schema.starts_with('#') || schema.contains('[') {
        return;
    }

    for op in &ops {
        match op {
            LintPipeOp::Filter { bind_var, body, span } => {
                if try_compile_sql_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe filter will be evaluated at runtime instead of SQL WHERE",
                        )
                        .label(*span, "filter lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::SortBy { bind_var, body, span } => {
                if try_sql_column_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe sortBy will be evaluated at runtime instead of SQL ORDER BY",
                        )
                        .label(*span, "sortBy lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Sum { bind_var, body, span } => {
                if try_sql_column_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe sum will be evaluated at runtime instead of SQL SUM",
                        )
                        .label(*span, "sum lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Avg { bind_var, body, span } => {
                if try_sql_column_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe avg will be evaluated at runtime instead of SQL AVG",
                        )
                        .label(*span, "avg lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Min { bind_var, body, span } => {
                if try_sql_column_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe min will be evaluated at runtime instead of SQL MIN",
                        )
                        .label(*span, "min lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Max { bind_var, body, span } => {
                if try_sql_column_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe max will be evaluated at runtime instead of SQL MAX",
                        )
                        .label(*span, "max lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::CountWhere { bind_var, body, span } => {
                if try_compile_sql_expr(bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe countWhere will be evaluated at runtime instead of SQL COUNT",
                        )
                        .label(*span, "countWhere predicate cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Map { .. }
            | LintPipeOp::Count { .. }
            | LintPipeOp::Take { .. }
            | LintPipeOp::Drop { .. } => {}
        }
    }
}

// ── Application form lint ──────────────────────────────────────────

/// Check `filter f *source`, `sum f *source`, etc. for SQL fallback.
fn lint_app_form(
    func: &Expr,
    arg: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    diags: &mut Vec<Diagnostic>,
) {
    // Match: fn_name lambda_arg  (partially applied, the source comes later via pipe)
    // Or: (fn_name lambda_arg) *source  (fully applied)
    // We care about the fully-applied case: App(App(Var(fn), lambda), *source)
    if let ExprKind::App {
        func: inner_func,
        arg: lambda_arg,
    } = &func.node
    {
        let fn_name = match &inner_func.node {
            ExprKind::Var(name) => name.as_str(),
            _ => return,
        };
        let source_name = match &arg.node {
            ExprKind::SourceRef(name) => name,
            _ => return,
        };
        if views.contains(source_name.as_str()) {
            return;
        }
        let schema = match source_schemas.get(source_name) {
            Some(s) => s,
            None => return,
        };
        if schema.starts_with('#') || schema.contains('[') {
            return;
        }

        let (bind_var, body) = match extract_single_param_lambda(lambda_arg) {
            Some(v) => v,
            None => return,
        };

        match fn_name {
            "filter" => {
                if try_compile_sql_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "filter will be evaluated at runtime instead of SQL WHERE",
                        )
                        .label(
                            lambda_arg.span,
                            "filter lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "sortBy" => {
                if try_sql_column_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "sortBy will be evaluated at runtime instead of SQL ORDER BY",
                        )
                        .label(
                            lambda_arg.span,
                            "sortBy lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "sum" => {
                if try_sql_column_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "sum will be evaluated at runtime instead of SQL SUM",
                        )
                        .label(
                            lambda_arg.span,
                            "sum lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "avg" => {
                if try_sql_column_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "avg will be evaluated at runtime instead of SQL AVG",
                        )
                        .label(
                            lambda_arg.span,
                            "avg lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "min" => {
                if try_sql_column_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "min will be evaluated at runtime instead of SQL MIN",
                        )
                        .label(
                            lambda_arg.span,
                            "min lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "max" => {
                if try_sql_column_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "max will be evaluated at runtime instead of SQL MAX",
                        )
                        .label(
                            lambda_arg.span,
                            "max lambda cannot be compiled to SQL",
                        ),
                    );
                }
            }
            "countWhere" => {
                if try_compile_sql_expr(&bind_var, body).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "countWhere will be evaluated at runtime instead of SQL COUNT",
                        )
                        .label(
                            lambda_arg.span,
                            "countWhere predicate cannot be compiled to SQL",
                        ),
                    );
                }
            }
            _ => {}
        }
    }
}

// ── SQL expression compilability check ─────────────────────────────
// Mirrors the logic in codegen.rs `try_compile_sql_expr` but without
// any Cranelift dependencies.

fn try_compile_sql_expr(bind_var: &str, expr: &Expr) -> Option<()> {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                try_compile_sql_expr(bind_var, lhs)?;
                try_compile_sql_expr(bind_var, rhs)?;
                Some(())
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                // Try simple field op value, then atom-based (handles arithmetic)
                try_sql_comparison(bind_var, lhs, rhs)
                    .or_else(|| try_sql_comparison(bind_var, rhs, lhs))
                    .or_else(|| {
                        try_sql_atom(bind_var, lhs)?;
                        try_sql_atom(bind_var, rhs)
                    })
            }
            _ => None,
        },
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => try_compile_sql_expr(bind_var, operand),
        // `not expr` function application → NOT (...)
        // `contains needle haystack` → INSTR(haystack, needle) > 0
        ExprKind::App { func, arg } => {
            if let ExprKind::Var(name) = &func.node {
                if name == "not" {
                    return try_compile_sql_expr(bind_var, arg);
                }
            }
            if let ExprKind::App { func: inner_func, arg: first_arg } = &func.node {
                if let ExprKind::Var(name) = &inner_func.node {
                    if name == "contains" {
                        try_sql_atom(bind_var, first_arg)?;
                        return try_sql_atom(bind_var, arg);
                    }
                    if name == "elem" {
                        try_sql_atom(bind_var, first_arg)?;
                        // The list arg must be a literal list; each element must
                        // be a sql-pushable atom.
                        if let ExprKind::List(elems) = &arg.node {
                            for e in elems {
                                try_sql_atom(bind_var, e)?;
                            }
                            return Some(());
                        }
                        return None;
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Check if `field_side op value_side` can be a SQL comparison.
fn try_sql_comparison(bind_var: &str, field_side: &Expr, value_side: &Expr) -> Option<()> {
    // field_side must be bind_var.field
    if let ExprKind::FieldAccess { expr, .. } = &field_side.node {
        if let ExprKind::Var(name) = &expr.node {
            if name != bind_var {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    }

    // value_side must be a literal, variable, or field access on a different var
    match &value_side.node {
        ExprKind::Lit(_) => Some(()),
        ExprKind::Var(_) => Some(()),
        ExprKind::FieldAccess { expr, .. } => {
            if let ExprKind::Var(var_name) = &expr.node {
                if var_name != bind_var {
                    Some(())
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => {
            if !expr_refs_var(value_side, bind_var) {
                Some(())
            } else {
                None
            }
        }
    }
}

/// Check if an expression can be compiled as a SQL atom (column ref, literal, var, arithmetic, concat, functions).
fn try_sql_atom(bind_var: &str, expr: &Expr) -> Option<()> {
    match &expr.node {
        ExprKind::FieldAccess { expr: inner, .. } => {
            // bind_var.field → column ref, other_var.field → parameter
            if let ExprKind::Var(_) = &inner.node {
                return Some(());
            }
            None
        }
        ExprKind::Lit(_) => Some(()),
        ExprKind::Var(name) => {
            if name == bind_var { None } else { Some(()) }
        }
        ExprKind::BinOp { op, lhs, rhs } => {
            match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Concat => {
                    try_sql_atom(bind_var, lhs)?;
                    try_sql_atom(bind_var, rhs)
                }
                _ => None,
            }
        }
        // Built-in functions: length, toUpper, toLower, trim
        ExprKind::App { func, arg } => {
            if let ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "length" | "toUpper" | "toLower" | "trim" => {
                        try_sql_atom(bind_var, arg)
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => {
            if !expr_refs_var(expr, bind_var) {
                Some(())
            } else {
                None
            }
        }
    }
}

/// Check if an expression references a given variable.
fn expr_refs_var(expr: &Expr, var: &str) -> bool {
    match &expr.node {
        ExprKind::Var(name) => name == var,
        ExprKind::FieldAccess { expr: inner, .. } => expr_refs_var(inner, var),
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_refs_var(lhs, var) || expr_refs_var(rhs, var)
        }
        ExprKind::UnaryOp { operand, .. } => expr_refs_var(operand, var),
        ExprKind::App { func, arg } => {
            expr_refs_var(func, var) || expr_refs_var(arg, var)
        }
        ExprKind::If { cond, then_branch, else_branch } => {
            expr_refs_var(cond, var)
                || expr_refs_var(then_branch, var)
                || expr_refs_var(else_branch, var)
        }
        ExprKind::Lambda { body, .. } => expr_refs_var(body, var),
        ExprKind::Record(fields) => fields.iter().any(|f| expr_refs_var(&f.value, var)),
        ExprKind::RecordUpdate { base, fields } => {
            expr_refs_var(base, var) || fields.iter().any(|f| expr_refs_var(&f.value, var))
        }
        _ => false,
    }
}

// ── Pattern matchers (mirror codegen.rs) ───────────────────────────

fn match_conditional_update<'a>(
    source_name: &str,
    value: &'a Expr,
) -> Option<(String, &'a Expr, Vec<(&'a str, &'a Expr)>)> {
    let stmts = match &value.node {
        ExprKind::Do(stmts) => stmts,
        _ => return None,
    };
    if stmts.len() != 2 {
        return None;
    }

    let bind_var = match &stmts[0].node {
        StmtKind::Bind { pat, expr } => match (&pat.node, &expr.node) {
            (PatKind::Var(v), ExprKind::SourceRef(name)) if name == source_name => v.clone(),
            _ => return None,
        },
        _ => return None,
    };

    if let StmtKind::Expr(e) = &stmts[1].node {
        if let Some(yield_inner) = e.node.as_yield_arg() {
            if let ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } = &yield_inner.node
            {
                if let ExprKind::Var(v) = &else_branch.node {
                    if v != &bind_var {
                        return None;
                    }
                } else {
                    return None;
                }
                if let ExprKind::RecordUpdate { base, fields } = &then_branch.node {
                    if let ExprKind::Var(v) = &base.node {
                        if v != &bind_var {
                            return None;
                        }
                    } else {
                        return None;
                    }
                    let update_fields: Vec<(&str, &Expr)> =
                        fields.iter().map(|f| (f.name.as_str(), &f.value)).collect();
                    return Some((bind_var, cond, update_fields));
                }
            }
        }
    }
    None
}

fn match_filter_only<'a>(
    source_name: &str,
    value: &'a Expr,
) -> Option<(String, Vec<&'a Expr>)> {
    let stmts = match &value.node {
        ExprKind::Do(stmts) => stmts,
        _ => return None,
    };
    if stmts.len() < 3 {
        return None;
    }

    let bind_var = match &stmts[0].node {
        StmtKind::Bind { pat, expr } => match (&pat.node, &expr.node) {
            (PatKind::Var(v), ExprKind::SourceRef(name)) if name == source_name => v.clone(),
            _ => return None,
        },
        _ => return None,
    };

    if let StmtKind::Expr(e) = &stmts.last()?.node {
        if let Some(inner) = e.node.as_yield_arg() {
            if let ExprKind::Var(v) = &inner.node {
                if v != &bind_var {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    }

    let mut conditions = Vec::new();
    for stmt in &stmts[1..stmts.len() - 1] {
        if let StmtKind::Where { cond } = &stmt.node {
            conditions.push(cond);
        } else {
            return None;
        }
    }
    if conditions.is_empty() {
        return None;
    }

    Some((bind_var, conditions))
}

fn references_source(expr: &Expr, source_name: &str) -> bool {
    match &expr.node {
        ExprKind::SourceRef(name) => name == source_name,
        ExprKind::Lit(_)
        | ExprKind::Var(_)
        | ExprKind::Constructor(_)
        | ExprKind::DerivedRef(_) => false,
        ExprKind::Record(fields) => fields.iter().any(|f| references_source(&f.value, source_name)),
        ExprKind::RecordUpdate { base, fields } => {
            references_source(base, source_name)
                || fields.iter().any(|f| references_source(&f.value, source_name))
        }
        ExprKind::FieldAccess { expr, .. } => references_source(expr, source_name),
        ExprKind::List(elems) => elems.iter().any(|e| references_source(e, source_name)),
        ExprKind::BinOp { lhs, rhs, .. } => {
            references_source(lhs, source_name) || references_source(rhs, source_name)
        }
        ExprKind::UnaryOp { operand, .. } => references_source(operand, source_name),
        ExprKind::App { func, arg } => {
            references_source(func, source_name) || references_source(arg, source_name)
        }
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            references_source(cond, source_name)
                || references_source(then_branch, source_name)
                || references_source(else_branch, source_name)
        }
        ExprKind::Lambda { body, .. } => references_source(body, source_name),
        ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
            StmtKind::Bind { expr, .. } => references_source(expr, source_name),
            StmtKind::Let { expr, .. } => references_source(expr, source_name),
            StmtKind::Where { cond } => references_source(cond, source_name),
            StmtKind::Expr(e) => references_source(e, source_name),
            StmtKind::GroupBy { .. } => false,
        }),
        ExprKind::Case { scrutinee, arms } => {
            references_source(scrutinee, source_name)
                || arms.iter().any(|a| references_source(&a.body, source_name))
        }
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            references_source(target, source_name) || references_source(value, source_name)
        }
        ExprKind::Annot { expr, .. } => references_source(expr, source_name),
        ExprKind::Atomic(inner) | ExprKind::Refine(inner) => {
            references_source(inner, source_name)
        }
        ExprKind::At { relation, time } => {
            references_source(relation, source_name) || references_source(time, source_name)
        }
        ExprKind::UnitLit { value, .. } => references_source(value, source_name),
    }
}

// ── Pipe chain analysis ────────────────────────────────────────────

enum LintPipeOp<'a> {
    Filter {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Map {
        #[allow(dead_code)]
        span: Span,
    },
    Count {
        #[allow(dead_code)]
        span: Span,
    },
    CountWhere {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Take {
        #[allow(dead_code)]
        span: Span,
    },
    Drop {
        #[allow(dead_code)]
        span: Span,
    },
    SortBy {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Sum {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Avg {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Min {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
    Max {
        bind_var: String,
        body: &'a Expr,
        span: Span,
    },
}

fn flatten_pipe_chain(expr: &Expr) -> Option<(&Expr, Vec<LintPipeOp<'_>>)> {
    let mut ops = Vec::new();
    let mut current = expr;

    loop {
        match &current.node {
            ExprKind::BinOp {
                op: BinOp::Pipe,
                lhs,
                rhs,
            } => {
                if let Some(pipe_op) = analyze_pipe_op(rhs) {
                    ops.push(pipe_op);
                } else {
                    return None;
                }
                current = lhs;
            }
            _ => break,
        }
    }

    ops.reverse();
    Some((current, ops))
}

fn analyze_pipe_op(expr: &Expr) -> Option<LintPipeOp<'_>> {
    match &expr.node {
        ExprKind::Var(name) if name == "count" => Some(LintPipeOp::Count { span: expr.span }),
        ExprKind::App { func, arg } => {
            if let ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "filter" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Filter {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "map" => Some(LintPipeOp::Map { span: arg.span }),
                    "take" | "takeRelation" => Some(LintPipeOp::Take { span: arg.span }),
                    "drop" => Some(LintPipeOp::Drop { span: arg.span }),
                    "sortBy" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::SortBy {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "sum" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Sum {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "avg" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Avg {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "min" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Min {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "max" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Max {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "countWhere" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::CountWhere {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_single_param_lambda(expr: &Expr) -> Option<(String, &Expr)> {
    if let ExprKind::Lambda { params, body } = &expr.node {
        if params.len() == 1 {
            if let PatKind::Var(name) = &params[0].node {
                return Some((name.clone(), body));
            }
        }
    }
    None
}

/// Check if a lambda body can be compiled to a SQL expression.
/// Mirrors codegen's `extract_sql_field_access` which handles simple field access,
/// arithmetic expressions (including ++), CASE WHEN, and built-in functions.
fn try_sql_column_expr(bind_var: &str, body: &Expr) -> Option<()> {
    match &body.node {
        ExprKind::FieldAccess { expr, .. } => {
            if let ExprKind::Var(name) = &expr.node {
                if name == bind_var { return Some(()); }
            }
            None
        }
        ExprKind::Lit(lit) => match lit {
            Literal::Int(_) | Literal::Float(_) | Literal::Text(_) | Literal::Bool(_) => Some(()),
            _ => None,
        },
        ExprKind::BinOp { op, lhs, rhs } => {
            match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Concat => {
                    try_sql_column_expr(bind_var, lhs)?;
                    try_sql_column_expr(bind_var, rhs)
                }
                _ => None,
            }
        }
        ExprKind::If { cond, then_branch, else_branch } => {
            try_sql_inline_cond(bind_var, cond)?;
            try_sql_column_expr(bind_var, then_branch)?;
            try_sql_column_expr(bind_var, else_branch)
        }
        ExprKind::App { func, arg } => {
            if let ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "length" | "toUpper" | "toLower" | "trim" => {
                        try_sql_column_expr(bind_var, arg)
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if a condition can be compiled to an inline SQL condition (for CASE WHEN).
fn try_sql_inline_cond(bind_var: &str, expr: &Expr) -> Option<()> {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                try_sql_inline_cond(bind_var, lhs)?;
                try_sql_inline_cond(bind_var, rhs)
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                try_sql_column_expr(bind_var, lhs)?;
                try_sql_column_expr(bind_var, rhs)
            }
            _ => None,
        },
        ExprKind::UnaryOp { op: UnaryOp::Not, operand } => {
            try_sql_inline_cond(bind_var, operand)
        }
        ExprKind::App { func, arg } => {
            if let ExprKind::Var(name) = &func.node {
                if name == "not" {
                    return try_sql_inline_cond(bind_var, arg);
                }
            }
            if let ExprKind::App { func: inner_func, arg: first_arg } = &func.node {
                if let ExprKind::Var(name) = &inner_func.node {
                    if name == "contains" {
                        try_sql_column_expr(bind_var, first_arg)?;
                        return try_sql_column_expr(bind_var, arg);
                    }
                    if name == "elem" {
                        try_sql_column_expr(bind_var, first_arg)?;
                        // The list arg must be a literal list of scalar literals.
                        if let ExprKind::List(elems) = &arg.node {
                            for e in elems {
                                match &e.node {
                                    ExprKind::Lit(Literal::Int(_))
                                    | ExprKind::Lit(Literal::Float(_))
                                    | ExprKind::Lit(Literal::Text(_))
                                    | ExprKind::Lit(Literal::Bool(_)) => {}
                                    _ => return None,
                                }
                            }
                            return Some(());
                        }
                        return None;
                    }
                }
            }
            None
        }
        _ => None,
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use knot::diagnostic::Severity;

    /// Parse a Knot source, run the full pipeline, and return SQL lint diagnostics.
    fn lint(source: &str) -> Vec<Diagnostic> {
        let lexer = knot::lexer::Lexer::new(source);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(source.to_string(), tokens);
        let (mut module, _) = parser.parse_module();
        crate::base::inject_prelude(&mut module);
        crate::desugar::desugar(&mut module);
        let type_env = TypeEnv::from_module(&module);
        check(&module, &type_env)
    }

    #[test]
    fn no_lint_on_simple_where() {
        // Simple field comparison — compiles to SQL fine.
        let diags = lint(
            "type T = {name: Text, age: Int}\n\
             *people : [T]\n\
             main = do\n\
               p <- *people\n\
               where p.age > 30\n\
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_length_in_where() {
        // length(p.name) now compiles to SQL LENGTH().
        let diags = lint(
            "type T = {name: Text, age: Int}\n\
             *people : [T]\n\
             main = do\n\
               p <- *people\n\
               where length p.name > 3\n\
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_contains_in_update() {
        // contains now compiles to SQL INSTR().
        let diags = lint(
            "type T = {name: Text, active: Int}\n\
             *items : [T]\n\
             process = \\target -> do\n\
               *items = do\n\
                 i <- *items\n\
                 yield (if contains target i.name\n\
                   then {i | active: 0}\n\
                   else i)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_unknown_function_in_where() {
        // Custom function calls still can't be SQL-compiled.
        let diags = lint(
            "type T = {name: Text, age: Int}\n\
             *people : [T]\n\
             isLong = \\t -> length t > 10\n\
             main = do\n\
               p <- *people\n\
               where isLong p.name\n\
               yield p\n",
        );
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Severity::Info);
        assert!(diags[0].message.contains("runtime"));
    }

    #[test]
    fn no_lint_on_set_conditional_update_simple() {
        // Simple field comparison — SQL UPDATE WHERE works.
        let diags = lint(
            "type T = {name: Text, active: Int}\n\
             *items : [T]\n\
             process = \\target -> do\n\
               *items = do\n\
                 i <- *items\n\
                 yield (if i.name == target\n\
                   then {i | active: 0}\n\
                   else i)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_set_filter_complex_cond() {
        // Filter with function call — can't be SQL DELETE WHERE.
        let diags = lint(
            "type T = {name: Text, score: Int}\n\
             isGood = \\x -> x > 50\n\
             *items : [T]\n\
             cleanup = do\n\
               *items = do\n\
                 i <- *items\n\
                 where isGood i.score\n\
                 yield i\n",
        );
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Severity::Info);
        assert!(diags[0].message.contains("DELETE"));
    }

    #[test]
    fn no_lint_on_set_filter_simple() {
        // Simple comparison — SQL DELETE WHERE works.
        let diags = lint(
            "type T = {name: Text, score: Int}\n\
             *items : [T]\n\
             cleanup = do\n\
               *items = do\n\
                 i <- *items\n\
                 where i.score > 50\n\
                 yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_pipe_filter_complex() {
        // Pipe filter with function call — can't be SQL WHERE.
        let diags = lint(
            "type T = {name: Text, score: Int}\n\
             isGood = \\x -> x.score > 50\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> filter (\\i -> isGood i))\n",
        );
        assert!(!diags.is_empty(), "expected diagnostics for complex pipe filter");
        assert!(diags.iter().any(|d| d.message.contains("runtime")));
    }

    #[test]
    fn no_lint_on_pipe_filter_simple() {
        // Simple field comparison — SQL WHERE works.
        let diags = lint(
            "type T = {name: Text, score: Int}\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> filter (\\i -> i.score > 50))\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_contains_in_where() {
        // contains compiles to SQL INSTR().
        let diags = lint(
            "type T = {name: Text, age: Int}\n\
             *people : [T]\n\
             main = do\n\
               p <- *people\n\
               where contains \"test\" p.name\n\
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_elem_literal_list_in_where() {
        // elem with a literal list pushes down to SQL IN (...).
        let diags = lint(
            "type T = {name: Text, status: Text}\n\
             *items : [T]\n\
             main = do\n\
               i <- *items\n\
               where elem i.status [\"open\", \"pending\"]\n\
               yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_elem_non_literal_list_in_where() {
        // elem against a non-literal list cannot be pushed to SQL IN.
        let diags = lint(
            "type T = {name: Text, status: Text}\n\
             *items : [T]\n\
             allowed = [\"open\", \"pending\"]\n\
             main = do\n\
               i <- *items\n\
               where elem i.status allowed\n\
               yield i\n",
        );
        assert!(!diags.is_empty(), "expected diagnostic for non-literal list");
    }

    #[test]
    fn no_lint_on_toupper_in_where() {
        // toUpper compiles to SQL UPPER().
        let diags = lint(
            "type T = {name: Text, age: Int}\n\
             *people : [T]\n\
             main = do\n\
               p <- *people\n\
               where toUpper p.name == \"ALICE\"\n\
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_not_function_in_where() {
        // `not` function compiles to SQL NOT.
        let diags = lint(
            "type T = {name: Text, active: Int}\n\
             *items : [T]\n\
             main = do\n\
               i <- *items\n\
               where not (i.active == 1)\n\
               yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_arithmetic_sum() {
        // sum with arithmetic lambda compiles to SQL SUM(col * col).
        let diags = lint(
            "type T = {price: Int, qty: Int}\n\
             *items : [T]\n\
             main = do\n\
               items <- *items\n\
               yield (sum (\\i -> i.price * i.qty) items)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_arithmetic_min_max() {
        // min/max with field access compile to SQL MIN/MAX.
        let diags = lint(
            "type T = {salary: Int}\n\
             *items : [T]\n\
             main = do\n\
               items <- *items\n\
               yield (min (\\i -> i.salary) items)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_complex_min_lambda() {
        // min with non-SQL-compilable lambda body — falls back to runtime.
        let diags = lint(
            "type T = {salary: Int}\n\
             classify = \\x -> x + 100\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> min (\\i -> classify i.salary))\n",
        );
        assert!(!diags.is_empty(), "expected diagnostic for non-SQL min");
        assert!(diags.iter().any(|d| d.message.contains("MIN")));
    }

    #[test]
    fn no_lint_on_count_where_simple() {
        // countWhere with simple predicate compiles to SQL COUNT(*) WHERE.
        let diags = lint(
            "type T = {salary: Int, dept: Text}\n\
             *items : [T]\n\
             main = do\n\
               items <- *items\n\
               yield (countWhere (\\i -> i.salary > 75) items)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_count_where_complex() {
        // countWhere with non-SQL-compilable predicate — falls back to runtime.
        let diags = lint(
            "type T = {salary: Int}\n\
             isHigh = \\x -> x.salary > 50\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> countWhere (\\i -> isHigh i))\n",
        );
        assert!(!diags.is_empty(), "expected diagnostic for non-SQL countWhere");
        assert!(diags.iter().any(|d| d.message.contains("COUNT")));
    }

    #[test]
    fn no_lint_on_pipe_min_max() {
        // Pipe forms `*items |> min ...` compile to SQL MIN/MAX.
        let diags = lint(
            "type T = {salary: Int}\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> max (\\i -> i.salary))\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_pipe_count_where() {
        // Pipe form `*items |> countWhere pred` compiles to SQL COUNT(*) WHERE.
        let diags = lint(
            "type T = {salary: Int}\n\
             *items : [T]\n\
             main = do\n\
               yield (*items |> countWhere (\\i -> i.salary > 75))\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_concat_in_where() {
        // ++ compiles to SQL ||.
        let diags = lint(
            "type T = {first: Text, last: Text}\n\
             *people : [T]\n\
             main = do\n\
               p <- *people\n\
               where p.first ++ \" \" ++ p.last == \"Alice Smith\"\n\
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }
}
