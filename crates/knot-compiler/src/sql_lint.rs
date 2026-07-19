//! SQL optimization lint: detects constructs that could theoretically
//! be folded into SQL but will fall back to runtime evaluation.
//!
//! This analysis runs on the desugared AST and produces informational
//! diagnostics for the LSP.

use std::collections::{HashMap, HashSet};

use knot::ast::*;
use knot::diagnostic::Diagnostic;

use crate::codegen::{
    beta_reduce, divisor_is_nonzero_int_literal, divisor_is_nonzero_literal, expr_has_tag_column,
    expr_refs_var, infer_sql_expr_type,
    lookup_col_type_from_schema,
    minmax_pushdown_type_ok, sortby_projection_pushable, sql_comparison_cast_mode, type_name_to_tag,
    SqlCastMode,
};
use crate::types::TypeEnv;

/// Mirror of codegen's `sql_pushdown_disabled_by_user_impls`: when the program
/// defines a user impl of an operator method (`eq`/`compare`/`add`/…) on a
/// primitive type, codegen disables SQL pushdown wholesale and evaluates every
/// comparison/arithmetic in memory. In that mode none of the pushdown lints
/// hold — staying silent on a construct would falsely imply it pushes down to
/// SQL — so the lint suppresses its pushdown diagnostics entirely.
fn pushdown_disabled_by_user_impls(module: &Module) -> bool {
    const OP_METHODS: &[&str] = &["eq", "compare", "add", "sub", "mul", "div", "mod", "negate"];
    module.decls.iter().any(|d| match &d.node {
        DeclKind::Impl { args, items, .. } => {
            let on_primitive = matches!(
                args.first().map(|t| &t.node),
                Some(TypeKind::Named(n)) if type_name_to_tag(n.as_str()).is_some()
            );
            on_primitive
                && items.iter().any(|it| {
                    matches!(it,
                        ImplItem::Method { name, .. } if OP_METHODS.contains(&name.as_str()))
                })
        }
        _ => false,
    })
}

/// Run the SQL lint analysis on a module and return informational diagnostics.
pub fn check(module: &Module, type_env: &TypeEnv) -> Vec<Diagnostic> {
    // When pushdown is globally disabled by a user primitive operator impl,
    // codegen evaluates every query in memory; the pushdown lints no longer
    // describe real behavior, so emit nothing rather than diverge from codegen.
    if pushdown_disabled_by_user_impls(module) {
        return Vec::new();
    }

    let views: HashSet<&str> = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            DeclKind::View { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();

    // Collect top-level function bodies so the pipe-chain lint can mirror
    // codegen's beta-reduction: a filter/aggregate lambda that calls a user
    // function (`\i -> isGood i`) is inlined before codegen's SQL-pushdown
    // check, so the lint must inline it too before deciding runtime fallback.
    let fun_bodies: HashMap<String, Expr> = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            DeclKind::Fun { name, body: Some(body), .. } => Some((name.clone(), body.clone())),
            _ => None,
        })
        .collect();

    let mut diags = Vec::new();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &fun_bodies, &mut diags);
            }
            DeclKind::Derived { body, .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &fun_bodies, &mut diags);
            }
            DeclKind::View { body, .. } => {
                lint_expr(body, &type_env.source_schemas, &views, &fun_bodies, &mut diags);
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
    fun_bodies: &HashMap<String, Expr>,
    diags: &mut Vec<Diagnostic>,
) {
    match &expr.node {
        ExprKind::Set { target, value } => {
            if let ExprKind::SourceRef(name) = &target.node {
                if let Some(schema) = source_schemas.get(name) {
                    lint_set_expr(name, schema, value, source_schemas, views, diags);
                }
                lint_expr(target, source_schemas, views, fun_bodies, diags);
                // Recurse into the value so sub-expressions referencing
                // OTHER sources (e.g. `set *a = (*b |> filter f)`) are still
                // linted. For a top-level do-block value, lint_set_expr
                // already provides the more specific diagnostics for where
                // clauses on binds from the target source itself (missed
                // DELETE WHERE / UPDATE WHERE), so the generic where-pushdown
                // lint skips that source to avoid double-reporting — but
                // still covers binds from other sources and all nested
                // sub-expressions.
                if let ExprKind::Do(stmts) = &value.node {
                    lint_do_block_skipping(stmts, Some(name), source_schemas, views, diags);
                    for stmt in stmts {
                        lint_stmt(stmt, source_schemas, views, fun_bodies, diags);
                    }
                } else {
                    lint_expr(value, source_schemas, views, fun_bodies, diags);
                }
            } else {
                lint_expr(target, source_schemas, views, fun_bodies, diags);
                lint_expr(value, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::ReplaceSet { target, value } => {
            lint_expr(target, source_schemas, views, fun_bodies, diags);
            lint_expr(value, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Do(stmts) => {
            lint_do_block(stmts, source_schemas, views, diags);
            for stmt in stmts {
                lint_stmt(stmt, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs,
            rhs,
        } => {
            let chain_covered = lint_pipe_chain(expr, source_schemas, views, fun_bodies, diags);
            // When `lint_pipe_chain` covers the chain it flattens the entire
            // thing (walking `lhs` through every nested `BinOp::Pipe`) and
            // emits one diagnostic per failing operation. In that case
            // recursing into `lhs` via `lint_expr` would re-enter this arm and
            // re-lint the sub-chain, producing duplicate diagnostics — so skip
            // `lhs` when it is itself a pipe (already covered by the flatten).
            // But when `lint_pipe_chain` bails (flatten fails, non-source head,
            // view/ADT schema) it lints nothing; skipping `lhs` there would
            // leave the inner sub-chain and its middle-stage lambda bodies
            // entirely unlinted, so recurse into `lhs` to lint it generically.
            let lhs_is_pipe = matches!(&lhs.node, ExprKind::BinOp { op: BinOp::Pipe, .. });
            if !(chain_covered && lhs_is_pipe) {
                lint_expr(lhs, source_schemas, views, fun_bodies, diags);
            }
            lint_expr(rhs, source_schemas, views, fun_bodies, diags);
        }
        // Recurse into all sub-expressions
        ExprKind::BinOp { lhs, rhs, .. } => {
            lint_expr(lhs, source_schemas, views, fun_bodies, diags);
            lint_expr(rhs, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::UnaryOp { operand, .. } => {
            lint_expr(operand, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::App { func, arg } => {
            lint_app_form(func, arg, source_schemas, views, fun_bodies, diags);
            lint_expr(func, source_schemas, views, fun_bodies, diags);
            lint_expr(arg, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::With { record, body } => {
            lint_expr(record, source_schemas, views, fun_bodies, diags);
            lint_expr(body, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            lint_expr(cond, source_schemas, views, fun_bodies, diags);
            lint_expr(then_branch, source_schemas, views, fun_bodies, diags);
            lint_expr(else_branch, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Lambda { body, .. } => {
            lint_expr(body, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Case { scrutinee, arms } => {
            lint_expr(scrutinee, source_schemas, views, fun_bodies, diags);
            for arm in arms {
                lint_expr(&arm.body, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::Record(fields) => {
            for f in fields {
                lint_expr(&f.value, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            lint_expr(base, source_schemas, views, fun_bodies, diags);
            for f in fields {
                lint_expr(&f.value, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::List(elems) => {
            for e in elems {
                lint_expr(e, source_schemas, views, fun_bodies, diags);
            }
        }
        ExprKind::FieldAccess { expr: inner, .. } => {
            lint_expr(inner, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Atomic(inner) => {
            lint_expr(inner, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Annot { expr: inner, .. } => {
            lint_expr(inner, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::TimeUnitLit { value, .. } => {
            lint_expr(value, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Refine(inner) => {
            lint_expr(inner, source_schemas, views, fun_bodies, diags);
        }
        ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                lint_expr(&h.body, source_schemas, views, fun_bodies, diags);
            }
        }
        // Terminals — nothing to recurse into
        ExprKind::Lit(_)
        | ExprKind::Var(_)
        | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_)
        | ExprKind::DerivedRef(_) => {}
        ExprKind::TypeCtor { .. } => {}
    }
}

fn lint_stmt(
    stmt: &Stmt,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    fun_bodies: &HashMap<String, Expr>,
    diags: &mut Vec<Diagnostic>,
) {
    match &stmt.node {
        StmtKind::Bind { expr, .. } => lint_expr(expr, source_schemas, views, fun_bodies, diags),
        StmtKind::Where { cond } => lint_expr(cond, source_schemas, views, fun_bodies, diags),
        StmtKind::Expr(e) => lint_expr(e, source_schemas, views, fun_bodies, diags),
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
    lint_do_block_skipping(stmts, None, source_schemas, views, diags);
}

/// Like [`lint_do_block`], but binds from `skip_source` are not checked.
/// Used for `set *src = do { ... }` values, where `lint_set_expr` already
/// reports where-clause misses on the target source with more specific
/// messages (DELETE WHERE / UPDATE WHERE) — re-checking them here would
/// double-report.
fn lint_do_block_skipping(
    stmts: &[Stmt],
    skip_source: Option<&str>,
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
            if skip_source == Some(source_name.as_str()) {
                continue;
            }
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
                        StmtKind::Bind { .. } | StmtKind::GroupBy { .. }
                    )
                })
                .map_or(stmts.len(), |p| i + 1 + p);

            for stmt in &stmts[i + 1..search_end] {
                if let StmtKind::Where { cond } = &stmt.node
                    && try_compile_sql_expr(bind_var, cond, schema).is_none() {
                        diags.push(
                            Diagnostic::info("where clause will be evaluated at runtime")
                                .label(
                                    stmt.span,
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
        let where_ok = try_compile_sql_expr(&bind_var, cond, schema).is_some();
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
            if try_compile_sql_expr(&bind_var, cond, schema).is_none() {
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
/// Lint a pipe chain for SQL-pushdown fallbacks. Returns `true` when the chain
/// was actually covered — i.e. it flattened to a plain-source head and either
/// walked every operation or emitted the operation-order diagnostic. Returns
/// `false` on every bail path (flatten fails, non-source head, view/ADT/nested
/// schema): in those cases nothing was linted, so the caller must fall back to
/// recursing into the sub-chain generically or its diagnostics are lost.
fn lint_pipe_chain(
    expr: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    fun_bodies: &HashMap<String, Expr>,
    diags: &mut Vec<Diagnostic>,
) -> bool {
    let (source, ops) = match flatten_pipe_chain(expr) {
        Some(v) => v,
        None => return false,
    };
    if ops.is_empty() {
        return false;
    }

    let source_name = match &source.node {
        ExprKind::SourceRef(name) => name,
        _ => return false,
    };
    if views.contains(source_name.as_str()) {
        return false;
    }
    let schema = match source_schemas.get(source_name) {
        Some(s) => s,
        None => return false,
    };
    if schema.starts_with('#') || schema.contains('[') {
        return false;
    }

    // Mirror codegen's pipe operation-order check: out-of-order chains
    // (e.g. `take 5 |> drop 2`, `take 3 |> filter f`) cannot be collapsed
    // into one SQL query and fall back to runtime evaluation entirely.
    if !lint_pipe_order_pushable(&ops) {
        diags.push(
            Diagnostic::info(
                "pipe chain will be evaluated at runtime instead of SQL",
            )
            .label(
                expr.span,
                "operation order doesn't match SQL clause order \
                 (filter, then sortBy, then map, then drop, then take)",
            ),
        );
        // The whole flattened chain was analyzed and flagged as one unit.
        return true;
    }

    // Mirror codegen's beta-reduction before its SQL-pushdown check: codegen
    // fully inlines a filter/aggregate lambda (expanding calls to user
    // functions like `\i -> isGood i`) before deciding whether the operation
    // compiles to SQL. Reduce the body here too so the lint's runtime-fallback
    // claims match what codegen actually emits — otherwise a lambda that calls
    // a user function is wrongly flagged as runtime-evaluated even though its
    // inlined body is a plain column comparison that pushes down to SQL.
    // Pipe-op bodies live at module scope, so there are no `let` bindings in
    // play; an empty map is the correct binding environment.
    let no_lets: HashMap<String, Expr> = HashMap::new();

    for op in &ops {
        match op {
            LintPipeOp::Filter { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_compile_sql_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe filter will be evaluated at runtime instead of SQL WHERE",
                        )
                        .label(*span, "filter lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::SortBy { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_sql_sortby_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe sortBy will be evaluated at runtime instead of SQL ORDER BY",
                        )
                        .label(*span, "sortBy lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Sum { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_sql_column_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe sum will be evaluated at runtime instead of SQL SUM",
                        )
                        .label(*span, "sum lambda cannot be compiled to SQL"),
                    );
                }
            }
            // Direct `sum` has no projection lambda to reject; it pushes down
            // whenever the preceding `map` did.
            LintPipeOp::SumDirect { .. } => {}
            LintPipeOp::Avg { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_sql_column_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe avg will be evaluated at runtime instead of SQL AVG",
                        )
                        .label(*span, "avg lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Min { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_sql_minmax_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe minOn will be evaluated at runtime instead of SQL MIN",
                        )
                        .label(*span, "minOn lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::Max { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_sql_minmax_expr(bind_var, &reduced, schema).is_none() {
                    diags.push(
                        Diagnostic::info(
                            "pipe maxOn will be evaluated at runtime instead of SQL MAX",
                        )
                        .label(*span, "maxOn lambda cannot be compiled to SQL"),
                    );
                }
            }
            LintPipeOp::CountWhere { bind_var, body, span } => {
                let reduced = beta_reduce(body, fun_bodies, &no_lets);
                if try_compile_sql_expr(bind_var, &reduced, schema).is_none() {
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
    // Every operation in the flattened chain was linted above.
    true
}

// ── Application form lint ──────────────────────────────────────────

/// Check `filter f *source`, `sum f *source`, etc. for SQL fallback.
fn lint_app_form(
    func: &Expr,
    arg: &Expr,
    source_schemas: &HashMap<String, String>,
    views: &HashSet<&str>,
    fun_bodies: &HashMap<String, Expr>,
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
        // Beta-reduce the lambda body so inlined function calls (e.g.
        // `filter (\i -> isGood i) *source` where `isGood = \x -> x.score > 50`)
        // are correctly recognized as SQL-compilable, mirroring codegen's
        // `extract_single_param_lambda` and `lint_pipe_chain`.
        let no_lets: HashMap<String, Expr> = HashMap::new();
        let reduced = beta_reduce(body, fun_bodies, &no_lets);

        match fn_name {
            "filter"
                if try_compile_sql_expr(&bind_var, &reduced, schema).is_none() => {
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
            "sortBy"
                if try_sql_sortby_expr(&bind_var, &reduced, schema).is_none() => {
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
            "sum"
                if try_sql_column_expr(&bind_var, &reduced, schema).is_none() => {
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
            "avg"
                if try_sql_column_expr(&bind_var, &reduced, schema).is_none() => {
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
            "minOn"
                if try_sql_minmax_expr(&bind_var, &reduced, schema).is_none() => {
                    diags.push(
                        Diagnostic::info(
                            "minOn will be evaluated at runtime instead of SQL MIN",
                        )
                        .label(
                            lambda_arg.span,
                            "minOn lambda cannot be compiled to SQL",
                        ),
                    );
                }
            "maxOn"
                if try_sql_minmax_expr(&bind_var, &reduced, schema).is_none() => {
                    diags.push(
                        Diagnostic::info(
                            "maxOn will be evaluated at runtime instead of SQL MAX",
                        )
                        .label(
                            lambda_arg.span,
                            "maxOn lambda cannot be compiled to SQL",
                        ),
                    );
                }
            "countWhere"
                if try_compile_sql_expr(&bind_var, &reduced, schema).is_none() => {
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
            _ => {}
        }
    }
}

// ── SQL expression compilability check ─────────────────────────────
// Mirrors the logic in codegen.rs `try_compile_sql_expr` but without
// any Cranelift dependencies.

fn try_compile_sql_expr(bind_var: &str, expr: &Expr, schema: &str) -> Option<()> {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                try_compile_sql_expr(bind_var, lhs, schema)?;
                try_compile_sql_expr(bind_var, rhs, schema)?;
                Some(())
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                // Try simple field op value, then atom-based (handles arithmetic)
                try_sql_comparison(bind_var, lhs, rhs, op, schema)
                    .or_else(|| {
                        let rev = match op {
                            BinOp::Eq | BinOp::Neq => *op,
                            BinOp::Lt => BinOp::Gt,
                            BinOp::Gt => BinOp::Lt,
                            BinOp::Le => BinOp::Ge,
                            BinOp::Ge => BinOp::Le,
                            _ => *op,
                        };
                        try_sql_comparison(bind_var, rhs, lhs, &rev, schema)
                    })
                    .or_else(|| {
                        // Mirror codegen's type-witness gate: arithmetic
                        // comparisons only push down when they can be typed
                        // (int → numeric-cast SQL, float → in-memory);
                        // untypable arithmetic falls back to runtime.
                        let col_ty = |v: &str, f: &str| {
                            if v == bind_var {
                                lookup_col_type(schema, f)
                            } else {
                                None
                            }
                        };
                        let mode = sql_comparison_cast_mode(lhs, rhs, &col_ty)?;
                        // Ordered comparisons on tag columns stay in memory
                        // (byte-wise name order ≠ Ord) — mirror codegen.
                        if matches!(op, BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge)
                            && (expr_has_tag_column(lhs, &col_ty)
                                || expr_has_tag_column(rhs, &col_ty))
                        {
                            return None;
                        }
                        if matches!(mode, SqlCastMode::NoArith)
                            && (atom_would_need_cast(lhs) || atom_would_need_cast(rhs))
                        {
                            return None;
                        }
                        try_sql_atom(bind_var, lhs)?;
                        try_sql_atom(bind_var, rhs)
                    })
            }
            _ => None,
        },
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => try_compile_sql_expr(bind_var, operand, schema),
        // `not expr` function application → NOT (...)
        // `contains needle haystack` → INSTR(haystack, needle) > 0
        ExprKind::App { func, arg } => {
            if let ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                && let ExprKind::Var(name) = &inner_func.node {
                    if name == "contains" {
                        try_sql_atom(bind_var, first_arg)?;
                        return try_sql_atom(bind_var, arg);
                    }
                    if name == "elem" {
                        // Mirror codegen: `IN` is equality under the hood —
                        // float equality stays in memory.
                        if let ExprKind::FieldAccess { expr: fa, field } = &first_arg.node
                            && matches!(&fa.node, ExprKind::Var(v) if v == bind_var)
                                && lookup_col_type(schema, field).as_deref() == Some("float")
                            {
                                return None;
                            }
                        try_sql_atom(bind_var, first_arg)?;
                        // Literal list: each element must be a sql-pushable atom
                        // (codegen emits `IN (?, ?, ...)`).
                        if let ExprKind::List(elems) = &arg.node {
                            for e in elems {
                                try_sql_atom(bind_var, e)?;
                            }
                            return Some(());
                        }
                        // Dynamic haystack: codegen also pushes this down via
                        // `IN (SELECT value FROM json_each(?))` as long as the
                        // haystack doesn't reference the row variable (it is
                        // evaluated outside the SQL row scope) and inference
                        // proves the element type is a SQL scalar. The lint
                        // has no type info, so mirror the syntactic half and
                        // stay quiet otherwise — for an informational lint a
                        // false negative beats warning about a query that
                        // does get pushed down.
                        if !expr_refs_var(arg, bind_var) {
                            return Some(());
                        }
                        return None;
                    }
                }
            None
        }
        _ => None,
    }
}

/// Check if `field_side op value_side` can be a SQL comparison.
fn try_sql_comparison(
    bind_var: &str,
    field_side: &Expr,
    value_side: &Expr,
    op: &BinOp,
    schema: &str,
) -> Option<()> {
    // field_side must be bind_var.field
    if let ExprKind::FieldAccess { expr, field } = &field_side.node {
        if let ExprKind::Var(name) = &expr.node {
            if name != bind_var {
                return None;
            }
            let col_ty = lookup_col_type(schema, field);
            // Mirror codegen: json columns (payload-bearing ADTs, nested
            // records) are never pushed down — the runtime binds the
            // compared value differently than it is stored.
            if col_ty.as_deref() == Some("json") {
                return None;
            }
            // Float comparisons stay in memory (total_cmp vs SQL
            // -0.0/NaN-as-NULL semantics) — mirror codegen.
            if col_ty.as_deref() == Some("float") {
                return None;
            }
            // Ordered comparisons on tag columns stay in memory
            // (byte-wise constructor-name order ≠ Ord) — mirror codegen.
            if col_ty.as_deref() == Some("tag")
                && matches!(op, BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge)
            {
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
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Concat => {
                    try_sql_atom(bind_var, lhs)?;
                    try_sql_atom(bind_var, rhs)
                }
                // Mirror codegen: `/`/`%` only push down with a provably
                // nonzero literal divisor (SQLite NULLs on division by zero
                // while the runtime panics); `%` must be integer-typed.
                BinOp::Div if divisor_is_nonzero_literal(rhs) => {
                    try_sql_atom(bind_var, lhs)?;
                    try_sql_atom(bind_var, rhs)
                }
                BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => {
                    try_sql_atom(bind_var, lhs)?;
                    try_sql_atom(bind_var, rhs)
                }
                _ => None,
            }
        }
        // Built-in functions (toUpper/toLower are NOT pushed
        // down — SQLite UPPER/LOWER are ASCII-only, the runtime is
        // Unicode-aware; trim likewise: SQLite TRIM strips ASCII spaces
        // only while the runtime trims all Unicode whitespace; length
        // likewise: SQLite LENGTH() counts chars before the first NUL
        // byte while knot_text_length counts all chars)
        ExprKind::App { func, .. } => {
            // No built-in functions are pushed down — always fall
            // through to the non-pushable case.
            if let ExprKind::Var(_) = &func.node {
                None
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

/// Mirror of codegen's `cast_arithmetic_for_where` applicability: an atom
/// would receive the KNOT_INT text-cast when it compiles to a parenthesized
/// arithmetic expression. (LENGTH() is no longer pushed down, so App
/// expressions never need the cast here.)
fn atom_would_need_cast(expr: &Expr) -> bool {
    matches!(&expr.node, ExprKind::BinOp {
        op: BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Concat,
        ..
    })
}

/// Look up a column's schema type. Delegates to codegen's
/// `lookup_col_type_from_schema` so the lint can never diverge from the
/// compiler's actual pushdown behavior — that function is bracket-aware
/// (handling nested relation fields like `items:[price:int,qty:int]`) and
/// recognizes ADT-relation schemas prefixed with `#`.
fn lookup_col_type(schema: &str, col_name: &str) -> Option<String> {
    lookup_col_type_from_schema(schema, col_name)
}

// `expr_refs_var` is shared with codegen (crate::codegen::expr_refs_var) so
// the lint can never diverge from the compiler's actual pushdown behavior.

// ── Pattern matchers (mirror codegen.rs) ───────────────────────────

/// Matched conditional-update shape: the condition expression, the update
/// value expression, and the `(column, value-expr)` field assignments.
type ConditionalUpdate<'a> = (String, &'a Expr, Vec<(&'a str, &'a Expr)>);

fn match_conditional_update<'a>(
    source_name: &str,
    value: &'a Expr,
) -> Option<ConditionalUpdate<'a>> {
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

    if let StmtKind::Expr(e) = &stmts[1].node
        && let Some(yield_inner) = e.node.as_yield_arg()
            && let ExprKind::If {
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
        ExprKind::TypeCtor { .. } => false,
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
        ExprKind::With { record, body } => {
            references_source(record, source_name) || references_source(body, source_name)
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
        ExprKind::TimeUnitLit { value, .. } => references_source(value, source_name),
        ExprKind::Serve { handlers, .. } => {
            handlers.iter().any(|h| references_source(&h.body, source_name))
        }
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
    /// Direct `rel |> sum` (no projection). The summand is the relation's own
    /// (already-mapped) element, so there's no lambda to inspect — it always
    /// pushes down when the preceding map did.
    SumDirect {
        #[allow(dead_code)]
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

    while let ExprKind::BinOp {
        op: BinOp::Pipe,
        lhs,
        rhs,
    } = &current.node
    {
        if let Some(pipe_op) = analyze_pipe_op(rhs) {
            ops.push(pipe_op);
        } else {
            return None;
        }
        current = lhs;
    }

    ops.reverse();
    Some((current, ops))
}

fn analyze_pipe_op(expr: &Expr) -> Option<LintPipeOp<'_>> {
    match &expr.node {
        ExprKind::Var(name) if name == "count" => Some(LintPipeOp::Count { span: expr.span }),
        ExprKind::Var(name) if name == "sum" => Some(LintPipeOp::SumDirect { span: expr.span }),
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
                    "avg" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Avg {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "minOn" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        LintPipeOp::Min {
                            bind_var,
                            body,
                            span: arg.span,
                        }
                    }),
                    "maxOn" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
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
    if let ExprKind::Lambda { params, body, .. } = &expr.node
        && params.len() == 1
            && let PatKind::Var(name) = &params[0].node {
                return Some((name.clone(), body));
            }
    None
}

/// Mirror of codegen's minOn/maxOn pushdown approval: the lambda body must
/// compile to a SQL column expression AND its Knot type must be Int or Text
/// (Float MIN/MAX diverges from `total_cmp`, so it stays in memory). The
/// MIN/MAX runtime is told whether the result column is textual (`is_text`)
/// so Text results are returned verbatim instead of being re-parsed as Int.
/// Int-typed if/then/else projections are also rejected (MIN/MAX over CASE
/// loses the KNOT_INT collation) — that check lives in
/// `minmax_pushdown_type_ok`.
fn try_sql_minmax_expr(bind_var: &str, body: &Expr, schema: &str) -> Option<()> {
    try_sql_column_expr(bind_var, body, schema)?;
    if minmax_pushdown_type_ok(bind_var, body, schema) {
        Some(())
    } else {
        None
    }
}

/// Mirror of codegen's sortBy pushdown approval: the lambda body must
/// compile to a SQL column expression, Int-typed if/then/else projections
/// are rejected (ORDER BY CASE loses the KNOT_INT collation), and float-typed
/// projections are rejected (SQLite REAL ordering diverges from Knot's
/// `total_cmp`). Uses `sortby_projection_pushable` — the same predicate codegen
/// applies — so this lint's "evaluated at runtime" info matches what actually
/// happens (previously it used `int_case_projection_pushable`, missing the
/// float rejection and falsely believing float sorts pushed down).
fn try_sql_sortby_expr(bind_var: &str, body: &Expr, schema: &str) -> Option<()> {
    try_sql_column_expr(bind_var, body, schema)?;
    if sortby_projection_pushable(bind_var, body, schema) {
        Some(())
    } else {
        None
    }
}

/// Check if a lambda body can be compiled to a SQL expression.
/// Mirrors codegen's `extract_sql_field_access` which handles simple field access,
/// arithmetic expressions (including ++), CASE WHEN, and built-in functions.
fn try_sql_column_expr(bind_var: &str, body: &Expr, schema: &str) -> Option<()> {
    match &body.node {
        ExprKind::FieldAccess { expr, .. } => {
            if let ExprKind::Var(name) = &expr.node
                && name == bind_var { return Some(()); }
            None
        }
        ExprKind::Lit(
            Literal::Int(_) | Literal::Float(_) | Literal::Text(_) | Literal::Bool(_),
        ) => Some(()),
        ExprKind::Lit(_) => None,
        ExprKind::BinOp { op, lhs, rhs } => {
            match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Concat => {
                    try_sql_column_expr(bind_var, lhs, schema)?;
                    try_sql_column_expr(bind_var, rhs, schema)
                }
                // Mirror codegen: `/`/`%` only push down with a provably
                // nonzero literal divisor; `%` must be integer-typed.
                BinOp::Div if divisor_is_nonzero_literal(rhs) => {
                    try_sql_column_expr(bind_var, lhs, schema)?;
                    try_sql_column_expr(bind_var, rhs, schema)
                }
                BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => {
                    try_sql_column_expr(bind_var, lhs, schema)?;
                    try_sql_column_expr(bind_var, rhs, schema)
                }
                _ => None,
            }
        }
        ExprKind::If { cond, then_branch, else_branch } => {
            try_sql_inline_cond(bind_var, cond, schema)?;
            try_sql_column_expr(bind_var, then_branch, schema)?;
            try_sql_column_expr(bind_var, else_branch, schema)
        }
        // toUpper/toLower are NOT pushed down (ASCII-only in SQLite);
        // trim likewise (SQLite TRIM strips ASCII spaces only, the
        // runtime trims all Unicode whitespace); length likewise
        // (SQLite LENGTH() counts chars before the first NUL byte,
        // while knot_text_length counts all chars).
        ExprKind::App { func, .. } => {
            // No built-in functions are pushed down.
            if let ExprKind::Var(_) = &func.node {
                None
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Mirror of codegen's `pipe_ops_order_pushable`: stages must be
/// non-decreasing (filter=1, sortBy=2, map=3, drop=4, take=5, aggregate=6),
/// at most one sortBy, and no aggregate after take/drop.
fn lint_pipe_order_pushable(ops: &[LintPipeOp]) -> bool {
    fn stage(op: &LintPipeOp) -> u8 {
        match op {
            LintPipeOp::Filter { .. } => 1,
            LintPipeOp::SortBy { .. } => 2,
            LintPipeOp::Map { .. } => 3,
            LintPipeOp::Drop { .. } => 4,
            LintPipeOp::Take { .. } => 5,
            LintPipeOp::Count { .. }
            | LintPipeOp::CountWhere { .. }
            | LintPipeOp::Sum { .. }
            | LintPipeOp::SumDirect { .. }
            | LintPipeOp::Avg { .. }
            | LintPipeOp::Min { .. }
            | LintPipeOp::Max { .. } => 6,
        }
    }
    let mut last_stage = 0u8;
    let mut sort_seen = false;
    for op in ops {
        let st = stage(op);
        if st < last_stage {
            return false;
        }
        if matches!(op, LintPipeOp::SortBy { .. }) {
            if sort_seen {
                return false;
            }
            sort_seen = true;
        }
        if st == 6 && last_stage >= 4 {
            return false;
        }
        last_stage = st;
    }
    true
}

/// Check if a condition can be compiled to an inline SQL condition (for CASE WHEN).
fn try_sql_inline_cond(bind_var: &str, expr: &Expr, schema: &str) -> Option<()> {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                try_sql_inline_cond(bind_var, lhs, schema)?;
                try_sql_inline_cond(bind_var, rhs, schema)
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                // Mirror codegen's inline-condition gates: float
                // comparisons stay in memory (total_cmp vs SQL
                // -0.0/NaN-as-NULL); ordered comparisons on tag columns
                // ignore the type's Ord.
                let lt = infer_sql_expr_type(bind_var, lhs, schema);
                let rt = infer_sql_expr_type(bind_var, rhs, schema);
                if lt.as_deref() == Some("float") || rt.as_deref() == Some("float") {
                    return None;
                }
                // json-stored columns (ADT payloads / nested records) compare
                // as raw JSON text in SQL, which can diverge from Knot's
                // structural equality. Codegen's `try_sql_inline_condition`
                // refuses to push these; mirror that here so the lint doesn't
                // claim a query is pushed down when codegen keeps it in memory.
                if lt.as_deref() == Some("json") || rt.as_deref() == Some("json") {
                    return None;
                }
                if matches!(op, BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge)
                    && (lt.as_deref() == Some("tag") || rt.as_deref() == Some("tag"))
                {
                    return None;
                }
                try_sql_column_expr(bind_var, lhs, schema)?;
                try_sql_column_expr(bind_var, rhs, schema)
            }
            _ => None,
        },
        ExprKind::UnaryOp { op: UnaryOp::Not, operand } => {
            try_sql_inline_cond(bind_var, operand, schema)
        }
        ExprKind::App { func, arg } => {
            if let ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                && let ExprKind::Var(name) = &inner_func.node {
                    if name == "contains" {
                        try_sql_column_expr(bind_var, first_arg, schema)?;
                        return try_sql_column_expr(bind_var, arg, schema);
                    }
                    if name == "elem" {
                        // Mirror codegen: float `IN` equality stays in memory.
                        if infer_sql_expr_type(bind_var, first_arg, schema).as_deref()
                            == Some("float")
                        {
                            return None;
                        }
                        try_sql_column_expr(bind_var, first_arg, schema)?;
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
            "type T = {name: Text, age: Int 1}\n\
             *people : [T]\n\
             main = do\n  \
               p <- *people\n  \
               where p.age > 30\n  \
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_length_in_where() {
        // length(p.name) is NOT pushed down to SQL LENGTH() because SQLite
        // LENGTH() counts chars before the first NUL byte while
        // knot_text_length counts all chars. The where clause falls back
        // to runtime evaluation.
        let diags = lint(
            "type T = {name: Text, age: Int 1}\n\
             *people : [T]\n\
             main = do\n  \
               p <- *people\n  \
               where length p.name > 3\n  \
               yield p\n",
        );
        assert!(!diags.is_empty(), "expected a diagnostic for non-pushable length, got none");
    }

    #[test]
    fn no_lint_on_contains_in_update() {
        // contains now compiles to SQL INSTR().
        let diags = lint(
            "type T = {name: Text, active: Int 1}\n\
             *items : [T]\n\
             process = \\target -> do\n  \
               *items = do\n    \
                 i <- *items\n    \
                 yield (if contains target i.name\n      \
                   then {i | active: 0}\n      \
                   else i)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_unknown_function_in_where() {
        // Custom function calls still can't be SQL-compiled.
        let diags = lint(
            "type T = {name: Text, age: Int 1}\n\
             *people : [T]\n\
             isLong = \\t -> length t > 10\n\
             main = do\n  \
               p <- *people\n  \
               where isLong p.name\n  \
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
            "type T = {name: Text, active: Int 1}\n\
             *items : [T]\n\
             process = \\target -> do\n  \
               *items = do\n    \
                 i <- *items\n    \
                 yield (if i.name == target\n      \
                   then {i | active: 0}\n      \
                   else i)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_set_filter_complex_cond() {
        // Filter with function call — can't be SQL DELETE WHERE.
        let diags = lint(
            "type T = {name: Text, score: Int 1}\n\
             isGood = \\x -> x > 50\n\
             *items : [T]\n\
             cleanup = do\n  \
               *items = do\n    \
                 i <- *items\n    \
                 where isGood i.score\n    \
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
            "type T = {name: Text, score: Int 1}\n\
             *items : [T]\n\
             cleanup = do\n  \
               *items = do\n    \
                 i <- *items\n    \
                 where i.score > 50\n    \
                 yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_set_value_pipe_over_other_source() {
        // `set *a = (*b |> filter complex)` — the value's pipe chain over a
        // DIFFERENT source must still be linted (previously the Set arm never
        // recursed into the value). The filter uses `toUpper`, which codegen
        // never pushes down (SQLite UPPER is ASCII-only), so this stays a
        // genuine runtime-fallback case even after beta-reduction.
        let diags = lint(
            "type T = {name: Text, score: Int 1}\n\
             *a : [T]\n\
             *b : [T]\n\
             sync = do\n  \
               *a = (*b |> filter (\\i -> toUpper i.name == \"BOB\"))\n",
        );
        assert!(
            !diags.is_empty(),
            "expected diagnostic for non-SQL filter over other source in set value"
        );
        assert!(diags.iter().any(|d| d.message.contains("runtime")));
    }

    #[test]
    fn lint_on_set_value_do_over_other_source() {
        // A set value do-block binding from a DIFFERENT source gets the
        // where-pushdown lint exactly once (no lint_set_expr overlap).
        let diags = lint(
            "type T = {name: Text, score: Int 1}\n\
             isGood = \\x -> x > 50\n\
             *a : [T]\n\
             *b : [T]\n\
             move = do\n  \
               *a = do\n    \
                 x <- *b\n    \
                 where isGood x.score\n    \
                 yield x\n",
        );
        assert_eq!(
            diags.len(),
            1,
            "expected exactly one diagnostic, got: {:?}",
            diags
        );
        assert!(diags[0].message.contains("runtime"));
    }

    #[test]
    fn lint_on_pipe_filter_complex() {
        // Pipe filter calling a user function — codegen beta-reduces
        // `\i -> isGood i` to `i.score > 50`, which pushes down to SQL WHERE.
        // The lint mirrors that inlining, so it must NOT warn about runtime
        // fallback (regression for B55).
        let diags = lint(
            "type T = {name: Text, score: Int 1}\n\
             isGood = \\x -> x.score > 50\n\
             *items : [T]\n\
             main = do\n  \
               yield (*items |> filter (\\i -> isGood i))\n",
        );
        assert!(
            diags.is_empty(),
            "expected no diagnostics — inlined filter pushes down to SQL, got: {:?}",
            diags
        );
    }

    #[test]
    fn no_lint_on_pipe_filter_simple() {
        // Simple field comparison — SQL WHERE works.
        let diags = lint(
            "type T = {name: Text, score: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
               yield (*items |> filter (\\i -> i.score > 50))\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_contains_in_where() {
        // contains compiles to SQL INSTR().
        let diags = lint(
            "type T = {name: Text, age: Int 1}\n\
             *people : [T]\n\
             main = do\n  \
               p <- *people\n  \
               where contains \"test\" p.name\n  \
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
             main = do\n  \
               i <- *items\n  \
               where elem i.status [\"open\", \"pending\"]\n  \
               yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_elem_non_literal_list_in_where() {
        // elem against a dynamic haystack that doesn't reference the row
        // variable IS pushed down by codegen via
        // `IN (SELECT value FROM json_each(?))` — no warning.
        let diags = lint(
            "type T = {name: Text, status: Text}\n\
             *items : [T]\n\
             allowed = [\"open\", \"pending\"]\n\
             main = do\n  \
               i <- *items\n  \
               where elem i.status allowed\n  \
               yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_elem_haystack_referencing_bind_var() {
        // A dynamic haystack that references the row variable cannot be
        // bound as a single SQL parameter — codegen falls back to runtime,
        // so the lint warns.
        let diags = lint(
            "type T = {name: Text, status: Text}\n\
             pick = \\x -> [x]\n\
             *items : [T]\n\
             main = do\n  \
               i <- *items\n  \
               where elem i.status (pick i.name)\n  \
               yield i\n",
        );
        assert!(
            !diags.is_empty(),
            "expected diagnostic for haystack referencing the bind var"
        );
        assert!(diags.iter().any(|d| d.message.contains("runtime")));
    }

    #[test]
    fn lint_on_toupper_in_where() {
        // toUpper is NOT pushed down to SQL UPPER(): SQLite UPPER is
        // ASCII-only while the runtime does Unicode case mapping, so the
        // where clause is evaluated at runtime (and the lint reports it).
        let diags = lint(
            "type T = {name: Text, age: Int 1}\n\
             *people : [T]\n\
             main = do\n  \
               p <- *people\n  \
               where toUpper p.name == \"ALICE\"\n  \
               yield p\n",
        );
        assert_eq!(diags.len(), 1, "expected runtime-fallback diagnostic, got: {:?}", diags);
        assert!(diags[0].message.contains("runtime"));
    }

    #[test]
    fn no_lint_on_not_function_in_where() {
        // `not` function compiles to SQL NOT.
        let diags = lint(
            "type T = {name: Text, active: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
               i <- *items\n  \
               where not (i.active == 1)\n  \
               yield i\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_arithmetic_sum() {
        // sum over an arithmetic map projection compiles to SQL SUM(col * col).
        let diags = lint(
            "type T = {price: Int 1, qty: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
               items <- *items\n  \
               yield (items |> map (\\i -> i.price * i.qty) |> sum)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_arithmetic_min_max() {
        // minOn/maxOn with field access compile to SQL MIN/MAX.
        let diags = lint(
            "type T = {salary: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
               items <- *items\n  \
               yield (minOn (\\i -> i.salary) items)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_complex_min_lambda() {
        // minOn calling a user function — codegen beta-reduces
        // `\i -> classify i.salary` to `i.salary + 100`, an Int column
        // arithmetic projection that pushes down to SQL MIN. The lint mirrors
        // that inlining, so it must NOT warn about runtime fallback
        // (regression for B55).
        let diags = lint(
            "type T = {salary: Int 1}\n\
             classify = \\x -> x + 100\n\
             *items : [T]\n\
             main = do\n  \
               yield (*items |> minOn (\\i -> classify i.salary))\n",
        );
        assert!(
            diags.is_empty(),
            "expected no diagnostics — inlined minOn pushes down to SQL, got: {:?}",
            diags
        );
    }

    #[test]
    fn no_lint_on_count_where_simple() {
        // countWhere with simple predicate compiles to SQL COUNT(*) WHERE.
        let diags = lint(
            "type T = {salary: Int 1, dept: Text}\n\
             *items : [T]\n\
             main = do\n  \
               items <- *items\n  \
               yield (countWhere (\\i -> i.salary > 75) items)\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn lint_on_count_where_complex() {
        // countWhere with a genuinely non-SQL-compilable predicate — `toUpper`
        // is not pushed down (SQLite UPPER is ASCII-only), so even after
        // beta-reduction the predicate falls back to runtime.
        let diags = lint(
            "type T = {salary: Int 1, name: Text}\n\
             *items : [T]\n\
             main = do\n  \
               yield (*items |> countWhere (\\i -> toUpper i.name == \"BOB\"))\n",
        );
        assert!(!diags.is_empty(), "expected diagnostic for non-SQL countWhere");
        assert!(diags.iter().any(|d| d.message.contains("COUNT")));
    }

    #[test]
    fn no_lint_on_pipe_min_max() {
        // Pipe forms `*items |> maxOn ...` compile to SQL MIN/MAX.
        let diags = lint(
            "type T = {salary: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
               yield (*items |> maxOn (\\i -> i.salary))\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn no_lint_on_pipe_count_where() {
        // Pipe form `*items |> countWhere pred` compiles to SQL COUNT(*) WHERE.
        let diags = lint(
            "type T = {salary: Int 1}\n\
             *items : [T]\n\
             main = do\n  \
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
             main = do\n  \
               p <- *people\n  \
               where p.first ++ \" \" ++ p.last == \"Alice Smith\"\n  \
               yield p\n",
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }
}
