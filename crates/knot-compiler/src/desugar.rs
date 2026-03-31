//! Do-block desugaring pass.
//!
//! Transforms "pure comprehension" do blocks into nested calls to
//! `__bind`, `yield`, and `[]` (empty). A pure comprehension is a do
//! block whose statements are all Bind, Where, Let, or a final Yield —
//! no bare side-effecting expressions like `set` or `println`.
//!
//! Desugaring rules (processing right-to-left):
//!
//! ```text
//! [yield e]                         =>  Yield(e)
//! [x <- e, ...rest]                 =>  App(App(__bind, \x -> desugar(rest)), e)
//! [Ctor pat <- e, ...rest]          =>  App(App(__bind, \__m -> case __m of
//!                                           Ctor pat -> desugar(rest)
//!                                           _        -> []), e)
//! [where cond, ...rest]             =>  App(App(__bind, \_ -> desugar(rest)),
//!                                           if cond then yield {} else [])
//! [let pat = e, ...rest]            =>  (\pat -> desugar(rest)) e
//! ```
//!
//! Do blocks that are direct values of Set/FullSet are NOT desugared
//! (to preserve SQL optimization patterns in codegen).

use knot::ast::*;
use std::collections::HashSet;

/// Desugar a module in place. Transforms pure-comprehension do blocks
/// into nested bind/yield/empty expressions, and routes into data declarations.
pub fn desugar(module: &mut Module) {
    desugar_routes(module);
    let io_fns = detect_io_functions(&module.decls);
    for decl in &mut module.decls {
        desugar_decl(&mut decl.node, &io_fns);
    }
}

/// Detect user functions whose bodies (transitively) produce IO values.
/// Uses fixed-point iteration to handle transitive IO (e.g., genToken calls randomInt).
fn detect_io_functions(decls: &[Decl]) -> HashSet<String> {
    let io_builtins: HashSet<&str> = [
        "println", "putLine", "print", "readLine", "readFile",
        "writeFile", "appendFile", "fileExists", "removeFile",
        "listDir", "now", "randomInt", "randomFloat", "fetch", "fetchWith",
        "fork", "listen", "generateKeyPair", "generateSigningKeyPair", "encrypt",
    ].into_iter().collect();

    let mut fun_bodies: Vec<(&str, &Expr)> = Vec::new();
    for decl in decls {
        if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
            fun_bodies.push((name, body));
        }
    }

    let mut io_fns = HashSet::new();
    loop {
        let mut changed = false;
        for (name, body) in &fun_bodies {
            if io_fns.contains(*name) {
                continue;
            }
            if expr_contains_io(body, &io_builtins, &io_fns) {
                io_fns.insert(name.to_string());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    io_fns
}

/// Check if an expression contains IO calls (builtins or known IO user functions).
fn expr_contains_io(expr: &Expr, builtins: &HashSet<&str>, io_fns: &HashSet<String>) -> bool {
    match &expr.node {
        ExprKind::Var(name) => builtins.contains(name.as_str()) || io_fns.contains(name.as_str()),
        ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => true,
        ExprKind::Set { .. } | ExprKind::FullSet { .. } => true,
        ExprKind::At { .. } | ExprKind::Atomic(_) => true,
        ExprKind::App { func, arg } => {
            expr_contains_io(func, builtins, io_fns)
                || expr_contains_io(arg, builtins, io_fns)
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_contains_io(lhs, builtins, io_fns)
                || expr_contains_io(rhs, builtins, io_fns)
        }
        ExprKind::UnaryOp { operand, .. } => {
            expr_contains_io(operand, builtins, io_fns)
        }
        ExprKind::Do(stmts) => {
            stmts.iter().any(|s| match &s.node {
                StmtKind::Bind { expr, .. } => expr_contains_io(expr, builtins, io_fns),
                StmtKind::Expr(expr) => expr_contains_io(expr, builtins, io_fns),
                StmtKind::Let { expr, .. } => expr_contains_io(expr, builtins, io_fns),
                StmtKind::Where { cond } => expr_contains_io(cond, builtins, io_fns),
                StmtKind::GroupBy { key } => expr_contains_io(key, builtins, io_fns),
            })
        }
        ExprKind::Lambda { body, .. } => expr_contains_io(body, builtins, io_fns),
        ExprKind::If { cond, then_branch, else_branch, .. } => {
            expr_contains_io(cond, builtins, io_fns)
                || expr_contains_io(then_branch, builtins, io_fns)
                || expr_contains_io(else_branch, builtins, io_fns)
        }
        ExprKind::Case { scrutinee, arms, .. } => {
            expr_contains_io(scrutinee, builtins, io_fns)
                || arms.iter().any(|arm| expr_contains_io(&arm.body, builtins, io_fns))
        }
        // Records, lists, field access are data constructors/accessors —
        // they don't produce IO even if they contain IO values as
        // subexpressions. Must match codegen's expr_contains_io to avoid
        // incorrectly preventing desugaring of pure comprehension do-blocks.
        ExprKind::Record(_)
        | ExprKind::RecordUpdate { .. }
        | ExprKind::FieldAccess { .. }
        | ExprKind::List(_) => false,
        ExprKind::Yield(inner) => expr_contains_io(inner, builtins, io_fns),
        _ => false,
    }
}

/// Generate `Data` declarations from `Route` and `RouteComposite` declarations.
/// The original route declarations are kept in place for codegen to extract HTTP metadata.
fn desugar_routes(module: &mut Module) {
    let mut new_decls: Vec<Decl> = Vec::new();

    // Collect route entries by name for RouteComposite lookup
    let route_map: std::collections::HashMap<&str, &Vec<RouteEntry>> = module
        .decls
        .iter()
        .filter_map(|d| {
            if let DeclKind::Route { name, entries } = &d.node {
                Some((name.as_str(), entries))
            } else {
                None
            }
        })
        .collect();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Route { name, entries } => {
                let ctors = route_entries_to_constructors(entries);
                new_decls.push(Decl {
                    node: DeclKind::Data {
                        name: name.clone(),
                        params: vec![],
                        constructors: ctors,
                        deriving: vec![],
                    },
                    span: decl.span,
                    exported: decl.exported,
                });
            }
            DeclKind::RouteComposite { name, components } => {
                let mut all_entries = Vec::new();
                for comp in components {
                    if let Some(entries) = route_map.get(comp.as_str()) {
                        all_entries.extend(entries.iter().cloned());
                    }
                }
                let ctors = route_entries_to_constructors(&all_entries);
                new_decls.push(Decl {
                    node: DeclKind::Data {
                        name: name.clone(),
                        params: vec![],
                        constructors: ctors,
                        deriving: vec![],
                    },
                    span: decl.span,
                    exported: decl.exported,
                });
            }
            _ => {}
        }
    }

    // Prepend synthetic data decls so they're available before route decls
    new_decls.append(&mut module.decls);
    module.decls = new_decls;
}

/// Convert route entries into constructor definitions.
/// All fields (path params, query params, body fields) are top-level constructor fields.
/// Routes with a response type get a `respond : ResponseType -> Response` field
/// that provides compile-time type safety for each handler branch.
fn route_entries_to_constructors(entries: &[RouteEntry]) -> Vec<ConstructorDef> {
    entries
        .iter()
        .map(|entry| {
            let mut fields: Vec<Field<Type>> = Vec::new();
            // Path params
            for seg in &entry.path {
                if let PathSegment::Param { name, ty } = seg {
                    fields.push(Field {
                        name: name.clone(),
                        value: ty.clone(),
                    });
                }
            }
            // Query params
            for qp in &entry.query_params {
                fields.push(Field {
                    name: qp.name.clone(),
                    value: qp.value.clone(),
                });
            }
            // Body fields — flat, same level as path/query params
            for bf in &entry.body_fields {
                fields.push(Field {
                    name: bf.name.clone(),
                    value: bf.value.clone(),
                });
            }
            // Request header fields — flat, same level as other params
            for hf in &entry.request_headers {
                fields.push(Field {
                    name: hf.name.clone(),
                    value: hf.value.clone(),
                });
            }
            // Add `respond` field if route has a response type.
            // With response headers: `respond : ResponseType -> {h1: T, ...} -> Response`
            // Without: `respond : ResponseType -> Response`
            if let Some(response_ty) = &entry.response_ty {
                let span = response_ty.span;
                let response_named = Spanned::new(
                    TypeKind::Named("Response".into()),
                    span,
                );
                let respond_ty = if entry.response_headers.is_empty() {
                    Spanned::new(
                        TypeKind::Function {
                            param: Box::new(response_ty.clone()),
                            result: Box::new(response_named),
                        },
                        span,
                    )
                } else {
                    // respond : ResponseType -> {h1: T, ...} -> Response
                    let headers_record = Spanned::new(
                        TypeKind::Record {
                            fields: entry.response_headers.clone(),
                            rest: None,
                        },
                        span,
                    );
                    Spanned::new(
                        TypeKind::Function {
                            param: Box::new(response_ty.clone()),
                            result: Box::new(Spanned::new(
                                TypeKind::Function {
                                    param: Box::new(headers_record),
                                    result: Box::new(response_named),
                                },
                                span,
                            )),
                        },
                        span,
                    )
                };
                fields.push(Field {
                    name: "respond".to_string(),
                    value: respond_ty,
                });
            }
            ConstructorDef {
                name: entry.constructor.clone(),
                fields,
            }
        })
        .collect()
}

fn desugar_decl(decl: &mut DeclKind, io_fns: &HashSet<String>) {
    match decl {
        DeclKind::Fun { body: Some(body), .. } => desugar_expr(body, io_fns),
        DeclKind::Fun { body: None, .. } => {},
        DeclKind::View { body, .. } => {
            // Don't desugar the top-level do block of a view body
            // (preserve structure for analyze_view), but recurse into sub-exprs.
            if let ExprKind::Do(stmts) = &mut body.node {
                for stmt in stmts.iter_mut() {
                    desugar_stmt(stmt, io_fns);
                }
            } else {
                desugar_expr(body, io_fns);
            }
        }
        DeclKind::Derived { body, .. } => desugar_expr(body, io_fns),
        DeclKind::Migrate { using_fn, .. } => desugar_expr(using_fn, io_fns),
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ImplItem::Method { body, .. } = item {
                    desugar_expr(body, io_fns);
                }
            }
        }
        DeclKind::Trait { items, .. } => {
            for item in items {
                if let TraitItem::Method {
                    default_body: Some(body),
                    ..
                } = item
                {
                    desugar_expr(body, io_fns);
                }
            }
        }
        _ => {}
    }
}

/// Recursively desugar expressions. The `Do` nodes that qualify as
/// pure comprehensions are replaced with nested App/Lambda/Yield nodes.
fn desugar_expr(expr: &mut Expr, io_fns: &HashSet<String>) {
    // First, recurse into sub-expressions (bottom-up).
    // We handle Set/FullSet specially to avoid desugaring their value do blocks.
    match &mut expr.node {
        ExprKind::Set { target, value } | ExprKind::FullSet { target, value } => {
            desugar_expr(target, io_fns);
            // Don't desugar the top-level do block of a set value,
            // but DO recurse into its sub-expressions.
            if let ExprKind::Do(stmts) = &mut value.node {
                for stmt in stmts.iter_mut() {
                    desugar_stmt(stmt, io_fns);
                }
            } else {
                desugar_expr(value, io_fns);
            }
            return; // Don't fall through to the Do check below
        }
        _ => recurse_into_children(expr, io_fns),
    }

    // Now check if this expression is a desugaring-eligible Do block.
    // Check eligibility with immutable borrows first to avoid borrow conflicts.
    let (sql_compilable, pure_comp) = if let ExprKind::Do(stmts) = &expr.node {
        (is_sql_compilable(stmts), is_pure_comprehension(stmts, io_fns))
    } else {
        (false, false)
    };

    if sql_compilable {
        // SQL-compilable do-blocks are preserved for codegen to compile
        // to a single SQL query. Still recurse into sub-expressions.
        if let ExprKind::Do(stmts) = &mut expr.node {
            for stmt in stmts.iter_mut() {
                desugar_stmt(stmt, io_fns);
            }
        }
        return;
    }
    if pure_comp {
        if let ExprKind::Do(stmts) = &expr.node {
            let span = expr.span;
            let desugared = desugar_stmts(stmts, span);
            *expr = desugared;
        }
    }
}

/// Recurse into all child expressions of a node (except Do blocks handled
/// by the caller).
fn recurse_into_children(expr: &mut Expr, io_fns: &HashSet<String>) {
    match &mut expr.node {
        ExprKind::Lit(_) | ExprKind::Var(_) | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => {}

        ExprKind::Record(fields) => {
            for f in fields {
                desugar_expr(&mut f.value, io_fns);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            desugar_expr(base, io_fns);
            for f in fields {
                desugar_expr(&mut f.value, io_fns);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } => desugar_expr(e, io_fns),
        ExprKind::List(elems) => {
            for e in elems {
                desugar_expr(e, io_fns);
            }
        }
        ExprKind::Lambda { body, .. } => desugar_expr(body, io_fns),
        ExprKind::App { func, arg } => {
            desugar_expr(func, io_fns);
            desugar_expr(arg, io_fns);
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            desugar_expr(lhs, io_fns);
            desugar_expr(rhs, io_fns);
        }
        ExprKind::UnaryOp { operand, .. } => desugar_expr(operand, io_fns),
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            desugar_expr(cond, io_fns);
            desugar_expr(then_branch, io_fns);
            desugar_expr(else_branch, io_fns);
        }
        ExprKind::Case { scrutinee, arms } => {
            desugar_expr(scrutinee, io_fns);
            for arm in arms {
                desugar_expr(&mut arm.body, io_fns);
            }
        }
        ExprKind::Do(stmts) => {
            for stmt in stmts {
                desugar_stmt(stmt, io_fns);
            }
        }
        ExprKind::Yield(inner) => desugar_expr(inner, io_fns),
        ExprKind::Set { target, value } | ExprKind::FullSet { target, value } => {
            desugar_expr(target, io_fns);
            desugar_expr(value, io_fns);
        }
        ExprKind::Atomic(inner) => desugar_expr(inner, io_fns),
        ExprKind::At { relation, time } => {
            desugar_expr(relation, io_fns);
            desugar_expr(time, io_fns);
        }
    }
}

fn desugar_stmt(stmt: &mut Stmt, io_fns: &HashSet<String>) {
    match &mut stmt.node {
        StmtKind::Bind { expr, .. } => desugar_expr(expr, io_fns),
        StmtKind::Let { expr, .. } => desugar_expr(expr, io_fns),
        StmtKind::Where { cond } => desugar_expr(cond, io_fns),
        StmtKind::GroupBy { key } => desugar_expr(key, io_fns),
        StmtKind::Expr(e) => desugar_expr(e, io_fns),
    }
}

// ── Eligibility check ────────────────────────────────────────────

/// A do block is SQL-compilable if:
/// 1. All non-final stmts are Bind(Var, SourceRef) or Where
/// 2. All Where conditions use only field accesses, literals, variables,
///    comparison operators, and boolean connectives
/// 3. The final stmt is Yield(Record) where each field is a field access
///    on a bound variable, or Yield(Var(bound_var)) for single-table
/// 4. At least one Bind
///
/// This is a purely syntactic check. The codegen does additional validation
/// (schema shape, views, etc.) and falls back to loop-based compilation
/// if the SQL path is not viable.
fn is_sql_compilable(stmts: &[Stmt]) -> bool {
    if stmts.len() < 2 {
        return false;
    }

    let mut bind_vars: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for stmt in &stmts[..stmts.len() - 1] {
        match &stmt.node {
            StmtKind::Bind { pat, expr } => {
                if let (PatKind::Var(name), ExprKind::SourceRef(_)) = (&pat.node, &expr.node) {
                    bind_vars.insert(name.as_str());
                } else {
                    return false;
                }
            }
            StmtKind::Where { cond } => {
                if !is_sql_where_expr(cond, &bind_vars) {
                    return false;
                }
            }
            _ => return false,
        }
    }

    if bind_vars.is_empty() {
        return false;
    }

    // Final statement must be yield of a record of field accesses or a bound var
    match &stmts.last().unwrap().node {
        StmtKind::Expr(e) => {
            if let ExprKind::Yield(inner) = &e.node {
                match &inner.node {
                    ExprKind::Record(fields) => {
                        !fields.is_empty()
                            && fields.iter().all(|f| is_bound_field_access(&f.value, &bind_vars))
                    }
                    ExprKind::Var(name) => bind_vars.contains(name.as_str()),
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_sql_where_expr(expr: &Expr, bind_vars: &std::collections::HashSet<&str>) -> bool {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                is_sql_where_expr(lhs, bind_vars) && is_sql_where_expr(rhs, bind_vars)
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                let l_ok = is_sql_atom(lhs);
                let r_ok = is_sql_atom(rhs);
                let l_bound = is_bound_field_access(lhs, bind_vars);
                let r_bound = is_bound_field_access(rhs, bind_vars);
                l_ok && r_ok && (l_bound || r_bound)
            }
            _ => false,
        },
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => is_sql_where_expr(operand, bind_vars),
        _ => false,
    }
}

fn is_sql_atom(expr: &Expr) -> bool {
    matches!(
        &expr.node,
        ExprKind::FieldAccess { expr, .. } if matches!(&expr.node, ExprKind::Var(_))
    ) || matches!(&expr.node, ExprKind::Lit(_) | ExprKind::Var(_))
}

fn is_bound_field_access(expr: &Expr, bind_vars: &std::collections::HashSet<&str>) -> bool {
    if let ExprKind::FieldAccess { expr, .. } = &expr.node {
        if let ExprKind::Var(name) = &expr.node {
            return bind_vars.contains(name.as_str());
        }
    }
    false
}

/// A do block is a "pure comprehension" if:
/// 1. It contains at least one Bind or Where statement
/// 2. All non-final statements are Bind, Where, or Let
/// 3. The final statement is Expr(Yield(..))
fn is_pure_comprehension(stmts: &[Stmt], io_fns: &HashSet<String>) -> bool {
    if stmts.is_empty() {
        return false;
    }

    let has_bind_or_where = stmts.iter().any(|s| {
        matches!(
            &s.node,
            StmtKind::Bind { .. } | StmtKind::Where { .. }
        )
    });
    if !has_bind_or_where {
        return false;
    }

    // GroupBy requires loop-based codegen — not eligible for desugaring
    if stmts.iter().any(|s| matches!(&s.node, StmtKind::GroupBy { .. })) {
        return false;
    }

    // IO do blocks use a dedicated codegen path (compile_io_do) — not eligible
    // for desugaring. Check if any bind or bare expression calls an IO builtin
    // or a user-defined IO function.
    if stmts.iter().any(|s| match &s.node {
        StmtKind::Bind { expr, .. } | StmtKind::Let { expr, .. } | StmtKind::Expr(expr) => expr_is_io(expr, io_fns),
        StmtKind::Where { cond } => expr_is_io(cond, io_fns),
        _ => false,
    }) {
        return false;
    }

    // Constructor pattern binds may be value pattern matches (not monadic
    // binds). The desugarer can't tell syntactically whether the expression
    // is a relation or a value, so we leave these for direct codegen which
    // handles both cases correctly.
    if stmts.iter().any(|s| matches!(
        &s.node,
        StmtKind::Bind { pat, .. } if matches!(&pat.node, PatKind::Constructor { .. })
    )) {
        return false;
    }

    // Check that all non-final statements are Bind/Where/Let
    for stmt in &stmts[..stmts.len() - 1] {
        match &stmt.node {
            StmtKind::Bind { .. } | StmtKind::Where { .. } | StmtKind::Let { .. } => {}
            _ => return false,
        }
    }

    // Final statement must be yield
    match &stmts.last().unwrap().node {
        StmtKind::Expr(e) => matches!(&e.node, ExprKind::Yield(_)),
        _ => false,
    }
}

/// Check if an expression contains an IO-returning builtin or user-defined IO function.
/// Recurses into nested expressions to catch IO buried inside if/case/lambda/etc.
fn expr_is_io(expr: &Expr, io_fns: &HashSet<String>) -> bool {
    match &expr.node {
        ExprKind::App { func, arg } => {
            expr_is_io(func, io_fns) || expr_is_io(arg, io_fns)
        }
        ExprKind::Var(name) => {
            matches!(
                name.as_str(),
                "println" | "putLine" | "print" | "readLine" | "readFile"
                    | "writeFile" | "appendFile" | "fileExists" | "removeFile"
                    | "listDir" | "now" | "randomInt" | "randomFloat"
                    | "fetch" | "fetchWith" | "fork" | "listen"
                    | "generateKeyPair" | "generateSigningKeyPair" | "encrypt"
            ) || io_fns.contains(name.as_str())
        }
        ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => true,
        ExprKind::Set { .. } | ExprKind::FullSet { .. } => true,
        ExprKind::At { .. } | ExprKind::Atomic(_) => true,
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_is_io(lhs, io_fns) || expr_is_io(rhs, io_fns)
        }
        ExprKind::UnaryOp { operand, .. } => expr_is_io(operand, io_fns),
        ExprKind::If { cond, then_branch, else_branch, .. } => {
            expr_is_io(cond, io_fns)
                || expr_is_io(then_branch, io_fns)
                || expr_is_io(else_branch, io_fns)
        }
        ExprKind::Case { scrutinee, arms, .. } => {
            expr_is_io(scrutinee, io_fns)
                || arms.iter().any(|arm| expr_is_io(&arm.body, io_fns))
        }
        ExprKind::Lambda { body, .. } => expr_is_io(body, io_fns),
        ExprKind::Yield(inner) => expr_is_io(inner, io_fns),
        ExprKind::Do(stmts) => {
            stmts.iter().any(|s| match &s.node {
                StmtKind::Bind { expr, .. } => expr_is_io(expr, io_fns),
                StmtKind::Expr(expr) => expr_is_io(expr, io_fns),
                StmtKind::Let { expr, .. } => expr_is_io(expr, io_fns),
                StmtKind::Where { cond } => expr_is_io(cond, io_fns),
                StmtKind::GroupBy { key } => expr_is_io(key, io_fns),
            })
        }
        // Records, lists, field access are data constructors/accessors —
        // they don't produce IO even if they contain IO values as
        // subexpressions. Only direct IO-producing expressions (calls to
        // IO functions, relation ops, etc.) should flag a do-block as IO.
        _ => false,
    }
}

// ── Core desugaring ──────────────────────────────────────────────

/// Counter for generating unique temporary variable names.
static DESUGAR_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn fresh_var() -> String {
    let n = DESUGAR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("__ds{}", n)
}

fn spanned<T>(node: T, span: Span) -> Spanned<T> {
    Spanned::new(node, span)
}

/// Desugar a list of statements into a single expression.
fn desugar_stmts(stmts: &[Stmt], span: Span) -> Expr {
    assert!(!stmts.is_empty());

    // Base case: single statement
    if stmts.len() == 1 {
        return match &stmts[0].node {
            StmtKind::Expr(e) => {
                // Transform yield e -> __yield(e) for generic monad support
                if let ExprKind::Yield(inner) = &e.node {
                    mk_yield((**inner).clone(), span)
                } else {
                    e.clone()
                }
            }
            // Shouldn't happen for valid pure comprehensions (last must be yield)
            _ => spanned(ExprKind::Var("__empty".into()), span),
        };
    }

    let rest = desugar_stmts(&stmts[1..], span);

    match &stmts[0].node {
        StmtKind::Bind { pat, expr } => {
            if let PatKind::Constructor { .. } = &pat.node {
                // Constructor pattern: case dispatch with empty fallback
                desugar_ctor_bind(pat, expr, &rest, span)
            } else {
                // Normal bind: App(App(__bind, \pat -> rest), expr)
                mk_bind(
                    spanned(
                        ExprKind::Lambda {
                            params: vec![pat.clone()],
                            body: Box::new(rest),
                        },
                        span,
                    ),
                    expr.clone(),
                    span,
                )
            }
        }

        StmtKind::Where { cond } => {
            // App(App(__bind, \_ -> rest), if cond then __yield({}) else __empty)
            let guard = spanned(
                ExprKind::If {
                    cond: Box::new(cond.clone()),
                    then_branch: Box::new(mk_yield(
                        spanned(ExprKind::Record(vec![]), span),
                        span,
                    )),
                    else_branch: Box::new(spanned(ExprKind::Var("__empty".into()), span)),
                },
                span,
            );
            mk_bind(
                spanned(
                    ExprKind::Lambda {
                        params: vec![spanned(PatKind::Wildcard, span)],
                        body: Box::new(rest),
                    },
                    span,
                ),
                guard,
                span,
            )
        }

        StmtKind::Let { pat, expr } => {
            // (\pat -> rest) expr
            spanned(
                ExprKind::App {
                    func: Box::new(spanned(
                        ExprKind::Lambda {
                            params: vec![pat.clone()],
                            body: Box::new(rest),
                        },
                        span,
                    )),
                    arg: Box::new(expr.clone()),
                },
                span,
            )
        }

        StmtKind::GroupBy { .. } => {
            // GroupBy blocks are not desugared (filtered by is_pure_comprehension)
            unreachable!("groupBy should not appear in desugared do blocks")
        }

        StmtKind::Expr(_) => {
            // Shouldn't happen in pure comprehension (non-final bare expr)
            rest
        }
    }
}

/// Desugar a constructor pattern bind:
/// `Ctor pat <- expr; rest` =>
/// `__bind (\__tmp -> case __tmp of { Ctor pat -> rest; _ -> [] }) expr`
fn desugar_ctor_bind(pat: &Pat, expr: &Expr, rest: &Expr, span: Span) -> Expr {
    let tmp = fresh_var();
    let tmp_var = spanned(ExprKind::Var(tmp.clone()), span);

    let case_expr = spanned(
        ExprKind::Case {
            scrutinee: Box::new(tmp_var),
            arms: vec![
                CaseArm {
                    pat: pat.clone(),
                    body: rest.clone(),
                },
                CaseArm {
                    pat: spanned(PatKind::Wildcard, span),
                    body: spanned(ExprKind::Var("__empty".into()), span),
                },
            ],
        },
        span,
    );

    mk_bind(
        spanned(
            ExprKind::Lambda {
                params: vec![spanned(PatKind::Var(tmp), span)],
                body: Box::new(case_expr),
            },
            span,
        ),
        expr.clone(),
        span,
    )
}

/// Build `App(Var("__yield"), inner)` — monadic yield for generic do-blocks.
fn mk_yield(inner: Expr, span: Span) -> Expr {
    spanned(
        ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("__yield".into()), span)),
            arg: Box::new(inner),
        },
        span,
    )
}

/// Build `App(App(Var("__bind"), func), collection)`
fn mk_bind(func: Expr, collection: Expr, span: Span) -> Expr {
    spanned(
        ExprKind::App {
            func: Box::new(spanned(
                ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("__bind".into()), span)),
                    arg: Box::new(func),
                },
                span,
            )),
            arg: Box::new(collection),
        },
        span,
    )
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> Module {
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (module, _) = parser.parse_module();
        module
    }

    fn has_bind_var(expr: &Expr) -> bool {
        match &expr.node {
            ExprKind::Var(name) => name == "__bind",
            ExprKind::App { func, arg } => has_bind_var(func) || has_bind_var(arg),
            ExprKind::Lambda { body, .. } => has_bind_var(body),
            ExprKind::If { cond, then_branch, else_branch } => {
                has_bind_var(cond) || has_bind_var(then_branch) || has_bind_var(else_branch)
            }
            ExprKind::Case { scrutinee, arms } => {
                has_bind_var(scrutinee) || arms.iter().any(|a| has_bind_var(&a.body))
            }
            ExprKind::Yield(inner) => has_bind_var(inner),
            _ => false,
        }
    }

    fn has_do_block(expr: &Expr) -> bool {
        match &expr.node {
            ExprKind::Do(_) => true,
            ExprKind::App { func, arg } => has_do_block(func) || has_do_block(arg),
            ExprKind::Lambda { body, .. } => has_do_block(body),
            ExprKind::If { cond, then_branch, else_branch } => {
                has_do_block(cond) || has_do_block(then_branch) || has_do_block(else_branch)
            }
            ExprKind::Case { scrutinee, arms } => {
                has_do_block(scrutinee) || arms.iter().any(|a| has_do_block(&a.body))
            }
            ExprKind::Yield(inner) => has_do_block(inner),
            ExprKind::Set { target, value } | ExprKind::FullSet { target, value } => {
                has_do_block(target) || has_do_block(value)
            }
            _ => false,
        }
    }

    #[test]
    fn pure_comprehension_is_desugared() {
        // Use an in-memory relation (not a source ref) so the do-block
        // is purely relational and eligible for desugaring.
        let src = r#"
            names = \people -> do
              p <- people
              where p.age > 27
              yield p.name
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "names" {
                    assert!(has_bind_var(body), "expected __bind in desugared body");
                }
            }
        }
    }

    #[test]
    fn mixed_do_block_not_desugared() {
        let src = r#"
            *people : [{name: Text, age: Int}]
            main = do
              set *people = [{name: "Alice", age: 30}]
              p <- *people
              yield p.name
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        // The main body should still be a Do block (mixed: has set + bind)
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "main" {
                    assert!(matches!(&body.node, ExprKind::Do(_)),
                        "mixed do block should not be desugared");
                }
            }
        }
    }

    #[test]
    fn set_value_do_not_desugared() {
        let src = r#"
            *todos : [{title: Text, done: Int}]
            complete = \title ->
              set *todos = do
                t <- *todos
                yield (if t.title == title then {t | done: 1} else t)
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        // The set value should still be a Do block
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "complete" {
                    // body is a lambda whose body is a set
                    if let ExprKind::Lambda { body: lbody, .. } = &body.node {
                        if let ExprKind::Set { value, .. } = &lbody.node {
                            assert!(matches!(&value.node, ExprKind::Do(_)),
                                "set value do block should not be desugared");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn sequential_do_not_desugared() {
        let src = r#"
            main = do
              println "hello"
              println "world"
              yield {}
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        // No bind/where → sequential, should not be desugared
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "main" {
                    assert!(matches!(&body.node, ExprKind::Do(_)),
                        "sequential do block should not be desugared");
                }
            }
        }
    }

    #[test]
    fn sql_compilable_do_preserved() {
        // SQL-compilable do-blocks (Bind→SourceRef + Where + Yield(Var))
        // are preserved as Do nodes for codegen to compile to SQL.
        let src = r#"
            *items : [{x: Int}]
            filtered = do
              i <- *items
              where i.x > 0
              yield i
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "filtered" {
                    assert!(
                        matches!(&body.node, ExprKind::Do(_)),
                        "sql-compilable do block should be preserved for codegen"
                    );
                }
            }
        }
    }

    #[test]
    fn where_with_non_record_yield_desugared() {
        // Use an in-memory relation so the do-block is purely relational.
        let src = r#"
            names = \items -> do
              i <- items
              where i.x > 0
              yield i.name
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "names" {
                    assert!(has_bind_var(body), "expected __bind in desugared body");
                    assert!(!has_do_block(body), "expected no Do block after desugaring");
                }
            }
        }
    }

    #[test]
    fn groupby_do_not_desugared() {
        let src = r#"
            *items : [{x: Int, cat: Text}]
            grouped = do
              i <- *items
              groupBy {i.cat}
              yield {cat: i.cat, n: count i}
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        // groupBy do blocks must stay as Do nodes (loop-based codegen)
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "grouped" {
                    assert!(
                        matches!(&body.node, ExprKind::Do(_)),
                        "groupBy do block should not be desugared"
                    );
                }
            }
        }
    }

    #[test]
    fn multi_table_sql_compilable_preserved() {
        let src = r#"
            *employees : [{name: Text, dept: Text}]
            *departments : [{name: Text, budget: Int}]
            joined = do
              e <- *employees
              d <- *departments
              where e.dept == d.name
              yield {name: e.name, budget: d.budget}
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "joined" {
                    assert!(
                        matches!(&body.node, ExprKind::Do(_)),
                        "multi-table sql-compilable do block should be preserved"
                    );
                }
            }
        }
    }

    #[test]
    fn ctor_pattern_bind_not_desugared() {
        // Constructor pattern binds may be value pattern matches
        // (not monadic binds) — the desugarer can't distinguish, so
        // these do blocks are left for direct codegen.
        let src = r#"
            data Status = Open {} | Closed {}
            *items : [{name: Text, status: Status}]
            main = do
              i <- *items
              Open {} <- i.status
              yield {name: i.name}
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        for decl in &module.decls {
            if let DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "main" {
                    assert!(
                        matches!(&body.node, ExprKind::Do(_)),
                        "ctor pattern bind do block should not be desugared"
                    );
                }
            }
        }
    }
}
