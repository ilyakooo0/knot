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

/// Desugar a module in place. Transforms pure-comprehension do blocks
/// into nested bind/yield/empty expressions, and routes into data declarations.
pub fn desugar(module: &mut Module) {
    desugar_routes(module);
    for decl in &mut module.decls {
        desugar_decl(&mut decl.node);
    }
}

/// Generate `Data` declarations from `Route` and `RouteComposite` declarations.
/// The original route declarations are kept in place for codegen to extract HTTP metadata.
fn desugar_routes(module: &mut Module) {
    let mut new_decls: Vec<Decl> = Vec::new();

    // Collect route entries by name for RouteComposite lookup
    let route_map: std::collections::HashMap<String, Vec<RouteEntry>> = module
        .decls
        .iter()
        .filter_map(|d| {
            if let DeclKind::Route { name, entries } = &d.node {
                Some((name.clone(), entries.clone()))
            } else {
                None
            }
        })
        .collect();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Route { name, entries } => {
                let ctors = route_entries_to_constructors(entries);
                new_decls.push(Spanned::new(
                    DeclKind::Data {
                        name: name.clone(),
                        params: vec![],
                        constructors: ctors,
                        deriving: vec![],
                    },
                    decl.span,
                ));
            }
            DeclKind::RouteComposite { name, components } => {
                let mut all_entries = Vec::new();
                for comp in components {
                    if let Some(entries) = route_map.get(comp) {
                        all_entries.extend(entries.clone());
                    }
                }
                let ctors = route_entries_to_constructors(&all_entries);
                new_decls.push(Spanned::new(
                    DeclKind::Data {
                        name: name.clone(),
                        params: vec![],
                        constructors: ctors,
                        deriving: vec![],
                    },
                    decl.span,
                ));
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
            // Add `respond : <response_ty> -> Response` field if route has a response type
            if let Some(response_ty) = &entry.response_ty {
                let dummy_span = Span::new(0, 0);
                fields.push(Field {
                    name: "respond".to_string(),
                    value: Spanned::new(
                        TypeKind::Function {
                            param: Box::new(response_ty.clone()),
                            result: Box::new(Spanned::new(
                                TypeKind::Named("Response".into()),
                                dummy_span,
                            )),
                        },
                        dummy_span,
                    ),
                });
            }
            ConstructorDef {
                name: entry.constructor.clone(),
                fields,
            }
        })
        .collect()
}

fn desugar_decl(decl: &mut DeclKind) {
    match decl {
        DeclKind::Fun { body, .. } => desugar_expr(body),
        DeclKind::View { body, .. } => desugar_expr(body),
        DeclKind::Derived { body, .. } => desugar_expr(body),
        DeclKind::Migrate { using_fn, .. } => desugar_expr(using_fn),
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ImplItem::Method { body, .. } = item {
                    desugar_expr(body);
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
                    desugar_expr(body);
                }
            }
        }
        _ => {}
    }
}

/// Recursively desugar expressions. The `Do` nodes that qualify as
/// pure comprehensions are replaced with nested App/Lambda/Yield nodes.
fn desugar_expr(expr: &mut Expr) {
    // First, recurse into sub-expressions (bottom-up).
    // We handle Set/FullSet specially to avoid desugaring their value do blocks.
    match &mut expr.node {
        ExprKind::Set { target, value } | ExprKind::FullSet { target, value } => {
            desugar_expr(target);
            // Don't desugar the top-level do block of a set value,
            // but DO recurse into its sub-expressions.
            if let ExprKind::Do(stmts) = &mut value.node {
                for stmt in stmts.iter_mut() {
                    desugar_stmt(stmt);
                }
            } else {
                desugar_expr(value);
            }
            return; // Don't fall through to the Do check below
        }
        _ => recurse_into_children(expr),
    }

    // Now check if this expression is a desugaring-eligible Do block.
    if let ExprKind::Do(stmts) = &expr.node {
        if is_pure_comprehension(stmts) {
            let span = expr.span;
            let desugared = desugar_stmts(stmts, span);
            *expr = desugared;
        }
    }
}

/// Recurse into all child expressions of a node (except Do blocks handled
/// by the caller).
fn recurse_into_children(expr: &mut Expr) {
    match &mut expr.node {
        ExprKind::Lit(_) | ExprKind::Var(_) | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => {}

        ExprKind::Record(fields) => {
            for f in fields {
                desugar_expr(&mut f.value);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            desugar_expr(base);
            for f in fields {
                desugar_expr(&mut f.value);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } => desugar_expr(e),
        ExprKind::List(elems) => {
            for e in elems {
                desugar_expr(e);
            }
        }
        ExprKind::Lambda { body, .. } => desugar_expr(body),
        ExprKind::App { func, arg } => {
            desugar_expr(func);
            desugar_expr(arg);
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            desugar_expr(lhs);
            desugar_expr(rhs);
        }
        ExprKind::UnaryOp { operand, .. } => desugar_expr(operand),
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            desugar_expr(cond);
            desugar_expr(then_branch);
            desugar_expr(else_branch);
        }
        ExprKind::Case { scrutinee, arms } => {
            desugar_expr(scrutinee);
            for arm in arms {
                desugar_expr(&mut arm.body);
            }
        }
        ExprKind::Do(stmts) => {
            for stmt in stmts {
                desugar_stmt(stmt);
            }
        }
        ExprKind::Yield(inner) => desugar_expr(inner),
        ExprKind::Set { target, value } | ExprKind::FullSet { target, value } => {
            desugar_expr(target);
            desugar_expr(value);
        }
        ExprKind::Atomic(inner) => desugar_expr(inner),
        ExprKind::At { relation, time } => {
            desugar_expr(relation);
            desugar_expr(time);
        }
    }
}

fn desugar_stmt(stmt: &mut Stmt) {
    match &mut stmt.node {
        StmtKind::Bind { expr, .. } => desugar_expr(expr),
        StmtKind::Let { expr, .. } => desugar_expr(expr),
        StmtKind::Where { cond } => desugar_expr(cond),
        StmtKind::GroupBy { key } => desugar_expr(key),
        StmtKind::Expr(e) => desugar_expr(e),
    }
}

// ── Eligibility check ────────────────────────────────────────────

/// A do block is a "pure comprehension" if:
/// 1. It contains at least one Bind or Where statement
/// 2. All non-final statements are Bind, Where, or Let
/// 3. The final statement is Expr(Yield(..))
fn is_pure_comprehension(stmts: &[Stmt]) -> bool {
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
        let src = r#"
            *people : [{name: Text, age: Int}]
            main = do
              p <- *people
              where p.age > 27
              yield p.name
        "#;
        let mut module = parse(src);
        desugar(&mut module);
        // The Fun body should now contain __bind calls, not a Do block
        for decl in &module.decls {
            if let DeclKind::Fun { name, body, .. } = &decl.node {
                if name == "main" {
                    assert!(has_bind_var(body), "expected __bind in desugared body");
                    assert!(!has_do_block(body), "expected no Do block after desugaring");
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
            if let DeclKind::Fun { name, body, .. } = &decl.node {
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
            if let DeclKind::Fun { name, body, .. } = &decl.node {
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
            if let DeclKind::Fun { name, body, .. } = &decl.node {
                if name == "main" {
                    assert!(matches!(&body.node, ExprKind::Do(_)),
                        "sequential do block should not be desugared");
                }
            }
        }
    }

    #[test]
    fn where_desugars_to_guard() {
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
            if let DeclKind::Fun { name, body, .. } = &decl.node {
                if name == "filtered" {
                    assert!(has_bind_var(body));
                    assert!(!has_do_block(body));
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
            if let DeclKind::Fun { name, body, .. } = &decl.node {
                if name == "grouped" {
                    assert!(
                        matches!(&body.node, ExprKind::Do(_)),
                        "groupBy do block should not be desugared"
                    );
                }
            }
        }
    }
}
