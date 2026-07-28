//! Static detection of definitions that provably never terminate.
//!
//! A definition diverges on every input when it unconditionally leads back to
//! itself with no possibility of reaching a base case. This module finds such
//! definitions so the compiler can reject them at compile time instead of
//! emitting code that overflows the stack at runtime.
//!
//! # The analysis
//!
//! Build a call graph over top-level function/value definitions. An edge
//! `f → g` means "when `f` runs, `g` necessarily runs too" — `g` is referenced
//! in `f`'s body on a path that is *unguarded*: it does not pass through a
//! conditional (`case` arm), a short-circuit RHS (`&&` / `||`), or a lambda
//! body (deferred until applied). Every other subexpression — application,
//! `do` statements, record fields, field access, non-short-circuit binary
//! operands, a `case` *scrutinee*, a short-circuit LHS — is evaluated
//! unconditionally.
//!
//! A definition is *provably divergent* when it sits on a cycle of such edges
//! where every self-application along the cycle either is 0-arg (a bare
//! constant reference that re-evaluates itself) or applies a function to
//! syntactically identical arguments (no progress toward a base case).
//!
//! # Soundness
//!
//! The analysis is **sound but incomplete** — it only rejects definitions it
//! can prove diverge. Guarded recursion (`case n of 0 -> 0; _ -> f (n-1)`)
//! and recursion with changing arguments (`g (n+1)`) are never flagged, even
//! when they happen to diverge, because proving those requires solving the
//! halting problem. This guarantees we never break a program that might
//! terminate.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{HashMap, HashSet};

use crate::decl_view::{decl_views, DeclViewKind};

/// A top-level definition relevant to the analysis.
struct Def<'a> {
    /// Runtime arity: 0 for a value/constant, N for an N-parameter function.
    arity: usize,
    /// The definition body (leading lambda peeled for functions).
    body: &'a ast::Expr,
    /// Parameter names (empty for 0-arg), used to detect unchanged self-calls.
    params: Vec<String>,
    /// Source span for diagnostics.
    span: Span,
}

/// Detect provably-divergent top-level definitions, returning one diagnostic
/// per detected cycle. Returns an empty vec when every definition may
/// terminate.
pub fn check(program: &ast::Expr) -> Vec<Diagnostic> {
    crate::stack::grow(|| check_inner(program))
}

fn check_inner(program: &ast::Expr) -> Vec<Diagnostic> {
    // Collect top-level function/value definitions.
    let mut defs: HashMap<String, Def> = HashMap::new();
    for d in decl_views(program) {
        if let DeclViewKind::Fun {
            body: Some(body), ..
        } = d.kind
        {
            let (params, peeled) = value_lambda_chain(body);
            defs.insert(
                d.name.to_string(),
                Def {
                    arity: params.len(),
                    body: peeled,
                    params: params.iter().filter_map(pat_var_name).collect(),
                    span: d.span,
                },
            );
        }
    }
    if defs.is_empty() {
        return Vec::new();
    }

    // Build the unguarded-call graph: edges f -> g where g necessarily runs
    // when f runs. Only edges that can prove divergence are kept: a 0-arg
    // reference (a constant that re-evaluates itself) or a param'd
    // self-application with syntactically identical arguments (no progress).
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    for (name, def) in &defs {
        let mut found: Vec<String> = Vec::new();
        collect_unguarded_calls(def.body, &defs, name, def, &mut found);
        edges.insert(name.clone(), found);
    }

    // A definition is divergent iff it can reach itself through unguarded
    // edges. Report each divergent definition once.
    let mut reported: HashSet<String> = HashSet::new();
    let mut diagnostics = Vec::new();
    for name in defs.keys() {
        if reported.contains(name) {
            continue;
        }
        if let Some(cycle) = find_cycle(name, &edges) {
            for member in &cycle {
                reported.insert(member.clone());
            }
            let span = defs
                .get(name)
                .map(|d| d.span)
                .unwrap_or(Span { start: 0, end: 0 });
            let chain = cycle.join(" -> ");
            let diag = Diagnostic::error(format!(
                "`{name}` never terminates: it unconditionally calls itself with no base case"
            ))
            .label(span, format!("cycle: {chain}"))
            .note(
                "the call is not guarded by a `case` and makes no progress toward a base case, \
                 so evaluating it recurses forever. Add a `case` guard with a base case, or \
                 change the argument on each recursive call.",
            );
            diagnostics.push(diag);
        }
    }
    diagnostics
}

/// Walk `expr` collecting unguarded divergent calls to known definitions.
/// Does NOT descend into `case` arms, short-circuit RHS, or lambda bodies —
/// those are guarded or deferred and cannot be part of an unconditional cycle.
fn collect_unguarded_calls<'a>(
    expr: &'a ast::Expr,
    defs: &HashMap<String, Def<'a>>,
    caller_name: &str,
    caller: &Def<'a>,
    out: &mut Vec<String>,
) {
    use ast::ExprKind::*;
    match &expr.node {
        Var(name) => {
            if let Some(callee) = defs.get(name.as_str()) {
                // A bare reference runs a 0-arg callee unconditionally. A
                // param'd callee referenced bare is just a value (not yet
                // applied), so it does not run — no edge.
                if callee.arity == 0 {
                    out.push(name.clone());
                }
            }
        }
        App { .. } => {
            // Peel the application spine to find the head and its arguments.
            let (head, args) = peel_app(expr);
            if let Var(name) = &head.node {
                if let Some(callee) = defs.get(name.as_str()) {
                    // A param'd self-application that passes the caller's own
                    // parameters unchanged makes no progress toward a base
                    // case — it loops forever. Only such edges prove
                    // divergence; changing or un-analyzable args do not.
                    if name.as_str() == caller_name
                        && callee.arity > 0
                        && args_match_params(&args, &caller.params)
                    {
                        out.push(name.clone());
                    }
                }
            }
            // The head and every argument are still evaluated unconditionally.
            collect_unguarded_calls(head, defs, caller_name, caller, out);
            for a in args {
                collect_unguarded_calls(a, defs, caller_name, caller, out);
            }
        }
        // Guarded: only the scrutinee runs unconditionally; arms are
        // conditional and cannot be part of an unconditional cycle.
        Case { scrutinee, .. } => {
            collect_unguarded_calls(scrutinee, defs, caller_name, caller, out)
        }
        // Short-circuit: LHS runs unconditionally, RHS is guarded.
        BinOp {
            op: ast::BinOp::And,
            lhs,
            ..
        }
        | BinOp {
            op: ast::BinOp::Or,
            lhs,
            ..
        } => collect_unguarded_calls(lhs, defs, caller_name, caller, out),
        BinOp { lhs, rhs, .. } => {
            collect_unguarded_calls(lhs, defs, caller_name, caller, out);
            collect_unguarded_calls(rhs, defs, caller_name, caller, out);
        }
        // Deferred: a lambda body only runs when the lambda is applied, so it
        // is not part of this definition's unconditional execution.
        Lambda { .. } => {}
        UnaryOp { operand, .. } => collect_unguarded_calls(operand, defs, caller_name, caller, out),
        Record(fields) => {
            for f in fields {
                collect_unguarded_calls(&f.value, defs, caller_name, caller, out);
            }
        }
        RecordUpdate { base, fields } => {
            collect_unguarded_calls(base, defs, caller_name, caller, out);
            for f in fields {
                collect_unguarded_calls(&f.value, defs, caller_name, caller, out);
            }
        }
        FieldAccess { expr, .. } => collect_unguarded_calls(expr, defs, caller_name, caller, out),
        List(items) => {
            for it in items {
                collect_unguarded_calls(it, defs, caller_name, caller, out);
            }
        }
        With { record, body, .. } => {
            collect_unguarded_calls(record, defs, caller_name, caller, out);
            collect_unguarded_calls(body, defs, caller_name, caller, out);
        }
        Do(stmts) => {
            for s in stmts {
                match &s.node {
                    ast::StmtKind::Bind { expr, .. } => {
                        collect_unguarded_calls(expr, defs, caller_name, caller, out)
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_unguarded_calls(cond, defs, caller_name, caller, out)
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_unguarded_calls(key, defs, caller_name, caller, out)
                    }
                    ast::StmtKind::Expr(x) => {
                        collect_unguarded_calls(x, defs, caller_name, caller, out)
                    }
                }
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            collect_unguarded_calls(target, defs, caller_name, caller, out);
            collect_unguarded_calls(value, defs, caller_name, caller, out);
        }
        Atomic(x) | Refine(x) => collect_unguarded_calls(x, defs, caller_name, caller, out),
        TimeUnitLit { value, .. } => collect_unguarded_calls(value, defs, caller_name, caller, out),
        Annot { expr, .. } => collect_unguarded_calls(expr, defs, caller_name, caller, out),
        Serve { handlers, .. } => {
            for h in handlers {
                collect_unguarded_calls(&h.body, defs, caller_name, caller, out);
            }
        }
        // Leaf / declaration nodes carry no unconditional calls.
        _ => {}
    }
}

/// Peel an application spine `f a b c` into (head, [a, b, c]).
fn peel_app(expr: &ast::Expr) -> (&ast::Expr, Vec<&ast::Expr>) {
    let mut args = Vec::new();
    let mut cur = expr;
    while let ast::ExprKind::App { func, arg } = &cur.node {
        args.push(arg.as_ref());
        cur = func;
    }
    args.reverse();
    (cur, args)
}

/// True when each application argument is syntactically the caller's own
/// parameter of the same position — a self-call that makes no progress.
fn args_match_params(args: &[&ast::Expr], params: &[String]) -> bool {
    if params.is_empty() || args.len() < params.len() {
        return false;
    }
    params
        .iter()
        .zip(args.iter())
        .all(|(p, a)| matches!(&a.node, ast::ExprKind::Var(v) if v == p))
}

/// The variable name bound by a pattern, if it is a plain variable.
fn pat_var_name(pat: &ast::Pat) -> Option<String> {
    match &pat.node {
        ast::PatKind::Var(n) => Some(n.clone()),
        _ => None,
    }
}

/// Peel leading type-witness lambda layers and one value-lambda layer to get a
/// definition's parameters and runtime body. Mirrors codegen's
/// `value_lambda_chain`, but takes the body by reference.
fn value_lambda_chain(expr: &ast::Expr) -> (Vec<ast::Pat>, &ast::Expr) {
    let mut cur = expr;
    // Skip leading witness-only layers (`\(A : Type) -> …`).
    while let ast::ExprKind::Lambda {
        params,
        ty_params,
        body,
        ..
    } = &cur.node
    {
        if params.is_empty() && !ty_params.is_empty() {
            cur = body;
        } else {
            break;
        }
    }
    match &cur.node {
        ast::ExprKind::Lambda { params, body, .. } => (params.clone(), body),
        _ => (Vec::new(), cur),
    }
}

/// Depth-first search for a path from `start` back to `start` through
/// unguarded edges. Returns the cycle (starting and ending at `start`) if one
/// exists.
fn find_cycle(start: &str, edges: &HashMap<String, Vec<String>>) -> Option<Vec<String>> {
    fn dfs(
        node: &str,
        start: &str,
        edges: &HashMap<String, Vec<String>>,
        path: &mut Vec<String>,
        visited: &mut HashSet<String>,
    ) -> Option<Vec<String>> {
        for next in edges.get(node).into_iter().flatten() {
            if next == start {
                let mut cycle = path.clone();
                cycle.push(start.to_string());
                return Some(cycle);
            }
            if visited.insert(next.clone()) {
                path.push(next.clone());
                if let Some(c) = dfs(next, start, edges, path, visited) {
                    return Some(c);
                }
                path.pop();
            }
        }
        None
    }
    let mut visited = HashSet::new();
    visited.insert(start.to_string());
    let mut path = vec![start.to_string()];
    dfs(start, start, edges, &mut path, &mut visited)
}
