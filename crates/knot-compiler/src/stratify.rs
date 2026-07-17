//! Stratification checking for recursive derived relations.
//!
//! A recursive derived relation that uses a non-monotone operation on itself
//! or on a mutually-recursive peer cannot converge to a unique minimal
//! fixpoint.  This pass builds a dependency graph between derived relations,
//! finds strongly-connected components, and rejects any cycle that contains
//! a negative edge.
//!
//! Two operations create negative edges: set difference (`diff a b`, which
//! negates `b`) and aggregates (`count`/`sum`/`avg`/`minOn`/`maxOn`) applied
//! to the recursive relation. An aggregate collapses the whole relation to a
//! scalar whose value shifts as the relation grows, so a body gated on it
//! (e.g. `where count self == 0`) oscillates instead of converging — caught
//! here rather than panicking after 10000 fixpoint iterations at runtime.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{HashMap, HashSet};

// ── Dependency graph ────────────────────────────────────────────

/// Why a dependency is negative (non-monotone). Carried on `Polarity::Negative`
/// so the diagnostic can name the concrete cause instead of always blaming
/// `diff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NegCause {
    /// Set difference (`diff a b`) — the subtracted set `b`.
    Diff,
    /// Boolean negation (`not`) wrapping a non-monotone check such as
    /// `any`/`elem`/`contains` over a relation — negation-as-failure.
    Not,
    /// A non-monotone aggregate (`count`/`sum`/`avg`/`minOn`/`maxOn`/`countWhere`)
    /// collapsing the relation to a scalar.
    Aggregate,
}

impl NegCause {
    /// How the offending operation is named in the headline and label
    /// ("negates `&x` through {}").
    fn operation(self) -> &'static str {
        match self {
            NegCause::Diff => "`diff`",
            NegCause::Not => "`not`",
            NegCause::Aggregate => "an aggregate (`count`/`sum`/`avg`/`minOn`/`maxOn`)",
        }
    }

    /// The verb form used in the mutual-recursion note ("{} across a cycle").
    fn gerund(self) -> &'static str {
        match self {
            NegCause::Diff => "negating",
            NegCause::Not => "negating",
            NegCause::Aggregate => "aggregating",
        }
    }

    /// The thing to extract in the fix-it note ("split the {} into …").
    fn noun(self) -> &'static str {
        match self {
            NegCause::Diff => "negation",
            NegCause::Not => "negation",
            NegCause::Aggregate => "aggregate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Polarity {
    Positive,
    Negative(NegCause),
}

#[derive(Debug, Clone)]
struct Edge {
    target: String,
    polarity: Polarity,
    span: Span,
}

/// Collect all derived-relation and view names plus their dependency edges.
///
/// Views participate in the stratification graph because a derived relation
/// may read a view (`*v`) whose body in turn reads the derived (`&d`),
/// forming an otherwise-invisible cycle `&d → *v → &d` that would bypass
/// the codegen self-recursion detector and stack-overflow at runtime. We
/// therefore treat both decl kinds as graph nodes and walk both bodies.
///
/// `diff_wrappers` maps user-defined function names whose body is exactly
/// `\a b -> diff a b` (a curried alias of the `diff` builtin) to the
/// positional parameter names, so a call like `minus self all` is
/// recognized as negation rather than a pair of positive edges (B31).
fn build_dependency_graph(module: &ast::Module) -> (HashSet<String>, HashMap<String, Vec<Edge>>) {
    let mut node_names = HashSet::new();
    let mut edges: HashMap<String, Vec<Edge>> = HashMap::new();
    let mut diff_wrappers: HashMap<String, [String; 2]> = HashMap::new();

    // First pass: collect all node names (derived relations and views),
    // and detect user functions that are curried aliases of `diff`.
    for decl in &module.decls {
        match &decl.node {
            ast::DeclKind::Derived { name, .. } => {
                node_names.insert(name.clone());
                edges.entry(name.clone()).or_default();
            }
            ast::DeclKind::View { name, .. } => {
                node_names.insert(name.clone());
                edges.entry(name.clone()).or_default();
            }
            ast::DeclKind::Fun { name, body: Some(body), .. } => {
                if let Some(params) = is_diff_wrapper(body) {
                    diff_wrappers.insert(name.clone(), params);
                }
            }
            _ => {}
        }
    }

    // Second pass: walk each node's body to find edges.
    for decl in &module.decls {
        match &decl.node {
            ast::DeclKind::Derived { name, body, .. } => {
                let mut found = Vec::new();
                let env = HashMap::new();
                let partial_diffs = HashMap::new();
                collect_edges(body, Polarity::Positive, &node_names, &env, &partial_diffs, &diff_wrappers, &mut found);
                edges.get_mut(name).unwrap().extend(found);
            }
            ast::DeclKind::View { name, body, .. } => {
                let mut found = Vec::new();
                let env = HashMap::new();
                let partial_diffs = HashMap::new();
                collect_edges(body, Polarity::Positive, &node_names, &env, &partial_diffs, &diff_wrappers, &mut found);
                edges.get_mut(name).unwrap().extend(found);
            }
            _ => {}
        }
    }

    (node_names, edges)
}

/// Check if a lambda body is exactly `\a b -> diff a b` (a curried alias of
/// the `diff` builtin). Returns the two parameter names in positional order
/// `[base_param, sub_param]` if so, so a call `f x y` can be treated as
/// `diff x y` (x positive, y negative). Also recognizes the pipe-desugared
/// form `\a b -> b |> diff a` (which is `diff a b` after beta-reduction).
fn is_diff_wrapper(body: &ast::Expr) -> Option<[String; 2]> {
    if let ast::ExprKind::Lambda { params, body } = &body.node {
        if params.len() != 2 {
            return None;
        }
        let p0 = match &params[0].node {
            ast::PatKind::Var(n) => n.clone(),
            _ => return None,
        };
        let p1 = match &params[1].node {
            ast::PatKind::Var(n) => n.clone(),
            _ => return None,
        };
        // \a b -> diff a b  →  App(App(Var("diff"), Var(a)), Var(b))
        if is_diff_app2(body, &p0, &p1) {
            return Some([p0, p1]);
        }
        // \a b -> b |> diff a  →  BinOp(Var(b), Pipe, App(Var("diff"), Var(a)))
        // which is semantically diff a b (a=base positive, b=subtracted negative)
        if let ast::ExprKind::BinOp { lhs, rhs, op } = &body.node
            && *op == ast::BinOp::Pipe
            && matches!(&lhs.node, ast::ExprKind::Var(n) if n == &p1)
        {
            let rhs = strip_head_wrappers(rhs);
            if let ast::ExprKind::App { func, arg } = &rhs.node
                && matches!(&strip_head_wrappers(func).node, ast::ExprKind::Var(n) if n == "diff")
                && matches!(&arg.node, ast::ExprKind::Var(n) if n == &p0)
            {
                return Some([p0, p1]);
            }
        }
    }
    None
}

/// Check if `expr` is `App(App(Var("diff"), Var(p0)), Var(p1))`.
fn is_diff_app2(expr: &ast::Expr, p0: &str, p1: &str) -> bool {
    let expr = strip_head_wrappers(expr);
    if let ast::ExprKind::App { func, arg } = &expr.node {
        let is_arg_p1 = matches!(&arg.node, ast::ExprKind::Var(n) if n == p1);
        let inner = strip_head_wrappers(func);
        if let ast::ExprKind::App { func: head, arg: first } = &inner.node {
            let is_diff = matches!(&strip_head_wrappers(head).node, ast::ExprKind::Var(n) if n == "diff");
            let is_first_p0 = matches!(&first.node, ast::ExprKind::Var(n) if n == p0);
            return is_diff && is_first_p0 && is_arg_p1;
        }
    }
    false
}

/// Walk an expression and collect derived-ref/view-ref edges with polarity.
///
/// `diff a b` makes `b` negative (it's the subtracted set). `a` stays positive.
/// Everything else propagates the current polarity unchanged.
///
/// `env` maps local variables bound *directly* to a node (`self <- &bad` /
/// `let self = &bad` / `r <- *view` / `let r = *view`) to that node's name. A
/// negative use of such a variable (`diff all self`) then records a negative
/// edge to the node — without this, self-negation laundered through a bind
/// escapes the stratification check (a bare `diff *all &bad` doesn't even
/// type-check, since `&bad` is `IO`-typed inside a derived body; it must be
/// bound first), so the oscillating fixpoint would only be caught at runtime
/// after 10000 iterations.
fn collect_edges(
    expr: &ast::Expr,
    polarity: Polarity,
    node_names: &HashSet<String>,
    env: &HashMap<String, String>,
    partial_diffs: &HashMap<String, ast::Expr>,
    diff_wrappers: &HashMap<String, [String; 2]>,
    out: &mut Vec<Edge>,
) {
    match &expr.node {
        ast::ExprKind::DerivedRef(name) if node_names.contains(name) => {
            out.push(Edge { target: name.clone(), polarity, span: expr.span });
        }
        // A source-read from a *view* creates an edge too. Views are nodes
        // (their bodies can in turn reference derived relations, forming
        // cycles); ordinary user sources are not nodes, so non-view
        // SourceRefs fall through and contribute no edge.
        ast::ExprKind::SourceRef(name) if node_names.contains(name) => {
            out.push(Edge { target: name.clone(), polarity, span: expr.span });
        }
        // A variable aliasing a node carries that node's dependency at the
        // variable's current polarity.
        ast::ExprKind::Var(name) => {
            // Skip the `diff`-alias sentinel: such a variable is a function
            // value, not a relation reference, so it contributes no edge here
            // (its negation effect is handled at the application site).
            if let Some(node) = env.get(name)
                && node != DIFF_ALIAS
            {
                out.push(Edge { target: node.clone(), polarity, span: expr.span });
            }
        }
        ast::ExprKind::DerivedRef(_)
        | ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_) => {}

        // `diff` is a curried 2-arg stdlib function. In the AST after
        // desugaring it appears as `App(App(Var("diff"), a), b)`.
        // `a` is the base set (positive), `b` is subtracted (negative).
        //
        // A user-defined wrapper like `minus = \a b -> diff a b` has the
        // same call shape `App(App(Var("minus"), a), b)` — detect it via
        // `diff_wrappers` and treat identically to a direct `diff` call
        // (B31: negation laundered through a user wrapper).
        ast::ExprKind::App { func, arg } => {
            // Fully-applied user diff-wrapper: `minus a b` → diff a b.
            // The wrapper takes exactly 2 args; if the outer App's func is
            // `App(Var("minus"), base)`, we have the full application.
            if let ast::ExprKind::App { func: inner, arg: base } = &strip_head_wrappers(func).node
                && let ast::ExprKind::Var(name) = &strip_head_wrappers(inner).node
                && diff_wrappers.contains_key(name)
            {
                collect_edges(base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Diff);
                collect_edges(arg, neg, node_names, env, partial_diffs, diff_wrappers, out);
            }
            // Direct partial application: `(diff a) b` or `(d) b` where `d`
            // was bound to bare `diff` via `let d = diff`.
            else if let Some(first_arg) = is_diff_applied_once(func, env) {
                // This is `(diff a) b` — `a` keeps polarity, `b` is negated.
                collect_edges(first_arg, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Diff);
                collect_edges(arg, neg, node_names, env, partial_diffs, diff_wrappers, out);
            }
            // Let-bound partial application: `let d = diff X` followed by
            // `d Y`. The App head is `Var("d")`, not an `App`, so the direct
            // check above misses it. Look up `d` in `partial_diffs` to recover
            // the bound positive base `X`, then treat `Y` as the subtracted
            // (negative) argument.
            else if let ast::ExprKind::Var(name) = &strip_head_wrappers(func).node
                && let Some(bound_base) = partial_diffs.get(name)
            {
                collect_edges(bound_base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Diff);
                collect_edges(arg, neg, node_names, env, partial_diffs, diff_wrappers, out);
            }
            // A non-monotone aggregate over the relation: `count self`,
            // `sum f self`, `minOn f self`, ... `arg` here is the aggregate's
            // relation argument (always the last one). Aggregates collapse the
            // relation to a scalar whose value changes as the relation grows,
            // so a recursive body gated on one never converges — record the
            // relation arg as a negative edge so the cycle is rejected.
            else if completes_aggregate_over_relation(func) {
                // The selector/predicate arg (in `func`) stays positive.
                collect_edges(func, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Aggregate);
                collect_edges(arg, neg, node_names, env, partial_diffs, diff_wrappers, out);
            } else {
                collect_edges(func, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                collect_edges(arg, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }

        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_edges(&f.value, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_edges(base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            for f in fields {
                collect_edges(&f.value, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_edges(expr, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_edges(e, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }
        ast::ExprKind::Lambda { params, body } => {
            // A lambda parameter shadows any outer alias of the same name.
            let mut inner = env.clone();
            let mut inner_partial = partial_diffs.clone();
            for p in params {
                if let ast::PatKind::Var(v) = &p.node {
                    inner.remove(v);
                    inner_partial.remove(v);
                }
            }
            collect_edges(body, polarity, node_names, &inner, &inner_partial, diff_wrappers, out);
        }
        ast::ExprKind::BinOp { lhs, rhs, op } => {
            // `a |> f` is `f a`. If `f` is a partially-applied `diff` (i.e.,
            // `diff base`), then `a` becomes the subtracted (negative) arg.
            if *op == ast::BinOp::Pipe
                && let Some(base) = is_diff_applied_once(rhs, env) {
                    collect_edges(base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                    let neg = negate(polarity, NegCause::Diff);
                    collect_edges(lhs, neg, node_names, env, partial_diffs, diff_wrappers, out);
                    return;
                }
            // Pipe into a let-bound partial diff: `a |> d` where `let d = diff X`.
            if *op == ast::BinOp::Pipe
                && let ast::ExprKind::Var(name) = &strip_head_wrappers(rhs).node
                && let Some(bound_base) = partial_diffs.get(name)
            {
                collect_edges(bound_base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Diff);
                collect_edges(lhs, neg, node_names, env, partial_diffs, diff_wrappers, out);
                return;
            }
            // Pipe into a non-monotone aggregate: `self |> count`,
            // `rel |> minOn f`. Since `a |> f` is `f a`, if `rhs` needs exactly
            // one more argument to complete an aggregate, `lhs` is that
            // aggregate's relation argument — negative.
            if *op == ast::BinOp::Pipe && completes_aggregate_over_relation(rhs) {
                collect_edges(rhs, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Aggregate);
                collect_edges(lhs, neg, node_names, env, partial_diffs, diff_wrappers, out);
                return;
            }
            // Pipe into a user diff-wrapper: `a |> minus b` where
            // `minus = \x y -> diff x y`. This is `minus b a` = `diff b a`,
            // so `b` (the partial) is positive and `a` (the piped lhs) is
            // the subtracted (negative) argument.
            if *op == ast::BinOp::Pipe
                && let ast::ExprKind::App { func: inner, arg: base } = &strip_head_wrappers(rhs).node
                && let ast::ExprKind::Var(name) = &strip_head_wrappers(inner).node
                && diff_wrappers.contains_key(name)
            {
                collect_edges(base, polarity, node_names, env, partial_diffs, diff_wrappers, out);
                let neg = negate(polarity, NegCause::Diff);
                collect_edges(lhs, neg, node_names, env, partial_diffs, diff_wrappers, out);
                return;
            }
            collect_edges(lhs, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            collect_edges(rhs, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::UnaryOp { op, operand } => {
            // `not` is boolean negation. When it wraps a non-monotone
            // check over a relation (e.g. `not (any p rel)`, `not (elem x rel)`),
            // it is negation-as-failure and must create a negative edge —
            // flip polarity before recursing, mirroring how `diff` works.
            // Double negation (`not (not ...)`) cancels back to positive.
            let child_polarity = if *op == ast::UnaryOp::Not {
                negate(polarity, NegCause::Not)
            } else {
                polarity
            };
            collect_edges(operand, child_polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            collect_edges(cond, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            collect_edges(then_branch, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            collect_edges(else_branch, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_edges(scrutinee, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            for arm in arms {
                collect_edges(&arm.body, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            // Bindings are visible to the statements that follow them, so
            // accumulate derived-relation aliases into a local env as we go.
            let mut local_env = env.clone();
            let mut local_partial = partial_diffs.clone();
            for s in stmts {
                match &s.node {
                    ast::StmtKind::Bind { pat, expr } => {
                        collect_edges(expr, polarity, node_names, &local_env, &local_partial, diff_wrappers, out);
                        bind_derived_alias(pat, expr, node_names, &mut local_env, &mut local_partial);
                    }
                    ast::StmtKind::Let { pat, expr } => {
                        collect_edges(expr, polarity, node_names, &local_env, &local_partial, diff_wrappers, out);
                        bind_derived_alias(pat, expr, node_names, &mut local_env, &mut local_partial);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_edges(cond, polarity, node_names, &local_env, &local_partial, diff_wrappers, out);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_edges(key, polarity, node_names, &local_env, &local_partial, diff_wrappers, out);
                    }
                    ast::StmtKind::Expr(e) => {
                        collect_edges(e, polarity, node_names, &local_env, &local_partial, diff_wrappers, out);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(inner) => {
            collect_edges(inner, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            collect_edges(target, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            collect_edges(value, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::UnitLit { value, .. } | ast::ExprKind::TimeUnitLit { value, .. } => {
            collect_edges(value, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::Annot { expr: inner, .. } => {
            collect_edges(inner, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::Refine(inner) => {
            collect_edges(inner, polarity, node_names, env, partial_diffs, diff_wrappers, out);
        }
        ast::ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                collect_edges(&h.body, polarity, node_names, env, partial_diffs, diff_wrappers, out);
            }
        }
    }
}

/// If `pat` binds a simple variable and `expr` refers directly to a known
/// derived relation, record `var -> derived` so a later negative use of the
/// variable is attributed to that relation. Any other binding of the same
/// variable name clears a stale alias (shadowing).
///
/// `partial_diffs` tracks let-bindings of partially-applied `diff`
/// (`let d = diff X`). The bare-`diff` alias (`let d = diff`) goes into `env`
/// under the DIFF_ALIAS sentinel; a *partially-applied* `diff` stores the
/// bound positive base in `partial_diffs` so the App arm of `collect_edges`
/// can recover the `(diff X) Y` shape from a syntactic `d Y`.
fn bind_derived_alias(
    pat: &ast::Pat,
    expr: &ast::Expr,
    node_names: &HashSet<String>,
    env: &mut HashMap<String, String>,
    partial_diffs: &mut HashMap<String, ast::Expr>,
) {
    if let ast::PatKind::Var(v) = &pat.node {
        let stripped = strip_head_wrappers(expr);
        match &stripped.node {
            ast::ExprKind::DerivedRef(name) if node_names.contains(name) => {
                env.insert(v.clone(), name.clone());
                partial_diffs.remove(v);
            }
            // `let d = diff` — track the alias so a later `d all self` is still
            // recognized as set difference (negation) at its application site.
            ast::ExprKind::Var(name) if name == "diff" => {
                env.insert(v.clone(), DIFF_ALIAS.to_string());
                partial_diffs.remove(v);
            }
            // `let d = diff X` — partially-applied diff. Store the bound
            // positive base `X` so a later `d Y` is recognized as
            // `(diff X) Y` (X positive, Y negative).
            ast::ExprKind::App { func, arg: base }
                if matches!(&strip_head_wrappers(func).node, ast::ExprKind::Var(n) if n == "diff") =>
            {
                partial_diffs.insert(v.clone(), (**base).clone());
                env.remove(v);
            }
            // Aliasing another already-aliased variable (`let y = x`) carries
            // the alias transitively; anything else drops a stale mapping.
            ast::ExprKind::Var(other) if env.contains_key(other) => {
                let target = env[other].clone();
                env.insert(v.clone(), target);
                partial_diffs.remove(v);
            }
            ast::ExprKind::Var(other) if partial_diffs.contains_key(other) => {
                let target = partial_diffs[other].clone();
                partial_diffs.insert(v.clone(), target);
                env.remove(v);
            }
            _ => {
                env.remove(v);
                partial_diffs.remove(v);
            }
        }
    }
}

/// Sentinel stored in the alias `env` to mark a local variable bound to the
/// `diff` builtin (`let d = diff`). `\0` can't appear in a real relation name,
/// so it never collides with a derived-relation alias value.
const DIFF_ALIAS: &str = "\0diff";

/// Strip the expression wrappers that are transparent for negation analysis —
/// a type annotation, a unit ascription, or a `refine` — mirroring
/// `effects::head_name`, so `(diff a : T)`, `diff a <unit>`, and
/// `refine (diff a)` are still recognized as `diff` applications.
fn strip_head_wrappers(expr: &ast::Expr) -> &ast::Expr {
    match &expr.node {
        ast::ExprKind::Annot { expr: inner, .. } => strip_head_wrappers(inner),
        ast::ExprKind::UnitLit { value, .. } | ast::ExprKind::TimeUnitLit { value, .. } => strip_head_wrappers(value),
        ast::ExprKind::Refine(inner) => strip_head_wrappers(inner),
        _ => expr,
    }
}

/// Check if `expr` is a single application of `diff` (the set-difference
/// builtin) — `App(Var("diff"), arg)` — returning the subtracted `arg` if so.
/// Transparent wrappers around the application or its head are stripped, and a
/// local variable aliased to `diff` (`let d = diff`, tracked in `env` via the
/// `DIFF_ALIAS` sentinel) is recognized too.
fn is_diff_applied_once<'a>(
    expr: &'a ast::Expr,
    env: &HashMap<String, String>,
) -> Option<&'a ast::Expr> {
    let expr = strip_head_wrappers(expr);
    if let ast::ExprKind::App { func, arg } = &expr.node {
        let func = strip_head_wrappers(func);
        if let ast::ExprKind::Var(name) = &func.node
            && (name == "diff" || env.get(name).map(String::as_str) == Some(DIFF_ALIAS))
        {
            return Some(arg);
        }
    }
    None
}

/// Flip polarity. Producing a *negative* edge needs a `cause` describing the
/// non-monotone operation responsible (used only when the result is
/// `Negative`); double negation back to `Positive` ignores it.
fn negate(p: Polarity, cause: NegCause) -> Polarity {
    match p {
        Polarity::Positive => Polarity::Negative(cause),
        Polarity::Negative(_) => Polarity::Positive,
    }
}

/// Check if an expression is a call to a non-monotone builtin over a
/// relation: the six aggregates (count, sum, avg, minOn, maxOn,
/// countWhere) and `all` (anti-monotone: ∀ over a growing set can flip
/// true→false). These must create negative edges when applied to a
/// self-referential relation.
fn completes_aggregate_over_relation(expr: &ast::Expr) -> bool {
    let expr = strip_head_wrappers(expr);
    match &expr.node {
        ast::ExprKind::Var(name) => {
            matches!(name.as_str(), "count" | "sum" | "avg" | "minOn" | "maxOn" | "countWhere" | "all" | "fold" | "single")
        }
        ast::ExprKind::App { func, .. } => completes_aggregate_over_relation(func),
        _ => false,
    }
}

// ── Tarjan's SCC ────────────────────────────────────────────────

struct Tarjan<'a> {
    edges: &'a HashMap<String, Vec<Edge>>,
    index_counter: usize,
    index: HashMap<String, usize>,
    lowlink: HashMap<String, usize>,
    on_stack: HashSet<String>,
    stack: Vec<String>,
    sccs: Vec<Vec<String>>,
}

impl<'a> Tarjan<'a> {
    fn run(edges: &'a HashMap<String, Vec<Edge>>) -> Vec<Vec<String>> {
        let mut t = Tarjan {
            edges,
            index_counter: 0,
            index: HashMap::new(),
            lowlink: HashMap::new(),
            on_stack: HashSet::new(),
            stack: Vec::new(),
            sccs: Vec::new(),
        };
        let nodes: Vec<String> = edges.keys().cloned().collect();
        for node in &nodes {
            if !t.index.contains_key(node) {
                t.strongconnect(node.clone());
            }
        }
        t.sccs
    }

    fn strongconnect(&mut self, v: String) {
        // Iterative formulation of Tarjan's SCC to avoid unbounded recursion
        // on deep derived-relation dependency graphs. The work stack holds
        // `(node, next_edge_idx)` so we can resume iterating a node's edges
        // after recursing into a successor.
        let mut work: Vec<(String, usize)> = Vec::new();
        self.start_node(&v);
        work.push((v, 0));

        // Peek at the top of the work stack without holding a borrow
        // across the body (we need to mutate `self` and `work` below).
        while let Some((n, i)) = work.last() {
            let (top_node, top_idx) = (n.clone(), *i);
            let edges = self.edges.get(&top_node).cloned();
            let edges = match edges {
                Some(e) => e,
                None => {
                    // No edges: pop and finish this node.
                    let finished = work.pop().unwrap().0;
                    self.finish_node(&finished);
                    continue;
                }
            };
            // Advance to the next unprocessed edge.
            let mut idx = top_idx;
            let mut pushed = false;
            while idx < edges.len() {
                let target = &edges[idx];
                if !self.index.contains_key(&target.target) {
                    // Unvisited successor: recurse by pushing onto the work
                    // stack. Save the advanced index so we resume after it.
                    if let Some((_, ei)) = work.last_mut() {
                        *ei = idx + 1;
                    }
                    self.start_node(&target.target);
                    work.push((target.target.clone(), 0));
                    pushed = true;
                    break;
                } else if self.on_stack.contains(&target.target) {
                    let t_idx = self.index[&target.target];
                    let cur = self.lowlink[&top_node];
                    self.lowlink.insert(top_node.clone(), cur.min(t_idx));
                }
                idx += 1;
            }
            if pushed {
                continue;
            }
            // All edges processed: update parent's lowlink, pop, and finish.
            let ll = self.lowlink[&top_node];
            work.pop();
            if let Some((parent, _)) = work.last() {
                let cur = self.lowlink[parent];
                self.lowlink.insert(parent.clone(), cur.min(ll));
            }
            self.finish_node(&top_node);
        }
    }

    fn start_node(&mut self, v: &str) {
        self.index.insert(v.to_string(), self.index_counter);
        self.lowlink.insert(v.to_string(), self.index_counter);
        self.index_counter += 1;
        self.stack.push(v.to_string());
        self.on_stack.insert(v.to_string());
    }

    fn finish_node(&mut self, v: &str) {
        if self.lowlink[v] == self.index[v] {
            let mut scc = Vec::new();
            loop {
                let w = self
                    .stack
                    .pop()
                    .expect("Tarjan SCC: stack should not be empty when lowlink == index");
                self.on_stack.remove(&w);
                scc.push(w.clone());
                if w == v {
                    break;
                }
            }
            self.sccs.push(scc);
        }
    }
}

// ── Main check ──────────────────────────────────────────────────

/// Check that all recursive derived relations are stratifiable.
///
/// Returns diagnostics for any cycle that contains a negative (diff) edge.
///
/// Runs on a grown stack — building the dependency graph walks expression
/// bodies recursively, including desugared `do` chains.
pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    crate::stack::grow(|| check_inner(module))
}

fn check_inner(module: &ast::Module) -> Vec<Diagnostic> {
    let (_, edges) = build_dependency_graph(module);
    let sccs = Tarjan::run(&edges);

    // Collect declaration spans for error reporting, and track which nodes are
    // views. Views and derived relations share the dependency graph, but they
    // compile very differently on a self-loop: a self-recursive *derived*
    // relation is legal (codegen emits a `knot_relation_fixpoint` wrapper),
    // whereas a self-recursive *view* has no fixpoint codegen — so we need to
    // tell the two apart when a single-node cycle is found below.
    let mut decl_spans: HashMap<String, Span> = HashMap::new();
    let mut view_names: HashSet<String> = HashSet::new();
    for decl in &module.decls {
        match &decl.node {
            ast::DeclKind::Derived { name, .. } => {
                decl_spans.insert(name.clone(), decl.span);
            }
            ast::DeclKind::View { name, .. } => {
                decl_spans.insert(name.clone(), decl.span);
                view_names.insert(name.clone());
            }
            _ => {}
        }
    }

    let mut diagnostics = Vec::new();

    for scc in &sccs {
        // Only consider non-trivial SCCs (cycles) or self-loops.
        let is_cycle = if scc.len() == 1 {
            // Check for self-edge.
            let name = &scc[0];
            edges.get(name).is_some_and(|es| es.iter().any(|e| &e.target == name))
        } else {
            true
        };
        if !is_cycle {
            continue;
        }

        // Check if any edge within this SCC is negative.
        let scc_set: HashSet<&String> = scc.iter().collect();
        let mut scc_has_negative = false;
        for name in scc {
            if let Some(es) = edges.get(name) {
                for edge in es {
                    if let Polarity::Negative(cause) = edge.polarity
                        && scc_set.contains(&edge.target)
                    {
                        scc_has_negative = true;
                        let mut diag = Diagnostic::error(format!(
                            "unstratifiable recursion: `&{}` negates `&{}` through {}",
                            name,
                            edge.target,
                            cause.operation(),
                        ))
                        .label(edge.span, format!("negative dependency via {}", cause.operation()));

                        if let Some(&decl_span) = decl_spans.get(name) {
                            diag = diag.label(decl_span, format!("`&{}` defined here", name));
                        }

                        if name == &edge.target {
                            diag = diag.note(match cause {
                                NegCause::Diff =>
                                    "a derived relation cannot subtract from itself in a recursive definition — \
                                     the fixpoint would oscillate instead of converging",
                                NegCause::Not =>
                                    "a derived relation cannot negate itself in a recursive definition — \
                                     the negation is non-monotone, so the fixpoint \
                                     oscillates instead of converging",
                                NegCause::Aggregate =>
                                    "a derived relation cannot aggregate over itself in a recursive definition — \
                                     the aggregate's value shifts as the relation grows, so the fixpoint \
                                     oscillates instead of converging",
                            });
                        } else {
                            diag = diag.note(format!(
                                "`&{}` and `&{}` are mutually recursive; {} across a \
                                 recursive cycle has no well-defined fixpoint",
                                name,
                                edge.target,
                                cause.gerund(),
                            ));
                        }

                        diag = diag.note(format!(
                            "split the {} into a separate, non-recursive derived relation",
                            cause.noun(),
                        ));

                        diagnostics.push(diag);
                    }
                }
            }
        }

        // Multi-relation (mutual / indirect) recursion. Codegen only emits a
        // `knot_relation_fixpoint` wrapper for a *single* self-recursive
        // derived relation (detected syntactically by the same name appearing
        // in its own body). A cycle spanning two or more relations is not
        // detected there, so each relation compiles as an ordinary recompute
        // and calls its peer, which calls back — unbounded mutual recursion
        // that stack-overflows at runtime instead of converging to a Datalog
        // fixpoint. Reject it with a clear diagnostic. (A cycle already
        // carrying a negative edge is reported as unstratifiable above; don't
        // pile on a second message.)
        if scc.len() >= 2 && !scc_has_negative {
            let mut names: Vec<String> = scc.clone();
            names.sort();
            let list = names
                .iter()
                .map(|n| format!("`&{}`", n))
                .collect::<Vec<_>>()
                .join(", ");
            let mut diag = Diagnostic::error(format!(
                "mutually recursive derived relations are not supported: {}",
                list
            ));
            // Attach the label to the first cycle member that actually has a
            // recorded span. `decl_spans` only holds derived relations, so the
            // alphabetically-first member may be absent (e.g. a view in the
            // cycle) — `find_map` skips those instead of dropping the label.
            if let Some(&decl_span) = names.iter().find_map(|n| decl_spans.get(n)) {
                diag = diag.label(decl_span, "part of a mutually recursive cycle");
            }
            diag = diag.note(
                "codegen computes a fixpoint only for a single self-recursive derived \
                 relation; a mutual cycle recomputes its peers without converging, \
                 overflowing the stack at runtime",
            );
            diag = diag.note(
                "combine the relations into one self-recursive derived relation, or make \
                 one of them non-recursive",
            );
            diagnostics.push(diag);
        }

        // Single-node positive self-loop on a *view* (`*v = do { r <- *v; … }`).
        // A self-recursive derived relation is handled by codegen's
        // `knot_relation_fixpoint` wrapper, but a view has no such path:
        // `analyze_view` treats the self-read as the view's own base source, so
        // the generated query targets a `_knot_<view>` table that is never
        // created and the program panics at runtime with "no such table". Catch
        // it here as a compile error instead. (A *negative* self-loop is already
        // reported as unstratifiable above; multi-node cycles fall into the
        // mutual-recursion branch above — this handles only the direct,
        // single-node view self-loop those two branches miss.)
        if scc.len() == 1 && !scc_has_negative && view_names.contains(&scc[0]) {
            let name = &scc[0];
            let mut diag = Diagnostic::error(format!(
                "self-recursive view is not supported: `*{}` reads from itself",
                name
            ));
            if let Some(&decl_span) = decl_spans.get(name) {
                diag = diag.label(decl_span, "this view reads from itself");
            }
            diag = diag.note(
                "a view is a bidirectional query over concrete sources, not a \
                 fixpoint-iterated relation — there is no table backing the view \
                 itself to read from",
            );
            diag = diag.note(
                "use a recursive derived relation (`&…`) for fixpoint iteration, or \
                 rewrite the view to read only from concrete sources",
            );
            diagnostics.push(diag);
        }
    }

    diagnostics
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use knot::ast::*;

    fn span() -> Span {
        Span::new(0, 0)
    }

    fn spanned<T>(node: T) -> Spanned<T> {
        Spanned::new(node, span())
    }

    fn derived(name: &str, body: Expr) -> Decl {
        Decl { node: DeclKind::Derived { name: name.to_string(), ty: None, body }, span: span(), exported: false }
    }

    fn view(name: &str, body: Expr) -> Decl {
        Decl { node: DeclKind::View { name: name.to_string(), ty: None, body }, span: span(), exported: false }
    }

    fn fun(name: &str, body: Expr) -> Decl {
        Decl { node: DeclKind::Fun { name: name.to_string(), ty: None, body: Some(body) }, span: span(), exported: false }
    }

    fn lambda(params: Vec<&str>, body: Expr) -> Expr {
        let params: Vec<Spanned<PatKind>> = params
            .into_iter()
            .map(|p| spanned(PatKind::Var(p.to_string())))
            .collect();
        spanned(ExprKind::Lambda { params, body: Box::new(body) })
    }

    fn derived_ref(name: &str) -> Expr {
        spanned(ExprKind::DerivedRef(name.to_string()))
    }

    fn var(name: &str) -> Expr {
        spanned(ExprKind::Var(name.to_string()))
    }

    fn app(func: Expr, arg: Expr) -> Expr {
        spanned(ExprKind::App { func: Box::new(func), arg: Box::new(arg) })
    }

    fn source_ref(name: &str) -> Expr {
        spanned(ExprKind::SourceRef(name.to_string()))
    }

    fn module(decls: Vec<Decl>) -> Module {
        Module { imports: Vec::new(), decls }
    }

    /// `diff a b` → `App(App(Var("diff"), a), b)`
    fn diff(a: Expr, b: Expr) -> Expr {
        app(app(var("diff"), a), b)
    }

    /// `union a b` → `App(App(Var("union"), a), b)`
    fn union(a: Expr, b: Expr) -> Expr {
        app(app(var("union"), a), b)
    }

    #[test]
    fn positive_self_recursion_is_ok() {
        // &reach = union *edges &reach
        let m = module(vec![
            derived("reach", union(source_ref("edges"), derived_ref("reach"))),
        ]);
        let diags = check(&m);
        assert!(diags.is_empty(), "positive self-recursion should be fine");
    }

    /// `count r` → `App(Var("count"), r)`
    fn count(r: Expr) -> Expr {
        app(var("count"), r)
    }

    fn where_stmt(cond: Expr) -> Spanned<StmtKind> {
        spanned(StmtKind::Where { cond })
    }

    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        spanned(ExprKind::BinOp { lhs: Box::new(lhs), rhs: Box::new(rhs), op: BinOp::Eq })
    }

    fn int_lit(n: i64) -> Expr {
        spanned(ExprKind::Lit(Literal::Int(n.to_string())))
    }

    #[test]
    fn negative_self_recursion_is_rejected() {
        // &bad = diff *all &bad
        let m = module(vec![
            derived("bad", diff(source_ref("all"), derived_ref("bad"))),
        ]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unstratifiable"));
        assert!(diags[0].message.contains("&bad"));
        // The cause here really is `diff`, so it must be the one named.
        assert!(diags[0].message.contains("`diff`"), "got: {}", diags[0].message);
        assert!(!diags[0].message.contains("aggregate"), "got: {}", diags[0].message);
    }

    #[test]
    fn aggregate_self_recursion_blames_the_aggregate_not_diff() {
        // &bad = do
        //   self <- &bad
        //   where count self == 0
        //   x    <- *items
        //   yield x
        //
        // The negative edge comes from `count` collapsing the recursive relation
        // to a scalar — there is no `diff` anywhere in this program, so the
        // diagnostic must name the aggregate rather than blaming `diff`.
        let body = do_expr(vec![
            bind_stmt("self", derived_ref("bad")),
            where_stmt(eq(count(var("self")), int_lit(0))),
            bind_stmt("x", source_ref("items")),
            yield_stmt(var("x")),
        ]);
        let m = module(vec![derived("bad", body)]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1, "one unstratifiable-recursion error");

        let diag = &diags[0];
        assert!(diag.message.contains("unstratifiable"), "got: {}", diag.message);
        assert!(diag.message.contains("&bad"), "got: {}", diag.message);
        assert!(
            diag.message.contains("aggregate") && diag.message.contains("count"),
            "message must name the aggregate that caused the negative edge, got: {}",
            diag.message
        );
        assert!(
            !diag.message.contains("`diff`"),
            "no `diff` in this program — it must not be blamed, got: {}",
            diag.message
        );
        assert!(
            diag.labels.iter().any(|l| l.message.contains("aggregate")),
            "label must name the aggregate, got: {:?}",
            diag.labels.iter().map(|l| &l.message).collect::<Vec<_>>()
        );
        assert!(
            diag.notes.iter().any(|n| n.contains("aggregate over itself")),
            "self-aggregate note expected, got: {:?}",
            diag.notes
        );
        assert!(
            diag.notes.iter().all(|n| !n.contains("subtract")),
            "must not suggest a subtraction that isn't there, got: {:?}",
            diag.notes
        );
    }

    #[test]
    fn pipe_aggregate_mutual_recursion_blames_the_aggregate() {
        // &a = do { self <- &b; where (self |> count) == 0; x <- *items; yield x }
        // &b = union *source &a
        //
        // Same cause reached through the pipe form, across a mutual cycle: the
        // note must talk about aggregating, not negating.
        let body = do_expr(vec![
            bind_stmt("self", derived_ref("b")),
            where_stmt(eq(
                spanned(ExprKind::BinOp {
                    lhs: Box::new(var("self")),
                    rhs: Box::new(var("count")),
                    op: BinOp::Pipe,
                }),
                int_lit(0),
            )),
            bind_stmt("x", source_ref("items")),
            yield_stmt(var("x")),
        ]);
        let m = module(vec![
            derived("a", body),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1, "one unstratifiable-recursion error");

        let diag = &diags[0];
        assert!(diag.message.contains("unstratifiable"), "got: {}", diag.message);
        assert!(diag.message.contains("aggregate"), "got: {}", diag.message);
        assert!(!diag.message.contains("`diff`"), "got: {}", diag.message);
        assert!(
            diag.notes.iter().any(|n| n.contains("aggregating across")),
            "mutual-cycle note must describe aggregation, got: {:?}",
            diag.notes
        );
    }

    #[test]
    fn negative_dep_on_non_recursive_is_ok() {
        // &a = *source
        // &b = diff *all &a
        let m = module(vec![
            derived("a", source_ref("source")),
            derived("b", diff(source_ref("all"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert!(diags.is_empty(), "negating a non-recursive dep is fine");
    }

    #[test]
    fn mutual_recursion_with_negation_is_rejected() {
        // &a = diff *source &b
        // &b = union *source &a
        let m = module(vec![
            derived("a", diff(source_ref("source"), derived_ref("b"))),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert!(!diags.is_empty(), "negative edge in mutual cycle should fail");
    }

    #[test]
    fn mutual_recursion_all_positive_is_rejected_as_unsupported() {
        // &a = union *source &b
        // &b = union *source &a
        // Positive mutual recursion is well-defined in Datalog, but codegen
        // only emits a fixpoint for a *single* self-recursive relation — a
        // mutual cycle compiles to unbounded mutual recompute (stack
        // overflow). It must be rejected with a clear diagnostic rather than
        // silently miscompiled.
        let m = module(vec![
            derived("a", union(source_ref("source"), derived_ref("b"))),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1, "one unsupported-mutual-recursion error");
        assert!(
            diags[0].message.contains("mutually recursive"),
            "expected mutual-recursion diagnostic, got: {}",
            diags[0].message
        );
        assert!(
            !diags[0].message.contains("unstratifiable"),
            "positive cycle is unsupported, not unstratifiable"
        );
    }

    #[test]
    fn indirect_positive_cycle_is_rejected() {
        // &a = union *s &b ; &b = union *s &c ; &c = union *s &a
        // A 3-relation cycle: none is directly self-recursive, so codegen's
        // syntactic self-ref check misses all three and would emit looping
        // recompute. The SCC detection must catch the whole cycle.
        let m = module(vec![
            derived("a", union(source_ref("s"), derived_ref("b"))),
            derived("b", union(source_ref("s"), derived_ref("c"))),
            derived("c", union(source_ref("s"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("mutually recursive"));
        assert!(diags[0].message.contains("&a"));
        assert!(diags[0].message.contains("&c"));
    }

    #[test]
    fn diff_first_arg_stays_positive() {
        // &a = diff &a *source  (self-ref in positive position of diff, source in negative)
        let m = module(vec![
            derived("a", diff(derived_ref("a"), source_ref("source"))),
        ]);
        let diags = check(&m);
        assert!(diags.is_empty(), "self-ref as first arg of diff is positive");
    }

    #[test]
    fn boolean_not_does_not_create_negative_dep() {
        // &a = if not (&b == []) then *source else []
        // `not` is boolean negation, not set complement — &b is read
        // positively (just checking emptiness), so this should NOT create
        // a negative edge.
        use knot::ast::{ExprKind, UnaryOp};
        let not_expr = spanned(ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(spanned(ExprKind::BinOp {
                lhs: Box::new(derived_ref("b")),
                rhs: Box::new(spanned(ExprKind::List(vec![]))),
                op: BinOp::Eq,
            })),
        });
        let if_expr = spanned(ExprKind::If {
            cond: Box::new(not_expr),
            then_branch: Box::new(source_ref("source")),
            else_branch: Box::new(spanned(ExprKind::List(vec![]))),
        });
        let m = module(vec![
            derived("a", if_expr),
            derived("b", source_ref("source")),
        ]);
        let diags = check(&m);
        // If `not` incorrectly created a negative edge, &a → &b would be
        // negative, and &b → *source is positive, so no cycle — this
        // particular test doesn't produce a cycle.  But if &b also
        // depended on &a, a false negative edge would create a spurious
        // unstratifiable error.  The mutual case is tested below.
        assert!(diags.is_empty(), "boolean `not` should not create negative dep");
    }

    #[test]
    fn boolean_not_in_mutual_recursion_is_unstratifiable() {
        // &a depends on &b through `not`, and &b depends on &a.
        // `not` is negation-as-failure: it creates a negative edge.
        // The cycle &a →(neg) &b →(pos) &a is unstratifiable.
        use knot::ast::{ExprKind, UnaryOp};
        let not_expr = spanned(ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(spanned(ExprKind::BinOp {
                lhs: Box::new(derived_ref("b")),
                rhs: Box::new(spanned(ExprKind::List(vec![]))),
                op: BinOp::Eq,
            })),
        });
        let if_a = spanned(ExprKind::If {
            cond: Box::new(not_expr),
            then_branch: Box::new(source_ref("source")),
            else_branch: Box::new(spanned(ExprKind::List(vec![]))),
        });
        let m = module(vec![
            derived("a", if_a),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        // `not` correctly creates a negative edge, so the cycle
        // &a →(neg) &b →(pos) &a is unstratifiable.
        assert_eq!(diags.len(), 1);
        assert!(
            diags[0].message.contains("unstratifiable"),
            "boolean `not` must create a negative edge: {}",
            diags[0].message
        );
    }

    #[test]
    fn pipe_diff_creates_negative_dep() {
        // &bad = &bad |> diff *all
        // This is `diff *all &bad` — &bad is the subtracted (negative) arg.
        // The pipe form must be recognized, otherwise the negative self-edge
        // is missed and the unstratifiable recursion goes undetected.
        let pipe = spanned(ExprKind::BinOp {
            lhs: Box::new(derived_ref("bad")),
            rhs: Box::new(app(var("diff"), source_ref("all"))),
            op: BinOp::Pipe,
        });
        let m = module(vec![
            derived("bad", pipe),
        ]);
        let diags = check(&m);
        assert!(!diags.is_empty(), "pipe-diff self-recursion should be rejected");
        assert!(diags[0].message.contains("unstratifiable"));
    }

    fn var_pat(name: &str) -> Pat {
        spanned(PatKind::Var(name.to_string()))
    }

    fn bind_stmt(name: &str, expr: Expr) -> Spanned<StmtKind> {
        spanned(StmtKind::Bind { pat: var_pat(name), expr })
    }

    fn yield_stmt(expr: Expr) -> Spanned<StmtKind> {
        spanned(StmtKind::Expr(expr))
    }

    fn do_expr(stmts: Vec<Spanned<StmtKind>>) -> Expr {
        spanned(ExprKind::Do(stmts))
    }

    #[test]
    fn do_bind_self_negation_is_rejected() {
        // &bad = do
        //   self <- &bad
        //   all  <- *items
        //   d    <- diff all self
        //   yield d
        //
        // `diff` is applied to the *variable* `self` (not `&bad` directly),
        // which is how a self-negating derived relation must actually be
        // written — `diff *all &bad` doesn't type-check inside a derived body.
        // The alias `self -> &bad` must be tracked so the negative edge is
        // recorded; otherwise this compiles and oscillates forever at runtime.
        let body = do_expr(vec![
            bind_stmt("self", derived_ref("bad")),
            bind_stmt("all", source_ref("items")),
            bind_stmt("d", diff(var("all"), var("self"))),
            yield_stmt(var("d")),
        ]);
        let m = module(vec![derived("bad", body)]);
        let diags = check(&m);
        assert!(
            !diags.is_empty(),
            "self-negation laundered through a do-bind should be rejected"
        );
        assert!(diags[0].message.contains("unstratifiable"));
        assert!(diags[0].message.contains("&bad"));
    }

    #[test]
    fn do_bind_negating_non_recursive_alias_is_ok() {
        // &a = *source
        // &b = do
        //   x   <- &a
        //   all <- *items
        //   d   <- diff all x
        //   yield d
        //
        // Negating an alias of a *non-recursive* peer (&a) is a valid
        // stratified negation — no cycle, so no error.
        let body = do_expr(vec![
            bind_stmt("x", derived_ref("a")),
            bind_stmt("all", source_ref("items")),
            bind_stmt("d", diff(var("all"), var("x"))),
            yield_stmt(var("d")),
        ]);
        let m = module(vec![
            derived("a", source_ref("source")),
            derived("b", body),
        ]);
        let diags = check(&m);
        assert!(
            diags.is_empty(),
            "negating an alias of a non-recursive derived relation is fine: {:?}",
            diags.first().map(|d| &d.message)
        );
    }

    #[test]
    fn pipe_diff_mutual_creates_negative_dep() {
        // &a = &b |> diff *all   (diff *all &b — &b is negative)
        // &b = union *source &a   (&a is positive)
        // The negative edge &a → &b in a cycle should be detected.
        let pipe = spanned(ExprKind::BinOp {
            lhs: Box::new(derived_ref("b")),
            rhs: Box::new(app(var("diff"), source_ref("all"))),
            op: BinOp::Pipe,
        });
        let m = module(vec![
            derived("a", pipe),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert!(
            !diags.is_empty(),
            "pipe-diff mutual recursion should be detected as unstratifiable"
        );
    }

    #[test]
    fn self_recursive_view_is_rejected() {
        // *v = do
        //   r <- *v
        //   yield r
        //
        // A view has no fixpoint codegen (unlike a derived relation): reading
        // itself makes `analyze_view` treat the view as its own base source, so
        // the generated query hits a nonexistent `_knot_v` table and panics at
        // runtime. It must be rejected at compile time instead.
        let body = do_expr(vec![
            bind_stmt("r", source_ref("v")),
            yield_stmt(var("r")),
        ]);
        let m = module(vec![view("v", body)]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1, "one self-recursive-view error");
        assert!(
            diags[0].message.contains("self-recursive view") && diags[0].message.contains("*v"),
            "expected self-recursive-view diagnostic, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn self_recursive_derived_is_still_ok() {
        // &v = do
        //   r <- &v
        //   yield r
        //
        // The view-specific rejection must NOT fire for a derived relation — a
        // single-node positive self-loop on a derived relation is the supported
        // recursive-derived-relation feature (fixpoint codegen).
        let body = do_expr(vec![
            bind_stmt("r", derived_ref("v")),
            yield_stmt(var("r")),
        ]);
        let m = module(vec![derived("v", body)]);
        let diags = check(&m);
        assert!(
            diags.is_empty(),
            "self-recursive derived relation should compile: {:?}",
            diags.first().map(|d| &d.message)
        );
    }

    #[test]
    fn non_recursive_view_is_ok() {
        // *v = do
        //   r <- *items
        //   yield r
        //
        // Reading a *different*, concrete source is a perfectly ordinary view —
        // no self-loop, no error.
        let body = do_expr(vec![
            bind_stmt("r", source_ref("items")),
            yield_stmt(var("r")),
        ]);
        let m = module(vec![view("v", body)]);
        let diags = check(&m);
        assert!(diags.is_empty(), "non-recursive view should be fine");
    }

    #[test]
    fn view_derived_mutual_cycle_is_rejected() {
        // *v = do { x <- &d; yield x }
        // &d = *v
        //
        // A 2-node cycle spanning a view and a derived relation is caught by the
        // mutual-recursion branch (not the single-node view self-loop branch).
        let v_body = do_expr(vec![
            bind_stmt("x", derived_ref("d")),
            yield_stmt(var("x")),
        ]);
        let m = module(vec![
            view("v", v_body),
            derived("d", source_ref("v")),
        ]);
        let diags = check(&m);
        assert_eq!(diags.len(), 1, "one mutual-recursion error");
        assert!(
            diags[0].message.contains("mutually recursive"),
            "expected mutual-recursion diagnostic, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn diff_wrapper_function_is_detected() {
        // minus = \a b -> diff a b
        // &bad = minus *all &bad
        //
        // The user-defined `minus` is a curried alias of `diff`; without
        // expanding its body, `collect_edges` would see both args as positive
        // and miss the negative self-edge — the unstratifiable recursion would
        // only surface as a runtime "did not converge" panic (B31).
        let minus_body = lambda(vec!["a", "b"], diff(var("a"), var("b")));
        let m = module(vec![
            fun("minus", minus_body),
            derived("bad", app(app(var("minus"), source_ref("all")), derived_ref("bad"))),
        ]);
        let diags = check(&m);
        assert!(
            !diags.is_empty(),
            "negation laundered through a user wrapper should be rejected"
        );
        assert!(diags[0].message.contains("unstratifiable"));
        assert!(diags[0].message.contains("&bad"));
    }

    #[test]
    fn diff_wrapper_through_do_bind_is_detected() {
        // minus = \a b -> diff a b
        // &bad = do
        //   self <- &bad
        //   all  <- *items
        //   d    <- minus all self
        //   yield d
        //
        // The wrapper is called on variables (aliases), not directly on the
        // derived ref — the alias tracking must still attribute the negative
        // edge to &bad through the wrapper.
        let minus_body = lambda(vec!["a", "b"], diff(var("a"), var("b")));
        let body = do_expr(vec![
            bind_stmt("self", derived_ref("bad")),
            bind_stmt("all", source_ref("items")),
            bind_stmt("d", app(app(var("minus"), var("all")), var("self"))),
            yield_stmt(var("d")),
        ]);
        let m = module(vec![
            fun("minus", minus_body),
            derived("bad", body),
        ]);
        let diags = check(&m);
        assert!(
            !diags.is_empty(),
            "negation through a wrapper + do-bind alias should be rejected"
        );
        assert!(diags[0].message.contains("unstratifiable"));
    }

    #[test]
    fn diff_wrapper_non_recursive_is_ok() {
        // minus = \a b -> diff a b
        // &a = *source
        // &b = minus *all &a   (negating a non-recursive peer — fine)
        let minus_body = lambda(vec!["a", "b"], diff(var("a"), var("b")));
        let m = module(vec![
            fun("minus", minus_body),
            derived("a", source_ref("source")),
            derived("b", app(app(var("minus"), source_ref("all")), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert!(
            diags.is_empty(),
            "negating a non-recursive peer through a wrapper is fine: {:?}",
            diags.first().map(|d| &d.message)
        );
    }

    #[test]
    fn non_diff_wrapper_is_not_negative() {
        // myUnion = \a b -> union a b
        // &reach = myUnion *edges &reach
        //
        // A wrapper around `union` (not `diff`) should NOT create negative
        // edges — positive self-recursion through a union wrapper is fine.
        let my_union_body = lambda(vec!["a", "b"], union(var("a"), var("b")));
        let m = module(vec![
            fun("myUnion", my_union_body),
            derived("reach", app(app(var("myUnion"), source_ref("edges")), derived_ref("reach"))),
        ]);
        let diags = check(&m);
        assert!(
            diags.is_empty(),
            "positive self-recursion through a union wrapper should be fine: {:?}",
            diags.first().map(|d| &d.message)
        );
    }

    #[test]
    fn diff_wrapper_pipe_form_is_detected() {
        // minus = \a b -> diff a b
        // &bad = &bad |> minus *all
        //
        // The pipe form `x |> minus y` is `minus y x` = `diff y x` — `x`
        // (the self-ref) is the subtracted (negative) argument.
        let minus_body = lambda(vec!["a", "b"], diff(var("a"), var("b")));
        let pipe = spanned(ExprKind::BinOp {
            lhs: Box::new(derived_ref("bad")),
            rhs: Box::new(app(var("minus"), source_ref("all"))),
            op: BinOp::Pipe,
        });
        let m = module(vec![
            fun("minus", minus_body),
            derived("bad", pipe),
        ]);
        let diags = check(&m);
        assert!(
            !diags.is_empty(),
            "pipe-form negation through a wrapper should be rejected"
        );
        assert!(diags[0].message.contains("unstratifiable"));
    }
}
