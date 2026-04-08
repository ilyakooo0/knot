//! Stratification checking for recursive derived relations.
//!
//! A recursive derived relation that uses negation (e.g. `diff`) on itself
//! or on a mutually-recursive peer cannot converge to a unique minimal
//! fixpoint.  This pass builds a dependency graph between derived relations,
//! finds strongly-connected components, and rejects any cycle that contains
//! a negative (diff) edge.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{HashMap, HashSet};

// ── Dependency graph ────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Polarity {
    Positive,
    Negative,
}

#[derive(Debug, Clone)]
struct Edge {
    target: String,
    polarity: Polarity,
    span: Span,
}

/// Collect all derived relation names and their dependency edges.
fn build_dependency_graph(module: &ast::Module) -> (HashSet<String>, HashMap<String, Vec<Edge>>) {
    let mut derived_names = HashSet::new();
    let mut edges: HashMap<String, Vec<Edge>> = HashMap::new();

    // First pass: collect all derived relation names.
    for decl in &module.decls {
        if let ast::DeclKind::Derived { name, .. } = &decl.node {
            derived_names.insert(name.clone());
            edges.entry(name.clone()).or_default();
        }
    }

    // Second pass: walk each derived relation body to find edges.
    for decl in &module.decls {
        if let ast::DeclKind::Derived { name, body, .. } = &decl.node {
            let mut found = Vec::new();
            collect_edges(body, Polarity::Positive, &derived_names, &mut found);
            edges.get_mut(name).unwrap().extend(found);
        }
    }

    (derived_names, edges)
}

/// Walk an expression and collect derived-ref edges with polarity.
///
/// `diff a b` makes `b` negative (it's the subtracted set). `a` stays positive.
/// Everything else propagates the current polarity unchanged.
fn collect_edges(
    expr: &ast::Expr,
    polarity: Polarity,
    derived_names: &HashSet<String>,
    out: &mut Vec<Edge>,
) {
    match &expr.node {
        ast::ExprKind::DerivedRef(name) if derived_names.contains(name) => {
            out.push(Edge { target: name.clone(), polarity, span: expr.span });
        }
        ast::ExprKind::DerivedRef(_)
        | ast::ExprKind::Lit(_)
        | ast::ExprKind::Var(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_) => {}

        // `diff` is a curried 2-arg stdlib function. In the AST after
        // desugaring it appears as `App(App(Var("diff"), a), b)`.
        // `a` is the base set (positive), `b` is subtracted (negative).
        ast::ExprKind::App { func, arg } => {
            if let Some(first_arg) = is_diff_applied_once(func) {
                // This is `(diff a) b` — `a` keeps polarity, `b` is negated.
                collect_edges(first_arg, polarity, derived_names, out);
                let neg = negate(polarity);
                collect_edges(arg, neg, derived_names, out);
            } else {
                collect_edges(func, polarity, derived_names, out);
                collect_edges(arg, polarity, derived_names, out);
            }
        }

        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_edges(&f.value, polarity, derived_names, out);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_edges(base, polarity, derived_names, out);
            for f in fields {
                collect_edges(&f.value, polarity, derived_names, out);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_edges(expr, polarity, derived_names, out);
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_edges(e, polarity, derived_names, out);
            }
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_edges(body, polarity, derived_names, out);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_edges(lhs, polarity, derived_names, out);
            collect_edges(rhs, polarity, derived_names, out);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_edges(operand, polarity, derived_names, out);
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            collect_edges(cond, polarity, derived_names, out);
            collect_edges(then_branch, polarity, derived_names, out);
            collect_edges(else_branch, polarity, derived_names, out);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_edges(scrutinee, polarity, derived_names, out);
            for arm in arms {
                collect_edges(&arm.body, polarity, derived_names, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for s in stmts {
                match &s.node {
                    ast::StmtKind::Bind { expr, .. } => {
                        collect_edges(expr, polarity, derived_names, out);
                    }
                    ast::StmtKind::Let { expr, .. } => {
                        collect_edges(expr, polarity, derived_names, out);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_edges(cond, polarity, derived_names, out);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_edges(key, polarity, derived_names, out);
                    }
                    ast::StmtKind::Expr(e) => {
                        collect_edges(e, polarity, derived_names, out);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(inner) => {
            collect_edges(inner, polarity, derived_names, out);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_edges(target, polarity, derived_names, out);
            collect_edges(value, polarity, derived_names, out);
        }
        ast::ExprKind::At { relation, time } => {
            collect_edges(relation, polarity, derived_names, out);
            collect_edges(time, polarity, derived_names, out);
        }
        ast::ExprKind::UnitLit { value, .. } => {
            collect_edges(value, polarity, derived_names, out);
        }
        ast::ExprKind::Annot { expr: inner, .. } => {
            collect_edges(inner, polarity, derived_names, out);
        }
    }
}

/// Check if `expr` is `App(Var("diff"), arg)`, returning the first arg if so.
fn is_diff_applied_once(expr: &ast::Expr) -> Option<&ast::Expr> {
    if let ast::ExprKind::App { func, arg } = &expr.node {
        if let ast::ExprKind::Var(name) = &func.node {
            if name == "diff" {
                return Some(arg);
            }
        }
    }
    None
}

fn negate(p: Polarity) -> Polarity {
    match p {
        Polarity::Positive => Polarity::Negative,
        Polarity::Negative => Polarity::Positive,
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
        self.index.insert(v.clone(), self.index_counter);
        self.lowlink.insert(v.clone(), self.index_counter);
        self.index_counter += 1;
        self.stack.push(v.clone());
        self.on_stack.insert(v.clone());

        if let Some(edges) = self.edges.get(&v) {
            for edge in edges {
                if !self.index.contains_key(&edge.target) {
                    self.strongconnect(edge.target.clone());
                    let ll = self.lowlink[&edge.target];
                    let cur = self.lowlink[&v];
                    self.lowlink.insert(v.clone(), cur.min(ll));
                } else if self.on_stack.contains(&edge.target) {
                    let idx = self.index[&edge.target];
                    let cur = self.lowlink[&v];
                    self.lowlink.insert(v.clone(), cur.min(idx));
                }
            }
        }

        if self.lowlink[&v] == self.index[&v] {
            let mut scc = Vec::new();
            loop {
                let w = self.stack.pop().unwrap();
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
pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    let (_, edges) = build_dependency_graph(module);
    let sccs = Tarjan::run(&edges);

    // Collect declaration spans for error reporting.
    let mut decl_spans: HashMap<String, Span> = HashMap::new();
    for decl in &module.decls {
        if let ast::DeclKind::Derived { name, .. } = &decl.node {
            decl_spans.insert(name.clone(), decl.span);
        }
    }

    let mut diagnostics = Vec::new();

    for scc in &sccs {
        // Only consider non-trivial SCCs (cycles) or self-loops.
        let is_cycle = if scc.len() == 1 {
            // Check for self-edge.
            let name = &scc[0];
            edges.get(name).map_or(false, |es| es.iter().any(|e| &e.target == name))
        } else {
            true
        };
        if !is_cycle {
            continue;
        }

        // Check if any edge within this SCC is negative.
        let scc_set: HashSet<&String> = scc.iter().collect();
        for name in scc {
            if let Some(es) = edges.get(name) {
                for edge in es {
                    if edge.polarity == Polarity::Negative && scc_set.contains(&edge.target) {
                        let mut diag = Diagnostic::error(format!(
                            "unstratifiable recursion: `&{}` negates `&{}` through `diff`",
                            name, edge.target,
                        ))
                        .label(edge.span, "negative dependency via `diff`");

                        if let Some(&decl_span) = decl_spans.get(name) {
                            diag = diag.label(decl_span, format!("`&{}` defined here", name));
                        }

                        if name == &edge.target {
                            diag = diag.note(
                                "a derived relation cannot subtract from itself in a recursive definition — \
                                 the fixpoint would oscillate instead of converging",
                            );
                        } else {
                            diag = diag.note(format!(
                                "`&{}` and `&{}` are mutually recursive; negating across a \
                                 recursive cycle has no well-defined fixpoint",
                                name, edge.target,
                            ));
                        }

                        diag = diag.note(
                            "split the negation into a separate, non-recursive derived relation",
                        );

                        diagnostics.push(diag);
                    }
                }
            }
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
    fn mutual_recursion_all_positive_is_ok() {
        // &a = union *source &b
        // &b = union *source &a
        let m = module(vec![
            derived("a", union(source_ref("source"), derived_ref("b"))),
            derived("b", union(source_ref("source"), derived_ref("a"))),
        ]);
        let diags = check(&m);
        assert!(diags.is_empty(), "all-positive mutual recursion is fine");
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
}
