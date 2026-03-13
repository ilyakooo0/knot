//! Effect inference for the Knot language.
//!
//! Infers effects (reads, writes, console, clock, etc.) for each declaration
//! and checks safety constraints (e.g. no IO inside atomic blocks). Also
//! validates explicit effect annotations against inferred effects.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

// ── Effect set ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectSet {
    pub reads: BTreeSet<String>,
    pub writes: BTreeSet<String>,
    pub console: bool,
    pub network: bool,
    pub fs: bool,
    pub clock: bool,
    pub random: bool,
}

#[allow(dead_code)]
impl EffectSet {
    pub fn empty() -> Self {
        Self {
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
            console: false,
            network: false,
            fs: false,
            clock: false,
            random: false,
        }
    }

    pub fn is_pure(&self) -> bool {
        self.reads.is_empty()
            && self.writes.is_empty()
            && !self.console
            && !self.network
            && !self.fs
            && !self.clock
            && !self.random
    }

    pub fn union(&self, other: &EffectSet) -> EffectSet {
        EffectSet {
            reads: self.reads.union(&other.reads).cloned().collect(),
            writes: self.writes.union(&other.writes).cloned().collect(),
            console: self.console || other.console,
            network: self.network || other.network,
            fs: self.fs || other.fs,
            clock: self.clock || other.clock,
            random: self.random || other.random,
        }
    }

    pub fn is_subset_of(&self, other: &EffectSet) -> bool {
        self.reads.is_subset(&other.reads)
            && self.writes.is_subset(&other.writes)
            && (!self.console || other.console)
            && (!self.network || other.network)
            && (!self.fs || other.fs)
            && (!self.clock || other.clock)
            && (!self.random || other.random)
    }

    /// Returns effects in `self` that are not in `other`.
    pub fn difference(&self, other: &EffectSet) -> EffectSet {
        EffectSet {
            reads: self.reads.difference(&other.reads).cloned().collect(),
            writes: self.writes.difference(&other.writes).cloned().collect(),
            console: self.console && !other.console,
            network: self.network && !other.network,
            fs: self.fs && !other.fs,
            clock: self.clock && !other.clock,
            random: self.random && !other.random,
        }
    }

    /// Returns true if any IO effects (console, network, fs) are present.
    pub fn has_io(&self) -> bool {
        self.console || self.network || self.fs
    }

    pub fn from_ast_effects(effects: &[ast::Effect]) -> EffectSet {
        let mut set = EffectSet::empty();
        for effect in effects {
            match effect {
                ast::Effect::Reads(name) => {
                    set.reads.insert(name.clone());
                }
                ast::Effect::Writes(name) => {
                    set.writes.insert(name.clone());
                }
                ast::Effect::Console => set.console = true,
                ast::Effect::Network => set.network = true,
                ast::Effect::Fs => set.fs = true,
                ast::Effect::Clock => set.clock = true,
                ast::Effect::Random => set.random = true,
            }
        }
        set
    }
}

impl fmt::Display for EffectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        for name in &self.reads {
            parts.push(format!("reads *{}", name));
        }
        for name in &self.writes {
            parts.push(format!("writes *{}", name));
        }
        if self.console {
            parts.push("console".into());
        }
        if self.network {
            parts.push("network".into());
        }
        if self.fs {
            parts.push("fs".into());
        }
        if self.clock {
            parts.push("clock".into());
        }
        if self.random {
            parts.push("random".into());
        }
        write!(f, "{{{}}}", parts.join(", "))
    }
}

// ── Effect checker ───────────────────────────────────────────────

struct EffectChecker {
    /// Inferred effects per declaration name.
    decl_effects: HashMap<String, EffectSet>,
    /// Built-in function effects.
    builtin_effects: HashMap<String, EffectSet>,
    /// Known source relation names.
    source_names: HashSet<String>,
    /// Accumulated diagnostics.
    diagnostics: Vec<Diagnostic>,
}

impl EffectChecker {
    fn new() -> Self {
        let mut builtin_effects = HashMap::new();

        // println, print: console effect
        let console_effect = {
            let mut e = EffectSet::empty();
            e.console = true;
            e
        };
        builtin_effects.insert("println".into(), console_effect.clone());
        builtin_effects.insert("putLine".into(), console_effect.clone());
        builtin_effects.insert("print".into(), console_effect);

        // now: clock effect
        let clock_effect = {
            let mut e = EffectSet::empty();
            e.clock = true;
            e
        };
        builtin_effects.insert("now".into(), clock_effect);

        // listen: network effect
        let network_effect = {
            let mut e = EffectSet::empty();
            e.network = true;
            e
        };
        builtin_effects.insert("listen".into(), network_effect);

        // Pure builtins
        builtin_effects.insert("show".into(), EffectSet::empty());
        builtin_effects.insert("union".into(), EffectSet::empty());
        builtin_effects.insert("count".into(), EffectSet::empty());
        builtin_effects.insert("filter".into(), EffectSet::empty());
        builtin_effects.insert("map".into(), EffectSet::empty());
        builtin_effects.insert("fold".into(), EffectSet::empty());

        Self {
            decl_effects: HashMap::new(),
            builtin_effects,
            source_names: HashSet::new(),
            diagnostics: Vec::new(),
        }
    }

    fn run(&mut self, module: &ast::Module) {
        // Collect source relation names
        for decl in &module.decls {
            if let ast::DeclKind::Source { name, .. } = &decl.node {
                self.source_names.insert(name.clone());
            }
        }

        // Process derived relations first, then views, then funs.
        // This ensures callees are processed before callers when possible.
        let mut derived = Vec::new();
        let mut views = Vec::new();
        let mut funs = Vec::new();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Derived { .. } => derived.push(decl),
                ast::DeclKind::View { .. } => views.push(decl),
                ast::DeclKind::Fun { .. } => funs.push(decl),
                _ => {}
            }
        }

        for decl in derived {
            self.process_decl(decl);
        }
        for decl in views {
            self.process_decl(decl);
        }
        for decl in funs {
            self.process_decl(decl);
        }
    }

    fn process_decl(&mut self, decl: &ast::Decl) {
        match &decl.node {
            ast::DeclKind::Derived { name, body, ty, .. } => {
                let effects = self.infer_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects, decl.span);
            }
            ast::DeclKind::View { name, body, ty, .. } => {
                let effects = self.infer_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects, decl.span);
            }
            ast::DeclKind::Fun { name, body, ty, .. } => {
                let effects = self.fun_body_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects, decl.span);
            }
            _ => {}
        }
    }

    /// Infer effects of evaluating an expression.
    fn infer_effects(&mut self, expr: &ast::Expr) -> EffectSet {
        match &expr.node {
            ast::ExprKind::Lit(_) | ast::ExprKind::Constructor(_) => EffectSet::empty(),

            ast::ExprKind::Var(name) => {
                if name == "now" {
                    let mut e = EffectSet::empty();
                    e.clock = true;
                    return e;
                }
                EffectSet::empty()
            }

            ast::ExprKind::SourceRef(name) => {
                let mut e = EffectSet::empty();
                e.reads.insert(name.clone());
                e
            }

            ast::ExprKind::DerivedRef(name) => {
                self.decl_effects.get(name).cloned().unwrap_or_else(EffectSet::empty)
            }

            ast::ExprKind::Lambda { .. } => {
                // Creating a lambda is pure; effects happen when it's called.
                EffectSet::empty()
            }

            ast::ExprKind::App { func, arg } => {
                let arg_effects = self.infer_effects(arg);
                let call_effects = self.callee_effects(func);
                arg_effects.union(&call_effects)
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                if *op == ast::BinOp::Pipe {
                    let lhs_effects = self.infer_effects(lhs);
                    let rhs_effects = self.callee_effects(rhs);
                    lhs_effects.union(&rhs_effects)
                } else {
                    let lhs_effects = self.infer_effects(lhs);
                    let rhs_effects = self.infer_effects(rhs);
                    lhs_effects.union(&rhs_effects)
                }
            }

            ast::ExprKind::UnaryOp { operand, .. } => self.infer_effects(operand),

            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
                let mut effects = self.infer_effects(value);
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    effects.writes.insert(name.clone());
                    effects.reads.insert(name.clone());
                } else if let ast::ExprKind::DerivedRef(name) = &target.node {
                    // Writing to a view — inherit the view's effects plus writes
                    let view_effects =
                        self.decl_effects.get(name).cloned().unwrap_or_else(EffectSet::empty);
                    effects = effects.union(&view_effects);
                }
                effects
            }

            ast::ExprKind::Atomic(inner) => {
                let inner_effects = self.infer_effects(inner);
                if inner_effects.has_io() {
                    self.diagnostics.push(
                        Diagnostic::error("IO effects are not allowed inside atomic blocks")
                            .label(expr.span, "this atomic block")
                            .note(format!(
                                "inferred effects: {}",
                                inner_effects
                            ))
                            .note(
                                "console, network, and fs effects cannot be rolled back",
                            ),
                    );
                }
                inner_effects
            }

            ast::ExprKind::Do(stmts) => {
                let mut effects = EffectSet::empty();
                for stmt in stmts {
                    let stmt_effects = self.infer_stmt_effects(stmt);
                    effects = effects.union(&stmt_effects);
                }
                effects
            }

            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = self.infer_effects(cond);
                let t = self.infer_effects(then_branch);
                let e = self.infer_effects(else_branch);
                c.union(&t).union(&e)
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                let mut effects = self.infer_effects(scrutinee);
                for arm in arms {
                    let arm_effects = self.infer_effects(&arm.body);
                    effects = effects.union(&arm_effects);
                }
                effects
            }

            ast::ExprKind::Record(fields) => {
                let mut effects = EffectSet::empty();
                for field in fields {
                    let field_effects = self.infer_effects(&field.value);
                    effects = effects.union(&field_effects);
                }
                effects
            }

            ast::ExprKind::RecordUpdate { base, fields } => {
                let mut effects = self.infer_effects(base);
                for field in fields {
                    let field_effects = self.infer_effects(&field.value);
                    effects = effects.union(&field_effects);
                }
                effects
            }

            ast::ExprKind::FieldAccess { expr: inner, .. } => self.infer_effects(inner),

            ast::ExprKind::List(elems) => {
                let mut effects = EffectSet::empty();
                for elem in elems {
                    let elem_effects = self.infer_effects(elem);
                    effects = effects.union(&elem_effects);
                }
                effects
            }

            ast::ExprKind::Yield(inner) => self.infer_effects(inner),

            ast::ExprKind::At { relation, time } => {
                let mut effects = self.infer_effects(relation);
                let time_effects = self.infer_effects(time);
                effects = effects.union(&time_effects);
                effects
            }
        }
    }

    /// Infer effects of a do-block statement.
    fn infer_stmt_effects(&mut self, stmt: &ast::Stmt) -> EffectSet {
        match &stmt.node {
            ast::StmtKind::Bind { expr, .. } => self.infer_effects(expr),
            ast::StmtKind::Let { expr, .. } => self.infer_effects(expr),
            ast::StmtKind::Where { cond } => self.infer_effects(cond),
            ast::StmtKind::Expr(expr) => self.infer_effects(expr),
        }
    }

    /// Resolve effects when a function expression is *called*.
    fn callee_effects(&mut self, func_expr: &ast::Expr) -> EffectSet {
        match &func_expr.node {
            ast::ExprKind::Var(name) => {
                // Check builtins first, then user declarations
                if let Some(effects) = self.builtin_effects.get(name) {
                    return effects.clone();
                }
                if let Some(effects) = self.decl_effects.get(name) {
                    return effects.clone();
                }
                // Unknown callee — treat as pure (conservative, no false positives)
                EffectSet::empty()
            }

            ast::ExprKind::Lambda { body, .. } => {
                // Immediately-applied lambda: effects are the body's effects
                self.infer_effects(body)
            }

            ast::ExprKind::App { func, arg } => {
                // Curried call: e.g. `(f x) y` — the callee is `f x` partially applied
                let func_effects = self.callee_effects(func);
                let arg_effects = self.infer_effects(arg);
                func_effects.union(&arg_effects)
            }

            other => {
                // Field access, case, etc. — fall back to infer_effects
                let _ = other;
                self.infer_effects(func_expr)
            }
        }
    }

    /// Unwrap lambda chain to get the effects of the function body.
    /// `\x y -> set *foo = ...` → effects of the `set` expression.
    fn fun_body_effects(&mut self, body: &ast::Expr) -> EffectSet {
        match &body.node {
            ast::ExprKind::Lambda { body: inner, .. } => self.fun_body_effects(inner),
            _ => self.infer_effects(body),
        }
    }

    /// Check that explicit effect annotations (if any) are a superset of inferred effects.
    fn check_annotation(
        &mut self,
        ty: &Option<ast::TypeScheme>,
        inferred: &EffectSet,
        decl_span: Span,
    ) {
        let scheme = match ty {
            Some(s) => s,
            None => return,
        };

        // Walk the type to find an Effectful wrapper
        if let Some(declared) = extract_effects(&scheme.ty) {
            if !inferred.is_subset_of(&declared) {
                let extra = inferred.difference(&declared);
                self.diagnostics.push(
                    Diagnostic::error("inferred effects exceed declared effects")
                        .label(decl_span, "this declaration")
                        .note(format!("declared effects: {}", declared))
                        .note(format!("inferred effects: {}", inferred))
                        .note(format!("undeclared effects: {}", extra)),
                );
            }
        }
    }
}

/// Extract effect set from a type, if it has an Effectful wrapper.
fn extract_effects(ty: &ast::Type) -> Option<EffectSet> {
    match &ty.node {
        ast::TypeKind::Effectful { effects, .. } => Some(EffectSet::from_ast_effects(effects)),
        ast::TypeKind::Function { param, result } => {
            // Effects could be on the outermost function type or nested
            extract_effects(param).or_else(|| extract_effects(result))
        }
        _ => None,
    }
}

// ── Public entry point ───────────────────────────────────────────

pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    let mut checker = EffectChecker::new();
    checker.run(module);
    checker.diagnostics
}

// ── Tests ────────────────────────────────────────────────────────

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

    // ── EffectSet unit tests ─────────────────────────────────────

    #[test]
    fn empty_is_pure() {
        assert!(EffectSet::empty().is_pure());
    }

    #[test]
    fn reads_is_not_pure() {
        let mut e = EffectSet::empty();
        e.reads.insert("people".into());
        assert!(!e.is_pure());
    }

    #[test]
    fn union_combines_effects() {
        let mut a = EffectSet::empty();
        a.reads.insert("people".into());
        a.console = true;

        let mut b = EffectSet::empty();
        b.reads.insert("todos".into());
        b.writes.insert("people".into());
        b.clock = true;

        let c = a.union(&b);
        assert_eq!(c.reads.len(), 2);
        assert!(c.reads.contains("people"));
        assert!(c.reads.contains("todos"));
        assert_eq!(c.writes.len(), 1);
        assert!(c.writes.contains("people"));
        assert!(c.console);
        assert!(c.clock);
        assert!(!c.network);
    }

    #[test]
    fn subset_check() {
        let mut small = EffectSet::empty();
        small.reads.insert("people".into());

        let mut big = EffectSet::empty();
        big.reads.insert("people".into());
        big.console = true;

        assert!(small.is_subset_of(&big));
        assert!(!big.is_subset_of(&small));
    }

    #[test]
    fn difference_computes_extra() {
        let mut inferred = EffectSet::empty();
        inferred.reads.insert("people".into());
        inferred.console = true;
        inferred.clock = true;

        let mut declared = EffectSet::empty();
        declared.reads.insert("people".into());
        declared.clock = true;

        let diff = inferred.difference(&declared);
        assert!(diff.reads.is_empty());
        assert!(diff.console);
        assert!(!diff.clock);
    }

    #[test]
    fn has_io_flags() {
        let mut e = EffectSet::empty();
        assert!(!e.has_io());

        e.console = true;
        assert!(e.has_io());

        e.console = false;
        e.clock = true;
        assert!(!e.has_io());
    }

    #[test]
    fn display_format() {
        let mut e = EffectSet::empty();
        e.reads.insert("people".into());
        e.console = true;
        assert_eq!(format!("{}", e), "{reads *people, console}");
    }

    #[test]
    fn from_ast_effects_conversion() {
        let effects = vec![
            Effect::Reads("people".into()),
            Effect::Writes("todos".into()),
            Effect::Console,
            Effect::Clock,
        ];
        let set = EffectSet::from_ast_effects(&effects);
        assert!(set.reads.contains("people"));
        assert!(set.writes.contains("todos"));
        assert!(set.console);
        assert!(set.clock);
        assert!(!set.network);
    }

    // ── Expression inference tests ───────────────────────────────

    fn check_module(decls: Vec<Decl>) -> (Vec<Diagnostic>, HashMap<String, EffectSet>) {
        let module = Module { name: None, decls };
        let mut checker = EffectChecker::new();
        checker.run(&module);
        (checker.diagnostics, checker.decl_effects)
    }

    fn make_source(name: &str) -> Decl {
        spanned(DeclKind::Source {
            name: name.into(),
            ty: spanned(TypeKind::Relation(Box::new(spanned(TypeKind::Named(
                "T".into(),
            ))))),
            history: false,
        })
    }

    fn make_fun(name: &str, body: Expr) -> Decl {
        spanned(DeclKind::Fun {
            name: name.into(),
            ty: None,
            body,
        })
    }

    fn make_fun_with_type(name: &str, body: Expr, ty: TypeScheme) -> Decl {
        spanned(DeclKind::Fun {
            name: name.into(),
            ty: Some(ty),
            body,
        })
    }

    #[test]
    fn literal_is_pure() {
        let body = spanned(ExprKind::Lit(Literal::Int(42)));
        let (diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].is_pure());
    }

    #[test]
    fn source_ref_reads() {
        let body = spanned(ExprKind::SourceRef("people".into()));
        let (diags, effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].reads.contains("people"));
        assert!(effects["f"].writes.is_empty());
    }

    #[test]
    fn set_writes_and_reads() {
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::Set {
                target: Box::new(spanned(ExprKind::SourceRef("todos".into()))),
                value: Box::new(spanned(ExprKind::Var("x".into()))),
            })),
        });
        let (diags, effects) =
            check_module(vec![make_source("todos"), make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].writes.contains("todos"));
        assert!(effects["f"].reads.contains("todos"));
    }

    #[test]
    fn println_has_console_effect() {
        // println "hello"
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Var("x".into()))),
            })),
        });
        let (diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].console);
    }

    #[test]
    fn now_has_clock_effect() {
        let body = spanned(ExprKind::Var("now".into()));
        let (diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].clock);
    }

    #[test]
    fn lambda_creation_is_pure() {
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Var("x".into()))),
            })),
        });
        // Just referencing a lambda (not calling it) — pure at the expression level
        let wrapper = spanned(ExprKind::Record(vec![Field {
            name: "f".into(),
            value: body,
        }]));
        let (diags, effects) = check_module(vec![make_fun("g", wrapper)]);
        assert!(diags.is_empty());
        assert!(effects["g"].is_pure());
    }

    #[test]
    fn do_block_unions_effects() {
        // do { p <- *people; println p; yield p }
        let body = spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Var("p".into())),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Var("p".into()))),
            }))),
            spanned(StmtKind::Expr(spanned(ExprKind::Yield(Box::new(spanned(
                ExprKind::Var("p".into()),
            )))))),
        ]));
        let (diags, effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].reads.contains("people"));
        assert!(effects["f"].console);
    }

    #[test]
    fn atomic_io_error() {
        // atomic (println "hello")
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("println".into()))),
            arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hello".into())))),
        }))));
        let (diags, _effects) = check_module(vec![make_fun("f", body)]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("IO effects"));
    }

    #[test]
    fn atomic_reads_writes_ok() {
        // atomic (set *people = [...])
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Set {
            target: Box::new(spanned(ExprKind::SourceRef("people".into()))),
            value: Box::new(spanned(ExprKind::List(vec![]))),
        }))));
        let (diags, effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].writes.contains("people"));
    }

    #[test]
    fn atomic_clock_ok() {
        // atomic (now) — clock is allowed in atomic
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Var(
            "now".into(),
        )))));
        let (diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].clock);
    }

    #[test]
    fn pipe_resolves_callee() {
        // x |> println
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::BinOp {
                op: BinOp::Pipe,
                lhs: Box::new(spanned(ExprKind::Var("x".into()))),
                rhs: Box::new(spanned(ExprKind::Var("println".into()))),
            })),
        });
        let (diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].console);
    }

    #[test]
    fn derived_ref_inherits_effects() {
        // &seniors reads *people
        let derived_body = spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Var("p".into())),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Expr(spanned(ExprKind::Yield(Box::new(spanned(
                ExprKind::Var("p".into()),
            )))))),
        ]));
        let derived = spanned(DeclKind::Derived {
            name: "seniors".into(),
            ty: None,
            body: derived_body,
        });

        // f = &seniors
        let body = spanned(ExprKind::DerivedRef("seniors".into()));
        let (diags, effects) =
            check_module(vec![make_source("people"), derived, make_fun("f", body)]);
        assert!(diags.is_empty());
        assert!(effects["f"].reads.contains("people"));
    }

    #[test]
    fn annotation_ok_when_superset() {
        // f : {reads *people, console} Int -> Int
        // f = \x -> do { println *people; yield x }
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::Do(vec![
                spanned(StmtKind::Expr(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::SourceRef("people".into()))),
                }))),
                spanned(StmtKind::Expr(spanned(ExprKind::Yield(Box::new(spanned(
                    ExprKind::Var("x".into()),
                )))))),
            ]))),
        });
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::Effectful {
                effects: vec![Effect::Reads("people".into()), Effect::Console],
                ty: Box::new(spanned(TypeKind::Function {
                    param: Box::new(spanned(TypeKind::Named("Int".into()))),
                    result: Box::new(spanned(TypeKind::Named("Int".into()))),
                })),
            }),
        };
        let (diags, _) = check_module(vec![
            make_source("people"),
            make_fun_with_type("f", body, ty),
        ]);
        assert!(diags.is_empty());
    }

    #[test]
    fn annotation_error_when_missing_effect() {
        // Declares only {reads *people} but actually uses console too
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::Do(vec![
                spanned(StmtKind::Expr(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::SourceRef("people".into()))),
                }))),
            ]))),
        });
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::Effectful {
                effects: vec![Effect::Reads("people".into())],
                ty: Box::new(spanned(TypeKind::Function {
                    param: Box::new(spanned(TypeKind::Named("Int".into()))),
                    result: Box::new(spanned(TypeKind::Named("Int".into()))),
                })),
            }),
        };
        let (diags, _) = check_module(vec![
            make_source("people"),
            make_fun_with_type("f", body, ty),
        ]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("exceed"));
    }

    #[test]
    fn if_unions_branches() {
        let body = spanned(ExprKind::If {
            cond: Box::new(spanned(ExprKind::Lit(Literal::Int(1)))),
            then_branch: Box::new(spanned(ExprKind::SourceRef("a".into()))),
            else_branch: Box::new(spanned(ExprKind::SourceRef("b".into()))),
        });
        let (diags, effects) = check_module(vec![
            make_source("a"),
            make_source("b"),
            make_fun("f", body),
        ]);
        assert!(diags.is_empty());
        assert!(effects["f"].reads.contains("a"));
        assert!(effects["f"].reads.contains("b"));
    }

    #[test]
    fn case_unions_arms() {
        let body = spanned(ExprKind::Case {
            scrutinee: Box::new(spanned(ExprKind::Var("x".into()))),
            arms: vec![
                CaseArm {
                    pat: spanned(PatKind::Lit(Literal::Int(1))),
                    body: spanned(ExprKind::SourceRef("a".into())),
                },
                CaseArm {
                    pat: spanned(PatKind::Wildcard),
                    body: spanned(ExprKind::SourceRef("b".into())),
                },
            ],
        });
        let (diags, effects) = check_module(vec![
            make_source("a"),
            make_source("b"),
            make_fun("f", body),
        ]);
        assert!(diags.is_empty());
        assert!(effects["f"].reads.contains("a"));
        assert!(effects["f"].reads.contains("b"));
    }
}
