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
    /// Whether evaluating this expression may run `race` (directly or via a
    /// helper). Not a user-declarable effect — it exists solely so the
    /// atomic gate can reject `race` reached through wrappers, where the
    /// syntactic walk of the atomic body can't see it. Deliberately ignored
    /// by `is_subset_of`/`difference`/`Display`, so effect annotations are
    /// unaffected.
    pub uses_race: bool,
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
            uses_race: false,
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
        // Fast paths for empty sets
        let reads = if self.reads.is_empty() {
            other.reads.clone()
        } else if other.reads.is_empty() {
            self.reads.clone()
        } else {
            self.reads.union(&other.reads).cloned().collect()
        };
        let writes = if self.writes.is_empty() {
            other.writes.clone()
        } else if other.writes.is_empty() {
            self.writes.clone()
        } else {
            self.writes.union(&other.writes).cloned().collect()
        };
        EffectSet {
            reads,
            writes,
            console: self.console || other.console,
            network: self.network || other.network,
            fs: self.fs || other.fs,
            clock: self.clock || other.clock,
            random: self.random || other.random,
            uses_race: self.uses_race || other.uses_race,
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
        let reads = if other.reads.is_empty() {
            self.reads.clone()
        } else {
            self.reads.difference(&other.reads).cloned().collect()
        };
        let writes = if other.writes.is_empty() {
            self.writes.clone()
        } else {
            self.writes.difference(&other.writes).cloned().collect()
        };
        EffectSet {
            reads,
            writes,
            console: self.console && !other.console,
            network: self.network && !other.network,
            fs: self.fs && !other.fs,
            clock: self.clock && !other.clock,
            random: self.random && !other.random,
            // Not user-declarable, so never reported as an annotation delta.
            uses_race: false,
        }
    }

    /// Returns true if any IO effects (console, network, fs, clock, random) are present.
    pub fn has_io(&self) -> bool {
        self.console || self.network || self.fs || self.clock || self.random
    }

    pub fn is_empty(&self) -> bool {
        self.reads.is_empty()
            && self.writes.is_empty()
            && !self.console
            && !self.network
            && !self.fs
            && !self.clock
            && !self.random
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
        let read_write: BTreeSet<&String> = self.reads.intersection(&self.writes).collect();
        for name in &self.reads {
            if !read_write.contains(name) {
                parts.push(format!("r *{}", name));
            }
        }
        for name in &self.writes {
            if !read_write.contains(name) {
                parts.push(format!("w *{}", name));
            }
        }
        for name in &read_write {
            parts.push(format!("rw *{}", name));
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
    /// Known view names. The parser produces `SourceRef` for *every* `*name`
    /// write target, so writes to views arrive looking like source writes —
    /// this set lets the `Set`/`ReplaceSet` arm recognize them and attribute
    /// read/write effects to the view's backing source(s) as well.
    view_names: HashSet<String>,
    /// Names of declarations whose annotated signature carries an effect-row
    /// variable (e.g. `IO {r *sessions | e} a`). Calls to these functions
    /// propagate the effects of their lambda arguments — matching HM's
    /// row-polymorphic effect inference. Without an effect row variable
    /// (e.g. `forEach : (a -> IO {} {}) -> IO {} {}`), the callback's effects
    /// are absorbed by the declared row, so we don't propagate.
    row_poly_decls: HashSet<String>,
    /// Names of declarations whose annotated signature declares a *closed*
    /// effect set (an `IO {…}` result with no row variable). Per the
    /// documented annotation semantics, such a signature absorbs the
    /// effects of callback arguments, so calls to these functions do NOT
    /// propagate lambda-argument effects. Every other callee (unannotated
    /// user functions, local bindings, unknown names, builtins) may invoke
    /// its functional arguments, so lambda-argument effects are
    /// conservatively charged to the call — over-approximating for HOFs
    /// that never call their argument, which is the safe direction for
    /// the atomic gate.
    fixed_row_decls: HashSet<String>,
    /// Stack of local scopes mapping let-bound (or immediately-applied-
    /// lambda-bound) function names to the effects of their bodies. Lets the
    /// checker see effects of calls through local bindings, e.g.
    /// `do { let f = \u -> *items; rows <- f {} }` reads `items`.
    local_fn_effects: Vec<HashMap<String, EffectSet>>,
    /// Bodies of user declarations, for the conservative atomic-gate scan
    /// that looks for IO-performing lambdas reachable through opaque
    /// callees (e.g. a lambda stored in a record field of another decl).
    decl_bodies: HashMap<String, ast::Expr>,
    /// Names currently shadowed by lambda params, do-binds, lets, or case
    /// binders. A shadowed name is a local value, not the builtin — without
    /// this, a do-bind named `race` would carry the `uses_race` marker and
    /// be wrongly rejected inside `atomic`.
    shadowed: Vec<String>,
    /// Name of the top-level declaration currently being checked. The atomic
    /// gate uses it to also scan the enclosing declaration's body for IO
    /// lambdas that a value laundered into the atomic block (through a let
    /// binding or record field) could be carrying.
    current_decl_name: Option<String>,
    /// Accumulated diagnostics.
    diagnostics: Vec<Diagnostic>,
}

impl EffectChecker {
    fn new() -> Self {
        let mut builtin_effects = HashMap::new();

        // Helper: batch-insert same effect for multiple builtins
        let mut insert_many = |names: &[&str], effect: EffectSet| {
            for name in names {
                builtin_effects.insert((*name).into(), effect.clone());
            }
        };

        // Builtin tables come from `crate::builtins` so the LSP can read the
        // exact same lists. Adding a new effectful builtin should only require
        // editing one file.
        use crate::builtins::{
            CLOCK_BUILTINS, CONSOLE_BUILTINS, FS_BUILTINS, NETWORK_BUILTINS, PURE_BUILTINS,
            RANDOM_BUILTINS,
        };

        let mut console_effect = EffectSet::empty();
        console_effect.console = true;
        insert_many(CONSOLE_BUILTINS, console_effect);

        let mut clock_effect = EffectSet::empty();
        clock_effect.clock = true;
        insert_many(CLOCK_BUILTINS, clock_effect);

        let mut random_effect = EffectSet::empty();
        random_effect.random = true;
        insert_many(RANDOM_BUILTINS, random_effect);

        let mut network_effect = EffectSet::empty();
        network_effect.network = true;
        insert_many(NETWORK_BUILTINS, network_effect);

        let mut fs_effect = EffectSet::empty();
        fs_effect.fs = true;
        insert_many(FS_BUILTINS, fs_effect);

        insert_many(PURE_BUILTINS, EffectSet::empty());

        // `race` carries a marker (not a user-declarable effect) so the
        // atomic gate catches it through helper functions — the syntactic
        // walk of the atomic body alone misses `raceIt = \a b -> race a b`.
        // `fork` is intentionally permitted inside atomic (see builtins.rs)
        // and `retry` is the STM primitive, so neither is marked.
        let mut race_effect = EffectSet::empty();
        race_effect.uses_race = true;
        builtin_effects.insert("race".into(), race_effect);

        Self {
            decl_effects: HashMap::new(),
            builtin_effects,
            source_names: HashSet::new(),
            view_names: HashSet::new(),
            row_poly_decls: HashSet::new(),
            fixed_row_decls: HashSet::new(),
            local_fn_effects: Vec::new(),
            decl_bodies: HashMap::new(),
            shadowed: Vec::new(),
            current_decl_name: None,
            diagnostics: Vec::new(),
        }
    }

    fn run(&mut self, module: &ast::Module) {
        // Collect source relation and view names, plus declaration bodies
        // for the atomic-gate's opaque-callee lambda scan.
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Source { name, .. } => {
                    self.source_names.insert(name.clone());
                }
                ast::DeclKind::View { name, body, .. }
                | ast::DeclKind::Derived { name, body, .. } => {
                    if let ast::DeclKind::View { .. } = &decl.node {
                        self.view_names.insert(name.clone());
                    }
                    self.decl_bodies.insert(name.clone(), body.clone());
                }
                ast::DeclKind::Fun { name, body: Some(body), .. } => {
                    self.decl_bodies.insert(name.clone(), body.clone());
                }
                _ => {}
            }
        }

        // Collect declarations whose annotated signature uses an effect-row
        // variable (these propagate lambda-arg effects) and declarations
        // with a *closed* declared effect set (these absorb lambda-arg
        // effects). Everything else conservatively propagates.
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, ty: Some(scheme), .. }
                | ast::DeclKind::View { name, ty: Some(scheme), .. }
                | ast::DeclKind::Derived { name, ty: Some(scheme), .. } => {
                    if type_has_effect_row_var(&scheme.ty) {
                        self.row_poly_decls.insert(name.clone());
                    } else if extract_effects(&scheme.ty).is_some() {
                        self.fixed_row_decls.insert(name.clone());
                    }
                }
                _ => {}
            }
        }

        // Collect trait-method bodies: impl methods and trait default
        // bodies, grouped by method name. A trait-method call dispatches on
        // the runtime type tag, so the only sound static approximation for
        // a call site is the UNION of effects across every known impl
        // (plus defaults). The union is stored in `decl_effects` under the
        // method name so Var/callee lookups see it. (Names that collide
        // with a top-level function are skipped — the function wins, which
        // matches name-resolution order.) BTreeMap keeps diagnostic order
        // deterministic.
        let fun_names: HashSet<&str> = module
            .decls
            .iter()
            .filter_map(|d| match &d.node {
                ast::DeclKind::Fun { name, .. }
                | ast::DeclKind::View { name, .. }
                | ast::DeclKind::Derived { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();
        let mut method_bodies: std::collections::BTreeMap<String, Vec<&ast::Expr>> =
            std::collections::BTreeMap::new();
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, body, .. } = item {
                            if !fun_names.contains(name.as_str()) {
                                method_bodies
                                    .entry(name.clone())
                                    .or_default()
                                    .push(body);
                            }
                        }
                    }
                }
                ast::DeclKind::Trait { items, .. } => {
                    for item in items {
                        if let ast::TraitItem::Method {
                            name,
                            default_body: Some(body),
                            ..
                        } = item
                        {
                            if !fun_names.contains(name.as_str()) {
                                method_bodies
                                    .entry(name.clone())
                                    .or_default()
                                    .push(body);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Group declarations so the final diagnostic pass keeps a stable
        // order (derived, then views, then funs).
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

        // Single global fixpoint over derived relations, views, AND funs:
        // a derived relation/view may call a user function (and vice versa),
        // so iterating them in separate fixpoints leaves stale empty effect
        // sets behind (e.g. `&d = readsB {}` where `readsB = \u -> *b` —
        // the derived loop ran before funs and never saw `r *b`). Forward
        // references and mutual recursion also need multiple passes.
        // Suppress diagnostics during the fixpoint since intermediate
        // states may be incomplete.
        loop {
            let mut changed = false;
            let saved_diags = std::mem::take(&mut self.diagnostics);
            for decl in &module.decls {
                let (name, effects) = match &decl.node {
                    ast::DeclKind::Derived { name, body, .. }
                    | ast::DeclKind::View { name, body, .. } => {
                        (name, self.infer_effects(body))
                    }
                    ast::DeclKind::Fun { name, body: Some(body), .. } => {
                        (name, self.fun_body_effects(body))
                    }
                    _ => continue,
                };
                let old = self.decl_effects.get(name);
                if old.map_or(true, |o| *o != effects) {
                    self.decl_effects.insert(name.clone(), effects);
                    changed = true;
                }
            }
            // Trait methods: union of all impl bodies and the trait
            // default body, so trait-method call sites are sound for the
            // atomic gate and relation reads/writes inside impls are
            // visible.
            for (name, bodies) in &method_bodies {
                let mut effects = EffectSet::empty();
                for body in bodies {
                    effects = effects.union(&self.fun_body_effects(body));
                }
                let old = self.decl_effects.get(name);
                if old.map_or(true, |o| *o != effects) {
                    self.decl_effects.insert(name.clone(), effects);
                    changed = true;
                }
            }
            self.diagnostics = saved_diags;
            if !changed { break; }
        }

        // Final pass: emit diagnostics and check annotations with converged effects
        for decl in derived {
            self.process_decl(decl);
        }
        for decl in views {
            self.process_decl(decl);
        }
        for decl in &funs {
            self.process_decl(decl);
        }
        // Impl-method and trait-default bodies were only walked with
        // diagnostics suppressed during the fixpoint — walk them once more
        // so atomic-safety violations inside them are reported.
        for bodies in method_bodies.values() {
            for body in bodies {
                let _ = self.fun_body_effects(body);
            }
        }
    }

    fn process_decl(&mut self, decl: &ast::Decl) {
        self.current_decl_name = match &decl.node {
            ast::DeclKind::Derived { name, .. }
            | ast::DeclKind::View { name, .. }
            | ast::DeclKind::Fun { name, .. } => Some(name.clone()),
            _ => None,
        };
        match &decl.node {
            ast::DeclKind::Derived { name, body, ty, .. } => {
                let effects = self.infer_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects);
            }
            ast::DeclKind::View { name, body, ty, .. } => {
                let effects = self.infer_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects);
            }
            ast::DeclKind::Fun { name, body, ty, .. } => {
                if let Some(body) = body {
                    let effects = self.fun_body_effects(body);
                    self.decl_effects.insert(name.clone(), effects.clone());
                    self.check_annotation(ty, &effects);
                }
            }
            _ => {}
        }
    }

    /// Infer effects of evaluating an expression.
    fn infer_effects(&mut self, expr: &ast::Expr) -> EffectSet {
        match &expr.node {
            ast::ExprKind::Lit(_) | ast::ExprKind::Constructor(_) => EffectSet::empty(),

            ast::ExprKind::Var(name) => {
                // A locally shadowed name is a local value, not the builtin
                // or top-level declaration of the same name.
                let is_shadowed = self.shadowed.iter().any(|s| s == name);
                // For zero-argument IO builtins (now, readLine, randomFloat,
                // randomUuid), referencing the bare name IS the IO action — so
                // return their effects. Multi-argument builtins (println,
                // readFile, sleep, …) perform no IO until applied; a bare
                // reference that is never called (e.g. `let f = println`) is
                // pure, and their effects manifest at the call site instead
                // (see head_call_effects / callee_effects). Attributing them
                // here wrongly tripped the atomic gate on unused references.
                if !is_shadowed && crate::builtins::NULLARY_IO_BUILTINS.contains(&name.as_str()) {
                    if let Some(effects) = self.builtin_effects.get(name) {
                        return effects.clone();
                    }
                }
                // A reference to a local let-bound lambda carries its
                // body's effects: the value may be invoked by whoever
                // receives it (e.g. `let cb = \r -> println "x"` passed
                // to a higher-order function). Mirrors callee_effects'
                // scope lookup — without it, effects laundered through a
                // local name bypassed the atomic gate.
                for scope in self.local_fn_effects.iter().rev() {
                    if let Some(effects) = scope.get(name) {
                        return effects.clone();
                    }
                }
                if !is_shadowed {
                    if let Some(effects) = self.decl_effects.get(name) {
                        return effects.clone();
                    }
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

            ast::ExprKind::App { .. } => {
                // Walk the whole application spine so lambda arguments in
                // *every* position contribute their effects — previously only
                // the syntactically last (outermost) argument was considered,
                // so `withCb2 (\u -> println "hi") 1` lost the console effect.
                let (head, args) = app_spine(expr);
                // Lambda-argument effects propagate to the call unless the
                // callee's annotation declares a *closed* effect row (the
                // documented "absorbing" signature shape). Unannotated
                // user functions (`apply = \f x -> f x`), local bindings,
                // unknown names, and builtins are all assumed to invoke
                // their functional arguments — otherwise an unannotated
                // higher-order helper launders IO past the atomic gate.
                // Immediately-applied lambda heads keep propagate=false:
                // `head_call_effects` analyzes those precisely by binding
                // the lambda-valued arguments into a local scope.
                let propagate_lambda = head_name(head)
                    .map(|n| {
                        self.row_poly_decls.contains(n)
                            || !self.fixed_row_decls.contains(n)
                    })
                    .unwrap_or(false);
                let mut effects = EffectSet::empty();
                for arg in &args {
                    let arg_effects = if propagate_lambda {
                        self.arg_effects(arg)
                    } else {
                        self.infer_effects(arg)
                    };
                    effects = effects.union(&arg_effects);
                }
                let call_effects = self.head_call_effects(head, &args);
                effects.union(&call_effects)
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                if *op == ast::BinOp::Pipe {
                    // `lhs |> rhs` desugars to `rhs lhs`, so `lhs` is an
                    // *argument*. When `lhs` is a lambda and the `rhs` callee is
                    // row-polymorphic (likely to invoke it), thread the lambda
                    // body's effects through `arg_effects`, exactly as the `App`
                    // spine does — otherwise a lambda piped directly into a
                    // higher-order callee (`(\u -> println "x") |> withCb`) loses
                    // its body effects, under-approximating (the unsafe
                    // direction) and slipping past the IO-in-atomic gate.
                    let propagate_lambda = head_name(rhs)
                        .map(|n| {
                            self.row_poly_decls.contains(n) || !self.fixed_row_decls.contains(n)
                        })
                        .unwrap_or(false);
                    let lhs_effects = if propagate_lambda {
                        self.arg_effects(lhs)
                    } else {
                        self.infer_effects(lhs)
                    };
                    let rhs_effects = self.callee_effects(rhs);
                    lhs_effects.union(&rhs_effects)
                } else {
                    let lhs_effects = self.infer_effects(lhs);
                    let rhs_effects = self.infer_effects(rhs);
                    lhs_effects.union(&rhs_effects)
                }
            }

            ast::ExprKind::UnaryOp { operand, .. } => self.infer_effects(operand),

            ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
                let is_replace = matches!(&expr.node, ast::ExprKind::ReplaceSet { .. });
                let mut effects = self.infer_effects(value);
                // The parser produces `SourceRef` for every `*name` write
                // target, including views, so handle both node kinds the
                // same way and dispatch on what the name refers to.
                if let ast::ExprKind::SourceRef(name)
                | ast::ExprKind::DerivedRef(name) = &target.node
                {
                    effects.writes.insert(name.clone());
                    // `replace *rel = v` blindly overwrites and does NOT read
                    // the existing relation, so it must not record a read.
                    // `set *rel = v` does read it, but the value expression
                    // already references `*rel` and contributes that read via
                    // infer_effects(value) above — so adding it here is only
                    // needed (and only correct) for the non-replace form.
                    if !is_replace {
                        effects.reads.insert(name.clone());
                    }
                    if self.view_names.contains(name) {
                        // Writing through a view writes the backing
                        // source(s). The view's inferred effects record
                        // which sources its body reads — those are
                        // exactly the relations a write lands in (plus
                        // any other effects evaluating the view incurs).
                        let view_effects = self
                            .decl_effects
                            .get(name)
                            .cloned()
                            .unwrap_or_else(EffectSet::empty);
                        for src in &view_effects.reads {
                            effects.writes.insert(src.clone());
                        }
                        // Writing through a view (even via `replace`) reads the
                        // backing source: the runtime must reconcile against the
                        // existing backing rows to preserve those outside the
                        // view and fill constant/auto columns, so the view's
                        // reads legitimately propagate as reads here. (This is
                        // why the `!is_replace` read-skip above applies only to
                        // direct relation writes, not view writes.)
                        effects = effects.union(&view_effects);
                    }
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
                                "console, network, fs, clock, and random effects cannot be rolled back",
                            ),
                    );
                }
                // Concurrency builtins that cannot be rolled back (`race`)
                // are rejected by a syntactic walk of the whole body — this
                // also catches indirect usage through locally-bound lambdas.
                // The authoritative list is `builtins::ATOMIC_DISALLOWED_BUILTINS`
                // (its IO members are already covered by the effect check above;
                // `fork` is intentionally permitted, see builtins.rs).
                // The walk is scope-aware: a lambda param, do-bind, let, or
                // case binder named `race` shadows the builtin, so references
                // under that binder are local values, not the primitive.
                let mut disallowed: Vec<(String, Span)> = Vec::new();
                let mut shadowed: Vec<String> = Vec::new();
                collect_unshadowed_disallowed(inner, &mut shadowed, &mut disallowed);
                let syntactic_race_found = !disallowed.is_empty();
                for (name, span) in disallowed {
                    self.diagnostics.push(
                        Diagnostic::error(format!(
                            "`{}` cannot be used inside atomic blocks",
                            name
                        ))
                        .label(span, "used inside an atomic block")
                        .note(
                            "`race` spawns worker threads with independent database \
                             connections; their work cannot be rolled back by the \
                             enclosing transaction",
                        ),
                    );
                }
                // `race` reached through a wrapper (`raceIt = \a b -> race a b`)
                // is invisible to the syntactic walk but its marker propagates
                // through `decl_effects` like any other effect.
                if inner_effects.uses_race && !syntactic_race_found {
                    self.diagnostics.push(
                        Diagnostic::error(
                            "`race` cannot be used inside atomic blocks",
                        )
                        .label(expr.span, "this atomic block calls `race` (possibly indirectly)")
                        .note(
                            "`race` spawns worker threads with independent database \
                             connections; their work cannot be rolled back by the \
                             enclosing transaction",
                        ),
                    );
                }
                // Calls through opaque callees (record fields, computed
                // callees) can hide lambdas whose bodies do IO — e.g.
                // `r = {fn: \u -> println "hidden"}` then `r.fn {}` inside
                // atomic. When the body contains such a call AND a lambda
                // doing IO is reachable from the body (syntactically inside
                // it, or inside any declaration the body references), reject
                // conservatively: atomic bodies are supposed to be DB-only.
                if !inner_effects.has_io() && self.body_may_call_opaque(inner) {
                    // Search for a reachable IO lambda starting from the atomic
                    // body AND from the enclosing declaration's body: a lambda
                    // laundered into the block (bound by a `let`/record field in
                    // the surrounding decl and passed in as a value) is not
                    // syntactically inside `inner`, but the opaque call may end
                    // up invoking it. Rooting at the enclosing decl catches it.
                    let mut roots: Vec<&ast::Expr> = vec![inner];
                    let enclosing = self
                        .current_decl_name
                        .as_ref()
                        .and_then(|n| self.decl_bodies.get(n));
                    if let Some(body) = enclosing {
                        roots.push(body);
                    }
                    if let Some(span) = self.reachable_io_lambda_from(&roots) {
                        self.diagnostics.push(
                            Diagnostic::error(
                                "IO effects are not allowed inside atomic blocks",
                            )
                            .label(expr.span, "this atomic block")
                            .label(
                                span,
                                "this function performs IO and is reachable from a call \
                                 the atomic block makes through an opaque callee",
                            )
                            .note(
                                "console, network, fs, clock, and random effects cannot be rolled back",
                            ),
                        );
                    }
                }
                // "Must interact with relations" is a hard error, so stay
                // conservative: only flag bodies that provably contain no
                // relation operations anywhere syntactically (including
                // inside local lambdas) — effect inference can miss reads
                // performed through bindings it cannot resolve. A call to
                // an opaque callee (lambda parameter, field-accessed
                // function, any name we cannot analyze) is not provably
                // relation-free either, so its presence suppresses the
                // error too.
                if !inner_effects.has_io()
                    && inner_effects.reads.is_empty()
                    && inner_effects.writes.is_empty()
                    && !touches_relations(inner)
                    && !self.body_may_call_opaque(inner)
                {
                    self.diagnostics.push(
                        Diagnostic::error("atomic block must interact with relations")
                            .label(expr.span, "this atomic block has no relation reads or writes"),
                    );
                }
                inner_effects
            }

            ast::ExprKind::Do(stmts) => {
                self.local_fn_effects.push(HashMap::new());
                let shadow_mark = self.shadowed.len();
                let mut effects = EffectSet::empty();
                for stmt in stmts {
                    // Track let-bound lambdas so later calls through the
                    // local name see the lambda body's effects (e.g.
                    // `let f = \u -> *items; rows <- f {}` reads `items`).
                    if let ast::StmtKind::Let { pat, expr } = &stmt.node {
                        if let ast::PatKind::Var(name) = &pat.node {
                            if is_lambda_arg(expr) {
                                let fn_effects = self.fun_body_effects(expr);
                                self.local_fn_effects
                                    .last_mut()
                                    .unwrap()
                                    .insert(name.clone(), fn_effects);
                            }
                        }
                    }
                    let stmt_effects = self.infer_stmt_effects(stmt);
                    effects = effects.union(&stmt_effects);
                    // Binders come into scope for *later* statements.
                    if let ast::StmtKind::Bind { pat, .. }
                    | ast::StmtKind::Let { pat, .. } = &stmt.node
                    {
                        collect_pat_binders(pat, &mut self.shadowed);
                    }
                }
                self.shadowed.truncate(shadow_mark);
                self.local_fn_effects.pop();
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
                    let mark = self.shadowed.len();
                    collect_pat_binders(&arm.pat, &mut self.shadowed);
                    let arm_effects = self.infer_effects(&arm.body);
                    self.shadowed.truncate(mark);
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

            ast::ExprKind::UnitLit { value, .. } => self.infer_effects(value),
            ast::ExprKind::Annot { expr: inner, .. } => self.infer_effects(inner),
            ast::ExprKind::Refine(inner) => self.infer_effects(inner),

            // A `serve` expression is a value (the Server). Its handlers
            // do not execute when `serve` is evaluated — they fire when the
            // server receives a request. But the only way to *use* a Server
            // is to hand it to `listen`, which will run the handlers, so
            // the type system attributes the union of handler effects to
            // the Server's effect row. Mirror that here so explicit
            // annotations on the decl that calls `listen api` actually
            // catch missing effects.
            ast::ExprKind::Serve { handlers, .. } => {
                let mut effects = EffectSet::empty();
                for h in handlers {
                    effects = effects.union(&self.fun_body_effects(&h.body));
                }
                effects
            }
        }
    }

    /// Whether the expression contains a call whose callee cannot be
    /// analyzed for relation access: lambda parameters, field-accessed
    /// functions, or names that are neither builtins nor declarations
    /// with (converged) inferred effects. Such a body is not *provably*
    /// relation-free, so the hard "atomic block must interact with
    /// relations" error must stay quiet. Trait methods are analyzable
    /// (their impl-body union lives in `decl_effects`); constructors and
    /// inline lambdas are analyzable too (the surrounding walk recurses
    /// into lambda bodies).
    fn body_may_call_opaque(&self, expr: &ast::Expr) -> bool {
        let mut visited: HashSet<String> = HashSet::new();
        self.body_may_call_opaque_rec(expr, &mut visited)
    }

    /// Recursive core of `body_may_call_opaque`. Follows calls into known user
    /// functions (`decl_bodies`) so an opaque call hidden one level down — a
    /// helper that invokes a lambda handed to it through a record field — is
    /// still detected. `visited` guards against cycles in mutual recursion.
    fn body_may_call_opaque_rec(
        &self,
        expr: &ast::Expr,
        visited: &mut HashSet<String>,
    ) -> bool {
        // Let-bound lambdas inside the body are analyzable: their bodies
        // are part of the walked tree, so `touches_relations` and effect
        // inference both see through them. (The local_fn_effects scopes
        // those bindings lived in are already popped by the time the
        // Atomic arm runs this check, so collect them syntactically.)
        // The same applies to lambda parameters bound to lambda-literal
        // arguments — including the `(\f -> …) (\u -> …)` shape that
        // desugared `let f = \u -> …` statements take.
        let mut local_lambdas: HashSet<String> = HashSet::new();
        walk_expr(expr, &mut |e| {
            match &e.node {
                ast::ExprKind::Do(stmts) => {
                    for stmt in stmts {
                        if let ast::StmtKind::Let { pat, expr } = &stmt.node {
                            if let ast::PatKind::Var(name) = &pat.node {
                                if is_lambda_arg(expr) {
                                    local_lambdas.insert(name.clone());
                                }
                            }
                        }
                    }
                }
                ast::ExprKind::App { .. } => {
                    let (head, args) = app_spine(e);
                    if let ast::ExprKind::Lambda { params, .. } = &head.node {
                        for (param, arg) in params.iter().zip(args.iter()) {
                            if let ast::PatKind::Var(name) = &param.node {
                                if is_lambda_arg(arg) {
                                    local_lambdas.insert(name.clone());
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        });
        let mut found = false;
        // Known user functions called from this body, to recurse into after the
        // walk (a callee's own body may make the opaque call).
        let mut callees_to_recurse: Vec<String> = Vec::new();
        walk_expr(expr, &mut |e| {
            if found {
                return;
            }
            let callee: Option<&ast::Expr> = match &e.node {
                ast::ExprKind::App { func, .. } => {
                    let mut head: &ast::Expr = func;
                    while let ast::ExprKind::App { func, .. } = &head.node {
                        head = func;
                    }
                    Some(head)
                }
                ast::ExprKind::BinOp { op: ast::BinOp::Pipe, rhs, .. } => {
                    Some(rhs.as_ref())
                }
                _ => None,
            };
            let Some(mut head) = callee else { return };
            // Unwrap annotation-style wrappers around the callee.
            loop {
                match &head.node {
                    ast::ExprKind::Annot { expr: inner, .. }
                    | ast::ExprKind::UnitLit { value: inner, .. }
                    | ast::ExprKind::Refine(inner) => head = inner,
                    _ => break,
                }
            }
            match &head.node {
                ast::ExprKind::Var(name) => {
                    // Desugaring helpers (__bind/__yield/__empty) dispatch
                    // to monad impls whose arguments are walked directly;
                    // operator-trait fallbacks (eq, compare, add, …) are
                    // intrinsically pure and user impls of them appear in
                    // decl_effects.
                    let known = self.builtin_effects.contains_key(name)
                        || self.decl_effects.contains_key(name)
                        || local_lambdas.contains(name)
                        || crate::builtins::INTERNAL_BUILTINS
                            .contains(&name.as_str())
                        || crate::builtins::TRAIT_METHOD_BUILTINS
                            .contains(&name.as_str())
                        || self
                            .local_fn_effects
                            .iter()
                            .any(|scope| scope.contains_key(name));
                    if !known {
                        found = true;
                    } else if self.decl_bodies.contains_key(name)
                        && !local_lambdas.contains(name)
                    {
                        // A known user function — its own body might make the
                        // opaque call (invoking a lambda it was handed via a
                        // record field). Queue it for transitive inspection.
                        callees_to_recurse.push(name.clone());
                    }
                }
                // Constructors build data; inline lambdas are walked
                // directly; a piped App spine is visited as its own
                // App node by the same walk.
                ast::ExprKind::Constructor(_)
                | ast::ExprKind::Lambda { .. }
                | ast::ExprKind::App { .. } => {}
                // Field access, computed callees (if/case results), and
                // anything else: opaque.
                _ => found = true,
            }
        });
        if found {
            return true;
        }
        // Transitively inspect the bodies of called user functions: the opaque
        // call may live one (or more) levels down the call chain.
        for name in callees_to_recurse {
            if visited.insert(name.clone()) {
                if let Some(body) = self.decl_bodies.get(&name) {
                    if self.body_may_call_opaque_rec(body, visited) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Span of a lambda whose body references an atomic-disallowed IO
    /// builtin, reachable from `expr`: either a lambda literal syntactically
    /// inside it, or one inside the body of any declaration it references
    /// (transitively). Used by the atomic gate when the body calls an opaque
    /// callee whose effects cannot be analyzed — such a lambda may be the
    /// thing the opaque call ends up invoking, so reject conservatively.
    fn reachable_io_lambda_from(&self, roots: &[&ast::Expr]) -> Option<Span> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut worklist: Vec<&ast::Expr> = roots.to_vec();
        let mut found: Option<Span> = None;
        while let Some(e) = worklist.pop() {
            if found.is_some() {
                break;
            }
            let mut referenced: Vec<String> = Vec::new();
            walk_expr(e, &mut |node| {
                match &node.node {
                    ast::ExprKind::Lambda { body, .. } => {
                        if found.is_none() && contains_atomic_disallowed_ref(body) {
                            found = Some(node.span);
                        }
                    }
                    ast::ExprKind::Var(name) => referenced.push(name.clone()),
                    _ => {}
                }
            });
            for name in referenced {
                if seen.insert(name.clone()) {
                    if let Some(body) = self.decl_bodies.get(&name) {
                        worklist.push(body);
                    }
                }
            }
        }
        found
    }

    /// Infer effects of a do-block statement.
    fn infer_stmt_effects(&mut self, stmt: &ast::Stmt) -> EffectSet {
        match &stmt.node {
            ast::StmtKind::Bind { expr, .. } => self.infer_effects(expr),
            ast::StmtKind::Let { expr, .. } => self.infer_effects(expr),
            ast::StmtKind::Where { cond } => self.infer_effects(cond),
            ast::StmtKind::GroupBy { key } => self.infer_effects(key),
            ast::StmtKind::Expr(expr) => self.infer_effects(expr),
        }
    }

    /// Effects of a function-call argument. Like `infer_effects`, but if the
    /// argument is a lambda the callee will likely invoke, also include the
    /// lambda body's effects. HM unifies higher-order callbacks via effect-row
    /// variables (e.g. `IO {| e}` → `IO {r *sessions | e}`); without this
    /// propagation, the effect checker under-approximates and disagrees with
    /// the LSP's HM-derived signatures.
    fn arg_effects(&mut self, arg: &ast::Expr) -> EffectSet {
        let mut effects = self.infer_effects(arg);
        if is_lambda_arg(arg) {
            effects = effects.union(&self.fun_body_effects(arg));
        }
        effects
    }

    /// Resolve effects when a function expression is *called*.
    fn callee_effects(&mut self, func_expr: &ast::Expr) -> EffectSet {
        match &func_expr.node {
            ast::ExprKind::Var(name) => {
                // Check builtins first, then local let-bound lambdas
                // (innermost scope first), then user declarations. A
                // locally shadowed name never resolves to the builtin or
                // the top-level declaration.
                let is_shadowed = self.shadowed.iter().any(|s| s == name);
                if !is_shadowed {
                    if let Some(effects) = self.builtin_effects.get(name) {
                        return effects.clone();
                    }
                }
                for scope in self.local_fn_effects.iter().rev() {
                    if let Some(effects) = scope.get(name) {
                        return effects.clone();
                    }
                }
                if !is_shadowed {
                    if let Some(effects) = self.decl_effects.get(name) {
                        return effects.clone();
                    }
                }
                // Unknown callee — treat as pure (conservative, no false positives)
                EffectSet::empty()
            }

            ast::ExprKind::Lambda { params, body } => {
                // Immediately-applied lambda: effects are the body's effects
                let mark = self.shadowed.len();
                for p in params {
                    collect_pat_binders(p, &mut self.shadowed);
                }
                let effects = self.infer_effects(body);
                self.shadowed.truncate(mark);
                effects
            }

            ast::ExprKind::App { .. } => {
                // Curried call: e.g. `(f x) y` — the callee is `f x` partially
                // applied. The effects of evaluating the application expression
                // already cover the head plus all arguments (spine walk in
                // `infer_effects`), so delegate there. This also propagates
                // lambda-argument effects of row-polymorphic callees reached
                // through pipes (`x |> withCb (\u -> ...)`).
                self.infer_effects(func_expr)
            }

            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                // Callee is an if/else: effects are the union of branches' callable effects
                let c = self.infer_effects(cond);
                let t = self.callee_effects(then_branch);
                let e = self.callee_effects(else_branch);
                c.union(&t).union(&e)
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                // Callee is a case: effects are the union of each arm's callable effects
                let mut effects = self.infer_effects(scrutinee);
                for arm in arms {
                    effects = effects.union(&self.callee_effects(&arm.body));
                }
                effects
            }

            // Wrapper expressions: unwrap to find the actual callee.
            // Without this, an annotated lambda like `(\x -> println x : Type) y`
            // would fall through to infer_effects, which treats Lambda as pure
            // (creating a lambda IS pure), missing the body's call effects.
            ast::ExprKind::UnitLit { value, .. } => self.callee_effects(value),
            ast::ExprKind::Annot { expr, .. } => self.callee_effects(expr),
            ast::ExprKind::Refine(inner) => self.callee_effects(inner),

            other => {
                // Field access, etc. — fall back to infer_effects
                let _ = other;
                self.infer_effects(func_expr)
            }
        }
    }

    /// Effects of invoking the head of an application spine with the given
    /// argument expressions. Lambdas applied immediately — including the
    /// shape `let pat = e` desugars to (`(\pat -> rest) e`) — bind their
    /// lambda-valued arguments into a local scope so that calls through the
    /// bound name inside the body see the callback's effects.
    fn head_call_effects(&mut self, head: &ast::Expr, args: &[&ast::Expr]) -> EffectSet {
        match &head.node {
            ast::ExprKind::Lambda { params, body } => {
                let mut scope = HashMap::new();
                for (param, arg) in params.iter().zip(args.iter()) {
                    if let ast::PatKind::Var(name) = &param.node {
                        if is_lambda_arg(arg) {
                            let fn_effects = self.fun_body_effects(arg);
                            scope.insert(name.clone(), fn_effects);
                        }
                    }
                }
                let mark = self.shadowed.len();
                for p in params {
                    collect_pat_binders(p, &mut self.shadowed);
                }
                self.local_fn_effects.push(scope);
                let effects = self.infer_effects(body);
                self.local_fn_effects.pop();
                self.shadowed.truncate(mark);
                effects
            }
            // Wrappers: unwrap to find the lambda (if any) inside.
            ast::ExprKind::UnitLit { value, .. } => self.head_call_effects(value, args),
            ast::ExprKind::Annot { expr, .. } => self.head_call_effects(expr, args),
            ast::ExprKind::Refine(inner) => self.head_call_effects(inner, args),
            _ => self.callee_effects(head),
        }
    }

    /// Unwrap lambda chain to get the effects of the function body.
    /// `\x y -> set *foo = ...` → effects of the `set` expression.
    fn fun_body_effects(&mut self, body: &ast::Expr) -> EffectSet {
        match &body.node {
            ast::ExprKind::Lambda { body: inner, params } => {
                let mark = self.shadowed.len();
                for p in params {
                    collect_pat_binders(p, &mut self.shadowed);
                }
                let effects = self.fun_body_effects(inner);
                self.shadowed.truncate(mark);
                effects
            }
            // Wrapper expressions: unwrap to find the lambda chain inside.
            ast::ExprKind::UnitLit { value, .. } => self.fun_body_effects(value),
            ast::ExprKind::Annot { expr, .. } => self.fun_body_effects(expr),
            ast::ExprKind::Refine(inner) => self.fun_body_effects(inner),
            _ => self.infer_effects(body),
        }
    }

    /// Check that explicit effect annotations (if any) are a superset of inferred effects.
    ///
    /// Anchors squiggles to the effect-bearing type subnode (e.g. the `IO {fs} Text`
    /// part of a signature) rather than the whole declaration span — a decl-wide
    /// span would also visibly underline any comments inside the body.
    fn check_annotation(&mut self, ty: &Option<ast::TypeScheme>, inferred: &EffectSet) {
        let scheme = match ty {
            Some(s) => s,
            None => return,
        };

        if let Some(declared) = extract_effects(&scheme.ty) {
            let label_span = effects_span(&scheme.ty).unwrap_or(scheme.ty.span);
            if !inferred.is_subset_of(&declared) {
                let extra = inferred.difference(&declared);
                self.diagnostics.push(
                    Diagnostic::error("inferred effects exceed declared effects")
                        .label(label_span, "declared effects here")
                        .note(format!("declared effects: {}", declared))
                        .note(format!("inferred effects: {}", inferred))
                        .note(format!("undeclared effects: {}", extra)),
                );
            }
            let unused = declared.difference(inferred);
            if !unused.is_empty() {
                self.diagnostics.push(
                    Diagnostic::warning("declared effects are not used")
                        .label(label_span, "declared effects here")
                        .note(format!("declared effects: {}", declared))
                        .note(format!("inferred effects: {}", inferred))
                        .note(format!("unused effects: {}", unused)),
                );
            }
        }
    }
}

/// Find the span of the effect-bearing subnode of a type, if any.
///
/// For `a -> IO {fs} b` returns the span of `IO {fs} b`; for a bare
/// `IO {fs} Text` returns the whole type's span. Used so effect-annotation
/// diagnostics underline the actual annotation rather than the whole
/// declaration (which would also cover comments inside the body).
/// Whether `arg` is a lambda (possibly wrapped in annotations / refinements).
/// Mirrors the wrappers `fun_body_effects` traverses, so the two stay in sync.
fn is_lambda_arg(arg: &ast::Expr) -> bool {
    match &arg.node {
        ast::ExprKind::Lambda { .. } => true,
        ast::ExprKind::UnitLit { value, .. }
        | ast::ExprKind::Annot { expr: value, .. }
        | ast::ExprKind::Refine(value) => is_lambda_arg(value),
        _ => false,
    }
}

/// Visit every expression node in `expr` (pre-order), including lambda
/// bodies, do-block statements, case arms, and serve handlers. Used for
/// conservative syntactic checks on atomic bodies.
fn walk_expr(expr: &ast::Expr, f: &mut impl FnMut(&ast::Expr)) {
    f(expr);
    match &expr.node {
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Var(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_) => {}
        ast::ExprKind::Record(fields) => {
            for field in fields {
                walk_expr(&field.value, f);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            walk_expr(base, f);
            for field in fields {
                walk_expr(&field.value, f);
            }
        }
        ast::ExprKind::FieldAccess { expr: inner, .. } => walk_expr(inner, f),
        ast::ExprKind::List(elems) => {
            for elem in elems {
                walk_expr(elem, f);
            }
        }
        ast::ExprKind::Lambda { body, .. } => walk_expr(body, f),
        ast::ExprKind::App { func, arg } => {
            walk_expr(func, f);
            walk_expr(arg, f);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            walk_expr(lhs, f);
            walk_expr(rhs, f);
        }
        ast::ExprKind::UnaryOp { operand, .. } => walk_expr(operand, f),
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            walk_expr(cond, f);
            walk_expr(then_branch, f);
            walk_expr(else_branch, f);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            walk_expr(scrutinee, f);
            for arm in arms {
                walk_expr(&arm.body, f);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. }
                    | ast::StmtKind::Let { expr, .. } => walk_expr(expr, f),
                    ast::StmtKind::Where { cond } => walk_expr(cond, f),
                    ast::StmtKind::GroupBy { key } => walk_expr(key, f),
                    ast::StmtKind::Expr(expr) => walk_expr(expr, f),
                }
            }
        }
        ast::ExprKind::Set { target, value }
        | ast::ExprKind::ReplaceSet { target, value } => {
            walk_expr(target, f);
            walk_expr(value, f);
        }
        ast::ExprKind::Atomic(inner) => walk_expr(inner, f),
        ast::ExprKind::UnitLit { value, .. } => walk_expr(value, f),
        ast::ExprKind::Annot { expr: inner, .. } => walk_expr(inner, f),
        ast::ExprKind::Refine(inner) => walk_expr(inner, f),
        ast::ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                walk_expr(&h.body, f);
            }
        }
    }
}

/// Collect every variable name bound by a pattern (for shadow tracking).
fn collect_pat_binders(pat: &ast::Pat, out: &mut Vec<String>) {
    match &pat.node {
        ast::PatKind::Var(name) => out.push(name.clone()),
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => {}
        ast::PatKind::Constructor { payload, .. } => collect_pat_binders(payload, out),
        ast::PatKind::Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => collect_pat_binders(p, out),
                    None => out.push(f.name.clone()),
                }
            }
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_binders(p, out);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_binders(head, out);
            collect_pat_binders(tail, out);
        }
    }
}

/// Scope-aware version of the disallowed-concurrency-builtin scan used on
/// atomic bodies: flags references to names in both
/// `ATOMIC_DISALLOWED_BUILTINS` and `CONCURRENCY_BUILTINS`, but skips
/// references that are shadowed by an enclosing lambda param, do-bind,
/// let binding, or case pattern binder of the same name.
fn collect_unshadowed_disallowed(
    expr: &ast::Expr,
    shadowed: &mut Vec<String>,
    out: &mut Vec<(String, Span)>,
) {
    match &expr.node {
        ast::ExprKind::Var(name) => {
            if crate::builtins::ATOMIC_DISALLOWED_BUILTINS.contains(&name.as_str())
                && crate::builtins::CONCURRENCY_BUILTINS.contains(&name.as_str())
                && !shadowed.iter().any(|s| s == name)
            {
                out.push((name.clone(), expr.span));
            }
        }
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_) => {}
        ast::ExprKind::Record(fields) => {
            for field in fields {
                collect_unshadowed_disallowed(&field.value, shadowed, out);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_unshadowed_disallowed(base, shadowed, out);
            for field in fields {
                collect_unshadowed_disallowed(&field.value, shadowed, out);
            }
        }
        ast::ExprKind::FieldAccess { expr: inner, .. } => {
            collect_unshadowed_disallowed(inner, shadowed, out);
        }
        ast::ExprKind::List(elems) => {
            for elem in elems {
                collect_unshadowed_disallowed(elem, shadowed, out);
            }
        }
        ast::ExprKind::Lambda { params, body } => {
            let mark = shadowed.len();
            for p in params {
                collect_pat_binders(p, shadowed);
            }
            collect_unshadowed_disallowed(body, shadowed, out);
            shadowed.truncate(mark);
        }
        ast::ExprKind::App { func, arg } => {
            collect_unshadowed_disallowed(func, shadowed, out);
            collect_unshadowed_disallowed(arg, shadowed, out);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_unshadowed_disallowed(lhs, shadowed, out);
            collect_unshadowed_disallowed(rhs, shadowed, out);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_unshadowed_disallowed(operand, shadowed, out);
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            collect_unshadowed_disallowed(cond, shadowed, out);
            collect_unshadowed_disallowed(then_branch, shadowed, out);
            collect_unshadowed_disallowed(else_branch, shadowed, out);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_unshadowed_disallowed(scrutinee, shadowed, out);
            for arm in arms {
                let mark = shadowed.len();
                collect_pat_binders(&arm.pat, shadowed);
                collect_unshadowed_disallowed(&arm.body, shadowed, out);
                shadowed.truncate(mark);
            }
        }
        ast::ExprKind::Do(stmts) => {
            let mark = shadowed.len();
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } => {
                        // The bound expression is evaluated before the
                        // binder comes into scope.
                        collect_unshadowed_disallowed(expr, shadowed, out);
                        collect_pat_binders(pat, shadowed);
                    }
                    ast::StmtKind::Let { pat, expr } => {
                        collect_unshadowed_disallowed(expr, shadowed, out);
                        collect_pat_binders(pat, shadowed);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_unshadowed_disallowed(cond, shadowed, out);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_unshadowed_disallowed(key, shadowed, out);
                    }
                    ast::StmtKind::Expr(expr) => {
                        collect_unshadowed_disallowed(expr, shadowed, out);
                    }
                }
            }
            shadowed.truncate(mark);
        }
        ast::ExprKind::Set { target, value }
        | ast::ExprKind::ReplaceSet { target, value } => {
            collect_unshadowed_disallowed(target, shadowed, out);
            collect_unshadowed_disallowed(value, shadowed, out);
        }
        ast::ExprKind::Atomic(inner) => collect_unshadowed_disallowed(inner, shadowed, out),
        ast::ExprKind::UnitLit { value, .. } => collect_unshadowed_disallowed(value, shadowed, out),
        ast::ExprKind::Annot { expr: inner, .. } => {
            collect_unshadowed_disallowed(inner, shadowed, out);
        }
        ast::ExprKind::Refine(inner) => collect_unshadowed_disallowed(inner, shadowed, out),
        ast::ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                collect_unshadowed_disallowed(&h.body, shadowed, out);
            }
        }
    }
}

/// Whether the expression syntactically references any builtin from
/// `ATOMIC_DISALLOWED_BUILTINS` (console/network/fs/clock/random IO plus
/// `race`; `fork` and `retry` are intentionally permitted in atomic).
/// Purely syntactic and shadow-unaware — used only for the conservative
/// opaque-callee scan inside atomic bodies, where false positives are
/// acceptable (atomic bodies are supposed to be DB-only).
fn contains_atomic_disallowed_ref(expr: &ast::Expr) -> bool {
    let mut found = false;
    walk_expr(expr, &mut |e| {
        if let ast::ExprKind::Var(name) = &e.node {
            if crate::builtins::ATOMIC_DISALLOWED_BUILTINS.contains(&name.as_str()) {
                found = true;
            }
        }
    });
    found
}

/// Whether the expression syntactically contains any relation operation
/// (source/derived reference or relation write) anywhere, including inside
/// lambdas that effect inference may not see being called.
fn touches_relations(expr: &ast::Expr) -> bool {
    let mut found = false;
    walk_expr(expr, &mut |e| {
        if matches!(
            &e.node,
            ast::ExprKind::SourceRef(_)
                | ast::ExprKind::DerivedRef(_)
                | ast::ExprKind::Set { .. }
                | ast::ExprKind::ReplaceSet { .. }
        ) {
            found = true;
        }
    });
    found
}

/// Unwind a (possibly curried) application into its head and argument list.
/// `f x y z` → `(f, [x, y, z])`.
fn app_spine(expr: &ast::Expr) -> (&ast::Expr, Vec<&ast::Expr>) {
    let mut args = Vec::new();
    let mut cur = expr;
    while let ast::ExprKind::App { func, arg } = &cur.node {
        args.push(arg.as_ref());
        cur = func;
    }
    args.reverse();
    (cur, args)
}

/// Resolve the head name of a (possibly curried) function expression.
/// `f x y z` → `Some("f")`. Returns `None` for non-named callees.
fn head_name(expr: &ast::Expr) -> Option<&str> {
    match &expr.node {
        ast::ExprKind::Var(name) => Some(name.as_str()),
        ast::ExprKind::App { func, .. } => head_name(func),
        ast::ExprKind::UnitLit { value, .. } | ast::ExprKind::Annot { expr: value, .. } => {
            head_name(value)
        }
        // Match the wrapper set unwrapped by is_lambda_arg/fun_body_effects.
        ast::ExprKind::Refine(inner) => head_name(inner),
        _ => None,
    }
}

/// Whether `ty` contains an `IO {... | r}` row variable anywhere in its
/// structure. Used to detect row-polymorphic effect signatures so that
/// higher-order callbacks pass their effects through to the caller — the
/// effect-checker analogue of HM's row-polymorphic IO unification.
fn type_has_effect_row_var(ty: &ast::Type) -> bool {
    match &ty.node {
        ast::TypeKind::IO { rest, ty: inner, .. } if !rest.is_empty() => {
            // `_` opts out of effect checking and isn't a row variable.
            // Any non-`_` name (including those joined by `\/`) counts.
            rest.iter().any(|name| name != "_") || type_has_effect_row_var(inner)
        }
        ast::TypeKind::IO { ty: inner, .. } => type_has_effect_row_var(inner),
        ast::TypeKind::Function { param, result } => {
            type_has_effect_row_var(param) || type_has_effect_row_var(result)
        }
        ast::TypeKind::Effectful { ty: inner, .. } => type_has_effect_row_var(inner),
        ast::TypeKind::App { func, arg } => {
            type_has_effect_row_var(func) || type_has_effect_row_var(arg)
        }
        ast::TypeKind::Relation(inner) => type_has_effect_row_var(inner),
        ast::TypeKind::UnitAnnotated { base, .. } => type_has_effect_row_var(base),
        ast::TypeKind::Refined { base, .. } => type_has_effect_row_var(base),
        _ => false,
    }
}

fn effects_span(ty: &ast::Type) -> Option<Span> {
    match &ty.node {
        ast::TypeKind::Effectful { .. } | ast::TypeKind::IO { .. } => Some(ty.span),
        ast::TypeKind::Function { result, .. } => {
            // Mirror `extract_effects`: declared effects come only from the
            // result side, so the label should point there too.
            effects_span(result)
        }
        _ => None,
    }
}

/// Extract the declared effect set from a type, if any.
///
/// Both `{effects} a -> b` (old syntax) and `IO {effects} a` (new syntax)
/// declare effect sets, including reads/writes. The two syntaxes share the
/// same `Effect` AST representation, so the same conversion applies.
///
/// `IO _ a` (wildcard) returns `None` — the user opted out of declaring
/// effects, so there's nothing for `check_annotation` to compare against.
fn extract_effects(ty: &ast::Type) -> Option<EffectSet> {
    match &ty.node {
        ast::TypeKind::Effectful { effects, .. } => {
            Some(EffectSet::from_ast_effects(effects))
        }
        ast::TypeKind::IO { effects, rest, .. } => {
            // An open effect row means the declared effect set is NOT a closed
            // upper bound: the row tail (`_` or any named row variable, alone
            // or in a `\/` union) can absorb additional effects, so the
            // subset check against the concrete prefix is meaningless and
            // would spuriously reject `g : IO {| e} {}` whose body does IO,
            // or `f : IO {console | e} a` whose body/callbacks add effects
            // beyond `console`. Only a fully closed row (`rest` empty) gives
            // a comparable declared set. Mirrors `type_has_effect_row_var`,
            // which classifies any non-`_` `rest` element as a row variable.
            if rest.is_empty() {
                Some(EffectSet::from_ast_effects(effects))
            } else {
                None
            }
        }
        ast::TypeKind::Function { result, .. } => {
            // Only the RESULT side of the arrow declares the function's own
            // effects. Effect rows on parameter types (e.g. the callback in
            // `(Int -> IO {console} {}) -> IO {} {}`) describe what the
            // *callback* may do — unioning them in would let a function's
            // body launder its own effects through a parameter annotation.
            extract_effects(result)
        }
        _ => None,
    }
}

// ── Public entry point ───────────────────────────────────────────

/// Per-declaration effect information: maps declaration names to their inferred effects.
pub type EffectInfo = HashMap<String, EffectSet>;

pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    let mut checker = EffectChecker::new();
    checker.run(module);
    checker.diagnostics
}

/// Like `check` but also returns per-declaration effect information.
pub fn check_with_effects(module: &ast::Module) -> (Vec<Diagnostic>, EffectInfo) {
    let mut checker = EffectChecker::new();
    checker.run(module);
    (checker.diagnostics, checker.decl_effects)
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
        assert!(e.has_io());
    }

    #[test]
    fn display_format() {
        let mut e = EffectSet::empty();
        e.reads.insert("people".into());
        e.console = true;
        assert_eq!(format!("{}", e), "{r *people, console}");
    }

    #[test]
    fn display_coalesces_rw() {
        let mut e = EffectSet::empty();
        e.reads.insert("people".into());
        e.writes.insert("people".into());
        e.reads.insert("logs".into());
        assert_eq!(format!("{}", e), "{r *logs, rw *people}");
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
        let module = Module { imports: vec![], decls };
        let mut checker = EffectChecker::new();
        checker.run(&module);
        (checker.diagnostics, checker.decl_effects)
    }

    fn make_decl(node: DeclKind) -> Decl {
        Decl { node, span: span(), exported: false }
    }

    fn make_source(name: &str) -> Decl {
        make_decl(DeclKind::Source {
            name: name.into(),
            ty: spanned(TypeKind::Relation(Box::new(spanned(TypeKind::Named(
                "T".into(),
            ))))),
        })
    }

    fn make_fun(name: &str, body: Expr) -> Decl {
        make_decl(DeclKind::Fun {
            name: name.into(),
            ty: None,
            body: Some(body),
        })
    }

    fn make_fun_with_type(name: &str, body: Expr, ty: TypeScheme) -> Decl {
        make_decl(DeclKind::Fun {
            name: name.into(),
            ty: Some(ty),
            body: Some(body),
        })
    }

    #[test]
    fn literal_is_pure() {
        let body = spanned(ExprKind::Lit(Literal::Int("42".into())));
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
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("yield".into()))),
                arg: Box::new(spanned(ExprKind::Var("p".into()))),
            }))),
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
    fn atomic_clock_without_relations_error() {
        // atomic (now) — clock alone has no relation interaction
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Var(
            "now".into(),
        )))));
        let (diags, _effects) = check_module(vec![make_fun("f", body)]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("IO effects are not allowed inside atomic"));
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
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("yield".into()))),
                arg: Box::new(spanned(ExprKind::Var("p".into()))),
            }))),
        ]));
        let derived = make_decl(DeclKind::Derived {
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
        // f : {r *people, console} Int -> Int
        // f = \x -> do { println *people; yield x }
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::Do(vec![
                spanned(StmtKind::Expr(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::SourceRef("people".into()))),
                }))),
                spanned(StmtKind::Expr(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("yield".into()))),
                    arg: Box::new(spanned(ExprKind::Var("x".into()))),
                }))),
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
        // Declares only {r *people} but actually uses console too
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
            cond: Box::new(spanned(ExprKind::Lit(Literal::Int("1".into())))),
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
    fn io_annotation_empty_rejects_console() {
        // f : IO {} {}
        // f = \_ -> println "hello"
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
            })),
        });
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::IO {
                effects: vec![],
                rest: vec![],
                ty: Box::new(spanned(TypeKind::Record { fields: vec![], rest: None })),
            }),
        };
        let (diags, _) = check_module(vec![make_fun_with_type("f", body, ty)]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("exceed"));
    }

    #[test]
    fn io_annotation_with_console_accepts_println() {
        // f : IO {console} {}
        let body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("x".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
            })),
        });
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::IO {
                effects: vec![Effect::Console],
                rest: vec![],
                ty: Box::new(spanned(TypeKind::Record { fields: vec![], rest: None })),
            }),
        };
        let (diags, _) = check_module(vec![make_fun_with_type("f", body, ty)]);
        assert!(diags.is_empty());
    }

    #[test]
    fn io_annotation_empty_rejects_reads() {
        // f : IO {} [T] — reading from a source should be rejected
        let body = spanned(ExprKind::SourceRef("people".into()));
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::IO {
                effects: vec![],
                rest: vec![],
                ty: Box::new(spanned(TypeKind::Relation(Box::new(spanned(
                    TypeKind::Named("T".into()),
                ))))),
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
    fn io_annotation_with_reads_accepts_source_ref() {
        // f : IO {r *people} [T] — reads declared, source ref allowed
        let body = spanned(ExprKind::SourceRef("people".into()));
        let ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::IO {
                effects: vec![Effect::Reads("people".into())],
                rest: vec![],
                ty: Box::new(spanned(TypeKind::Relation(Box::new(spanned(
                    TypeKind::Named("T".into()),
                ))))),
            }),
        };
        let (diags, _) = check_module(vec![
            make_source("people"),
            make_fun_with_type("f", body, ty),
        ]);
        assert!(diags.is_empty());
    }

    /// Regression: effect-annotation diagnostics anchor to the IO/Effectful
    /// subnode of the type, not the whole declaration. A decl-wide span runs
    /// from the declaration's first token to the end of its body — when the
    /// body contains comments, the LSP renders a squiggle through them, which
    /// the user reported as "errors underline comments".
    #[test]
    fn annotation_diagnostic_label_targets_type_not_decl() {
        let io_ty = Spanned::new(
            TypeKind::IO {
                effects: vec![Effect::Reads("people".into())],
                rest: vec![],
                ty: Box::new(spanned(TypeKind::Relation(Box::new(spanned(
                    TypeKind::Named("T".into()),
                ))))),
            },
            Span::new(100, 130),
        );
        let ty = TypeScheme { constraints: vec![], ty: io_ty };
        let body = spanned(ExprKind::Lit(Literal::Int("42".into())));
        let decl = Decl {
            node: DeclKind::Fun {
                name: "f".into(),
                ty: Some(ty),
                body: Some(body),
            },
            span: Span::new(0, 200), // wide decl span, would cover comments
            exported: false,
        };
        let (diags, _) = check_module(vec![decl]);
        assert_eq!(diags.len(), 1, "expected unused-effects warning");
        let label_span = diags[0].labels[0].span;
        assert_eq!(
            label_span,
            Span::new(100, 130),
            "label must point at the IO type subnode (100..130), \
             not the decl span (0..200) which would underline body comments"
        );
    }

    /// Effects of a lambda passed to a row-polymorphic callee propagate to the
    /// caller. Mirrors the HM behavior where the IO row variable in
    /// `withCallback : (a -> IO {| e} b) -> IO {| e} b` unifies with the
    /// lambda's effect row.
    #[test]
    fn row_poly_callee_propagates_lambda_effects() {
        // withCb : (Int -> IO {| e} {}) -> IO {| e} {}
        // withCb = \cb -> cb 0
        let with_cb_body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("cb".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("cb".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Int("0".into())))),
            })),
        });
        let unit_ty = || spanned(TypeKind::Record { fields: vec![], rest: None });
        let with_cb_ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::Function {
                param: Box::new(spanned(TypeKind::Function {
                    param: Box::new(spanned(TypeKind::Named("Int".into()))),
                    result: Box::new(spanned(TypeKind::IO {
                        effects: vec![],
                        rest: vec!["e".into()],
                        ty: Box::new(unit_ty()),
                    })),
                })),
                result: Box::new(spanned(TypeKind::IO {
                    effects: vec![],
                    rest: vec!["e".into()],
                    ty: Box::new(unit_ty()),
                })),
            }),
        };

        // f = withCb (\_ -> println "hi")
        let f_body = spanned(ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("withCb".into()))),
            arg: Box::new(spanned(ExprKind::Lambda {
                params: vec![spanned(PatKind::Var("_".into()))],
                body: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
                })),
            })),
        });

        let (diags, effects) = check_module(vec![
            make_fun_with_type("withCb", with_cb_body, with_cb_ty),
            make_fun("f", f_body),
        ]);
        assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
        assert!(
            effects["f"].console,
            "lambda's println effect should propagate through row-poly callee"
        );
    }

    /// A non-row-poly callee (e.g. `forEach : (a -> IO {} {}) -> IO {} {}`)
    /// absorbs its callback's effects in its declared row, so we do *not*
    /// propagate the lambda's body effects to the caller.
    #[test]
    fn non_row_poly_callee_does_not_propagate_lambda_effects() {
        // runIt : (Int -> IO {} {}) -> IO {} {}
        // runIt = \cb -> cb 0
        let run_it_body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("cb".into()))],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("cb".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Int("0".into())))),
            })),
        });
        let unit_ty = || spanned(TypeKind::Record { fields: vec![], rest: None });
        let run_it_ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::Function {
                param: Box::new(spanned(TypeKind::Function {
                    param: Box::new(spanned(TypeKind::Named("Int".into()))),
                    result: Box::new(spanned(TypeKind::IO {
                        effects: vec![],
                        rest: vec![],
                        ty: Box::new(unit_ty()),
                    })),
                })),
                result: Box::new(spanned(TypeKind::IO {
                    effects: vec![],
                    rest: vec![],
                    ty: Box::new(unit_ty()),
                })),
            }),
        };

        // g = runIt (\_ -> println "hi") — caller declares no console effect
        let g_body = spanned(ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("runIt".into()))),
            arg: Box::new(spanned(ExprKind::Lambda {
                params: vec![spanned(PatKind::Var("_".into()))],
                body: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
                })),
            })),
        });

        let (_diags, effects) = check_module(vec![
            make_fun_with_type("runIt", run_it_body, run_it_ty),
            make_fun("g", g_body),
        ]);
        assert!(
            !effects["g"].console,
            "non-row-poly callee should absorb lambda effects, not propagate"
        );
    }

    /// Regression: an open effect row (`IO {| e} a` or `IO {console | e} a`)
    /// must not be treated as a closed declared set. The row variable can
    /// absorb extra effects, so the subset check would spuriously reject a
    /// body that performs IO beyond the concrete prefix. Only `_` was
    /// previously recognized as opting out; a named row variable was not.
    #[test]
    fn open_row_var_annotation_absorbs_body_effects() {
        let unit_ty = || spanned(TypeKind::Record { fields: vec![], rest: None });
        let g_body = spanned(ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("println".into()))),
            arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
        });
        let g_ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::IO {
                effects: vec![],
                rest: vec!["e".into()],
                ty: Box::new(unit_ty()),
            }),
        };
        let (diags, _effects) = check_module(vec![make_fun_with_type("g", g_body, g_ty)]);
        assert!(
            diags.is_empty(),
            "open row-var signature should absorb body effects: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn case_unions_arms() {
        let body = spanned(ExprKind::Case {
            scrutinee: Box::new(spanned(ExprKind::Var("x".into()))),
            arms: vec![
                CaseArm {
                    pat: spanned(PatKind::Lit(Literal::Int("1".into()))),
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
