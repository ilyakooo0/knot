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
            && !self.uses_race
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
            && !self.uses_race
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
    /// Inferred effects per declaration name, computed under fork-stripping
    /// (`suppress_fork_io = true`). Consulted by cross-declaration Var/DerivedRef
    /// lookups when recomputing the atomic gate's fork-stripped view, so that a
    /// helper function whose only IO is a `fork`ed action (`helper = \u -> fork
    /// (println …)`) is not falsely treated as IO when called inside `atomic`.
    decl_effects_atomic_safe: HashMap<String, EffectSet>,
    /// Built-in function effects.
    builtin_effects: HashMap<String, EffectSet>,
    /// Known source relation names.
    source_names: HashSet<String>,
    /// Source names whose declared type is a plain scalar (not `[T]`). A `set`
    /// on a scalar source is a pure overwrite unless its value references the
    /// source (in which case `infer_effects(value)` already records the read),
    /// so the `Set` arm must not add a spurious `r *rel` for these — that would
    /// force an honest `{w *rel}` signature to widen to `{rw *rel}`.
    scalar_source_names: HashSet<String>,
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
    /// The *declared* closed result-effect set for each `fixed_row_decls`
    /// entry. Because a closed-row callee "absorbs" its callback arguments,
    /// the callback's effects are dropped at the call site — but the callee's
    /// own declared row is exactly its promised effects (e.g. `runTwice :
    /// (a -> IO {console} {}) -> a -> IO {console} {}` promises `{console}`).
    /// The callee's *inferred* effects are computed with its callback
    /// parameter masked, so they'd be empty; using the declared row instead
    /// prevents under-reporting the caller's effects (and the spurious
    /// "declared effects are not used" warning it causes).
    fixed_row_effects: HashMap<String, EffectSet>,
    /// Stack of local scopes mapping with-bound (or immediately-applied-
    /// lambda-bound) function names to the effects of their bodies. Lets the
    /// checker see effects of calls through local bindings, e.g.
    /// `with {f: \u -> *items} (do { rows <- f {} })` reads `items`.
    ///
    /// The `bool` flags an *opaque mask*: an entry inserted only to shadow an
    /// outer effectful binding of the same name (a lambda/case parameter or a
    /// `<-` bind), whose body is NOT an analyzable local function. Effect
    /// lookups treat masks as pure (their real effects, if any, flow in at the
    /// call site), but `body_may_call_opaque` must NOT count them as "known"
    /// functions — a call through such a name is still an opaque call.
    local_fn_effects: Vec<HashMap<String, (EffectSet, bool)>>,
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
    /// When set, the `App` arm strips the *IO* effects (console/network/fs/
    /// clock/random) contributed by a `fork`'s spawned argument, keeping only
    /// its relation reads/writes and `uses_race` marker. `fork`'s spawned IO
    /// runs on an independent connection and is intentionally permitted inside
    /// `atomic` (see `builtins::ATOMIC_DISALLOWED_BUILTINS`), so the atomic IO
    /// gate must not count it — even though those effects legitimately
    /// propagate through `fork`'s type into the enclosing declaration's
    /// overall inferred effect set (computed with this flag off).
    suppress_fork_io: bool,
    /// Declarations that are `fork` wrappers: `name -> forwarded param index`.
    /// A fork wrapper is a lambda chain whose body is exactly `fork <param>`
    /// (or `<param> |> fork`); the argument passed at the forwarded position is
    /// spawned on an independent connection, so — like a syntactic
    /// `fork (…)` — its IO never runs in the caller and is stripped from the
    /// atomic-gate view at the call site. Without this, `atomic (forkIt
    /// (println …))` is falsely rejected while `atomic (fork (println …))` is
    /// allowed. Deliberately minimal (single direct forward) so it can only
    /// ever *remove* effects that fork already defers — never launder real IO.
    fork_wrapper_params: HashMap<String, usize>,
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
            BYTES_BUILTINS, CLOCK_BUILTINS, CONSOLE_BUILTINS, FS_BUILTINS, NETWORK_BUILTINS,
            PURE_BUILTINS, RANDOM_BUILTINS,
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
        // Bytes/crypto intrinsics (`hash`, `bytesConcat`, `textToBytes`, …) are
        // registered directly in `infer.rs`, not as prelude decls, so they
        // never reach `decl_effects`. They are pure; without this a call to one
        // inside an `atomic` block is treated as an opaque callee and trips the
        // "IO effects are not allowed inside atomic blocks" gate.
        insert_many(BYTES_BUILTINS, EffectSet::empty());

        // `race` carries a marker (not a user-declarable effect) so the
        // atomic gate catches it through helper functions — the syntactic
        // walk of the atomic body alone misses `raceIt = \a b -> race a b`.
        // `fork` is intentionally permitted inside atomic (see builtins.rs)
        // and `retry` is the STM primitive, so neither is marked.
        let mut race_effect = EffectSet::empty();
        race_effect.uses_race = true;
        builtin_effects.insert("race".into(), race_effect);
        // `fork` and `retry` carry no intrinsic effect (fork's effects flow
        // through its argument; retry is the STM primitive). Register them as
        // known/pure so the atomic-gate's opaque-callee scan does not mistake
        // `fork (helper {})` for a call through an unanalyzable callee.
        builtin_effects.insert("fork".into(), EffectSet::empty());
        builtin_effects.insert("retry".into(), EffectSet::empty());

        Self {
            decl_effects: HashMap::new(),
            decl_effects_atomic_safe: HashMap::new(),
            builtin_effects,
            source_names: HashSet::new(),
            scalar_source_names: HashSet::new(),
            view_names: HashSet::new(),
            row_poly_decls: HashSet::new(),
            fixed_row_decls: HashSet::new(),
            fixed_row_effects: HashMap::new(),
            local_fn_effects: Vec::new(),
            decl_bodies: HashMap::new(),
            shadowed: Vec::new(),
            current_decl_name: None,
            suppress_fork_io: false,
            fork_wrapper_params: HashMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn run(&mut self, module: &ast::Module) {
        // Collect source relation and view names, plus declaration bodies
        // for the atomic-gate's opaque-callee lambda scan.
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Source { name, ty } => {
                    self.source_names.insert(name.clone());
                    if !matches!(&ty.node, ast::TypeKind::Relation(_)) {
                        self.scalar_source_names.insert(name.clone());
                    }
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
            // Record `fork` wrappers so the atomic gate can strip the forwarded
            // argument's IO at the call site (see `fork_wrapper_params`).
            if let ast::DeclKind::Fun { name, body: Some(body), .. }
            | ast::DeclKind::View { name, body, .. }
            | ast::DeclKind::Derived { name, body, .. } = &decl.node
                && let Some(idx) = fork_wrapper_param(body) {
                    self.fork_wrapper_params.insert(name.clone(), idx);
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
                    } else if let Some(declared) = extract_effects(&scheme.ty) {
                        self.fixed_row_decls.insert(name.clone());
                        self.fixed_row_effects.insert(name.clone(), declared);
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
                if old.is_none_or(|o| *o != effects) {
                    self.decl_effects.insert(name.clone(), effects);
                    changed = true;
                }
            }
            self.diagnostics = saved_diags;
            if !changed { break; }
        }

        // Second fixpoint: recompute each declaration's effects under
        // fork-stripping into `decl_effects_atomic_safe`. The Var/DerivedRef
        // lookups consult this map (instead of `decl_effects`) while
        // `suppress_fork_io` is set, so a helper whose only IO is a `fork`ed
        // action contributes no IO to an enclosing `atomic` gate — matching the
        // syntactic `fork (…)` stripping the App/Pipe arms already do. Reads,
        // writes, and the race marker are preserved (fork stripping only clears
        // the spawned action's console/network/fs/clock/random). Diagnostics are
        // suppressed throughout (intermediate states may be incomplete and this
        // map is only consulted, never the source of user-visible errors).
        {
            let prev_suppress = self.suppress_fork_io;
            self.suppress_fork_io = true;
            let saved_diags = std::mem::take(&mut self.diagnostics);
            loop {
                let mut changed = false;
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
                    let old = self.decl_effects_atomic_safe.get(name);
                    if old.is_none_or(|o| *o != effects) {
                        self.decl_effects_atomic_safe.insert(name.clone(), effects);
                        changed = true;
                    }
                }
                if !changed { break; }
            }
            self.diagnostics = saved_diags;
            self.suppress_fork_io = prev_suppress;
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
            ast::DeclKind::Fun { name, body: Some(body), ty, .. } => {
                let effects = self.fun_body_effects(body);
                self.decl_effects.insert(name.clone(), effects.clone());
                self.check_annotation(ty, &effects);
            }
            _ => {}
        }
    }

    /// Push a set of lambda/case/comprehension binders as shadows AND as
    /// masking entries in `local_fn_effects` (mapped to no effects), then
    /// return the previous `shadowed` length for later truncation. Masking is
    /// essential: without it, an inner binder that shadows an outer let-bound
    /// *effectful* name (e.g. `let log = \_ -> println "x"` then `\log -> log`)
    /// would resolve the inner opaque reference to the outer binding's latent
    /// effects via the `local_fn_effects` scope lookup — an over-approximation
    /// that spuriously trips the atomic gate / annotation checks. A shadowing
    /// binder is opaque here; its real effects (if any) flow in at the call
    /// site through `head_call_effects`' scope map, not this lookup.
    fn push_masking_binders<'p>(
        &mut self,
        pats: impl IntoIterator<Item = &'p ast::Pat>,
    ) -> usize {
        let mark = self.shadowed.len();
        let mut names: Vec<String> = Vec::new();
        for p in pats {
            collect_pat_binders(p, &mut names);
        }
        let mut frame = HashMap::new();
        for n in &names {
            frame.insert(n.clone(), (EffectSet::empty(), true));
        }
        self.shadowed.extend(names);
        self.local_fn_effects.push(frame);
        mark
    }

    /// Undo a `push_masking_binders`: pop the masking frame and truncate the
    /// shadow stack back to `mark`.
    fn pop_masking_binders(&mut self, mark: usize) {
        self.local_fn_effects.pop();
        self.shadowed.truncate(mark);
    }

    /// Infer effects of evaluating an expression.
    fn infer_effects(&mut self, expr: &ast::Expr) -> EffectSet {
        match &expr.node {
            ast::ExprKind::Lit(_) | ast::ExprKind::Constructor(_) => EffectSet::empty(),

            ast::ExprKind::ImplicitRef(_) => EffectSet::empty(),

            ast::ExprKind::TypeCtor { .. } | ast::ExprKind::DataCtor { .. } | ast::ExprKind::SourceDecl { .. } | ast::ExprKind::SubsetConstraint { .. } | ast::ExprKind::RouteDecl { .. } | ast::ExprKind::RouteCompositeDecl { .. } => EffectSet::empty(),

            // An embedded view's body reads sources — infer its effects like
            // any other expression.
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => self.infer_effects(body),

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
                if !is_shadowed && crate::builtins::NULLARY_IO_BUILTINS.contains(&name.as_str())
                    && let Some(effects) = self.builtin_effects.get(name) {
                        return effects.clone();
                    }
                // A reference to a local let-bound lambda carries its
                // body's effects: the value may be invoked by whoever
                // receives it (e.g. `let cb = \r -> println "x"` passed
                // to a higher-order function). Mirrors callee_effects'
                // scope lookup — without it, effects laundered through a
                // local name bypassed the atomic gate.
                for scope in self.local_fn_effects.iter().rev() {
                    if let Some((effects, _opaque)) = scope.get(name) {
                        return effects.clone();
                    }
                }
                if !is_shadowed
                    && let Some(effects) = self.lookup_decl_effects(name) {
                        return effects.clone();
                    }
                EffectSet::empty()
            }

            ast::ExprKind::SourceRef(name) => {
                let mut e = EffectSet::empty();
                e.reads.insert(name.clone());
                e
            }

            ast::ExprKind::DerivedRef(name) => {
                self.lookup_decl_effects(name).cloned().unwrap_or_else(EffectSet::empty)
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
                // During the atomic-gate recomputation (`suppress_fork_io`),
                // never trust a *closed* declared row to "absorb" a callback's
                // effects: a `fixed_row` helper's own recorded effects are
                // computed with its callback parameter masked, so dropping the
                // lambda arg here would let synchronous IO inside the callback
                // launder past the atomic gate — e.g.
                // `atomic (runTwice (\_ -> println "hi") u)` where
                // `runTwice : (a -> IO {console} {}) -> a -> IO {console} {}`.
                // Propagate lambda-arg effects conservatively at the gate;
                // genuinely fork-deferred args are still stripped below.
                let propagate_lambda = self.suppress_fork_io
                    || (!is_lambda_head(head)
                        && head_name(head)
                            .map(|n| {
                                self.row_poly_decls.contains(n)
                                    || !self.fixed_row_decls.contains(n)
                            })
                            .unwrap_or(true));
                // `fork <action>` spawns its argument's IO on an independent
                // connection. When computing the atomic-gate view of effects,
                // strip the spawned action's IO so `atomic (fork (println …))`
                // is not falsely rejected — fork is intentionally allowed
                // inside atomic. Relation reads/writes and the `uses_race`
                // marker still propagate (a forked `race`, or relation work
                // that escapes the savepoint, must remain visible).
                let strip_fork_io = self.suppress_fork_io
                    && head_name(head) == Some("fork")
                    && !self.shadowed.iter().any(|s| s == "fork");
                // `forkIt (println …)` where `forkIt = \a -> fork a`: the
                // forwarded argument is spawned just like a syntactic `fork`,
                // so strip its IO from the atomic-gate view. The wrapper name
                // must resolve to the top-level decl (not a shadowing local).
                let fork_wrapper_idx = if self.suppress_fork_io {
                    head_name(head)
                        .filter(|n| !self.shadowed.iter().any(|s| s == n))
                        .and_then(|n| self.fork_wrapper_params.get(n).copied())
                } else {
                    None
                };
                let mut effects = EffectSet::empty();
                for (i, arg) in args.iter().enumerate() {
                    let mut arg_effects = if propagate_lambda {
                        self.arg_effects(arg)
                    } else {
                        self.infer_effects(arg)
                    };
                    if strip_fork_io || fork_wrapper_idx == Some(i) {
                        arg_effects.console = false;
                        arg_effects.network = false;
                        arg_effects.fs = false;
                        arg_effects.clock = false;
                        arg_effects.random = false;
                    }
                    effects = effects.union(&arg_effects);
                }
                let call_effects = self.head_call_effects(head, &args);
                effects = effects.union(&call_effects);
                // A closed-row callee absorbs its callback arguments
                // (`propagate_lambda == false`), so those effects were dropped
                // above. The callee's *declared* row is its promised effect set
                // — recover it here, since its *inferred* effects are computed
                // with the callback parameter masked and would under-report
                // (e.g. `runTwice : (a -> IO {console} {}) -> a -> IO {console}
                // {}` would otherwise contribute nothing). Skipped at the atomic
                // gate, where `propagate_lambda` is forced true.
                if !propagate_lambda
                    && let Some(name) = head_name(head)
                    && let Some(declared) = self.fixed_row_effects.get(name)
                {
                    effects = effects.union(declared);
                }
                // The declared row above covers the callee's IO effects but not
                // relation reads/writes performed by its callback arguments —
                // those are `IO {}` at the type level, so no closed declared row
                // can express them. Charge them explicitly, or a callback like
                // `\n -> *secrets` laundered through a closed-row callee reports
                // no reads (bug B17). No-op for `propagate_lambda`, where the
                // lambda's full effects already flowed in via `arg_effects`.
                if !propagate_lambda {
                    let db = self.lambda_arg_db_effects(&args);
                    effects = effects.union(&db);
                }
                effects
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
                    // See the `App` arm: at the atomic gate never trust a closed
                    // declared row to absorb a piped-in lambda's effects, or
                    // `(\u -> println "x") |> runTwiceClosed` would launder IO
                    // past the gate. Fork-piped args are still stripped below.
                    let propagate_lambda = self.suppress_fork_io
                        || head_name(rhs)
                            .map(|n| {
                                self.row_poly_decls.contains(n) || !self.fixed_row_decls.contains(n)
                            })
                            .unwrap_or(true);
                    let mut lhs_effects = if propagate_lambda {
                        self.arg_effects(lhs)
                    } else {
                        self.infer_effects(lhs)
                    };
                    // `lhs |> fork` ≡ `fork lhs`: strip the spawned action's IO
                    // for the atomic-gate view, exactly as the `App` spine does,
                    // so `(println "x") |> fork` is not falsely rejected inside
                    // `atomic`. Reads/writes/`uses_race` still propagate.
                    let strip_fork_io = self.suppress_fork_io
                        && head_name(rhs) == Some("fork")
                        && !self.shadowed.iter().any(|s| s == "fork");
                    // `action |> forkIt` ≡ `forkIt action`: `lhs` is arg 0, so
                    // strip it when `forkIt` is a fork wrapper forwarding its
                    // first parameter (mirrors the `App` spine's handling).
                    let strip_fork_wrapper = self.suppress_fork_io
                        && head_name(rhs)
                            .filter(|n| !self.shadowed.iter().any(|s| s == n))
                            .and_then(|n| self.fork_wrapper_params.get(n).copied())
                            == Some(0);
                    if strip_fork_io || strip_fork_wrapper {
                        lhs_effects.console = false;
                        lhs_effects.network = false;
                        lhs_effects.fs = false;
                        lhs_effects.clock = false;
                        lhs_effects.random = false;
                    }
                    let rhs_effects = self.callee_effects(rhs);
                    let mut result = lhs_effects.union(&rhs_effects);
                    // Mirror the `App` spine: a closed-row piped callee absorbs
                    // the piped-in lambda, so recover its promised effects from
                    // its declared row (its inferred effects mask the callback).
                    if !propagate_lambda
                        && let Some(name) = head_name(rhs)
                        && let Some(declared) = self.fixed_row_effects.get(name)
                    {
                        result = result.union(declared);
                    }
                    // Mirror the App spine: recover relation reads/writes the
                    // piped-in lambda performs, which the closed declared row
                    // cannot express (bug B17). `lhs` is the sole argument.
                    if !propagate_lambda {
                        let db = self.lambda_arg_db_effects(&[&**lhs]);
                        result = result.union(&db);
                    }
                    result
                } else {
                    let lhs_effects = self.infer_effects(lhs);
                    let rhs_effects = self.infer_effects(rhs);
                    lhs_effects.union(&rhs_effects)
                }
            }

            ast::ExprKind::UnaryOp { operand, .. } => self.infer_effects(operand),

            ast::ExprKind::With { record, body } => {
                let record_effects = self.infer_effects(record);
                // A record-*literal* `with` exposes each field's bound
                // sub-expression, so field binders can carry the latent
                // effects of their values into the body — exactly like the
                // old do-block `let` (which desugared to an immediately
                // applied lambda and registered each lambda-valued binding
                // in `local_fn_effects`). Without this, a with-bound lambda
                // called in the body (`with {f: \u -> *items} (f {})`) drops
                // the `r *items` read, and a with-bound callback passed by
                // name to a higher-order function loses its IO effects.
                if let ast::ExprKind::Record(fields) = &record.node {
                    let mut scope = HashMap::new();
                    for field in fields {
                        let latent = self.latent_effects_of(&field.value);
                        scope.insert(field.name.clone(), (latent, false));
                    }
                    let mark = self.shadowed.len();
                    for field in fields {
                        self.shadowed.push(field.name.clone());
                    }
                    self.local_fn_effects.push(scope);
                    let body_effects = self.infer_effects(body);
                    self.local_fn_effects.pop();
                    self.shadowed.truncate(mark);
                    record_effects.union(&body_effects)
                } else {
                    // Computed record — field values are opaque; effects of
                    // evaluating the record itself were already charged.
                    let body_effects = self.infer_effects(body);
                    record_effects.union(&body_effects)
                }
            }

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
                    // Skip it for a *scalar* source: `*counter = 5` reads
                    // nothing, and a read-modify-write like `*counter = *counter
                    // + 1` already has its read from infer_effects(value). (A
                    // relation `set` requires its value to reference the source,
                    // but that reference isn't always surfaced as a read here —
                    // e.g. `union xs [new]` with `xs <- *rel` — so keep the
                    // explicit read for relations and views.)
                    if !is_replace && !self.scalar_source_names.contains(name) {
                        effects.reads.insert(name.clone());
                    }
                    if self.view_names.contains(name) {
                        // Writing through a view writes the backing
                        // source(s). The view's inferred effects record
                        // which sources its body reads — those are
                        // exactly the relations a write lands in (plus
                        // any other effects evaluating the view incurs).
                        let view_effects = self
                            .lookup_decl_effects(name)
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
                // The IO gate uses a fork-stripped view of the body's effects:
                // `fork`'s spawned IO runs on an independent connection and is
                // intentionally permitted inside atomic, so it must not trip
                // this check (the full `inner_effects` — with fork's IO intact
                // — is what propagates to the enclosing declaration).
                let prev_suppress = self.suppress_fork_io;
                self.suppress_fork_io = true;
                // This is a second traversal of `inner`, purely to recompute the
                // has_io() boolean under fork-stripping. The diagnostics it
                // produces duplicate those already emitted by the first
                // `infer_effects(inner)` traversal above (nested atomics manage
                // their own gate independently), so discard anything appended
                // here to avoid double-reporting nested violations.
                let diags_before_gate = self.diagnostics.len();
                let gate_effects = self.infer_effects(inner);
                self.diagnostics.truncate(diags_before_gate);
                self.suppress_fork_io = prev_suppress;
                if gate_effects.has_io() {
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
                // under that binder are local values, not the primitive. Seed
                // the walk with the ENCLOSING binders (`self.shadowed`) too, so
                // e.g. `\race -> atomic (... race ...)` treats `race` as the
                // lambda param, not the primitive.
                let mut disallowed: Vec<(String, Span)> = Vec::new();
                let mut shadowed: Vec<String> = self.shadowed.clone();
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
                if !gate_effects.has_io() && self.body_may_call_opaque(inner) {
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
                if !gate_effects.has_io()
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
                    let stmt_effects = self.infer_stmt_effects(stmt);
                    effects = effects.union(&stmt_effects);
                    // Binders come into scope for *later* statements.
                    if let ast::StmtKind::Bind { pat, .. } = &stmt.node
                    {
                        collect_pat_binders(pat, &mut self.shadowed);
                    }
                    // A `<-` bind rebinds its name to (opaque) relation rows, not
                    // an effectful action. Mask any same/outer-scope effect entry
                    // for that name so a later reference doesn't launder those
                    // effects.
                    if let ast::StmtKind::Bind { pat, .. } = &stmt.node {
                        let mut bind_names: Vec<String> = Vec::new();
                        collect_pat_binders(pat, &mut bind_names);
                        let frame = self.local_fn_effects.last_mut().unwrap();
                        for n in bind_names {
                            frame.insert(n, (EffectSet::empty(), true));
                        }
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
                    let mark = self.push_masking_binders(std::iter::once(&arm.pat));
                    let arm_effects = self.infer_effects(&arm.body);
                    self.pop_masking_binders(mark);
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

            ast::ExprKind::TimeUnitLit { value, .. } => self.infer_effects(value),
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
        self.body_may_call_opaque_rec(expr, &mut visited, &HashSet::new())
    }

    /// Recursive core of `body_may_call_opaque`. Follows calls into known user
    /// functions (`decl_bodies`) so an opaque call hidden one level down — a
    /// helper that invokes a lambda handed to it through a record field — is
    /// still detected. `visited` guards against cycles in mutual recursion.
    ///
    /// `known_params` are the formal parameters of the callee whose body we're
    /// currently analyzing. A higher-order function applying its OWN parameter
    /// (`findFirst = \items pred -> … pred x …`, `forEach = \items action -> …
    /// action x …`) is not an opaque call in the dangerous sense: the actual
    /// argument is supplied at the call site, where it is either a lambda
    /// literal (walked directly, and whose effects flow through type inference)
    /// or an opaque value (already flagged there). Treating a callee's own
    /// params as known therefore avoids false positives for the common prelude
    /// HOFs without weakening the record-field / computed-callee laundering
    /// checks, which key on field-access (non-Var) callees regardless.
    fn body_may_call_opaque_rec(
        &self,
        expr: &ast::Expr,
        visited: &mut HashSet<String>,
        known_params: &HashSet<String>,
    ) -> bool {
        // Locally-bound lambdas inside the body (with-bound record fields,
        // immediately-applied-lambda parameters) are analyzable: their bodies
        // are part of the walked tree, so `touches_relations` and effect
        // inference both see through them. (The local_fn_effects scopes
        // those bindings lived in are already popped by the time the
        // Atomic arm runs this check, so collect them syntactically.)
        let mut local_lambdas: HashSet<String> = HashSet::new();
        walk_expr(expr, &mut |e| {
            match &e.node {
                // A record-literal `with` binds each field name to the field's
                // value for the body — a lambda-valued field is analyzable
                // exactly like an old `let`-bound lambda, so calls through it
                // are NOT opaque.
                ast::ExprKind::With { record, .. } => {
                    if let ast::ExprKind::Record(fields) = &record.node {
                        for field in fields {
                            if is_lambda_arg(&field.value) {
                                local_lambdas.insert(field.name.clone());
                            }
                        }
                    }
                }
                ast::ExprKind::App { .. } => {
                    let (head, args) = app_spine(e);
                    if let ast::ExprKind::Lambda { params, .. } = &head.node {
                        for (param, arg) in params.iter().zip(args.iter()) {
                            if let ast::PatKind::Var(name) = &param.node
                                && is_lambda_arg(arg) {
                                    local_lambdas.insert(name.clone());
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
            while let ast::ExprKind::Annot { expr: inner, .. }
            | ast::ExprKind::TimeUnitLit { value: inner, .. }
            | ast::ExprKind::Refine(inner) = &head.node
            {
                head = inner;
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
                        || known_params.contains(name)
                        || crate::builtins::INTERNAL_BUILTINS
                            .contains(&name.as_str())
                        || crate::builtins::TRAIT_METHOD_BUILTINS
                            .contains(&name.as_str())
                        || self
                            .local_fn_effects
                            .iter()
                            .any(|scope| {
                                // Only a REAL analyzable local binding counts as
                                // "known"; an opaque shadowing mask does not — a
                                // call through it is still an opaque call.
                                scope.get(name).is_some_and(|(_, opaque)| !opaque)
                            });
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
        // call may live one (or more) levels down the call chain. The callee's
        // own formal parameters are passed as `known_params` so a HOF applying
        // its callback parameter isn't mistaken for an opaque call (see the
        // doc comment).
        for name in callees_to_recurse {
            if visited.insert(name.clone())
                && let Some(body) = self.decl_bodies.get(&name) {
                    let params = lambda_param_names(body);
                    if self.body_may_call_opaque_rec(body, visited, &params) {
                        return true;
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
        // The flag marks an expression that is itself in *executed* position:
        // true for the roots (the atomic body and the enclosing declaration's
        // body — both are run), false for a declaration body pulled in through
        // a reference, which is reached as a *value* and so is a candidate for
        // being the thing the opaque call invokes.
        let mut worklist: Vec<(&ast::Expr, bool)> = roots.iter().map(|e| (*e, true)).collect();
        let mut found: Option<Span> = None;
        while let Some((e, executed)) = worklist.pop() {
            if found.is_some() {
                break;
            }
            let mut called: Vec<Span> = Vec::new();
            executed_builtin_calls(e, executed, &mut called);
            let mut referenced: Vec<String> = Vec::new();
            walk_expr(e, &mut |node| {
                match &node.node {
                    ast::ExprKind::Lambda { body, .. }
                        if found.is_none() && contains_atomic_disallowed_ref(body) => {
                            found = Some(node.span);
                        }
                    // A bare reference to an atomic-disallowed IO builtin
                    // (`println`, `logInfo`, `writeFile`, …) reaches IO directly
                    // — it need not be wrapped in a lambda. `r = {fn: println}`
                    // then `r.fn "x"` inside atomic launders the builtin through
                    // a record field; the opaque call ends up invoking it, so
                    // reject conservatively, mirroring the Lambda arm above and
                    // the shadow-unaware convention of `contains_atomic_disallowed_ref`.
                    // A builtin *called* in statement position (`called`) is not
                    // such a value — it performs its IO where it stands, outside
                    // the atomic block (an IO call *inside* the block is already
                    // rejected by the `gate_effects.has_io()` check that guards
                    // this scan), so flagging it rejects valid programs.
                    ast::ExprKind::Var(name)
                        if found.is_none()
                            && crate::builtins::ATOMIC_DISALLOWED_BUILTINS
                                .contains(&name.as_str())
                            && !called.contains(&node.span) =>
                    {
                        found = Some(node.span);
                    }
                    ast::ExprKind::Var(name) => referenced.push(name.clone()),
                    _ => {}
                }
            });
            for name in referenced {
                if seen.insert(name.clone())
                    && let Some(body) = self.decl_bodies.get(&name) {
                        worklist.push((body, false));
                    }
            }
        }
        found
    }

    /// Latent effects of a value bound by a `let` — the effects a later
    /// *execution* of the bound name recovers, while the `let` itself charges
    /// nothing (binding an IO action ≠ running it). A bound lambda contributes
    /// its body's effects when later called (`let f = \u -> *items` → `r
    /// *items`); a point-free alias of a non-nullary IO builtin (`let f =
    /// readFile`) contributes the builtin's effects so applying `f` later does
    /// not launder IO past the atomic gate; anything else contributes the
    /// value expression's own effects.
    /// Infer effects of a do-block statement.
    fn infer_stmt_effects(&mut self, stmt: &ast::Stmt) -> EffectSet {
        match &stmt.node {
            ast::StmtKind::Bind { expr, .. } => self.infer_effects(expr),
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
        } else if let Some(latent) = self.callback_arg_latent_effects(arg) {
            // A *bare* (point-free) reference to an effectful callable passed as
            // a higher-order callback — an IO builtin (`forEach xs removeFile`)
            // or a user decl (`forEach xs deleteThing`). The `infer_effects`
            // `Var` arm treats such an unapplied reference as pure, but the
            // callee will invoke it, so its latent effects are performed here.
            // Recover them so the IO-in-atomic gate is not bypassed, mirroring
            // the lambda-arg and `let`-binding paths (`fun_body_effects` /
            // `builtin_alias_effects`).
            effects = effects.union(&latent);
        }
        effects
    }

    /// Latent effects of a bare point-free callable passed as a HOF callback.
    /// Unwraps the same wrappers as `is_lambda_arg`, then resolves either an IO
    /// builtin alias (`removeFile`, `println`, …) or a user declaration's
    /// converged effects (using the atomic-gate-safe view when applicable).
    /// Returns `None` for anything that isn't a bare reference to a callable.
    fn callback_arg_latent_effects(&self, arg: &ast::Expr) -> Option<EffectSet> {
        match &arg.node {
            ast::ExprKind::TimeUnitLit { value, .. }
            | ast::ExprKind::Annot { expr: value, .. }
            | ast::ExprKind::Refine(value) => self.callback_arg_latent_effects(value),
            ast::ExprKind::Var(name) => {
                if self.shadowed.iter().any(|s| s == name) {
                    return None;
                }
                if let Some(e) = self.builtin_alias_effects(arg) {
                    return Some(e);
                }
                self.lookup_decl_effects(name).cloned()
            }
            _ => None,
        }
    }

    /// Resolve effects when a function expression is *called*.
    /// Look up a declaration's converged effects, choosing the fork-stripped
    /// view while `suppress_fork_io` is set (the atomic-gate recomputation) so
    /// IO laundered through a `fork`ing helper does not trip the gate. Both maps
    /// share the same key set (every Fun/View/Derived/trait-method is inserted
    /// into both fixpoints), so this never silently falls back across views.
    fn lookup_decl_effects(&self, name: &str) -> Option<&EffectSet> {
        if self.suppress_fork_io {
            self.decl_effects_atomic_safe.get(name)
        } else {
            self.decl_effects.get(name)
        }
    }

    fn callee_effects(&mut self, func_expr: &ast::Expr) -> EffectSet {
        match &func_expr.node {
            ast::ExprKind::Var(name) => {
                // Check builtins first, then local let-bound lambdas
                // (innermost scope first), then user declarations. A
                // locally shadowed name never resolves to the builtin or
                // the top-level declaration.
                let is_shadowed = self.shadowed.iter().any(|s| s == name);
                if !is_shadowed
                    && let Some(effects) = self.builtin_effects.get(name) {
                        return effects.clone();
                    }
                for scope in self.local_fn_effects.iter().rev() {
                    if let Some((effects, _opaque)) = scope.get(name) {
                        return effects.clone();
                    }
                }
                if !is_shadowed
                    && let Some(effects) = self.lookup_decl_effects(name) {
                        return effects.clone();
                    }
                // Unknown callee — treat as pure (conservative, no false positives)
                EffectSet::empty()
            }

            ast::ExprKind::Lambda { params, body, .. } => {
                // Immediately-applied lambda: effects are the body's effects
                let mark = self.push_masking_binders(params.iter());
                let effects = self.infer_effects(body);
                self.pop_masking_binders(mark);
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
                    let mark = self.push_masking_binders(std::iter::once(&arm.pat));
                    let arm_effects = self.callee_effects(&arm.body);
                    self.pop_masking_binders(mark);
                    effects = effects.union(&arm_effects);
                }
                effects
            }

            // Wrapper expressions: unwrap to find the actual callee.
            // Without this, an annotated lambda like `(\x -> println x : Type) y`
            // would fall through to infer_effects, which treats Lambda as pure
            // (creating a lambda IS pure), missing the body's call effects.
            ast::ExprKind::TimeUnitLit { value, .. } => self.callee_effects(value),
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
            ast::ExprKind::Lambda { params, body, .. } => {
                let mut scope = HashMap::new();
                for (param, arg) in params.iter().zip(args.iter()) {
                    if let ast::PatKind::Var(name) = &param.node
                        && is_lambda_arg(arg) {
                            let fn_effects = self.fun_body_effects(arg);
                            scope.insert(name.clone(), (fn_effects, false));
                        }
                }
                let mark = self.shadowed.len();
                let mut binders: Vec<String> = Vec::new();
                for p in params {
                    collect_pat_binders(p, &mut binders);
                }
                // Any binder not given a concrete callback effect above is an
                // opaque local; mask it (→ no effects) so a reference inside
                // the body isn't laundered to an outer let-bound name of the
                // same identifier via the scope lookup.
                for n in &binders {
                    scope.entry(n.clone()).or_insert_with(|| (EffectSet::empty(), true));
                }
                self.shadowed.extend(binders);
                self.local_fn_effects.push(scope);
                let effects = self.infer_effects(body);
                self.local_fn_effects.pop();
                self.shadowed.truncate(mark);
                effects
            }
            // Wrappers: unwrap to find the lambda (if any) inside.
            ast::ExprKind::TimeUnitLit { value, .. } => self.head_call_effects(value, args),
            ast::ExprKind::Annot { expr, .. } => self.head_call_effects(expr, args),
            ast::ExprKind::Refine(inner) => self.head_call_effects(inner, args),
            _ => self.callee_effects(head),
        }
    }

    /// Unwrap lambda chain to get the effects of the function body.
    /// `\x y -> set *foo = ...` → effects of the `set` expression.
    fn fun_body_effects(&mut self, body: &ast::Expr) -> EffectSet {
        match &body.node {
            ast::ExprKind::Lambda { body: inner, params, .. } => {
                let mark = self.push_masking_binders(params.iter());
                let effects = self.fun_body_effects(inner);
                self.pop_masking_binders(mark);
                effects
            }
            // Wrapper expressions: unwrap to find the lambda chain inside.
            ast::ExprKind::TimeUnitLit { value, .. } => self.fun_body_effects(value),
            ast::ExprKind::Annot { expr, .. } => self.fun_body_effects(expr),
            ast::ExprKind::Refine(inner) => self.fun_body_effects(inner),
            _ => self
                .builtin_alias_effects(body)
                .unwrap_or_else(|| self.infer_effects(body)),
        }
    }

    /// Latent effects of a value bound by a `with` record field — the effects
    /// a later *execution* of the bound name recovers, while the `with` itself
    /// charges nothing (binding an IO action ≠ running it). A bound lambda
    /// contributes its body's effects when later called (`with {f: \u ->
    /// *items} (f {})` reads `items`); a point-free alias of a non-nullary IO
    /// builtin contributes the builtin's effects so applying the field later
    /// does not launder IO past the atomic gate; anything else contributes
    /// the value expression's own effects. Mirrors the old do-block `let`
    /// latent-effect registration.
    fn latent_effects_of(&mut self, expr: &ast::Expr) -> EffectSet {
        if is_lambda_arg(expr) {
            self.fun_body_effects(expr)
        } else if let Some(alias) = self.builtin_alias_effects(expr) {
            alias
        } else {
            self.infer_effects(expr)
        }
    }

    /// Relation reads/writes performed by a closed-row (`fixed_row`) callee's
    /// *lambda* arguments. When `propagate_lambda` is false the callee "absorbs"
    /// its callbacks and we recover its *declared* row instead of the callbacks'
    /// effects — correct for the five IO effects (console/fs/clock/network/
    /// random), which are tracked in the type-level effect row and so are
    /// genuinely covered by the declared set. But relation reads/writes are
    /// invisible at the type level (`*rel` reads as `IO {}`), so no closed
    /// declared row can ever express them — a callback like `\n -> *secrets`
    /// laundered through such a callee would otherwise report no reads at all
    /// (bug B17). Recover only those db effects here; IO effects stay absorbed
    /// (see `non_row_poly_callee_does_not_propagate_lambda_effects`).
    fn lambda_arg_db_effects(&mut self, args: &[&ast::Expr]) -> EffectSet {
        let mut db = EffectSet::empty();
        for arg in args {
            if is_lambda_arg(arg) {
                let body = self.fun_body_effects(arg);
                db.reads.extend(body.reads);
                db.writes.extend(body.writes);
            }
        }
        db
    }

    /// If `expr` is a point-free reference to a *non-nullary* IO builtin — an
    /// alias like `readIt = readFile` or a local `let f = println` — return
    /// that builtin's call effects.
    ///
    /// The `infer_effects` `Var` arm deliberately treats such a bare reference
    /// as pure (an unapplied reference must not trip the atomic gate — see the
    /// comment there). But when the reference is the *value of a binding*, the
    /// binding is a callable that performs the builtin's effects when applied,
    /// so its latent/decl effects must recover them. Without this, aliasing an
    /// IO builtin and then applying the alias inside `atomic` launders the IO
    /// past the atomic gate. (Nullary IO builtins already surface their effects
    /// from a bare reference, so they need no special handling here.)
    fn builtin_alias_effects(&self, expr: &ast::Expr) -> Option<EffectSet> {
        if let ast::ExprKind::Var(name) = &expr.node
            && !self.shadowed.iter().any(|s| s == name)
            && !crate::builtins::NULLARY_IO_BUILTINS.contains(&name.as_str())
        {
            return self.builtin_effects.get(name).cloned();
        }
        None
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
            // A closed-row higher-order function legitimately declares effects
            // that its *body* never performs directly — they are contributed by
            // invoking an effectful callback parameter (whose effects are masked
            // out of `inferred`). Don't flag those as "unused": subtract effects
            // that also appear in a parameter (callback) position of the
            // signature. Effects declared but present in neither the body nor any
            // callback param are still reported.
            let param_effects = param_declared_effects(&scheme.ty);
            let unused = declared.difference(inferred).difference(&param_effects);
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
        ast::ExprKind::TimeUnitLit { value, .. }
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
        | ast::ExprKind::ImplicitRef(_)
        | ast::ExprKind::DerivedRef(_) => {}
        ast::ExprKind::TypeCtor { .. } | ast::ExprKind::DataCtor { .. } | ast::ExprKind::SourceDecl { .. } | ast::ExprKind::SubsetConstraint { .. } | ast::ExprKind::RouteDecl { .. } | ast::ExprKind::RouteCompositeDecl { .. } => {}
        ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => walk_expr(body, f),
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
        ast::ExprKind::With { record, body } => {
            walk_expr(record, f);
            walk_expr(body, f);
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
                    ast::StmtKind::Bind { expr, .. } => walk_expr(expr, f),
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
        ast::ExprKind::TimeUnitLit { value, .. } => walk_expr(value, f),
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
        ast::PatKind::Annot { pat, .. } => collect_pat_binders(pat, out),
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
        | ast::ExprKind::ImplicitRef(_)
        | ast::ExprKind::DerivedRef(_) => {}
        ast::ExprKind::TypeCtor { .. } | ast::ExprKind::DataCtor { .. } | ast::ExprKind::SourceDecl { .. } | ast::ExprKind::SubsetConstraint { .. } | ast::ExprKind::RouteDecl { .. } | ast::ExprKind::RouteCompositeDecl { .. } => {}
        ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
            collect_unshadowed_disallowed(body, shadowed, out)
        }
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
        ast::ExprKind::Lambda { params, body, .. } => {
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
        ast::ExprKind::With { record, body } => {
            collect_unshadowed_disallowed(record, shadowed, out);
            collect_unshadowed_disallowed(body, shadowed, out);
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
        ast::ExprKind::TimeUnitLit { value, .. } => collect_unshadowed_disallowed(value, shadowed, out),
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

/// Formal parameter names of a function body, peeling the curried lambda
/// chain (`\a b -> …` desugars to nested single-param lambdas, but the parser
/// keeps multi-param `Lambda` nodes — handle both). Used by the atomic
/// opaque-callee scan so a callee that applies its own parameter isn't
/// mistaken for a call through an opaque value.
fn lambda_param_names(expr: &ast::Expr) -> HashSet<String> {
    let mut names = HashSet::new();
    let mut cur = expr;
    while let ast::ExprKind::Lambda { params, body, .. } = &cur.node {
        for p in params {
            if let ast::PatKind::Var(name) = &p.node {
                names.insert(name.clone());
            }
        }
        cur = body;
    }
    names
}

/// Whether the expression syntactically references any builtin from
/// `ATOMIC_DISALLOWED_BUILTINS` (console/network/fs/clock/random IO plus
/// `race`; `fork` and `retry` are intentionally permitted in atomic).
/// Purely syntactic and shadow-unaware — used only for the conservative
/// opaque-callee scan inside atomic bodies, where false positives are
/// acceptable (atomic bodies are supposed to be DB-only).
/// Spans of the atomic-disallowed IO builtins that `expr` *calls* in statement
/// position — a do-block statement, or (when `executed` is set) the expression
/// itself. Such a reference performs its IO where it stands and cannot flow
/// anywhere else, so `reachable_io_lambda_from` must not mistake it for a
/// callable laundered into an opaque call: `main = do { …; atomic (r.fn {});
/// println (show c) }` is a valid program.
///
/// Everything else is left out, so a builtin applied in a *value* position —
/// `r = {fn: writeFile "log"}`, later invoked as `r.fn "x"` inside atomic — is
/// still flagged. `executed` is false for a declaration body reached through a
/// reference, since that body is being inspected as a value.
fn executed_builtin_calls(expr: &ast::Expr, executed: bool, out: &mut Vec<Span>) {
    match &expr.node {
        // Statements run wherever the block sits, so this arm ignores `executed`.
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                if let ast::StmtKind::Expr(e) | ast::StmtKind::Bind { expr: e, .. } = &stmt.node {
                    executed_builtin_calls(e, true, out);
                }
            }
        }
        ast::ExprKind::Atomic(inner) => executed_builtin_calls(inner, executed, out),
        ast::ExprKind::If { then_branch, else_branch, .. } if executed => {
            executed_builtin_calls(then_branch, true, out);
            executed_builtin_calls(else_branch, true, out);
        }
        ast::ExprKind::Case { arms, .. } if executed => {
            for arm in arms {
                executed_builtin_calls(&arm.body, true, out);
            }
        }
        ast::ExprKind::App { .. } if executed => {
            let (head, _) = app_spine(expr);
            if let ast::ExprKind::Var(name) = &head.node
                && crate::builtins::ATOMIC_DISALLOWED_BUILTINS.contains(&name.as_str())
            {
                out.push(head.span);
            }
        }
        _ => {}
    }
}

fn contains_atomic_disallowed_ref(expr: &ast::Expr) -> bool {
    // Argument subtrees of `fork (<action>)` are exempt: forked IO runs on an
    // independent connection and is intentionally permitted inside `atomic`, so
    // a disallowed builtin reached only through a `fork` must not count. walk_expr
    // is pre-order, so the enclosing `fork` App is visited (and its arg spans
    // recorded) before any disallowed Var nested inside it.
    let mut pruned: Vec<Span> = Vec::new();
    let mut found = false;
    walk_expr(expr, &mut |e| {
        if let ast::ExprKind::App { .. } = &e.node {
            let (head, args) = app_spine(e);
            if head_name(head) == Some("fork") {
                for a in args {
                    pruned.push(a.span);
                }
            }
        }
        if let ast::ExprKind::Var(name) = &e.node
            && crate::builtins::ATOMIC_DISALLOWED_BUILTINS.contains(&name.as_str())
                && !pruned
                    .iter()
                    .any(|p| p.start <= e.span.start && e.span.end <= p.end)
            {
                found = true;
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

/// Whether an application-spine head is an immediately-applied lambda,
/// looking through the same wrappers `head_call_effects` unwraps. Such heads
/// are analyzed precisely by `head_call_effects` (which binds the lambda-valued
/// arguments into a local scope and only counts an argument's effects when the
/// body actually invokes the parameter), so the caller must NOT independently
/// propagate lambda-arg body effects — doing so double-counts the effects of an
/// *unused* lambda argument, spuriously rejecting a well-typed program.
fn is_lambda_head(expr: &ast::Expr) -> bool {
    match &expr.node {
        ast::ExprKind::Lambda { .. } => true,
        ast::ExprKind::TimeUnitLit { value, .. }
        | ast::ExprKind::Annot { expr: value, .. } => {
            is_lambda_head(value)
        }
        ast::ExprKind::Refine(inner) => is_lambda_head(inner),
        _ => false,
    }
}

/// Resolve the head name of a (possibly curried) function expression.
/// `f x y z` → `Some("f")`. Returns `None` for non-named callees.
fn head_name(expr: &ast::Expr) -> Option<&str> {
    match &expr.node {
        ast::ExprKind::Var(name) => Some(name.as_str()),
        ast::ExprKind::App { func, .. } => head_name(func),
        ast::ExprKind::TimeUnitLit { value, .. }
        | ast::ExprKind::Annot { expr: value, .. } => {
            head_name(value)
        }
        // Match the wrapper set unwrapped by is_lambda_arg/fun_body_effects.
        ast::ExprKind::Refine(inner) => head_name(inner),
        _ => None,
    }
}

/// If `body` is a `fork` wrapper — a lambda chain whose body is exactly
/// `fork <param>` (or `<param> |> fork`) — return the (curry-order) index of
/// the forwarded parameter. The argument passed there is spawned on an
/// independent connection, so its IO never runs in the caller and may be
/// stripped from the atomic-gate view, exactly like a syntactic `fork (…)`.
///
/// Deliberately minimal: only this single-direct-forward shape is recognized.
/// Any richer body keeps the parameter's effects (the safe over-approximating
/// direction), so this can only ever remove IO that `fork` already defers.
fn fork_wrapper_param(body: &ast::Expr) -> Option<usize> {
    let mut params: Vec<&str> = Vec::new();
    let inner = unwrap_lambda_params(body, &mut params);
    // If a parameter shadows `fork`, the `fork` in the body is not the builtin.
    if params.contains(&"fork") {
        return None;
    }
    let forked = fork_call_arg_var(inner)?;
    params.iter().position(|p| *p == forked)
}

/// Unwrap a (possibly curried) lambda chain, collecting parameter names in
/// order, and return the innermost body. Non-`Var` parameters push `""` (which
/// never matches a forwarded variable name), keeping index alignment intact.
fn unwrap_lambda_params<'a>(
    expr: &'a ast::Expr,
    params: &mut Vec<&'a str>,
) -> &'a ast::Expr {
    match &expr.node {
        ast::ExprKind::Lambda { params: ps, body, .. } => {
            for p in ps {
                match &p.node {
                    ast::PatKind::Var(n) => params.push(n.as_str()),
                    _ => params.push(""),
                }
            }
            unwrap_lambda_params(body, params)
        }
        ast::ExprKind::Annot { expr: inner, .. }
        | ast::ExprKind::TimeUnitLit { value: inner, .. } => unwrap_lambda_params(inner, params),
        ast::ExprKind::Refine(inner) => unwrap_lambda_params(inner, params),
        _ => expr,
    }
}

/// If `expr` is exactly `fork <Var>` or `<Var> |> fork` (through the usual
/// wrapper nodes), return the variable's name.
fn fork_call_arg_var(expr: &ast::Expr) -> Option<&str> {
    match &expr.node {
        ast::ExprKind::App { .. } => {
            let (head, args) = app_spine(expr);
            if head_name(head) == Some("fork") && args.len() == 1
                && let ast::ExprKind::Var(v) = &args[0].node {
                    return Some(v.as_str());
                }
            None
        }
        ast::ExprKind::BinOp {
            op: ast::BinOp::Pipe,
            lhs,
            rhs,
        } => {
            if head_name(rhs) == Some("fork")
                && let ast::ExprKind::Var(v) = &lhs.node {
                    return Some(v.as_str());
                }
            None
        }
        ast::ExprKind::Annot { expr: inner, .. }
        | ast::ExprKind::TimeUnitLit { value: inner, .. } => fork_call_arg_var(inner),
        ast::ExprKind::Refine(inner) => fork_call_arg_var(inner),
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
        ast::TypeKind::Forall { ty: inner, .. } => type_has_effect_row_var(inner),
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
        ast::TypeKind::Forall { ty: inner, .. } => effects_span(inner),
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
/// Collect the effects declared in *parameter* (callback) positions of a
/// function signature — the mirror of `extract_effects`, which looks only at
/// the result side. Used to recognize that a higher-order function's declared
/// result effects may be contributed by invoking an effectful callback argument
/// rather than by its own body, so the "declared but unused" warning does not
/// fire on legitimate closed-row HOFs.
fn param_declared_effects(ty: &ast::Type) -> EffectSet {
    match &ty.node {
        ast::TypeKind::Function { param, result } => {
            // The parameter's own effects (a callback like `a -> IO {console} {}`
            // contributes `{console}`), plus any effects nested deeper in the
            // parameter, plus the parameter positions of the remaining (curried)
            // arrows. The final result's effects are intentionally NOT collected
            // here — those are the function's own, handled by `extract_effects`.
            let mut e = extract_effects(param).unwrap_or_else(EffectSet::empty);
            e = e.union(&param_declared_effects(param));
            e.union(&param_declared_effects(result))
        }
        ast::TypeKind::Forall { ty: inner, .. } => param_declared_effects(inner),
        _ => EffectSet::empty(),
    }
}

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
        ast::TypeKind::Forall { ty: inner, .. } => extract_effects(inner),
        _ => None,
    }
}

// ── Public entry point ───────────────────────────────────────────

/// Per-declaration effect information: maps declaration names to their inferred effects.
pub type EffectInfo = HashMap<String, EffectSet>;

/// Runs on a grown stack — the effect walker recurses through the `__bind`
/// chain a desugared `do` block expands into.
pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    check_with_effects(module).0
}

/// Like `check` but also returns per-declaration effect information.
pub fn check_with_effects(module: &ast::Module) -> (Vec<Diagnostic>, EffectInfo) {
    crate::stack::grow(|| {
        let mut checker = EffectChecker::new();
        checker.run(module);
        (checker.diagnostics, checker.decl_effects)
    })
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
            ty_params: vec![],
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
            ty_params: vec![],
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
            ty_params: vec![],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Var("x".into()))),
            })),
        });
        // Just referencing a lambda (not calling it) — pure at the expression level
        let wrapper = spanned(ExprKind::Record(vec![RecordField {
            name: "f".into(),
            value: body,
            sig: None,
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
    fn atomic_fork_io_allowed() {
        // atomic (do { _ <- *people; fork (println "spawned"); set *people = [] })
        // `fork`'s spawned IO runs on its own connection and is intentionally
        // permitted inside atomic, so the IO gate must NOT reject this — even
        // though the console effect still propagates to the decl's effects.
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("fork".into()))),
                arg: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::Lit(Literal::Text(
                        "spawned".into(),
                    )))),
                })),
            }))),
            spanned(StmtKind::Expr(spanned(ExprKind::Set {
                target: Box::new(spanned(ExprKind::SourceRef("people".into()))),
                value: Box::new(spanned(ExprKind::List(vec![]))),
            }))),
        ])))));
        let (diags, effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        assert!(
            diags.is_empty(),
            "fork-spawned IO inside atomic should not be rejected: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
        // The spawned console effect still propagates to the decl's effects.
        assert!(effects["f"].console, "fork's console effect should propagate");
        assert!(effects["f"].writes.contains("people"));
    }

    #[test]
    fn atomic_fork_plus_direct_io_error() {
        // atomic (do { _ <- *people; fork (println "ok"); println "direct" })
        // The direct (non-forked) println IO is still rejected.
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("fork".into()))),
                arg: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::Lit(Literal::Text("ok".into())))),
                })),
            }))),
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Text("direct".into())))),
            }))),
        ])))));
        let (diags, _effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        assert!(
            diags.iter().any(|d| d.message.contains("IO effects are not allowed inside atomic")),
            "direct IO alongside fork should still be rejected: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn atomic_fork_through_wrapper_allowed() {
        // forkIt = \a -> fork a
        // f = atomic (do { _ <- *people; forkIt (println "spawned"); set *people = [] })
        // The forwarded argument is spawned exactly like a syntactic `fork`, so
        // the IO gate must not reject it. Regression: only a literal `fork`
        // head was stripped, so fork-through-a-wrapper was falsely rejected.
        let fork_it = make_fun(
            "forkIt",
            spanned(ExprKind::Lambda {
                params: vec![spanned(PatKind::Var("a".into()))],
                ty_params: vec![],
                body: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("fork".into()))),
                    arg: Box::new(spanned(ExprKind::Var("a".into()))),
                })),
            }),
        );
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Expr(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("forkIt".into()))),
                arg: Box::new(spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("println".into()))),
                    arg: Box::new(spanned(ExprKind::Lit(Literal::Text(
                        "spawned".into(),
                    )))),
                })),
            }))),
            spanned(StmtKind::Expr(spanned(ExprKind::Set {
                target: Box::new(spanned(ExprKind::SourceRef("people".into()))),
                value: Box::new(spanned(ExprKind::List(vec![]))),
            }))),
        ])))));
        let (diags, effects) =
            check_module(vec![make_source("people"), fork_it, make_fun("f", body)]);
        assert!(
            diags.is_empty(),
            "fork-through-wrapper IO inside atomic should not be rejected: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
        assert!(effects["f"].writes.contains("people"));
    }

    #[test]
    fn atomic_non_fork_wrapper_io_still_rejected() {
        // sink = \a -> a  (returns its argument unchanged — NOT a fork wrapper)
        // f = atomic (do { _ <- *people; _ <- sink (println "x") })
        // `_ <- sink (…)` runs the IO in the transaction, so the console effect
        // must still trip the gate. Guards against the fork-wrapper stripping
        // over-reaching and laundering real IO.
        let sink = make_fun(
            "sink",
            spanned(ExprKind::Lambda {
                params: vec![spanned(PatKind::Var("a".into()))],
                ty_params: vec![],
                body: Box::new(spanned(ExprKind::Var("a".into()))),
            }),
        );
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::App {
                    func: Box::new(spanned(ExprKind::Var("sink".into()))),
                    arg: Box::new(spanned(ExprKind::App {
                        func: Box::new(spanned(ExprKind::Var("println".into()))),
                        arg: Box::new(spanned(ExprKind::Lit(Literal::Text("x".into())))),
                    })),
                }),
            }),
        ])))));
        let (diags, _effects) =
            check_module(vec![make_source("people"), sink, make_fun("f", body)]);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("IO effects are not allowed inside atomic")),
            "IO run through a non-fork wrapper must still be rejected: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn nested_atomic_io_violation_reported_once_per_block() {
        // atomic (do { _ <- *people; _ <- atomic (do { _ <- *people; now }) })
        // The inner atomic's clock violation must be reported exactly once for
        // the inner block (plus once for the outer, which sees the propagated
        // clock effect) — not duplicated by the outer block's fork-stripped
        // gate re-traversal of `inner`.
        let inner_atomic = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(
            vec![
                spanned(StmtKind::Bind {
                    pat: spanned(PatKind::Wildcard),
                    expr: spanned(ExprKind::SourceRef("people".into())),
                }),
                spanned(StmtKind::Expr(spanned(ExprKind::Var("now".into())))),
            ],
        )))));
        let body = spanned(ExprKind::Atomic(Box::new(spanned(ExprKind::Do(vec![
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: spanned(ExprKind::SourceRef("people".into())),
            }),
            spanned(StmtKind::Bind {
                pat: spanned(PatKind::Wildcard),
                expr: inner_atomic,
            }),
        ])))));
        let (diags, _effects) =
            check_module(vec![make_source("people"), make_fun("f", body)]);
        let io_errors = diags
            .iter()
            .filter(|d| d.message.contains("IO effects are not allowed inside atomic"))
            .count();
        // One per atomic block (inner + outer), with no gate-traversal dupes.
        assert_eq!(
            io_errors, 2,
            "nested atomic IO violation should be reported once per block, got {io_errors}: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
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
            ty_params: vec![],
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
            ty_params: vec![],
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
            ty_params: vec![],
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
            ty_params: vec![],
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
            ty_params: vec![],
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
            ty_params: vec![],
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
                ty_params: vec![],
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
            ty_params: vec![],
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
                ty_params: vec![],
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

    /// Regression (bug B17): a closed-row (`fixed_row`) callee absorbs its
    /// callback's *IO* effects into its declared row, but relation reads/writes
    /// are invisible at the type level (`*rel` is `IO {}`) — no declared row can
    /// express them. So a callback that reads a relation, laundered through a
    /// closed-row callee, must still report that read at the call site;
    /// otherwise a dishonest `leak : IO {} [Item]` passes and the honest
    /// `leak : IO {r *secrets} [Item]` wrongly warns "declared effects unused".
    #[test]
    fn fixed_row_callee_propagates_lambda_db_reads() {
        // runCb : (Int -> IO {} [Item]) -> IO {} [Item]
        // runCb = \cb -> cb 0
        let run_cb_body = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("cb".into()))],
            ty_params: vec![],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("cb".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Int("0".into())))),
            })),
        });
        let items_ty = || {
            spanned(TypeKind::Relation(Box::new(spanned(TypeKind::Named(
                "Item".into(),
            )))))
        };
        let run_cb_ty = TypeScheme {
            constraints: vec![],
            ty: spanned(TypeKind::Function {
                param: Box::new(spanned(TypeKind::Function {
                    param: Box::new(spanned(TypeKind::Named("Int".into()))),
                    result: Box::new(spanned(TypeKind::IO {
                        effects: vec![],
                        rest: vec![],
                        ty: Box::new(items_ty()),
                    })),
                })),
                result: Box::new(spanned(TypeKind::IO {
                    effects: vec![],
                    rest: vec![],
                    ty: Box::new(items_ty()),
                })),
            }),
        };

        // leak = runCb (\n -> *secrets)
        let leak_body = spanned(ExprKind::App {
            func: Box::new(spanned(ExprKind::Var("runCb".into()))),
            arg: Box::new(spanned(ExprKind::Lambda {
                params: vec![spanned(PatKind::Var("n".into()))],
                ty_params: vec![],
                body: Box::new(spanned(ExprKind::SourceRef("secrets".into()))),
            })),
        });

        let (_diags, effects) = check_module(vec![
            make_source("secrets"),
            make_fun_with_type("runCb", run_cb_body, run_cb_ty),
            make_fun("leak", leak_body),
        ]);
        assert!(
            effects["leak"].reads.contains("secrets"),
            "callback's relation read must propagate through a closed-row callee"
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

    /// Regression: when a lambda is piped into a non-Var callee (e.g. an
    /// inline lambda), the pipe arm's `head_name(rhs)` returns None.
    /// The default must be `true` (matching the App spine) so the lambda's
    /// body effects propagate — otherwise IO slips past the atomic gate.
    #[test]
    fn pipe_lambda_into_non_var_callee_propagates_effects() {
        // f = (\_ -> println "hi") |> (\g -> g 0)
        let lhs = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("_".into()))],
            ty_params: vec![],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("println".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Text("hi".into())))),
            })),
        });
        let rhs = spanned(ExprKind::Lambda {
            params: vec![spanned(PatKind::Var("g".into()))],
            ty_params: vec![],
            body: Box::new(spanned(ExprKind::App {
                func: Box::new(spanned(ExprKind::Var("g".into()))),
                arg: Box::new(spanned(ExprKind::Lit(Literal::Int("0".into())))),
            })),
        });
        let body = spanned(ExprKind::BinOp {
            op: BinOp::Pipe,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        });
        let (_diags, effects) = check_module(vec![make_fun("f", body)]);
        assert!(
            effects["f"].console,
            "lambda piped into non-Var callee should still propagate console effect"
        );
    }
}
