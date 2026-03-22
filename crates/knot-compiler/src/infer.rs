//! Hindley-Milner type inference for the Knot language.
//!
//! Infers and checks types for all declarations. Reports type errors as
//! diagnostics. The runtime uses uniform pointer representation, so this
//! pass is purely for error detection — it does not affect code generation.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// ── Monad info (shared with codegen) ──────────────────────────────

/// Which monad a desugared do-block targets.
#[derive(Debug, Clone, PartialEq)]
pub enum MonadKind {
    /// The built-in `[]` relation monad.
    Relation,
    /// An ADT-based monad (e.g., `Maybe`, `Result`).
    Adt(String),
    /// The IO monad for sequencing side effects.
    IO,
}

/// IO effect kinds tracked in the IO type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoEffect {
    Console,
    Fs,
    Network,
    Clock,
    Random,
}

/// Maps desugared do-block spans to their resolved monad type.
pub type MonadInfo = HashMap<Span, MonadKind>;

/// Maps declaration names to their inferred type display strings.
pub type TypeInfo = HashMap<String, String>;

/// Maps binding spans (local variables, params, patterns) to their inferred type strings.
pub type LocalTypeInfo = HashMap<Span, String>;

// ── Internal type representation ──────────────────────────────────

type TyVar = u32;

/// Internal type representation for unification-based inference.
#[derive(Debug, Clone)]
enum Ty {
    /// Unification variable.
    Var(TyVar),
    /// Primitives.
    Int,
    Float,
    Text,
    Bool,
    Bytes,
    /// Function type.
    Fun(Box<Ty>, Box<Ty>),
    /// Record with named fields and optional row variable (open record).
    Record(BTreeMap<String, Ty>, Option<TyVar>),
    /// Relation (set) type: [T].
    Relation(Box<Ty>),
    /// Named algebraic data type with optional type arguments.
    Con(String, Vec<Ty>),
    /// Variant with named constructors and optional row variable (open variant).
    /// Each constructor maps to its field types as a Record.
    Variant(BTreeMap<String, Ty>, Option<TyVar>),
    /// Unapplied type constructor (e.g. `[]`, `Maybe`).
    /// Used for higher-kinded type polymorphism.
    TyCon(String),
    /// Type-level application (e.g. `f a` where `f` is a HK variable).
    App(Box<Ty>, Box<Ty>),
    /// IO monad with tracked effects: IO {console, fs} a
    IO(BTreeSet<IoEffect>, Box<Ty>),
    /// Error sentinel — suppresses cascading errors.
    Error,
}

impl Ty {
    fn unit() -> Ty {
        Ty::Record(BTreeMap::new(), None)
    }
}

/// A trait constraint on a type variable: `TraitName a`.
#[derive(Debug, Clone)]
struct TyConstraint {
    trait_name: String,
    type_var: TyVar,
}

/// Polymorphic type scheme: ∀ vars. constraints => ty
#[derive(Debug, Clone)]
struct Scheme {
    vars: Vec<TyVar>,
    constraints: Vec<TyConstraint>,
    ty: Ty,
}

impl Scheme {
    fn mono(ty: Ty) -> Self {
        Scheme {
            vars: vec![],
            constraints: vec![],
            ty,
        }
    }

    fn poly(vars: Vec<TyVar>, ty: Ty) -> Self {
        Scheme {
            vars,
            constraints: vec![],
            ty,
        }
    }

    fn constrained(
        vars: Vec<TyVar>,
        constraints: Vec<TyConstraint>,
        ty: Ty,
    ) -> Self {
        Scheme {
            vars,
            constraints,
            ty,
        }
    }
}

/// A deferred constraint check: after inference resolves type variables,
/// verify that the concrete type satisfies the required trait.
#[derive(Debug, Clone)]
struct DeferredConstraint {
    trait_name: String,
    type_var: TyVar,
    span: Span,
}

// ── Constructor and data type metadata ────────────────────────────

#[derive(Debug, Clone)]
struct CtorInfo {
    data_type: String,
    data_params: Vec<String>,
    fields: Vec<(String, ast::Type)>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DataInfo {
    params: Vec<String>,
    ctors: Vec<(String, Vec<(String, ast::Type)>)>,
}

// ── Inference engine ──────────────────────────────────────────────

struct Infer {
    next_var: TyVar,
    subst: HashMap<TyVar, Ty>,

    /// Scoped variable environment (functions, let-bindings, params).
    scopes: Vec<HashMap<String, Scheme>>,

    /// Constructor metadata: ctor_name → info.
    constructors: HashMap<String, CtorInfo>,

    /// Data type definitions: type_name → info.
    data_types: HashMap<String, DataInfo>,

    /// Source/view relation types: name → full type (always Relation(...)).
    source_types: HashMap<String, Ty>,

    /// Derived relation types: name → full type.
    derived_types: HashMap<String, Ty>,

    /// Names that are views (for lenient set checking).
    view_names: HashSet<String>,

    /// Associated type names (from trait declarations).
    assoc_type_names: HashSet<String>,

    /// Type aliases: name → resolved Ty.
    aliases: HashMap<String, Ty>,

    /// Mapping from annotation type-variable names to TyVars (per-declaration).
    annotation_vars: HashMap<String, TyVar>,

    /// Accumulated type errors.
    errors: Vec<(String, Span)>,

    /// Monad type-constructor variables from desugared do-blocks.
    /// Each entry records (span, monad_tyvar) so we can resolve the
    /// concrete monad after inference completes.
    monad_vars: Vec<(Span, TyVar)>,

    /// Trait method → trait name mapping (e.g. "display" → "Display").
    trait_method_traits: HashMap<String, String>,

    /// Known trait implementations: (trait_name, type_name).
    known_impls: HashSet<(String, String)>,

    /// Deferred trait constraint checks, resolved after inference.
    deferred_constraints: Vec<DeferredConstraint>,

    /// Trait definitions: trait_name → list of param names.
    trait_params: HashMap<String, Vec<String>>,

    /// Spans of local variable bindings and their types (for LSP hover).
    binding_types: Vec<(Span, Ty)>,

    /// Route constructor → response type mapping (for `fetch` return type resolution).
    fetch_response_types: HashMap<String, ast::Type>,

    /// Route constructor → response header fields (for `fetch` response wrapping).
    fetch_response_headers: HashMap<String, Vec<ast::Field<ast::Type>>>,

    /// Whether we are currently inside an IO do-block. When true, `yield expr`
    /// produces `IO {} expr_type` instead of `[expr_type]`, allowing yield to
    /// be used as "return unit" in if/case branches within IO do blocks.
    in_io_do: bool,

    /// Whether we are currently inside an `atomic` block.
    in_atomic: bool,
}

// ── Core operations ───────────────────────────────────────────────

impl Infer {
    fn new() -> Self {
        Self {
            next_var: 0,
            subst: HashMap::new(),
            scopes: vec![HashMap::new()],
            constructors: HashMap::new(),
            data_types: HashMap::new(),
            source_types: HashMap::new(),
            derived_types: HashMap::new(),
            view_names: HashSet::new(),
            assoc_type_names: HashSet::new(),
            aliases: HashMap::new(),
            annotation_vars: HashMap::new(),
            errors: Vec::new(),
            monad_vars: Vec::new(),
            trait_method_traits: HashMap::new(),
            known_impls: HashSet::new(),
            deferred_constraints: Vec::new(),
            binding_types: Vec::new(),
            trait_params: HashMap::new(),
            fetch_response_types: HashMap::new(),
            fetch_response_headers: HashMap::new(),
            in_io_do: false,
            in_atomic: false,
        }
    }

    fn fresh(&mut self) -> Ty {
        Ty::Var(self.fresh_var())
    }

    fn fresh_var(&mut self) -> TyVar {
        let v = self.next_var;
        self.next_var += 1;
        v
    }

    fn error(&mut self, msg: String, span: Span) {
        self.errors.push((msg, span));
    }

    /// Compress all substitution chains so every variable points directly
    /// to its fully resolved type. Makes subsequent `apply` calls O(1).
    fn compress_substitution(&mut self) {
        let vars: Vec<TyVar> = self.subst.keys().copied().collect();
        for v in vars {
            let resolved = self.apply(&Ty::Var(v));
            self.subst.insert(v, resolved);
        }
    }

    // ── Substitution application ─────────────────────────────────

    fn apply(&self, ty: &Ty) -> Ty {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.apply(resolved),
                None => ty.clone(),
            },
            Ty::Fun(p, r) => {
                Ty::Fun(Box::new(self.apply(p)), Box::new(self.apply(r)))
            }
            Ty::Record(fields, row) => {
                let mut applied: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.apply(v)))
                    .collect();
                if let Some(rv) = row {
                    let resolved = self.apply(&Ty::Var(*rv));
                    match resolved {
                        Ty::Record(extra, rest) => {
                            for (k, v) in extra {
                                applied.entry(k).or_insert(v);
                            }
                            Ty::Record(applied, rest)
                        }
                        Ty::Var(rv2) => Ty::Record(applied, Some(rv2)),
                        _ => Ty::Record(applied, None),
                    }
                } else {
                    Ty::Record(applied, None)
                }
            }
            Ty::Variant(ctors, row) => {
                let mut applied: BTreeMap<String, Ty> = ctors
                    .iter()
                    .map(|(k, v)| (k.clone(), self.apply(v)))
                    .collect();
                if let Some(rv) = row {
                    let resolved = self.apply(&Ty::Var(*rv));
                    match resolved {
                        Ty::Variant(extra, rest) => {
                            for (k, v) in extra {
                                applied.entry(k).or_insert(v);
                            }
                            Ty::Variant(applied, rest)
                        }
                        Ty::Var(rv2) => Ty::Variant(applied, Some(rv2)),
                        _ => Ty::Variant(applied, None),
                    }
                } else {
                    Ty::Variant(applied, None)
                }
            }
            Ty::Relation(inner) => {
                Ty::Relation(Box::new(self.apply(inner)))
            }
            Ty::Con(name, args) => Ty::Con(
                name.clone(),
                args.iter().map(|a| self.apply(a)).collect(),
            ),
            Ty::TyCon(_) => ty.clone(),
            Ty::App(f, a) => {
                let f = self.apply(f);
                let a = self.apply(a);
                Self::normalize_app(f, a)
            }
            Ty::IO(effects, inner) => {
                Ty::IO(effects.clone(), Box::new(self.apply(inner)))
            }
            _ => ty.clone(),
        }
    }

    /// Normalize a type-level application after substitution.
    /// Reduces `App(TyCon("[]"), a)` → `Relation(a)`,
    /// `App(TyCon(name), a)` → `Con(name, [a])`, etc.
    fn normalize_app(f: Ty, a: Ty) -> Ty {
        match f {
            Ty::TyCon(ref name) if name == "[]" => Ty::Relation(Box::new(a)),
            Ty::TyCon(name) => Ty::Con(name, vec![a]),
            Ty::Con(name, mut args) => {
                args.push(a);
                Ty::Con(name, args)
            }
            _ => Ty::App(Box::new(f), Box::new(a)),
        }
    }

    // ── Occurs check ─────────────────────────────────────────────

    fn occurs_in(&self, var: TyVar, ty: &Ty) -> bool {
        match ty {
            Ty::Var(v) => {
                if *v == var {
                    return true;
                }
                match self.subst.get(v) {
                    Some(resolved) => self.occurs_in(var, resolved),
                    None => false,
                }
            }
            Ty::Fun(p, r) => {
                self.occurs_in(var, p) || self.occurs_in(var, r)
            }
            Ty::Record(fields, row) => {
                if fields.values().any(|v| self.occurs_in(var, v)) {
                    return true;
                }
                if let Some(rv) = row {
                    if *rv == var {
                        return true;
                    }
                    if let Some(resolved) = self.subst.get(rv) {
                        return self.occurs_in(var, resolved);
                    }
                }
                false
            }
            Ty::Variant(ctors, row) => {
                if ctors.values().any(|v| self.occurs_in(var, v)) {
                    return true;
                }
                if let Some(rv) = row {
                    if *rv == var {
                        return true;
                    }
                    if let Some(resolved) = self.subst.get(rv) {
                        return self.occurs_in(var, resolved);
                    }
                }
                false
            }
            Ty::Relation(inner) => self.occurs_in(var, inner),
            Ty::Con(_, args) => args.iter().any(|a| self.occurs_in(var, a)),
            Ty::TyCon(_) => false,
            Ty::App(f, a) => {
                self.occurs_in(var, f) || self.occurs_in(var, a)
            }
            Ty::IO(_, inner) => self.occurs_in(var, inner),
            _ => false,
        }
    }

    // ── Unification ──────────────────────────────────────────────

    fn unify(&mut self, t1: &Ty, t2: &Ty, span: Span) {
        let t1 = self.apply(t1);
        let t2 = self.apply(t2);

        match (&t1, &t2) {
            (Ty::Error, _) | (_, Ty::Error) => {}
            (Ty::Var(a), Ty::Var(b)) if a == b => {}
            (Ty::Var(v), _) => {
                let v = *v;
                if self.occurs_in(v, &t2) {
                    self.error("infinite type".into(), span);
                } else {
                    self.subst.insert(v, t2);
                }
            }
            (_, Ty::Var(v)) => {
                let v = *v;
                if self.occurs_in(v, &t1) {
                    self.error("infinite type".into(), span);
                } else {
                    self.subst.insert(v, t1);
                }
            }
            (Ty::Int, Ty::Int)
            | (Ty::Float, Ty::Float)
            | (Ty::Text, Ty::Text)
            | (Ty::Bool, Ty::Bool)
            | (Ty::Bytes, Ty::Bytes) => {}
            (Ty::Fun(p1, r1), Ty::Fun(p2, r2)) => {
                self.unify(p1, p2, span);
                self.unify(r1, r2, span);
            }
            (Ty::Relation(a), Ty::Relation(b)) => {
                self.unify(a, b, span);
            }
            (Ty::Con(n1, a1), Ty::Con(n2, a2))
                if n1 == n2 && a1.len() == a2.len() =>
            {
                let a1 = a1.clone();
                let a2 = a2.clone();
                for (a, b) in a1.iter().zip(a2.iter()) {
                    self.unify(a, b, span);
                }
            }
            (Ty::Record(f1, r1), Ty::Record(f2, r2)) => {
                self.unify_records(f1, *r1, f2, *r2, span);
            }
            // ── Higher-kinded type support ─────────────────────
            (Ty::TyCon(a), Ty::TyCon(b)) if a == b => {}
            (Ty::App(f1, a1), Ty::App(f2, a2)) => {
                self.unify(f1, f2, span);
                self.unify(a1, a2, span);
            }
            // App(f, a) vs Relation(b) → f = [], a = b
            (Ty::App(f, a), Ty::Relation(b))
            | (Ty::Relation(b), Ty::App(f, a)) => {
                self.unify(f, &Ty::TyCon("[]".into()), span);
                self.unify(a, b, span);
            }
            // App(f, a) vs Con(name, args) — decompose the constructor
            (Ty::App(f, a), Ty::Con(name, args))
            | (Ty::Con(name, args), Ty::App(f, a)) => {
                if args.is_empty() {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    self.error(
                        format!(
                            "type mismatch: expected {}, found {}",
                            d1, d2
                        ),
                        span,
                    );
                } else {
                    let last = args.last().unwrap().clone();
                    let init: Vec<Ty> =
                        args[..args.len() - 1].to_vec();
                    let partial = if init.is_empty() {
                        Ty::TyCon(name.clone())
                    } else {
                        Ty::Con(name.clone(), init)
                    };
                    self.unify(&f, &partial, span);
                    self.unify(&a, &last, span);
                }
            }
            // ── IO monad ──────────────────────────────────────
            (Ty::IO(e1, a), Ty::IO(e2, b)) => {
                self.unify(a, b, span);
                // Effects are merged (union) — not unified
                let _ = (e1, e2);
            }
            // In IO do blocks, silently allow Relation types to unify
            // with other types. Route handlers freely mix relational
            // operations and `respond` calls in if/case branches.
            (Ty::Relation(_), _) | (_, Ty::Relation(_)) if self.in_io_do => {}

            // ── Row-polymorphic variants ────────────────────────
            (Ty::Variant(c1, r1), Ty::Variant(c2, r2)) => {
                self.unify_variants(c1, *r1, c2, *r2, span);
            }
            (Ty::Con(name, args), Ty::Variant(c2, r2)) => {
                if let Some(expanded) = self.con_to_variant(name, args) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(&ec, er, c2, *r2, span);
                } else {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    self.error(
                        format!(
                            "type mismatch: expected {}, found {}",
                            d1, d2
                        ),
                        span,
                    );
                }
            }
            (Ty::Variant(c1, r1), Ty::Con(name, args)) => {
                if let Some(expanded) = self.con_to_variant(name, args) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(c1, *r1, &ec, er, span);
                } else {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    self.error(
                        format!(
                            "type mismatch: expected {}, found {}",
                            d1, d2
                        ),
                        span,
                    );
                }
            }
            _ => {
                let d1 = self.display_ty(&t1);
                let d2 = self.display_ty(&t2);
                self.error(
                    format!("type mismatch: expected {}, found {}", d1, d2),
                    span,
                );
            }
        }
    }

    fn unify_records(
        &mut self,
        f1: &BTreeMap<String, Ty>,
        r1: Option<TyVar>,
        f2: &BTreeMap<String, Ty>,
        r2: Option<TyVar>,
        span: Span,
    ) {
        // Unify common fields (BTreeMap lookup is O(log n), no HashSet needed)
        for (key, ty1) in f1 {
            if let Some(ty2) = f2.get(key) {
                self.unify(ty1, ty2, span);
            }
        }

        let only1: BTreeMap<String, Ty> = f1
            .iter()
            .filter(|(k, _)| !f2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let only2: BTreeMap<String, Ty> = f2
            .iter()
            .filter(|(k, _)| !f1.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        match (r1, r2) {
            (None, None) => {
                if !only1.is_empty() || !only2.is_empty() {
                    let extras: Vec<_> =
                        only1.keys().chain(only2.keys()).cloned().collect();
                    self.error(
                        format!(
                            "record fields don't match: extra fields {{{}}}",
                            extras.join(", ")
                        ),
                        span,
                    );
                }
            }
            (Some(rv), None) => {
                if !only1.is_empty() {
                    let names: Vec<_> = only1.keys().cloned().collect();
                    self.error(
                        format!(
                            "record has unexpected fields: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Record(only2, None));
            }
            (None, Some(rv)) => {
                if !only2.is_empty() {
                    let names: Vec<_> = only2.keys().cloned().collect();
                    self.error(
                        format!(
                            "record has unexpected fields: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Record(only1, None));
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "record fields don't match".into(),
                            span,
                        );
                    }
                } else {
                    let fresh = self.fresh_var();
                    self.subst
                        .insert(rv1, Ty::Record(only2, Some(fresh)));
                    self.subst
                        .insert(rv2, Ty::Record(only1, Some(fresh)));
                }
            }
        }
    }

    fn unify_variants(
        &mut self,
        c1: &BTreeMap<String, Ty>,
        r1: Option<TyVar>,
        c2: &BTreeMap<String, Ty>,
        r2: Option<TyVar>,
        span: Span,
    ) {
        // Unify common constructors' field types (BTreeMap lookup is O(log n))
        for (key, ty1) in c1 {
            if let Some(ty2) = c2.get(key) {
                self.unify(ty1, ty2, span);
            }
        }

        let only1: BTreeMap<String, Ty> = c1
            .iter()
            .filter(|(k, _)| !c2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let only2: BTreeMap<String, Ty> = c2
            .iter()
            .filter(|(k, _)| !c1.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        match (r1, r2) {
            (None, None) => {
                if !only1.is_empty() || !only2.is_empty() {
                    let extras: Vec<_> =
                        only1.keys().chain(only2.keys()).cloned().collect();
                    self.error(
                        format!(
                            "variant constructors don't match: extra constructors {}",
                            extras.join(", ")
                        ),
                        span,
                    );
                }
            }
            (Some(rv), None) => {
                if !only1.is_empty() {
                    let names: Vec<_> = only1.keys().cloned().collect();
                    self.error(
                        format!(
                            "variant has unexpected constructors: {}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Variant(only2, None));
            }
            (None, Some(rv)) => {
                if !only2.is_empty() {
                    let names: Vec<_> = only2.keys().cloned().collect();
                    self.error(
                        format!(
                            "variant has unexpected constructors: {}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Variant(only1, None));
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "variant constructors don't match".into(),
                            span,
                        );
                    }
                } else {
                    let fresh = self.fresh_var();
                    self.subst
                        .insert(rv1, Ty::Variant(only2, Some(fresh)));
                    self.subst
                        .insert(rv2, Ty::Variant(only1, Some(fresh)));
                }
            }
        }
    }

    /// Expand a nominal ADT (`Con(name, args)`) to a structural `Variant`.
    fn con_to_variant(
        &mut self,
        name: &str,
        args: &[Ty],
    ) -> Option<Ty> {
        let info = self.data_types.get(name)?.clone();
        // Build param → arg mapping
        let mapping: HashMap<TyVar, Ty> = info
            .params
            .iter()
            .zip(args.iter())
            .map(|(param_name, arg_ty)| {
                let var = self.annotation_var(param_name);
                (var, arg_ty.clone())
            })
            .collect();
        let mut ctors = BTreeMap::new();
        for (ctor_name, fields) in &info.ctors {
            let field_tys: BTreeMap<String, Ty> = fields
                .iter()
                .map(|(fname, fty)| {
                    let ty = self.ast_type_to_ty(fty);
                    let ty = self.subst_ty(&ty, &mapping);
                    (fname.clone(), ty)
                })
                .collect();
            ctors.insert(ctor_name.clone(), Ty::Record(field_tys, None));
        }
        self.annotation_vars.clear();
        Some(Ty::Variant(ctors, None))
    }

    // ── Scheme operations ────────────────────────────────────────

    fn instantiate(&mut self, scheme: &Scheme) -> Ty {
        self.instantiate_at(scheme, Span::new(0, 0))
    }

    fn instantiate_at(&mut self, scheme: &Scheme, span: Span) -> Ty {
        if scheme.vars.is_empty() {
            return scheme.ty.clone();
        }
        let mapping: HashMap<TyVar, Ty> = scheme
            .vars
            .iter()
            .map(|v| (*v, self.fresh()))
            .collect();
        // Create deferred constraints for each constraint in the scheme
        for c in &scheme.constraints {
            if let Some(Ty::Var(new_var)) = mapping.get(&c.type_var) {
                self.deferred_constraints.push(DeferredConstraint {
                    trait_name: c.trait_name.clone(),
                    type_var: *new_var,
                    span,
                });
            }
        }
        self.subst_ty(&scheme.ty, &mapping)
    }

    /// Substitute type variables according to a mapping (for instantiation).
    fn subst_ty(&self, ty: &Ty, mapping: &HashMap<TyVar, Ty>) -> Ty {
        match ty {
            Ty::Var(v) => {
                if let Some(replacement) = mapping.get(v) {
                    replacement.clone()
                } else if let Some(resolved) = self.subst.get(v) {
                    self.subst_ty(resolved, mapping)
                } else {
                    ty.clone()
                }
            }
            Ty::Fun(p, r) => Ty::Fun(
                Box::new(self.subst_ty(p, mapping)),
                Box::new(self.subst_ty(r, mapping)),
            ),
            Ty::Record(fields, row) => {
                let new_fields: BTreeMap<_, _> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.subst_ty(v, mapping)))
                    .collect();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::Record(new_fields, new_row)
            }
            Ty::Variant(ctors, row) => {
                let new_ctors: BTreeMap<_, _> = ctors
                    .iter()
                    .map(|(k, v)| (k.clone(), self.subst_ty(v, mapping)))
                    .collect();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::Variant(new_ctors, new_row)
            }
            Ty::Relation(inner) => {
                Ty::Relation(Box::new(self.subst_ty(inner, mapping)))
            }
            Ty::Con(name, args) => Ty::Con(
                name.clone(),
                args.iter().map(|a| self.subst_ty(a, mapping)).collect(),
            ),
            Ty::TyCon(_) => ty.clone(),
            Ty::App(f, a) => Ty::App(
                Box::new(self.subst_ty(f, mapping)),
                Box::new(self.subst_ty(a, mapping)),
            ),
            Ty::IO(effects, inner) => Ty::IO(
                effects.clone(),
                Box::new(self.subst_ty(inner, mapping)),
            ),
            _ => ty.clone(),
        }
    }

    fn generalize(&self, ty: &Ty) -> Scheme {
        self.generalize_with_constraints(ty, vec![])
    }

    fn generalize_with_constraints(
        &self,
        ty: &Ty,
        all_constraints: Vec<TyConstraint>,
    ) -> Scheme {
        let applied = self.apply(ty);
        let env_fv = self.free_vars_in_env();
        let ty_fv = self.free_vars(&applied);
        let gen_vars: Vec<TyVar> =
            ty_fv.difference(&env_fv).copied().collect();
        let gen_set: HashSet<TyVar> = gen_vars.iter().copied().collect();
        // Only keep constraints on generalized variables
        let constraints: Vec<TyConstraint> = all_constraints
            .into_iter()
            .filter(|c| {
                let resolved = self.apply(&Ty::Var(c.type_var));
                match resolved {
                    Ty::Var(v) => gen_set.contains(&v),
                    _ => false, // concrete type — will be checked immediately
                }
            })
            .collect();
        Scheme {
            vars: gen_vars,
            constraints,
            ty: applied,
        }
    }

    fn free_vars(&self, ty: &Ty) -> HashSet<TyVar> {
        let mut s = HashSet::new();
        self.collect_free_vars(ty, &mut s);
        s
    }

    fn collect_free_vars(&self, ty: &Ty, out: &mut HashSet<TyVar>) {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.collect_free_vars(resolved, out),
                None => {
                    out.insert(*v);
                }
            },
            Ty::Fun(p, r) => {
                self.collect_free_vars(p, out);
                self.collect_free_vars(r, out);
            }
            Ty::Record(fields, row) => {
                for v in fields.values() {
                    self.collect_free_vars(v, out);
                }
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            self.collect_free_vars(resolved, out)
                        }
                        None => {
                            out.insert(*rv);
                        }
                    }
                }
            }
            Ty::Variant(ctors, row) => {
                for v in ctors.values() {
                    self.collect_free_vars(v, out);
                }
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            self.collect_free_vars(resolved, out)
                        }
                        None => {
                            out.insert(*rv);
                        }
                    }
                }
            }
            Ty::Relation(inner) => self.collect_free_vars(inner, out),
            Ty::Con(_, args) => {
                for a in args {
                    self.collect_free_vars(a, out);
                }
            }
            Ty::TyCon(_) => {}
            Ty::App(f, a) => {
                self.collect_free_vars(f, out);
                self.collect_free_vars(a, out);
            }
            Ty::IO(_, inner) => {
                self.collect_free_vars(inner, out);
            }
            _ => {}
        }
    }

    fn free_vars_in_env(&self) -> HashSet<TyVar> {
        let mut s = HashSet::new();
        for scope in &self.scopes {
            for scheme in scope.values() {
                let mut fv = self.free_vars(&scheme.ty);
                for v in &scheme.vars {
                    fv.remove(v);
                }
                s.extend(fv);
            }
        }
        for ty in self.source_types.values() {
            self.collect_free_vars(ty, &mut s);
        }
        for ty in self.derived_types.values() {
            self.collect_free_vars(ty, &mut s);
        }
        s
    }

    // ── Environment ──────────────────────────────────────────────

    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    fn bind(&mut self, name: &str, scheme: Scheme) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    fn bind_top(&mut self, name: &str, scheme: Scheme) {
        if let Some(scope) = self.scopes.first_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    fn lookup(&self, name: &str) -> Option<&Scheme> {
        for scope in self.scopes.iter().rev() {
            if let Some(scheme) = scope.get(name) {
                return Some(scheme);
            }
        }
        None
    }

    fn lookup_instantiate(&mut self, name: &str) -> Option<Ty> {
        let scheme = self.lookup(name)?.clone();
        Some(self.instantiate(&scheme))
    }

    fn lookup_instantiate_at(
        &mut self,
        name: &str,
        span: Span,
    ) -> Option<Ty> {
        let scheme = self.lookup(name)?.clone();
        Some(self.instantiate_at(&scheme, span))
    }

    // ── AST type → Ty ────────────────────────────────────────────

    fn ast_type_to_ty(&mut self, ty: &ast::Type) -> Ty {
        match &ty.node {
            ast::TypeKind::Named(name) => match name.as_str() {
                "Int" => Ty::Int,
                "Float" => Ty::Float,
                "Text" => Ty::Text,
                "Bool" => Ty::Bool,
                "Bytes" => Ty::Bytes,
                "[]" => Ty::TyCon("[]".into()),
                _ => {
                    if let Some(aliased) = self.aliases.get(name).cloned() {
                        aliased
                    } else if self
                        .data_types
                        .get(name)
                        .map_or(false, |d| !d.params.is_empty())
                    {
                        // Parameterized data type used without arguments
                        // → type constructor (for HKT support).
                        Ty::TyCon(name.clone())
                    } else {
                        Ty::Con(name.clone(), vec![])
                    }
                }
            },
            ast::TypeKind::Var(name) => {
                let var = self.annotation_var(name);
                Ty::Var(var)
            }
            ast::TypeKind::Record { fields, rest } => {
                if fields.is_empty() && rest.is_none() {
                    return Ty::unit();
                }
                let field_tys: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|f| (f.name.clone(), self.ast_type_to_ty(&f.value)))
                    .collect();
                let row_var =
                    rest.as_ref().map(|name| self.annotation_var(name));
                Ty::Record(field_tys, row_var)
            }
            ast::TypeKind::Relation(inner) => {
                Ty::Relation(Box::new(self.ast_type_to_ty(inner)))
            }
            ast::TypeKind::Function { param, result } => Ty::Fun(
                Box::new(self.ast_type_to_ty(param)),
                Box::new(self.ast_type_to_ty(result)),
            ),
            ast::TypeKind::App { func, arg } => {
                // Check for associated type applications first.
                if let ast::TypeKind::Named(name) = &func.node {
                    if self.assoc_type_names.contains(name) {
                        return self.fresh();
                    }
                }
                let arg_ty = self.ast_type_to_ty(arg);
                let func_ty = self.ast_type_to_ty(func);
                match func_ty {
                    // Named constructor accumulates arguments.
                    Ty::Con(name, mut args) => {
                        args.push(arg_ty);
                        Ty::Con(name, args)
                    }
                    // HK type variable or nested App — produce App node.
                    Ty::Var(_) | Ty::App(_, _) | Ty::TyCon(_) => {
                        Ty::App(Box::new(func_ty), Box::new(arg_ty))
                    }
                    Ty::Error => Ty::Error,
                    _ => Ty::Error,
                }
            }
            ast::TypeKind::Hole => self.fresh(),
            ast::TypeKind::Variant {
                constructors,
                rest,
            } => {
                let ctor_tys: BTreeMap<String, Ty> = constructors
                    .iter()
                    .map(|c| {
                        let field_tys: BTreeMap<String, Ty> = c
                            .fields
                            .iter()
                            .map(|f| {
                                (
                                    f.name.clone(),
                                    self.ast_type_to_ty(&f.value),
                                )
                            })
                            .collect();
                        (c.name.clone(), Ty::Record(field_tys, None))
                    })
                    .collect();
                let row_var =
                    rest.as_ref().map(|name| self.annotation_var(name));
                Ty::Variant(ctor_tys, row_var)
            }
            ast::TypeKind::Effectful { ty, .. } => self.ast_type_to_ty(ty),
            ast::TypeKind::IO { effects, ty } => {
                let io_effects: BTreeSet<IoEffect> = effects.iter().filter_map(|e| match e.as_str() {
                    "console" => Some(IoEffect::Console),
                    "fs" => Some(IoEffect::Fs),
                    "network" => Some(IoEffect::Network),
                    "clock" => Some(IoEffect::Clock),
                    "random" => Some(IoEffect::Random),
                    _ => None,
                }).collect();
                Ty::IO(io_effects, Box::new(self.ast_type_to_ty(ty)))
            }
        }
    }

    fn annotation_var(&mut self, name: &str) -> TyVar {
        if let Some(&var) = self.annotation_vars.get(name) {
            var
        } else {
            let var = self.fresh_var();
            self.annotation_vars.insert(name.to_string(), var);
            var
        }
    }

    // ── Type display ─────────────────────────────────────────────

    fn display_ty(&self, ty: &Ty) -> String {
        self.display_ty_inner(ty, false)
    }

    fn display_ty_inner(&self, ty: &Ty, in_fun: bool) -> String {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.display_ty(resolved),
                None => {
                    let idx = *v as usize;
                    if idx < 26 {
                        format!("{}", (b'a' + idx as u8) as char)
                    } else {
                        format!("t{}", v)
                    }
                }
            },
            Ty::Int => "Int".into(),
            Ty::Float => "Float".into(),
            Ty::Text => "Text".into(),
            Ty::Bool => "Bool".into(),
            Ty::Bytes => "Bytes".into(),
            Ty::Fun(p, r) => {
                let s = format!(
                    "{} -> {}",
                    self.display_ty_inner(p, true),
                    self.display_ty_inner(r, false)
                );
                if in_fun {
                    format!("({})", s)
                } else {
                    s
                }
            }
            Ty::Record(fields, row) => {
                if fields.is_empty() && row.is_none() {
                    return "{}".into();
                }
                let mut parts: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| {
                        format!("{}: {}", n, self.display_ty(t))
                    })
                    .collect();
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            parts.push(format!(
                                "| {}",
                                self.display_ty(resolved)
                            ));
                        }
                        None => {
                            let idx = *rv as usize;
                            let name = if idx < 26 {
                                format!("{}", (b'a' + idx as u8) as char)
                            } else {
                                format!("r{}", rv)
                            };
                            parts.push(format!("| {}", name));
                        }
                    }
                }
                format!("{{{}}}", parts.join(", "))
            }
            Ty::Relation(inner) => {
                format!("[{}]", self.display_ty(inner))
            }
            Ty::Con(name, args) => {
                if args.is_empty() {
                    name.clone()
                } else {
                    let args_str: Vec<String> =
                        args.iter().map(|a| self.display_ty(a)).collect();
                    format!("{} {}", name, args_str.join(" "))
                }
            }
            Ty::Variant(ctors, row) => {
                let mut parts: Vec<String> = ctors
                    .iter()
                    .map(|(name, fields_ty)| {
                        let fields_str =
                            self.display_ty_inner(fields_ty, false);
                        format!("{} {}", name, fields_str)
                    })
                    .collect();
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            parts.push(self.display_ty(resolved));
                        }
                        None => {
                            let idx = *rv as usize;
                            let name = if idx < 26 {
                                format!("{}", (b'a' + idx as u8) as char)
                            } else {
                                format!("r{}", rv)
                            };
                            parts.push(name);
                        }
                    }
                }
                format!("<{}>", parts.join(" | "))
            }
            Ty::TyCon(name) => name.clone(),
            Ty::App(f, a) => {
                format!(
                    "({} {})",
                    self.display_ty(f),
                    self.display_ty(a)
                )
            }
            Ty::IO(effects, inner) => {
                let effects_str = if effects.is_empty() {
                    String::new()
                } else {
                    let names: Vec<&str> = effects.iter().map(|e| match e {
                        IoEffect::Console => "console",
                        IoEffect::Fs => "fs",
                        IoEffect::Network => "network",
                        IoEffect::Clock => "clock",
                        IoEffect::Random => "random",
                    }).collect();
                    format!(" {{{}}}", names.join(", "))
                };
                format!("IO{} {}", effects_str, self.display_ty(inner))
            }
            Ty::Error => "<error>".into(),
        }
    }

    // ── Constructor instantiation ────────────────────────────────

    /// Returns (data_type, field_record_type) with fresh vars for params.
    fn instantiate_ctor(
        &mut self,
        name: &str,
        _span: Span,
    ) -> Option<(Ty, Ty)> {
        let info = self.constructors.get(name)?.clone();

        self.annotation_vars.clear();
        let param_tys: Vec<Ty> = info
            .data_params
            .iter()
            .map(|p| {
                let v = self.fresh_var();
                self.annotation_vars.insert(p.clone(), v);
                Ty::Var(v)
            })
            .collect();

        let field_tys: BTreeMap<String, Ty> = info
            .fields
            .iter()
            .map(|(name, ty)| (name.clone(), self.ast_type_to_ty(ty)))
            .collect();

        let data_ty = if info.data_type == "Bool" {
            Ty::Bool
        } else {
            Ty::Con(info.data_type.clone(), param_tys)
        };
        let record_ty = Ty::Record(field_tys, None);

        Some((data_ty, record_ty))
    }

    // ── Expression inference ─────────────────────────────────────

    fn infer_expr(&mut self, expr: &ast::Expr) -> Ty {
        match &expr.node {
            ast::ExprKind::Lit(lit) => self.literal_type(lit),

            ast::ExprKind::Var(name) if name == "__yield" => {
                // ∀m a. a -> App(m, a)  — monadic yield (from do-desugaring)
                let m = self.fresh_var();
                let a = self.fresh_var();
                self.monad_vars.push((expr.span, m));
                Ty::Fun(
                    Box::new(Ty::Var(a)),
                    Box::new(Ty::App(
                        Box::new(Ty::Var(m)),
                        Box::new(Ty::Var(a)),
                    )),
                )
            }

            ast::ExprKind::Var(name) if name == "__empty" => {
                // ∀m a. App(m, a)  — monadic empty (from do-desugaring)
                let m = self.fresh_var();
                let a = self.fresh_var();
                self.monad_vars.push((expr.span, m));
                Ty::App(Box::new(Ty::Var(m)), Box::new(Ty::Var(a)))
            }

            ast::ExprKind::Var(name) if name == "__bind" => {
                // ∀m a b. (a -> App(m, b)) -> App(m, a) -> App(m, b)
                let m = self.fresh_var();
                let a = self.fresh_var();
                let b = self.fresh_var();
                self.monad_vars.push((expr.span, m));
                Ty::Fun(
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(a)),
                        Box::new(Ty::App(
                            Box::new(Ty::Var(m)),
                            Box::new(Ty::Var(b)),
                        )),
                    )),
                    Box::new(Ty::Fun(
                        Box::new(Ty::App(
                            Box::new(Ty::Var(m)),
                            Box::new(Ty::Var(a)),
                        )),
                        Box::new(Ty::App(
                            Box::new(Ty::Var(m)),
                            Box::new(Ty::Var(b)),
                        )),
                    )),
                )
            }

            ast::ExprKind::Var(name) => {
                if name == "retry" && !self.in_atomic {
                    self.error(
                        "'retry' can only be used inside an 'atomic' block".to_string(),
                        expr.span,
                    );
                }
                if let Some(ty) = self.lookup_instantiate_at(name, expr.span) {
                    ty
                } else {
                    self.error(
                        format!("undefined variable '{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::Constructor(name) => {
                if let Some((data_ty, record_ty)) =
                    self.instantiate_ctor(name, expr.span)
                {
                    Ty::Fun(Box::new(record_ty), Box::new(data_ty))
                } else {
                    self.error(
                        format!("unknown constructor '{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::SourceRef(name) => {
                if let Some(ty) = self.source_types.get(name).cloned() {
                    ty
                } else {
                    self.error(
                        format!("unknown source relation '*{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::DerivedRef(name) => {
                if let Some(ty) = self.derived_types.get(name).cloned() {
                    ty
                } else {
                    self.error(
                        format!("unknown derived relation '&{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::Record(fields) => {
                if fields.is_empty() {
                    return Ty::unit();
                }
                let field_tys: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|f| {
                        (f.name.clone(), self.infer_expr(&f.value))
                    })
                    .collect();
                Ty::Record(field_tys, None)
            }

            ast::ExprKind::RecordUpdate { base, fields } => {
                let base_ty = self.infer_expr(base);
                let mut update_fields = BTreeMap::new();
                for field in fields {
                    let val_ty = self.infer_expr(&field.value);
                    update_fields.insert(field.name.clone(), val_ty);
                }
                let rv = self.fresh_var();
                let constraint = Ty::Record(update_fields, Some(rv));
                self.unify(&base_ty, &constraint, expr.span);
                base_ty
            }

            ast::ExprKind::FieldAccess { expr: e, field } => {
                let expr_ty = self.infer_expr(e);
                let resolved = self.apply(&expr_ty);
                // If the expression is a relation (e.g., after groupBy), unwrap
                // to access fields on the element type. At runtime, this accesses
                // the field from the first element of the relation.
                let base_ty = if let Ty::Relation(elem) = resolved {
                    *elem
                } else {
                    expr_ty
                };
                let field_ty = self.fresh();
                let rv = self.fresh_var();
                let constraint = Ty::Record(
                    BTreeMap::from([(field.clone(), field_ty.clone())]),
                    Some(rv),
                );
                self.unify(&base_ty, &constraint, e.span);
                field_ty
            }

            ast::ExprKind::List(elems) => {
                let elem_ty = self.fresh();
                for e in elems {
                    let t = self.infer_expr(e);
                    self.unify(&elem_ty, &t, e.span);
                }
                Ty::Relation(Box::new(elem_ty))
            }

            ast::ExprKind::Lambda { params, body } => {
                self.push_scope();
                let mut param_types = Vec::new();
                for param in params {
                    let t = self.fresh();
                    self.check_pattern(param, &t);
                    param_types.push(t);
                }
                let body_ty = self.infer_expr(body);
                self.pop_scope();

                let mut result = body_ty;
                for pt in param_types.into_iter().rev() {
                    result = Ty::Fun(Box::new(pt), Box::new(result));
                }
                result
            }

            ast::ExprKind::App { func, arg } => {
                // Special case: fully handle `fetch url (Ctor {..})` to
                // skip the `respond` field and resolve the response type.
                if let Some(ty) = self.try_infer_fetch(expr) {
                    return ty;
                }

                let func_ty = self.infer_expr(func);
                let arg_ty = self.infer_expr(arg);
                let result_ty = self.fresh();
                let expected = Ty::Fun(
                    Box::new(arg_ty),
                    Box::new(result_ty.clone()),
                );
                self.unify(&func_ty, &expected, expr.span);
                result_ty
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                self.infer_binop(*op, lhs, rhs, expr.span)
            }

            ast::ExprKind::UnaryOp { op, operand } => {
                let operand_ty = self.infer_expr(operand);
                match op {
                    ast::UnaryOp::Neg => {
                        // numeric negation — result same type as operand
                        operand_ty
                    }
                    ast::UnaryOp::Not => {
                        self.unify(
                            &operand_ty,
                            &Ty::Bool,
                            operand.span,
                        );
                        Ty::Bool
                    }
                }
            }

            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let cond_ty = self.infer_expr(cond);
                self.unify(&cond_ty, &Ty::Bool, cond.span);
                let then_ty = self.infer_expr(then_branch);
                let else_ty = self.infer_expr(else_branch);
                self.unify(&then_ty, &else_ty, expr.span);
                then_ty
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                let scrut_ty = self.infer_expr(scrutinee);
                let result_ty = self.fresh();

                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify(&result_ty, &body_ty, arm.body.span);
                    self.pop_scope();
                }

                self.check_exhaustiveness(&scrut_ty, arms, expr.span);

                result_ty
            }

            ast::ExprKind::Do(stmts) => self.infer_do(stmts, expr.span),

            ast::ExprKind::Yield(inner) => {
                let inner_ty = self.infer_expr(inner);
                if self.in_io_do {
                    Ty::IO(BTreeSet::new(), Box::new(inner_ty))
                } else {
                    Ty::Relation(Box::new(inner_ty))
                }
            }

            ast::ExprKind::Set { target, value } => {
                let is_view = matches!(&target.node,
                    ast::ExprKind::SourceRef(n) if self.view_names.contains(n));
                if is_view {
                    // View writes have constant columns auto-filled by
                    // codegen — skip type-checking the value expression
                    // since its type intentionally differs from the view's
                    // read type.
                } else {
                    let target_ty = self.infer_expr(target);
                    let value_ty = self.infer_expr(value);
                    self.unify(&target_ty, &value_ty, expr.span);
                }
                Ty::unit()
            }

            ast::ExprKind::FullSet { target, value } => {
                let is_view = matches!(&target.node,
                    ast::ExprKind::SourceRef(n) if self.view_names.contains(n));
                if is_view {
                    // Same as Set: skip view write checking.
                } else {
                    let target_ty = self.infer_expr(target);
                    let value_ty = self.infer_expr(value);
                    self.unify(&target_ty, &value_ty, expr.span);
                }
                Ty::unit()
            }

            ast::ExprKind::Atomic(inner) => {
                let prev = self.in_atomic;
                self.in_atomic = true;
                let ty = self.infer_expr(inner);
                self.in_atomic = prev;
                ty
            }

            ast::ExprKind::At { relation, time } => {
                let rel_ty = self.infer_expr(relation);
                let time_ty = self.infer_expr(time);
                self.unify(&time_ty, &Ty::Int, time.span);
                rel_ty
            }
        }
    }

    /// Try to infer a `fetch` call. Returns `Some(ty)` if the expression
    /// is `fetch url (Ctor {..})` or `fetch url opts (Ctor {..})`.
    /// This skips the constructor's `respond` field and resolves the
    /// response type from route metadata.
    fn try_infer_fetch(&mut self, expr: &ast::Expr) -> Option<Ty> {
        let ctor_name = fetch_ctor_name(expr)?;

        // Collect all App arguments and the root function
        let (func_expr, args) = uncurry_fetch(expr);

        // Root must be Var("fetch") or Var("fetchWith")
        let is_fetch_with = match &func_expr.node {
            ast::ExprKind::Var(name) if name == "fetch" => false,
            ast::ExprKind::Var(name) if name == "fetchWith" => true,
            _ => return None,
        };

        // Validate arg count: fetch needs 2, fetchWith needs 3
        if (!is_fetch_with && args.len() != 2) || (is_fetch_with && args.len() != 3) {
            return None;
        }

        // Infer URL argument (should be Text)
        let url_ty = self.infer_expr(args[0]);
        self.unify(&url_ty, &Ty::Text, args[0].span);

        // If fetchWith, infer the options record
        if is_fetch_with {
            let _opts_ty = self.infer_expr(args[1]);
        }

        // Infer the constructor's record payload WITHOUT the `respond` field.
        let ctor_arg = args.last().unwrap();
        let record_arg = match &ctor_arg.node {
            ast::ExprKind::App { arg, .. } => arg.as_ref(),
            _ => ctor_arg,
        };
        let record_ty = self.infer_expr(record_arg);

        // Build the expected request fields from the route entry (exclude `respond`)
        if let Some(info) = self.constructors.get(ctor_name).cloned() {
            self.annotation_vars.clear();
            for p in &info.data_params {
                let v = self.fresh_var();
                self.annotation_vars.insert(p.clone(), v);
            }
            let field_tys: BTreeMap<String, Ty> = info
                .fields
                .iter()
                .filter(|(name, _)| name != "respond")
                .map(|(name, ty)| (name.clone(), self.ast_type_to_ty(ty)))
                .collect();
            let expected_record = Ty::Record(field_tys, None);
            self.unify(&record_ty, &expected_record, ctor_arg.span);
        }

        // Build the return type: IO {network} (Result {status, message} ResponseTy)
        // When response headers are declared, wrap as {body: ResponseTy, headers: {h: T, ...}}
        let resp_ty = self
            .fetch_response_types
            .get(ctor_name)
            .cloned();
        let raw_body_ty = match resp_ty {
            Some(ref ty) => self.ast_type_to_ty(ty),
            None => Ty::Text,
        };
        let ok_ty = match self.fetch_response_headers.get(ctor_name).cloned() {
            Some(ref hdr_fields) if !hdr_fields.is_empty() => {
                let headers_ty = Ty::Record(
                    hdr_fields
                        .iter()
                        .map(|f| (f.name.clone(), self.ast_type_to_ty(&f.value)))
                        .collect(),
                    None,
                );
                Ty::Record(
                    BTreeMap::from([
                        ("body".into(), raw_body_ty),
                        ("headers".into(), headers_ty),
                    ]),
                    None,
                )
            }
            _ => raw_body_ty,
        };
        let err_ty = Ty::Record(
            BTreeMap::from([
                ("message".into(), Ty::Text),
                ("status".into(), Ty::Int),
            ]),
            None,
        );
        let result_adt = Ty::Con("Result".into(), vec![err_ty, ok_ty]);
        Some(Ty::IO(
            BTreeSet::from([IoEffect::Network]),
            Box::new(result_adt),
        ))
    }

    fn infer_binop(
        &mut self,
        op: ast::BinOp,
        lhs: &ast::Expr,
        rhs: &ast::Expr,
        span: Span,
    ) -> Ty {
        let lhs_ty = self.infer_expr(lhs);
        let rhs_ty = self.infer_expr(rhs);

        match op {
            // Arithmetic: both same type, result same type
            ast::BinOp::Add
            | ast::BinOp::Sub
            | ast::BinOp::Mul
            | ast::BinOp::Div => {
                self.unify(&lhs_ty, &rhs_ty, span);
                lhs_ty
            }
            // Comparison: both same type, result Bool
            ast::BinOp::Eq
            | ast::BinOp::Neq
            | ast::BinOp::Lt
            | ast::BinOp::Gt
            | ast::BinOp::Le
            | ast::BinOp::Ge => {
                self.unify(&lhs_ty, &rhs_ty, span);
                Ty::Bool
            }
            // Boolean: both Bool, result Bool
            ast::BinOp::And | ast::BinOp::Or => {
                self.unify(&lhs_ty, &Ty::Bool, lhs.span);
                self.unify(&rhs_ty, &Ty::Bool, rhs.span);
                Ty::Bool
            }
            // Concat: both same type (Semigroup), result same type
            ast::BinOp::Concat => {
                self.unify(&lhs_ty, &rhs_ty, span);
                lhs_ty
            }
            // Pipe: a |> f  =  f a
            ast::BinOp::Pipe => {
                let result_ty = self.fresh();
                let fun_ty = Ty::Fun(
                    Box::new(lhs_ty),
                    Box::new(result_ty.clone()),
                );
                self.unify(&rhs_ty, &fun_ty, span);
                result_ty
            }
        }
    }

    fn literal_type(&self, lit: &ast::Literal) -> Ty {
        match lit {
            ast::Literal::Int(_) => Ty::Int,
            ast::Literal::Float(_) => Ty::Float,
            ast::Literal::Text(_) => Ty::Text,
            ast::Literal::Bytes(_) => Ty::Bytes,
            ast::Literal::Bool(_) => Ty::Bool,
        }
    }

    // ── Pattern checking ─────────────────────────────────────────

    fn check_pattern(&mut self, pat: &ast::Pat, expected: &Ty) {
        match &pat.node {
            ast::PatKind::Var(name) => {
                self.bind(name, Scheme::mono(expected.clone()));
                self.binding_types.push((pat.span, expected.clone()));
            }
            ast::PatKind::Wildcard => {}
            ast::PatKind::Constructor { name, payload } => {
                if let Some((_data_ty, record_ty)) =
                    self.instantiate_ctor(name, pat.span)
                {
                    // Create an open variant with just this constructor.
                    // This enables row-polymorphic variant matching: the
                    // scrutinee only needs to *contain* this constructor,
                    // not be the exact nominal ADT that defines it.
                    let row_var = self.fresh_var();
                    let mut ctors = BTreeMap::new();
                    ctors.insert(name.clone(), record_ty.clone());
                    let variant_ty =
                        Ty::Variant(ctors, Some(row_var));
                    self.unify(expected, &variant_ty, pat.span);
                    self.check_pattern(payload, &record_ty);
                } else {
                    self.error(
                        format!(
                            "unknown constructor '{}' in pattern",
                            name
                        ),
                        pat.span,
                    );
                }
            }
            ast::PatKind::Record(field_pats) => {
                let mut field_types = BTreeMap::new();
                for fp in field_pats {
                    let ft = self.fresh();
                    field_types.insert(fp.name.clone(), ft.clone());
                    if let Some(p) = &fp.pattern {
                        self.check_pattern(p, &ft);
                    } else {
                        // Punned: {name} → bind variable 'name' to field type
                        self.bind(&fp.name, Scheme::mono(ft.clone()));
                        self.binding_types.push((pat.span, ft));
                    }
                }
                let row_var = self.fresh_var();
                let record_ty = Ty::Record(field_types, Some(row_var));
                self.unify(expected, &record_ty, pat.span);
            }
            ast::PatKind::Lit(lit) => {
                let lit_ty = self.literal_type(lit);
                self.unify(expected, &lit_ty, pat.span);
            }
            ast::PatKind::List(pats) => {
                let elem_ty = self.fresh();
                for p in pats {
                    self.check_pattern(p, &elem_ty);
                }
                let list_ty = Ty::Relation(Box::new(elem_ty));
                self.unify(expected, &list_ty, pat.span);
            }
        }
    }

    // ── Exhaustiveness checking ────────────────────────────────

    /// Check that a case expression covers all constructors of the
    /// scrutinee's type.  Emits an error listing missing patterns when
    /// the match is non-exhaustive.
    fn check_exhaustiveness(
        &mut self,
        scrut_ty: &Ty,
        arms: &[ast::CaseArm],
        span: Span,
    ) {
        // Resolve the scrutinee type through substitution.
        let resolved = self.apply(scrut_ty);

        // If any arm has an unconditional catch-all pattern (wildcard or
        // variable) at the top level, the match is trivially exhaustive.
        let has_catchall = arms.iter().any(|arm| {
            matches!(
                &arm.pat.node,
                ast::PatKind::Wildcard | ast::PatKind::Var(_)
            )
        });
        if has_catchall {
            return;
        }

        match &resolved {
            Ty::Con(name, _) => {
                let data_info = match self.data_types.get(name) {
                    Some(info) => info.clone(),
                    None => return,
                };

                let covered: HashSet<&str> = arms
                    .iter()
                    .filter_map(|arm| match &arm.pat.node {
                        ast::PatKind::Constructor { name, .. } => {
                            Some(name.as_str())
                        }
                        _ => None,
                    })
                    .collect();

                let missing: Vec<&str> = data_info
                    .ctors
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .filter(|c| !covered.contains(c))
                    .collect();

                if !missing.is_empty() {
                    self.error(
                        format!(
                            "non-exhaustive pattern match — missing: {}",
                            missing.join(", "),
                        ),
                        span,
                    );
                }
            }
            Ty::Variant(ctors, row) => {
                let covered: HashSet<&str> = arms
                    .iter()
                    .filter_map(|arm| match &arm.pat.node {
                        ast::PatKind::Constructor { name, .. } => {
                            Some(name.as_str())
                        }
                        _ => None,
                    })
                    .collect();

                if let Some(rv) = row {
                    // Open variant — check if the covered constructors
                    // exhaust a known data type; if so, close the row.
                    let mut data_type_name = None;
                    let mut all_same = true;
                    for ctor_name in ctors.keys() {
                        if let Some(info) =
                            self.constructors.get(ctor_name)
                        {
                            match &data_type_name {
                                None => {
                                    data_type_name =
                                        Some(info.data_type.clone())
                                }
                                Some(dt)
                                    if *dt == info.data_type => {}
                                _ => {
                                    all_same = false;
                                    break;
                                }
                            }
                        }
                    }

                    if all_same {
                        if let Some(dt) = &data_type_name {
                            if let Some(dt_info) =
                                self.data_types.get(dt).cloned()
                            {
                                let all_ctors: HashSet<&str> = dt_info
                                    .ctors
                                    .iter()
                                    .map(|(n, _)| n.as_str())
                                    .collect();
                                if covered == all_ctors {
                                    // All constructors of a known type
                                    // are covered — close the row var.
                                    let rv = *rv;
                                    self.subst.insert(
                                        rv,
                                        Ty::Variant(
                                            BTreeMap::new(),
                                            None,
                                        ),
                                    );
                                    return;
                                }
                                // Some constructors of the type are
                                // missing — report which ones.
                                let missing: Vec<&str> = all_ctors
                                    .iter()
                                    .copied()
                                    .filter(|c| !covered.contains(c))
                                    .collect();
                                if !missing.is_empty() {
                                    self.error(
                                        format!(
                                            "non-exhaustive pattern \
                                             match — missing: {}",
                                            missing.join(", "),
                                        ),
                                        span,
                                    );
                                    return;
                                }
                            }
                        }
                    }

                    // Open variant with unknown remaining
                    // constructors — a wildcard is required.
                    self.error(
                        "non-exhaustive pattern match on open variant \
                         — add a wildcard `_` case"
                            .into(),
                        span,
                    );
                } else {
                    // Closed variant — check all constructors covered.
                    let all: HashSet<&str> =
                        ctors.keys().map(|s| s.as_str()).collect();
                    let missing: Vec<&str> = all
                        .iter()
                        .copied()
                        .filter(|c| !covered.contains(c))
                        .collect();
                    if !missing.is_empty() {
                        self.error(
                            format!(
                                "non-exhaustive pattern match \
                                 — missing: {}",
                                missing.join(", "),
                            ),
                            span,
                        );
                    }
                }
            }
            // Primitives (Int, Text, etc.) have infinite domains.
            _ => {}
        }
    }

    // ── Do-block inference ───────────────────────────────────────

    /// Pre-scan do-block statements to detect IO builtins (mirrors codegen's
    /// `is_io_do_block` / desugar's `expr_is_io`).
    fn stmt_has_io(stmts: &[ast::Stmt]) -> bool {
        fn expr_is_io(expr: &ast::Expr) -> bool {
            match &expr.node {
                ast::ExprKind::App { func, .. } => expr_is_io(func),
                ast::ExprKind::Var(name) => matches!(
                    name.as_str(),
                    "println" | "putLine" | "print" | "readLine" | "readFile"
                        | "writeFile" | "appendFile" | "fileExists" | "removeFile"
                        | "listDir" | "now" | "randomInt" | "randomFloat"
                        | "fetch" | "fetchWith" | "fork"
                ),
                _ => false,
            }
        }
        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Expr(expr) => {
                    if expr_is_io(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn infer_do(&mut self, stmts: &[ast::Stmt], _span: Span) -> Ty {
        self.push_scope();
        let mut yield_ty: Option<Ty> = None;
        let mut is_io = false;
        let mut has_relation_bind = false;
        let mut io_effects: BTreeSet<IoEffect> = BTreeSet::new();

        // Pre-scan: if any statement uses IO builtins, set in_io_do so that
        // `yield` expressions inside case/if branches produce IO types.
        let prev_in_io_do = self.in_io_do;
        if Self::stmt_has_io(stmts) {
            self.in_io_do = true;
        }

        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    let resolved = self.apply(&expr_ty);
                    let is_ctor_pat =
                        matches!(&pat.node, ast::PatKind::Constructor { .. });

                    if let Ty::IO(ref effects, ref inner) = resolved {
                        // IO bind: x <- ioAction
                        is_io = true;
                        io_effects.extend(effects.iter().cloned());
                        self.check_pattern(pat, inner);
                    } else if is_ctor_pat
                        && !matches!(&resolved, Ty::Relation(_) | Ty::Var(_))
                    {
                        // Value pattern match: `Constructor pat <- value_expr`
                        // Filters the enclosing iteration (skip if no match)
                        self.check_pattern(pat, &expr_ty);
                    } else {
                        // Normal relation bind
                        has_relation_bind = true;
                        let elem_ty = self.fresh();
                        self.unify(
                            &expr_ty,
                            &Ty::Relation(Box::new(elem_ty.clone())),
                            expr.span,
                        );
                        self.check_pattern(pat, &elem_ty);
                    }
                }
                ast::StmtKind::Let { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    self.check_pattern(pat, &expr_ty);
                }
                ast::StmtKind::Where { cond } => {
                    let cond_ty = self.infer_expr(cond);
                    self.unify(&cond_ty, &Ty::Bool, cond.span);
                }
                ast::StmtKind::GroupBy { key } => {
                    // Infer the key expression type (must be a record)
                    let _ = self.infer_expr(key);
                    // After groupBy, rebind all preceding Bind variables
                    // from T to [T] (they now represent groups)
                    for prev_stmt in stmts {
                        if std::ptr::eq(prev_stmt, stmt) {
                            break;
                        }
                        if let ast::StmtKind::Bind { pat, .. } = &prev_stmt.node {
                            if let ast::PatKind::Var(name) = &pat.node {
                                if let Some(scheme) = self.lookup(name).cloned() {
                                    let ty = self.instantiate(&scheme);
                                    self.bind(name, Scheme::mono(Ty::Relation(Box::new(ty))));
                                }
                            }
                        }
                    }
                }
                ast::StmtKind::Expr(expr) => {
                    if let ast::ExprKind::Yield(inner) = &expr.node {
                        let inner_ty = self.infer_expr(inner);
                        if let Some(ref yt) = yield_ty {
                            let yt = yt.clone();
                            self.unify(&yt, &inner_ty, expr.span);
                        } else {
                            yield_ty = Some(inner_ty);
                        }
                    } else {
                        let expr_ty = self.infer_expr(expr);
                        let resolved = self.apply(&expr_ty);
                        if let Ty::IO(ref effects, _) = resolved {
                            is_io = true;
                            io_effects.extend(effects.iter().cloned());
                        }
                    }
                }
            }
        }

        self.pop_scope();
        // Don't restore in_io_do to false — once IO is detected in any
        // do block within the current function, keep relaxed type checking
        // for IO/Relation/Response branch mismatches.
        if !self.in_io_do {
            self.in_io_do = prev_in_io_do;
        }

        // Determine block result type:
        // - IO if any statement is IO
        // - IO if we're inside an outer IO do block and this is NOT a
        //   relational comprehension (i.e., no `x <- relation` binds)
        // - Relation otherwise
        //
        // When there's no explicit yield, use the last bare expression's type
        // as the result (like Rust's implicit return), falling back to unit.
        let promote_to_io = is_io || (self.in_io_do && !has_relation_bind);
        if promote_to_io {
            let inner = yield_ty.unwrap_or_else(Ty::unit);
            Ty::IO(io_effects, Box::new(inner))
        } else {
            match yield_ty {
                Some(ty) => Ty::Relation(Box::new(ty)),
                None => Ty::Relation(Box::new(Ty::unit())),
            }
        }
    }

    // ── Declaration collection (phase 1) ─────────────────────────

    fn collect_types(&mut self, module: &ast::Module) {
        // First pass: type aliases
        for decl in &module.decls {
            if let ast::DeclKind::TypeAlias { name, params, ty } =
                &decl.node
            {
                if params.is_empty() {
                    self.annotation_vars.clear();
                    let resolved = self.ast_type_to_ty(ty);
                    self.aliases.insert(name.clone(), resolved);
                }
            }
        }

        // Second pass: data types and constructors
        for decl in &module.decls {
            if let ast::DeclKind::Data {
                name,
                params,
                constructors: ctors,
                ..
            } = &decl.node
            {
                let mut ctor_list = Vec::new();
                for ctor in ctors {
                    let fields: Vec<(String, ast::Type)> = ctor
                        .fields
                        .iter()
                        .map(|f| (f.name.clone(), f.value.clone()))
                        .collect();
                    self.constructors.insert(
                        ctor.name.clone(),
                        CtorInfo {
                            data_type: name.clone(),
                            data_params: params.clone(),
                            fields: fields.clone(),
                        },
                    );
                    ctor_list.push((ctor.name.clone(), fields));
                }

                // For single-variant data types, also register as alias
                if ctors.len() == 1 {
                    self.annotation_vars.clear();
                    for p in params {
                        let v = self.fresh_var();
                        self.annotation_vars.insert(p.clone(), v);
                    }
                    let field_tys: BTreeMap<String, Ty> = ctors[0]
                        .fields
                        .iter()
                        .map(|f| {
                            (
                                f.name.clone(),
                                self.ast_type_to_ty(&f.value),
                            )
                        })
                        .collect();
                    if params.is_empty() {
                        self.aliases.insert(
                            name.clone(),
                            Ty::Record(field_tys, None),
                        );
                    }
                }

                self.data_types.insert(
                    name.clone(),
                    DataInfo {
                        params: params.clone(),
                        ctors: ctor_list,
                    },
                );
            }
        }

        // Third pass: collect associated type names from traits
        for decl in &module.decls {
            if let ast::DeclKind::Trait { items, .. } = &decl.node {
                for item in items {
                    if let ast::TraitItem::AssociatedType { name, .. } =
                        item
                    {
                        self.assoc_type_names.insert(name.clone());
                    }
                }
            }
        }
    }

    // ── Source/view collection (phase 2) ──────────────────────────

    fn collect_sources(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Source { name, ty, .. } => {
                    self.annotation_vars.clear();
                    let resolved = self.ast_type_to_ty(ty);
                    self.source_types.insert(name.clone(), resolved);
                }
                ast::DeclKind::View { name, .. } => {
                    let elem = self.fresh();
                    self.source_types.insert(
                        name.clone(),
                        Ty::Relation(Box::new(elem)),
                    );
                    self.view_names.insert(name.clone());
                }
                ast::DeclKind::Derived { name, .. } => {
                    let ty = self.fresh();
                    self.derived_types.insert(name.clone(), ty);
                }
                _ => {}
            }
        }
    }

    // ── Impl collection (phase 2b) ─────────────────────────────

    fn collect_impls(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            if let ast::DeclKind::Impl {
                trait_name, args, ..
            } = &decl.node
            {
                // Extract type name from impl args
                // e.g. `impl Display Int where` → ("Display", "Int")
                // e.g. `impl Functor [] where` → ("Functor", "[]")
                if let Some(first_arg) = args.first() {
                    let type_name = Self::type_name_from_ast(first_arg);
                    if let Some(name) = type_name {
                        self.known_impls
                            .insert((trait_name.clone(), name));
                    }
                }
            }
        }
    }

    /// Extract a simple type name from an AST type node.
    fn type_name_from_ast(ty: &ast::Type) -> Option<String> {
        match &ty.node {
            ast::TypeKind::Named(name) => Some(name.clone()),
            ast::TypeKind::Relation(_) => Some("[]".into()),
            _ => None,
        }
    }

    /// Get the type name of a resolved Ty for impl lookup.
    fn type_name_of(&self, ty: &Ty) -> Option<String> {
        let resolved = self.apply(ty);
        match &resolved {
            Ty::Int => Some("Int".into()),
            Ty::Float => Some("Float".into()),
            Ty::Text => Some("Text".into()),
            Ty::Bool => Some("Bool".into()),
            Ty::Bytes => Some("Bytes".into()),
            Ty::Relation(_) => Some("[]".into()),
            Ty::TyCon(name) => Some(name.clone()),
            Ty::Con(name, _) => Some(name.clone()),
            _ => None,
        }
    }

    // ── Pre-registration (phase 3) ───────────────────────────────

    fn pre_register(&mut self, module: &ast::Module) {
        // Register built-in functions
        self.register_builtins();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, ty, .. } => {
                    if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        // Convert AST constraints to internal constraints
                        let mut constraints = Vec::new();
                        for c in &scheme.constraints {
                            for arg in &c.args {
                                if let ast::TypeKind::Var(var_name) = &arg.node {
                                    let v = self.annotation_var(var_name);
                                    constraints.push(TyConstraint {
                                        trait_name: c.trait_name.clone(),
                                        type_var: v,
                                    });
                                }
                            }
                        }
                        let ty = self.ast_type_to_ty(&scheme.ty);
                        let vars: Vec<TyVar> =
                            self.annotation_vars.values().copied().collect();
                        self.bind_top(
                            name,
                            Scheme::constrained(vars, constraints, ty),
                        );
                    } else {
                        let var = self.fresh();
                        self.bind_top(name, Scheme::mono(var));
                    }
                }
                ast::DeclKind::Trait {
                    name: trait_name,
                    items,
                    params,
                    ..
                } => {
                    self.register_trait_methods(trait_name, params, items);
                }
                ast::DeclKind::Route { entries, .. } => {
                    for entry in entries {
                        if let Some(ref resp_ty) = entry.response_ty {
                            self.fetch_response_types
                                .insert(entry.constructor.clone(), resp_ty.clone());
                        }
                        if !entry.response_headers.is_empty() {
                            self.fetch_response_headers
                                .insert(entry.constructor.clone(), entry.response_headers.clone());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn register_builtins(&mut self) {
        // Built-in ADT: data Maybe a = Nothing {} | Just {value: a}
        let dummy_span = Span::new(0, 0);
        self.constructors.insert(
            "Nothing".into(),
            CtorInfo {
                data_type: "Maybe".into(),
                data_params: vec!["a".into()],
                fields: vec![],
            },
        );
        self.constructors.insert(
            "Just".into(),
            CtorInfo {
                data_type: "Maybe".into(),
                data_params: vec!["a".into()],
                fields: vec![(
                    "value".into(),
                    ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span),
                )],
            },
        );
        self.data_types.insert(
            "Maybe".into(),
            DataInfo {
                params: vec!["a".into()],
                ctors: vec![
                    ("Nothing".into(), vec![]),
                    ("Just".into(), vec![("value".into(), ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span))]),
                ],
            },
        );

        // Built-in ADT: data Bool = True {} | False {}
        self.constructors.insert(
            "True".into(),
            CtorInfo {
                data_type: "Bool".into(),
                data_params: vec![],
                fields: vec![],
            },
        );
        self.constructors.insert(
            "False".into(),
            CtorInfo {
                data_type: "Bool".into(),
                data_params: vec![],
                fields: vec![],
            },
        );
        self.data_types.insert(
            "Bool".into(),
            DataInfo {
                params: vec![],
                ctors: vec![
                    ("True".into(), vec![]),
                    ("False".into(), vec![]),
                ],
            },
        );

        // Built-in ADT: data Result e a = Err {error: e} | Ok {value: a}
        self.constructors.insert(
            "Err".into(),
            CtorInfo {
                data_type: "Result".into(),
                data_params: vec!["e".into(), "a".into()],
                fields: vec![(
                    "error".into(),
                    ast::Type::new(ast::TypeKind::Var("e".into()), dummy_span),
                )],
            },
        );
        self.constructors.insert(
            "Ok".into(),
            CtorInfo {
                data_type: "Result".into(),
                data_params: vec!["e".into(), "a".into()],
                fields: vec![(
                    "value".into(),
                    ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span),
                )],
            },
        );
        self.data_types.insert(
            "Result".into(),
            DataInfo {
                params: vec!["e".into(), "a".into()],
                ctors: vec![
                    ("Err".into(), vec![("error".into(), ast::Type::new(ast::TypeKind::Var("e".into()), dummy_span))]),
                    ("Ok".into(), vec![("value".into(), ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span))]),
                ],
            },
        );

        // println : ∀a. a -> IO {console} {}
        let a = self.fresh_var();
        self.bind_top(
            "println",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), Box::new(Ty::unit()))),
            )),
        );

        // print : ∀a. a -> IO {console} {}
        let a = self.fresh_var();
        self.bind_top(
            "print",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), Box::new(Ty::unit()))),
            )),
        );

        // readLine : IO {console} Text
        self.bind_top("readLine", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Console]), Box::new(Ty::Text)),
        ));

        // show : ∀a. a -> Text
        let a = self.fresh_var();
        self.bind_top(
            "show",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );

        // union : ∀a. [a] -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "union",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // count : ∀a. [a] -> Int
        let a = self.fresh_var();
        self.bind_top(
            "count",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Int),
                ),
            ),
        );

        // putLine : ∀a. a -> IO {console} {} (alias for println)
        let a = self.fresh_var();
        self.bind_top(
            "putLine",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), Box::new(Ty::unit()))),
            )),
        );

        // now : IO {clock} Int
        self.bind_top("now", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Clock]), Box::new(Ty::Int)),
        ));

        // randomInt : Int -> IO {random} Int
        self.bind_top(
            "randomInt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Random]), Box::new(Ty::Int))),
            )),
        );

        // randomFloat : IO {random} Float
        self.bind_top("randomFloat", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Random]), Box::new(Ty::Float)),
        ));

        // fork : IO {} {} -> IO {} {}
        // Argument must be an IO action. Empty effect set unifies with any
        // concrete IO type since IO unification merges effect sets.
        self.bind_top(
            "fork",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::IO(BTreeSet::new(), Box::new(Ty::unit()))),
                Box::new(Ty::IO(BTreeSet::new(), Box::new(Ty::unit()))),
            )),
        );

        // retry : ∀a. a (polymorphic bottom — usable in any context inside atomic)
        let a = self.fresh_var();
        self.bind_top("retry", Scheme::poly(vec![a], Ty::Var(a)));

        // __bind, __yield, __empty are handled as special cases in infer_expr
        // with polymorphic HKT types: ∀m a b. (a -> m b) -> m a -> m b, etc.
        // This allows do-block desugaring to work with any monad, not just [].

        // listen : ∀a b. Int -> (a -> b) -> {}
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "listen",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Int),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                        Box::new(Ty::unit()),
                    )),
                ),
            ),
        );

        // fetch : ∀a b. Text -> a -> IO {network} (Result {status: Int, message: Text} b)
        // (also accepts 3-arg form with options record in the middle)
        // The response type `b` is resolved via special inference when the
        // second/third arg is a route constructor with a known response type.
        {
            let a = self.fresh_var();
            let b = self.fresh_var();
            let err_ty = Ty::Record(
                BTreeMap::from([
                    ("message".into(), Ty::Text),
                    ("status".into(), Ty::Int),
                ]),
                None,
            );
            let result_ty = Ty::Con("Result".into(), vec![err_ty.clone(), Ty::Var(b)]);
            let io_ty = Ty::IO(BTreeSet::from([IoEffect::Network]), Box::new(result_ty));
            self.bind_top(
                "fetch",
                Scheme::poly(
                    vec![a, b],
                    Ty::Fun(
                        Box::new(Ty::Text),
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(io_ty))),
                    ),
                ),
            );

            // fetchWith : ∀a b c. Text -> c -> a -> IO {network} (Result ... b)
            let a2 = self.fresh_var();
            let b2 = self.fresh_var();
            let c2 = self.fresh_var();
            let result_ty2 = Ty::Con("Result".into(), vec![err_ty, Ty::Var(b2)]);
            let io_ty2 = Ty::IO(BTreeSet::from([IoEffect::Network]), Box::new(result_ty2));
            self.bind_top(
                "fetchWith",
                Scheme::poly(
                    vec![a2, b2, c2],
                    Ty::Fun(
                        Box::new(Ty::Text),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Var(c2)),
                            Box::new(Ty::Fun(Box::new(Ty::Var(a2)), Box::new(io_ty2))),
                        )),
                    ),
                ),
            );
        }

        // ── Standard library ─────────────────────────────────────

        // filter : ∀a. (a -> Bool) -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "filter",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // map and fold are now trait methods (Functor.map, Foldable.fold)
        // registered via the prelude's trait declarations.

        // diff : ∀a. [a] -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "diff",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // inter : ∀a. [a] -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "inter",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // sum : ∀a b. (a -> b) -> [a] -> b
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "sum",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Var(b)),
                    )),
                ),
            ),
        );

        // avg : ∀a. (a -> Float) -> [a] -> Float
        let a = self.fresh_var();
        self.bind_top(
            "avg",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Float))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Float),
                    )),
                ),
            ),
        );

        // match : ∀a b. (a -> b) -> [b] -> [a]
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "match",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(b)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // single : ∀a. [a] -> Maybe a
        let a = self.fresh_var();
        self.bind_top(
            "single",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                ),
            ),
        );

        // toUpper : Text -> Text
        self.bind_top(
            "toUpper",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
        );

        // toLower : Text -> Text
        self.bind_top(
            "toLower",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
        );

        // take : Int -> Text -> Text
        self.bind_top(
            "take",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
            )),
        );

        // drop : Int -> Text -> Text
        self.bind_top(
            "drop",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
            )),
        );

        // length : Text -> Int
        self.bind_top(
            "length",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Int))),
        );

        // trim : Text -> Text
        self.bind_top(
            "trim",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
        );

        // contains : Text -> Text -> Bool
        self.bind_top(
            "contains",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bool))),
            )),
        );

        // reverse : Text -> Text
        self.bind_top(
            "reverse",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
        );

        // chars : Text -> [Text]
        self.bind_top(
            "chars",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Relation(Box::new(Ty::Text))),
            )),
        );

        // id : ∀a. a -> a
        let a = self.fresh_var();
        self.bind_top(
            "id",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(a)))),
        );

        // not : Bool -> Bool
        self.bind_top(
            "not",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bool), Box::new(Ty::Bool))),
        );

        // ── JSON standard library ─────────────────────────────────

        // toJson : ∀a. a -> Text
        let a = self.fresh_var();
        self.bind_top(
            "toJson",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );

        // parseJson : ∀a. Text -> a
        let a = self.fresh_var();
        self.bind_top(
            "parseJson",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Var(a)))),
        );

        // ── File system standard library ─────────────────────────

        // readFile : Text -> IO {fs} Text
        self.bind_top(
            "readFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::Text))),
            )),
        );

        // writeFile : Text -> Text -> IO {fs} {}
        self.bind_top(
            "writeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::unit()))),
                )),
            )),
        );

        // appendFile : Text -> Text -> IO {fs} {}
        self.bind_top(
            "appendFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::unit()))),
                )),
            )),
        );

        // fileExists : Text -> IO {fs} Bool
        self.bind_top(
            "fileExists",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::Bool))),
            )),
        );

        // removeFile : Text -> IO {fs} {}
        self.bind_top(
            "removeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::unit()))),
            )),
        );

        // listDir : Text -> IO {fs} [Text]
        self.bind_top(
            "listDir",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), Box::new(Ty::Relation(Box::new(Ty::Text))))),
            )),
        );

        // ── Bytes standard library ────────────────────────────────

        // bytesLength : Bytes -> Int
        self.bind_top(
            "bytesLength",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Int))),
        );

        // bytesSlice : Int -> Int -> Bytes -> Bytes
        self.bind_top(
            "bytesSlice",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::Fun(
                    Box::new(Ty::Int),
                    Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
                )),
            )),
        );

        // bytesConcat : Bytes -> Bytes -> Bytes
        self.bind_top(
            "bytesConcat",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
            )),
        );

        // textToBytes : Text -> Bytes
        self.bind_top(
            "textToBytes",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bytes))),
        );

        // bytesToText : Bytes -> Text
        self.bind_top(
            "bytesToText",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Text))),
        );

        // bytesToHex : Bytes -> Text
        self.bind_top(
            "bytesToHex",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Text))),
        );

        // bytesFromHex / hexDecode : Text -> Bytes
        self.bind_top(
            "bytesFromHex",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bytes))),
        );
        self.bind_top(
            "hexDecode",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bytes))),
        );

        // bytesGet : Int -> Bytes -> Int
        self.bind_top(
            "bytesGet",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Int))),
            )),
        );

        // Elliptic curve cryptography

        // generateKeyPair : {privateKey: Bytes, publicKey: Bytes}
        let key_pair_record = Ty::Record(
            BTreeMap::from([
                ("privateKey".into(), Ty::Bytes),
                ("publicKey".into(), Ty::Bytes),
            ]),
            None,
        );
        self.bind_top("generateKeyPair", Scheme::mono(key_pair_record.clone()));

        // generateSigningKeyPair : {privateKey: Bytes, publicKey: Bytes}
        self.bind_top("generateSigningKeyPair", Scheme::mono(key_pair_record));

        // encrypt : Bytes -> Bytes -> Bytes
        self.bind_top(
            "encrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
            )),
        );

        // decrypt : Bytes -> Bytes -> Bytes
        self.bind_top(
            "decrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
            )),
        );

        // sign : Bytes -> Bytes -> Bytes
        self.bind_top(
            "sign",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
            )),
        );

        // verify : Bytes -> Bytes -> Bytes -> Bool
        self.bind_top(
            "verify",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(
                    Box::new(Ty::Bytes),
                    Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bool))),
                )),
            )),
        );
    }

    fn register_trait_methods(
        &mut self,
        trait_name: &str,
        params: &[ast::TraitParam],
        items: &[ast::TraitItem],
    ) {
        // Record trait param names
        self.trait_params.insert(
            trait_name.to_string(),
            params.iter().map(|p| p.name.clone()).collect(),
        );

        for item in items {
            if let ast::TraitItem::Method { name, ty, .. } = item {
                // Skip default-body entries with placeholder types
                if let ast::TypeKind::Named(n) = &ty.ty.node {
                    if n == "_" {
                        continue;
                    }
                }
                self.annotation_vars.clear();
                // Register trait params as annotation vars
                for p in params {
                    self.annotation_var(&p.name);
                }
                let method_ty = self.ast_type_to_ty(&ty.ty);
                let vars: Vec<TyVar> =
                    self.annotation_vars.values().copied().collect();

                // Build constraints: each trait param must implement this trait
                let constraints: Vec<TyConstraint> = params
                    .iter()
                    .filter_map(|p| {
                        self.annotation_vars.get(&p.name).map(|&v| TyConstraint {
                            trait_name: trait_name.to_string(),
                            type_var: v,
                        })
                    })
                    .collect();

                // Record method → trait mapping
                self.trait_method_traits
                    .insert(name.clone(), trait_name.to_string());

                self.bind_top(
                    name,
                    Scheme::constrained(vars, constraints, method_ty),
                );
            }
        }
    }

    // ── Declaration inference (phase 4) ──────────────────────────

    fn infer_declarations(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body, ty, .. } => {
                    let expected =
                        self.lookup_instantiate(name).unwrap_or_else(|| {
                            self.fresh()
                        });
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);

                    // Remove the old monomorphic binding before
                    // generalizing, so its free variables don't block
                    // quantification.
                    if let Some(scope) = self.scopes.first_mut() {
                        scope.remove(name.as_str());
                    }

                    // If the function has explicit constraints in its
                    // annotation, rebuild the scheme from the annotation.
                    // (We already verified the body matches via unification.)
                    let has_constraints = ty
                        .as_ref()
                        .map_or(false, |ts| !ts.constraints.is_empty());
                    if has_constraints {
                        let ts = ty.as_ref().unwrap();
                        self.annotation_vars.clear();
                        let mut constraints = Vec::new();
                        for c in &ts.constraints {
                            for arg in &c.args {
                                if let ast::TypeKind::Var(var_name) =
                                    &arg.node
                                {
                                    let v = self.annotation_var(var_name);
                                    constraints.push(TyConstraint {
                                        trait_name: c.trait_name.clone(),
                                        type_var: v,
                                    });
                                }
                            }
                        }
                        let ann_ty = self.ast_type_to_ty(&ts.ty);
                        let vars: Vec<TyVar> = self
                            .annotation_vars
                            .values()
                            .copied()
                            .collect();
                        self.bind_top(
                            name,
                            Scheme::constrained(vars, constraints, ann_ty),
                        );
                    } else {
                        let applied = self.apply(&inferred);
                        let scheme = self.generalize(&applied);
                        self.bind_top(name, scheme);
                    }
                }
                ast::DeclKind::View { name, body, .. } => {
                    let expected =
                        self.source_types.get(name).cloned().unwrap_or_else(
                            || self.fresh(),
                        );
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);
                }
                ast::DeclKind::Derived { name, body, .. } => {
                    let expected = self
                        .derived_types
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| self.fresh());
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);
                }
                ast::DeclKind::Impl {
                    trait_name,
                    items,
                    ..
                } => {
                    self.check_impl_items(trait_name, items);
                }
                _ => {}
            }
        }
    }

    fn check_impl_items(
        &mut self,
        _trait_name: &str,
        items: &[ast::ImplItem],
    ) {
        for item in items {
            if let ast::ImplItem::Method {
                name, params, body, ..
            } = item
            {
                // Type-check each impl method body
                self.push_scope();
                let mut param_types = Vec::new();
                for param in params {
                    let t = self.fresh();
                    self.check_pattern(param, &t);
                    param_types.push(t);
                }
                let _ = self.infer_expr(body);
                self.pop_scope();

                // If there's a known trait method type, we could unify
                // but for now just check the body is well-typed
                let _ = name;
            }
        }
    }

    // ── Constraint checking ─────────────────────────────────────

    /// Check all deferred constraints after inference is complete.
    /// For each constraint (trait_name, type_var), resolve the type variable
    /// and verify that the concrete type has an implementation of the trait.
    fn check_constraints(&mut self) {
        let constraints = std::mem::take(&mut self.deferred_constraints);
        for dc in &constraints {
            let resolved = self.apply(&Ty::Var(dc.type_var));
            // Skip unresolved type variables (polymorphic — checked at use site)
            if matches!(resolved, Ty::Var(_)) {
                continue;
            }
            if let Some(type_name) = self.type_name_of(&resolved) {
                let key = (dc.trait_name.clone(), type_name.clone());
                if !self.known_impls.contains(&key) {
                    // Only emit error if the span is real (not dummy)
                    if dc.span.start != 0 || dc.span.end != 0 {
                        self.error(
                            format!(
                                "no implementation of trait '{}' for type '{}'",
                                dc.trait_name, type_name
                            ),
                            dc.span,
                        );
                    }
                }
            }
        }
    }

    // ── Error conversion ─────────────────────────────────────────

    fn to_diagnostics(&self) -> Vec<Diagnostic> {
        self.errors
            .iter()
            .map(|(msg, span)| {
                Diagnostic::error(msg.clone()).label(*span, msg.clone())
            })
            .collect()
    }

    // ── Type info extraction ────────────────────────────────────

    fn extract_type_info(&self) -> TypeInfo {
        let mut info = TypeInfo::new();

        if let Some(scope) = self.scopes.first() {
            for (name, scheme) in scope {
                if name.starts_with("__") {
                    continue;
                }
                info.insert(name.clone(), self.display_scheme(scheme));
            }
        }

        for (name, ty) in &self.source_types {
            let applied = self.apply(ty);
            info.insert(name.clone(), display_ty_clean(&applied, &var_map_for(&applied)));
        }

        for (name, ty) in &self.derived_types {
            let applied = self.apply(ty);
            info.insert(name.clone(), display_ty_clean(&applied, &var_map_for(&applied)));
        }

        info
    }

    fn extract_local_type_info(&self) -> LocalTypeInfo {
        let mut info = LocalTypeInfo::new();
        for (span, ty) in &self.binding_types {
            let applied = self.apply(ty);
            let var_map = var_map_for(&applied);
            info.insert(*span, display_ty_clean(&applied, &var_map));
        }
        info
    }

    fn display_scheme(&self, scheme: &Scheme) -> String {
        let applied = self.apply(&scheme.ty);
        let var_map = var_map_for(&applied);
        let ty_str = display_ty_clean(&applied, &var_map);

        if scheme.constraints.is_empty() {
            return ty_str;
        }

        let mut parts = Vec::new();
        for c in &scheme.constraints {
            let resolved = self.apply(&Ty::Var(c.type_var));
            if let Ty::Var(v) = resolved {
                let name = var_letter(var_map.get(&v).copied().unwrap_or(v as usize));
                parts.push(format!("{} {}", c.trait_name, name));
            }
        }

        if parts.is_empty() {
            ty_str
        } else {
            format!("{} => {}", parts.join(" => "), ty_str)
        }
    }
}

// ── Standalone type display (for export, no subst lookups) ────────

fn var_map_for(ty: &Ty) -> HashMap<TyVar, usize> {
    let mut vars = Vec::new();
    collect_vars_ordered(ty, &mut vars);
    vars.iter()
        .enumerate()
        .map(|(i, &v)| (v, i))
        .collect()
}

fn collect_vars_ordered(ty: &Ty, out: &mut Vec<TyVar>) {
    match ty {
        Ty::Var(v) => {
            if !out.contains(v) {
                out.push(*v);
            }
        }
        Ty::Fun(p, r) => {
            collect_vars_ordered(p, out);
            collect_vars_ordered(r, out);
        }
        Ty::Record(fields, row) => {
            for t in fields.values() {
                collect_vars_ordered(t, out);
            }
            if let Some(rv) = row {
                if !out.contains(rv) {
                    out.push(*rv);
                }
            }
        }
        Ty::Relation(inner) => collect_vars_ordered(inner, out),
        Ty::Con(_, args) => {
            for a in args {
                collect_vars_ordered(a, out);
            }
        }
        Ty::Variant(ctors, row) => {
            for t in ctors.values() {
                collect_vars_ordered(t, out);
            }
            if let Some(rv) = row {
                if !out.contains(rv) {
                    out.push(*rv);
                }
            }
        }
        Ty::App(f, a) => {
            collect_vars_ordered(f, out);
            collect_vars_ordered(a, out);
        }
        Ty::IO(_, inner) => collect_vars_ordered(inner, out),
        _ => {}
    }
}

fn var_letter(idx: usize) -> String {
    if idx < 26 {
        format!("{}", (b'a' + idx as u8) as char)
    } else {
        format!("t{}", idx)
    }
}

fn display_ty_clean(ty: &Ty, names: &HashMap<TyVar, usize>) -> String {
    display_ty_clean_inner(ty, names, false)
}

fn display_ty_clean_inner(ty: &Ty, names: &HashMap<TyVar, usize>, in_fun: bool) -> String {
    match ty {
        Ty::Var(v) => var_letter(names.get(v).copied().unwrap_or(*v as usize)),
        Ty::Int => "Int".into(),
        Ty::Float => "Float".into(),
        Ty::Text => "Text".into(),
        Ty::Bool => "Bool".into(),
        Ty::Bytes => "Bytes".into(),
        Ty::Fun(p, r) => {
            let s = format!(
                "{} -> {}",
                display_ty_clean_inner(p, names, true),
                display_ty_clean_inner(r, names, false)
            );
            if in_fun {
                format!("({})", s)
            } else {
                s
            }
        }
        Ty::Record(fields, row) => {
            if fields.is_empty() && row.is_none() {
                return "{}".into();
            }
            let mut parts: Vec<String> = fields
                .iter()
                .map(|(n, t)| format!("{}: {}", n, display_ty_clean(t, names)))
                .collect();
            if let Some(rv) = row {
                parts.push(format!("| {}", var_letter(names.get(rv).copied().unwrap_or(*rv as usize))));
            }
            format!("{{{}}}", parts.join(", "))
        }
        Ty::Relation(inner) => format!("[{}]", display_ty_clean(inner, names)),
        Ty::Con(name, args) => {
            if args.is_empty() {
                name.clone()
            } else {
                let args_str: Vec<String> =
                    args.iter().map(|a| display_ty_clean(a, names)).collect();
                format!("{} {}", name, args_str.join(" "))
            }
        }
        Ty::Variant(ctors, row) => {
            let mut parts: Vec<String> = ctors
                .iter()
                .map(|(name, ft)| format!("{} {}", name, display_ty_clean(ft, names)))
                .collect();
            if let Some(rv) = row {
                parts.push(var_letter(names.get(rv).copied().unwrap_or(*rv as usize)));
            }
            format!("<{}>", parts.join(" | "))
        }
        Ty::TyCon(name) => name.clone(),
        Ty::App(f, a) => format!(
            "({} {})",
            display_ty_clean(f, names),
            display_ty_clean(a, names)
        ),
        Ty::IO(effects, inner) => {
            let effects_str = if effects.is_empty() {
                String::new()
            } else {
                let eff_names: Vec<&str> = effects
                    .iter()
                    .map(|e| match e {
                        IoEffect::Console => "console",
                        IoEffect::Fs => "fs",
                        IoEffect::Network => "network",
                        IoEffect::Clock => "clock",
                        IoEffect::Random => "random",
                    })
                    .collect();
                format!(" {{{}}}", eff_names.join(", "))
            };
            format!("IO{} {}", effects_str, display_ty_clean(inner, names))
        }
        Ty::Error => "<error>".into(),
    }
}

// ── Public API ────────────────────────────────────────────────────

/// Run type inference on a parsed module. Returns diagnostics,
/// resolved monad info for desugared do-blocks, and inferred type info
/// mapping declaration names to their display type strings.
pub fn check(module: &ast::Module) -> (Vec<Diagnostic>, MonadInfo, TypeInfo, LocalTypeInfo) {
    let mut infer = Infer::new();

    // Phase 1: Collect type aliases, data types, constructors
    infer.collect_types(module);

    // Phase 2: Register source/view/derived relation types
    infer.collect_sources(module);

    // Phase 2b: Collect known trait implementations
    infer.collect_impls(module);

    // Phase 2c: Register builtin [] impls for HKT traits
    for trait_name in &["Functor", "Applicative", "Monad", "Alternative", "Foldable"] {
        infer
            .known_impls
            .insert((trait_name.to_string(), "[]".to_string()));
    }

    // Phase 3: Pre-register top-level names (builtins, functions, trait methods)
    infer.pre_register(module);

    // Phase 4: Infer all declaration bodies
    infer.infer_declarations(module);

    // Phase 4b: Check deferred trait constraints
    infer.check_constraints();

    // Phase 4c: Compress substitution chains for faster resolution
    infer.compress_substitution();

    // Phase 5: Resolve monad types from desugared do-blocks
    let mut monad_info = MonadInfo::new();
    for (span, m_var) in &infer.monad_vars {
        let resolved = infer.apply(&Ty::Var(*m_var));
        let kind = match &resolved {
            Ty::TyCon(name) if name == "[]" => MonadKind::Relation,
            Ty::TyCon(name) => MonadKind::Adt(name.clone()),
            Ty::Relation(_) => MonadKind::Relation,
            Ty::IO(_, _) => MonadKind::IO,
            // Partially applied type constructor, e.g. Result e (App(TyCon("Result"), e))
            Ty::App(f, _) => match f.as_ref() {
                Ty::TyCon(name) => MonadKind::Adt(name.clone()),
                _ => MonadKind::Relation,
            },
            // Saturated ADT used as monad, e.g. Con("Result", [Text]) from Result Text a
            Ty::Con(name, _) => MonadKind::Adt(name.clone()),
            _ => MonadKind::Relation, // default unresolved to Relation
        };
        monad_info.insert(*span, kind);
    }

    let type_info = infer.extract_type_info();
    let local_type_info = infer.extract_local_type_info();

    (infer.to_diagnostics(), monad_info, type_info, local_type_info)
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> ast::Module {
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (module, _) = parser.parse_module();
        module
    }

    fn check_src(src: &str) -> Vec<Diagnostic> {
        let (diags, _monad_info, _type_info, _local_types) = check(&parse(src));
        diags
    }

    fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
        diags.iter().any(|d| d.message.contains(needle))
    }

    #[test]
    fn literal_arithmetic() {
        assert!(check_src("main = 1 + 2").is_empty());
    }

    #[test]
    fn arithmetic_type_mismatch() {
        let diags = check_src("main = 1 + \"hello\"");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn boolean_ops_require_bool() {
        let diags = check_src("main = 1 && 2");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn concat_is_polymorphic() {
        // ++ is now Semigroup: both sides must agree, but any type is allowed
        assert!(check_src("main = \"a\" ++ \"b\"").is_empty());
        assert!(check_src("main = [1, 2] ++ [3, 4]").is_empty());
        let diags = check_src("main = \"a\" ++ 1");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn if_branches_must_agree() {
        assert!(check_src(
            "main = if 1 == 1 then 42 else 0"
        )
        .is_empty());
        let diags = check_src(
            "main = if 1 == 1 then 42 else \"hello\"",
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn field_access_on_record() {
        assert!(check_src("main = {name: \"Alice\"}.name").is_empty());
    }

    #[test]
    fn field_access_nonexistent() {
        let diags = check_src("main = {name: \"Alice\"}.age");
        assert!(has_error(&diags, "unexpected fields"));
    }

    #[test]
    fn lambda_inference() {
        assert!(check_src("f = \\x -> x + 1\nmain = f 42").is_empty());
    }

    #[test]
    fn let_polymorphism() {
        // id should work with both Int and Text
        assert!(check_src(
            "id = \\x -> x\nmain = do\n  println (id 42)\n  println (id \"hello\")\n  yield {}"
        ).is_empty());
    }

    #[test]
    fn recursive_function() {
        assert!(check_src(
            "fac = \\n -> if n == 0 then 1 else n * fac (n - 1)\nmain = fac 5"
        ).is_empty());
    }

    #[test]
    fn row_polymorphism() {
        // getName should work on any record with a name field
        assert!(check_src(
            "getName = \\r -> r.name\nmain = do\n  let x = getName {name: \"A\", age: 1}\n  let y = getName {name: \"B\", email: \"b\"}\n  yield {}"
        ).is_empty());
    }

    #[test]
    fn data_type_constructor() {
        assert!(check_src(
            "data Shape = Circle {radius: Int} | Rect {w: Int, h: Int}\nmain = Circle {radius: 5}"
        ).is_empty());
    }

    #[test]
    fn case_expression() {
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\nf = \\s -> case s of\n  Circle {r} -> r\n  Rect {w, h} -> w * h\nmain = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn case_branch_type_mismatch() {
        let diags = check_src(
            "data AB = A {} | B {}\nf = \\x -> case x of\n  A {} -> 42\n  B {} -> \"hello\"\nmain = f (A {})"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn do_block_bind() {
        assert!(check_src(
            "*people : [{name: Text, age: Int}]\nmain = do\n  p <- *people\n  yield p.name"
        ).is_empty());
    }

    #[test]
    fn do_block_where_bool() {
        let diags = check_src(
            "*people : [{name: Text}]\nmain = do\n  p <- *people\n  where p.name\n  yield p"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn undefined_variable() {
        let diags = check_src("main = undefined_var");
        assert!(has_error(&diags, "undefined variable"));
    }

    #[test]
    fn pipe_operator() {
        assert!(check_src(
            "inc = \\x -> x + 1\nmain = 5 |> inc"
        ).is_empty());
    }

    #[test]
    fn set_type_matches_source() {
        assert!(check_src(
            "*nums : [Int]\nmain = set *nums = [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn set_type_mismatch() {
        let diags = check_src(
            "*nums : [Int]\nmain = set *nums = [\"a\", \"b\"]"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn union_builtin() {
        assert!(check_src(
            "main = union [1, 2] [3, 4]"
        ).is_empty());
    }

    #[test]
    fn count_builtin() {
        assert!(check_src("main = count [1, 2, 3]").is_empty());
    }

    #[test]
    fn trait_method() {
        assert!(check_src(
            "trait Show a where\n  show_ : a -> Text\nimpl Show Int where\n  show_ n = \"int\"\nmain = show_ 42"
        ).is_empty());
    }

    #[test]
    fn record_update() {
        assert!(check_src(
            "main = {name: \"Alice\", age: 30} |> \\r -> {r | age: r.age + 1}"
        ).is_empty());
    }

    #[test]
    fn higher_order_function() {
        assert!(check_src(
            "apply = \\f x -> f x\nmain = apply (\\x -> x + 1) 5"
        ).is_empty());
    }

    #[test]
    fn now_builtin() {
        // now should type as IO {clock} Int — cannot directly add to Int
        assert!(!check_src("main = now + 1000").is_empty());
        // But using in IO do-block works:
        assert!(check_src("main = do\n  t <- now\n  println t").is_empty());
    }

    #[test]
    fn temporal_at_expression() {
        // @(timestamp) expects Int, now returns IO — need do-block to unwrap
        assert!(check_src(
            "*people : [{name: Text, age: Int}]\nmain = do\n  t <- now\n  yield (*people @(t))"
        ).is_empty());
    }

    #[test]
    fn temporal_at_requires_int_time() {
        // time expression must be Int, not Text
        let diags = check_src(
            "*people : [{name: Text}]\nmain = *people @(\"yesterday\")"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    // ── Exhaustiveness checking ─────────────────────────────────

    #[test]
    fn exhaustive_case_all_constructors() {
        // Covering all constructors is fine.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  Rect {w, h} -> w * h\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn exhaustive_case_wildcard() {
        // A wildcard catch-all makes any match exhaustive.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  _ -> 0\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn exhaustive_case_var_catchall() {
        // A variable catch-all also makes it exhaustive.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  other -> 0\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn non_exhaustive_case_missing_constructor() {
        // Missing Rect — should produce an error.
        let diags = check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n\
             main = f (Circle {r: 5})"
        );
        assert!(has_error(&diags, "non-exhaustive"));
        assert!(has_error(&diags, "Rect"));
    }

    #[test]
    fn non_exhaustive_case_missing_multiple() {
        // Missing two out of three constructors.
        let diags = check_src(
            "data Color = Red {} | Green {} | Blue {}\n\
             f = \\c -> case c of\n  Red {} -> 1\n\
             main = f (Red {})"
        );
        assert!(has_error(&diags, "non-exhaustive"));
        assert!(has_error(&diags, "Green"));
        assert!(has_error(&diags, "Blue"));
    }

    #[test]
    fn exhaustive_case_single_constructor() {
        // Data type with one constructor — a single arm is exhaustive.
        assert!(check_src(
            "data Wrapper = Wrap {val: Int}\n\
             f = \\w -> case w of\n  Wrap {val} -> val\n\
             main = f (Wrap {val: 42})"
        ).is_empty());
    }

    #[test]
    fn case_on_primitive_skips_exhaustiveness() {
        // Matching on Int — no exhaustiveness check (infinite domain).
        assert!(check_src(
            "f = \\n -> case n of\n  0 -> 1\n  1 -> 2\n\
             main = f 0"
        ).is_empty());
    }

    // ── Higher-kinded types ───────────────────────────────────────

    #[test]
    fn hkt_trait_method_with_relation() {
        // map : (a -> b) -> f a -> f b, used with [] (relation)
        assert!(check_src(
            "trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = fmap (\\x -> x + 1) [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn hkt_trait_method_type_propagation() {
        // The result of fmap should have the correct element type
        assert!(check_src(
            "trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = do\n\
             \x20 x <- fmap (\\n -> show n) [1, 2, 3]\n\
             \x20 yield (x ++ \"!\")"
        ).is_empty());
    }

    #[test]
    fn hkt_trait_method_type_error() {
        // fmap expects a function, not a plain value
        let diags = check_src(
            "trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = fmap 42 [1, 2, 3]"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn hkt_with_adt() {
        // HKT trait with an ADT type constructor
        assert!(check_src(
            "data Maybe a = Nothing {} | Just {value: a}\n\
             trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor Maybe where\n\
             \x20 fmap f m = case m of\n\
             \x20   Nothing {} -> Nothing {}\n\
             \x20   Just {value} -> Just {value: f value}\n\
             main = fmap (\\x -> x + 1) (Just {value: 42})"
        ).is_empty());
    }

    #[test]
    fn hkt_multiple_methods() {
        // Trait with multiple HK-parameterized methods
        assert!(check_src(
            "trait Container (f : Type -> Type) where\n\
             \x20 wrap : a -> f a\n\
             \x20 unwrap : f a -> a\n\
             impl Container [] where\n\
             \x20 wrap x = [x]\n\
             \x20 unwrap rel = do\n\
             \x20   x <- rel\n\
             \x20   yield x\n\
             main = wrap 42"
        ).is_empty());
    }

    #[test]
    fn hkt_bare_relation_constructor() {
        // [] used as a bare type in impl should work
        assert!(check_src(
            "trait Empty (f : Type -> Type) where\n\
             \x20 empty : f a\n\
             impl Empty [] where\n\
             \x20 empty = []\n\
             main = empty"
        ).is_empty());
    }

    #[test]
    fn hkt_tycon_unifies_with_relation() {
        // When HK var is solved to [], App([], a) should equal [a]
        assert!(check_src(
            "*nums : [Int]\n\
             trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = fmap (\\x -> x + 1) *nums"
        ).is_empty());
    }

    #[test]
    fn hkt_multi_arg_type_application() {
        // Multi-arg type constructors in annotations should work
        assert!(check_src(
            "data Pair a b = MkPair {fst: a, snd: b}\n\
             main = MkPair {fst: 1, snd: \"hello\"}"
        ).is_empty());
    }

    // ── Trait bounds ────────────────────────────────────────────────

    #[test]
    fn explicit_trait_bound_satisfied() {
        // Calling a function with explicit trait bounds on a type that has an impl
        assert!(check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             printAll : Display a => [a] -> [Text]\n\
             printAll = \\rel -> do\n\
             \x20 r <- rel\n\
             \x20 yield (display r)\n\
             main = printAll [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn explicit_trait_bound_unsatisfied() {
        // Calling a function with trait bounds on a type without an impl
        let diags = check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             printAll : Display a => [a] -> [Text]\n\
             printAll = \\rel -> do\n\
             \x20 r <- rel\n\
             \x20 yield (display r)\n\
             main = printAll [\"hello\"]"
        );
        assert!(has_error(&diags, "no implementation of trait 'Display' for type 'Text'"));
    }

    #[test]
    fn trait_method_constraint_satisfied() {
        // Using a trait method directly with a type that has an impl
        assert!(check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             main = display 42"
        ).is_empty());
    }

    #[test]
    fn trait_method_constraint_unsatisfied() {
        // Using a trait method with a type that doesn't have an impl
        let diags = check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             main = display \"hello\""
        );
        assert!(has_error(&diags, "no implementation of trait 'Display' for type 'Text'"));
    }

    #[test]
    fn multiple_trait_bounds() {
        // Multiple constraints: Display a => Eq a => ...
        assert!(check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             trait Eq_ a where\n\
             \x20 eq : a -> a -> Bool\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             impl Eq_ Int where\n\
             \x20 eq a b = a == b\n\
             showAndCompare : Display a => Eq_ a => a -> a -> Text\n\
             showAndCompare = \\x y -> if eq x y then display x else display y\n\
             main = showAndCompare 1 2"
        ).is_empty());
    }

    #[test]
    fn multiple_trait_bounds_one_missing() {
        // One of multiple bounds is unsatisfied
        let diags = check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             trait Eq_ a where\n\
             \x20 eq : a -> a -> Bool\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             showAndCompare : Display a => Eq_ a => a -> a -> Text\n\
             showAndCompare = \\x y -> display x\n\
             main = showAndCompare 1 2"
        );
        // Eq_ Int is missing
        assert!(has_error(&diags, "no implementation of trait 'Eq_' for type 'Int'"));
    }

    #[test]
    fn trait_bound_polymorphic_passthrough() {
        // When a constrained function is called with a still-polymorphic
        // variable, the constraint should not trigger (it's checked later)
        assert!(check_src(
            "trait Display a where\n\
             \x20 display : a -> Text\n\
             impl Display Int where\n\
             \x20 display n = show n\n\
             printAll : Display a => [a] -> [Text]\n\
             printAll = \\rel -> do\n\
             \x20 r <- rel\n\
             \x20 yield (display r)\n\
             main = println 42"
        ).is_empty());
    }

    #[test]
    fn hkt_trait_bound_satisfied() {
        // HKT trait method call with [] should succeed (impl exists)
        assert!(check_src(
            "trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = fmap (\\x -> x + 1) [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn hkt_trait_bound_unsatisfied() {
        // HKT trait method call with a type that doesn't have an impl
        let diags = check_src(
            "data Box a = MkBox {value: a}\n\
             trait Functor (f : Type -> Type) where\n\
             \x20 fmap : (a -> b) -> f a -> f b\n\
             impl Functor [] where\n\
             \x20 fmap f rel = do\n\
             \x20   x <- rel\n\
             \x20   yield (f x)\n\
             main = fmap (\\x -> x + 1) (MkBox {value: 42})"
        );
        assert!(has_error(&diags, "no implementation of trait 'Functor'"));
    }

    // ── Row-polymorphic variants ─────────────────────────────────

    #[test]
    fn closed_variant_unifies_with_matching_adt() {
        let diags = check_src(
            "data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}\n\
             f : <Circle {radius: Float} | Rect {w: Float, h: Float}> -> Float\n\
             f = \\s -> 1.0\n\
             main = f (Circle {radius: 3.0})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn open_variant_accepts_adt_with_extra_constructors() {
        let diags = check_src(
            "data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}\n\
             f : <Circle {radius: Float} | r> -> Float\n\
             f = \\s -> 1.0\n\
             main = f (Circle {radius: 3.0})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn variant_missing_constructor_error() {
        let diags = check_src(
            "data Color = Red {} | Blue {}\n\
             f : <Red {} | Blue {} | Green {}> -> Int\n\
             f = \\c -> 1\n\
             main = f (Red {})",
        );
        assert!(has_error(&diags, "variant constructors don't match"));
    }

    #[test]
    fn open_variant_polymorphic_function() {
        let diags = check_src(
            "data Status = Open {} | Closed {} | InProgress {assignee: Text}\n\
             isOpen : <Open {} | r> -> Int\n\
             isOpen = \\s -> 1\n\
             main = isOpen (Open {})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn case_pattern_infers_open_variant() {
        // Matching one constructor with wildcard should accept any ADT
        // that has that constructor — row-polymorphic variant inference.
        let diags = check_src(
            "data Status = Open {} | Closed {}\n\
             data TaskStatus = Open {} | Done {}\n\
             f = \\s -> case s of\n\
             \x20 Open {} -> 1\n\
             \x20 _ -> 0\n\
             main = f (Open {})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn case_all_constructors_closes_variant() {
        // Matching all constructors without wildcard should close the
        // variant and the exhaustiveness check should pass.
        let diags = check_src(
            "data Shape = Circle {r: Float} | Rect {w: Float, h: Float}\n\
             area = \\s -> case s of\n\
             \x20 Circle {r} -> r\n\
             \x20 Rect {w, h} -> w * h\n\
             main = area (Circle {r: 3.0})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn open_variant_requires_wildcard() {
        // Partial match without wildcard on an open variant.
        let diags = check_src(
            "data Color = Red {} | Green {} | Blue {}\n\
             f = \\c -> case c of\n\
             \x20 Red {} -> 1\n\
             \x20 Green {} -> 2\n\
             main = f (Red {})",
        );
        assert!(has_error(&diags, "non-exhaustive"));
        assert!(has_error(&diags, "Blue"));
    }

    #[test]
    fn do_bind_pattern_infers_open_variant() {
        // Constructor pattern in do-bind should work with open variants.
        let diags = check_src(
            "data Status = Open {} | Closed {} | InProgress {assignee: Text}\n\
             *items : [{name: Text, status: Status}]\n\
             &openItems = do\n\
             \x20 i <- *items\n\
             \x20 Open {} <- i.status\n\
             \x20 yield {name: i.name}\n\
             main = &openItems",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }

    #[test]
    fn open_variant_applied_to_multiple_adts() {
        // A function with an inferred open variant type should accept
        // values from different ADTs that share the matched constructor.
        let diags = check_src(
            "data AB = A {} | B {}\n\
             data AC = A {} | C {}\n\
             hasA = \\x -> case x of\n\
             \x20 A {} -> 1\n\
             \x20 _ -> 0\n\
             main = hasA (A {})",
        );
        assert!(diags.is_empty(), "unexpected errors: {:?}", diags);
    }
}

/// Extract the constructor name from a `fetch url (Ctor {..})` or
/// `fetch url opts (Ctor {..})` expression tree.  Returns `None` if
/// the expression is not a fetch call with a constructor argument.
fn fetch_ctor_name(expr: &ast::Expr) -> Option<&str> {
    let ast::ExprKind::App { func, arg } = &expr.node else {
        return None;
    };
    // The last argument should be a constructor application
    let ctor_name = match &arg.node {
        ast::ExprKind::App { func: ctor_func, .. } => {
            if let ast::ExprKind::Constructor(name) = &ctor_func.node {
                name.as_str()
            } else {
                return None;
            }
        }
        ast::ExprKind::Constructor(name) => name.as_str(),
        _ => return None,
    };
    // Walk the function chain to find Var("fetch") or Var("fetchWith") at the root
    let mut f = func.as_ref();
    loop {
        match &f.node {
            ast::ExprKind::Var(name) if name == "fetch" || name == "fetchWith" => {
                return Some(ctor_name);
            }
            ast::ExprKind::App { func: inner, .. } => f = inner.as_ref(),
            _ => return None,
        }
    }
}

/// Uncurry a fetch application into its root function and arguments.
fn uncurry_fetch<'a>(expr: &'a ast::Expr) -> (&'a ast::Expr, Vec<&'a ast::Expr>) {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            let (f, mut args) = uncurry_fetch(func);
            args.push(arg);
            (f, args)
        }
        _ => (expr, Vec::new()),
    }
}
