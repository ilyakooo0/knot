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

/// Maps `refine` expression spans to their resolved refined type name.
pub type RefineTargets = HashMap<Span, String>;

/// Refined type info exported for codegen: type_name → predicate expression.
pub type RefinedTypeInfoMap = HashMap<String, knot::ast::Expr>;

/// Maps declaration names to their inferred type display strings.
pub type TypeInfo = HashMap<String, String>;

/// Maps binding spans (local variables, params, patterns) to their inferred type strings.
pub type LocalTypeInfo = HashMap<Span, String>;

// ── Units of measure ──────────────────────────────────────────────

type UnitVar = u32;

/// Normalized unit: a product of base-unit powers, e.g. m^1 * s^-2.
/// Dimensionless = empty map.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnitTy {
    /// base_unit_name -> exponent
    bases: BTreeMap<String, i32>,
    /// Unit variable for polymorphism
    var: Option<UnitVar>,
}

#[allow(dead_code)]
impl UnitTy {
    fn dimensionless() -> Self {
        UnitTy { bases: BTreeMap::new(), var: None }
    }

    fn named(name: &str) -> Self {
        let mut bases = BTreeMap::new();
        bases.insert(name.to_string(), 1);
        UnitTy { bases, var: None }
    }

    fn var(v: UnitVar) -> Self {
        UnitTy { bases: BTreeMap::new(), var: Some(v) }
    }

    fn is_dimensionless(&self) -> bool {
        self.bases.is_empty() && self.var.is_none()
    }

    fn normalize(&mut self) {
        self.bases.retain(|_, exp| *exp != 0);
    }

    fn mul(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            *result.bases.entry(name.clone()).or_insert(0) += exp;
        }
        // If both have variables, we can't compose them simply — this would
        // be caught as an error during unification.
        if result.var.is_none() {
            result.var = other.var;
        }
        result.normalize();
        result
    }

    fn div(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            *result.bases.entry(name.clone()).or_insert(0) -= exp;
        }
        if result.var.is_none() {
            result.var = other.var.map(|_| {
                // Dividing by a unit variable requires negation — not representable
                // in our simple model. We'll leave var as None and let unification
                // handle the error if needed.
                0 // placeholder
            });
            // Actually, just leave var as None for division by variable
            result.var = None;
        }
        result.normalize();
        result
    }

    fn pow(&self, n: i32) -> UnitTy {
        let mut result = self.clone();
        for exp in result.bases.values_mut() {
            *exp *= n;
        }
        result.normalize();
        result
    }

    /// Canonical display string for unit, e.g. "kg*m/s^2"
    fn display(&self) -> String {
        if self.is_dimensionless() {
            return "1".to_string();
        }
        let mut num_parts = Vec::new();
        let mut den_parts = Vec::new();
        for (name, exp) in &self.bases {
            if *exp > 0 {
                if *exp == 1 {
                    num_parts.push(name.clone());
                } else {
                    num_parts.push(format!("{}^{}", name, exp));
                }
            } else if *exp < 0 {
                if *exp == -1 {
                    den_parts.push(name.clone());
                } else {
                    den_parts.push(format!("{}^{}", name, -exp));
                }
            }
        }
        if let Some(v) = self.var {
            num_parts.push(format!("?u{}", v));
        }
        if den_parts.is_empty() {
            if num_parts.is_empty() {
                "1".to_string()
            } else {
                num_parts.join("*")
            }
        } else if num_parts.is_empty() {
            format!("1/{}", den_parts.join("*"))
        } else {
            format!("{}/{}", num_parts.join("*"), den_parts.join("*"))
        }
    }
}

// ── Internal type representation ──────────────────────────────────

type TyVar = u32;

/// Internal type representation for unification-based inference.
#[derive(Debug, Clone, PartialEq)]
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
    /// Int with unit of measure (compile-time only).
    IntUnit(UnitTy),
    /// Float with unit of measure (compile-time only).
    FloatUnit(UnitTy),
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
    span: Span,
}

/// Polymorphic type scheme: ∀ vars. constraints => ty
#[derive(Debug, Clone)]
struct Scheme {
    vars: Vec<TyVar>,
    unit_vars: Vec<UnitVar>,
    constraints: Vec<TyConstraint>,
    ty: Ty,
}

impl Scheme {
    fn mono(ty: Ty) -> Self {
        Scheme {
            vars: vec![],
            unit_vars: vec![],
            constraints: vec![],
            ty,
        }
    }

    fn poly(vars: Vec<TyVar>, ty: Ty) -> Self {
        Scheme {
            vars,
            unit_vars: vec![],
            constraints: vec![],
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

    /// Trait method → list of trait param TyVars in the method's scheme.
    /// Used to map trait params to impl types during impl validation.
    trait_method_param_vars: HashMap<String, Vec<TyVar>>,

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

    // ── Units of measure ──────────────────────────────────────────
    /// Next unit variable ID.
    next_unit_var: UnitVar,
    /// Unit variable substitution.
    unit_subst: HashMap<UnitVar, UnitTy>,
    /// Declared units: name → definition (None for base units).
    declared_units: HashMap<String, Option<UnitTy>>,
    /// Unit variable names from type annotations: name → UnitVar.
    annotation_unit_vars: HashMap<String, UnitVar>,
    /// Whether we are currently processing a type annotation (so undeclared
    /// unit names are treated as polymorphic unit variables).
    in_type_annotation: bool,
    /// Maps show call-site spans to their unit display strings (for codegen).
    #[allow(dead_code)]
    pub show_unit_strings: HashMap<Span, String>,

    // ── Refined types ─────────────────────────────────────────────
    /// Refined type metadata: type_name → (base Ty, predicate Expr).
    refined_types: HashMap<String, (Ty, knot::ast::Expr)>,
    /// Refine expression type vars: (span, alpha_var, inner_ty) for post-inference resolution.
    refine_vars: Vec<(Span, TyVar, Ty)>,
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
            trait_method_param_vars: HashMap::new(),
            known_impls: HashSet::new(),
            deferred_constraints: Vec::new(),
            binding_types: Vec::new(),
            trait_params: HashMap::new(),
            fetch_response_types: HashMap::new(),
            fetch_response_headers: HashMap::new(),
            in_io_do: false,
            in_atomic: false,
            next_unit_var: 0,
            unit_subst: HashMap::new(),
            declared_units: HashMap::new(),
            annotation_unit_vars: HashMap::new(),
            in_type_annotation: false,
            show_unit_strings: HashMap::new(),
            refined_types: HashMap::new(),
            refine_vars: Vec::new(),
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

    fn fresh_unit_var(&mut self) -> UnitVar {
        let v = self.next_unit_var;
        self.next_unit_var += 1;
        v
    }

    fn apply_unit(&self, u: &UnitTy) -> UnitTy {
        match u.var {
            Some(v) => {
                if let Some(resolved) = self.unit_subst.get(&v) {
                    let mut result = u.clone();
                    result.var = resolved.var;
                    for (name, exp) in &resolved.bases {
                        *result.bases.entry(name.clone()).or_insert(0) += exp;
                    }
                    result.normalize();
                    self.apply_unit(&result)
                } else {
                    u.clone()
                }
            }
            None => u.clone(),
        }
    }

    fn unify_units(&mut self, a: &UnitTy, b: &UnitTy, span: Span) {
        let a = self.apply_unit(a);
        let b = self.apply_unit(b);

        match (a.var, b.var) {
            (Some(va), Some(vb)) if va == vb => {
                // Same variable — just check concrete parts
                if a.bases != b.bases {
                    self.error(
                        format!("unit mismatch: {} vs {}", a.display(), b.display()),
                        span,
                    );
                }
            }
            (Some(va), _) => {
                // Solve va: need a + va = b, so va = b - a(concrete)
                let mut solution = b.clone();
                for (name, exp) in &a.bases {
                    *solution.bases.entry(name.clone()).or_insert(0) -= exp;
                }
                solution.normalize();
                self.unit_subst.insert(va, solution);
            }
            (_, Some(vb)) => {
                let mut solution = a.clone();
                for (name, exp) in &b.bases {
                    *solution.bases.entry(name.clone()).or_insert(0) -= exp;
                }
                solution.normalize();
                self.unit_subst.insert(vb, solution);
            }
            (None, None) => {
                if a.bases != b.bases {
                    self.error(
                        format!("unit mismatch: {} vs {}", a.display(), b.display()),
                        span,
                    );
                }
            }
        }
    }

    /// Convert an AST UnitExpr to our internal UnitTy, expanding aliases.
    /// When `in_type_annotation` is true, undeclared unit names are treated
    /// as polymorphic unit variables (analogous to type variables).
    fn ast_unit_to_unit_ty(&mut self, u: &ast::UnitExpr) -> UnitTy {
        match u {
            ast::UnitExpr::Dimensionless => UnitTy::dimensionless(),
            ast::UnitExpr::Named(name) => {
                // Check if it's a derived unit alias
                if let Some(Some(def)) = self.declared_units.get(name) {
                    def.clone()
                } else if self.in_type_annotation && name.starts_with(|c: char| c.is_lowercase()) {
                    // In annotation context, lowercase unit names are variables
                    let var = self.annotation_unit_var(name);
                    UnitTy::var(var)
                } else {
                    UnitTy::named(name)
                }
            }
            ast::UnitExpr::Mul(a, b) => {
                let a_ty = self.ast_unit_to_unit_ty(a);
                let b_ty = self.ast_unit_to_unit_ty(b);
                a_ty.mul(&b_ty)
            }
            ast::UnitExpr::Div(a, b) => {
                let a_ty = self.ast_unit_to_unit_ty(a);
                let b_ty = self.ast_unit_to_unit_ty(b);
                a_ty.div(&b_ty)
            }
            ast::UnitExpr::Pow(base, exp) => self.ast_unit_to_unit_ty(base).pow(*exp),
        }
    }

    /// Get the unit from a type, if it has one. Returns None for dimensionless.
    #[allow(dead_code)]
    fn type_unit(&self, ty: &Ty) -> Option<UnitTy> {
        match ty {
            Ty::IntUnit(u) | Ty::FloatUnit(u) => Some(self.apply_unit(u)),
            _ => None,
        }
    }

    /// Check if a type is numeric (Int, Float, IntUnit, FloatUnit).
    #[allow(dead_code)]
    fn is_numeric(&self, ty: &Ty) -> bool {
        matches!(ty, Ty::Int | Ty::Float | Ty::IntUnit(_) | Ty::FloatUnit(_))
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
            Ty::IntUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() { Ty::Int } else { Ty::IntUnit(u) }
            }
            Ty::FloatUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() { Ty::Float } else { Ty::FloatUnit(u) }
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
            Ty::TyCon(ref name) if name == "IO" => {
                Ty::IO(BTreeSet::new(), Box::new(a))
            }
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
        // Capture root vars before apply shadows them — needed to propagate
        // merged IO effects back into the substitution.
        let var1 = if let Ty::Var(v) = t1 { Some(*v) } else { None };
        let var2 = if let Ty::Var(v) = t2 { Some(*v) } else { None };
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
            // App(f, a) vs IO(effects, b) → f = IO, a = b
            (Ty::App(f, a), Ty::IO(_effects, b))
            | (Ty::IO(_effects, b), Ty::App(f, a)) => {
                self.unify(f, &Ty::TyCon("IO".into()), span);
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
                // Merge IO effect sets (union) and propagate into substitution
                if e1 != e2 {
                    let mut merged = e1.clone();
                    merged.extend(e2.iter().cloned());
                    let unified_inner = self.apply(a);
                    let merged_io = Ty::IO(merged, Box::new(unified_inner));
                    if let Some(v) = var1 {
                        self.subst.insert(v, merged_io.clone());
                    }
                    if let Some(v) = var2 {
                        self.subst.insert(v, merged_io);
                    }
                }
            }
            // In IO do blocks, allow Relation types to unify with IO or
            // Unit types. Route handlers mix relational operations and
            // `respond` calls in if/case branches.
            (Ty::Relation(a), Ty::IO(_, b)) | (Ty::IO(_, b), Ty::Relation(a)) if self.in_io_do => {
                self.unify(a, b, span);
            }
            (Ty::Relation(_), Ty::Record(fields, None)) | (Ty::Record(fields, None), Ty::Relation(_))
                if self.in_io_do && fields.is_empty() => {}

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
            // ── Units of measure ──────────────────────────────
            (Ty::IntUnit(u1), Ty::IntUnit(u2)) => {
                self.unify_units(u1, u2, span);
            }
            (Ty::FloatUnit(u1), Ty::FloatUnit(u2)) => {
                self.unify_units(u1, u2, span);
            }
            // Plain Int/Float unifies with any IntUnit/FloatUnit — plain
            // numeric types are unit-agnostic (not "dimensionless").
            // To express dimensionless explicitly, use Int<1>/Float<1>.
            (Ty::Int, Ty::IntUnit(_))
            | (Ty::IntUnit(_), Ty::Int)
            | (Ty::Float, Ty::FloatUnit(_))
            | (Ty::FloatUnit(_), Ty::Float) => {}
            // Bool is Ty::Bool (not Ty::Con), so handle Bool/Variant
            // unification explicitly to support True {}/False {} patterns.
            (Ty::Bool, Ty::Variant(c2, r2)) => {
                if let Some(expanded) = self.con_to_variant("Bool", &[]) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(&ec, er, c2, *r2, span);
                }
            }
            (Ty::Variant(c1, r1), Ty::Bool) => {
                if let Some(expanded) = self.con_to_variant("Bool", &[]) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(c1, *r1, &ec, er, span);
                }
            }
            // Refined type subsumption: Con("Nat", []) ↔ Int, etc.
            (Ty::Con(name, args), other)
                if args.is_empty() && self.refined_types.contains_key(name) =>
            {
                let base_ty = self.refined_types[name].0.clone();
                let other = other.clone();
                self.unify(&base_ty, &other, span);
            }
            (other, Ty::Con(name, args))
                if args.is_empty() && self.refined_types.contains_key(name) =>
            {
                let base_ty = self.refined_types[name].0.clone();
                let other = other.clone();
                self.unify(&other, &base_ty, span);
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
                let target = Ty::Record(only2, None);
                if self.occurs_in(rv, &target) {
                    self.error("infinite type (record row variable)".into(), span);
                } else {
                    self.subst.insert(rv, target);
                }
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
                let target = Ty::Record(only1, None);
                if self.occurs_in(rv, &target) {
                    self.error("infinite type (record row variable)".into(), span);
                } else {
                    self.subst.insert(rv, target);
                }
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
                    let t1 = Ty::Record(only2, Some(fresh));
                    let t2 = Ty::Record(only1, Some(fresh));
                    if self.occurs_in(rv1, &t1) || self.occurs_in(rv2, &t2) {
                        self.error("infinite type (record row variable)".into(), span);
                    } else {
                        self.subst.insert(rv1, t1);
                        self.subst.insert(rv2, t2);
                    }
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
                let target = Ty::Variant(only2, None);
                if self.occurs_in(rv, &target) {
                    self.error("infinite type (variant row variable)".into(), span);
                } else {
                    self.subst.insert(rv, target);
                }
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
                let target = Ty::Variant(only1, None);
                if self.occurs_in(rv, &target) {
                    self.error("infinite type (variant row variable)".into(), span);
                } else {
                    self.subst.insert(rv, target);
                }
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
                    let t1 = Ty::Variant(only2, Some(fresh));
                    let t2 = Ty::Variant(only1, Some(fresh));
                    if self.occurs_in(rv1, &t1) || self.occurs_in(rv2, &t2) {
                        self.error("infinite type (variant row variable)".into(), span);
                    } else {
                        self.subst.insert(rv1, t1);
                        self.subst.insert(rv2, t2);
                    }
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
        // Save and restore annotation_vars so this doesn't corrupt
        // the enclosing declaration's type variable mapping.
        let saved_annotation_vars = self.annotation_vars.clone();
        self.annotation_vars.clear();
        // Build param → arg mapping
        if args.len() != info.params.len() {
            self.annotation_vars = saved_annotation_vars;
            return None;
        }
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
        self.annotation_vars = saved_annotation_vars;
        Some(Ty::Variant(ctors, None))
    }

    // ── Scheme operations ────────────────────────────────────────

    fn instantiate(&mut self, scheme: &Scheme) -> Ty {
        self.instantiate_at(scheme, Span::new(0, 0))
    }

    fn instantiate_at(&mut self, scheme: &Scheme, span: Span) -> Ty {
        if scheme.vars.is_empty() && scheme.unit_vars.is_empty() {
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
        let ty = self.subst_ty(&scheme.ty, &mapping);
        // Freshen unit variables so each instantiation gets independent units
        if scheme.unit_vars.is_empty() {
            ty
        } else {
            let unit_mapping: HashMap<UnitVar, UnitVar> = scheme
                .unit_vars
                .iter()
                .map(|v| (*v, self.fresh_unit_var()))
                .collect();
            self.subst_unit_vars_in_ty(&ty, &unit_mapping)
        }
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
                let mut new_fields: BTreeMap<_, _> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.subst_ty(v, mapping)))
                    .collect();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            Ty::Record(extra_fields, extra_row) => {
                                // Merge fields from the replacement record
                                for (k, v) in extra_fields {
                                    new_fields.entry(k.clone()).or_insert_with(|| v.clone());
                                }
                                *extra_row
                            }
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::Record(new_fields, new_row)
            }
            Ty::Variant(ctors, row) => {
                let mut new_ctors: BTreeMap<_, _> = ctors
                    .iter()
                    .map(|(k, v)| (k.clone(), self.subst_ty(v, mapping)))
                    .collect();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            Ty::Variant(extra_ctors, extra_row) => {
                                // Merge constructors from the replacement variant
                                for (k, v) in extra_ctors {
                                    new_ctors.entry(k.clone()).or_insert_with(|| v.clone());
                                }
                                *extra_row
                            }
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

    /// Replace unit variables in a type according to a freshening mapping.
    fn subst_unit_vars_in_ty(&self, ty: &Ty, mapping: &HashMap<UnitVar, UnitVar>) -> Ty {
        match ty {
            Ty::IntUnit(u) => Ty::IntUnit(Self::subst_unit_var(u, mapping)),
            Ty::FloatUnit(u) => Ty::FloatUnit(Self::subst_unit_var(u, mapping)),
            Ty::Fun(p, r) => Ty::Fun(
                Box::new(self.subst_unit_vars_in_ty(p, mapping)),
                Box::new(self.subst_unit_vars_in_ty(r, mapping)),
            ),
            Ty::Relation(inner) => Ty::Relation(Box::new(self.subst_unit_vars_in_ty(inner, mapping))),
            Ty::Record(fields, row) => {
                let new_fields = fields.iter()
                    .map(|(k, v)| (k.clone(), self.subst_unit_vars_in_ty(v, mapping)))
                    .collect();
                Ty::Record(new_fields, *row)
            }
            Ty::Variant(ctors, row) => {
                let new_ctors = ctors.iter()
                    .map(|(k, v)| (k.clone(), self.subst_unit_vars_in_ty(v, mapping)))
                    .collect();
                Ty::Variant(new_ctors, *row)
            }
            Ty::Con(name, args) => Ty::Con(
                name.clone(),
                args.iter().map(|a| self.subst_unit_vars_in_ty(a, mapping)).collect(),
            ),
            Ty::App(f, a) => Ty::App(
                Box::new(self.subst_unit_vars_in_ty(f, mapping)),
                Box::new(self.subst_unit_vars_in_ty(a, mapping)),
            ),
            Ty::IO(effects, inner) => Ty::IO(
                effects.clone(),
                Box::new(self.subst_unit_vars_in_ty(inner, mapping)),
            ),
            _ => ty.clone(),
        }
    }

    fn subst_unit_var(u: &UnitTy, mapping: &HashMap<UnitVar, UnitVar>) -> UnitTy {
        match u.var {
            Some(v) => {
                if let Some(&new_v) = mapping.get(&v) {
                    UnitTy { bases: u.bases.clone(), var: Some(new_v) }
                } else {
                    u.clone()
                }
            }
            None => u.clone(),
        }
    }

    /// Collect all free (unsolved) unit variables in a type.
    fn free_unit_vars_in_ty(&self, ty: &Ty) -> Vec<UnitVar> {
        let mut vars = HashSet::new();
        self.collect_free_unit_vars(ty, &mut vars);
        vars.into_iter().collect()
    }

    fn collect_free_unit_vars(&self, ty: &Ty, out: &mut HashSet<UnitVar>) {
        match ty {
            Ty::IntUnit(u) | Ty::FloatUnit(u) => {
                let applied = self.apply_unit(u);
                if let Some(v) = applied.var {
                    out.insert(v);
                }
            }
            Ty::Fun(p, r) => {
                self.collect_free_unit_vars(p, out);
                self.collect_free_unit_vars(r, out);
            }
            Ty::Relation(inner) => self.collect_free_unit_vars(inner, out),
            Ty::Record(fields, _) => {
                for v in fields.values() {
                    self.collect_free_unit_vars(v, out);
                }
            }
            Ty::Variant(ctors, _) => {
                for v in ctors.values() {
                    self.collect_free_unit_vars(v, out);
                }
            }
            Ty::Con(_, args) => {
                for a in args {
                    self.collect_free_unit_vars(a, out);
                }
            }
            Ty::App(f, a) => {
                self.collect_free_unit_vars(f, out);
                self.collect_free_unit_vars(a, out);
            }
            Ty::IO(_, inner) => self.collect_free_unit_vars(inner, out),
            _ => {}
        }
    }

    fn generalize(&mut self, ty: &Ty) -> Scheme {
        self.generalize_with_constraints(ty, vec![])
    }

    fn generalize_with_constraints(
        &mut self,
        ty: &Ty,
        all_constraints: Vec<TyConstraint>,
    ) -> Scheme {
        let applied = self.apply(ty);
        let env_fv = self.free_vars_in_env();
        let ty_fv = self.free_vars(&applied);
        let gen_vars: Vec<TyVar> =
            ty_fv.difference(&env_fv).copied().collect();
        let gen_set: HashSet<TyVar> = gen_vars.iter().copied().collect();
        // Only keep constraints on generalized variables; immediately
        // validate constraints that resolved to concrete types.
        let mut kept = Vec::new();
        for c in all_constraints {
            let resolved = self.apply(&Ty::Var(c.type_var));
            match resolved {
                Ty::Var(v) if gen_set.contains(&v) => kept.push(c),
                Ty::Var(_) => {} // env-bound var, not generalized
                concrete => {
                    // Constraint resolved to a concrete type — check now
                    if let Some(type_name) = self.type_name_of(&concrete) {
                        let key = (c.trait_name.clone(), type_name.clone());
                        if !self.known_impls.contains(&key) {
                            self.error(
                                format!(
                                    "no implementation of trait '{}' for type '{}'",
                                    c.trait_name, type_name
                                ),
                                c.span,
                            );
                        }
                    }
                }
            }
        }
        let unit_vars = self.free_unit_vars_in_ty(&applied);
        Scheme {
            vars: gen_vars,
            unit_vars,
            constraints: kept,
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
            ast::TypeKind::IO { effects, ty: inner_ty } => {
                let mut io_effects = BTreeSet::new();
                for e in effects {
                    match e.as_str() {
                        "console" => { io_effects.insert(IoEffect::Console); }
                        "fs" => { io_effects.insert(IoEffect::Fs); }
                        "network" => { io_effects.insert(IoEffect::Network); }
                        "clock" => { io_effects.insert(IoEffect::Clock); }
                        "random" => { io_effects.insert(IoEffect::Random); }
                        unknown => {
                            self.error(
                                format!("unknown IO effect '{}' (expected one of: console, fs, network, clock, random)", unknown),
                                ty.span,
                            );
                        }
                    }
                }
                Ty::IO(io_effects, Box::new(self.ast_type_to_ty(inner_ty)))
            }
            ast::TypeKind::UnitAnnotated { base, unit } => {
                let base_ty = self.ast_type_to_ty(base);
                let unit_ty = self.ast_unit_to_unit_ty(unit);
                match base_ty {
                    Ty::Int => Ty::IntUnit(unit_ty),
                    Ty::Float => Ty::FloatUnit(unit_ty),
                    _ => {
                        self.error(
                            "unit annotations are only allowed on Int and Float types".into(),
                            ty.span,
                        );
                        Ty::Error
                    }
                }
            }

            ast::TypeKind::Refined { base, .. } => {
                // Inline refined types resolve to their base type.
                // Named refined type aliases are kept nominal (handled in Named arm).
                self.ast_type_to_ty(base)
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

    fn annotation_unit_var(&mut self, name: &str) -> UnitVar {
        if let Some(&var) = self.annotation_unit_vars.get(name) {
            var
        } else {
            let var = self.fresh_unit_var();
            self.annotation_unit_vars.insert(name.to_string(), var);
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
            Ty::IntUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() {
                    "Int".into()
                } else {
                    format!("Int<{}>", u.display())
                }
            }
            Ty::FloatUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() {
                    "Float".into()
                } else {
                    format!("Float<{}>", u.display())
                }
            }
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

        // Save and restore annotation_vars so constructor instantiation
        // doesn't corrupt the enclosing declaration's type variable mapping.
        let saved_annotation_vars = self.annotation_vars.clone();
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

        self.annotation_vars = saved_annotation_vars;
        Some((data_ty, record_ty))
    }

    // ── Expression inference ─────────────────────────────────────

    fn infer_expr(&mut self, expr: &ast::Expr) -> Ty {
        match &expr.node {
            ast::ExprKind::Lit(lit) => self.literal_type(lit),

            ast::ExprKind::Var(name) if name == "__yield" || name == "yield" => {
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
                    Ty::IO(BTreeSet::new(), Box::new(ty))
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
                    Ty::IO(BTreeSet::new(), Box::new(ty))
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
                    resolved
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
                        // numeric negation — reject known non-numeric types
                        let resolved = self.apply(&operand_ty);
                        match &resolved {
                            Ty::Int | Ty::Float | Ty::IntUnit(_) | Ty::FloatUnit(_) | Ty::Var(_) | Ty::Error => {}
                            _ => {
                                self.error(
                                    format!(
                                        "cannot negate value of type {}",
                                        self.display_ty(&resolved)
                                    ),
                                    operand.span,
                                );
                            }
                        }
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
                // Merge IO effects from both branches — unify only checks
                // inner types and discards effect sets.
                let applied_then = self.apply(&then_ty);
                let applied_else = self.apply(&else_ty);
                match (&applied_then, &applied_else) {
                    (Ty::IO(e1, inner), Ty::IO(e2, _)) => {
                        let mut merged = e1.clone();
                        merged.extend(e2.iter().cloned());
                        Ty::IO(merged, inner.clone())
                    }
                    _ => then_ty,
                }
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                let scrut_ty = self.infer_expr(scrutinee);
                let result_ty = self.fresh();
                let mut case_io_effects: BTreeSet<IoEffect> = BTreeSet::new();

                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify(&result_ty, &body_ty, arm.body.span);
                    // Collect IO effects from each arm
                    let applied = self.apply(&body_ty);
                    if let Ty::IO(ref effects, _) = applied {
                        case_io_effects.extend(effects.iter().cloned());
                    }
                    self.pop_scope();
                }

                self.check_exhaustiveness(&scrut_ty, arms, expr.span);

                // Merge IO effects from all arms into the result type
                if !case_io_effects.is_empty() {
                    let applied_result = self.apply(&result_ty);
                    if let Ty::IO(_, inner) = applied_result {
                        Ty::IO(case_io_effects, inner)
                    } else {
                        result_ty
                    }
                } else {
                    result_ty
                }
            }

            ast::ExprKind::Do(stmts) => self.infer_do(stmts, expr.span),

            ast::ExprKind::Set { target, value } => {
                let target_ty = self.infer_expr(target);
                let value_ty = self.infer_expr(value);
                // Unwrap IO from both sides for unification —
                // target is IO (source ref), value may also be IO
                // (do-block reading from relations).
                // Apply substitution first so type variables resolved
                // to IO are properly unwrapped.
                let target_applied = self.apply(&target_ty);
                let value_applied = self.apply(&value_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(_, inner) => (**inner).clone(),
                    other => other.clone(),
                };
                self.unify(&unwrap_io(&target_applied), &unwrap_io(&value_applied), expr.span);
                Ty::IO(BTreeSet::new(), Box::new(Ty::unit()))
            }

            ast::ExprKind::FullSet { target, value } => {
                let target_ty = self.infer_expr(target);
                let value_ty = self.infer_expr(value);
                let target_applied = self.apply(&target_ty);
                let value_applied = self.apply(&value_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(_, inner) => (**inner).clone(),
                    other => other.clone(),
                };
                self.unify(&unwrap_io(&target_applied), &unwrap_io(&value_applied), expr.span);
                Ty::IO(BTreeSet::new(), Box::new(Ty::unit()))
            }

            ast::ExprKind::Atomic(inner) => {
                let prev = self.in_atomic;
                self.in_atomic = true;
                let inner_ty = self.infer_expr(inner);
                self.in_atomic = prev;
                // atomic : IO {} a -> IO {} a
                let inner_applied = self.apply(&inner_ty);
                match &inner_applied {
                    Ty::IO(_, _) => inner_applied,
                    _ => {
                        self.error(
                            "atomic body must be an IO expression".to_string(),
                            expr.span,
                        );
                        inner_ty
                    }
                }
            }

            ast::ExprKind::At { relation, time } => {
                let rel_ty = self.infer_expr(relation);
                let time_ty = self.infer_expr(time);
                self.unify(&time_ty, &Ty::Int, time.span);
                // Temporal query is a DB read — preserve IO wrapping
                rel_ty
            }

            ast::ExprKind::UnitLit { value, unit } => {
                let val_ty = self.infer_expr(value);
                let unit_ty = self.ast_unit_to_unit_ty(unit);
                match &val_ty {
                    Ty::Int | Ty::IntUnit(_) => Ty::IntUnit(unit_ty),
                    Ty::Float | Ty::FloatUnit(_) => Ty::FloatUnit(unit_ty),
                    _ => {
                        self.error(
                            "unit annotations are only allowed on numeric literals".into(),
                            expr.span,
                        );
                        val_ty
                    }
                }
            }

            ast::ExprKind::Annot { expr: inner, ty } => {
                let inner_ty = self.infer_expr(inner);
                let annot_ty = self.ast_type_to_ty(ty);
                self.unify(&inner_ty, &annot_ty, expr.span);
                annot_ty
            }

            ast::ExprKind::Refine(inner) => {
                let inner_ty = self.infer_expr(inner);
                let alpha = self.fresh();
                // Unify alpha with inner_ty so the Result type is fully determined.
                // Context may further constrain alpha to a refined type via subsumption.
                self.unify(&inner_ty, &alpha, expr.span);
                let alpha_var = match &alpha {
                    Ty::Var(v) => *v,
                    _ => unreachable!(),
                };
                self.refine_vars.push((expr.span, alpha_var, inner_ty));
                // Return Result RefinementError alpha
                // Use the actual record type for RefinementError (not Con) so field access works
                let refinement_error_ty = self.aliases.get("RefinementError")
                    .cloned()
                    .unwrap_or_else(|| Ty::Con("RefinementError".into(), vec![]));
                Ty::Con(
                    "Result".into(),
                    vec![refinement_error_ty, alpha],
                )
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
        // Save and restore annotation_vars so fetch inference doesn't corrupt
        // the enclosing declaration's type variable mapping.
        let saved_annotation_vars = self.annotation_vars.clone();
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
        self.annotation_vars = saved_annotation_vars;
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
            // Add/Sub: units must match, result has same unit
            ast::BinOp::Add | ast::BinOp::Sub => {
                let lhs_applied = self.apply(&lhs_ty);
                let rhs_applied = self.apply(&rhs_ty);
                // For unit-bearing types, unify normally (which checks unit equality)
                self.unify(&lhs_applied, &rhs_applied, span);
                lhs_applied
            }
            // Mul/Div: units compose
            ast::BinOp::Mul | ast::BinOp::Div => {
                let lhs_applied = self.apply(&lhs_ty);
                let rhs_applied = self.apply(&rhs_ty);
                match (&lhs_applied, &rhs_applied) {
                    // Both have units → compose
                    (Ty::IntUnit(u1), Ty::IntUnit(u2)) => {
                        let result_unit = if op == ast::BinOp::Mul {
                            u1.mul(u2)
                        } else {
                            u1.div(u2)
                        };
                        if result_unit.is_dimensionless() { Ty::Int } else { Ty::IntUnit(result_unit) }
                    }
                    (Ty::FloatUnit(u1), Ty::FloatUnit(u2)) => {
                        let result_unit = if op == ast::BinOp::Mul {
                            u1.mul(u2)
                        } else {
                            u1.div(u2)
                        };
                        if result_unit.is_dimensionless() { Ty::Float } else { Ty::FloatUnit(result_unit) }
                    }
                    // One unit, one dimensionless → preserve unit
                    (Ty::IntUnit(u), Ty::Int) | (Ty::Int, Ty::IntUnit(u)) => {
                        if op == ast::BinOp::Div && matches!(&rhs_applied, Ty::IntUnit(_)) {
                            // x / y<u> → x<1/u>
                            let inv = u.pow(-1);
                            if inv.is_dimensionless() { Ty::Int } else { Ty::IntUnit(inv) }
                        } else if op == ast::BinOp::Div && matches!(&lhs_applied, Ty::IntUnit(_)) {
                            // x<u> / y → x<u>
                            Ty::IntUnit(u.clone())
                        } else {
                            Ty::IntUnit(u.clone())
                        }
                    }
                    (Ty::FloatUnit(u), Ty::Float) | (Ty::Float, Ty::FloatUnit(u)) => {
                        if op == ast::BinOp::Div && matches!(&rhs_applied, Ty::FloatUnit(_)) {
                            let inv = u.pow(-1);
                            if inv.is_dimensionless() { Ty::Float } else { Ty::FloatUnit(inv) }
                        } else if op == ast::BinOp::Div && matches!(&lhs_applied, Ty::FloatUnit(_)) {
                            Ty::FloatUnit(u.clone())
                        } else {
                            Ty::FloatUnit(u.clone())
                        }
                    }
                    // No units involved → default behavior
                    _ => {
                        self.unify(&lhs_applied, &rhs_applied, span);
                        lhs_applied
                    }
                }
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
                        ast::PatKind::Lit(ast::Literal::Bool(true)) => Some("True"),
                        ast::PatKind::Lit(ast::Literal::Bool(false)) => Some("False"),
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
                        ast::PatKind::Lit(ast::Literal::Bool(true)) => Some("True"),
                        ast::PatKind::Lit(ast::Literal::Bool(false)) => Some("False"),
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
            // Bool is Ty::Bool (not Ty::Con), so handle it explicitly.
            Ty::Bool => {
                if let Some(data_info) = self.data_types.get("Bool").cloned() {
                    let covered: HashSet<&str> = arms
                        .iter()
                        .filter_map(|arm| match &arm.pat.node {
                            ast::PatKind::Constructor { name, .. } => {
                                Some(name.as_str())
                            }
                            ast::PatKind::Lit(ast::Literal::Bool(true)) => Some("True"),
                            ast::PatKind::Lit(ast::Literal::Bool(false)) => Some("False"),
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
            }
            // Primitives (Int, Text, etc.) have infinite domains.
            _ => {}
        }
    }

    // ── Do-block inference ───────────────────────────────────────

    /// Pre-scan do-block statements to detect IO builtins and user-defined IO
    /// functions (mirrors codegen's `is_io_do_block` / `expr_is_io`).
    fn stmt_has_io(&self, stmts: &[ast::Stmt]) -> bool {
        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } | ast::StmtKind::Expr(expr) => {
                    if self.expr_is_io_prescan(expr) {
                        return true;
                    }
                }
                ast::StmtKind::Where { cond } => {
                    if self.expr_is_io_prescan(cond) {
                        return true;
                    }
                }
                ast::StmtKind::GroupBy { key } => {
                    if self.expr_is_io_prescan(key) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if an expression returns IO — checks builtins and user-defined
    /// functions whose already-inferred type returns IO.
    fn expr_is_io_prescan(&self, expr: &ast::Expr) -> bool {
        match &expr.node {
            ast::ExprKind::App { func, arg } => {
                self.expr_is_io_prescan(func) || self.expr_is_io_prescan(arg)
            }
            ast::ExprKind::Var(name) => {
                matches!(
                    name.as_str(),
                    "println" | "putLine" | "print" | "readLine" | "readFile"
                        | "writeFile" | "appendFile" | "fileExists" | "removeFile"
                        | "listDir" | "now" | "sleep" | "randomInt" | "randomFloat"
                        | "fetch" | "fetchWith" | "fork" | "listen"
                        | "generateKeyPair" | "generateSigningKeyPair" | "encrypt"
                ) || self.lookup(name).map_or(false, |scheme| {
                    fn returns_io(ty: &Ty) -> bool {
                        match ty {
                            Ty::IO(_, _) => true,
                            Ty::Fun(_, ret) => returns_io(ret),
                            _ => false,
                        }
                    }
                    let resolved = self.apply(&scheme.ty);
                    returns_io(&resolved)
                })
            }
            ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => true,
            ast::ExprKind::Set { .. } | ast::ExprKind::FullSet { .. } => true,
            ast::ExprKind::At { .. } | ast::ExprKind::Atomic(_) => true,
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.expr_is_io_prescan(lhs) || self.expr_is_io_prescan(rhs)
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.expr_is_io_prescan(operand),
            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                self.expr_is_io_prescan(cond)
                    || self.expr_is_io_prescan(then_branch)
                    || self.expr_is_io_prescan(else_branch)
            }
            ast::ExprKind::Case { scrutinee, arms, .. } => {
                self.expr_is_io_prescan(scrutinee)
                    || arms.iter().any(|arm| self.expr_is_io_prescan(&arm.body))
            }
            ast::ExprKind::Do(stmts) => {
                stmts.iter().any(|s| match &s.node {
                    ast::StmtKind::Bind { expr, .. } => self.expr_is_io_prescan(expr),
                    ast::StmtKind::Expr(expr) => self.expr_is_io_prescan(expr),
                    ast::StmtKind::Let { expr, .. } => self.expr_is_io_prescan(expr),
                    ast::StmtKind::Where { cond } => self.expr_is_io_prescan(cond),
                    ast::StmtKind::GroupBy { key } => self.expr_is_io_prescan(key),
                })
            }
            ast::ExprKind::Lambda { body, .. } => self.expr_is_io_prescan(body),
            _ => false,
        }
    }

    fn infer_do(&mut self, stmts: &[ast::Stmt], _span: Span) -> Ty {
        self.push_scope();
        let mut yield_ty: Option<Ty> = None;
        let mut last_expr_ty: Option<Ty> = None;
        let mut is_io = false;
        let mut has_relation_bind = false;
        let mut io_effects: BTreeSet<IoEffect> = BTreeSet::new();

        // Pre-scan: if any statement uses IO builtins, set in_io_do so that
        // `yield` expressions inside case/if branches produce IO types.
        let prev_in_io_do = self.in_io_do;
        self.in_io_do = self.stmt_has_io(stmts);

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
                    } else if self.in_io_do && matches!(&resolved, Ty::Var(_)) {
                        // In an IO do-block with an unresolved type variable —
                        // assume IO so we don't incorrectly unify with Relation.
                        is_io = true;
                        let inner_ty = self.fresh();
                        self.unify(
                            &expr_ty,
                            &Ty::IO(BTreeSet::new(), Box::new(inner_ty.clone())),
                            expr.span,
                        );
                        self.check_pattern(pat, &inner_ty);
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
                    let resolved = self.apply(&expr_ty);
                    if let Ty::IO(ref effects, _) = resolved {
                        is_io = true;
                        io_effects.extend(effects.iter().cloned());
                    }
                    // Let-generalization: for simple variable patterns,
                    // generalize the binding so it can be used polymorphically
                    // (e.g., `let id = \x -> x` should be usable at multiple types).
                    if let ast::PatKind::Var(name) = &pat.node {
                        let applied = self.apply(&expr_ty);
                        let scheme = self.generalize(&applied);
                        self.bind(name, scheme);
                        self.binding_types.push((pat.span, applied));
                    } else {
                        self.check_pattern(pat, &expr_ty);
                    }
                }
                ast::StmtKind::Where { cond } => {
                    let cond_ty = self.infer_expr(cond);
                    self.unify(&cond_ty, &Ty::Bool, cond.span);
                }
                ast::StmtKind::GroupBy { key } => {
                    // Infer the key expression type (must be a record)
                    let _ = self.infer_expr(key);
                    // After groupBy, rebind all preceding Bind variables
                    // from T to [T] (they now represent groups).
                    // Unwrap any existing Relation wrapping first to avoid
                    // double-wrapping from multiple groupBy statements.
                    for prev_stmt in stmts {
                        if std::ptr::eq(prev_stmt, stmt) {
                            break;
                        }
                        if let ast::StmtKind::Bind { pat, .. } = &prev_stmt.node {
                            if let ast::PatKind::Var(name) = &pat.node {
                                if let Some(scheme) = self.lookup(name).cloned() {
                                    let ty = self.instantiate(&scheme);
                                    let elem_ty = match ty {
                                        Ty::Relation(inner) => *inner,
                                        other => other,
                                    };
                                    self.bind(name, Scheme::mono(Ty::Relation(Box::new(elem_ty))));
                                }
                            }
                        }
                    }
                }
                ast::StmtKind::Expr(expr) => {
                    if let Some(inner) = expr.node.as_yield_arg() {
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
                        if let Ty::IO(ref effects, ref inner) = resolved {
                            is_io = true;
                            io_effects.extend(effects.iter().cloned());
                            last_expr_ty = Some(*inner.clone());
                        } else if self.in_io_do {
                            if let Ty::App(ref f, ref inner) = resolved {
                                // In IO do-blocks, App(m, a) from yield in
                                // case/if branches — resolve m to IO.
                                self.unify(f, &Ty::TyCon("IO".into()), expr.span);
                                is_io = true;
                                last_expr_ty = Some(*inner.clone());
                            } else {
                                last_expr_ty = Some(expr_ty);
                            }
                        } else {
                            last_expr_ty = Some(expr_ty);
                        }
                    }
                }
            }
        }

        self.pop_scope();
        self.in_io_do = prev_in_io_do;

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
            let inner = yield_ty.or(last_expr_ty).unwrap_or_else(Ty::unit);
            Ty::IO(io_effects, Box::new(inner))
        } else {
            match yield_ty {
                Some(ty) => Ty::Relation(Box::new(ty)),
                None if !has_relation_bind && last_expr_ty.is_some() => {
                    // No yield, no relation bind, but has bare expressions:
                    // use the last expression's type directly. This preserves
                    // polymorphism for do-blocks that sequence operations
                    // through a polymorphic monad parameter (e.g. `a {}`).
                    last_expr_ty.unwrap()
                }
                None => Ty::Relation(Box::new(Ty::unit())),
            }
        }
    }

    // ── Declaration collection (phase 1) ─────────────────────────

    fn collect_types(&mut self, module: &ast::Module) {
        // First pass: type aliases (multi-pass to handle forward references)
        // Separate refined type aliases from regular ones.
        let mut alias_decls: Vec<(String, ast::Type)> = Vec::new();
        let mut refined_alias_decls: Vec<(String, ast::Type, ast::Expr)> = Vec::new();
        for decl in &module.decls {
            if let ast::DeclKind::TypeAlias { name, params, ty } = &decl.node {
                if params.is_empty() {
                    if let ast::TypeKind::Refined { base, predicate } = &ty.node {
                        refined_alias_decls.push((
                            name.clone(),
                            (**base).clone(),
                            (**predicate).clone(),
                        ));
                    } else {
                        alias_decls.push((name.clone(), ty.clone()));
                    }
                }
            }
        }
        // Iterate until alias resolutions stabilize (fixpoint).
        // Clear annotation_vars once before the loop so that type variable
        // names (e.g. `a` in `type T = a`) map to stable TyVars across
        // iterations — clearing inside would allocate fresh vars each time,
        // preventing convergence.
        self.annotation_vars.clear();
        loop {
            let mut changed = false;
            for (name, ty) in &alias_decls {
                let resolved = self.ast_type_to_ty(ty);
                if self.aliases.get(name) != Some(&resolved) {
                    self.aliases.insert(name.clone(), resolved);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        // Populate refined types (after alias fixpoint so bases can reference aliases)
        for (name, base_ty_ast, predicate) in &refined_alias_decls {
            let base_ty = self.ast_type_to_ty(base_ty_ast);
            self.refined_types
                .insert(name.clone(), (base_ty, predicate.clone()));
        }

        // Collect unit declarations
        for decl in &module.decls {
            if let ast::DeclKind::UnitDecl { name, definition } = &decl.node {
                let def = definition.as_ref().map(|u| self.ast_unit_to_unit_ty(u));
                self.declared_units.insert(name.clone(), def);
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

                // Clear annotation_vars for data type field resolution
                self.annotation_vars.clear();

                // For single-variant data types, also register as alias
                if ctors.len() == 1 {
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
            Ty::IO(_, _) => Some("IO".into()),
            Ty::Fun(_, _) => Some("Fun".into()),
            Ty::Record(_, _) => Some("Record".into()),
            Ty::Variant(_, _) => Some("Variant".into()),
            Ty::App(_, _) => Some("App".into()),
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
                        self.annotation_unit_vars.clear();
                        self.in_type_annotation = true;
                        // Convert AST constraints to internal constraints
                        let mut constraints = Vec::new();
                        for c in &scheme.constraints {
                            for arg in &c.args {
                                if let ast::TypeKind::Var(var_name) = &arg.node {
                                    let v = self.annotation_var(var_name);
                                    constraints.push(TyConstraint {
                                        trait_name: c.trait_name.clone(),
                                        type_var: v,
                                        span: arg.span,
                                    });
                                }
                            }
                        }
                        let ty = self.ast_type_to_ty(&scheme.ty);
                        self.in_type_annotation = false;
                        let vars: Vec<TyVar> =
                            self.annotation_vars.values().copied().collect();
                        let unit_vars: Vec<UnitVar> =
                            self.annotation_unit_vars.values().copied().collect();
                        self.bind_top(
                            name,
                            Scheme { vars, unit_vars, constraints, ty },
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

        // Built-in type: RefinementError = {typeName: Text, violations: [{field: Maybe Text, message: Text}]}
        // Register as a type alias so field access (e.typeName) works.
        self.aliases.insert(
            "RefinementError".into(),
            Ty::Record(
                BTreeMap::from([
                    ("typeName".into(), Ty::Text),
                    ("violations".into(), Ty::Relation(Box::new(Ty::Record(
                        BTreeMap::from([
                            ("field".into(), Ty::Con("Maybe".into(), vec![Ty::Text])),
                            ("message".into(), Ty::Text),
                        ]),
                        None,
                    )))),
                ]),
                None,
            ),
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

        // sleep : Int -> IO {clock} {}
        self.bind_top(
            "sleep",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Int),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Clock]), Box::new(Ty::unit()))),
            )),
        );

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

        // listen : ∀a b. Int -> (a -> b) -> IO {network} {}
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
                        Box::new(Ty::IO(
                            BTreeSet::from([IoEffect::Network]),
                            Box::new(Ty::unit()),
                        )),
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

        // avg : ∀a u. (a -> Float<u>) -> [a] -> Float<u>
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let float_u = Ty::FloatUnit(UnitTy::var(u));
            self.bind_top(
                "avg",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(float_u.clone()))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(float_u),
                        )),
                    ),
                },
            );
        }

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

        // generateKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
        let key_pair_record = Ty::Record(
            BTreeMap::from([
                ("privateKey".into(), Ty::Bytes),
                ("publicKey".into(), Ty::Bytes),
            ]),
            None,
        );
        self.bind_top("generateKeyPair", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Random]), Box::new(key_pair_record.clone())),
        ));

        // generateSigningKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
        self.bind_top("generateSigningKeyPair", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Random]), Box::new(key_pair_record)),
        ));

        // encrypt : Bytes -> Bytes -> IO {random} Bytes
        self.bind_top(
            "encrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(
                    Box::new(Ty::Bytes),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Random]), Box::new(Ty::Bytes))),
                )),
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
                self.annotation_unit_vars.clear();
                self.in_type_annotation = true;
                // Register trait params as annotation vars
                for p in params {
                    self.annotation_var(&p.name);
                }
                let method_ty = self.ast_type_to_ty(&ty.ty);
                self.in_type_annotation = false;
                let vars: Vec<TyVar> =
                    self.annotation_vars.values().copied().collect();
                let unit_vars: Vec<UnitVar> =
                    self.annotation_unit_vars.values().copied().collect();

                // Build constraints: each trait param must implement this trait
                let mut constraints: Vec<TyConstraint> = params
                    .iter()
                    .filter_map(|p| {
                        self.annotation_vars.get(&p.name).map(|&v| TyConstraint {
                            trait_name: trait_name.to_string(),
                            type_var: v,
                            span: Span::new(0, 0),
                        })
                    })
                    .collect();

                // Also include per-method constraints from the type scheme
                // (e.g., `Applicative f =>` on Traversable.traverse)
                for c in &ty.constraints {
                    if c.args.len() == 1 {
                        if let ast::TypeKind::Named(var_name) = &c.args[0].node {
                            if let Some(&v) = self.annotation_vars.get(var_name) {
                                constraints.push(TyConstraint {
                                    trait_name: c.trait_name.clone(),
                                    type_var: v,
                                    span: Span::new(0, 0),
                                });
                            }
                        }
                    }
                }

                // Record method → trait mapping
                self.trait_method_traits
                    .insert(name.clone(), trait_name.to_string());

                // Record which scheme vars are trait params (for impl validation)
                let param_vars: Vec<TyVar> = params.iter()
                    .filter_map(|p| self.annotation_vars.get(&p.name).copied())
                    .collect();
                self.trait_method_param_vars
                    .insert(name.clone(), param_vars);

                self.bind_top(
                    name,
                    Scheme { vars, unit_vars, constraints, ty: method_ty },
                );
            }
        }
    }

    // ── Declaration inference (phase 4) ──────────────────────────

    fn infer_declarations(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body, ty, .. } => {
                    if let Some(body) = body {
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
                            self.annotation_unit_vars.clear();
                            self.in_type_annotation = true;
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
                                            span: arg.span,
                                        });
                                    }
                                }
                            }
                            let ann_ty = self.ast_type_to_ty(&ts.ty);
                            self.in_type_annotation = false;
                            let vars: Vec<TyVar> = self
                                .annotation_vars
                                .values()
                                .copied()
                                .collect();
                            let unit_vars: Vec<UnitVar> = self
                                .annotation_unit_vars
                                .values()
                                .copied()
                                .collect();
                            self.bind_top(
                                name,
                                Scheme { vars, unit_vars, constraints, ty: ann_ty },
                            );
                        } else {
                            let applied = self.apply(&inferred);
                            let scheme = self.generalize(&applied);
                            self.bind_top(name, scheme);
                        }
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
                    args,
                    items,
                    ..
                } => {
                    self.check_impl_items(trait_name, args, items);
                }
                _ => {}
            }
        }
    }

    fn check_impl_items(
        &mut self,
        trait_name: &str,
        impl_args: &[ast::Type],
        items: &[ast::ImplItem],
    ) {
        // Build mapping from trait type params to the impl's concrete types.
        // e.g. `trait Display a` + `impl Display Int` → { a_var => Int }
        self.annotation_vars.clear();
        let impl_types: Vec<Ty> = impl_args.iter().map(|a| self.ast_type_to_ty(&a)).collect();

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
                let body_ty = self.infer_expr(body);

                // Build the inferred method type: params -> body_ty
                let mut inferred_method_ty = body_ty;
                for pt in param_types.iter().rev() {
                    inferred_method_ty = Ty::Fun(
                        Box::new(pt.clone()),
                        Box::new(inferred_method_ty),
                    );
                }

                self.pop_scope();

                // Validate against the trait's declared method type
                if let Some(scheme) = self.lookup(name).cloned() {
                    // Map only trait param vars to impl types; other vars
                    // (local type vars like `a`) get fresh variables.
                    let param_vars = self.trait_method_param_vars
                        .get(name.as_str())
                        .cloned()
                        .unwrap_or_default();
                    let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
                    for (pv, impl_ty) in param_vars.iter().zip(impl_types.iter()) {
                        mapping.insert(*pv, impl_ty.clone());
                    }
                    // Give remaining scheme vars fresh variables
                    for v in &scheme.vars {
                        mapping.entry(*v).or_insert_with(|| self.fresh());
                    }
                    let expected = self.subst_ty(&scheme.ty, &mapping);
                    self.unify(&expected, &inferred_method_ty, body.span);
                } else {
                    // Method not found in scope — check if it belongs to a
                    // different trait or is simply unknown.  Default-body
                    // methods registered with placeholder types won't have
                    // a lookup entry, so only error when the method isn't
                    // associated with this trait at all.
                    let belongs_to = self.trait_method_traits.get(name);
                    if belongs_to != Some(&trait_name.to_string()) {
                        self.error(
                            format!(
                                "method '{}' is not declared in trait '{}'",
                                name, trait_name
                            ),
                            body.span,
                        );
                    }
                }
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
        Ty::IntUnit(u) => {
            if u.is_dimensionless() { "Int".into() } else { format!("Int<{}>", u.display()) }
        }
        Ty::FloatUnit(u) => {
            if u.is_dimensionless() { "Float".into() } else { format!("Float<{}>", u.display()) }
        }
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
pub fn check(module: &ast::Module) -> (Vec<Diagnostic>, MonadInfo, TypeInfo, LocalTypeInfo, RefineTargets, RefinedTypeInfoMap) {
    let mut infer = Infer::new();

    // Phase 1: Collect type aliases, data types, constructors
    infer.collect_types(module);

    // Phase 2: Register source/view/derived relation types
    infer.collect_sources(module);

    // Phase 2b: Collect known trait implementations
    infer.collect_impls(module);

    // Phase 2c: Register builtin [] and Result impls for HKT traits
    for trait_name in &["Functor", "Applicative", "Monad", "Alternative", "Foldable", "Traversable"] {
        infer
            .known_impls
            .insert((trait_name.to_string(), "[]".to_string()));
    }
    for trait_name in &["Functor", "Applicative", "Monad", "Alternative"] {
        infer
            .known_impls
            .insert((trait_name.to_string(), "Result".to_string()));
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
            Ty::TyCon(name) if name == "IO" => MonadKind::IO,
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

    // Phase 6: Resolve refine expression targets
    let mut refine_targets = RefineTargets::new();
    for (span, var, _inner_ty) in &infer.refine_vars {
        let resolved = infer.apply(&Ty::Var(*var));
        if let Ty::Con(name, args) = &resolved {
            if args.is_empty() && infer.refined_types.contains_key(name) {
                refine_targets.insert(*span, name.clone());
                continue;
            }
        }
        // Alpha resolved to a base type (e.g. Int). Search for a refined type
        // whose base matches — this handles the do-block case where subsumption
        // unified alpha with the base type rather than the refined type.
        let mut found = None;
        for (name, (base_ty, _)) in &infer.refined_types {
            if *base_ty == resolved {
                found = Some(name.clone());
                break;
            }
        }
        if let Some(name) = found {
            refine_targets.insert(*span, name);
        } else {
            infer.errors.push((
                format!(
                    "cannot infer refined type target for refine expression (got {}); use a context that constrains the type (e.g., pass to a function expecting a refined type)",
                    infer.display_ty(&resolved)
                ),
                *span,
            ));
        }
    }

    // Export refined type predicates for codegen
    let refined_type_info: RefinedTypeInfoMap = infer
        .refined_types
        .iter()
        .map(|(name, (_, pred))| (name.clone(), pred.clone()))
        .collect();

    let type_info = infer.extract_type_info();
    let local_type_info = infer.extract_local_type_info();

    (infer.to_diagnostics(), monad_info, type_info, local_type_info, refine_targets, refined_type_info)
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
        let mut module = parse(src);
        crate::desugar::desugar(&mut module);
        let (diags, _monad_info, _type_info, _local_types, _refine_targets, _refined_types) = check(&module);
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
             \x20 unwrap : f a -> f a\n\
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
        // Uses in-memory relation (not source ref) since source refs are IO.
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

    // ── Units of measure ─────────────────────────────────────────

    #[test]
    fn unit_literal_typechecks() {
        assert!(check_src("unit M\nmain = 42.0<M>").is_empty());
    }

    #[test]
    fn unit_addition_same_unit() {
        assert!(check_src("unit M\nmain = 10.0<M> + 5.0<M>").is_empty());
    }

    #[test]
    fn unit_addition_mismatch() {
        let diags = check_src("unit M\nunit S\nmain = 10.0<M> + 5.0<S>");
        assert!(has_error(&diags, "unit mismatch"));
    }

    #[test]
    fn unit_multiplication_composes() {
        // M * M should not error (produces M^2)
        assert!(check_src("unit M\nmain = 10.0<M> * 5.0<M>").is_empty());
    }

    #[test]
    fn unit_division_composes() {
        // M / S should not error (produces M/S)
        assert!(check_src("unit M\nunit S\nmain = 100.0<M> / 10.0<S>").is_empty());
    }

    #[test]
    fn unit_dimensionless_scalar_mul() {
        // Float * Float<M> should produce Float<M>
        assert!(check_src("unit M\nmain = 2.0 * 5.0<M>").is_empty());
    }

    #[test]
    fn unit_in_type_annotation() {
        assert!(check_src("unit M\nf : Float<M> -> Float<M>\nf = \\x -> x").is_empty());
    }

    #[test]
    fn unit_derived_alias() {
        assert!(check_src("unit M\nunit S\nunit Mps = M / S\nmain = 10.0<Mps>").is_empty());
    }

    #[test]
    fn unit_int_literal() {
        assert!(check_src("unit Usd\nmain = 999<Usd>").is_empty());
    }

    #[test]
    fn unit_int_addition_mismatch() {
        let diags = check_src("unit Usd\nunit Eur\nmain = 100<Usd> + 50<Eur>");
        assert!(has_error(&diags, "unit mismatch"));
    }

    #[test]
    fn unit_in_record() {
        assert!(check_src(
            "unit M\nunit S\nmain = {distance: 100.0<M>, time: 10.0<S>}"
        ).is_empty());
    }

    #[test]
    fn unit_expr_annotation() {
        // (expr : Type) syntax
        let diags = check_src("unit M\nmain = (42.0 : Float<M>)");
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_avg_preserves_unit() {
        // avg should preserve the unit from the projection function
        let diags = check_src(
            "unit M\n\
             main = avg (\\p -> p.x) [{x: 1.0<M>}, {x: 2.0<M>}] + 1.0<M>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_avg_mismatch() {
        // avg result has unit from projection — adding mismatched unit should fail
        let diags = check_src(
            "unit M\nunit S\n\
             main = avg (\\p -> p.x) [{x: 1.0<M>}] + 1.0<S>"
        );
        assert!(!diags.is_empty(), "should reject adding Float<M> avg result to Float<S>");
    }

    #[test]
    fn unit_negation_preserves() {
        // Unary negation should preserve units
        let diags = check_src(
            "unit M\nmain = -(5.0<M>) + 3.0<M>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_annotation_on_function_concrete() {
        // Function with concrete unit annotation (identity-style)
        let diags = check_src(
            "unit M\n\
             wrap : Float<M> -> Float<M>\n\
             wrap = \\x -> x\n\
             main = wrap 5.0<M>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_annotation_concrete_rejects_wrong_unit() {
        // Calling a concrete-annotated function with wrong unit should fail
        let diags = check_src(
            "unit M\nunit S\n\
             wrap : Float<M> -> Float<M>\n\
             wrap = \\x -> x\n\
             main = wrap 5.0<S>"
        );
        assert!(!diags.is_empty(), "should reject Float<S> for Float<M> param");
    }

    #[test]
    fn unit_annotation_on_function_polymorphic() {
        // Function with polymorphic unit variable should type-check
        let diags = check_src(
            "unit M\n\
             double : Float<u> -> Float<u>\n\
             double = \\x -> x + x\n\
             main = double 5.0<M>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_annotation_polymorphic_mismatch() {
        // Polymorphic unit annotation should reject unit mismatches
        let diags = check_src(
            "unit M\nunit S\n\
             double : Float<u> -> Float<u>\n\
             double = \\x -> x + x\n\
             main = double 5.0<M> + 1.0<S>"
        );
        assert!(!diags.is_empty(), "should reject Float<M> + Float<S>");
    }

    #[test]
    fn unit_annotation_polymorphic_reuse() {
        // Polymorphic unit function can be called with different units at different sites
        let diags = check_src(
            "unit M\nunit S\n\
             double : Float<u> -> Float<u>\n\
             double = \\x -> x + x\n\
             main = do\n\
               let a = double 5.0<M>\n\
               let b = double 3.0<S>\n\
               yield {}"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    // ── Refined types ─────────────────────────────────────────────

    #[test]
    fn refined_type_alias_definition() {
        // Defining a refined type should not produce errors
        let diags = check_src("type Nat = Int where \\x -> x >= 0\nmain = 42");
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refined_type_subsumption() {
        // A function accepting Nat should accept Int via subsumption
        let diags = check_src(
            "type Nat = Int where \\x -> x >= 0\nf : Nat -> Int\nf = \\x -> x\nmain = f 42"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refined_type_in_record() {
        // Inline refined type in a record should parse and type-check
        let diags = check_src(
            "type Person = {name: Text, age: Int where \\x -> x >= 0}\nmain = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refine_expr_with_case() {
        // refine should return Result RefinementError T, usable with case
        let diags = check_src(
            "type Nat = Int where \\x -> x >= 0\nf : Nat -> Int\nf = \\x -> x\nmain = case refine 42 of\n  Ok {value: n} -> f n\n  Err {error: _} -> 0"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refine_expr_in_result_do_block() {
        // refine in a do-block should use Result monad (bind short-circuits on Err)
        let diags = check_src(
            "type Nat = Int where \\x -> x >= 0\nf : Nat -> Int\nf = \\x -> x\nmain = do\n  n <- refine 42\n  yield (f n)"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refined_type_stacking() {
        // Stacked refinements: Age = Nat where <= 150, Nat = Int where >= 0
        let diags = check_src(
            "type Nat = Int where \\x -> x >= 0\ntype Age = Nat where \\x -> x <= 150\nf : Age -> Int\nf = \\x -> x\nmain = f 25"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refined_type_with_units() {
        // Refinement and units are orthogonal
        let diags = check_src(
            "unit M\ntype PosFloat = Float where \\x -> x > 0.0\nmain = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn refine_target_must_be_refined_type() {
        // refine with a non-refined target should error
        let diags = check_src(
            "main = case refine 42 of\n  Ok {value: n} -> n\n  Err {error: _} -> 0"
        );
        assert!(has_error(&diags, "cannot infer refined type target"));
    }

    #[test]
    fn refined_cross_field() {
        // Cross-field refinement on a record type
        let diags = check_src(
            "type Range = {lo: Int, hi: Int} where \\r -> r.lo <= r.hi\nmain = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
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
