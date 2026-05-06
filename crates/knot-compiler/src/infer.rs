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
///
/// Reads/Writes carry the source-relation name. Other effects are nullary tags.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoEffect {
    Reads(String),
    Writes(String),
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

/// Maps parseJson call-site spans to the resolved target type name for compile-time FromJSON dispatch.
pub type FromJsonTargets = HashMap<Span, String>;

/// Spans of `elem needle haystack` haystack arguments whose element type is a
/// SQL-pushable scalar (Int/Text/Float/Bool, peeling aliases & refined types).
/// Codegen consults this set to decide whether to emit
/// `IN (SELECT value FROM json_each(?))` for dynamic haystacks.
pub type ElemPushdownOk = HashSet<Span>;

// ── Units of measure ──────────────────────────────────────────────

type UnitVar = u32;

/// Normalized unit: a product of base-unit powers, e.g. m^1 * s^-2.
/// Dimensionless = empty map.  Unit variables track polymorphic units
/// with arbitrary exponents, e.g. `u^2` or `u^-1`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnitTy {
    /// base_unit_name -> exponent
    bases: BTreeMap<String, i32>,
    /// Unit variables with exponents for polymorphism (e.g. u^1, u^-1, u^2)
    vars: BTreeMap<UnitVar, i32>,
}

#[allow(dead_code)]
impl UnitTy {
    fn dimensionless() -> Self {
        UnitTy { bases: BTreeMap::new(), vars: BTreeMap::new() }
    }

    fn named(name: &str) -> Self {
        let mut bases = BTreeMap::new();
        bases.insert(name.to_string(), 1);
        UnitTy { bases, vars: BTreeMap::new() }
    }

    fn var(v: UnitVar) -> Self {
        let mut vars = BTreeMap::new();
        vars.insert(v, 1);
        UnitTy { bases: BTreeMap::new(), vars }
    }

    fn is_dimensionless(&self) -> bool {
        self.bases.is_empty() && self.vars.is_empty()
    }

    fn normalize(&mut self) {
        self.bases.retain(|_, exp| *exp != 0);
        self.vars.retain(|_, exp| *exp != 0);
    }

    fn mul(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            *result.bases.entry(name.clone()).or_insert(0) += exp;
        }
        for (&v, &exp) in &other.vars {
            *result.vars.entry(v).or_insert(0) += exp;
        }
        result.normalize();
        result
    }

    fn div(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            *result.bases.entry(name.clone()).or_insert(0) -= exp;
        }
        for (&v, &exp) in &other.vars {
            *result.vars.entry(v).or_insert(0) -= exp;
        }
        result.normalize();
        result
    }

    fn pow(&self, n: i32) -> UnitTy {
        let mut result = self.clone();
        for exp in result.bases.values_mut() {
            *exp *= n;
        }
        for exp in result.vars.values_mut() {
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
        for (&v, &exp) in &self.vars {
            if exp > 0 {
                if exp == 1 {
                    num_parts.push(format!("?u{}", v));
                } else {
                    num_parts.push(format!("?u{}^{}", v, exp));
                }
            } else if exp < 0 {
                if exp == -1 {
                    den_parts.push(format!("?u{}", v));
                } else {
                    den_parts.push(format!("?u{}^{}", v, -exp));
                }
            }
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
    /// IO monad with tracked effects and an optional row variable for
    /// effect polymorphism: `IO {console, fs} a` or `IO {console | r} a`.
    /// The row tail unifies with whatever extra effects the caller's
    /// context introduces.
    IO(BTreeSet<IoEffect>, Option<TyVar>, Box<Ty>),
    /// Tail of an effect row — the binding form for an effect row variable.
    /// `Ty::EffectRow(extras, tail)` says: "the original row variable now
    /// stands for `extras`, possibly followed by another row variable
    /// `tail`". Only legal as the right-hand side of a substitution for a
    /// row variable that appeared in `Ty::IO`'s tail position.
    EffectRow(BTreeSet<IoEffect>, Option<TyVar>),
    /// Int with unit of measure (compile-time only).
    IntUnit(UnitTy),
    /// Float with unit of measure (compile-time only).
    FloatUnit(UnitTy),
    /// Higher-rank universal quantifier (predicative). The bound vars are
    /// rigid skolems for the body of `ty`; users introduce them via
    /// explicit `forall a. T` syntax. Only legal in function arg/result
    /// positions; never inside Record/Variant/Con/App.
    Forall(Vec<TyVar>, Box<Ty>),
    /// Named type alias preserved through inference. The wrapped type is
    /// the fully-resolved expansion. Unification and structural matching
    /// look through the alias; display preserves the name so type hints
    /// reference the alias instead of the expanded form.
    Alias(String, Box<Ty>),
    /// Error sentinel — suppresses cascading errors.
    Error,
}

impl Ty {
    fn unit() -> Ty {
        Ty::Record(BTreeMap::new(), None)
    }

    /// Strip outer `Ty::Alias` wrappers to expose the underlying type.
    /// Used at structural-inspection sites (case exhaustiveness, unary
    /// ops, monad detection, etc.) so callers don't have to handle the
    /// wrapper case explicitly. Unification peels at the entry point, so
    /// most other call sites don't need this helper.
    fn peel_alias(&self) -> &Ty {
        let mut t = self;
        while let Ty::Alias(_, inner) = t {
            t = inner;
        }
        t
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

    /// TyVars allocated as rigid skolems by `check_expr` against a
    /// `Ty::Forall`. Skolems represent universally-quantified variables
    /// inside a higher-rank check. Unification refuses to bind a skolem
    /// to anything other than itself, ensuring the body is generic in
    /// those vars rather than monomorphic to a leaked unification var.
    skolems: HashSet<TyVar>,

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

    /// Tracks `parseJson` application sites for compile-time FromJSON dispatch.
    /// Each entry records (app_span, return_type_var).
    from_json_calls: Vec<(Span, TyVar)>,

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

    /// Names of ADT types declared via `route` / `route ... =` (the only types
    /// `listen` accepts as a handler input). Populated in `pre_register`.
    route_types: HashSet<String>,

    /// `(span, handler_arg_ty, handler_expr)` for each `listen` / `listenOn`
    /// call site. Validated post-inference:
    /// - The resolved arg type must be a route ADT.
    /// - Every leaf return position in the handler body must be a `respond`
    ///   call (or chain to one). This prevents handlers like
    ///   `\req -> bottom` from collapsing a free type variable to `Response`.
    listen_calls: Vec<(Span, Ty, ast::Expr)>,

    /// Whether we are currently inside an IO do-block. When true, `yield expr`
    /// produces `IO {} expr_type` instead of `[expr_type]`, allowing yield to
    /// be used as "return unit" in if/case branches within IO do blocks.
    in_io_do: bool,

    /// Local variables bound directly to a source ref (`x <- *foo` or
    /// `let x = *foo`). Used to recognize incremental `set` patterns where
    /// the value references the source via an alias instead of `*foo`.
    /// Saved and restored across do-blocks (mirrors codegen's
    /// `source_var_binds`).
    source_var_binds: HashMap<String, String>,

    /// In-scope `let pat = expr` bindings inside the current do-block.
    /// Used by the set/replace full-replacement detector so that
    /// `*rel = let_bound_var` is correctly classified as incremental
    /// when the let body references the source.  Mirrors codegen's
    /// `let_bindings`.
    let_bindings: HashMap<String, ast::Expr>,

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

    /// Spans of `elem` haystack args whose element type is SQL-pushable
    /// (Text/Float/Bool). Recorded during App inference, exported for codegen.
    elem_pushdown_ok: ElemPushdownOk,
}

// ── Core operations ───────────────────────────────────────────────

impl Infer {
    fn new() -> Self {
        Self {
            next_var: 0,
            subst: HashMap::new(),
            skolems: HashSet::new(),
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
            from_json_calls: Vec::new(),
            trait_method_traits: HashMap::new(),
            trait_method_param_vars: HashMap::new(),
            known_impls: HashSet::new(),
            deferred_constraints: Vec::new(),
            binding_types: Vec::new(),
            trait_params: HashMap::new(),
            fetch_response_types: HashMap::new(),
            fetch_response_headers: HashMap::new(),
            route_types: HashSet::new(),
            listen_calls: Vec::new(),
            in_io_do: false,
            in_atomic: false,
            source_var_binds: HashMap::new(),
            let_bindings: HashMap::new(),
            next_unit_var: 0,
            unit_subst: HashMap::new(),
            declared_units: HashMap::new(),
            annotation_unit_vars: HashMap::new(),
            in_type_annotation: false,
            show_unit_strings: HashMap::new(),
            refined_types: HashMap::new(),
            refine_vars: Vec::new(),
            elem_pushdown_ok: HashSet::new(),
        }
    }

    fn fresh(&mut self) -> Ty {
        Ty::Var(self.fresh_var())
    }

    /// Whether a resolved haystack type for `elem` is SQL-pushable: it must
    /// be `[a]` (`Ty::Relation`) and `a` must be a scalar (Int/Text/Float/Bool)
    /// — ADTs/Records would JSON-encode as objects and don't compare cleanly.
    fn is_elem_haystack_pushable(&self, ty: &Ty) -> bool {
        let peeled = ty.peel_alias();
        let inner = match peeled {
            Ty::Relation(t) => self.apply(t),
            _ => return false,
        };
        self.is_sql_pushable_scalar_for_elem(&inner)
    }

    fn is_sql_pushable_scalar_for_elem(&self, ty: &Ty) -> bool {
        match ty.peel_alias() {
            Ty::Int | Ty::Text | Ty::Float | Ty::Bool | Ty::IntUnit(_) | Ty::FloatUnit(_) => true,
            // Refined nominal alias `type Nat = Int where ...` shows up as
            // `Con(name, [])`; recurse to its base type.
            Ty::Con(name, args) if args.is_empty() => {
                self.refined_types
                    .get(name)
                    .map(|(base, _)| self.is_sql_pushable_scalar_for_elem(base))
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    /// Resolve a refined-type alias to its non-refined base, following alias
    /// chains and detecting cycles. Returns `None` and emits a diagnostic on
    /// the first cycle so the caller can stop unifying without overflowing
    /// the stack. The returned `Ty` is guaranteed not to be a refined alias
    /// (or another nullary `Con` whose name is in `refined_types`).
    fn resolve_refined_base(&mut self, name: &str, span: Span) -> Option<Ty> {
        let mut visited: Vec<String> = vec![name.to_string()];
        let mut current = self.refined_types.get(name)?.0.clone();
        loop {
            match &current {
                Ty::Con(n, args) if args.is_empty() && self.refined_types.contains_key(n) => {
                    if visited.iter().any(|v| v == n) {
                        self.error(
                            format!("refined type alias '{}' has a cyclic definition", visited[0]),
                            span,
                        );
                        return None;
                    }
                    visited.push(n.clone());
                    current = self.refined_types[n].0.clone();
                }
                _ => return Some(current),
            }
        }
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
        // Iterate to a fixed point. With well-formed `unit_subst` the chain
        // terminates after one pass per dependency level — the cap is purely a
        // safety net so that a cycle (which would otherwise be a stack overflow)
        // surfaces as a recoverable panic instead of taking the process down.
        // 256 levels is far beyond any sane unit-substitution depth.
        const MAX_ITERATIONS: usize = 256;
        let mut current = u.clone();
        for _ in 0..MAX_ITERATIONS {
            if current.vars.is_empty() {
                return current;
            }
            let mut next = UnitTy { bases: current.bases.clone(), vars: BTreeMap::new() };
            let mut changed = false;
            for (&v, &exp) in &current.vars {
                if let Some(resolved) = self.unit_subst.get(&v) {
                    changed = true;
                    for (name, &base_exp) in &resolved.bases {
                        *next.bases.entry(name.clone()).or_insert(0) += base_exp * exp;
                    }
                    for (&rv, &rexp) in &resolved.vars {
                        *next.vars.entry(rv).or_insert(0) += rexp * exp;
                    }
                } else {
                    *next.vars.entry(v).or_insert(0) += exp;
                }
            }
            next.normalize();
            if !changed {
                return next;
            }
            current = next;
        }
        panic!(
            "knot type inference: unit substitution did not converge within {} iterations — likely a cycle in unit_subst",
            MAX_ITERATIONS
        );
    }

    fn unify_units(&mut self, a: &UnitTy, b: &UnitTy, span: Span) {
        let a = self.apply_unit(a);
        let b = self.apply_unit(b);

        if a == b { return; }

        // Find a variable in either side that we can solve.
        // Prefer solving a variable that appears on only one side.
        let a_only_vars: Vec<UnitVar> = a.vars.keys()
            .filter(|v| !b.vars.contains_key(v))
            .copied().collect();
        let b_only_vars: Vec<UnitVar> = b.vars.keys()
            .filter(|v| !a.vars.contains_key(v))
            .copied().collect();

        if let Some(&va) = a_only_vars.first() {
            // Solve va: a_bases + va^ea + shared_vars = b_bases + b_vars
            // => va^ea = (b - a_without_va)
            let ea = a.vars[&va];
            let mut solution = b.div(&UnitTy { bases: a.bases.clone(), vars: a.vars.iter()
                .filter(|(k, _)| **k != va).map(|(&k, &v)| (k, v)).collect() });
            // Scale by 1/ea if ea != 1
            if ea != 1 {
                // Can only solve cleanly if all exponents divide evenly
                let mut clean = true;
                for exp in solution.bases.values() {
                    if exp % ea != 0 { clean = false; break; }
                }
                for exp in solution.vars.values() {
                    if exp % ea != 0 { clean = false; break; }
                }
                if clean {
                    for exp in solution.bases.values_mut() { *exp /= ea; }
                    for exp in solution.vars.values_mut() { *exp /= ea; }
                    solution.normalize();
                } else {
                    self.error(
                        format!("unit mismatch: {} vs {}", a.display(), b.display()),
                        span,
                    );
                    return;
                }
            }
            solution.normalize();
            self.unit_subst.insert(va, solution);
        } else if let Some(&vb) = b_only_vars.first() {
            // Symmetric: solve vb
            let eb = b.vars[&vb];
            let mut solution = a.div(&UnitTy { bases: b.bases.clone(), vars: b.vars.iter()
                .filter(|(k, _)| **k != vb).map(|(&k, &v)| (k, v)).collect() });
            if eb != 1 {
                let mut clean = true;
                for exp in solution.bases.values() {
                    if exp % eb != 0 { clean = false; break; }
                }
                for exp in solution.vars.values() {
                    if exp % eb != 0 { clean = false; break; }
                }
                if clean {
                    for exp in solution.bases.values_mut() { *exp /= eb; }
                    for exp in solution.vars.values_mut() { *exp /= eb; }
                    solution.normalize();
                } else {
                    self.error(
                        format!("unit mismatch: {} vs {}", a.display(), b.display()),
                        span,
                    );
                    return;
                }
            }
            solution.normalize();
            self.unit_subst.insert(vb, solution);
        } else if a.vars.is_empty() && b.vars.is_empty() {
            // No variables — bases must match exactly
            if a.bases != b.bases {
                self.error(
                    format!("unit mismatch: {} vs {}", a.display(), b.display()),
                    span,
                );
            }
        } else {
            // Both sides share all variables — check if difference is concrete
            let diff = a.div(&b);
            if !diff.is_dimensionless() {
                self.error(
                    format!("unit mismatch: {} vs {}", a.display(), b.display()),
                    span,
                );
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
            Ty::IO(effects, row, inner) => {
                let inner = self.apply(inner);
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                Ty::IO(effects, row, Box::new(inner))
            }
            Ty::EffectRow(effects, row) => {
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                Ty::EffectRow(effects, row)
            }
            Ty::IntUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() { Ty::Int } else { Ty::IntUnit(u) }
            }
            Ty::FloatUnit(u) => {
                let u = self.apply_unit(u);
                if u.is_dimensionless() { Ty::Float } else { Ty::FloatUnit(u) }
            }
            Ty::Forall(vars, inner) => {
                Ty::Forall(vars.clone(), Box::new(self.apply(inner)))
            }
            Ty::Alias(name, inner) => {
                Ty::Alias(name.clone(), Box::new(self.apply(inner)))
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
                Ty::IO(BTreeSet::new(), None, Box::new(a))
            }
            Ty::TyCon(name) => Ty::Con(name, vec![a]),
            Ty::Con(name, mut args) => {
                args.push(a);
                Ty::Con(name, args)
            }
            _ => Ty::App(Box::new(f), Box::new(a)),
        }
    }

    // ── Effect-row helpers ───────────────────────────────────────

    /// Walk an effect-row tail through the substitution, merging any
    /// effects that have been bound to the chain. Returns the fully
    /// resolved (effects, tail) pair.
    fn resolve_effect_row(
        &self,
        mut effects: BTreeSet<IoEffect>,
        mut row: Option<TyVar>,
    ) -> (BTreeSet<IoEffect>, Option<TyVar>) {
        while let Some(rv) = row {
            match self.subst.get(&rv) {
                Some(Ty::EffectRow(extras, tail)) => {
                    for e in extras {
                        effects.insert(e.clone());
                    }
                    row = *tail;
                }
                Some(Ty::Var(other)) => {
                    if *other == rv {
                        break;
                    }
                    row = Some(*other);
                }
                Some(_) => break,
                None => break,
            }
        }
        (effects, row)
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
            Ty::IO(_, row, inner) => {
                if let Some(rv) = row {
                    if *rv == var {
                        return true;
                    }
                    if let Some(resolved) = self.subst.get(rv) {
                        if self.occurs_in(var, resolved) {
                            return true;
                        }
                    }
                }
                self.occurs_in(var, inner)
            }
            Ty::EffectRow(_, row) => {
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
            Ty::Forall(bound, inner) => {
                if bound.contains(&var) {
                    false
                } else {
                    self.occurs_in(var, inner)
                }
            }
            Ty::Alias(_, inner) => self.occurs_in(var, inner),
            _ => false,
        }
    }

    /// Bind a unification variable to a type. Refuses to bind skolems
    /// (rigid variables introduced by higher-rank checking) and emits a
    /// diagnostic instead — keeping universally-quantified parameters
    /// from collapsing into their concrete usage.
    fn bind_var(&mut self, v: TyVar, ty: Ty, span: Span) {
        if self.skolems.contains(&v) {
            // Allow self-binding (already handled by Var(a)==Var(b)).
            if let Ty::Var(other) = &ty {
                if *other == v {
                    return;
                }
            }
            self.error(
                format!(
                    "rigid type variable would escape: cannot unify with {}",
                    self.display_ty(&ty)
                ),
                span,
            );
            return;
        }
        if self.occurs_in(v, &ty) {
            self.error("infinite type".into(), span);
            return;
        }
        self.subst.insert(v, ty);
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
            // Peel alias wrappers — they're transparent to unification.
            (Ty::Alias(_, inner), _) => {
                let inner = (**inner).clone();
                self.unify(&inner, &t2, span);
                return;
            }
            (_, Ty::Alias(_, inner)) => {
                let inner = (**inner).clone();
                self.unify(&t1, &inner, span);
                return;
            }
            // Forall types: instantiate with fresh vars and unify the body.
            // Two Forall types with the same shape unify by α-renaming both
            // sides to fresh vars; a Forall vs. a non-Forall type instantiates
            // the polymorphic side at the monomorphic side's witness.
            (Ty::Forall(vars, body), _) => {
                let scheme = Scheme {
                    vars: vars.clone(),
                    unit_vars: vec![],
                    constraints: vec![],
                    ty: (**body).clone(),
                };
                let inst = self.instantiate_at(&scheme, span);
                self.unify(&inst, &t2, span);
                return;
            }
            (_, Ty::Forall(vars, body)) => {
                let scheme = Scheme {
                    vars: vars.clone(),
                    unit_vars: vec![],
                    constraints: vec![],
                    ty: (**body).clone(),
                };
                let inst = self.instantiate_at(&scheme, span);
                self.unify(&t1, &inst, span);
                return;
            }
            (Ty::Var(a), Ty::Var(b)) if a == b => {}
            (Ty::Var(a), Ty::Var(b)) => {
                // When unifying two variables, bind the non-skolem one
                // toward the other. If both are skolems, neither can be
                // bound — error.
                let a = *a;
                let b = *b;
                if !self.skolems.contains(&a) {
                    self.bind_var(a, Ty::Var(b), span);
                } else if !self.skolems.contains(&b) {
                    self.bind_var(b, Ty::Var(a), span);
                } else {
                    self.error(
                        format!(
                            "cannot unify rigid type variables {} and {}",
                            self.display_ty(&Ty::Var(a)),
                            self.display_ty(&Ty::Var(b))
                        ),
                        span,
                    );
                }
            }
            (Ty::Var(v), _) => {
                let v = *v;
                self.bind_var(v, t2.clone(), span);
            }
            (_, Ty::Var(v)) => {
                let v = *v;
                self.bind_var(v, t1.clone(), span);
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
            // App(f, a) vs IO(effects, row, b) → f = IO, a = b
            (Ty::App(f, a), Ty::IO(_effects, _row, b))
            | (Ty::IO(_effects, _row, b), Ty::App(f, a)) => {
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
            // ── IO monad with effect-row unification ──────────
            (Ty::IO(e1, r1, a), Ty::IO(e2, r2, b)) => {
                let e1 = e1.clone();
                let r1 = *r1;
                let e2 = e2.clone();
                let r2 = *r2;
                let a = a.clone();
                let b = b.clone();
                self.unify(&a, &b, span);
                // If at least one side started as a `Ty::Var` (an inferred
                // IO from a case-arm or if-branch), widen its effect set to
                // the union instead of running strict row unification —
                // mirrors the pre-row-polymorphism behavior so case/if
                // arms with different effects merge cleanly.
                if (var1.is_some() || var2.is_some())
                    && r1.is_none()
                    && r2.is_none()
                    && e1 != e2
                {
                    let mut merged = e1.clone();
                    merged.extend(e2.iter().cloned());
                    let unified_inner = self.apply(&a);
                    let merged_io =
                        Ty::IO(merged, None, Box::new(unified_inner));
                    if let Some(v) = var1 {
                        self.subst.insert(v, merged_io.clone());
                    }
                    if let Some(v) = var2 {
                        self.subst.insert(v, merged_io);
                    }
                } else {
                    self.unify_io_effects(&e1, r1, &e2, r2, span);
                }
            }
            (Ty::EffectRow(e1, r1), Ty::EffectRow(e2, r2)) => {
                let e1 = e1.clone();
                let r1 = *r1;
                let e2 = e2.clone();
                let r2 = *r2;
                self.unify_io_effects(&e1, r1, &e2, r2, span);
            }
            // In IO do blocks, allow Relation types to unify with IO or
            // Unit types. Route handlers mix relational operations and
            // `respond` calls in if/case branches.
            (Ty::Relation(a), Ty::IO(_, _, b))
            | (Ty::IO(_, _, b), Ty::Relation(a)) if self.in_io_do => {
                self.unify(a, b, span);
            }
            // A route handler's expected return type is `Response`, but a
            // do-block ending in `respond x` is wrapped to `IO _ _ Response`
            // when it contains any IO statement. Treat that wrapper as
            // transparent: the runtime unwraps IO from the handler result
            // before sending the HTTP response.
            (Ty::IO(_, _, inner), Ty::Con(name, args))
            | (Ty::Con(name, args), Ty::IO(_, _, inner))
                if name == "Response" && args.is_empty() =>
            {
                let inner = (**inner).clone();
                self.unify(&inner, &Ty::Con("Response".into(), vec![]), span);
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
            // Refined type subsumption: Con("Nat", []) ↔ Int, etc. Resolve the
            // refined alias to its non-refined base, with cycle detection so
            // `type T = T where ...` or `type A = B / type B = A` diagnoses
            // instead of overflowing the stack.
            (Ty::Con(name, args), other)
                if args.is_empty() && self.refined_types.contains_key(name) =>
            {
                match self.resolve_refined_base(name, span) {
                    Some(base_ty) => {
                        let other = other.clone();
                        self.unify(&base_ty, &other, span);
                    }
                    None => {} // cycle already reported
                }
            }
            (other, Ty::Con(name, args))
                if args.is_empty() && self.refined_types.contains_key(name) =>
            {
                match self.resolve_refined_base(name, span) {
                    Some(base_ty) => {
                        let other = other.clone();
                        self.unify(&other, &base_ty, span);
                    }
                    None => {} // cycle already reported
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
                let target = Ty::Record(only2, None);
                self.bind_var(rv, target, span);
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
                self.bind_var(rv, target, span);
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "record fields don't match".into(),
                            span,
                        );
                    }
                } else if only1.is_empty() && only2.is_empty() {
                    // Both rows match exactly — link them via `unify` so
                    // skolem-vs-unification-var binding is directed
                    // toward the non-skolem.
                    self.unify(&Ty::Var(rv1), &Ty::Var(rv2), span);
                } else {
                    let rv1_skolem = self.skolems.contains(&rv1);
                    let rv2_skolem = self.skolems.contains(&rv2);
                    match (rv1_skolem, rv2_skolem) {
                        // Skolem on one side with no extras to absorb:
                        // keep the rigid tail intact and bind the free row
                        // var to a record using the skolem as its tail.
                        (true, false) if only2.is_empty() => {
                            let target = Ty::Record(only1, Some(rv1));
                            self.bind_var(rv2, target, span);
                        }
                        (false, true) if only1.is_empty() => {
                            let target = Ty::Record(only2, Some(rv2));
                            self.bind_var(rv1, target, span);
                        }
                        _ => {
                            let fresh = self.fresh_var();
                            let t1 = Ty::Record(only2, Some(fresh));
                            let t2 = Ty::Record(only1, Some(fresh));
                            self.bind_var(rv1, t1, span);
                            self.bind_var(rv2, t2, span);
                        }
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
                self.bind_var(rv, target, span);
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
                self.bind_var(rv, target, span);
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "variant constructors don't match".into(),
                            span,
                        );
                    }
                } else if only1.is_empty() && only2.is_empty() {
                    self.unify(&Ty::Var(rv1), &Ty::Var(rv2), span);
                } else {
                    let rv1_skolem = self.skolems.contains(&rv1);
                    let rv2_skolem = self.skolems.contains(&rv2);
                    match (rv1_skolem, rv2_skolem) {
                        (true, false) if only2.is_empty() => {
                            let target = Ty::Variant(only1, Some(rv1));
                            self.bind_var(rv2, target, span);
                        }
                        (false, true) if only1.is_empty() => {
                            let target = Ty::Variant(only2, Some(rv2));
                            self.bind_var(rv1, target, span);
                        }
                        _ => {
                            let fresh = self.fresh_var();
                            let t1 = Ty::Variant(only2, Some(fresh));
                            let t2 = Ty::Variant(only1, Some(fresh));
                            self.bind_var(rv1, t1, span);
                            self.bind_var(rv2, t2, span);
                        }
                    }
                }
            }
        }
    }

    /// Unify two effect rows. Mirrors `unify_records`/`unify_variants` but
    /// over `BTreeSet<IoEffect>` instead of fielded maps. Effects are
    /// equality-keyed (no inner type to unify on shared elements), so we
    /// only need to ensure each closed side covers the other's extras.
    /// When both rows are closed, subset on either side is allowed —
    /// only effects unique to *both* sides are a true conflict.
    fn unify_io_effects(
        &mut self,
        e1: &BTreeSet<IoEffect>,
        r1: Option<TyVar>,
        e2: &BTreeSet<IoEffect>,
        r2: Option<TyVar>,
        span: Span,
    ) {
        let (e1, r1) = self.resolve_effect_row(e1.clone(), r1);
        let (e2, r2) = self.resolve_effect_row(e2.clone(), r2);

        let only1: BTreeSet<IoEffect> = e1.difference(&e2).cloned().collect();
        let only2: BTreeSet<IoEffect> = e2.difference(&e1).cloned().collect();

        match (r1, r2) {
            (None, None) => {
                if !only1.is_empty() && !only2.is_empty() {
                    let extras: Vec<String> = only1
                        .iter()
                        .chain(only2.iter())
                        .map(format_io_effect)
                        .collect();
                    self.error(
                        format!(
                            "IO effects don't match: extra effects {{{}}}",
                            extras.join(", ")
                        ),
                        span,
                    );
                }
            }
            (Some(rv), None) => {
                if !only1.is_empty() {
                    let names: Vec<String> =
                        only1.iter().map(format_io_effect).collect();
                    self.error(
                        format!(
                            "IO has unexpected effects: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.bind_var(rv, Ty::EffectRow(only2, None), span);
            }
            (None, Some(rv)) => {
                if !only2.is_empty() {
                    let names: Vec<String> =
                        only2.iter().map(format_io_effect).collect();
                    self.error(
                        format!(
                            "IO has unexpected effects: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.bind_var(rv, Ty::EffectRow(only1, None), span);
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "IO effects don't match".into(),
                            span,
                        );
                    }
                } else if only1.is_empty() && only2.is_empty() {
                    self.unify(&Ty::Var(rv1), &Ty::Var(rv2), span);
                } else {
                    let rv1_skolem = self.skolems.contains(&rv1);
                    let rv2_skolem = self.skolems.contains(&rv2);
                    match (rv1_skolem, rv2_skolem) {
                        (true, false) if only2.is_empty() => {
                            self.bind_var(
                                rv2,
                                Ty::EffectRow(only1, Some(rv1)),
                                span,
                            );
                        }
                        (false, true) if only1.is_empty() => {
                            self.bind_var(
                                rv1,
                                Ty::EffectRow(only2, Some(rv2)),
                                span,
                            );
                        }
                        _ => {
                            let fresh = self.fresh_var();
                            self.bind_var(
                                rv1,
                                Ty::EffectRow(only2, Some(fresh)),
                                span,
                            );
                            self.bind_var(
                                rv2,
                                Ty::EffectRow(only1, Some(fresh)),
                                span,
                            );
                        }
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
        // Create deferred constraints for each constraint in the scheme.
        //
        // Most constraints reference a TyVar in `scheme.vars` (e.g.
        // `Ord a => a -> a -> Bool`), which we freshen alongside the type so
        // the constraint follows the freshened variable. A constraint can
        // also reference a variable *not* in `scheme.vars` — that means the
        // constraint applies to a variable from the outer scope (e.g. a
        // generalization corner case where the var is shared with an outer
        // binding). In that case keep the original variable so the
        // constraint still gets discharged in the outer scope rather than
        // being silently dropped.
        for c in &scheme.constraints {
            let target_var = match mapping.get(&c.type_var) {
                Some(Ty::Var(new_var)) => *new_var,
                Some(_) => {
                    debug_assert!(
                        false,
                        "instantiate_at: scheme constraint mapped to non-Var",
                    );
                    continue;
                }
                None => c.type_var,
            };
            self.deferred_constraints.push(DeferredConstraint {
                trait_name: c.trait_name.clone(),
                type_var: target_var,
                span,
            });
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
            Ty::IO(effects, row, inner) => {
                let mut new_effects = effects.clone();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            Ty::EffectRow(extra, extra_row) => {
                                for e in extra {
                                    new_effects.insert(e.clone());
                                }
                                *extra_row
                            }
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::IO(
                    new_effects,
                    new_row,
                    Box::new(self.subst_ty(inner, mapping)),
                )
            }
            Ty::EffectRow(effects, row) => {
                let mut new_effects = effects.clone();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            Ty::EffectRow(extra, extra_row) => {
                                for e in extra {
                                    new_effects.insert(e.clone());
                                }
                                *extra_row
                            }
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::EffectRow(new_effects, new_row)
            }
            Ty::Forall(bound, inner) => {
                // Avoid capturing bound vars: shadow them in the mapping.
                let mut shadowed = mapping.clone();
                for b in bound {
                    shadowed.remove(b);
                }
                Ty::Forall(
                    bound.clone(),
                    Box::new(self.subst_ty(inner, &shadowed)),
                )
            }
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
            Ty::IO(effects, row, inner) => Ty::IO(
                effects.clone(),
                *row,
                Box::new(self.subst_unit_vars_in_ty(inner, mapping)),
            ),
            Ty::EffectRow(effects, row) => Ty::EffectRow(effects.clone(), *row),
            Ty::Forall(bound, inner) => Ty::Forall(
                bound.clone(),
                Box::new(self.subst_unit_vars_in_ty(inner, mapping)),
            ),
            _ => ty.clone(),
        }
    }

    fn subst_unit_var(u: &UnitTy, mapping: &HashMap<UnitVar, UnitVar>) -> UnitTy {
        if u.vars.is_empty() {
            return u.clone();
        }
        let new_vars = u.vars.iter().map(|(&v, &exp)| {
            let new_v = mapping.get(&v).copied().unwrap_or(v);
            (new_v, exp)
        }).collect();
        UnitTy { bases: u.bases.clone(), vars: new_vars }
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
                for &v in applied.vars.keys() {
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
            Ty::IO(_, _, inner) => self.collect_free_unit_vars(inner, out),
            Ty::EffectRow(_, _) => {}
            Ty::Forall(_, inner) => self.collect_free_unit_vars(inner, out),
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
        let env_unit_fv = self.free_unit_vars_in_env();
        let unit_vars: Vec<UnitVar> = self
            .free_unit_vars_in_ty(&applied)
            .into_iter()
            .filter(|u| !env_unit_fv.contains(u))
            .collect();
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
            Ty::IO(_, row, inner) => {
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
                self.collect_free_vars(inner, out);
            }
            Ty::EffectRow(_, row) => {
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
            Ty::Forall(bound, inner) => {
                let mut inner_set = HashSet::new();
                self.collect_free_vars(inner, &mut inner_set);
                for v in bound {
                    inner_set.remove(v);
                }
                out.extend(inner_set);
            }
            Ty::Alias(_, inner) => self.collect_free_vars(inner, out),
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

    fn free_unit_vars_in_env(&self) -> HashSet<UnitVar> {
        let mut s = HashSet::new();
        for scope in &self.scopes {
            for scheme in scope.values() {
                let mut fv = HashSet::new();
                self.collect_free_unit_vars(&scheme.ty, &mut fv);
                for u in &scheme.unit_vars {
                    fv.remove(u);
                }
                s.extend(fv);
            }
        }
        for ty in self.source_types.values() {
            self.collect_free_unit_vars(ty, &mut s);
        }
        for ty in self.derived_types.values() {
            self.collect_free_unit_vars(ty, &mut s);
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
                        // Wrap nullary alias references so the name flows
                        // through inference into LSP type hints. Skip the
                        // wrapper when the alias already names itself
                        // (e.g. data-as-alias for single-variant ADTs).
                        match &aliased {
                            Ty::Con(n, args) if n == name && args.is_empty() => aliased,
                            Ty::Alias(n, _) if n == name => aliased,
                            _ => Ty::Alias(name.clone(), Box::new(aliased)),
                        }
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
            ast::TypeKind::IO { effects, rest, ty: inner_ty } => {
                let io_effects = ast_effects_to_io_effects(effects);
                // `_` (wildcard) gets a fresh row variable per occurrence so
                // multiple `_`s don't accidentally unify; named variables share
                // a fresh var across the same annotation scope.
                let row_var = rest.as_ref().map(|name| {
                    if name == "_" {
                        self.fresh_var()
                    } else {
                        self.annotation_var(name)
                    }
                });
                Ty::IO(
                    io_effects,
                    row_var,
                    Box::new(self.ast_type_to_ty(inner_ty)),
                )
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

            ast::TypeKind::Forall { vars, ty: inner } => {
                // Allocate fresh TyVars for the bound names and shadow any
                // existing annotation_vars binding for the duration of the
                // body, then restore. This keeps inner-quantified vars
                // separate from outer-scope annotation vars.
                let saved: Vec<(String, Option<TyVar>)> = vars
                    .iter()
                    .map(|v| (v.clone(), self.annotation_vars.get(v).copied()))
                    .collect();
                let bound: Vec<TyVar> = vars
                    .iter()
                    .map(|v| {
                        let fv = self.fresh_var();
                        self.annotation_vars.insert(v.clone(), fv);
                        fv
                    })
                    .collect();
                let inner_ty = self.ast_type_to_ty(inner);
                for (name, prev) in saved {
                    match prev {
                        Some(v) => {
                            self.annotation_vars.insert(name, v);
                        }
                        None => {
                            self.annotation_vars.remove(&name);
                        }
                    }
                }
                Ty::Forall(bound, Box::new(inner_ty))
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

    /// Render ` {e1, e2 | r}` for an effect set with optional row tail. An
    /// empty closed set renders as ` {}` — IO and Effects always carry an
    /// effects row, even when empty.
    fn display_effect_set(
        &self,
        effects: &BTreeSet<IoEffect>,
        row: Option<TyVar>,
    ) -> String {
        let mut parts: Vec<String> =
            effects.iter().map(format_io_effect).collect();
        if let Some(rv) = row {
            let name = match self.subst.get(&rv) {
                Some(resolved) => self.display_ty(resolved),
                None => {
                    let idx = rv as usize;
                    if idx < 26 {
                        format!("{}", (b'a' + idx as u8) as char)
                    } else {
                        format!("e{}", rv)
                    }
                }
            };
            parts.push(format!("| {}", name));
        }
        format!(" {{{}}}", parts.join(", "))
    }

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
            Ty::IO(effects, row, inner) => {
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                format!(
                    "IO{} {}",
                    self.display_effect_set(&effects, row),
                    self.display_ty(inner),
                )
            }
            Ty::EffectRow(effects, row) => {
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                format!("Effects{}", self.display_effect_set(&effects, row))
            }
            Ty::Forall(vars, inner) => {
                if vars.is_empty() {
                    self.display_ty_inner(inner, in_fun)
                } else {
                    let names: Vec<String> = vars
                        .iter()
                        .map(|v| {
                            let idx = *v as usize;
                            if idx < 26 {
                                format!("{}", (b'a' + idx as u8) as char)
                            } else {
                                format!("t{}", v)
                            }
                        })
                        .collect();
                    let s = format!(
                        "forall {}. {}",
                        names.join(" "),
                        self.display_ty_inner(inner, false)
                    );
                    if in_fun {
                        format!("({})", s)
                    } else {
                        s
                    }
                }
            }
            Ty::Alias(name, _) => name.clone(),
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
                    let mut effects = BTreeSet::new();
                    effects.insert(IoEffect::Reads(name.clone()));
                    Ty::IO(effects, None, Box::new(ty))
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
                    // A derived relation's reads aren't known at this site;
                    // the effect-checker pass tracks them. Type-system effects
                    // start empty here and grow via unification.
                    Ty::IO(BTreeSet::new(), None, Box::new(ty))
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
                self.unify(&base_ty, &constraint, base.span);
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

                // Higher-rank arg slot: when the function's parameter is
                // a `Ty::Forall`, check the argument against the Forall's
                // body so the arg can be used polymorphically inside the
                // callee. Predicative — relies on later escape checks.
                let func_applied = self.apply(&func_ty);
                if let Ty::Fun(arg_slot, ret_ty) = &func_applied {
                    let arg_slot_resolved = self.apply(arg_slot);
                    if matches!(arg_slot_resolved, Ty::Forall(..)) {
                        self.check_expr(arg, &arg_slot_resolved);
                        let result_ty = (**ret_ty).clone();
                        if let ast::ExprKind::Var(name) = &func.node {
                            if name == "parseJson" {
                                if let Ty::Var(v) = &result_ty {
                                    self.from_json_calls.push((expr.span, *v));
                                }
                            }
                        }
                        return result_ty;
                    }
                }

                let arg_ty = self.infer_expr(arg);
                let result_ty = self.fresh();
                let expected = Ty::Fun(
                    Box::new(arg_ty.clone()),
                    Box::new(result_ty.clone()),
                );
                self.unify(&func_ty, &expected, arg.span);

                // Track parseJson calls for compile-time FromJSON dispatch
                if let ast::ExprKind::Var(name) = &func.node {
                    if name == "parseJson" {
                        if let Ty::Var(v) = &result_ty {
                            self.from_json_calls.push((expr.span, *v));
                        }
                    }
                }

                // Track `elem needle haystack` haystack types for SQL pushdown.
                // Curried: outer App's func is `App(Var("elem"), needle)`,
                // outer App's arg is the haystack. Record only when the
                // haystack's element type is a SQL-pushable scalar.
                if let ast::ExprKind::App { func: inner_f, .. } = &func.node {
                    if let ast::ExprKind::Var(name) = &inner_f.node {
                        if name == "elem" {
                            let resolved = self.apply(&arg_ty);
                            if self.is_elem_haystack_pushable(&resolved) {
                                self.elem_pushdown_ok.insert(arg.span);
                            }
                        }
                    }
                }

                // Track `listen` / `listenOn` call sites so we can verify
                // post-inference that the handler's argument type is a
                // route ADT. Without this, identity-like handlers
                // (`\req -> req`) bypass the `Response` constraint by
                // collapsing the handler's input and output types.
                if let Some(handler_arg_ty) = self.detect_listen_handler_arg(expr, &arg_ty) {
                    self.listen_calls.push((expr.span, handler_arg_ty, (**arg).clone()));
                }

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
                        match resolved.peel_alias() {
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
                self.unify(&then_ty, &else_ty, else_branch.span);
                // Merge IO effects from both branches — unify only checks
                // inner types and discards effect sets.
                let applied_then = self.apply(&then_ty);
                let applied_else = self.apply(&else_ty);
                match (&applied_then, &applied_else) {
                    (Ty::IO(e1, r1, inner), Ty::IO(e2, r2, _)) => {
                        let mut merged = e1.clone();
                        merged.extend(e2.iter().cloned());
                        Ty::IO(merged, r1.or(*r2), inner.clone())
                    }
                    // When one branch is IO and the other Relation, prefer IO.
                    // This handles functions whose IO nature wasn't detected
                    // due to declaration ordering (callee inferred after caller).
                    (Ty::IO(e, r, inner), Ty::Relation(_))
                    | (Ty::Relation(_), Ty::IO(e, r, inner)) => {
                        Ty::IO(e.clone(), *r, inner.clone())
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
                    if let Ty::IO(ref effects, _, _) = applied {
                        case_io_effects.extend(effects.iter().cloned());
                    }
                    self.pop_scope();
                }

                self.check_exhaustiveness(&scrut_ty, arms, expr.span);

                // Merge IO effects from all arms into the result type
                if !case_io_effects.is_empty() {
                    let applied_result = self.apply(&result_ty);
                    if let Ty::IO(_, row, inner) = applied_result {
                        Ty::IO(case_io_effects, row, inner)
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
                let target_applied = self.apply(&target_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(_, _, inner) => (**inner).clone(),
                    other => other.clone(),
                };
                let target_inner = unwrap_io(&target_applied);
                // Push target's element type into the value so element-level
                // mismatches highlight just the offending element.
                self.check_expr(value, &target_inner);
                let mut effects = BTreeSet::new();
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    effects.insert(IoEffect::Writes(name.clone()));
                    effects.insert(IoEffect::Reads(name.clone()));

                    // Require `replace *rel = ...` when the value is a full
                    // replacement (doesn't reference *rel directly or via a
                    // local alias `xs <- *rel`). Skip views and scalar
                    // sources where the distinction is meaningless.
                    let is_view = self.view_names.contains(name);
                    let is_relation = matches!(
                        self.source_types.get(name),
                        Some(Ty::Relation(_))
                    );
                    if !is_view
                        && is_relation
                        && !value_references_source(
                            value,
                            name,
                            &self.source_var_binds,
                            &self.let_bindings,
                        )
                    {
                        self.error(
                            format!(
                                "`*{name} = ...` must reference `*{name}` \
                                 (directly or via a `<- *{name}` bind); \
                                 use `replace *{name} = ...` for a full replacement"
                            ),
                            expr.span,
                        );
                    }
                }
                Ty::IO(effects, None, Box::new(Ty::unit()))
            }

            ast::ExprKind::ReplaceSet { target, value } => {
                let target_ty = self.infer_expr(target);
                let target_applied = self.apply(&target_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(_, _, inner) => (**inner).clone(),
                    other => other.clone(),
                };
                let target_inner = unwrap_io(&target_applied);
                self.check_expr(value, &target_inner);
                let mut effects = BTreeSet::new();
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    effects.insert(IoEffect::Writes(name.clone()));
                    effects.insert(IoEffect::Reads(name.clone()));

                    // Reject `replace *rel = ...` when the value references
                    // `*rel` (directly, via a `<- *rel` bind, or via a let
                    // binding that ultimately reads from `*rel`) — `set`
                    // would produce the same final state more efficiently.
                    // Skip views and scalar sources where the distinction
                    // is meaningless.
                    let is_view = self.view_names.contains(name);
                    let is_relation = matches!(
                        self.source_types.get(name),
                        Some(Ty::Relation(_))
                    );
                    if !is_view
                        && is_relation
                        && value_references_source(
                            value,
                            name,
                            &self.source_var_binds,
                            &self.let_bindings,
                        )
                    {
                        self.error(
                            format!(
                                "`replace *{name} = ...` is unnecessary when \
                                 the value references `*{name}` \
                                 (directly or via a `<- *{name}` bind); \
                                 use `*{name} = ...` instead"
                            ),
                            expr.span,
                        );
                    }
                }
                Ty::IO(effects, None, Box::new(Ty::unit()))
            }

            ast::ExprKind::Atomic(inner) => {
                let prev = self.in_atomic;
                self.in_atomic = true;
                let inner_ty = self.infer_expr(inner);
                self.in_atomic = prev;
                // atomic : IO {} a -> IO {} a
                let inner_applied = self.apply(&inner_ty);
                match &inner_applied {
                    Ty::IO(_, _, _) => inner_applied,
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
                self.unify(&inner_ty, &annot_ty, inner.span);
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

    /// Bidirectional checking entry point. Infers `expr` and unifies the
    /// result against `expected`. Specialised cases push `expected` down
    /// to enable higher-rank types — when a lambda parameter's expected
    /// type is `forall vs. body`, the param is bound polymorphically.
    fn check_expr(&mut self, expr: &ast::Expr, expected: &Ty) {
        // Higher-rank: when the expected type is `forall vs. body`,
        // skolemise the bound vars (mark them rigid in `self.skolems`)
        // and recurse with the skolemised body. After the check, drop
        // the skolems. Unification refuses to bind a skolem, so leaks
        // surface as type errors at the offending site.
        let resolved = self.apply(expected);
        if let Ty::Forall(vars, body) = resolved {
            let mut fresh_skolems: Vec<TyVar> = Vec::with_capacity(vars.len());
            let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
            for v in &vars {
                let s = self.fresh_var();
                self.skolems.insert(s);
                fresh_skolems.push(s);
                mapping.insert(*v, Ty::Var(s));
            }
            let body_skolemised = self.subst_ty(&body, &mapping);
            self.check_expr(expr, &body_skolemised);
            for s in fresh_skolems {
                self.skolems.remove(&s);
            }
            return;
        }
        match &expr.node {
            ast::ExprKind::Annot { expr: inner, ty } => {
                let annot_ty = self.ast_type_to_ty(ty);
                self.check_expr(inner, &annot_ty);
                self.unify(&annot_ty, expected, ty.span);
            }
            ast::ExprKind::Lambda { params, body } => {
                // Peel `Fun(p, r)` off `expected` for each lambda param,
                // resolving substitutions as we go. If the expected type
                // turns out to have fewer arrows than the lambda has
                // params, fall back to synthesise + unify (mono).
                let resolved = self.apply(expected);
                let mut current = resolved;
                let mut peeled: Vec<Ty> = Vec::new();
                for _ in params {
                    match current {
                        Ty::Fun(p, r) => {
                            peeled.push(*p);
                            current = self.apply(&r);
                        }
                        other => {
                            // Not enough arrows — fall back to inference.
                            current = other;
                            break;
                        }
                    }
                }
                if peeled.len() == params.len() {
                    self.push_scope();
                    for (param, p_ty) in params.iter().zip(peeled.iter()) {
                        self.check_pattern(param, p_ty);
                    }
                    self.check_expr(body, &current);
                    self.pop_scope();
                } else {
                    let inferred = self.infer_expr(expr);
                    self.unify(expected, &inferred, expr.span);
                }
            }
            ast::ExprKind::Do(stmts) => {
                // Bidirectional hint: if the expected type is `IO _ _`, set
                // `in_io_do` so a do-block with only `yield x` (no IO stmts,
                // no relation binds) is inferred as IO instead of defaulting
                // to Relation. `infer_do` ORs this with `stmt_has_io`, so the
                // hint propagates while still letting genuinely IO statements
                // turn it on bottom-up.
                let resolved_expected = self.apply(expected);
                let prev_in_io_do = self.in_io_do;
                if matches!(resolved_expected, Ty::IO(_, _, _)) {
                    self.in_io_do = true;
                }
                let inferred = self.infer_do(stmts, expr.span);
                self.in_io_do = prev_in_io_do;
                self.unify(expected, &inferred, do_result_span(stmts, expr.span));
            }
            ast::ExprKind::Record(fields) if !fields.is_empty() => {
                // Bidirectional record checking: when the expected type is a
                // closed record, push each field's expected type down so a
                // mismatch lights up just the offending field value, not the
                // whole record literal.
                let resolved = self.apply(expected);
                if let Ty::Record(expected_fields, None) = resolved.peel_alias() {
                    let expected_fields = expected_fields.clone();
                    let mut field_tys = BTreeMap::new();
                    for f in fields {
                        if let Some(exp_ty) = expected_fields.get(&f.name) {
                            self.check_expr(&f.value, exp_ty);
                            field_tys.insert(f.name.clone(), exp_ty.clone());
                        } else {
                            let val_ty = self.infer_expr(&f.value);
                            field_tys.insert(f.name.clone(), val_ty);
                        }
                    }
                    self.unify(expected, &Ty::Record(field_tys, None), expr.span);
                } else {
                    let inferred = self.infer_expr(expr);
                    self.unify(expected, &inferred, expr.span);
                }
            }
            ast::ExprKind::If { cond, then_branch, else_branch } => {
                // Push expected into both branches so a mismatch lights up
                // just the offending branch instead of the whole if.
                let cond_ty = self.infer_expr(cond);
                self.unify(&cond_ty, &Ty::Bool, cond.span);
                let then_ty = self.infer_expr(then_branch);
                self.unify(expected, &then_ty, then_branch.span);
                let else_ty = self.infer_expr(else_branch);
                self.unify(expected, &else_ty, else_branch.span);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                // Push expected into each arm body so a mismatch lights up
                // just the offending arm instead of the whole case.
                let scrut_ty = self.infer_expr(scrutinee);
                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify(expected, &body_ty, arm.body.span);
                    self.pop_scope();
                }
                self.check_exhaustiveness(&scrut_ty, arms, expr.span);
            }
            ast::ExprKind::List(elems) if !elems.is_empty() => {
                // When expected is `[T]`, push T into each element so a
                // mismatch lights up just the offending element instead of
                // the whole list literal.
                let resolved = self.apply(expected);
                if let Ty::Relation(elem_ty) = resolved.peel_alias() {
                    let elem_ty = (**elem_ty).clone();
                    for e in elems {
                        self.check_expr(e, &elem_ty);
                    }
                } else {
                    let inferred = self.infer_expr(expr);
                    self.unify(expected, &inferred, expr.span);
                }
            }
            _ => {
                let inferred = self.infer_expr(expr);
                self.unify(expected, &inferred, expr.span);
            }
        }
    }

    /// If `expr` is `listen port handler` or `listenOn host port handler`,
    /// return the handler argument's input type so it can be validated
    /// post-inference. The check is anchored on the outer App so it fires
    /// once per call site (inner curried Apps have too few arguments).
    fn detect_listen_handler_arg(&self, expr: &ast::Expr, arg_ty: &Ty) -> Option<Ty> {
        let (root, args) = uncurry_fetch(expr);
        let name = match &root.node {
            ast::ExprKind::Var(n) => n.as_str(),
            _ => return None,
        };
        let expected_args = match name {
            "listen" => 2,
            "listenOn" => 3,
            _ => return None,
        };
        if args.len() != expected_args {
            return None;
        }
        // The handler is the last argument; `arg_ty` is its inferred type
        // (a function type). Pull out its parameter type.
        if let Ty::Fun(param, _) = arg_ty {
            Some((**param).clone())
        } else {
            None
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
            None,
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
                // If the expected type is `forall vs. body`, bind the var
                // with a polymorphic Scheme so each use freshly instantiates
                // the quantified variables. This is what makes higher-rank
                // arguments usable at multiple types inside the body.
                let scheme = match self.apply(expected) {
                    Ty::Forall(vars, body) => Scheme {
                        vars,
                        unit_vars: vec![],
                        constraints: vec![],
                        ty: *body,
                    },
                    _ => Scheme::mono(expected.clone()),
                };
                self.bind(name, scheme);
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
        // Resolve the scrutinee type through substitution and peel any
        // alias wrappers so exhaustiveness sees the underlying ADT shape.
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

        match resolved.peel_alias() {
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
                (crate::builtins::is_io_builtin(name) || name == "fork")
                || self.lookup(name).map_or(false, |scheme| {
                    fn returns_io(ty: &Ty) -> bool {
                        match ty {
                            Ty::IO(_, _, _) => true,
                            Ty::Fun(_, ret) => returns_io(ret),
                            _ => false,
                        }
                    }
                    let resolved = self.apply(&scheme.ty);
                    returns_io(&resolved)
                })
            }
            ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => true,
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. } => true,
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
            ast::ExprKind::UnitLit { value, .. } => self.expr_is_io_prescan(value),
            ast::ExprKind::Annot { expr, .. } => self.expr_is_io_prescan(expr),
            ast::ExprKind::Refine(inner) => self.expr_is_io_prescan(inner),
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
        // Effect row tail accumulated from IO statements. Multiple row vars
        // are unified so the do-block's result has a single polymorphic
        // tail. None means the block's effects are fully closed.
        let mut io_row: Option<TyVar> = None;

        // Pre-scan: if any statement uses IO builtins, set in_io_do so that
        // `yield` expressions inside case/if branches produce IO types.
        // Preserve any outer hint (from `check_expr` against an `IO _ _`
        // expected type) — yield-only blocks rely on the hint to promote.
        let prev_in_io_do = self.in_io_do;
        self.in_io_do = self.in_io_do || self.stmt_has_io(stmts);

        // Save source aliases so binds inside this do-block don't leak out.
        let prev_source_var_binds = self.source_var_binds.clone();
        let prev_let_bindings = self.let_bindings.clone();

        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    let resolved = self.apply(&expr_ty);
                    let is_ctor_pat =
                        matches!(&pat.node, ast::PatKind::Constructor { .. });

                    if let Ty::IO(ref effects, ref row, ref inner) = resolved {
                        // IO bind: x <- ioAction
                        is_io = true;
                        io_effects.extend(effects.iter().cloned());
                        if let Some(rv) = row {
                            match io_row {
                                None => io_row = Some(*rv),
                                Some(existing) if existing != *rv => {
                                    self.unify(
                                        &Ty::Var(existing),
                                        &Ty::Var(*rv),
                                        expr.span,
                                    );
                                }
                                _ => {}
                            }
                        }
                        self.check_pattern(pat, inner);
                    } else if self.in_io_do && matches!(&resolved, Ty::Var(_)) {
                        // In an IO do-block with an unresolved type variable —
                        // assume IO so we don't incorrectly unify with Relation.
                        is_io = true;
                        let inner_ty = self.fresh();
                        self.unify(
                            &expr_ty,
                            &Ty::IO(BTreeSet::new(), None, Box::new(inner_ty.clone())),
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

                    // Track `x <- *foo` for `set` full-replacement detection.
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        if let ast::ExprKind::SourceRef(source_name) = &expr.node {
                            self.source_var_binds
                                .insert(var_name.clone(), source_name.clone());
                        }
                    }
                }
                ast::StmtKind::Let { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    let resolved = self.apply(&expr_ty);
                    if let Ty::IO(ref effects, ref row, _) = resolved {
                        is_io = true;
                        io_effects.extend(effects.iter().cloned());
                        if let Some(rv) = row {
                            match io_row {
                                None => io_row = Some(*rv),
                                Some(existing) if existing != *rv => {
                                    self.unify(
                                        &Ty::Var(existing),
                                        &Ty::Var(*rv),
                                        expr.span,
                                    );
                                }
                                _ => {}
                            }
                        }
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

                    // Track `let x = *foo` for `set` full-replacement detection.
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        if let ast::ExprKind::SourceRef(source_name) = &expr.node {
                            self.source_var_binds
                                .insert(var_name.clone(), source_name.clone());
                        }
                        // Track the let body so the full-replacement check
                        // can fold through `*rel = let_bound_var` when the
                        // body references the source.
                        self.let_bindings.insert(var_name.clone(), expr.clone());
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
                        if let Ty::IO(ref effects, ref row, ref inner) = resolved {
                            is_io = true;
                            io_effects.extend(effects.iter().cloned());
                            if let Some(rv) = row {
                                match io_row {
                                    None => io_row = Some(*rv),
                                    Some(existing) if existing != *rv => {
                                        self.unify(
                                            &Ty::Var(existing),
                                            &Ty::Var(*rv),
                                            expr.span,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            last_expr_ty = Some(*inner.clone());
                        } else if self.in_io_do {
                            if let Ty::App(ref f, ref inner) = resolved {
                                // In IO do-blocks, App(m, a) from yield in
                                // case/if branches — resolve m to IO.
                                self.unify(f, &Ty::TyCon("IO".into()), expr.span);
                                is_io = true;
                                last_expr_ty = Some(*inner.clone());
                            } else if matches!(&resolved, Ty::Var(_)) {
                                // In IO do-block with unresolved type var:
                                // constrain to IO to prevent double-wrapping
                                // when the var later resolves to IO (e.g.
                                // polymorphic callbacks in withSessionAuth).
                                is_io = true;
                                let inner_ty = self.fresh();
                                self.unify(
                                    &expr_ty,
                                    &Ty::IO(BTreeSet::new(), None, Box::new(inner_ty.clone())),
                                    expr.span,
                                );
                                last_expr_ty = Some(inner_ty);
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
        self.source_var_binds = prev_source_var_binds;
        self.let_bindings = prev_let_bindings;

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
            Ty::IO(io_effects, io_row, Box::new(inner))
        } else {
            match yield_ty {
                Some(ty) => Ty::Relation(Box::new(ty)),
                None if last_expr_ty.is_some() => {
                    let last = last_expr_ty.unwrap();
                    if has_relation_bind {
                        // Flat-map / concatMap semantics: the last bare expression
                        // should itself be a list (e.g. from a case with yield/[]
                        // arms). Use it as the do-block type directly.
                        let applied = self.apply(&last);
                        match applied.peel_alias() {
                            Ty::Relation(_) => applied,
                            Ty::App(f, _) => {
                                let f_applied = self.apply(f);
                                if matches!(f_applied.peel_alias(), Ty::TyCon(n) if n == "[]") {
                                    applied
                                } else {
                                    Ty::Relation(Box::new(Ty::unit()))
                                }
                            }
                            _ => Ty::Relation(Box::new(Ty::unit())),
                        }
                    } else {
                        // No yield, no relation bind, but has bare expressions:
                        // use the last expression's type directly. This preserves
                        // polymorphism for do-blocks that sequence operations
                        // through a polymorphic monad parameter (e.g. `a {}`).
                        last
                    }
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
                    // NOTE: distinct ADTs may legally share a constructor name —
                    // see CLAUDE.md "Constructor patterns in case and do-bind
                    // create open variant types" and the regression tests
                    // `case_pattern_infers_open_variant` /
                    // `open_variant_applied_to_multiple_adts`. Row-polymorphic
                    // variants depend on this; an error here would forbid the
                    // documented feature. The later registration *replaces* the
                    // earlier in `self.constructors` so closed-variant lookups
                    // resolve to the most recent definition; open-variant
                    // dispatch goes through `knot_constructor_matches` at
                    // runtime which doesn't depend on this map.
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
                ast::DeclKind::View { name, ty, .. } => {
                    let resolved = if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        self.ast_type_to_ty(&scheme.ty)
                    } else {
                        Ty::Relation(Box::new(self.fresh()))
                    };
                    self.source_types.insert(name.clone(), resolved);
                    self.view_names.insert(name.clone());
                }
                ast::DeclKind::Derived { name, ty, .. } => {
                    let resolved = if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        self.ast_type_to_ty(&scheme.ty)
                    } else {
                        self.fresh()
                    };
                    self.derived_types.insert(name.clone(), resolved);
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
        match resolved.peel_alias() {
            Ty::Int => Some("Int".into()),
            Ty::Float => Some("Float".into()),
            Ty::Text => Some("Text".into()),
            Ty::Bool => Some("Bool".into()),
            Ty::Bytes => Some("Bytes".into()),
            Ty::Relation(_) => Some("[]".into()),
            Ty::TyCon(name) => Some(name.clone()),
            Ty::Con(name, _) => Some(name.clone()),
            Ty::IO(_, _, _) => Some("IO".into()),
            Ty::Fun(_, _) => Some("Fun".into()),
            Ty::Record(_, _) => Some("Record".into()),
            Ty::Variant(_, _) => Some("Variant".into()),
            Ty::App(_, _) => Some("App".into()),
            // Units are erased at runtime, so trait dispatch on a unit-typed
            // value resolves to the underlying primitive's impl.
            Ty::IntUnit(_) => Some("Int".into()),
            Ty::FloatUnit(_) => Some("Float".into()),
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
                        let raw_ty = self.ast_type_to_ty(&scheme.ty);
                        self.in_type_annotation = false;
                        let mut vars: Vec<TyVar> =
                            self.annotation_vars.values().copied().collect();
                        let unit_vars: Vec<UnitVar> =
                            self.annotation_unit_vars.values().copied().collect();
                        // Lift any outermost `Ty::Forall` into the Scheme so
                        // standard instantiation handles its quantified vars.
                        // Inner `Ty::Forall` (in arg positions) stays as-is
                        // for higher-rank handling.
                        let ty = match raw_ty {
                            Ty::Forall(forall_vars, body) => {
                                vars.extend(forall_vars);
                                *body
                            }
                            other => other,
                        };
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
                ast::DeclKind::Route { name, entries } => {
                    self.route_types.insert(name.clone());
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
                ast::DeclKind::RouteComposite { name, .. } => {
                    self.route_types.insert(name.clone());
                }
                _ => {}
            }
        }

        // Re-bind toJson/parseJson as unconstrained after trait processing.
        // The ToJSON/FromJSON traits register these methods with constraints,
        // but we want calling them to work on all types without explicit impls
        // (the runtime provides generic JSON encoding/decoding for all types).
        let a = self.fresh_var();
        self.bind_top(
            "toJson",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );
        let a = self.fresh_var();
        self.bind_top(
            "parseJson",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Var(a)))),
        );
    }

    fn register_builtins(&mut self) {
        // Built-in unit: Ms (milliseconds) — used by now/sleep
        self.declared_units.insert("Ms".into(), None);

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
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), None, Box::new(Ty::unit()))),
            )),
        );

        // print : ∀a. a -> IO {console} {}
        let a = self.fresh_var();
        self.bind_top(
            "print",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), None, Box::new(Ty::unit()))),
            )),
        );

        // logInfo / logWarn / logError / logDebug : ∀a. a -> IO {console} {}
        for log_name in ["logInfo", "logWarn", "logError", "logDebug"] {
            let a = self.fresh_var();
            self.bind_top(
                log_name,
                Scheme::poly(vec![a], Ty::Fun(
                    Box::new(Ty::Var(a)),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), None, Box::new(Ty::unit()))),
                )),
            );
        }

        // readLine : IO {console} Text
        self.bind_top("readLine", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Console]), None, Box::new(Ty::Text)),
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

        // count : ∀a u. [a] -> Int<u>
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "count",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(int_u),
                    ),
                },
            );
        }

        // countWhere : ∀a u. (a -> Bool) -> [a] -> Int<u>
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "countWhere",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(int_u),
                        )),
                    ),
                },
            );
        }

        // putLine : ∀a. a -> IO {console} {} (alias for println)
        let a = self.fresh_var();
        self.bind_top(
            "putLine",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Console]), None, Box::new(Ty::unit()))),
            )),
        );

        // now : IO {clock} Int<Ms>
        {
            let int_ms = Ty::IntUnit(UnitTy::named("Ms"));
            self.bind_top("now", Scheme::mono(
                Ty::IO(BTreeSet::from([IoEffect::Clock]), None, Box::new(int_ms)),
            ));
        }

        // sleep : Int<Ms> -> IO {clock} {}
        {
            let int_ms = Ty::IntUnit(UnitTy::named("Ms"));
            self.bind_top(
                "sleep",
                Scheme::mono(Ty::Fun(
                    Box::new(int_ms),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Clock]), None, Box::new(Ty::unit()))),
                )),
            );
        }

        // randomInt : ∀u. Int<u> -> IO {random} Int<u>
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "randomInt",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u.clone()),
                        Box::new(Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(int_u))),
                    ),
                },
            );
        }

        // randomFloat : ∀u. IO {random} Float<u>
        {
            let u = self.fresh_unit_var();
            let float_u = Ty::FloatUnit(UnitTy::var(u));
            self.bind_top("randomFloat", Scheme {
                vars: vec![],
                unit_vars: vec![u],
                constraints: vec![],
                ty: Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(float_u)),
            });
        }

        // fork : ∀a r. IO r a -> IO {} {}
        // Argument is any IO action (any effects, any result). The spawned
        // thread runs to completion in the background, so fork's return type
        // is closed-empty IO {} {} — none of the spawned action's effects
        // propagate to the caller.
        {
            let a = self.fresh_var();
            let r = self.fresh_var();
            self.bind_top(
                "fork",
                Scheme::poly(
                    vec![a, r],
                    Ty::Fun(
                        Box::new(Ty::IO(BTreeSet::new(), Some(r), Box::new(Ty::Var(a)))),
                        Box::new(Ty::IO(BTreeSet::new(), None, Box::new(Ty::unit()))),
                    ),
                ),
            );
        }

        // retry : ∀a. a (polymorphic bottom — usable in any context inside atomic)
        let a = self.fresh_var();
        self.bind_top("retry", Scheme::poly(vec![a], Ty::Var(a)));

        // __bind, __yield, __empty are handled as special cases in infer_expr
        // with polymorphic HKT types: ∀m a b. (a -> m b) -> m a -> m b, etc.
        // This allows do-block desugaring to work with any monad, not just [].

        // listen : ∀a u. Int<u> -> (a -> Response) -> IO {network} {}
        // The handler must return `Response`, the synthetic type produced
        // by each route's `respond` field — this forces every case branch
        // to call `respond`. Branches using IO (e.g. relation reads)
        // produce `IO _ _ Response`, which a unification rule below
        // treats as compatible with `Response`.
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            let response = Ty::Con("Response".into(), vec![]);
            self.bind_top(
                "listen",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(response))),
                            Box::new(Ty::IO(
                                BTreeSet::from([IoEffect::Network]),
                                None,
                                Box::new(Ty::unit()),
                            )),
                        )),
                    ),
                },
            );
        }

        // listenOn : ∀a u. Text -> Int<u> -> (a -> Response) -> IO {network} {}
        // Like `listen`, but binds to the supplied host (e.g. "127.0.0.1",
        // "0.0.0.0", "::1") rather than hardcoding "0.0.0.0".
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            let response = Ty::Con("Response".into(), vec![]);
            self.bind_top(
                "listenOn",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Text),
                        Box::new(Ty::Fun(
                            Box::new(int_u),
                            Box::new(Ty::Fun(
                                Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(response))),
                                Box::new(Ty::IO(
                                    BTreeSet::from([IoEffect::Network]),
                                    None,
                                    Box::new(Ty::unit()),
                                )),
                            )),
                        )),
                    ),
                },
            );
        }

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
            let io_ty = Ty::IO(BTreeSet::from([IoEffect::Network]), None, Box::new(result_ty));
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
            let io_ty2 = Ty::IO(BTreeSet::from([IoEffect::Network]), None, Box::new(result_ty2));
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

        // sortBy : ∀a b. (a -> b) -> [a] -> [a]
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "sortBy",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // takeRelation : ∀a. Int -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "takeRelation",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Int),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

        // dropRelation : ∀a. Int -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "dropRelation",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Int),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            ),
        );

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

        // min : ∀a b. (a -> b) -> [a] -> b
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "min",
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

        // max : ∀a b. (a -> b) -> [a] -> b
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "max",
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

        // take : ∀u. Int<u> -> Text -> Text
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "take",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u),
                        Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
                    ),
                },
            );
        }

        // drop : ∀u. Int<u> -> Text -> Text
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "drop",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u),
                        Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
                    ),
                },
            );
        }

        // length : ∀u. Text -> Int<u>
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "length",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(Box::new(Ty::Text), Box::new(int_u)),
                },
            );
        }

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

        // elem : ∀a. a -> [a] -> Bool
        let a = self.fresh_var();
        self.bind_top(
            "elem",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Var(a)),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Bool),
                    )),
                ),
            ),
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

        // stripUnit : ∀u. Int<u> -> Int — drop the unit tag from an Int
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "stripUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::IntUnit(UnitTy::var(u))),
                        Box::new(Ty::Int),
                    ),
                },
            );
        }

        // withUnit : ∀u. Int -> Int<u> — attach a unit (caller must annotate result)
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "withUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Int),
                        Box::new(Ty::IntUnit(UnitTy::var(u))),
                    ),
                },
            );
        }

        // stripFloatUnit : ∀u. Float<u> -> Float
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "stripFloatUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::FloatUnit(UnitTy::var(u))),
                        Box::new(Ty::Float),
                    ),
                },
            );
        }

        // withFloatUnit : ∀u. Float -> Float<u>
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "withFloatUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Float),
                        Box::new(Ty::FloatUnit(UnitTy::var(u))),
                    ),
                },
            );
        }

        // not : Bool -> Bool
        self.bind_top(
            "not",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bool), Box::new(Ty::Bool))),
        );

        // toJson and parseJson are now trait methods (ToJSON/FromJSON)
        // registered via register_trait_methods from the prelude.
        // They are re-bound as unconstrained after trait processing in pre_register().

        // ── File system standard library ─────────────────────────

        // readFile : Text -> IO {fs} Text
        self.bind_top(
            "readFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::Text))),
            )),
        );

        // writeFile : Text -> Text -> IO {fs} {}
        self.bind_top(
            "writeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::unit()))),
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
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::unit()))),
                )),
            )),
        );

        // fileExists : Text -> IO {fs} Bool
        self.bind_top(
            "fileExists",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::Bool))),
            )),
        );

        // removeFile : Text -> IO {fs} {}
        self.bind_top(
            "removeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::unit()))),
            )),
        );

        // listDir : Text -> IO {fs} [Text]
        self.bind_top(
            "listDir",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(BTreeSet::from([IoEffect::Fs]), None, Box::new(Ty::Relation(Box::new(Ty::Text))))),
            )),
        );

        // ── Bytes standard library ────────────────────────────────

        // bytesLength : ∀u. Bytes -> Int<u>
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::IntUnit(UnitTy::var(u));
            self.bind_top(
                "bytesLength",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    ty: Ty::Fun(Box::new(Ty::Bytes), Box::new(int_u)),
                },
            );
        }

        // bytesSlice : ∀u1 u2. Int<u1> -> Int<u2> -> Bytes -> Bytes
        {
            let u1 = self.fresh_unit_var();
            let u2 = self.fresh_unit_var();
            let int_u1 = Ty::IntUnit(UnitTy::var(u1));
            let int_u2 = Ty::IntUnit(UnitTy::var(u2));
            self.bind_top(
                "bytesSlice",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u1, u2],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u1),
                        Box::new(Ty::Fun(
                            Box::new(int_u2),
                            Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Bytes))),
                        )),
                    ),
                },
            );
        }

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

        // bytesToText : Bytes -> Maybe Text  (Nothing on invalid UTF-8)
        self.bind_top(
            "bytesToText",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Text])),
            )),
        );

        // bytesToHex : Bytes -> Text  (always succeeds)
        self.bind_top(
            "bytesToHex",
            Scheme::mono(Ty::Fun(Box::new(Ty::Bytes), Box::new(Ty::Text))),
        );

        // hash : ∀a. a -> Bytes  (SHA-256, returns 32 bytes; Bytes/Text hash
        // their raw contents, structured values hash a canonical serialization)
        {
            let a = self.fresh_var();
            self.bind_top(
                "hash",
                Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bytes))),
            );
        }

        // bytesFromHex / hexDecode : Text -> Maybe Bytes  (Nothing on
        // odd-length / non-hex / non-ASCII input)
        self.bind_top(
            "bytesFromHex",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Bytes])),
            )),
        );
        self.bind_top(
            "hexDecode",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Bytes])),
            )),
        );

        // bytesGet : ∀u1 u2. Int<u1> -> Bytes -> Int<u2>
        {
            let u1 = self.fresh_unit_var();
            let u2 = self.fresh_unit_var();
            let int_u1 = Ty::IntUnit(UnitTy::var(u1));
            let int_u2 = Ty::IntUnit(UnitTy::var(u2));
            self.bind_top(
                "bytesGet",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u1, u2],
                    constraints: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u1),
                        Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(int_u2))),
                    ),
                },
            );
        }

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
            Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(key_pair_record.clone())),
        ));

        // generateSigningKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
        self.bind_top("generateSigningKeyPair", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(key_pair_record)),
        ));

        // encrypt : Bytes -> Bytes -> IO {random} Bytes
        self.bind_top(
            "encrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(
                    Box::new(Ty::Bytes),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(Ty::Bytes))),
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
                // (e.g., `Applicative f =>` on Traversable.traverse). Lowercase
                // type variables parse as `TypeKind::Var`, but a constraint
                // could also reference an explicit type-parameter name parsed
                // as `TypeKind::Named`; accept either.
                for c in &ty.constraints {
                    if c.args.len() == 1 {
                        let var_name = match &c.args[0].node {
                            ast::TypeKind::Var(n) | ast::TypeKind::Named(n) => Some(n),
                            _ => None,
                        };
                        if let Some(var_name) = var_name {
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
                        self.check_expr(body, &expected);
                        let inferred = self.apply(&expected);

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
            info.insert(
                name.clone(),
                display_ty_clean(&applied, &var_map_for(&applied), &unit_var_map_for(&applied)),
            );
        }

        for (name, ty) in &self.derived_types {
            let applied = self.apply(ty);
            info.insert(
                name.clone(),
                display_ty_clean(&applied, &var_map_for(&applied), &unit_var_map_for(&applied)),
            );
        }

        info
    }

    fn extract_local_type_info(&self) -> LocalTypeInfo {
        let mut info = LocalTypeInfo::new();
        for (span, ty) in &self.binding_types {
            let applied = self.apply(ty);
            let var_map = var_map_for(&applied);
            let unit_var_map = unit_var_map_for(&applied);
            info.insert(*span, display_ty_clean(&applied, &var_map, &unit_var_map));
        }
        info
    }

    fn display_scheme(&self, scheme: &Scheme) -> String {
        let applied = self.apply(&scheme.ty);
        let var_map = var_map_for(&applied);
        let unit_var_map = unit_var_map_for(&applied);
        let ty_str = display_ty_clean(&applied, &var_map, &unit_var_map);

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

// ── Do-block span helper ──────────────────────────────────────────

/// The span that determines a do-block's result type — the last `yield`'s
/// argument or the last bare expression. Used to narrow type-error highlights
/// from the whole do-block to just the offending result expression.
fn do_result_span(stmts: &[ast::Stmt], fallback: Span) -> Span {
    for stmt in stmts.iter().rev() {
        if let ast::StmtKind::Expr(e) = &stmt.node {
            if let Some(inner) = e.node.as_yield_arg() {
                return inner.span;
            }
            return e.span;
        }
    }
    fallback
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

fn unit_var_map_for(ty: &Ty) -> HashMap<UnitVar, usize> {
    let mut vars = Vec::new();
    collect_unit_vars_ordered(ty, &mut vars);
    vars.iter()
        .enumerate()
        .map(|(i, &v)| (v, i))
        .collect()
}

fn collect_unit_vars_ordered(ty: &Ty, out: &mut Vec<UnitVar>) {
    match ty {
        Ty::IntUnit(u) | Ty::FloatUnit(u) => {
            for &v in u.vars.keys() {
                if !out.contains(&v) {
                    out.push(v);
                }
            }
        }
        Ty::Fun(p, r) => {
            collect_unit_vars_ordered(p, out);
            collect_unit_vars_ordered(r, out);
        }
        Ty::Record(fields, _) => {
            for t in fields.values() {
                collect_unit_vars_ordered(t, out);
            }
        }
        Ty::Variant(ctors, _) => {
            for t in ctors.values() {
                collect_unit_vars_ordered(t, out);
            }
        }
        Ty::Relation(inner) => collect_unit_vars_ordered(inner, out),
        Ty::Con(_, args) => {
            for a in args {
                collect_unit_vars_ordered(a, out);
            }
        }
        Ty::App(f, a) => {
            collect_unit_vars_ordered(f, out);
            collect_unit_vars_ordered(a, out);
        }
        Ty::IO(_, _, inner) => collect_unit_vars_ordered(inner, out),
        Ty::Forall(_, inner) => collect_unit_vars_ordered(inner, out),
        Ty::Alias(_, inner) => collect_unit_vars_ordered(inner, out),
        _ => {}
    }
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
        Ty::IO(_, row, inner) => {
            if let Some(rv) = row {
                if !out.contains(rv) {
                    out.push(*rv);
                }
            }
            collect_vars_ordered(inner, out);
        }
        Ty::EffectRow(_, row) => {
            if let Some(rv) = row {
                if !out.contains(rv) {
                    out.push(*rv);
                }
            }
        }
        Ty::Forall(bound, inner) => {
            // Collect free vars from the body, then drop the bound ones.
            let mut inner_vars = Vec::new();
            collect_vars_ordered(inner, &mut inner_vars);
            for v in inner_vars {
                if !bound.contains(&v) && !out.contains(&v) {
                    out.push(v);
                }
            }
        }
        Ty::Alias(_, inner) => collect_vars_ordered(inner, out),
        _ => {}
    }
}

/// Convert AST-level effects to type-system IoEffects (lossless — all
/// kinds, including reads/writes, are tracked in the type now).
fn ast_effects_to_io_effects(effects: &[ast::Effect]) -> BTreeSet<IoEffect> {
    let mut out = BTreeSet::new();
    for e in effects {
        let io = match e {
            ast::Effect::Reads(name) => IoEffect::Reads(name.clone()),
            ast::Effect::Writes(name) => IoEffect::Writes(name.clone()),
            ast::Effect::Console => IoEffect::Console,
            ast::Effect::Fs => IoEffect::Fs,
            ast::Effect::Network => IoEffect::Network,
            ast::Effect::Clock => IoEffect::Clock,
            ast::Effect::Random => IoEffect::Random,
        };
        out.insert(io);
    }
    out
}

fn format_io_effect(e: &IoEffect) -> String {
    match e {
        IoEffect::Reads(name) => format!("r *{}", name),
        IoEffect::Writes(name) => format!("w *{}", name),
        IoEffect::Console => "console".into(),
        IoEffect::Fs => "fs".into(),
        IoEffect::Network => "network".into(),
        IoEffect::Clock => "clock".into(),
        IoEffect::Random => "random".into(),
    }
}

fn display_effect_set_clean(
    effects: &BTreeSet<IoEffect>,
    row: Option<TyVar>,
    names: &HashMap<TyVar, usize>,
) -> String {
    let mut parts: Vec<String> = format_io_effects_coalesced(effects);
    if let Some(rv) = row {
        parts.push(format!(
            "| {}",
            var_letter(names.get(&rv).copied().unwrap_or(rv as usize))
        ));
    }
    format!(" {{{}}}", parts.join(", "))
}

fn format_io_effects_coalesced(effects: &BTreeSet<IoEffect>) -> Vec<String> {
    let mut reads: BTreeSet<&String> = BTreeSet::new();
    let mut writes: BTreeSet<&String> = BTreeSet::new();
    let mut others: Vec<String> = Vec::new();
    for e in effects {
        match e {
            IoEffect::Reads(name) => {
                reads.insert(name);
            }
            IoEffect::Writes(name) => {
                writes.insert(name);
            }
            _ => others.push(format_io_effect(e)),
        }
    }
    let read_write: BTreeSet<&&String> = reads.intersection(&writes).collect();
    let mut parts: Vec<String> = Vec::new();
    for name in &reads {
        if !read_write.contains(name) {
            parts.push(format!("r *{}", name));
        }
    }
    for name in &writes {
        if !read_write.contains(name) {
            parts.push(format!("w *{}", name));
        }
    }
    for name in &read_write {
        parts.push(format!("rw *{}", name));
    }
    parts.extend(others);
    parts
}

fn var_letter(idx: usize) -> String {
    if idx < 26 {
        format!("{}", (b'a' + idx as u8) as char)
    } else {
        format!("t{}", idx)
    }
}

fn unit_var_letter(idx: usize) -> String {
    if idx == 0 {
        "u".to_string()
    } else {
        format!("u{}", idx)
    }
}

fn display_unit_clean(u: &UnitTy, unit_names: &HashMap<UnitVar, usize>) -> String {
    if u.is_dimensionless() {
        return "1".to_string();
    }
    let mut num_parts = Vec::new();
    let mut den_parts = Vec::new();
    for (name, exp) in &u.bases {
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
    for (&v, &exp) in &u.vars {
        let var_name = unit_names
            .get(&v)
            .copied()
            .map(unit_var_letter)
            .unwrap_or_else(|| format!("?u{}", v));
        if exp > 0 {
            if exp == 1 {
                num_parts.push(var_name);
            } else {
                num_parts.push(format!("{}^{}", var_name, exp));
            }
        } else if exp < 0 {
            if exp == -1 {
                den_parts.push(var_name);
            } else {
                den_parts.push(format!("{}^{}", var_name, -exp));
            }
        }
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

fn display_ty_clean(
    ty: &Ty,
    names: &HashMap<TyVar, usize>,
    unit_names: &HashMap<UnitVar, usize>,
) -> String {
    display_ty_clean_inner(ty, names, unit_names, false)
}

fn display_ty_clean_inner(
    ty: &Ty,
    names: &HashMap<TyVar, usize>,
    unit_names: &HashMap<UnitVar, usize>,
    in_fun: bool,
) -> String {
    match ty {
        Ty::Var(v) => var_letter(names.get(v).copied().unwrap_or(*v as usize)),
        Ty::Int => "Int".into(),
        Ty::Float => "Float".into(),
        Ty::IntUnit(u) => {
            if u.is_dimensionless() {
                "Int".into()
            } else {
                format!("Int<{}>", display_unit_clean(u, unit_names))
            }
        }
        Ty::FloatUnit(u) => {
            if u.is_dimensionless() {
                "Float".into()
            } else {
                format!("Float<{}>", display_unit_clean(u, unit_names))
            }
        }
        Ty::Text => "Text".into(),
        Ty::Bool => "Bool".into(),
        Ty::Bytes => "Bytes".into(),
        Ty::Fun(p, r) => {
            let s = format!(
                "{} -> {}",
                display_ty_clean_inner(p, names, unit_names, true),
                display_ty_clean_inner(r, names, unit_names, false)
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
                .map(|(n, t)| format!("{}: {}", n, display_ty_clean(t, names, unit_names)))
                .collect();
            if let Some(rv) = row {
                parts.push(format!("| {}", var_letter(names.get(rv).copied().unwrap_or(*rv as usize))));
            }
            format!("{{{}}}", parts.join(", "))
        }
        Ty::Relation(inner) => format!("[{}]", display_ty_clean(inner, names, unit_names)),
        Ty::Con(name, args) => {
            if args.is_empty() {
                name.clone()
            } else {
                let args_str: Vec<String> =
                    args.iter().map(|a| display_ty_clean(a, names, unit_names)).collect();
                format!("{} {}", name, args_str.join(" "))
            }
        }
        Ty::Variant(ctors, row) => {
            let mut parts: Vec<String> = ctors
                .iter()
                .map(|(name, ft)| format!("{} {}", name, display_ty_clean(ft, names, unit_names)))
                .collect();
            if let Some(rv) = row {
                parts.push(var_letter(names.get(rv).copied().unwrap_or(*rv as usize)));
            }
            format!("<{}>", parts.join(" | "))
        }
        Ty::TyCon(name) => name.clone(),
        Ty::App(f, a) => format!(
            "({} {})",
            display_ty_clean(f, names, unit_names),
            display_ty_clean(a, names, unit_names)
        ),
        Ty::IO(effects, row, inner) => {
            let effects_str =
                display_effect_set_clean(effects, *row, names);
            format!("IO{} {}", effects_str, display_ty_clean(inner, names, unit_names))
        }
        Ty::EffectRow(effects, row) => {
            format!("Effects{}", display_effect_set_clean(effects, *row, names))
        }
        Ty::Forall(vars, inner) => {
            if vars.is_empty() {
                display_ty_clean_inner(inner, names, unit_names, in_fun)
            } else {
                let bound: Vec<String> = vars
                    .iter()
                    .map(|v| var_letter(names.get(v).copied().unwrap_or(*v as usize)))
                    .collect();
                let s = format!(
                    "forall {}. {}",
                    bound.join(" "),
                    display_ty_clean_inner(inner, names, unit_names, false)
                );
                if in_fun {
                    format!("({})", s)
                } else {
                    s
                }
            }
        }
        Ty::Alias(name, _) => name.clone(),
        Ty::Error => "<error>".into(),
    }
}

// ── `set` full-replacement detection ──────────────────────────────

/// Whether `expr` references `*source_name` — either directly via
/// `SourceRef`, or via a local variable bound to `*source_name`
/// (e.g. `xs <- *foo`, then `xs` counts as a reference), or via a
/// `let`-bound expression that itself references the source. Used
/// to distinguish incremental `set` (must reference the source)
/// from full replacement (which requires `replace *rel = ...`).
fn value_references_source(
    expr: &ast::Expr,
    source_name: &str,
    aliases: &HashMap<String, String>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> bool {
    let mut visited: HashSet<String> = HashSet::new();
    value_references_source_inner(expr, source_name, aliases, let_bindings, &mut visited)
}

fn value_references_source_inner(
    expr: &ast::Expr,
    source_name: &str,
    aliases: &HashMap<String, String>,
    let_bindings: &HashMap<String, ast::Expr>,
    visited: &mut HashSet<String>,
) -> bool {
    match &expr.node {
        ast::ExprKind::SourceRef(name) => name == source_name,
        ast::ExprKind::Var(name) => {
            if aliases.get(name).map(|s| s.as_str()) == Some(source_name) {
                return true;
            }
            // Fold through let bindings: `let foo = ...; *rel = foo`
            // counts as referencing the source if the body does.
            if visited.insert(name.clone()) {
                if let Some(body) = let_bindings.get(name) {
                    let result = value_references_source_inner(
                        body, source_name, aliases, let_bindings, visited,
                    );
                    visited.remove(name);
                    return result;
                }
            }
            false
        }
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::DerivedRef(_) => false,
        ast::ExprKind::Record(fields) => fields.iter().any(|f| {
            value_references_source_inner(
                &f.value, source_name, aliases, let_bindings, visited,
            )
        }),
        ast::ExprKind::RecordUpdate { base, fields } => {
            value_references_source_inner(
                base, source_name, aliases, let_bindings, visited,
            ) || fields.iter().any(|f| {
                value_references_source_inner(
                    &f.value, source_name, aliases, let_bindings, visited,
                )
            })
        }
        ast::ExprKind::FieldAccess { expr, .. } => value_references_source_inner(
            expr, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::List(elems) => elems.iter().any(|e| {
            value_references_source_inner(e, source_name, aliases, let_bindings, visited)
        }),
        ast::ExprKind::Lambda { body, .. } => value_references_source_inner(
            body, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::App { func, arg } => {
            value_references_source_inner(func, source_name, aliases, let_bindings, visited)
                || value_references_source_inner(
                    arg, source_name, aliases, let_bindings, visited,
                )
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            value_references_source_inner(lhs, source_name, aliases, let_bindings, visited)
                || value_references_source_inner(
                    rhs, source_name, aliases, let_bindings, visited,
                )
        }
        ast::ExprKind::UnaryOp { operand, .. } => value_references_source_inner(
            operand, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            value_references_source_inner(
                cond, source_name, aliases, let_bindings, visited,
            ) || value_references_source_inner(
                then_branch, source_name, aliases, let_bindings, visited,
            ) || value_references_source_inner(
                else_branch, source_name, aliases, let_bindings, visited,
            )
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            value_references_source_inner(
                scrutinee, source_name, aliases, let_bindings, visited,
            ) || arms.iter().any(|a| {
                value_references_source_inner(
                    &a.body, source_name, aliases, let_bindings, visited,
                )
            })
        }
        ast::ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
            ast::StmtKind::Bind { expr, .. } => value_references_source_inner(
                expr, source_name, aliases, let_bindings, visited,
            ),
            ast::StmtKind::Let { expr, .. } => value_references_source_inner(
                expr, source_name, aliases, let_bindings, visited,
            ),
            ast::StmtKind::Where { cond } => value_references_source_inner(
                cond, source_name, aliases, let_bindings, visited,
            ),
            ast::StmtKind::GroupBy { key } => value_references_source_inner(
                key, source_name, aliases, let_bindings, visited,
            ),
            ast::StmtKind::Expr(e) => value_references_source_inner(
                e, source_name, aliases, let_bindings, visited,
            ),
        }),
        ast::ExprKind::Set { target, value }
        | ast::ExprKind::ReplaceSet { target, value } => {
            value_references_source_inner(
                target, source_name, aliases, let_bindings, visited,
            ) || value_references_source_inner(
                value, source_name, aliases, let_bindings, visited,
            )
        }
        ast::ExprKind::Atomic(inner) | ast::ExprKind::Refine(inner) => {
            value_references_source_inner(
                inner, source_name, aliases, let_bindings, visited,
            )
        }
        ast::ExprKind::At { relation, time } => {
            value_references_source_inner(
                relation, source_name, aliases, let_bindings, visited,
            ) || value_references_source_inner(
                time, source_name, aliases, let_bindings, visited,
            )
        }
        ast::ExprKind::UnitLit { value, .. } => value_references_source_inner(
            value, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::Annot { expr, .. } => value_references_source_inner(
            expr, source_name, aliases, let_bindings, visited,
        ),
    }
}

// ── Public API ────────────────────────────────────────────────────

/// Run type inference on a parsed module. Returns diagnostics,
/// resolved monad info for desugared do-blocks, and inferred type info
/// mapping declaration names to their display type strings.
pub fn check(module: &ast::Module) -> (Vec<Diagnostic>, MonadInfo, TypeInfo, LocalTypeInfo, RefineTargets, RefinedTypeInfoMap, FromJsonTargets, ElemPushdownOk) {
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
    for trait_name in &["Functor", "Applicative", "Monad"] {
        infer
            .known_impls
            .insert((trait_name.to_string(), "IO".to_string()));
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
        let kind = match resolved.peel_alias() {
            Ty::TyCon(name) if name == "[]" => MonadKind::Relation,
            Ty::TyCon(name) if name == "IO" => MonadKind::IO,
            Ty::TyCon(name) => MonadKind::Adt(name.clone()),
            Ty::Relation(_) => MonadKind::Relation,
            Ty::IO(_, _, _) => MonadKind::IO,
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

    // Phase 7: Resolve parseJson call targets for compile-time FromJSON dispatch
    let mut from_json_targets = FromJsonTargets::new();
    for (span, var) in &infer.from_json_calls {
        let resolved = infer.apply(&Ty::Var(*var));
        if let Some(name) = ty_to_type_name(&resolved) {
            from_json_targets.insert(*span, name);
        }
    }

    // Phase 8: Validate `listen` / `listenOn` handler argument types and
    // bodies.
    //   (a) The handler input type must be a route ADT — without this,
    //       an identity handler `\req -> req` would type-check by
    //       collapsing `a = Response`.
    //   (b) Every leaf return position in the handler body must be a
    //       `respond` call (or chain to one). Without this, a handler
    //       like `\req -> bottom` would type-check by collapsing a free
    //       type variable to `Response`, even though no `Response` value
    //       is ever produced at runtime.
    // Case-pattern handlers infer the arg as an open `Ty::Variant`;
    // accept those when every constructor belongs to a single route ADT.
    let listen_calls = std::mem::take(&mut infer.listen_calls);
    for (span, arg_ty, handler_expr) in listen_calls {
        let resolved = infer.apply(&arg_ty);
        if !is_route_handler_arg(&resolved, &infer) {
            infer.errors.push((
                format!(
                    "listen handler must take a route ADT, found {}",
                    infer.display_ty(&resolved)
                ),
                span,
            ));
        }
        if let Some(bad_span) =
            find_non_respond_leaf(&handler_expr, module, &mut Vec::new())
        {
            infer.errors.push((
                "listen handler must call `respond` in every branch".into(),
                bad_span,
            ));
        }
    }

    let type_info = infer.extract_type_info();
    let local_type_info = infer.extract_local_type_info();
    let elem_pushdown_ok = infer.elem_pushdown_ok.clone();

    (infer.to_diagnostics(), monad_info, type_info, local_type_info, refine_targets, refined_type_info, from_json_targets, elem_pushdown_ok)
}

/// Check that the inferred argument type of a `listen` handler is a route ADT.
/// Accepts:
/// - `Ty::Alias(name, _)` where `name` is a route (single-variant route ADTs
///   surface as aliases of their inner record).
/// - `Ty::Con(name, _)` where `name` is a route.
/// - `Ty::Variant(ctors, _)` where every constructor belongs to one route ADT
///   (case-pattern handlers infer the scrutinee as an open variant rather
///   than the named ADT).
fn is_route_handler_arg(ty: &Ty, infer: &Infer) -> bool {
    if let Ty::Alias(name, _) = ty {
        if infer.route_types.contains(name) {
            return true;
        }
    }
    match ty.peel_alias() {
        Ty::Con(name, _) => infer.route_types.contains(name),
        Ty::Variant(ctors, _) => {
            let mut data_type: Option<&str> = None;
            ctors.keys().all(|ctor_name| {
                let info = match infer.constructors.get(ctor_name) {
                    Some(info) => info,
                    None => return false,
                };
                match data_type {
                    None => {
                        data_type = Some(info.data_type.as_str());
                        infer.route_types.contains(&info.data_type)
                    }
                    Some(dt) => dt == info.data_type,
                }
            })
        }
        _ => false,
    }
}

/// Result of structurally checking a `listen` handler body's leaves.
enum LeafCheck {
    /// Every path through this expression reaches a `respond` call (or a
    /// trusted function application).
    Ok,
    /// Every path through this expression hits a self-recursive call back
    /// into a function we're already walking. Legitimate when *some other*
    /// path through that function reaches `respond`; bad when the entire
    /// function is just a recursive cycle (e.g. `bottom = bottom`).
    Cycle,
    /// A non-respond leaf was found at the given span.
    Bad(Span),
}

/// Walk a `listen` handler argument expression and verify that every leaf
/// is a `respond` call (or chain that reaches one). Returns the span of
/// the first offending leaf, or `None` if the handler is well-formed.
///
/// What counts as a leaf and how it's classified:
/// - `respond x` (or curried `respond x y` for header responses) — Ok
/// - Other function applications — Ok (their return type is constrained
///   by the type system, so a non-`Response` return errors elsewhere)
/// - `Case` / `If` — recurse into each branch
/// - `Lambda` / `Atomic` / `Annot` — recurse into the inner expression
/// - `Do` — recurse into the last statement's expression
/// - `Var(name)` referencing a top-level function — recurse into its body
///   so `listen port handler` works when `handler` is defined separately
/// - Self-recursive `Var(name)` (already in the call stack) — Cycle
/// - Bare locals, literals, records, etc. — Bad
///
/// At the top level, `Cycle` is treated as `Bad`: a function whose every
/// path is a self-recursion never produces a `Response` value.
fn find_non_respond_leaf(
    expr: &ast::Expr,
    module: &ast::Module,
    visiting: &mut Vec<String>,
) -> Option<Span> {
    match check_handler_leaf(expr, module, visiting) {
        LeafCheck::Ok => None,
        LeafCheck::Bad(s) => Some(s),
        LeafCheck::Cycle => Some(expr.span),
    }
}

fn check_handler_leaf(
    expr: &ast::Expr,
    module: &ast::Module,
    visiting: &mut Vec<String>,
) -> LeafCheck {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            // Walk through curried applications to find the root function.
            let mut f = func.as_ref();
            loop {
                match &f.node {
                    ast::ExprKind::Var(name) if name == "respond" => {
                        return LeafCheck::Ok;
                    }
                    ast::ExprKind::App { func: inner, .. } => {
                        f = inner.as_ref();
                    }
                    ast::ExprKind::Lambda { params, body } => {
                        // Lambda applications (e.g., from `let...in` desugaring).
                        // If the body is just a reference to a parameter, the lambda
                        // returns its argument, so we need to check the argument.
                        // Otherwise, check the lambda body.
                        if let ast::ExprKind::Var(name) = &body.node {
                            if params.iter().any(|p| match &p.node {
                                ast::PatKind::Var(pname) => pname == name,
                                _ => false,
                            }) {
                                // Body is just a parameter - check the argument
                                return check_handler_leaf(arg, module, visiting);
                            }
                        }
                        // Body does something more complex - check it
                        return check_handler_leaf(f, module, visiting);
                    }
                    _ => {
                        // Other function references (e.g., calls to defined functions)
                        // are trusted to call respond.
                        return LeafCheck::Ok;
                    }
                }
            }
        }
        ast::ExprKind::Case { arms, .. } => combine_branches(
            arms.iter().map(|arm| {
                check_handler_leaf(&arm.body, module, visiting)
            }),
        ),
        ast::ExprKind::If { then_branch, else_branch, .. } => combine_branches(
            [
                check_handler_leaf(then_branch, module, visiting),
                check_handler_leaf(else_branch, module, visiting),
            ]
            .into_iter(),
        ),
        ast::ExprKind::Lambda { body, .. } => {
            check_handler_leaf(body, module, visiting)
        }
        ast::ExprKind::Atomic(inner) => {
            check_handler_leaf(inner, module, visiting)
        }
        ast::ExprKind::Annot { expr, .. } => {
            check_handler_leaf(expr, module, visiting)
        }
        ast::ExprKind::Do(stmts) => match stmts.last().map(|s| &s.node) {
            Some(ast::StmtKind::Expr(e)) => {
                check_handler_leaf(e, module, visiting)
            }
            _ => LeafCheck::Bad(expr.span),
        },
        ast::ExprKind::Var(name) => {
            if visiting.iter().any(|n| n == name) {
                return LeafCheck::Cycle;
            }
            for decl in &module.decls {
                if let ast::DeclKind::Fun {
                    name: fn_name,
                    body: Some(body),
                    ..
                } = &decl.node
                {
                    if fn_name == name {
                        visiting.push(name.clone());
                        let r = check_handler_leaf(body, module, visiting);
                        visiting.pop();
                        return r;
                    }
                }
            }
            LeafCheck::Bad(expr.span)
        }
        _ => LeafCheck::Bad(expr.span),
    }
}

/// Combine sibling branches (case arms, if branches). Bad short-circuits;
/// any Ok branch makes the whole expression Ok (the recursive path is
/// well-founded if at least one branch reaches respond); only when *all*
/// branches are Cycle do we propagate Cycle.
fn combine_branches(results: impl Iterator<Item = LeafCheck>) -> LeafCheck {
    let mut has_ok = false;
    let mut has_cycle = false;
    for r in results {
        match r {
            LeafCheck::Bad(s) => return LeafCheck::Bad(s),
            LeafCheck::Ok => has_ok = true,
            LeafCheck::Cycle => has_cycle = true,
        }
    }
    if has_ok {
        LeafCheck::Ok
    } else if has_cycle {
        LeafCheck::Cycle
    } else {
        // No branches at all — vacuously Ok.
        LeafCheck::Ok
    }
}

/// Extract a simple type name from a resolved type for trait dispatch purposes.
fn ty_to_type_name(ty: &Ty) -> Option<String> {
    match ty {
        Ty::Int | Ty::IntUnit(_) => Some("Int".to_string()),
        Ty::Float | Ty::FloatUnit(_) => Some("Float".to_string()),
        Ty::Text => Some("Text".to_string()),
        Ty::Bool => Some("Bool".to_string()),
        Ty::Bytes => Some("Bytes".to_string()),
        Ty::Con(name, _) => Some(name.clone()),
        Ty::Relation(_) => Some("Relation".to_string()),
        Ty::Record(_, _) => Some("Record".to_string()),
        _ => None,
    }
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
        let (diags, _monad_info, _type_info, _local_types, _refine_targets, _refined_types, _from_json, _elem_pushdown) = check(&module);
        diags
    }

    fn type_info_for(src: &str) -> TypeInfo {
        let mut module = parse(src);
        crate::desugar::desugar(&mut module);
        let (_diags, _monad_info, type_info, _local_types, _refine_targets, _refined_types, _from_json, _elem_pushdown) = check(&module);
        type_info
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
    fn rank2_row_polymorphic_field_access() {
        // A rank-2 predicate may access any field listed in the row
        // pattern. The skolemised row variable must stay rigid even when
        // field-access constraints introduce extra fresh row variables.
        let src = "\
applyPred : (forall a. {x: Int, y: Int | a} -> Bool) -> Int\n\
applyPred = \\pred -> if pred {x: 1, y: 2} then 1 else 0\n\
main = applyPred (\\r -> r.x == r.y)\
";
        let diags = check_src(src);
        assert!(diags.is_empty(), "unexpected diagnostics: {:?}", diags);
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
            "*nums : [Int]\nmain = replace *nums = [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn bare_set_full_replacement_errors() {
        // `*nums = [1, 2, 3]` doesn't reference *nums, so it's a full
        // replacement and must use `replace` syntax.
        let diags = check_src(
            "*nums : [Int]\nmain = *nums = [1, 2, 3]"
        );
        assert!(has_error(&diags, "replace *nums"));
    }

    #[test]
    fn bare_set_via_local_alias_ok() {
        // `xs <- *people` makes `xs` an alias for *people, so writing
        // `union xs [...]` counts as referencing the source.
        let diags = check_src(
            "type P = {name: Text, age: Int}\n\
             *people : [P]\n\
             insert = \\name age -> do\n\
               ps <- *people\n\
               *people = union ps [{name: name, age: age}]"
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn bare_set_value_unrelated_to_source_errors() {
        // Even with a local source bind, replacing with an unrelated value
        // is a full replacement and must use `replace`.
        let diags = check_src(
            "type P = {name: Text, age: Int}\n\
             *people : [P]\n\
             *other : [P]\n\
             copy = do\n\
               os <- *other\n\
               *people = os"
        );
        assert!(has_error(&diags, "replace *people"));
    }

    #[test]
    fn replace_referencing_source_errors() {
        // `replace *rel = expr` where the value references *rel is
        // unnecessary — `set` would produce the same final state.
        let diags = check_src(
            "type P = {name: Text, age: Int}\n\
             *people : [P]\n\
             birthday = do\n\
               replace *people = do\n\
                 p <- *people\n\
                 yield {p | age: p.age + 1}\n\
               yield {}"
        );
        assert!(has_error(&diags, "`replace *people = ...` is unnecessary"));
    }

    #[test]
    fn replace_referencing_source_via_alias_errors() {
        // Aliases (`xs <- *rel`) count as referencing the source, so
        // `replace *rel = union xs new` is also unnecessary.
        let diags = check_src(
            "type P = {name: Text, age: Int}\n\
             *people : [P]\n\
             insert = \\name age -> do\n\
               ps <- *people\n\
               replace *people = union ps [{name: name, age: age}]\n\
               yield {}"
        );
        assert!(has_error(&diags, "`replace *people = ...` is unnecessary"));
    }

    #[test]
    fn replace_with_literal_ok() {
        // The canonical `replace` use case: replacing with a literal that
        // doesn't reference the source.
        let diags = check_src(
            "type P = {name: Text, age: Int}\n\
             *people : [P]\n\
             main = do\n\
               replace *people = [{name: \"Alice\", age: 30}]\n\
               yield {}"
        );
        assert!(diags.is_empty(), "expected no diagnostics, got: {:?}", diags);
    }

    #[test]
    fn set_type_mismatch() {
        let diags = check_src(
            "*nums : [Int]\nmain = replace *nums = [\"a\", \"b\"]"
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
    fn min_builtin_int() {
        // min : (a -> b) -> [a] -> b ; numeric projection
        assert!(check_src(
            "type T = {x: Int}\n*ts : [T]\nmain = do\n  ts <- *ts\n  yield (min (\\t -> t.x) ts)"
        ).is_empty());
    }

    #[test]
    fn max_builtin_text() {
        // max works with Text projections (lexicographic ordering)
        assert!(check_src(
            "type T = {name: Text}\n*ts : [T]\nmain = do\n  ts <- *ts\n  yield (max (\\t -> t.name) ts)"
        ).is_empty());
    }

    #[test]
    fn count_where_builtin() {
        assert!(check_src(
            "type T = {x: Int}\n*ts : [T]\nmain = do\n  ts <- *ts\n  yield (countWhere (\\t -> t.x > 5) ts)"
        ).is_empty());
    }

    #[test]
    fn count_where_rejects_non_bool() {
        // countWhere predicate must return Bool
        let diags = check_src(
            "type T = {x: Int}\n*ts : [T]\nmain = do\n  ts <- *ts\n  yield (countWhere (\\t -> t.x) ts)"
        );
        assert!(has_error(&diags, "type mismatch"));
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

    #[test]
    fn type_info_renders_polymorphic_unit_vars_cleanly() {
        // Inferred type signatures exposed to the LSP must render polymorphic
        // unit variables as `u`, `u1`, ... — not as raw `?u104` substitutions.
        let info = type_info_for("len = \\s -> length s");
        let s = info.get("len").expect("len type should be inferred");
        assert!(
            !s.contains("?u"),
            "type info should not leak raw unit vars: got {:?}",
            s
        );
        assert_eq!(s, "Text -> Int<u>", "got {:?}", s);
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

    // ── Unit annotations on stdlib functions ─────────────────────

    #[test]
    fn unit_randomint_preserves_unit() {
        // randomInt should preserve the unit from the bound argument
        let diags = check_src(
            "unit Usd\n\
             f : IO {random} Int<Usd>\n\
             f = randomInt 100<Usd>\n\
             main = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_randomint_mismatch() {
        // randomInt result has unit from bound — annotating with wrong unit should fail
        let diags = check_src(
            "unit Usd\nunit Eur\n\
             f : IO {random} Int<Eur>\n\
             f = randomInt 100<Usd>\n\
             main = 1"
        );
        assert!(has_error(&diags, "unit mismatch"));
    }

    #[test]
    fn unit_count_accepts_unit_context() {
        // count result can unify with a unit context
        let diags = check_src(
            "unit N\n\
             main = count [1, 2, 3] + 0<N>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_length_accepts_unit_context() {
        // length result can unify with a unit context
        let diags = check_src(
            "unit N\n\
             main = length \"hello\" + 0<N>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_sleep_accepts_ms() {
        // sleep accepts Int<Ms> (built-in unit)
        let diags = check_src(
            "main = sleep 1000<Ms>"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_sleep_rejects_wrong_unit() {
        // sleep requires Ms — passing a different unit should fail
        let diags = check_src(
            "unit Kg\n\
             main = sleep 1000<Kg>"
        );
        assert!(has_error(&diags, "unit mismatch"));
    }

    #[test]
    fn unit_now_returns_ms() {
        // now returns IO {clock} Int<Ms>
        let diags = check_src(
            "f : IO {clock} Int<Ms>\n\
             f = now\n\
             main = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn unit_now_rejects_wrong_unit() {
        // now returns Int<Ms> — annotating with wrong unit should fail
        let diags = check_src(
            "unit Kg\n\
             f : IO {clock} Int<Kg>\n\
             f = now\n\
             main = 1"
        );
        assert!(has_error(&diags, "unit mismatch"));
    }

    #[test]
    fn strip_with_unit_int_round_trip() {
        let diags = check_src(
            "unit Ms\nunit S\n\
             toS : Int<Ms> -> Int<S>\n\
             toS = \\ms -> withUnit (stripUnit ms / 1000)\n\
             main = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn strip_unit_float() {
        let diags = check_src(
            "unit M\n\
             f : Float<M> -> Float\n\
             f = \\x -> stripFloatUnit x\n\
             main = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn with_unit_float() {
        let diags = check_src(
            "unit M\n\
             f : Float -> Float<M>\n\
             f = \\x -> withFloatUnit x\n\
             main = 1"
        );
        assert!(diags.is_empty(), "errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn strip_unit_rejects_float_arg() {
        // stripUnit is Int-only — passing a Float should fail
        let diags = check_src(
            "unit M\n\
             f = stripUnit 1.0<M>\n\
             main = 1"
        );
        assert!(!diags.is_empty());
    }

    #[test]
    fn alias_preserved_in_record_type_hint() {
        // type Person = {...} should appear as "Person" in the inferred
        // type of any function that mentions it, not as the expanded form.
        let info = type_info_for(
            "type Person = {name: Text, age: Int}\n\
             greet : Person -> Text\n\
             greet = \\p -> p.name\n\
             main = 1"
        );
        let ty = info.get("greet").expect("missing greet type");
        assert!(
            ty.contains("Person") && !ty.contains("name: Text"),
            "expected alias preserved, got {}",
            ty
        );
    }

    #[test]
    fn alias_preserved_in_text_synonym() {
        let info = type_info_for(
            "type UserId = Text\n\
             format : UserId -> Text\n\
             format = \\u -> u\n\
             main = 1"
        );
        let ty = info.get("format").expect("missing format type");
        assert!(
            ty.contains("UserId"),
            "expected UserId in type, got {}",
            ty
        );
    }

    #[test]
    fn alias_record_field_access_works() {
        // Field access through an alias must still type-check — the
        // structural inspection has to peel the alias to find the field.
        let diags = check_src(
            "type Person = {name: Text, age: Int}\n\
             greet : Person -> Text\n\
             greet = \\p -> p.name\n\
             main = greet {name: \"Alice\", age: 30}"
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_relation_bind_typechecks() {
        // *people : [Person]; reading from it must still typecheck.
        let diags = check_src(
            "type Person = {name: Text, age: Int}\n\
             *people : [Person]\n\
             main = do\n\
               replace *people = [{name: \"A\", age: 1}]\n\
               p <- *people\n\
               yield p.name"
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_for_function_type() {
        // type Handler = Int -> Text; calling a Handler must work.
        let diags = check_src(
            "type Handler = Int -> Text\n\
             apply : Handler -> Int -> Text\n\
             apply = \\h x -> h x\n\
             main = apply (\\n -> show n) 42"
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_subsumes_base_type() {
        // Bidirectional compatibility: alias and base should interoperate.
        let diags = check_src(
            "type UserId = Text\n\
             toText : UserId -> Text\n\
             toText = \\u -> u\n\
             main = toText \"hello\""
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_unary_negation() {
        // Negation on an aliased numeric type must work — the unary op
        // path inspects the resolved type structurally.
        let diags = check_src(
            "type Cents = Int\n\
             flip : Cents -> Cents\n\
             flip = \\x -> -x\n\
             main = flip 100"
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_case_on_data_type() {
        // Pattern matching on an aliased data type must pass exhaustiveness.
        let diags = check_src(
            "data Color = Red {} | Green {} | Blue {}\n\
             type Hue = Color\n\
             name : Hue -> Text\n\
             name = \\h -> case h of\n  Red {} -> \"red\"\n  Green {} -> \"green\"\n  Blue {} -> \"blue\"\n\
             main = name (Red {})"
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
    }

    #[test]
    fn alias_case_non_exhaustive_detected() {
        // Exhaustiveness must still flag missing constructors when the
        // scrutinee is an alias of a data type — the check has to peel.
        let diags = check_src(
            "data Color = Red {} | Green {} | Blue {}\n\
             type Hue = Color\n\
             name : Hue -> Text\n\
             name = \\h -> case h of\n  Red {} -> \"red\"\n  Green {} -> \"green\"\n\
             main = name (Red {})"
        );
        assert!(
            has_error(&diags, "non-exhaustive"),
            "expected non-exhaustive error, got: {:?}",
            diags
        );
    }

    #[test]
    fn respond_field_returns_response_type() {
        // The synthetic `respond` field on each route constructor must be
        // typed as `ResponseType -> Response`, not polymorphic. A handler
        // that uses respond's result in a non-Response context must error.
        let diags = check_src(
            "type Todo = {title: Text}\n\
             route Api where\n  GET /todos -> [Todo] = GetTodos\n\
             bad = \\req -> case req of\n  GetTodos {respond} -> respond [{title: \"x\"}] + 1\n\
             main = listen 8080 bad"
        );
        assert!(
            has_error(&diags, "Response"),
            "respond's result should be Response, not polymorphic: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn handler_with_bottom_should_be_rejected() {
        // A handler that returns a polymorphic value (like bottom) instead
        // of calling respond should be rejected. Today the free type var
        // collapses to Response, which is wrong: the handler must produce
        // a Response by calling respond, not by lifting an arbitrary value.
        let diags = check_src(
            "type Todo = {title: Text}\n\
             route Api where\n  GET /todos -> [Todo] = GetTodos\n\
             bottom : a\n\
             bottom = bottom\n\
             bad = \\req -> case req of\n  GetTodos {respond} -> bottom\n\
             main = listen 8080 bad"
        );
        assert!(
            !diags.is_empty(),
            "handler returning polymorphic value should be rejected"
        );
    }

    #[test]
    fn respond_typed_as_response_in_handler() {
        // The inferred type of a handler body using `respond` should be
        // `Response`, so a route ADT handler is `Api -> Response`.
        let info = type_info_for(
            "type Todo = {title: Text}\n\
             route Api where\n  GET /todos -> [Todo] = GetTodos\n\
             handler = \\req -> case req of\n  GetTodos {respond} -> respond [{title: \"x\"}]\n\
             main = listen 8080 handler"
        );
        let ty = info.get("handler").expect("missing handler type");
        // The handler's return type after `-> ` should be `Response`,
        // not a free type variable. The argument is an open variant
        // containing the route's constructors, but the return must be
        // the synthetic `Response` type produced by `respond`.
        assert!(
            ty.ends_with("-> Response"),
            "handler should return Response, got: {}",
            ty
        );
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
