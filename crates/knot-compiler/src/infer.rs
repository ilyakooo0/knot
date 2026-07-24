//! Hindley-Milner type inference for the Knot language.
//!
//! Infers and checks types for all declarations. Reports type errors as
//! diagnostics. The runtime uses uniform pointer representation, so this
//! pass is purely for error detection — it does not affect code generation.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Collect all variable names bound by a pattern, recursing into
/// Constructor, Record, List, and Cons sub-patterns.
fn collect_pat_bound_names(pat: &ast::Pat, out: &mut Vec<String>) {
    use knot::ast::PatKind;
    match &pat.node {
        PatKind::Var(name) => out.push(name.clone()),
        PatKind::Wildcard | PatKind::Lit(_) => {}
        PatKind::Constructor { payload, .. } => collect_pat_bound_names(payload, out),
        PatKind::Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => collect_pat_bound_names(p, out),
                    None => out.push(f.name.clone()),
                }
            }
        }
        PatKind::List(pats) => {
            for p in pats {
                collect_pat_bound_names(p, out);
            }
        }
        PatKind::Cons { head, tail } => {
            collect_pat_bound_names(head, out);
            collect_pat_bound_names(tail, out);
        }
        PatKind::Annot { pat, .. } => collect_pat_bound_names(pat, out),
    }
}

/// Reinterpret an argument *expression* as a *type* AST. Used for Π-lite
/// explicit type arguments: when a function's next parameter is a type-witness
/// (kind `Type`), the argument expression is a type written in value syntax —
/// a bare name `Int` / `T` (`Constructor`/`Var`) or an application `Maybe Int`.
/// Returns `None` for expressions that can't denote a type (then the argument
/// is treated as an ordinary value).
fn expr_to_type(expr: &ast::Expr) -> Option<ast::Type> {
    use knot::ast::{ExprKind, TypeKind};
    let span = expr.span;
    let node = match &expr.node {
        // Bare numeric base as a type argument means dimensionless (`Int 1`).
        // `ast_type_to_ty` rejects a bare `Int`/`Float` (unit enforcement), so
        // elaborate it to the dimensionless form here.
        ExprKind::Constructor(name)
            if name == "Int" || name == "Float" =>
        {
            TypeKind::UnitAnnotated {
                base: Box::new(knot::ast::Spanned {
                    node: TypeKind::Named(name.clone()),
                    span,
                }),
                unit: knot::ast::UnitExpr::Dimensionless,
            }
        }
        // Type heads are uppercase (`Constructor`); lowercase `Var` is always
        // a value (e.g. `f x`), never a type argument.
        ExprKind::Constructor(name) => TypeKind::Named(name.clone()),
        // `Int 1` — a numeric base applied to a dimensionless unit literal.
        ExprKind::App { func, arg }
            if matches!(&arg.node, ExprKind::Lit(knot::ast::Literal::Int(n)) if n == "1") =>
        {
            let base = expr_to_type(func)?;
            TypeKind::UnitAnnotated {
                base: Box::new(base),
                unit: knot::ast::UnitExpr::Dimensionless,
            }
        }
        ExprKind::App { func, arg } => {
            let f = expr_to_type(func)?;
            let a = expr_to_type(arg)?;
            TypeKind::App {
                func: Box::new(f),
                arg: Box::new(a),
            }
        }
        ExprKind::Annot { expr: inner, .. } => return expr_to_type(inner),
        _ => return None,
    };
    Some(knot::ast::Spanned { node, span })
}

/// Flatten an application spine `f a b …` into `[f, a, b, …]` (head-first).
/// A non-application expression yields a single-element vector.
fn flatten_spine(expr: &ast::Expr) -> Vec<&ast::Expr> {
    let mut spine = Vec::new();
    let mut cur = expr;
    while let ast::ExprKind::App { func, arg } = &cur.node {
        spine.push(arg.as_ref());
        cur = func.as_ref();
    }
    spine.push(cur);
    spine.reverse();
    spine
}

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

/// A synthesized `__result e` node: the final bare expression of a desugared
/// do-block, whose meaning is type-directed. If `e`'s type is an action in the
/// block's monad `m` the block's result IS `e`; otherwise `e` is a plain value
/// and the result is `pure e`. `resolve_result_markers` decides, unifies
/// accordingly, and rewrites the node.
struct ResultMarker {
    /// Span of the `__result` Var itself — the `monad_vars` key, and the node
    /// the AST rewrite looks for.
    span: Span,
    /// The do-block's monad type constructor.
    monad: TyVar,
    /// The do-block's result element type (`a` in `m a`).
    elem: TyVar,
    /// Inferred type of the final expression.
    arg: Ty,
    /// Span of the final expression, for the mismatch diagnostic.
    arg_span: Span,
    /// The rigid signature vars, and the `\/` unions declared over them, in
    /// force where the marker was written. `resolve_result_markers` runs long
    /// after the enclosing declaration dropped both, but the unify it performs
    /// *is* the do-block's sequencing step: re-checked in a context where
    /// nothing is rigid any more, two distinct signature rows (`IO {| r1}`
    /// sequenced with `IO {| r2}`) would merge silently and one row's effects
    /// would vanish from the declared result.
    skolems: Vec<TyVar>,
    effect_unions: Vec<EffectUnion>,
}

/// Maps `refine` expression spans to their resolved refined type name.
pub type RefineTargets = HashMap<Span, String>;

/// Refined type info exported for codegen: type_name → predicate expression.
pub type RefinedTypeInfoMap = HashMap<String, knot::ast::Expr>;

/// Maps `show` call-site spans to the canonical unit string of the argument
/// (e.g. `"M"`, `"M/S^2"`). Only concrete units appear: units are erased at
/// runtime, so this is the sole channel by which the unit reaches the emitted
/// code. Codegen emits `knot_value_show_unit` for spans found here and plain
/// `knot_value_show` for the rest.
pub type ShowUnitStrings = HashMap<Span, String>;

/// Maps declaration names to their inferred type display strings.
pub type TypeInfo = HashMap<String, String>;

/// Maps binding spans (local variables, params, patterns) to their inferred type strings.
pub type LocalTypeInfo = HashMap<Span, String>;

/// Resolved parseJson call-site info: the simple type name (for compile-time
/// FromJSON impl dispatch) and a wire type descriptor (for Maybe
/// normalization in the generic decoder — `null`/absent → Nothing, present
/// value → Just at `?`-marked positions).
#[derive(Debug, Clone, Default)]
pub struct FromJsonTarget {
    pub type_name: Option<String>,
    pub wire_schema: Option<String>,
}

/// Maps parseJson call-site spans to their resolved target info.
pub type FromJsonTargets = HashMap<Span, FromJsonTarget>;

/// Maps a `with` expression's span to the field names bound in its body.
/// Codegen cannot re-derive these — the record's field names come from its
/// *type*, not the AST — yet it must project each field into a local binding.
pub type WithFields = HashMap<Span, Vec<String>>;

/// Resolved `^name` implicit-field projections, keyed by the expression's
/// span: (root binding name, field path from the root to the field).
/// Codegen lowers `^name` to the root variable followed by one record-field
/// projection per path element.
pub type ImplicitRefs = HashMap<Span, (String, Vec<String>)>;

/// Callsite resolutions for implicit dictionaries: application span → the
/// `(root_binding, field_path)` of the in-scope record supplying the
/// dictionary. Codegen splices the projected record as the leading argument.
pub type ImplicitDictArgs = HashMap<Span, (String, Vec<String>)>;

/// Prefix for the unique, per-`with`-site alias a `with` field is also bound
/// under during inference (and codegen's flat `Env`): `{PREFIX}{with_span_start}@{field}`.
/// `^field` resolves against the alias so its codegen `Var` hits the lexically
/// correct slot; the bare field name keeps working for direct references.
/// `\0` makes the alias unutterable in source, so it can never collide with a
/// user binding. Shared with codegen (`crate::infer::WITH_FIELD_ALIAS_PREFIX`).
pub const WITH_FIELD_ALIAS_PREFIX: &str = "\0with:";

/// Prefix for the unique, per-`with`-site alias a `with` block's RECORD VALUE
/// is bound under during codegen, so an implicit dictionary resolved to that
/// `with` frame can project the whole record: `{PREFIX}{with_span_start}`.
/// Distinct from `WITH_FIELD_ALIAS_PREFIX` (which aliases each *field*); the
/// record alias is only created when a `^`-constrained callsite inside the
/// body resolved its dictionary to this `with`. Shared with codegen.
pub const WITH_RECORD_ALIAS_PREFIX: &str = "\0withrec:";

/// Spans of field-access expressions (`t.members`) whose field type is a
/// relation. Codegen cannot re-derive this — a record's field types are not
/// reachable from the AST — yet it must know: a do-bind whose right-hand side
/// is relation-typed iterates the rows (which is how inference types it), while
/// any other right-hand side binds the value whole.
pub type RelationFieldSpans = HashSet<Span>;

/// Spans of full `sum f rel` applications (including the `rel |> sum f` pipe
/// form) whose result type is a Float. Codegen passes this as an `is_float`
/// flag to the runtime, which needs it ONLY for an EMPTY relation: with no
/// summands there is nothing to take the numeric type from, so `sum` over an
/// empty `[Float]` would otherwise return `Int 0` instead of `Float 0.0`.
pub type SumFloatSpans = HashSet<Span>;

/// Spans of explicit type arguments consumed by the Π-lite application
/// diversion (`apply Int …` — the `Int` head). Codegen drops these arguments
/// (they are erased; the type-witness param has no runtime representation), so
/// an application `f Int x` compiles to `f x`.
pub type TypeArgSpans = HashSet<Span>;

/// Spans of `elem needle haystack` haystack arguments whose element type is a
/// SQL-pushable scalar (peeling aliases & refined types). Codegen consults these
/// sets to decide whether to push an `elem` down to SQL.
///
/// The two paths have different type constraints, so they are tracked separately:
/// - `literal` — the `IN (?, ?, …)` form for a syntactic list literal. Each
///   element binds as its stored representation, so `Int` (stored as TEXT) works.
/// - `dynamic` — the `IN (SELECT value FROM json_each(?))` form for a computed
///   haystack. `json_each` yields JSON storage classes (numbers as INTEGER), so
///   `Int`/`Int u` (TEXT-stored) never match and are EXCLUDED here even though
///   they are in `literal`. Only `Text`/`Bool`/`Uuid` are dynamic-safe.
///   `dynamic` is always a subset of `literal`.
#[derive(Clone, Default, Debug)]
pub struct ElemPushdownOk {
    pub literal: HashSet<Span>,
    pub dynamic: HashSet<Span>,
}

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

    /// True when this unit can still be dimensionless: it has no concrete
    /// base units (so every component is an unsolved variable that can bind
    /// to exponent 0). A concrete unit (`bases` non-empty) is NOT compatible
    /// with the bare dimensionless `Int`/`Float`.
    fn is_compatible_with_dimensionless(&self) -> bool {
        self.bases.is_empty()
    }

    fn normalize(&mut self) {
        self.bases.retain(|_, exp| *exp != 0);
        self.vars.retain(|_, exp| *exp != 0);
    }

    // Exponent arithmetic saturates rather than wrapping/panicking. Absurd
    // exponents (e.g. `M^2000000000 * M^2000000000`) are meaningless units, so
    // clamping to `i32::MIN/MAX` is harmless — but unchecked `+=`/`*=` would
    // panic in debug builds and silently wrap in release on a type-correct
    // program, turning a nonsensical annotation into a compiler crash.
    fn mul(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            let e = result.bases.entry(name.clone()).or_insert(0);
            *e = e.saturating_add(*exp);
        }
        for (&v, &exp) in &other.vars {
            let e = result.vars.entry(v).or_insert(0);
            *e = e.saturating_add(exp);
        }
        result.normalize();
        result
    }

    fn div(&self, other: &UnitTy) -> UnitTy {
        let mut result = self.clone();
        for (name, exp) in &other.bases {
            let e = result.bases.entry(name.clone()).or_insert(0);
            *e = e.saturating_sub(*exp);
        }
        for (&v, &exp) in &other.vars {
            let e = result.vars.entry(v).or_insert(0);
            *e = e.saturating_sub(exp);
        }
        result.normalize();
        result
    }

    fn pow(&self, n: i32) -> UnitTy {
        let mut result = self.clone();
        for exp in result.bases.values_mut() {
            *exp = exp.saturating_mul(n);
        }
        for exp in result.vars.values_mut() {
            *exp = exp.saturating_mul(n);
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
// `TyCon` deliberately shares the `Ty` prefix — it is standard PL terminology
// ("type constructor") and renaming would ripple across the whole crate.
#[allow(clippy::enum_variant_names)]
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
    Uuid,
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
    /// effect polymorphism: `IO {console, fs} a` or `IO {console | _} a`.
    /// The row tail unifies with whatever extra effects the caller's
    /// context introduces.
    IO(BTreeSet<IoEffect>, Option<TyVar>, Box<Ty>),
    /// Tail of an effect row — the binding form for an effect row variable.
    /// `Ty::EffectRow(extras, tail)` says: "the original row variable now
    /// stands for `extras`, possibly followed by another row variable
    /// `tail`". Only legal as the right-hand side of a substitution for a
    /// row variable that appeared in `Ty::IO`'s tail position.
    EffectRow(BTreeSet<IoEffect>, Option<TyVar>),
    /// Unit of measure carrier, used as a type argument to `Con("Int"/"Float", [Unit(u)])`.
    /// A standalone `Ty::Unit(u)` only appears as the sole argument of
    /// `Con("Int", _)` / `Con("Float", _)`; it is the kind-`Unit` type that
    /// describes the unit dimension of a numeric type. It is erased at
    /// runtime and has no value inhabitants.
    Unit(UnitTy),
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
    /// Associated-type projection `AssocName arg` (e.g. `Elem c`). Carries the
    /// projection through inference so it can be reduced once `arg` resolves to
    /// a concrete type matching an impl's `type AssocName <head> = <body>`
    /// definition. While `arg` is still a variable the projection is rigid: it
    /// only unifies with an identical projection (or a variable), which keeps
    /// the result from being silently equated with an arbitrary type.
    Assoc(String, Box<Ty>),
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

    /// True for `Ty::Int` and `Con("Int", [Unit(_)])` (unit-bearing Int).
    /// Use this instead of matching `Ty::Int` directly at sites that must
    /// also accept a unit-bearing Int.
    fn is_int_like(&self) -> bool {
        match self.peel_alias() {
            Ty::Int => true,
            Ty::Con(name, args) => name == "Int" && args.len() == 1 && matches!(args[0].peel_alias(), Ty::Unit(_)),
            _ => false,
        }
    }

    /// True for `Ty::Float` and `Con("Float", [Unit(_)])` (unit-bearing Float).
    fn is_float_like(&self) -> bool {
        match self.peel_alias() {
            Ty::Float => true,
            Ty::Con(name, args) => name == "Float" && args.len() == 1 && matches!(args[0].peel_alias(), Ty::Unit(_)),
            _ => false,
        }
    }

    /// Extract the `UnitTy` from `Con("Int"/"Float", [Unit(u)])`, peeling
    /// aliases. Returns `None` for plain `Int`/`Float` or anything else.
    fn unit_of(&self) -> Option<&UnitTy> {
        match self.peel_alias() {
            Ty::Con(name, args)
                if (name == "Int" || name == "Float")
                    && args.len() == 1 =>
            {
                match args[0].peel_alias() {
                    Ty::Unit(u) => Some(u),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Build `Con("Int", [Unit(u)])` — the canonical unit-bearing Int type.
    fn int_with_unit(u: UnitTy) -> Ty {
        Ty::Con("Int".to_string(), vec![Ty::Unit(u)])
    }

    /// Build `Con("Float", [Unit(u)])` — the canonical unit-bearing Float type.
    fn float_with_unit(u: UnitTy) -> Ty {
        Ty::Con("Float".to_string(), vec![Ty::Unit(u)])
    }
}

/// Can `value`'s numeric unit serve as `base`'s numeric unit? `base` is a
/// numeric type (`Int`, `Float`, `Int u`, or `Float u`) and `value` is a
/// numeric of the same kind. A dimensionless `base` (bare `Int`/`Float`)
/// accepts a value whose unit is dimensionless or still an unsolved var (a
/// literal). A concrete-unit base requires the value to carry a unit that can
/// match it.
fn numeric_unit_compatible(base: &Ty, value: &Ty) -> bool {
    let base_unit = base.unit_of();
    let value_unit = value.unit_of();
    match (base_unit, value_unit) {
        // Bare `Int`/`Float` base (dimensionless). The value qualifies unless
        // it carries a concrete non-trivial unit.
        (None, None) => true,
        (None, Some(vu)) => vu.is_compatible_with_dimensionless(),
        // Unit-bearing base: the value must carry a compatible unit. An
        // unsolved-var value unit can still bind to the base's unit.
        (Some(bu), None) => bu.is_compatible_with_dimensionless(),
        (Some(bu), Some(vu)) => {
            (bu.is_compatible_with_dimensionless() && vu.is_compatible_with_dimensionless())
                || bu == vu
        }
    }
}

/// Replace every free unit variable in a type with dimensionless (`1`),
/// leaving concrete units untouched. Used only for display/extraction of
/// monomorphic types, where an unsolved unit var means inference never pinned
/// the unit — mirroring the dimensionless defaulting codegen applies at
/// runtime. Never call this on a type that still participates in unification.
fn default_free_unit_vars(ty: &Ty) -> Ty {
    match ty {
        Ty::Unit(u) => {
            let mut u = u.clone();
            u.vars.clear();
            u.normalize();
            Ty::Unit(u)
        }
        Ty::Fun(p, r) => Ty::Fun(
            Box::new(default_free_unit_vars(p)),
            Box::new(default_free_unit_vars(r)),
        ),
        Ty::Record(fields, row) => Ty::Record(
            fields.iter().map(|(n, t)| (n.clone(), default_free_unit_vars(t))).collect(),
            *row,
        ),
        Ty::Relation(inner) => Ty::Relation(Box::new(default_free_unit_vars(inner))),
        Ty::Con(name, args) => Ty::Con(
            name.clone(),
            args.iter().map(default_free_unit_vars).collect(),
        ),
        Ty::Variant(ctors, row) => Ty::Variant(
            ctors.iter().map(|(n, t)| (n.clone(), default_free_unit_vars(t))).collect(),
            *row,
        ),
        Ty::App(f, a) => Ty::App(
            Box::new(default_free_unit_vars(f)),
            Box::new(default_free_unit_vars(a)),
        ),
        Ty::IO(eff, row, inner) => Ty::IO(
            eff.clone(),
            *row,
            Box::new(default_free_unit_vars(inner)),
        ),
        Ty::Assoc(name, inner) => Ty::Assoc(name.clone(), Box::new(default_free_unit_vars(inner))),
        Ty::Alias(name, inner) => Ty::Alias(name.clone(), Box::new(default_free_unit_vars(inner))),
        _ => ty.clone(),
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
    /// `r3 := r1 \/ r2` row-union constraints captured during generalization.
    /// Each `EffectUnion`'s vars reference `vars` and get freshened together
    /// at instantiation.
    effect_unions: Vec<EffectUnion>,
    /// Deferred `*`/`/` unit-composition checks captured during generalization
    /// (e.g. `\x -> x * x`). Like `effect_unions`, each one references `vars`
    /// and is freshened per instantiation so the same unit-polymorphic
    /// function can be applied at different units (`square 3.0 M` and
    /// `square 4.0 S` each get their own composition `M^2` / `S^2`).
    unit_binops: Vec<DeferredUnitBinop>,
    ty: Ty,
}

impl Scheme {
    fn mono(ty: Ty) -> Self {
        Scheme {
            vars: vec![],
            unit_vars: vec![],
            constraints: vec![],
            effect_unions: vec![],
            unit_binops: vec![],
            ty,
        }
    }

    fn poly(vars: Vec<TyVar>, ty: Ty) -> Self {
        Scheme {
            vars,
            unit_vars: vec![],
            constraints: vec![],
            effect_unions: vec![],
            unit_binops: vec![],
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
    /// Monotonically increasing push order. `check_skolem_constraints` uses
    /// this — not a positional index into `deferred_constraints` — to identify
    /// "constraints this body added", because `generalize_with_constraints`
    /// *removes* entries mid-body, which would invalidate any length snapshot.
    seq: u64,
}

/// A deferred unit-composition check for `*`/`/`: one operand carried a
/// concrete unit while the other was still an unresolved type variable at
/// the binop node (e.g. a field access on a lambda parameter whose record
/// type is only pinned later, when the lambda unifies with its call site).
/// Re-checked after inference completes, when the operand may have resolved;
/// `result` is the fresh variable returned as the binop's type, unified with
/// the composed unit once both sides are known.
#[derive(Debug, Clone)]
struct DeferredUnitBinop {
    op: knot::ast::BinOp,
    lhs: Ty,
    rhs: Ty,
    result: TyVar,
    span: Span,
}

/// `result := union(sources)` constraint produced by `r1 \/ r2 \/ ...`
/// effect-row syntax. The result row variable is bound to the union of each
/// source row variable's effects once those sources are resolved.
#[derive(Debug, Clone)]
struct EffectUnion {
    result: TyVar,
    sources: Vec<TyVar>,
    /// True when the constraint comes from a `\/` the user actually wrote
    /// (a signature's `IO {| r1 \/ r2}`, or a builtin like `race` declared
    /// the same way). Only a declared union licenses merging two *rigid*
    /// rows: constraints synthesised while checking a body (`merge_do_io_row`)
    /// must not license further merges, or a body could invent the very
    /// permission its signature withheld.
    declared: bool,
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

/// A type name brought into scope by a `with` peel over a record containing an
/// embedded `type`/`data` declaration. Confined to the `with` body.
#[derive(Debug, Clone)]
enum RecordTypeBinding {
    /// A parameterized embedded `type` alias referenced bare (`Pair`) — an
    /// unapplied type constructor. (Nullary embedded aliases are injected into
    /// the global `aliases` map for the body instead, so they behave exactly
    /// like top-level aliases.)
    TyCon,
    /// Embedded `data Name params = ctors`. Only the param count is needed:
    /// the type name resolves to a nominal `Ty::Con` (or a `TyCon` when
    /// parameterized), while constructor VALUES are reached through the
    /// record's namespace field (`rec.Name.Ctor`), not this binding.
    Data { params: Vec<String> },
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

    /// TyVars minted to freshen the free variables of a type alias body
    /// (the `a` in `type Box = {val: a}`). They are quantified in the
    /// enclosing scheme so each *reference* to the alias gets its own copy,
    /// but they are not universals the annotation promises: `b1 : Box` says
    /// "some `val` type", not "every `val` type", so checking `b1 = {val: 1}`
    /// must be free to solve the copy to `Int`. `skolemise_scheme` therefore
    /// instantiates these flexibly instead of turning them rigid.
    alias_free_vars: HashSet<TyVar>,

    /// Scoped variable environment (functions, let-bindings, params).
    scopes: Vec<HashMap<String, Scheme>>,

    /// Constructor metadata: ctor_name → one entry per ADT that declares it.
    /// Distinct ADTs may legally share a constructor name; keeping all
    /// candidates (rather than last-write-wins) lets `instantiate_ctor`
    /// produce a row-polymorphic open variant for an overloaded name instead
    /// of arbitrarily committing to whichever ADT was declared last.
    constructors: HashMap<String, Vec<CtorInfo>>,

    /// Data type definitions: type_name → info.
    data_types: HashMap<String, DataInfo>,

    /// Names of the built-in ADTs (`Bool`, `Maybe`, `Result`). Their
    /// constructors stay referenceable bare (`True`, `Just`, `Ok`); every
    /// user-declared constructor must be qualified. Populated by the built-in
    /// registration; user `data` decls never add to it.
    builtin_data_types: std::collections::HashSet<String>,

    /// Source/view relation types: name → full type (always Relation(...)).
    source_types: HashMap<String, Ty>,

    /// Derived relation types: name → full type.
    derived_types: HashMap<String, Ty>,

    /// Names that are views (for lenient set checking).
    view_names: HashSet<String>,

    /// Type aliases: name → resolved Ty.
    aliases: HashMap<String, Ty>,

    /// Parameterized type aliases: name → (param names, body AST type). Kept as
    /// the AST body (not a resolved Ty) so each application can elaborate the
    /// body with FRESH parameter variables and substitute the actual arguments,
    /// avoiding the shared-var pinning that resolving once would cause.
    param_aliases: HashMap<String, (Vec<String>, ast::Type)>,

    /// Lexically-scoped type names introduced by a `with` peel over a record
    /// that contains an embedded `type`/`data` declaration. A stack of scopes
    /// (one per enclosing `with`), each mapping a type name to its confined
    /// meaning. Consulted FIRST in `ast_type_to_ty`'s `Named` arm so these
    /// shadow everything else and vanish when the `with` body ends — nothing
    /// defined inside a record leaks into the enclosing type namespace.
    record_type_scopes: Vec<HashMap<String, RecordTypeBinding>>,

    /// Per-`with` stack of global-alias saves: when a `with` peels a record
    /// containing an embedded `type` alias, the alias is temporarily injected
    /// into the global `aliases` map (so it behaves exactly like a top-level
    /// alias) and the previous binding is recorded here. Each `with` pushes one
    /// frame; on body end the frame is popped and the prior aliases restored,
    /// so the alias never leaks past the body.
    with_alias_saves: Vec<Vec<(String, Option<Ty>)>>,

    /// Mapping from annotation type-variable names to TyVars (per-declaration).
    annotation_vars: HashMap<String, TyVar>,

    /// Π-lite type-witness parameters in scope: a stack of scopes (one per
    /// enclosing lambda that binds `\(T : Type)`), each mapping the witness
    /// name to its rigid skolem TyVar. Consulted by `ast_type_to_ty`'s `Named`
    /// arm so `x : T` inside the lambda resolves to the witness.
    type_param_scopes: Vec<HashMap<String, TyVar>>,

    /// Spans of application arguments that were consumed as a *type* (an
    /// explicit type argument for a type-witness parameter), not a value.
    /// Codegen erases these (emits no runtime argument for them).
    type_arg_spans: std::collections::HashSet<Span>,

    /// Accumulated type errors.
    errors: Vec<(String, Span)>,

    /// Monad type-constructor variables from desugared do-blocks.
    /// Each entry records (span, monad_tyvar) so we can resolve the
    /// concrete monad after inference completes.
    monad_vars: Vec<(Span, TyVar)>,
    /// Spans of synthesized `__empty` nodes (from desugaring a `where` guard or
    /// `Alternative`-using comprehension). After inference resolves the monad,
    /// we check the resolved type actually has an `Alternative` impl so a
    /// user-defined monad lacking one gets a clean diagnostic instead of a
    /// missing-impl panic in codegen.
    empty_spans: std::collections::HashSet<Span>,
    /// Spans of monad vars that were let-generalized (quantified into a
    /// local let-binding's type scheme). Used at Phase 5 to emit a warning
    /// when such a var stays unresolved and defaults to Relation dispatch —
    /// a sign the monad was polymorphic but never pinned to a concrete
    /// instance. Top-level function generalization is excluded (via
    /// `in_top_level_generalize`) to avoid false positives on `main = do …`
    /// and other top-level Relation do-blocks whose default is correct.
    generalized_monad_spans: std::collections::HashSet<Span>,
    /// Set to `true` while generalizing a top-level function body so that
    /// `generalize_with_constraints` skips marking monad vars as
    /// let-generalized. Reset to `false` afterwards.
    in_top_level_generalize: bool,
    /// Synthesized `__result e` nodes — a desugared do-block's final bare
    /// expression, whose meaning (`pure e` vs. `e`) depends on types.
    /// Resolved and rewritten away by `resolve_result_markers`.
    result_markers: Vec<ResultMarker>,
    /// Recursion depth of `unify_dir`. Refinement widening looks for
    /// unification *variables*, which only the outermost call still sees —
    /// recursive calls get sub-terms that `apply` has already substituted.
    unify_depth: usize,
    /// Full `traverse f rel` applications: (call span, result type var,
    /// container type var). Post-inference, relation-container calls get a
    /// `monad_info` entry keyed by the call span so codegen can tell the
    /// runtime which applicative's `pure []` an EMPTY input must produce.
    traverse_calls: Vec<(Span, TyVar, TyVar)>,
    /// Full `sum f rel` applications: (call span, result type var).
    /// Post-inference, calls whose result is a Float land in `SumFloatSpans`
    /// so codegen can tell the runtime which zero an EMPTY relation sums to.
    sum_calls: Vec<(Span, TyVar)>,

    /// Tracks `parseJson` application sites for compile-time FromJSON dispatch.
    /// Each entry records (app_span, return_type_var).
    from_json_calls: Vec<(Span, TyVar)>,

    /// Tracks `show` application sites so their argument's unit of measure can
    /// be resolved after inference. Each entry records (app_span, arg_ty); the
    /// arg type is recorded unresolved because a unit variable may only be
    /// solved by a later constraint. See `show_unit_strings`.
    show_calls: Vec<(Span, Ty)>,

    /// Known trait implementations: (trait_name, type_name). Only the
    /// intrinsic operator kernel remains: `deriving (Eq, Ord, …)` registrations
    /// plus the builtin primitive seeding in `check_inner` — these back the
    /// `+`/`<`/`++`/unary-`-`/`==` operator checks.
    known_impls: HashSet<(String, String)>,

    /// Top-level functions carrying signature-level `^`-field constraints:
    /// name → ordered `(field, field_type)` list. The function's stored scheme
    /// has already been elaborated to take a leading dictionary record per
    /// constraint (see desugar); this side-table records WHICH leading
    /// parameters are implicit dictionaries so each callsite can resolve them
    /// from scope instead of receiving them explicitly.
    implicit_dict_fns: HashMap<String, Vec<(String, Ty)>>,

    /// Callsite resolutions for implicit dictionaries: application span → the
    /// `(root_binding, field_path)` of the in-scope record that supplies the
    /// dictionary. Codegen splices the projected record as the leading
    /// argument at that application. Keyed by the application's span (the
    /// outermost `App` node's span).
    implicit_dict_args: HashMap<Span, (String, Vec<String>)>,

    /// Deferred trait constraint checks, resolved after inference.
    deferred_constraints: Vec<DeferredConstraint>,

    /// Next sequence number to stamp onto a pushed `DeferredConstraint`.
    next_constraint_seq: u64,

    /// `r3 := r1 \/ r2 \/ ...` row-union constraints produced by `\/`
    /// syntax in IO type annotations. Resolved after each declaration's
    /// inference so the result row picks up the union of its sources'
    /// effects.
    pending_effect_unions: Vec<EffectUnion>,

    /// Upper bounds recorded when an effect-union RESULT row var is unified
    /// against a *closed* (concrete, tail-`None`) required effect row before
    /// the union is resolved. Maps the union-result var → the closed set its
    /// row must stay within. `unify_io_effects` only stores the *difference*
    /// when it closes a row, so the full required set is otherwise lost by the
    /// time `resolve_effect_union` runs; without this, the union's resolution
    /// would silently overwrite that closed binding and launder a `race`/`fork`
    /// result's effects through a value typed with fewer effects. Checked in
    /// `resolve_effect_union`. Multiple bounds intersect (most restrictive wins).
    /// The `Span` anchors the violation diagnostic at the offending unify site.
    effect_union_upper_bounds: HashMap<TyVar, (BTreeSet<IoEffect>, Span)>,

    /// Spans of local variable bindings and their types (for LSP hover).
    binding_types: Vec<(Span, Ty)>,

    /// Route constructor → response type mapping (for `fetch` return type resolution).
    fetch_response_types: HashMap<String, ast::Type>,

    /// Route constructor → response header fields (for `fetch` response wrapping).
    fetch_response_headers: HashMap<String, Vec<ast::Field<ast::Type>>>,

    /// Names of ADT types declared via `route` / `route ... =` (the only types
    /// `listen` accepts as a handler input). Populated in `pre_register`.
    route_types: HashSet<String>,

    /// Route ADT name → flat list of (constructor name, route entry) pairs,
    /// including composite routes which inherit their components' entries.
    /// Used by `serve` typing to derive each handler's expected type from
    /// the matching route entry. Populated in `pre_register`.
    route_entries_by_api: HashMap<String, Vec<ast::RouteEntry>>,

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

    /// Whether we are typing a view body. View bodies are relation
    /// comprehensions (mirrors codegen's `analyze_view`): a do-block bind
    /// from an IO-wrapped relation iterates the relation's ELEMENTS, and
    /// the block's result is the relation of yielded values — not an IO.
    in_view_comprehension: bool,

    // ── Units of measure ──────────────────────────────────────────
    /// Next unit variable ID.
    next_unit_var: UnitVar,
    /// Unit variable substitution.
    unit_subst: HashMap<UnitVar, UnitTy>,
    /// Rigid unit variables introduced while checking a function body against a
    /// unit-polymorphic signature. `unify_units` refuses to solve these, so the
    /// body cannot silently narrow a `∀u` signature to a concrete unit (which
    /// would be unsound — e.g. a body mixing `<S>` and `<M>` would otherwise
    /// type-check). Removed once the body check completes.
    unit_skolems: HashSet<UnitVar>,
    /// Unit variable names from type annotations: name → UnitVar.
    annotation_unit_vars: HashMap<String, UnitVar>,
    /// Whether we are currently processing a type annotation (so undeclared
    /// unit names are treated as polymorphic unit variables).
    in_type_annotation: bool,
    /// Whether bare `Int`/`Float` are rejected (require an explicit unit).
    /// Set for value annotations AND for type-alias / data-decl bodies, which
    /// are converted outside `in_type_annotation` but must still be checked.
    enforce_units: bool,

    // ── Refined types ─────────────────────────────────────────────
    /// Refined type metadata: type_name → (base Ty, predicate Expr).
    refined_types: HashMap<String, (Ty, knot::ast::Expr)>,
    /// Refine expression type vars: (span, alpha_var, inner_ty) for post-inference resolution.
    refine_vars: Vec<(Span, TyVar, Ty)>,

    /// Field-access expressions: (span, field type). The field's type is often
    /// still an unsolved variable when the access is inferred, so the relation-
    /// valued ones are sieved out post-inference into `RelationFieldSpans`.
    field_accesses: Vec<(Span, Ty)>,
    /// Field names bound by each `with` expression, keyed by the `with`'s span.
    /// Codegen projects exactly these fields into locals for the body.
    with_fields: Vec<(Span, Vec<String>)>,
    /// Stack of `(with_expr_span, field → scheme)` frames for the `with`
    /// expressions enclosing the expression currently being inferred — parallel
    /// to `self.scopes` (a `with` pushes exactly one scope). Lets the `Var` arm
    /// detect that a variable resolved to a `with` FIELD and redirect codegen's
    /// flat-`Env` lookup to that `with` site's unique alias (see the `Var` arm
    /// and `WITH_FIELD_ALIAS_PREFIX`). A `None` scope entry keeps the two stacks
    /// aligned when a non-`with` construct pushes a scope.
    with_scope_stack: Vec<Option<(Span, HashMap<String, Scheme>)>>,
    /// Resolved `^name` implicit-field projections: span → (root binding,
    /// field path). Populated when an `ImplicitRef` is resolved; handed to
    /// codegen via `ImplicitRefs` so it can emit the projection chain.
    implicit_refs: ImplicitRefs,
    /// Refined-type names for which the directional refined-type check (which
    /// otherwise rejects implicitly introducing a refinement, e.g. a raw `Int`
    /// flowing where a `Nat` is required) is suppressed. `None` = suppress
    /// nothing (the default). Set to `Some(names)` ONLY while checking a `set` /
    /// `replace` value against its source's element type, where `names` is
    /// exactly the set of refined types appearing in that element type: every
    /// row written is validated at runtime (`knot_refinement_validate_relation`),
    /// so implicitly coercing a base value into one of *those* refinements is
    /// sound — including when the value flows through plumbing like
    /// `union rows [newRow]`.
    ///
    /// Crucially, it is scoped to the source's own refinements: a refined type
    /// used only as a *function parameter* inside the value expression (e.g.
    /// `divBy : Pos -> Int` called as `divBy someInt`) is NOT in this set, so
    /// the raw argument is still rejected — the runtime never validates that
    /// call boundary, so implicit introduction there would be unsound. Saved/
    /// restored around the single `check_expr`. See the refined arms in
    /// `unify_dir`.
    suppress_refine_intro: Option<HashSet<String>>,

    /// Unit-composition checks for `*`/`/` deferred because one operand was
    /// still an unresolved type variable when the binop was inferred. When the
    /// enclosing binding is generalized, `generalize` moves the relevant
    /// entries onto the resulting `Scheme` (`Scheme::unit_binops`) so each
    /// instantiation re-arms its own copy; the rest are resolved once at
    /// end-of-inference by `resolve_deferred_unit_binops`.
    deferred_unit_binops: Vec<DeferredUnitBinop>,

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
            alias_free_vars: HashSet::new(),
            scopes: vec![HashMap::new()],
            // One `None` per starting scope (the global scope is not a `with`).
            constructors: HashMap::new(),
            data_types: HashMap::new(),
            builtin_data_types: std::collections::HashSet::new(),
            source_types: HashMap::new(),
            derived_types: HashMap::new(),
            view_names: HashSet::new(),
            aliases: HashMap::new(),
            param_aliases: HashMap::new(),
            record_type_scopes: Vec::new(),
            with_alias_saves: Vec::new(),
            annotation_vars: HashMap::new(),
            type_param_scopes: Vec::new(),
            type_arg_spans: std::collections::HashSet::new(),
            errors: Vec::new(),
            monad_vars: Vec::new(),
            empty_spans: std::collections::HashSet::new(),
            generalized_monad_spans: std::collections::HashSet::new(),
            in_top_level_generalize: false,
            result_markers: Vec::new(),
            unify_depth: 0,
            traverse_calls: Vec::new(),
            sum_calls: Vec::new(),
            from_json_calls: Vec::new(),
            show_calls: Vec::new(),
            known_impls: HashSet::new(),
            implicit_dict_fns: HashMap::new(),
            implicit_dict_args: HashMap::new(),
            deferred_constraints: Vec::new(),
            next_constraint_seq: 0,
            pending_effect_unions: Vec::new(),
            effect_union_upper_bounds: HashMap::new(),
            binding_types: Vec::new(),
            fetch_response_types: HashMap::new(),
            route_entries_by_api: HashMap::new(),
            fetch_response_headers: HashMap::new(),
            route_types: HashSet::new(),
            in_io_do: false,
            in_atomic: false,
            in_view_comprehension: false,
            source_var_binds: HashMap::new(),
            let_bindings: HashMap::new(),
            next_unit_var: 0,
            unit_subst: HashMap::new(),
            unit_skolems: HashSet::new(),
            annotation_unit_vars: HashMap::new(),
            in_type_annotation: false,
            enforce_units: false,
            refined_types: HashMap::new(),
            refine_vars: Vec::new(),
            field_accesses: Vec::new(),
            with_fields: Vec::new(),
            with_scope_stack: vec![None],
            implicit_refs: HashMap::new(),
            suppress_refine_intro: None,
            deferred_unit_binops: Vec::new(),
            elem_pushdown_ok: ElemPushdownOk::default(),
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
            // Float is deliberately excluded: `IN` / `=` in SQL is IEEE equality
            // (-0.0 = +0.0, NaN stored as NULL), while Knot compares floats with
            // `total_cmp` (-0.0 ≠ +0.0, NaN orderable). Pushing a float `elem`
            // down — whether the needle is a bare column, a computed value, or
            // a literal — would silently disagree with in-memory semantics, so
            // keep every float `elem` in memory (see the codegen `elem` gates).
            Ty::Int | Ty::Text | Ty::Bool | Ty::Uuid => true,
            // Unit-bearing Int is `Con("Int", [Unit(_)])` — the unit is erased
            // at runtime, so it is SQL-pushable just like plain Int. (Unit-
            // bearing Float is excluded for the same total_cmp reason.)
            Ty::Con(name, args)
                if name == "Int" && args.len() == 1 && matches!(args[0].peel_alias(), Ty::Unit(_)) =>
            {
                true
            }
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

    /// Whether a resolved haystack type is safe for the *dynamic* `elem` path
    /// (`IN (SELECT value FROM json_each(?))`). Stricter than
    /// `is_elem_haystack_pushable`: `json_each` yields JSON storage classes, so
    /// `Int`/`Int u` (stored as TEXT) never match and must fall back to memory.
    fn is_elem_haystack_dynamic_pushable(&self, ty: &Ty) -> bool {
        let peeled = ty.peel_alias();
        let inner = match peeled {
            Ty::Relation(t) => self.apply(t),
            _ => return false,
        };
        self.is_dynamic_pushable_scalar_for_elem(&inner)
    }

    fn is_dynamic_pushable_scalar_for_elem(&self, ty: &Ty) -> bool {
        match ty.peel_alias() {
            // Int is stored as TEXT but JSON-encodes as a number, so
            // `json_each` yields INTEGER values that never match the TEXT column
            // (see the literal path, which binds Int elements as TEXT and works).
            // Float is excluded for the same total_cmp-vs-IEEE reason as above.
            Ty::Text | Ty::Bool | Ty::Uuid => true,
            Ty::Con(name, args) if args.is_empty() => {
                self.refined_types
                    .get(name)
                    .map(|(base, _)| self.is_dynamic_pushable_scalar_for_elem(base))
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

    /// True when `ty` resolves to a concrete type that can serve as a
    /// refinement base. Used to distinguish "a real base value is being
    /// supplied" (reject the implicit introduction of a refinement) from
    /// "the type is still an unresolved variable" (let inference flow). An
    /// unrelated concrete type is left to the normal mismatch path.
    ///
    /// Covers the primitive bases (Int/Float/Text/Bool, with or without
    /// units) as well as the composite bases a refined alias can wrap —
    /// records (`type Valid = {x: Int 1} where …`) and relations. Without the
    /// composite forms, a concrete record/relation value flowing into a
    /// refined type with a matching base would skip the guard and be
    /// laundered into the refined type with no predicate check.
    /// Whether a refined type's declared base is compatible with the type of
    /// the value being refined. Exact structural equality misses the case
    /// where the value is a unit-polymorphic numeric (`Int <var>`, from a
    /// literal) while the declared base is dimensionless (`Int`) — both are
    /// the same numeric kind and an unsolved-unit value can always be
    /// dimensionless, so they match. A concrete-unit base (`Metres =
    /// Float M`) still requires the value to carry that exact unit.
    fn refined_base_compatible(&self, base: &Ty, value: &Ty) -> bool {
        let base = self.apply(base);
        let value = self.apply(value);
        match (base.peel_alias(), value.peel_alias()) {
            // Same numeric kind. The value's unit must be able to be the
            // base's unit: when the base is dimensionless (`Int`/`Float`),
            // any value whose unit is dimensionless or still an unsolved var
            // qualifies (a literal-derived `Int <var>`).
            (Ty::Int, v) if v.is_int_like() => numeric_unit_compatible(&base, v),
            (Ty::Float, v) if v.is_float_like() => numeric_unit_compatible(&base, v),
            (Ty::Con(bn, ba), Ty::Con(vn, va))
                if bn == vn
                    && (bn == "Int" || bn == "Float")
                    && matches!(ba.first(), Some(Ty::Unit(_)))
                    && matches!(va.first(), Some(Ty::Unit(_))) =>
            {
                numeric_unit_compatible(&base, &value)
            }
            _ => false,
        }
    }

    fn is_concrete_refinement_base(&self, ty: &Ty) -> bool {
        matches!(
            self.apply(ty),
            Ty::Int
                | Ty::Float
                | Ty::Text
                | Ty::Bool
                | Ty::Bytes
                | Ty::Uuid
                | Ty::Record(..)
                | Ty::Relation(_)
                // A nominal ADT / data base (`type Warm = Color where …`).
                // Without this, a plain `Color` value flowing where `Warm` is
                // required skips the introduction guard and is laundered into
                // the refined type with no predicate check. (A *different*
                // refined `Con` never reaches the guard — the refined
                // subsumption arms exclude it — so this only ever rejects a
                // genuine base value.)
                | Ty::Con(..)
                // Open variants (`<Ctor {} | r>`) can serve as a refinement
                // base. Without this, constructor-pattern scrutinees typed as
                // open variants bypass the introduction guard and unify
                // through `resolve_refined_base` with no `refine` and no
                // runtime validation.
                | Ty::Variant(..)
        )
    }

    /// Arithmetic and concatenation do not preserve a refinement predicate
    /// (`Nat - Nat` can be negative; `Short ++ Short` can exceed the length
    /// bound), so the result of a `Num`/negation/`Semigroup` op on a refined
    /// operand must degrade to the refined type's *base*. Otherwise a value
    /// that never passed `refine` inhabits the refined type — e.g.
    /// `sub : Nat -> Nat -> Nat = \a b -> a - b` would launder `-2` into `Nat`.
    /// After degrading, the directional subsumption check forces the caller to
    /// `refine` the result wherever a refined type is required.
    fn degrade_refinement(&mut self, ty: Ty, span: Span) -> Ty {
        if let Ty::Con(name, args) = self.apply(&ty)
            && args.is_empty() && self.refined_types.contains_key(&name)
                && let Some(base) = self.resolve_refined_base(&name, span) {
                    return base;
                }
        ty
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
                    // Saturating: exponents this large are nonsensical units,
                    // but unchecked arithmetic would panic/wrap on a
                    // type-correct program (see `UnitTy::mul`).
                    for (name, &base_exp) in &resolved.bases {
                        let e = next.bases.entry(name.clone()).or_insert(0);
                        *e = e.saturating_add(base_exp.saturating_mul(exp));
                    }
                    for (&rv, &rexp) in &resolved.vars {
                        let e = next.vars.entry(rv).or_insert(0);
                        *e = e.saturating_add(rexp.saturating_mul(exp));
                    }
                } else {
                    let e = next.vars.entry(v).or_insert(0);
                    *e = e.saturating_add(exp);
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

        // Reduce the two sides to a single equation: `a == b` iff the quotient
        // `diff = a / b` is dimensionless. Solving for one flexible variable in
        // `diff` yields the most general unifier and handles both one-sided and
        // shared variables uniformly.
        let diff = a.div(&b);
        if diff.is_dimensionless() {
            return;
        }

        // Solve `diff = 1` for one flexible (non-skolem) unit variable `v`:
        // `v^e · rest = 1` ⇒ `v = rest^(-1/e)`, which needs `e` to divide every
        // remaining exponent. Skolems are rigid and must never be solved (a
        // unit-polymorphic signature variable cannot be narrowed by the body),
        // though a flexible var may still be solved *to* a skolem. Prefer a
        // variable with |e| == 1 (always cleanly solvable); otherwise try any
        // whose exponent evenly divides the rest. Considering *every* candidate
        // rather than the first is what lets `Float (u*u*v)` unify with
        // `Float M` (solve the exp-1 `v`, leaving `u` free) and `Float u`
        // unify with `Float (u^2)` (solve the shared `u` to dimensionless) — a
        // first-only greedy pick would wrongly reject both.
        let mut candidates: Vec<UnitVar> = diff.vars.keys()
            .filter(|v| !self.unit_skolems.contains(v))
            .copied()
            .collect();
        candidates.sort_by_key(|v| diff.vars[v].abs());

        for v in candidates {
            let e = diff.vars[&v];
            // rest = diff without `v`; `v` therefore can't appear in its own
            // solution (so no occurs-cycle for `apply_unit` to chase).
            let mut rest = diff.clone();
            rest.vars.remove(&v);
            let clean = rest.bases.values().all(|x| x % e == 0)
                && rest.vars.values().all(|x| x % e == 0);
            if !clean {
                continue;
            }
            // v = rest^(-1/e): negate and divide every exponent by e.
            for x in rest.bases.values_mut() { *x = -(*x / e); }
            for x in rest.vars.values_mut() { *x = -(*x / e); }
            rest.normalize();
            self.unit_subst.insert(v, rest);
            return;
        }

        // No solvable flexible variable remains: the residual difference is
        // concrete bases and/or rigid skolem variables, so the units genuinely
        // differ.
        self.error(
            format!("unit mismatch: {} vs {}", a.display(), b.display()),
            span,
        );
    }

    /// Convert an AST UnitExpr to our internal UnitTy.
    /// When `in_type_annotation` is true, lowercase unit names are treated
    /// as polymorphic unit variables (analogous to type variables).
    fn ast_unit_to_unit_ty(&mut self, u: &ast::UnitExpr) -> UnitTy {
        match u {
            ast::UnitExpr::Dimensionless => UnitTy::dimensionless(),
            ast::UnitExpr::Named(name) => {
                if self.in_type_annotation && name.starts_with(|c: char| c.is_lowercase()) {
                    // In annotation context, lowercase unit names are variables
                    let var = self.annotation_unit_var(name);
                    UnitTy::var(var)
                } else {
                    // Uppercase (or non-annotation) names are concrete units.
                    // Units need no declaration: any name is a valid unit.
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
            ast::UnitExpr::Hole => {
                // Unit hole `_`: a fresh unit variable bound by unification.
                // Each occurrence is independent (matching value-type `_`).
                UnitTy::var(self.fresh_unit_var())
            }
        }
    }

    /// Get the unit from a type, if it has one. Returns None for dimensionless
    /// or non-numeric types.
    #[allow(dead_code)]
    fn type_unit(&self, ty: &Ty) -> Option<UnitTy> {
        ty.unit_of().map(|u| self.apply_unit(u))
    }

    /// Check if a type is numeric (Int, Float, or unit-bearing Int/Float).
    #[allow(dead_code)]
    fn is_numeric(&self, ty: &Ty) -> bool {
        ty.is_int_like() || ty.is_float_like()
    }

    fn error(&mut self, msg: String, span: Span) {
        // Dedup identical diagnostics: the alias fixpoint and multi-pass
        // collection can re-derive the same error at the same span several
        // times (e.g. a bare `Int` inside a type alias). Report it once.
        if self.errors.iter().any(|(m, s)| *m == msg && *s == span) {
            return;
        }
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
        self.apply_impl(ty, &[])
    }

    /// True when an implicit introduction of the refined type `name` should be
    /// allowed (suppressed) at the current site — i.e. `name` is one of the
    /// source refinements the runtime validates for the `set`/`replace` value
    /// being checked. See `suppress_refine_intro`.
    fn refine_intro_suppressed(&self, name: &str) -> bool {
        match &self.suppress_refine_intro {
            Some(names) => names.contains(name),
            None => false,
        }
    }

    /// Collect the names of every refined type (a `Con(name, [])` with `name`
    /// registered in `refined_types`) reachable in `ty`, resolving
    /// substitutions first. Used to scope refined-introduction suppression to
    /// exactly the source relation's own refinements.
    fn refined_names_in(&self, ty: &Ty) -> HashSet<String> {
        let mut out = HashSet::new();
        self.collect_refined_names(&self.apply(ty), &mut out);
        out
    }

    fn collect_refined_names(&self, ty: &Ty, out: &mut HashSet<String>) {
        match ty {
            Ty::Con(name, args) => {
                if args.is_empty() && self.refined_types.contains_key(name) {
                    out.insert(name.clone());
                }
                for a in args {
                    self.collect_refined_names(a, out);
                }
            }
            Ty::Fun(p, r) => {
                self.collect_refined_names(p, out);
                self.collect_refined_names(r, out);
            }
            Ty::Record(fields, _) | Ty::Variant(fields, _) => {
                for v in fields.values() {
                    self.collect_refined_names(v, out);
                }
            }
            Ty::Relation(inner)
            | Ty::IO(_, _, inner)
            | Ty::Alias(_, inner)
            | Ty::Assoc(_, inner)
            | Ty::Forall(_, inner) => self.collect_refined_names(inner, out),
            Ty::App(f, a) => {
                self.collect_refined_names(f, out);
                self.collect_refined_names(a, out);
            }
            _ => {}
        }
    }

    /// Like `apply` but skips substitution for any `TyVar` in `excluded`,
    /// so that `Forall`-bound variables are not captured by the outer
    /// substitution (mirrors `subst_ty`'s shadowing).
    fn apply_impl(&self, ty: &Ty, excluded: &[TyVar]) -> Ty {
        match ty {
            Ty::Var(v) => {
                if excluded.contains(v) {
                    return ty.clone();
                }
                match self.subst.get(v) {
                    Some(resolved) => self.apply_impl(resolved, excluded),
                    None => ty.clone(),
                }
            }
            Ty::Fun(p, r) => {
                Ty::Fun(Box::new(self.apply_impl(p, excluded)), Box::new(self.apply_impl(r, excluded)))
            }
            Ty::Record(fields, row) => {
                let mut applied: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.apply_impl(v, excluded)))
                    .collect();
                if let Some(rv) = row {
                    let resolved = self.apply_impl(&Ty::Var(*rv), excluded);
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
                    .map(|(k, v)| (k.clone(), self.apply_impl(v, excluded)))
                    .collect();
                if let Some(rv) = row {
                    let resolved = self.apply_impl(&Ty::Var(*rv), excluded);
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
                Ty::Relation(Box::new(self.apply_impl(inner, excluded)))
            }
            Ty::Con(name, args) => {
                let applied_args: Vec<Ty> =
                    args.iter().map(|a| self.apply_impl(a, excluded)).collect();
                // Unit folding: `Con("Int"/"Float", [Unit(u)])` resolves the
                // unit and collapses a dimensionless result back to plain
                // `Ty::Int`/`Ty::Float`. Anything else is a normal type
                // application. The `Unit` arm below handles unit substitution
                // for the inner `Ty::Unit(u)`, so by the time we get here the
                // arg may already be a substituted `Unit(u)` — but if the arg
                // was something exotic we must not pretend it's a unit.
                if (name == "Int" || name == "Float") && applied_args.len() == 1 {
                    if let Ty::Unit(u) = &applied_args[0] {
                        let u = self.apply_unit(u);
                        if u.is_dimensionless() {
                            return if name == "Int" { Ty::Int } else { Ty::Float };
                        }
                        return if name == "Int" {
                            Ty::int_with_unit(u)
                        } else {
                            Ty::float_with_unit(u)
                        };
                    }
                }
                Ty::Con(name.clone(), applied_args)
            }
            Ty::TyCon(_) => ty.clone(),
            Ty::App(f, a) => {
                let f = self.apply_impl(f, excluded);
                let a = self.apply_impl(a, excluded);
                Self::normalize_app(f, a)
            }
            Ty::IO(effects, row, inner) => {
                let inner = self.apply_impl(inner, excluded);
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                Ty::IO(effects, row, Box::new(inner))
            }
            Ty::EffectRow(effects, row) => {
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                Ty::EffectRow(effects, row)
            }
            Ty::Unit(u) => {
                let u = self.apply_unit(u);
                // A standalone `Ty::Unit` is only meaningful inside
                // `Con("Int"/"Float", [Unit(u)])`, whose `Con` arm does the
                // dimensionless fold. Keep the substituted unit here; the
                // surrounding `Con` arm re-folds if needed.
                Ty::Unit(u)
            }
            Ty::Forall(vars, inner) => {
                let mut new_excluded = excluded.to_vec();
                for v in vars.iter() {
                    new_excluded.push(*v);
                }
                Ty::Forall(vars.clone(), Box::new(self.apply_impl(inner, &new_excluded)))
            }
            Ty::Alias(name, inner) => {
                Ty::Alias(name.clone(), Box::new(self.apply_impl(inner, excluded)))
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
                // Keep partial-application form when applying to an effect row
                // so `App(App(IO, EffectRow), val)` normalizes via the next arm
                // to `IO(effects, row, val)` instead of collapsing the effect
                // row into the value-type slot.
                if matches!(&a, Ty::EffectRow(_, _)) {
                    Ty::App(Box::new(f), Box::new(a))
                } else {
                    Ty::IO(BTreeSet::new(), None, Box::new(a))
                }
            }
            // `App(App(TyCon("IO"), EffectRow), a)` — IO partially applied to
            // an effect row, then applied to a value. Built by the App-vs-IO
            // unification when binding a monad type variable to IO so that
            // effect/row information carries through `App(m, _)` instead of
            // being lost (which would force closed-empty IO).
            Ty::App(ref inner_f, ref eff) => {
                if let (Ty::TyCon(name), Ty::EffectRow(effects, row)) =
                    (inner_f.as_ref(), eff.as_ref())
                    && name == "IO" {
                        return Ty::IO(effects.clone(), *row, Box::new(a));
                    }
                Ty::App(Box::new(f), Box::new(a))
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
                    if let Some(resolved) = self.subst.get(rv)
                        && self.occurs_in(var, resolved) {
                            return true;
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
            Ty::Assoc(_, inner) => self.occurs_in(var, inner),
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
            if let Ty::Var(other) = &ty
                && *other == v {
                    return;
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

    /// Follow a substitution chain of `Var → Var → …` links to its last
    /// variable (the union-find representative). Rebinding must happen
    /// *there* — inserting at an interior var would orphan every alias
    /// further down the chain.
    fn var_chain_end(&self, v: TyVar) -> TyVar {
        let mut cur = v;
        let mut steps = 0usize;
        while let Some(Ty::Var(next)) = self.subst.get(&cur) {
            if *next == cur {
                break;
            }
            // A substitution chain this long can only mean an occurs-check bug
            // left a cycle behind. Silently breaking here would return an
            // interior variable and miscompile downstream, so fail loudly with a
            // clear message rather than papering over the compiler bug.
            if steps > 10_000 {
                panic!(
                    "knot type inference: substitution chain exceeds 10K steps for var {:?} \
                     — possible occurs-check bug",
                    cur
                );
            }
            cur = *next;
            steps += 1;
        }
        // If the chain exceeds 100K steps, something is fundamentally wrong
        // (the occurs check should prevent cycles). Report via eprintln
        // rather than self.error, which would need &mut self — var_chain_end
        // is called from contexts that only have &self.
        if steps > 100_000 {
            eprintln!(
                "knot infer: warning: substitution chain exceeds 100K steps for var {:?} — possible occurs-check bug",
                cur
            );
        }
        cur
    }

    // ── Unification ──────────────────────────────────────────────

    fn unify(&mut self, t1: &Ty, t2: &Ty, span: Span) {
        // By convention `t1` is the actual/provided type and `t2` the
        // expected/required type (most call sites follow this order).
        self.unify_dir(t1, t2, span, true);
    }

    /// Skolemise the body of a `Ty::Forall`: each quantified var becomes a
    /// fresh rigid TyVar registered in `self.skolems`. Used when a Forall
    /// appears on the *required* side of unification — the polymorphic
    /// interface must hold for every instantiation, so the quantified vars
    /// must stay rigid rather than being instantiated at a single witness.
    fn skolemise_forall_body(&mut self, vars: &[TyVar], body: &Ty) -> (Ty, Vec<TyVar>) {
        let mut fresh_skolems: Vec<TyVar> = Vec::with_capacity(vars.len());
        let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
        for v in vars {
            let s = self.fresh_var();
            self.skolems.insert(s);
            fresh_skolems.push(s);
            mapping.insert(*v, Ty::Var(s));
        }
        (self.subst_ty(body, &mapping), fresh_skolems)
    }

    /// Directed unification. `t1_provided` records which side currently
    /// plays the "provided/actual" role: it starts as `t1` and flips each
    /// time we descend into a function parameter (contravariance). The
    /// polarity only matters for `Ty::Forall`: a polymorphic type that is
    /// *provided* may be instantiated at any witness, while a polymorphic
    /// type that is *required* must be skolemised so the requirement can't
    /// be silently narrowed to a single instantiation (rank-2 soundness).
    fn unify_dir(&mut self, t1: &Ty, t2: &Ty, span: Span, t1_provided: bool) {
        // Before `apply` erases which parts of these types are still
        // *variables*, re-point any variable pinned to a refined type that is
        // required to hold the refinement's base type. Only the outermost call
        // can find anything: the recursive ones receive fully-applied
        // sub-terms, whose variables `apply` has already substituted away.
        if self.unify_depth == 0 {
            self.widen_refined_vars(t1, t2, 0);
        }
        self.unify_depth += 1;
        self.unify_inner(t1, t2, span, t1_provided);
        self.unify_depth -= 1;
    }

    /// Re-point unification variables that were pinned to a refined type but
    /// are required to hold values of the refinement's *base* type.
    ///
    /// `elem : a -> [a] -> Bool` called as `elem (n : ServerName) (xs : [Text])`
    /// binds `a := ServerName` from the needle, then rejects `xs` — a plain
    /// `Text` cannot implicitly become a `ServerName`. But nothing is being
    /// laundered *into* the refinement here: the call simply wants `a` to be
    /// the wider `Text`, which every `ServerName` already is. Forgetting a
    /// refinement is the same subsumption `unify` already permits between
    /// concrete types, so widen the variable and let the call through.
    ///
    /// Strictly one-way. Re-pointing a variable pinned to `Text` at
    /// `ServerName` would let `\t -> filter (\_ -> True) [t] : [ServerName]`
    /// launder an arbitrary `Text` into a refined list, so that never happens.
    /// It is also why this walks *variables* and not concrete types:
    /// `asNat : Int 1 -> Nat; asNat = \x -> x` offers no variable to widen and
    /// stays rejected.
    fn widen_refined_vars(&mut self, t1: &Ty, t2: &Ty, depth: usize) {
        // Types are finite (the occurs check rules out cyclic substitutions),
        // but bound the walk anyway — this runs on every unification.
        if depth > 64 {
            return;
        }
        let (v1, a) = self.shallow_resolve(t1);
        let (v2, b) = self.shallow_resolve(t2);

        if let Some(v) = v1
            && let Some(base) = self.widened_base(&a, &b)
        {
            self.subst.insert(v, base);
            return;
        }
        if let Some(v) = v2
            && let Some(base) = self.widened_base(&b, &a)
        {
            self.subst.insert(v, base);
            return;
        }

        match (&a, &b) {
            (Ty::Relation(x), Ty::Relation(y)) => self.widen_refined_vars(x, y, depth + 1),
            (Ty::Fun(p1, r1), Ty::Fun(p2, r2)) => {
                self.widen_refined_vars(p1, p2, depth + 1);
                self.widen_refined_vars(r1, r2, depth + 1);
            }
            (Ty::App(f1, x1), Ty::App(f2, x2)) => {
                self.widen_refined_vars(f1, f2, depth + 1);
                self.widen_refined_vars(x1, x2, depth + 1);
            }
            (Ty::IO(_, _, x), Ty::IO(_, _, y)) => self.widen_refined_vars(x, y, depth + 1),
            (Ty::Con(n1, a1), Ty::Con(n2, a2)) if n1 == n2 && a1.len() == a2.len() => {
                for (x, y) in a1.clone().iter().zip(a2.clone().iter()) {
                    self.widen_refined_vars(x, y, depth + 1);
                }
            }
            (Ty::Record(f1, _), Ty::Record(f2, _)) => {
                let common: Vec<(Ty, Ty)> = f1
                    .iter()
                    .filter_map(|(k, x)| f2.get(k).map(|y| (x.clone(), y.clone())))
                    .collect();
                for (x, y) in common {
                    self.widen_refined_vars(&x, &y, depth + 1);
                }
            }
            _ => {}
        }
    }

    /// When `refined` is a refined type and `required` is exactly its base,
    /// return that base — the type a variable pinned to `refined` should widen
    /// to. `None` otherwise, including refined-vs-refined: two refinements over
    /// one base must not interchange without a `refine`.
    fn widened_base(&mut self, refined: &Ty, required: &Ty) -> Option<Ty> {
        let Ty::Con(name, args) = refined else {
            return None;
        };
        if !args.is_empty() || !self.refined_types.contains_key(name) {
            return None;
        }
        if matches!(required, Ty::Con(n, a)
            if a.is_empty() && self.refined_types.contains_key(n))
        {
            return None;
        }
        let base = self.refined_base_ty(name)?;
        (base == *required).then_some(base)
    }

    /// The ultimate base type of a refined alias, following chains of
    /// refinements over refinements. `None` on a cycle — `resolve_refined_base`
    /// is the reporting variant; this one stays quiet because it runs
    /// speculatively on every unification.
    fn refined_base_ty(&self, name: &str) -> Option<Ty> {
        let mut visited: Vec<&str> = vec![name];
        let mut current = &self.refined_types.get(name)?.0;
        loop {
            match current {
                Ty::Con(n, args) if args.is_empty() && self.refined_types.contains_key(n) => {
                    if visited.contains(&n.as_str()) {
                        return None;
                    }
                    visited.push(n.as_str());
                    current = &self.refined_types[n].0;
                }
                _ => return Some(current.clone()),
            }
        }
    }

    /// Follow a variable's substitution chain to the type it points at
    /// *without* substituting inside that type, so the variable's identity
    /// survives. Returns the last variable in the chain — the one to rebind —
    /// alongside the resolved type. A deep `apply` would have replaced the
    /// variable with its binding, which is precisely what
    /// `widen_refined_vars` needs to see.
    fn shallow_resolve(&self, ty: &Ty) -> (Option<TyVar>, Ty) {
        let mut last_var = None;
        let mut current = ty.clone();
        loop {
            let Ty::Var(v) = current else {
                return (last_var, current);
            };
            match self.subst.get(&v) {
                Some(next) => {
                    last_var = Some(v);
                    current = next.clone();
                }
                None => return (None, Ty::Var(v)),
            }
        }
    }

    fn unify_inner(&mut self, t1: &Ty, t2: &Ty, span: Span, t1_provided: bool) {
        // Capture root vars before apply shadows them — needed to propagate
        // merged IO effects back into the substitution.
        let var1 = if let Ty::Var(v) = t1 { Some(*v) } else { None };
        let var2 = if let Ty::Var(v) = t2 { Some(*v) } else { None };
        // Bind variables to the *unsubstituted* type (see the `Var` arms
        // below): `apply` is recursive, so a binding that still mentions
        // variables resolves identically — but it keeps those variables
        // reachable, which is what lets `widen_refined_vars` find and re-point
        // them later. Binding the applied copy instead freezes whatever they
        // happened to point at when the binding was made.
        let (raw1, raw2) = (t1, t2);
        let t1 = self.apply(t1);
        let t2 = self.apply(t2);

        match (&t1, &t2) {
            (Ty::Error, _) | (_, Ty::Error) => {}
            // Peel alias wrappers — they're transparent to unification.
            // Exception: a nominal `data` alias (a single-variant record data
            // type also registered as a record alias) keeps its identity and
            // unifies by name as `Con(name)`, NOT by peeling to its structural
            // body. Peeling would erase the name and let two distinct
            // single-variant data types with matching field shapes unify
            // (defeating nominal typing). Pure `type` aliases stay transparent.
            (Ty::Alias(name, inner), _) => {
                if self.data_types.contains_key(name) {
                    let nominal = Ty::Con(name.clone(), vec![]);
                    self.unify_dir(&nominal, &t2, span, t1_provided);
                } else {
                    let inner = (**inner).clone();
                    self.unify_dir(&inner, &t2, span, t1_provided);
                }
            }
            (_, Ty::Alias(name, inner)) => {
                if self.data_types.contains_key(name) {
                    let nominal = Ty::Con(name.clone(), vec![]);
                    self.unify_dir(&t1, &nominal, span, t1_provided);
                } else {
                    let inner = (**inner).clone();
                    self.unify_dir(&t1, &inner, span, t1_provided);
                }
            }
            // Forall types. A Forall on the provided side is instantiated
            // with fresh unification vars (the value is polymorphic, so it
            // can be used at whatever witness the other side demands). A
            // Forall on the required side is skolemised: the requirement
            // must hold for *all* instantiations, so its quantified vars
            // stay rigid and only unify with themselves. Forall-vs-Forall
            // instantiates the provided side against the skolemised
            // required side — standard polytype subsumption.
            (Ty::Forall(vars, body), _) => {
                if t1_provided {
                    let scheme = Scheme {
                        vars: vars.clone(),
                        unit_vars: vec![],
                        constraints: vec![],
                        effect_unions: vec![],
                        unit_binops: vec![],
                        ty: (**body).clone(),
                    };
                    let inst = self.instantiate_at(&scheme, span);
                    self.unify_dir(&inst, &t2, span, t1_provided);
                } else {
                    let (skolemised, fresh_skolems) =
                        self.skolemise_forall_body(vars, body);
                    self.unify_dir(&skolemised, &t2, span, t1_provided);
                    for s in fresh_skolems {
                        self.skolems.remove(&s);
                    }
                }
            }
            (_, Ty::Forall(vars, body)) => {
                if t1_provided {
                    // t2 is the required side — skolemise.
                    let (skolemised, fresh_skolems) =
                        self.skolemise_forall_body(vars, body);
                    self.unify_dir(&t1, &skolemised, span, t1_provided);
                    for s in fresh_skolems {
                        self.skolems.remove(&s);
                    }
                } else {
                    // t2 is the provided side — instantiate.
                    let scheme = Scheme {
                        vars: vars.clone(),
                        unit_vars: vec![],
                        constraints: vec![],
                        effect_unions: vec![],
                        unit_binops: vec![],
                        ty: (**body).clone(),
                    };
                    let inst = self.instantiate_at(&scheme, span);
                    self.unify_dir(&t1, &inst, span, t1_provided);
                }
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
                self.bind_var(v, raw2.clone(), span);
            }
            (_, Ty::Var(v)) => {
                let v = *v;
                self.bind_var(v, raw1.clone(), span);
            }
            (Ty::Int, Ty::Int)
            | (Ty::Float, Ty::Float)
            | (Ty::Text, Ty::Text)
            | (Ty::Bool, Ty::Bool)
            | (Ty::Bytes, Ty::Bytes)
            | (Ty::Uuid, Ty::Uuid) => {}
            (Ty::Fun(p1, r1), Ty::Fun(p2, r2)) => {
                // Parameters are contravariant: the provided/required roles
                // swap when descending into the argument position.
                self.unify_dir(p1, p2, span, !t1_provided);
                self.unify_dir(r1, r2, span, t1_provided);
            }
            (Ty::Relation(a), Ty::Relation(b)) => {
                self.unify_dir(a, b, span, t1_provided);
            }
            (Ty::Con(n1, a1), Ty::Con(n2, a2))
                if n1 == n2 && a1.len() == a2.len() =>
            {
                let a1 = a1.clone();
                let a2 = a2.clone();
                for (a, b) in a1.iter().zip(a2.iter()) {
                    self.unify_dir(a, b, span, t1_provided);
                }
            }
            (Ty::Record(f1, r1), Ty::Record(f2, r2)) => {
                self.unify_records(f1, *r1, f2, *r2, span, t1_provided);
            }
            // ── Higher-kinded type support ─────────────────────
            (Ty::TyCon(a), Ty::TyCon(b)) if a == b => {}
            (Ty::App(f1, a1), Ty::App(f2, a2)) => {
                self.unify_dir(f1, f2, span, t1_provided);
                self.unify_dir(a1, a2, span, t1_provided);
            }
            // App(f, a) vs Relation(b) → f = [], a = b.
            // These arms are split by direction (rather than `|`-merged) so the
            // recursive unifications carry the correct polarity: when the `App`
            // is on the t1 side its parts inherit `t1_provided`; when it is on
            // the t2 side they take `!t1_provided`. Polarity is what
            // distinguishes instantiating vs skolemising a `Forall` reached
            // through the decomposition, so collapsing both directions (or
            // hardcoding `t1_provided = true` via bare `unify`) is unsound for
            // rank-2 types — mirrors the `(App, App)` arm above.
            (Ty::App(f, a), Ty::Relation(b)) => {
                self.unify_dir(f, &Ty::TyCon("[]".into()), span, t1_provided);
                self.unify_dir(a, b, span, t1_provided);
            }
            (Ty::Relation(b), Ty::App(f, a)) => {
                self.unify_dir(f, &Ty::TyCon("[]".into()), span, !t1_provided);
                self.unify_dir(a, b, span, !t1_provided);
            }
            // App(f, Unit(u)) vs dimensionless Int/Float: a unit-carrying
            // application against the collapsed dimensionless numeric. Only
            // matches when `u` is dimensionless (`1`); then `f` is the numeric
            // constructor. Needed because `dress (3.0 : Float 1)` collapses to
            // bare `Ty::Float` while `dress`'s parameter is `f 1`.
            (Ty::App(f, a), Ty::Int) => {
                self.unify_dir(f, &Ty::TyCon("Int".into()), span, t1_provided);
                if let Ty::Unit(u) = self.apply(a) {
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::Int, Ty::App(f, a)) => {
                self.unify_dir(f, &Ty::TyCon("Int".into()), span, !t1_provided);
                if let Ty::Unit(u) = self.apply(a) {
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::App(f, a), Ty::Float) => {
                self.unify_dir(f, &Ty::TyCon("Float".into()), span, t1_provided);
                if let Ty::Unit(u) = self.apply(a) {
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::Float, Ty::App(f, a)) => {
                self.unify_dir(f, &Ty::TyCon("Float".into()), span, !t1_provided);
                if let Ty::Unit(u) = self.apply(a) {
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            // App(f, a) vs IO(effects, row, b) → f = App(IO, EffectRow(effects, row)), a = b
            // Binding f to a partially-applied IO (carrying the effect row)
            // instead of just TyCon("IO") preserves effect/row info through
            // monad-type variables — otherwise `App(m, _)` always normalizes
            // to closed-empty IO, breaking polymorphic-effect code like
            // `forEach : [a] -> (a -> IO {| e} {}) -> IO {| e} {}`.
            (Ty::App(f, a), Ty::IO(effects, row, b)) => {
                let io_app = Ty::App(
                    Box::new(Ty::TyCon("IO".into())),
                    Box::new(Ty::EffectRow(effects.clone(), *row)),
                );
                self.unify_dir(f, &io_app, span, t1_provided);
                self.unify_dir(a, b, span, t1_provided);
            }
            (Ty::IO(effects, row, b), Ty::App(f, a)) => {
                let io_app = Ty::App(
                    Box::new(Ty::TyCon("IO".into())),
                    Box::new(Ty::EffectRow(effects.clone(), *row)),
                );
                self.unify_dir(f, &io_app, span, !t1_provided);
                self.unify_dir(a, b, span, !t1_provided);
            }
            // App(f, a) vs Con(name, args) — decompose the constructor. Split by
            // direction for correct polarity (see the Relation arms above). The
            // mismatch diagnostic uses the original `t1`/`t2`/`t1_provided`, so
            // it is identical in both arms.
            (Ty::App(f, a), Ty::Con(name, args)) => {
                if args.is_empty() {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    let (exp, fnd) = if t1_provided { (d2, d1) } else { (d1, d2) };
                    self.error(
                        format!("type mismatch: expected {}, found {}", exp, fnd),
                        span,
                    );
                } else {
                    let last = args.last().unwrap().clone();
                    let init: Vec<Ty> = args[..args.len() - 1].to_vec();
                    let partial = if init.is_empty() {
                        Ty::TyCon(name.clone())
                    } else {
                        Ty::Con(name.clone(), init)
                    };
                    self.unify_dir(f, &partial, span, t1_provided);
                    self.unify_dir(a, &last, span, t1_provided);
                }
            }
            (Ty::Con(name, args), Ty::App(f, a)) => {
                if args.is_empty() {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    let (exp, fnd) = if t1_provided { (d2, d1) } else { (d1, d2) };
                    self.error(
                        format!("type mismatch: expected {}, found {}", exp, fnd),
                        span,
                    );
                } else {
                    let last = args.last().unwrap().clone();
                    let init: Vec<Ty> = args[..args.len() - 1].to_vec();
                    let partial = if init.is_empty() {
                        Ty::TyCon(name.clone())
                    } else {
                        Ty::Con(name.clone(), init)
                    };
                    self.unify_dir(f, &partial, span, !t1_provided);
                    self.unify_dir(a, &last, span, !t1_provided);
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
                self.unify_dir(&a, &b, span, t1_provided);
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
                    // Widening is for *accumulators*: a fresh result var
                    // (if/case arms, do-block sequencing) absorbs the
                    // union of the branches' effects. But when the
                    // required side is a concrete closed row (e.g. the
                    // `IO {} {}` in a callee's annotated parameter), the
                    // provided side's extra effects are a real violation —
                    // silently merging would let `\u -> println …` check
                    // against `IO {} {}` and launder console IO past both
                    // the effect annotation and the atomic gate. The sound
                    // direction (fewer effects than required) still merges.
                    let (provided, required, required_is_var) = if t1_provided {
                        (&e1, &e2, var2.is_some())
                    } else {
                        (&e2, &e1, var1.is_some())
                    };
                    if !required_is_var {
                        let extras: Vec<String> = provided
                            .difference(required)
                            .map(format_io_effect)
                            .collect();
                        if !extras.is_empty() {
                            self.error(
                                format!(
                                    "IO effects don't match: the provided IO has effects not allowed by the expected type: {{{}}}",
                                    extras.join(", ")
                                ),
                                span,
                            );
                        }
                    }
                    let mut merged = e1.clone();
                    merged.extend(e2.iter().cloned());
                    let unified_inner = self.apply(&a);
                    let merged_io =
                        Ty::IO(merged.clone(), None, Box::new(unified_inner));
                    // Bind at the *end* of each var's substitution chain
                    // (via bind_var, which checks skolems/occurs) so any
                    // aliases along the chain keep seeing the widened IO —
                    // a raw insert at the root var would orphan them.
                    //
                    // The REQUIRED-side var is a fresh result accumulator
                    // (an if/case result var, or a do-block's sequencing
                    // var): it legitimately absorbs the union of the
                    // branches' effects, so overwriting it is correct.
                    //
                    // The PROVIDED-side var is an actual branch *value*.
                    // Widening fires whenever a side was *syntactically* a
                    // `Ty::Var` — but that var may already be bound (through
                    // the substitution) to a concrete closed IO by an
                    // earlier, already-discharged obligation (e.g. `g act`
                    // pinning `act : IO {} {}`, or a lambda parameter fixed
                    // by a prior call). Overwriting that chain-end binding
                    // with the widened effect set would relabel a value whose
                    // type was already fixed, laundering the extra effects
                    // past both the effect annotation and the atomic gate,
                    // since that obligation is never revisited. So resolve
                    // the provided-side var through `self.subst`: if it is
                    // already bound, preserve the binding and only validate
                    // compatibility via `unify_io_effects` (which does not
                    // rebind in the closed/closed case, mirroring
                    // `merge_do_io_row`'s unify-don't-clobber merge — its
                    // effects are ⊆ the union by construction, so merging a
                    // pure branch with an effectful one is still accepted);
                    // only a genuinely unbound provided-side var is bound to
                    // the widened IO.
                    let (provided_var, required_var, provided_effects) =
                        if t1_provided {
                            (var1, var2, &e1)
                        } else {
                            (var2, var1, &e2)
                        };

                    let required_root =
                        required_var.map(|v| self.var_chain_end(v));
                    if let Some(root) = required_root {
                        self.bind_var(root, merged_io.clone(), span);
                    }
                    if let Some(v) = provided_var {
                        let root = self.var_chain_end(v);
                        if Some(root) != required_root {
                            if self.subst.contains_key(&root) {
                                // Already bound to a concrete closed IO:
                                // preserve it, only checking compatibility.
                                self.unify_io_effects(
                                    provided_effects,
                                    None,
                                    &merged,
                                    None,
                                    span,
                                    true,
                                );
                            } else {
                                self.bind_var(root, merged_io, span);
                            }
                        }
                    }
                } else {
                    self.unify_io_effects(&e1, r1, &e2, r2, span, t1_provided);
                }
            }
            (Ty::EffectRow(e1, r1), Ty::EffectRow(e2, r2)) => {
                let e1 = e1.clone();
                let r1 = *r1;
                let e2 = e2.clone();
                let r2 = *r2;
                self.unify_io_effects(&e1, r1, &e2, r2, span, t1_provided);
            }
            // In IO do blocks, allow Relation types to unify with IO or
            // Unit types. Route handlers mix relational operations and
            // their declared response type in if/case branches. The plain
            // relation `[T]` stands for the *whole* result of the IO side,
            // so it unifies with the IO's inner type — not the relation's
            // element type with the inner (which produced nonsense
            // "expected {x: Int 1}, found [{x: Int 1}]" mismatches).
            (Ty::Relation(_), Ty::IO(_, _, b)) if self.in_io_do => {
                let b = (**b).clone();
                self.unify_dir(&t1, &b, span, t1_provided);
            }
            (Ty::IO(_, _, b), Ty::Relation(_)) if self.in_io_do => {
                let b = (**b).clone();
                self.unify_dir(&b, &t2, span, t1_provided);
            }
            (Ty::Relation(_), Ty::Record(fields, None)) | (Ty::Record(fields, None), Ty::Relation(_))
                if self.in_io_do && fields.is_empty() => {}

            // ── Row-polymorphic variants ────────────────────────
            (Ty::Variant(c1, r1), Ty::Variant(c2, r2)) => {
                self.unify_variants(c1, *r1, c2, *r2, span, t1_provided);
            }
            // Skip these arms when `name` is a refined type alias: a refined
            // ADT base (`type Warm = Color where …`) is registered in
            // `refined_types`, not `data_types`, so `con_to_variant` returns
            // None and would spuriously fail. The refined-subsumption arms
            // below handle the reduction (`Warm → Color → Variant`) instead.
            (Ty::Con(name, args), Ty::Variant(c2, r2))
                if !self.refined_types.contains_key(name) =>
            {
                if let Some(expanded) = self.con_to_variant(name, args) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(&ec, er, c2, *r2, span, t1_provided);
                } else {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    let (exp, fnd) =
                        if t1_provided { (d2, d1) } else { (d1, d2) };
                    self.error(
                        format!(
                            "type mismatch: expected {}, found {}",
                            exp, fnd
                        ),
                        span,
                    );
                }
            }
            (Ty::Variant(c1, r1), Ty::Con(name, args))
                if !self.refined_types.contains_key(name) =>
            {
                if let Some(expanded) = self.con_to_variant(name, args) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(c1, *r1, &ec, er, span, t1_provided);
                } else {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    let (exp, fnd) =
                        if t1_provided { (d2, d1) } else { (d1, d2) };
                    self.error(
                        format!(
                            "type mismatch: expected {}, found {}",
                            exp, fnd
                        ),
                        span,
                    );
                }
            }
            // ── Units of measure ──────────────────────────────
            // Unit-bearing Int/Float are now `Con("Int"/"Float", [Unit(u)])`.
            // Same-name same-arity `Con` unifies the args, so
            // `Con("Int",[Unit(u1)])` vs `Con("Int",[Unit(u2)])` recurses into
            // the `Unit vs Unit` arm below.
            (Ty::Unit(u1), Ty::Unit(u2)) => {
                self.unify_units(u1, u2, span);
            }
            // A bare `Ty::Int`/`Ty::Float` is dimensionless (the `Int`/`Float`
            // annotation lowers to unit `1`). It unifies with a unit-bearing
            // `Con("Int"/"Float", [Unit(u)])` when that unit can still be
            // dimensionless — i.e. it carries no concrete base units, only
            // unsolved unit variables, which we then solve to dimensionless.
            // This keeps literals and unit-polymorphic-but-actually-plain
            // computations flowing into `Float` fields, while a concrete unit
            // (`M`) does NOT unify — closing the laundering hole where
            // `x : Float 1; x = (1.5 : Float M)` silently dropped the unit.
            (Ty::Int, Ty::Con(name, args))
                if name == "Int" && matches!(args.first(), Some(Ty::Unit(u)) if self.apply_unit(u).is_compatible_with_dimensionless()) =>
            {
                if let Some(Ty::Unit(u)) = args.first() {
                    let u = u.clone();
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::Con(name, args), Ty::Int)
                if name == "Int" && matches!(args.first(), Some(Ty::Unit(u)) if self.apply_unit(u).is_compatible_with_dimensionless()) =>
            {
                if let Some(Ty::Unit(u)) = args.first() {
                    let u = u.clone();
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::Float, Ty::Con(name, args))
                if name == "Float" && matches!(args.first(), Some(Ty::Unit(u)) if self.apply_unit(u).is_compatible_with_dimensionless()) =>
            {
                if let Some(Ty::Unit(u)) = args.first() {
                    let u = u.clone();
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            (Ty::Con(name, args), Ty::Float)
                if name == "Float" && matches!(args.first(), Some(Ty::Unit(u)) if self.apply_unit(u).is_compatible_with_dimensionless()) =>
            {
                if let Some(Ty::Unit(u)) = args.first() {
                    let u = u.clone();
                    self.unify_units(&u, &UnitTy::dimensionless(), span);
                }
            }
            // Bool is Ty::Bool (not Ty::Con), so handle Bool/Variant
            // unification explicitly to support True {}/False {} patterns.
            (Ty::Bool, Ty::Variant(c2, r2)) => {
                if let Some(expanded) = self.con_to_variant("Bool", &[]) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(&ec, er, c2, *r2, span, t1_provided);
                }
            }
            (Ty::Variant(c1, r1), Ty::Bool) => {
                if let Some(expanded) = self.con_to_variant("Bool", &[]) {
                    let (ec, er) = match expanded {
                        Ty::Variant(c, r) => (c, r),
                        _ => unreachable!(),
                    };
                    self.unify_variants(c1, *r1, &ec, er, span, t1_provided);
                }
            }
            // Refined type subsumption: Con("Nat", []) ↔ Int, etc. Resolve the
            // refined alias to its non-refined base, with cycle detection so
            // `type T = T where ...` or `type A = B / type B = A` diagnoses
            // instead of overflowing the stack.
            //
            // Subsumption is DIRECTIONAL. *Forgetting* a refinement (a `Nat`
            // value flowing where an `Int` is required) is always sound. But
            // *introducing* one — a plain `Int` value flowing where a refined
            // `Nat` is required — must NOT happen implicitly at an unchecked
            // boundary: the predicate would never run, so e.g. `asNat : Int 1 ->
            // Nat; asNat = \x -> x` would launder a negative into a `Nat`. The
            // sound introduction form is `refine`, which performs the runtime
            // check and yields `Result RefinementError Nat`. We therefore
            // reject the introducing direction (mirroring the IO-effect
            // directional check below) — EXCEPT when `suppress_refine_intro`
            // is set, i.e. while unifying a `set`/`replace` value against its
            // source type, where the runtime validates every written row.
            (Ty::Con(name, args), other)
                if args.is_empty()
                    && self.refined_types.contains_key(name)
                    // …but NOT when `other` is a *different* refined type:
                    // reducing both to their shared base would let e.g. `Nat`
                    // and `Pos` (both `Int where …`) interchange with no
                    // predicate re-check, defeating nominal refinement. Let that
                    // fall through to the mismatch arm so the user must `refine`.
                    // (Same-name refined `Con`s are handled by the `Con`/`Con`
                    // arm above.)
                    && !matches!(other, Ty::Con(n2, a2)
                        if a2.is_empty()
                            && n2 != name
                            && self.refined_types.contains_key(n2)) =>
            {
                // None => cycle already reported
                if let Some(base_ty) = self.resolve_refined_base(name, span) {
                    let other = other.clone();
                    // The refined type is `t1`; introducing = it is the
                    // *required* side (`!t1_provided`) and a concrete base
                    // value is supplied.
                    if !self.refine_intro_suppressed(name)
                        && !t1_provided
                        && self.is_concrete_refinement_base(&other)
                    {
                        self.error(
                            format!(
                                "cannot implicitly use `{}` where refined type `{}` is required; use `refine` to check the predicate",
                                self.display_ty(&other),
                                name
                            ),
                            span,
                        );
                    } else {
                        self.unify_dir(&base_ty, &other, span, t1_provided);
                    }
                }
            }
            (other, Ty::Con(name, args))
                if args.is_empty()
                    && self.refined_types.contains_key(name)
                    && !matches!(other, Ty::Con(n2, a2)
                        if a2.is_empty()
                            && n2 != name
                            && self.refined_types.contains_key(n2)) =>
            {
                // None => cycle already reported
                if let Some(base_ty) = self.resolve_refined_base(name, span) {
                    let other = other.clone();
                    // The refined type is `t2`; introducing = it is the
                    // *required* side (`t1_provided`) and a concrete base
                    // value (`other`, the provided side) flows into it.
                    if !self.refine_intro_suppressed(name)
                        && t1_provided
                        && self.is_concrete_refinement_base(&other)
                    {
                        self.error(
                            format!(
                                "cannot implicitly use `{}` where refined type `{}` is required; use `refine` to check the predicate",
                                self.display_ty(&other),
                                name
                            ),
                            span,
                        );
                    } else {
                        self.unify_dir(&other, &base_ty, span, t1_provided);
                    }
                }
            }
            // Single-variant record data subsumption: a single-variant,
            // parameterless data type is registered both nominally (constructor
            // application yields `Con(name)`) and as a record alias (a `: name`
            // annotation or field type resolves to the record). Bridge the two
            // so `Box {val: 5} : Box` unifies. The same-name `Con`/`Con` case
            // above already short-circuits identical names; refined types are
            // handled above and excluded here.
            (Ty::Con(name, args), other)
                if args.is_empty()
                    && !self.refined_types.contains_key(name)
                    && self.aliases.contains_key(name)
                    // Only bridge against a structural type (record/var/etc.),
                    // never against another nominal aliased `Con`: reducing
                    // both sides to their record shapes would let two distinct
                    // single-variant data types (e.g. `UserId`/`Email` with
                    // matching fields) unify, defeating nominal typing. The
                    // same-name `Con`/`Con` arm above already handles identical
                    // names, so a `Con` here is necessarily a different type.
                    && !matches!(other, Ty::Con(n2, a2)
                        if a2.is_empty() && self.aliases.contains_key(n2)) =>
            {
                let aliased = self.aliases[name].clone();
                let other = other.clone();
                self.unify_dir(&aliased, &other, span, t1_provided);
            }
            (other, Ty::Con(name, args))
                if args.is_empty()
                    && !self.refined_types.contains_key(name)
                    && self.aliases.contains_key(name)
                    && !matches!(other, Ty::Con(n2, a2)
                        if a2.is_empty() && self.aliases.contains_key(n2)) =>
            {
                let aliased = self.aliases[name].clone();
                let other = other.clone();
                self.unify_dir(&other, &aliased, span, t1_provided);
            }
            // Two irreducible associated-type projections (both `apply`'d
            // above, so neither reduced): they're equal iff they name the same
            // associated type applied to unifiable arguments. A projection that
            // failed to reduce is otherwise rigid and will not unify with a
            // concrete type, which is what keeps `Elem c` from being silently
            // equated with an arbitrary type.
            (Ty::Assoc(n1, a1), Ty::Assoc(n2, a2)) if n1 == n2 => {
                let a1 = (**a1).clone();
                let a2 = (**a2).clone();
                self.unify_dir(&a1, &a2, span, t1_provided);
            }
            _ => {
                let d1 = self.display_ty(&t1);
                let d2 = self.display_ty(&t2);
                // `t1` is the provided/actual side when `t1_provided` (see
                // `unify`), so the expected type is `t2` then — and vice
                // versa after a contravariant flip or a check-mode call.
                let (exp, fnd) =
                    if t1_provided { (d2, d1) } else { (d1, d2) };
                self.error(
                    format!("type mismatch: expected {}, found {}", exp, fnd),
                    span,
                );
            }
        }
    }

    /// Fold every field a record's row tail contributes into its field map,
    /// returning the merged fields plus the tail that is still unresolved.
    /// A field carried both explicitly and by the tail is one field: unify
    /// the two payloads rather than dropping either, which would leave them
    /// unlinked.
    fn flatten_record_row(
        &mut self,
        fields: &BTreeMap<String, Ty>,
        row: Option<TyVar>,
        span: Span,
    ) -> (BTreeMap<String, Ty>, Option<TyVar>) {
        let mut all = fields.clone();
        let mut tail = row;
        while let Some(rv) = tail {
            match self.apply(&Ty::Var(rv)) {
                Ty::Record(extra, rest) => {
                    for (k, v) in extra {
                        match all.get(&k) {
                            Some(existing) => {
                                let existing = existing.clone();
                                self.unify(&existing, &v, span);
                            }
                            None => {
                                all.insert(k, v);
                            }
                        }
                    }
                    tail = rest;
                }
                // An unbound tail: nothing more to fold in.
                Ty::Var(rv2) => return (all, Some(rv2)),
                _ => return (all, None),
            }
        }
        (all, None)
    }

    fn unify_records(
        &mut self,
        f1: &BTreeMap<String, Ty>,
        r1: Option<TyVar>,
        f2: &BTreeMap<String, Ty>,
        r2: Option<TyVar>,
        span: Span,
        t1_provided: bool,
    ) {
        // Unify common fields (BTreeMap lookup is O(log n), no HashSet needed)
        for (key, ty1) in f1 {
            if let Some(ty2) = f2.get(key) {
                self.unify_dir(ty1, ty2, span, t1_provided);
            }
        }

        // Flatten each side's row tail into its field map. This happens
        // after common-field unification, which may have bound a tail if a
        // field type shares the row variable — without re-resolving here,
        // bind_var below would overwrite the field-loop's binding.
        //
        // A field a side carries in its tail is present just as much as an
        // explicit one, so flattening before splitting keeps it out of the
        // `only` sets. Comparing the explicit maps alone reported a field as
        // extra on one side while the other side held it in its tail.
        let (all1, r1) = self.flatten_record_row(f1, r1, span);
        let (all2, r2) = self.flatten_record_row(f2, r2, span);

        // Unify every field both sides carry, however each one carries it.
        // Fields explicit on both were already unified above.
        let shared: Vec<(Ty, Ty)> = all1
            .iter()
            .filter(|(k, _)| !(f1.contains_key(*k) && f2.contains_key(*k)))
            .filter_map(|(k, v1)| all2.get(k).map(|v2| (v1.clone(), v2.clone())))
            .collect();
        for (v1, v2) in shared {
            self.unify_dir(&v1, &v2, span, t1_provided);
        }

        let only1: BTreeMap<String, Ty> = all1
            .iter()
            .filter(|(k, _)| !all2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let only2: BTreeMap<String, Ty> = all2
            .iter()
            .filter(|(k, _)| !all1.contains_key(*k))
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
        t1_provided: bool,
    ) {
        // Unify common constructors' field types (BTreeMap lookup is O(log n))
        for (key, ty1) in c1 {
            if let Some(ty2) = c2.get(key) {
                self.unify_dir(ty1, ty2, span, t1_provided);
            }
        }

        let mut only1: BTreeMap<String, Ty> = c1
            .iter()
            .filter(|(k, _)| !c2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut only2: BTreeMap<String, Ty> = c2
            .iter()
            .filter(|(k, _)| !c1.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Re-resolve row tails after common-constructor unification, which
        // may have bound them if a field type shares the row variable.
        let r1 = match r1 {
            Some(rv) => match self.apply(&Ty::Var(rv)) {
                Ty::Variant(extra, rest) => {
                    for (k, v) in extra {
                        // A tail constructor shared with an explicit
                        // constructor on the other side is common: unify their
                        // payloads rather than dropping it.
                        if let Some(v2) = c2.get(&k) {
                            self.unify_dir(&v, v2, span, t1_provided);
                        } else {
                            only1.entry(k).or_insert(v);
                        }
                    }
                    rest
                }
                Ty::Var(rv2) => Some(rv2),
                _ => None,
            },
            None => None,
        };
        let r2 = match r2 {
            Some(rv) => match self.apply(&Ty::Var(rv)) {
                Ty::Variant(extra, rest) => {
                    for (k, v) in extra {
                        if let Some(v1) = c1.get(&k) {
                            self.unify_dir(v1, &v, span, t1_provided);
                        } else {
                            only2.entry(k).or_insert(v);
                        }
                    }
                    rest
                }
                Ty::Var(rv2) => Some(rv2),
                _ => None,
            },
            None => None,
        };

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
    /// When both rows are closed, subsumption is directional: the
    /// provided/actual side's effects must be a subset of the
    /// required/expected side's (`t1_provided` says which is which) —
    /// an `IO {}` value where `IO {console}` is required is fine, but an
    /// effectful value cannot check against a smaller closed row.
    fn unify_io_effects(
        &mut self,
        e1: &BTreeSet<IoEffect>,
        r1: Option<TyVar>,
        e2: &BTreeSet<IoEffect>,
        r2: Option<TyVar>,
        span: Span,
        t1_provided: bool,
    ) {
        let (e1, r1) = self.resolve_effect_row(e1.clone(), r1);
        let (e2, r2) = self.resolve_effect_row(e2.clone(), r2);

        let only1: BTreeSet<IoEffect> = e1.difference(&e2).cloned().collect();
        let only2: BTreeSet<IoEffect> = e2.difference(&e1).cloned().collect();

        match (r1, r2) {
            (None, None) => {
                let provided_extras = if t1_provided { &only1 } else { &only2 };
                if !provided_extras.is_empty() {
                    let extras: Vec<String> = provided_extras
                        .iter()
                        .map(format_io_effect)
                        .collect();
                    self.error(
                        format!(
                            "IO effects don't match: the provided IO has effects not allowed by the expected type: {{{}}}",
                            extras.join(", ")
                        ),
                        span,
                    );
                }
            }
            (Some(rv), None) => {
                // The open side is `t1`. Reject only when that open side is the
                // *provided* side and carries fixed effects the closed required
                // side lacks — same directional rule as the `(None, None)` and
                // `(Some, Some)` arms. When the open side is the *required* one,
                // its row var absorbs the provided side's effects, and its own
                // extra fixed effects (`only1`) are a legal over-declaration.
                if t1_provided && !only1.is_empty() {
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
                // Closed required row is side 2 (effects `e2`): if `rv` is an
                // effect-union result, its row must stay within `e2`.
                self.record_effect_union_upper_bound(rv, &e2, span);
                self.bind_var(rv, Ty::EffectRow(only2, None), span);
            }
            (None, Some(rv)) => {
                // Mirror of the `(Some, None)` arm: here the open side is `t2`,
                // which is the provided side when `!t1_provided`. Reject only
                // then, and only for its extra fixed effects (`only2`).
                if !t1_provided && !only2.is_empty() {
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
                // Closed required row is side 1 (effects `e1`): if `rv` is an
                // effect-union result, its row must stay within `e1`.
                self.record_effect_union_upper_bound(rv, &e1, span);
                self.bind_var(rv, Ty::EffectRow(only1, None), span);
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    // Same row tail on both sides: only the *fixed* effects
                    // can differ, and they subsume directionally just like
                    // the closed/closed `(None, None)` case. The provided
                    // side may carry FEWER fixed effects than the required
                    // side (over-declaring a result row, e.g.
                    // `f : IO {| e} {} -> IO {console | e} {}; f = \act -> act`),
                    // which is sound — reject only when the *provided* side
                    // has fixed effects the required side lacks.
                    let provided_extras = if t1_provided { &only1 } else { &only2 };
                    if !provided_extras.is_empty() {
                        let extras: Vec<String> = provided_extras
                            .iter()
                            .map(format_io_effect)
                            .collect();
                        self.error(
                            format!(
                                "IO effects don't match: the provided IO has effects not allowed by the expected type: {{{}}}",
                                extras.join(", ")
                            ),
                            span,
                        );
                    }
                } else if only1.is_empty() && only2.is_empty() {
                    // Two *rigid* rows can never unify — but if the user's
                    // annotation declared their union (`IO {| r1 \/ r2}`),
                    // a pending effect-union constraint mentions them and
                    // the clash is sanctioned: sequencing `IO {| r1}` with
                    // `IO {| r2}` (desugared `__bind` forces both through
                    // one monad row) yields a row covered by `r1 \/ r2`,
                    // and a body row matching one source is subsumed by the
                    // declared union result. Accept without binding.
                    if self.skolems.contains(&rv1)
                        && self.skolems.contains(&rv2)
                        && self.effect_union_sanctions(rv1, rv2)
                    {
                        return;
                    }
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

    /// Whether a pending `\/` effect-union constraint sanctions treating
    /// the two rigid rows `a` and `b` as compatible: either both are
    /// sources of the same union (sequencing them produces the union),
    /// or one is the union's declared result and the other one of its
    /// sources (a single source is subsumed by the union).
    fn effect_union_sanctions(&self, a: TyVar, b: TyVar) -> bool {
        let root = |v: TyVar| -> TyVar {
            match self.apply(&Ty::Var(v)) {
                Ty::Var(x) => x,
                _ => v,
            }
        };
        let a = root(a);
        let b = root(b);
        self.pending_effect_unions.iter().any(|u| {
            let result = root(u.result);
            let sources: Vec<TyVar> =
                u.sources.iter().map(|&s| root(s)).collect();
            (sources.contains(&a) && sources.contains(&b))
                || (result == a && sources.contains(&b))
                || (result == b && sources.contains(&a))
        })
    }

    /// Like `effect_union_sanctions`, but only a `\/` the user actually
    /// wrote counts. `merge_do_io_row` builds its own union constraints
    /// while checking a body; consulting those would be circular — the
    /// first merge would license the second, and a signature declaring a
    /// single row (`IO {| r1}`) could absorb a second rigid row for free.
    fn declared_union_sanctions(&self, a: TyVar, b: TyVar) -> bool {
        let root = |v: TyVar| -> TyVar {
            match self.apply(&Ty::Var(v)) {
                Ty::Var(x) => x,
                _ => v,
            }
        };
        let a = root(a);
        let b = root(b);
        self.pending_effect_unions
            .iter()
            .filter(|u| u.declared)
            .any(|u| {
                let result = root(u.result);
                let sources: Vec<TyVar> =
                    u.sources.iter().map(|&s| root(s)).collect();
                (sources.contains(&a) && sources.contains(&b))
                    || (result == a && sources.contains(&b))
                    || (result == b && sources.contains(&a))
            })
    }

    /// Whether the rigid row `r` may join the sources of the pending union at
    /// `idx` — it may only when a declared `\/` puts `r` in a union with every
    /// source already there (`r1 \/ r2 \/ r3` admits `r3` into a `r1`+`r2`
    /// merge; a signature declaring only `r1 \/ r2` does not).
    fn union_admits_source(&self, idx: usize, r: TyVar) -> bool {
        self.pending_effect_unions[idx]
            .sources
            .iter()
            .all(|&s| s == r || self.declared_union_sanctions(s, r))
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

    fn instantiate_at(&mut self, scheme: &Scheme, span: Span) -> Ty {
        if scheme.vars.is_empty()
            && scheme.unit_vars.is_empty()
            && scheme.unit_binops.is_empty()
            && scheme.constraints.is_empty()
            && scheme.effect_unions.is_empty()
        {
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
            let seq = self.next_constraint_seq();
            self.deferred_constraints.push(DeferredConstraint {
                trait_name: c.trait_name.clone(),
                type_var: target_var,
                span,
                seq,
            });
        }
        // Freshen effect-union constraints alongside the type — each
        // instantiation gets its own copy so a polymorphic `\/`-typed
        // function can be called multiple times with distinct rows.
        for u in &scheme.effect_unions {
            let fresh_var = |v: TyVar| -> TyVar {
                match mapping.get(&v) {
                    Some(Ty::Var(nv)) => *nv,
                    _ => v,
                }
            };
            let result = fresh_var(u.result);
            let sources = u.sources.iter().copied().map(fresh_var).collect();
            self.pending_effect_unions.push(EffectUnion {
                result,
                sources,
                declared: u.declared,
            });
        }
        // Freshen unit variables so each instantiation gets independent units.
        let unit_mapping: HashMap<UnitVar, UnitVar> = scheme
            .unit_vars
            .iter()
            .map(|v| (*v, self.fresh_unit_var()))
            .collect();
        // Freshen captured `*`/`/` unit-composition checks alongside the type
        // and unit variables, then re-arm them for end-of-inference resolution
        // — each instantiation resolves its own composition (so `square 3.0 M`
        // yields `M^2` independently of `square 4.0 S` → `S^2`).
        for b in &scheme.unit_binops {
            let result = match mapping.get(&b.result) {
                Some(Ty::Var(nv)) => *nv,
                _ => b.result,
            };
            let mut lhs = self.subst_ty(&b.lhs, &mapping);
            let mut rhs = self.subst_ty(&b.rhs, &mapping);
            if !unit_mapping.is_empty() {
                lhs = self.subst_unit_vars_in_ty(&lhs, &unit_mapping);
                rhs = self.subst_unit_vars_in_ty(&rhs, &unit_mapping);
            }
            self.deferred_unit_binops.push(DeferredUnitBinop {
                op: b.op,
                lhs,
                rhs,
                result,
                span: b.span,
            });
        }
        let ty = self.subst_ty(&scheme.ty, &mapping);
        if unit_mapping.is_empty() {
            ty
        } else {
            self.subst_unit_vars_in_ty(&ty, &unit_mapping)
        }
    }

    /// Skolemise a scheme's quantified type variables: each `vars` entry
    /// becomes a fresh rigid TyVar registered in `self.skolems`. Used when
    /// checking a function body against its explicit type annotation, so the
    /// body cannot silently narrow the signature's polymorphism by binding
    /// the quantified variables to concrete types. The returned skolems must
    /// be removed from `self.skolems` once the body check completes.
    /// Unit vars are freshened as in `instantiate_at` (no unit-skolem
    /// mechanism exists yet); deferred constraints follow the new skolems.
    fn skolemise_scheme(
        &mut self,
        scheme: &Scheme,
        span: Span,
    ) -> (Ty, Vec<TyVar>, Vec<UnitVar>) {
        if scheme.vars.is_empty() && scheme.unit_vars.is_empty() {
            return (scheme.ty.clone(), Vec::new(), Vec::new());
        }
        let mut fresh_skolems: Vec<TyVar> = Vec::with_capacity(scheme.vars.len());
        let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
        for v in &scheme.vars {
            let s = self.fresh_var();
            // Vars freshened out of a type-alias body are quantified so each
            // alias reference gets its own copy, but the annotation never
            // promised the body works for *every* instantiation of them —
            // `b1 : Box` with `type Box = {val: a}` lets the body pick `val`'s
            // type. Keep those flexible; skolemising them would reject
            // `b1 = {val: 1}` as a rigid-variable escape.
            if self.alias_free_vars.contains(v) {
                self.alias_free_vars.insert(s);
            } else {
                self.skolems.insert(s);
                fresh_skolems.push(s);
            }
            mapping.insert(*v, Ty::Var(s));
        }
        for c in &scheme.constraints {
            let target_var = match mapping.get(&c.type_var) {
                Some(Ty::Var(new_var)) => *new_var,
                _ => c.type_var,
            };
            let seq = self.next_constraint_seq();
            self.deferred_constraints.push(DeferredConstraint {
                trait_name: c.trait_name.clone(),
                type_var: target_var,
                span,
                seq,
            });
        }
        // Freshen effect-union constraints to track row-union semantics
        // through the function body's type check.
        for u in &scheme.effect_unions {
            let fresh_var = |v: TyVar| -> TyVar {
                match mapping.get(&v) {
                    Some(Ty::Var(nv)) => *nv,
                    _ => v,
                }
            };
            let result = fresh_var(u.result);
            let sources = u.sources.iter().copied().map(fresh_var).collect();
            self.pending_effect_unions.push(EffectUnion {
                result,
                sources,
                declared: u.declared,
            });
        }
        // Freshen unit variables to fresh *skolems* (rigid): the body must hold
        // for every unit, so it may not narrow `∀u` to a concrete unit. Marking
        // them in `unit_skolems` makes `unify_units` refuse to solve them.
        let mut fresh_unit_skolems: Vec<UnitVar> = Vec::with_capacity(scheme.unit_vars.len());
        let unit_mapping: HashMap<UnitVar, UnitVar> = scheme
            .unit_vars
            .iter()
            .map(|v| {
                let s = self.fresh_unit_var();
                self.unit_skolems.insert(s);
                fresh_unit_skolems.push(s);
                (*v, s)
            })
            .collect();
        // Re-arm captured `*`/`/` unit-composition checks alongside the
        // skolems and fresh units, mirroring `instantiate_at`. Without this,
        // a unit-polymorphic annotation like `square : Float u -> Float u2`
        // (carrying a deferred `u2 = u * u` obligation) would have that
        // obligation silently dropped while checking the body, so the body
        // could violate the declared unit relationship undetected.
        for b in &scheme.unit_binops {
            let result = match mapping.get(&b.result) {
                Some(Ty::Var(nv)) => *nv,
                _ => b.result,
            };
            let mut lhs = self.subst_ty(&b.lhs, &mapping);
            let mut rhs = self.subst_ty(&b.rhs, &mapping);
            if !unit_mapping.is_empty() {
                lhs = self.subst_unit_vars_in_ty(&lhs, &unit_mapping);
                rhs = self.subst_unit_vars_in_ty(&rhs, &unit_mapping);
            }
            self.deferred_unit_binops.push(DeferredUnitBinop {
                op: b.op,
                lhs,
                rhs,
                result,
                span: b.span,
            });
        }
        let ty = self.subst_ty(&scheme.ty, &mapping);
        let ty = if unit_mapping.is_empty() {
            ty
        } else {
            self.subst_unit_vars_in_ty(&ty, &unit_mapping)
        };
        (ty, fresh_skolems, fresh_unit_skolems)
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
            // Aliases must be substituted through: `collect_free_vars`
            // descends into the alias body, so quantified vars can live
            // there (e.g. `type Box = {val: a}`). Skipping the body would
            // share the original var across every instantiation — pinning
            // it at the first use site and falsely rejecting later uses
            // at other types.
            Ty::Alias(name, inner) => Ty::Alias(
                name.clone(),
                Box::new(self.subst_ty(inner, mapping)),
            ),
            Ty::Assoc(name, inner) => {
                let inner = self.subst_ty(inner, mapping);
                Ty::Assoc(name.clone(), Box::new(inner))
            }
            _ => ty.clone(),
        }
    }

    /// Replace unit variables in a type according to a freshening mapping.
    fn subst_unit_vars_in_ty(&self, ty: &Ty, mapping: &HashMap<UnitVar, UnitVar>) -> Ty {
        match ty {
            Ty::Unit(u) => Ty::Unit(Self::subst_unit_var(u, mapping)),
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
            // Mirror `subst_ty`: unit vars can occur inside alias bodies.
            Ty::Alias(name, inner) => Ty::Alias(
                name.clone(),
                Box::new(self.subst_unit_vars_in_ty(inner, mapping)),
            ),
            Ty::Assoc(name, inner) => Ty::Assoc(
                name.clone(),
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
            // Follow the substitution: an env entry may be a bare type
            // variable (e.g. a lambda parameter bound as Scheme::mono(Var α))
            // that was later substituted to a unit-bearing type — its unit
            // vars are env-bound and must NOT be generalized.
            Ty::Var(v) => {
                if let Some(resolved) = self.subst.get(v) {
                    self.collect_free_unit_vars(resolved, out);
                }
            }
            Ty::Unit(u) => {
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
            Ty::Record(fields, row) => {
                for v in fields.values() {
                    self.collect_free_unit_vars(v, out);
                }
                if let Some(rv) = row
                    && let Some(resolved) = self.subst.get(rv) {
                        self.collect_free_unit_vars(resolved, out);
                    }
            }
            Ty::Variant(ctors, row) => {
                for v in ctors.values() {
                    self.collect_free_unit_vars(v, out);
                }
                if let Some(rv) = row
                    && let Some(resolved) = self.subst.get(rv) {
                        self.collect_free_unit_vars(resolved, out);
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
            Ty::IO(_, row, inner) => {
                if let Some(rv) = row
                    && let Some(resolved) = self.subst.get(rv) {
                        self.collect_free_unit_vars(resolved, out);
                    }
                self.collect_free_unit_vars(inner, out);
            }
            Ty::EffectRow(_, row) => {
                if let Some(rv) = row
                    && let Some(resolved) = self.subst.get(rv) {
                        self.collect_free_unit_vars(resolved, out);
                    }
            }
            Ty::Forall(_, inner) => self.collect_free_unit_vars(inner, out),
            Ty::Alias(_, inner) => self.collect_free_unit_vars(inner, out),
            Ty::Assoc(_, inner) => self.collect_free_unit_vars(inner, out),
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
        // B7: Track monad vars that are being let-generalized (quantified
        // into a local let-binding's scheme). At Phase 5, if such a var is
        // still unresolved it defaults to Relation dispatch — which is likely
        // wrong for a monad-polymorphic function. We skip top-level function
        // generalization (flagged by `in_top_level_generalize`) to avoid
        // false positives on `main = do …` where the Relation default is
        // correct.
        if !self.in_top_level_generalize {
            for (span, m_var) in &self.monad_vars {
                if gen_set.contains(m_var) {
                    self.generalized_monad_spans.insert(*span);
                }
            }
        }
        // Deferred trait constraints (pushed by `require_trait` when the
        // body used e.g. `<` or a trait method on a still-polymorphic type)
        // whose variables are being quantified here must travel with the
        // scheme — `check_constraints` skips unresolved vars on the
        // assumption that the obligation is checked at the use site, which
        // only happens if instantiation re-registers it. Constraints on
        // vars NOT quantified here stay in the deferred list. Removing the
        // generalized entries reorders/shrinks the list, but
        // `check_skolem_constraints` keys off each constraint's stable `seq`
        // (not a positional index), so its bookkeeping survives this take.
        let mut all_constraints = all_constraints;
        let mut captured: HashSet<(String, TyVar)> = all_constraints
            .iter()
            .map(|c| (c.trait_name.clone(), c.type_var))
            .collect();
        let deferred = std::mem::take(&mut self.deferred_constraints);
        let mut remaining = Vec::with_capacity(deferred.len());
        for dc in deferred {
            match self.apply(&Ty::Var(dc.type_var)) {
                Ty::Var(v) if gen_set.contains(&v) => {
                    if captured.insert((dc.trait_name.clone(), v)) {
                        all_constraints.push(TyConstraint {
                            trait_name: dc.trait_name,
                            type_var: v,
                            span: dc.span,
                        });
                    }
                }
                _ => remaining.push(dc),
            }
        }
        self.deferred_constraints = remaining;
        // Only keep constraints on generalized variables; immediately
        // validate constraints that resolved to concrete types.
        let mut kept = Vec::new();
        for c in all_constraints {
            let resolved = self.apply(&Ty::Var(c.type_var));
            match resolved {
                // Normalize to the representative var `v` (the one actually
                // quantified in the scheme). If `c.type_var` was aliased to `v`
                // during body inference, keeping the stale `c.type_var` would
                // make `instantiate_at` fail to freshen this constraint (its
                // `mapping` is keyed on the scheme's `vars`), silently dropping
                // the trait obligation at the use site.
                Ty::Var(v) if gen_set.contains(&v) => {
                    kept.push(TyConstraint { type_var: v, ..c })
                }
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
        // Drain effect-union constraints whose result var is generalized
        // here. Anything resolved to a concrete row is resolved now; the
        // rest is captured by the scheme so each instantiation gets its
        // own freshened copy.
        let pending = std::mem::take(&mut self.pending_effect_unions);
        let mut effect_unions = Vec::new();
        for u in pending {
            let result_resolved = self.apply(&Ty::Var(u.result));
            match result_resolved {
                Ty::Var(v) if gen_set.contains(&v) => {
                    effect_unions.push(EffectUnion {
                        result: v,
                        sources: u.sources,
                        declared: u.declared,
                    });
                }
                Ty::Var(_) => {
                    // Result var is env-bound or already moved by another
                    // path; keep it pending — generalization in an outer
                    // scope will pick it up, or end-of-inference resolves it.
                    self.pending_effect_unions.push(EffectUnion {
                        result: u.result,
                        sources: u.sources,
                        declared: u.declared,
                    });
                }
                _ => {
                    // Result already resolved to a concrete row — resolve
                    // the union and unify against it now.
                    self.resolve_effect_union(&u);
                }
            }
        }
        // Drain deferred `*`/`/` unit-composition checks whose result var is
        // generalized here, capturing them on the scheme (freshened per
        // instantiation) just like effect-unions above. This is what lets a
        // function like `\x -> x * x` be unit-polymorphic: each call site gets
        // its own composition (`square 3.0 M` → `M^2`, `square 4.0 S` →
        // `S^2`) instead of all uses being pinned to one monomorphic unit.
        // Binops not generalized here stay pending for the end-of-inference
        // global resolution.
        let pending_binops = std::mem::take(&mut self.deferred_unit_binops);
        let mut unit_binops = Vec::new();
        for b in pending_binops {
            match self.apply(&Ty::Var(b.result)) {
                Ty::Var(v) if gen_set.contains(&v) => {
                    unit_binops.push(DeferredUnitBinop {
                        op: b.op,
                        lhs: self.apply(&b.lhs),
                        rhs: self.apply(&b.rhs),
                        result: v,
                        span: b.span,
                    });
                }
                _ => self.deferred_unit_binops.push(b),
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
            effect_unions,
            unit_binops,
            ty: applied,
        }
    }

    /// Bind a union constraint's result row var to the union of its sources'
    /// resolved effects. Sources whose tails are still open contribute their
    /// leftover row var to the result so future growth still flows through.
    /// Record that the effect-union result row `rv` was unified against a
    /// *closed* required row whose effects are `bound`. Only stores a bound for
    /// vars that are actually a pending union's result (others close normally).
    /// Intersects with any prior bound so the most restrictive requirement wins.
    /// See `effect_union_upper_bounds`.
    fn record_effect_union_upper_bound(
        &mut self,
        rv: TyVar,
        bound: &BTreeSet<IoEffect>,
        span: Span,
    ) {
        let end = self.var_chain_end(rv);
        let is_union_result = self
            .pending_effect_unions
            .iter()
            .any(|u| self.var_chain_end(u.result) == end);
        if !is_union_result {
            return;
        }
        // Store the bound at `end` (the canonical chain representative used by
        // `resolve_effect_union`'s lookup) AND at every var along the chain
        // from `rv` to `end`. The next step after a record is typically
        // `bind_var(rv, EffectRow(...))`, which cuts the chain at `rv`:
        // afterwards `var_chain_end(u.result)` becomes `rv` for any union
        // result whose chain reached `end` through `rv`. Storing only at `end`
        // would orphan the bound in that case, laundering stricter effects
        // through the union. Storing at every link makes the bound survive
        // any later cut.
        let mut chain_vars = Vec::new();
        let mut cur = rv;
        chain_vars.push(cur);
        let mut steps = 0usize;
        while let Some(Ty::Var(next)) = self.subst.get(&cur) {
            if *next == cur || *next == end || steps > 10_000 {
                break;
            }
            cur = *next;
            chain_vars.push(cur);
            steps += 1;
        }
        for key in chain_vars.into_iter().chain(std::iter::once(end)) {
            self.effect_union_upper_bounds
                .entry(key)
                .and_modify(|(existing, sp)| {
                    *existing = existing.intersection(bound).cloned().collect();
                    *sp = span;
                })
                .or_insert_with(|| (bound.clone(), span));
        }
    }

    fn resolve_effect_union(&mut self, u: &EffectUnion) {
        let mut effects: BTreeSet<IoEffect> = BTreeSet::new();
        let mut leftover: Option<TyVar> = None;
        let span = Span::new(0, 0);
        for s in &u.sources {
            let (e, tail) = self.resolve_effect_row(BTreeSet::new(), Some(*s));
            effects.extend(e);
            let Some(t) = tail else { continue };
            match leftover {
                None => leftover = Some(t),
                Some(kept) if kept == t => {}
                Some(kept) => {
                    // `EffectRow` has a single tail slot, so a union of
                    // several still-open sources can't keep each tail
                    // separately. Chain the extra tail into the kept one
                    // (unify them) so effects flowing into ANY source later
                    // still propagate to the union result. This may share
                    // effects between the sources' rows — a sound
                    // over-approximation given the representation.
                    let t_rigid = self.skolems.contains(&t);
                    let k_rigid = self.skolems.contains(&kept);
                    if t_rigid && k_rigid {
                        // Both tails are rigid signature vars (e.g. `r1`/`r2`
                        // in a user-annotated `\/` type): they can't be
                        // unified. Keep the first; the scheme-captured union
                        // constraint is re-registered with freshened
                        // (flexible) vars at every instantiation, so callers
                        // still see the full union.
                        continue;
                    }
                    self.unify(&Ty::Var(t), &Ty::Var(kept), span);
                }
            }
        }
        // If this union's result row was unified against a *closed* required
        // row during body checking (e.g. a `race`/`fork` result passed to an
        // `IO {}` parameter), enforce that the now-known union of effects stays
        // within that bound. Without this, the `bind_var` below would silently
        // overwrite that closed binding and launder the sources' effects
        // through a value typed with fewer effects. A bound recorded against a
        // *larger* closed row (e.g. the do-block monad row that main's own
        // `IO {console}` annotation governs) accommodates the union and passes.
        let end = self.var_chain_end(u.result);
        if let Some((bound, bound_span)) = self.effect_union_upper_bounds.get(&end).cloned() {
            let excess: Vec<String> =
                effects.difference(&bound).map(format_io_effect).collect();
            if !excess.is_empty() {
                self.error(
                    format!(
                        "IO effects don't match: the provided IO has effects not allowed by the expected type: {{{}}}",
                        excess.join(", ")
                    ),
                    bound_span,
                );
            }
        }
        // Use bind_var so the binding goes through occurs check + unification
        // — handles the case where `result` has already been narrowed. Bind the
        // chain representative `end`, not `u.result`: if `u.result` was aliased
        // to another row var during body inference, binding the interior var
        // would orphan every alias past it (`end` and the do-block's IO row
        // stay unbound), laundering/dropping the union's effects.
        self.bind_var(end, Ty::EffectRow(effects, leftover), span);
    }

    /// Final-pass resolution of all remaining effect-union constraints.
    /// Called after a declaration's body finishes inference, so source row
    /// vars have been bound by argument-type unification.
    fn resolve_pending_effect_unions(&mut self) {
        let pending = std::mem::take(&mut self.pending_effect_unions);
        for u in pending {
            self.resolve_effect_union(&u);
        }
        // Bounds are per-declaration: clear them so a closed-row requirement in
        // one declaration can't spuriously constrain another's unions.
        self.effect_union_upper_bounds.clear();
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
            Ty::EffectRow(_, Some(rv)) => match self.subst.get(rv) {
                Some(resolved) => self.collect_free_vars(resolved, out),
                None => {
                    out.insert(*rv);
                }
            },
            Ty::EffectRow(_, None) => {}
            Ty::Forall(bound, inner) => {
                let mut inner_set = HashSet::new();
                self.collect_free_vars(inner, &mut inner_set);
                for v in bound {
                    inner_set.remove(v);
                }
                out.extend(inner_set);
            }
            Ty::Alias(_, inner) => self.collect_free_vars(inner, out),
            Ty::Assoc(_, inner) => self.collect_free_vars(inner, out),
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
        // Keep the `with`-frame stack aligned with `scopes` (one entry per
        // scope, `None` for non-`with` scopes). The `With` arm overwrites the
        // entry it just pushed with `Some((span, fields))`.
        self.with_scope_stack.push(None);
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
        self.with_scope_stack.pop();
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

    fn lookup_instantiate_at(
        &mut self,
        name: &str,
        span: Span,
    ) -> Option<Ty> {
        let scheme = self.lookup(name)?.clone();
        let inst = self.instantiate_at(&scheme, span);
        Some(inst)
    }

    // ── AST type → Ty ────────────────────────────────────────────

    /// If `ty` is a (possibly applied) reference to a parameterized type alias
    /// (`Pair Int Text`), expand it: peel the `App` spine, elaborate the alias
    /// body with FRESH parameter variables, and substitute the actual
    /// arguments. Returns `None` when the head is not a parameterized alias, so
    /// the caller falls through to the normal `Named`/`App` handling.
    fn expand_param_alias(&mut self, ty: &ast::Type) -> Option<Ty> {
        // Peel the application spine into (head, args in application order).
        let mut args: Vec<&ast::Type> = Vec::new();
        let mut head = ty;
        while let ast::TypeKind::App { func, arg } = &head.node {
            args.push(arg);
            head = func;
        }
        args.reverse();
        let ast::TypeKind::Named(name) = &head.node else {
            return None;
        };
        let (params, body) = self.param_aliases.get(name)?.clone();
        if args.len() != params.len() {
            self.error(
                format!(
                    "type alias `{name}` expects {} argument(s), but {} were supplied",
                    params.len(),
                    args.len()
                ),
                ty.span,
            );
            return Some(Ty::Error);
        }
        // Bind each parameter name to a FRESH variable so every use of the
        // alias elaborates independently (no shared pinning across call sites).
        let saved: Vec<(String, Option<TyVar>)> = params
            .iter()
            .map(|p| (p.clone(), self.annotation_vars.get(p).copied()))
            .collect();
        let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
        for (p, arg_ty) in params.iter().zip(args.iter()) {
            let pv = self.fresh_var();
            self.annotation_vars.insert(p.clone(), pv);
            let arg = self.ast_type_to_ty(arg_ty);
            mapping.insert(pv, arg);
        }
        let body_ty = self.ast_type_to_ty(&body);
        // Restore the caller's annotation-vars bindings for these param names.
        for (p, old) in saved {
            match old {
                Some(v) => {
                    self.annotation_vars.insert(p, v);
                }
                None => {
                    self.annotation_vars.remove(&p);
                }
            }
        }
        let expanded = self.subst_ty(&body_ty, &mapping);
        Some(Ty::Alias(name.clone(), Box::new(expanded)))
    }

    /// Arity (number of type arguments) of a type constructor by name.
    /// Base scalar types are arity 0; ADTs use their `data` param count;
    /// parameterized aliases use their param count. Unknown names default to 0
    /// (treated as a saturated opaque type).
    fn type_head_arity(&self, name: &str) -> usize {
        match name {
            "Int" | "Float" | "Text" | "Bool" | "Bytes" | "Uuid" => 0,
            "Maybe" => 1,
            "Result" => 2,
            _ => {
                if let Some(info) = self.data_types.get(name) {
                    info.params.len()
                } else if let Some((params, _)) = self.param_aliases.get(name) {
                    params.len()
                } else {
                    0
                }
            }
        }
    }

    /// Consume one *complete* type from the head of a flattened application
    /// spine (`[head, a, b, …]`), arity-aware: a head of arity `n` eats the
    /// next `n` spine elements (each recursively a complete type). Returns the
    /// type AST and the number of spine elements consumed. `None` if the head
    /// is not a type.
    fn consume_type_arg<'a>(&self, spine: &[&'a ast::Expr]) -> Option<(ast::Type, usize)> {
        use knot::ast::TypeKind;
        let head = spine.first()?;
        let mut head_expr = *head;
        while let ast::ExprKind::Annot { expr: inner, .. } = &head_expr.node {
            head_expr = inner;
        }
        let ast::ExprKind::Constructor(name) = &head_expr.node else {
            return None;
        };
        let arity = self.type_head_arity(name);
        let mut consumed = 1;
        let mut ty = knot::ast::Spanned {
            node: if name == "Int" || name == "Float" {
                // Bare numeric base as a type argument means dimensionless.
                TypeKind::UnitAnnotated {
                    base: Box::new(knot::ast::Spanned {
                        node: TypeKind::Named(name.clone()),
                        span: head.span,
                    }),
                    unit: knot::ast::UnitExpr::Dimensionless,
                }
            } else {
                TypeKind::Named(name.clone())
            },
            span: head.span,
        };
        for _ in 0..arity {
            let sub = spine.get(consumed)?;
            let sub_flat = flatten_spine(sub);
            let (sub_ty, sub_consumed) = self.consume_type_arg(&sub_flat)?;
            if sub_consumed != sub_flat.len() {
                // The type argument itself must be a complete type (no trailing).
                return None;
            }
            ty = knot::ast::Spanned {
                node: TypeKind::App {
                    func: Box::new(ty.clone()),
                    arg: Box::new(sub_ty),
                },
                span: head.span,
            };
            consumed += 1;
        }
        Some((ty, consumed))
    }

    fn ast_type_to_ty(&mut self, ty: &ast::Type) -> Ty {
        match &ty.node {
            ast::TypeKind::Named(name) => match name.as_str() {
                "Int" => {
                    if self.in_type_annotation || self.enforce_units {
                        self.error(
                            "bare `Int` requires a unit — write `Int 1` (dimensionless), `Int M`, or `Int u`".into(),
                            ty.span,
                        );
                        return Ty::Error;
                    }
                    Ty::Int
                }
                "Float" => {
                    if self.in_type_annotation || self.enforce_units {
                        self.error(
                            "bare `Float` requires a unit — write `Float 1` (dimensionless), `Float M`, or `Float u`".into(),
                            ty.span,
                        );
                        return Ty::Error;
                    }
                    Ty::Float
                }
                "Text" => Ty::Text,
                "Bool" => Ty::Bool,
                "Bytes" => Ty::Bytes,
                "Uuid" => Ty::Uuid,
                "[]" => Ty::TyCon("[]".into()),
                _ => {
                    // Record-confined type name from an enclosing `with` peel
                    // over an embedded `type`/`data`. Consulted FIRST so it
                    // shadows any outer/global meaning, and it only exists for
                    // the duration of the `with` body.
                    let mut record_binding = None;
                    for scope in self.record_type_scopes.iter().rev() {
                        if let Some(b) = scope.get(name) {
                            record_binding = Some(b.clone());
                            break;
                        }
                    }
                    if let Some(binding) = record_binding {
                        match binding {
                            RecordTypeBinding::TyCon => {
                                // Parameterized embedded alias referenced bare:
                                // an unapplied type constructor.
                                return Ty::TyCon(name.clone());
                            }
                            RecordTypeBinding::Data { params, .. } => {
                                if params.is_empty() {
                                    return Ty::Con(name.clone(), vec![]);
                                }
                                return Ty::TyCon(name.clone());
                            }
                        }
                    }
                    // Π-lite type-witness parameter: `x : T` inside a lambda
                    // that binds `\(T : Type)` resolves to the witness skolem.
                    // Checked before aliases so a witness shadows an alias.
                    for scope in self.type_param_scopes.iter().rev() {
                        if let Some(s) = scope.get(name) {
                            return Ty::Var(*s);
                        }
                    }
                    // Parameterized alias referenced bare (`Pair` with 0 args).
                    if self.param_aliases.contains_key(name) {
                        if let Some(t) = self.expand_param_alias(ty) {
                            return t;
                        }
                    }
                    if let Some(aliased) = self.aliases.get(name).cloned() {
                        // Freshen any free type variables in the alias body
                        // (e.g. the `a` in `type Box = {val: a}`): the body
                        // was converted ONCE at collection time, so without
                        // freshening every reference to the alias shares
                        // the same variable — the first use pins it (e.g.
                        // to Int) and later uses at other types are falsely
                        // rejected.
                        let mut fv = HashSet::new();
                        self.collect_free_vars(&aliased, &mut fv);
                        let aliased = if fv.is_empty() {
                            aliased
                        } else {
                            let mapping: HashMap<TyVar, Ty> = fv
                                .into_iter()
                                .map(|v| (v, self.fresh()))
                                .collect();
                            // These freshly-minted alias-body vars must be
                            // quantified in the enclosing annotation's scheme.
                            // Without registering them in `annotation_vars`,
                            // the pre-registered scheme leaves them unquantified
                            // and shares them across every call site — the first
                            // use pins the alias (e.g. `Box` to `{val: Int 1}`) and
                            // later uses at other types are falsely rejected. The
                            // bug surfaced only when the annotated decl was
                            // declared after its first caller, so re-generalization
                            // (which never happens for constrained functions)
                            // couldn't paper over it (bug B21). Guarded on
                            // `in_type_annotation` so only scheme-building callers
                            // are affected, not alias-definition collection.
                            // They are quantified, but not *universally
                            // promised* by the annotation: `b1 : Box` leaves
                            // `val`'s type open for the body to choose, so the
                            // vars are recorded here and instantiated flexibly
                            // (not skolemised) when the body is checked.
                            if self.in_type_annotation {
                                for fresh in mapping.values() {
                                    if let Ty::Var(v) = fresh {
                                        self.annotation_vars
                                            .insert(format!("__alias_fv#{v}"), *v);
                                        self.alias_free_vars.insert(*v);
                                    }
                                }
                            }
                            self.subst_ty(&aliased, &mapping)
                        };
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
                        .is_some_and(|d| !d.params.is_empty())
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
                // Applied parameterized alias (`Pair Int Text`): expand it.
                if let Some(t) = self.expand_param_alias(ty) {
                    return t;
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
                let mut row_var = |name: &str| -> TyVar {
                    if name == "_" {
                        self.fresh_var()
                    } else {
                        self.annotation_var(name)
                    }
                };
                let row_tail = match rest.len() {
                    0 => None,
                    1 => Some(row_var(&rest[0])),
                    _ => {
                        // `r1 \/ r2 \/ ...` — introduce a fresh result row var
                        // and register an effect-union constraint so it gets
                        // bound to the union of the named source rows.
                        let sources: Vec<TyVar> = rest.iter().map(|n| row_var(n)).collect();
                        let result = self.fresh_var();
                        self.pending_effect_unions.push(EffectUnion {
                            result,
                            sources,
                            declared: true,
                        });
                        Some(result)
                    }
                };
                Ty::IO(
                    io_effects,
                    row_tail,
                    Box::new(self.ast_type_to_ty(inner_ty)),
                )
            }
            ast::TypeKind::UnitAnnotated { base, unit } => {
                // Convert the base (`Int`/`Float`) without the bare-numeric
                // check — the unit is supplied right here.
                let saved_flag = self.in_type_annotation;
                let saved_enforce = self.enforce_units;
                self.in_type_annotation = false;
                self.enforce_units = false;
                let base_ty = self.ast_type_to_ty(base);
                self.in_type_annotation = saved_flag;
                self.enforce_units = saved_enforce;
                let unit_ty = self.ast_unit_to_unit_ty(unit);
                match base_ty {
                    Ty::Int => Ty::int_with_unit(unit_ty),
                    Ty::Float => Ty::float_with_unit(unit_ty),
                    _ => {
                        self.error(
                            "unit annotations are only allowed on Int and Float types".into(),
                            ty.span,
                        );
                        Ty::Error
                    }
                }
            }
            ast::TypeKind::Unit(_unit) => {
                // A standalone type-level unit expression. Only meaningful as
                // the argument of `Con("Int"/"Float", [Unit(u)])`, which is
                // built via `UnitAnnotated`. Reaching here means the unit
                // appeared bare in a type position — treat it as an error
                // since a unit is not a value-inhabited type.
                self.error(
                    "a unit expression cannot appear as a standalone type — it must be the argument of Int or Float".into(),
                    ty.span,
                );
                Ty::Error
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

    /// Render an effect set with optional row tail as a standalone string —
    /// `{e1, e2 | _}`, `{}` for closed empty, `{e1}` when no tail, or just
    /// `_` when the set is empty and only a row variable is present
    /// (parser-supported shorthand for `{| _}`). Promoting a bare row var
    /// to `{| _}` would falsely advertise an explicit empty effect row when
    /// the user only meant a polymorphic placeholder.
    ///
    /// Callers (IO display, Server display via generic `Ty::Con` rendering)
    /// add their own spacing.
    fn display_effect_set(
        &self,
        effects: &BTreeSet<IoEffect>,
        row: Option<TyVar>,
    ) -> String {
        let row_name = row.map(|rv| match self.subst.get(&rv) {
            Some(resolved) => self.display_ty(resolved),
            None => "_".into(),
        });
        if effects.is_empty() {
            return match row_name {
                Some(name) => name,
                None => "{}".into(),
            };
        }
        let effects_str: String = effects
            .iter()
            .map(format_io_effect)
            .collect::<Vec<_>>()
            .join(", ");
        match row_name {
            Some(name) => format!("{{{} | {}}}", effects_str, name),
            None => format!("{{{}}}", effects_str),
        }
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
            Ty::Text => "Text".into(),
            Ty::Bool => "Bool".into(),
            Ty::Bytes => "Bytes".into(),
            Ty::Uuid => "Uuid".into(),
            Ty::Assoc(name, inner) => {
                format!("{} {}", name, self.display_ty_inner(inner, true))
            }
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
                // Unit-bearing Int/Float: `Con("Int", [Unit(u)])` → `Int u`,
                // collapsing to `Int`/`Float` when the unit is dimensionless.
                if (name == "Int" || name == "Float") && args.len() == 1 {
                    if let Ty::Unit(u) = args[0].peel_alias() {
                        let u = self.apply_unit(u);
                        if u.is_dimensionless() {
                            return name.clone();
                        }
                        return format!("{} {}", name, u.display());
                    }
                }
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
                    "IO {} {}",
                    self.display_effect_set(&effects, row),
                    self.display_ty(inner),
                )
            }
            Ty::EffectRow(effects, row) => {
                let (effects, row) =
                    self.resolve_effect_row(effects.clone(), *row);
                self.display_effect_set(&effects, row)
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
            // A standalone `Ty::Unit` only appears as the argument of
            // `Con("Int"/"Float", [Unit(u)])`, whose `Con` arm renders it.
            // Render it bare here as a defensive fallback.
            Ty::Unit(u) => format!("Unit<{}>", u.display()),
            Ty::Error => "<error>".into(),
        }
    }

    // ── Constructor instantiation ────────────────────────────────

    /// Returns (data_type, field_record_type) with fresh vars for params.
    /// Is `name` a constructor provided ONLY by built-in ADTs (`Bool`,
    /// `Maybe`, `Result`)? Built-ins stay referenceable bare (`True`, `Just`,
    /// `Ok`); every user-defined constructor must be qualified (`Color.Red`).
    /// Returns false when the name is unknown or any user ADT provides it.
    fn is_builtin_ctor(&self, name: &str) -> bool {
        match self.constructors.get(name) {
            Some(infos) if !infos.is_empty() => infos
                .iter()
                .all(|i| self.builtin_data_types.contains(&i.data_type)),
            _ => false,
        }
    }

    fn instantiate_ctor(
        &mut self,
        name: &str,
        _span: Span,
    ) -> Option<(Ty, Ty)> {
        let infos = self.constructors.get(name)?.clone();

        // A constructor name shared by more than one ADT is genuinely
        // ambiguous at this site — without a known expected type we can't tell
        // which ADT (or payload shape) is meant. With open variants removed,
        // an ambiguous bare name is an error; only a single-ADT (built-in)
        // bare constructor resolves here. User constructors go through the
        // qualified path (`instantiate_qualified_ctor`).
        if infos.len() > 1 {
            return None;
        }
        let info = infos.into_iter().next()?;

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

    /// Instantiate a constructor reached through its declaring data type:
    /// `Color.Red` resolves `Red` **within `Color`** specifically, never via
    /// the global constructor map. This is the confined, qualified-constructor
    /// path — the bare-ctor open-variant behavior does not apply here.
    ///
    /// Returns `(data_ty, payload_record_ty)` like `instantiate_ctor`, or
    /// `None` when `data_name` is not a data type or has no such constructor.
    fn instantiate_qualified_ctor(
        &mut self,
        data_name: &str,
        ctor_name: &str,
    ) -> Option<(Ty, Ty)> {
        let info = self.data_types.get(data_name)?.clone();
        let fields = info
            .ctors
            .iter()
            .find(|(n, _)| n == ctor_name)?
            .1
            .clone();

        let saved_annotation_vars = self.annotation_vars.clone();
        self.annotation_vars.clear();
        let param_tys: Vec<Ty> = info
            .params
            .iter()
            .map(|p| {
                let v = self.fresh_var();
                self.annotation_vars.insert(p.clone(), v);
                Ty::Var(v)
            })
            .collect();

        let field_tys: BTreeMap<String, Ty> = fields
            .iter()
            .map(|(name, ty)| (name.clone(), self.ast_type_to_ty(ty)))
            .collect();

        let data_ty = if data_name == "Bool" {
            Ty::Bool
        } else {
            Ty::Con(data_name.to_string(), param_tys)
        };
        let record_ty = Ty::Record(field_tys, None);

        self.annotation_vars = saved_annotation_vars;
        Some((data_ty, record_ty))
    }

    // ── Expression inference ─────────────────────────────────────

    /// Resolve an application of a `^`-constrained function whose leading
    /// dictionary arguments are implicit. Returns `Some(result_ty)` when the
    /// spine's head names such a function and the dictionaries were resolved
    /// from scope; `None` to fall through to the generic application path
    /// (head not constrained, or dictionaries already supplied explicitly).
    /// Resolve the type of a `Var`-rooted field-access path (`fns.greet`) by
    /// instantiating the root's scheme and walking the record fields. Needed
    /// for implicit-dict callsites: the field's (elaborated) function type
    /// exposes the leading dictionary params, which `infer_expr` on the same
    /// expression would hide behind a fresh unification var.
    fn resolve_field_path_ty(&mut self, expr: &ast::Expr) -> Option<Ty> {
        let mut fields = Vec::new();
        let mut cur = expr;
        let root = loop {
            match &cur.node {
                ast::ExprKind::FieldAccess { expr: base, field } => {
                    fields.push(field.clone());
                    cur = base;
                }
                ast::ExprKind::Var(root) => break root.clone(),
                _ => return None,
            }
        };
        fields.reverse();
        let scheme = self.lookup(&root)?.clone();
        let mut ty = self.instantiate_at(&scheme, expr.span);
        for field in fields {
            let resolved = self.apply(&ty);
            let next = match resolved.peel_alias() {
                Ty::Record(fmap, _) => fmap.get(&field).cloned()?,
                _ => return None,
            };
            ty = next;
        }
        Some(ty)
    }

    fn try_infer_implicit_dict_app(&mut self, expr: &ast::Expr) -> Option<Ty> {
        // Peel the application spine into (head, args in application order).
        let mut args: Vec<&ast::Expr> = Vec::new();
        let mut head = expr;
        while let ast::ExprKind::App { func, arg } = &head.node {
            args.push(arg);
            head = func;
        }
        args.reverse();
        let ast::ExprKind::Var(name) = &head.node else {
            // A record-field fun with a `^`-field constraint is called through
            // a field path (`fns.greet`). Register/look up its dictionaries
            // under the dotted path so scope resolution works the same way.
            let Some(path) = implicit_dict_head_path(head) else {
                return None;
            };
            let dicts = self.implicit_dict_fns.get(&path)?.clone();
            let n_dicts = dicts.len();
            // Resolve the field's type structurally from the record root's
            // scheme (walking the path), so the leading dictionary params the
            // desugarer prepended are visible. `infer_expr(head)` would return
            // a fresh unification var, not the function type.
            let Some(head_ty) = self.resolve_field_path_ty(head) else {
                return None;
            };
            let arity = curry_arity(&head_ty);
            let explicit_arity = arity - n_dicts;
            if args.len() != explicit_arity {
                return None;
            }
            // The field's type is monomorphic within its record (no `Forall`
            // to instantiate): split off the leading dictionary params
            // structurally, then type the explicit args against the rest.
            let mut inst = head_ty;
            let mut dict_tys: Vec<Ty> = Vec::with_capacity(n_dicts);
            for _ in 0..n_dicts {
                let Ty::Fun(param, rest) = inst else {
                    return None;
                };
                dict_tys.push((*param).clone());
                inst = (*rest).clone();
            }
            let mut result = inst;
            for a in &args {
                let arg_ty = self.infer_expr(a);
                let ret = self.fresh();
                self.unify(&result, &Ty::Fun(Box::new(arg_ty), Box::new(ret.clone())), a.span);
                result = ret;
            }
            for (i, (field, _)) in dicts.iter().enumerate() {
                let dict_ty = self.apply(&dict_tys[i]);
                let field_ty = match dict_ty.peel_alias() {
                    Ty::Record(fields, _) => fields.get(field).cloned().unwrap_or(dict_ty),
                    _ => dict_ty,
                };
                if let Some((root, path)) = self.resolve_dict(field, &field_ty, expr.span) {
                    self.implicit_dict_args.insert(expr.span, (root, path));
                }
            }
            return Some(result);
        };
        let dicts = self.implicit_dict_fns.get(name)?.clone();
        let n_dicts = dicts.len();
        // If the caller already supplied the dictionaries explicitly (more
        // args than the non-dict parameters), don't treat this as implicit.
        // The non-dict arity is the function's curried arity minus the dicts;
        // with exactly `arity - n_dicts` args the dicts are implicit.
        let scheme = self.lookup(name)?.clone();
        let arity = curry_arity(&scheme.ty);
        let explicit_arity = arity - n_dicts;
        if args.len() != explicit_arity {
            return None;
        }

        // Instantiate and resolve each leading dictionary from scope. We must
        // ground the dictionary type from the supplied arguments first, so
        // type the full application (with fresh dict placeholders) and then
        // solve each placeholder against the scope.
        let mut inst = self.instantiate_at(&scheme, expr.span);
        let mut dict_tys: Vec<Ty> = Vec::with_capacity(n_dicts);
        for _ in 0..n_dicts {
            let Ty::Fun(param, rest) = inst else {
                return None;
            };
            dict_tys.push((*param).clone());
            inst = (*rest).clone();
        }
        // Type the explicit arguments against the remaining curried type.
        let mut result = inst;
        for a in &args {
            let arg_ty = self.infer_expr(a);
            let ret = self.fresh();
            self.unify(&result, &Ty::Fun(Box::new(arg_ty), Box::new(ret.clone())), a.span);
            result = ret;
        }
        // Now the dictionary types are ground; resolve each against the
        // in-scope records and record the splice for codegen.
        for (i, (field, _)) in dicts.iter().enumerate() {
            let dict_ty = self.apply(&dict_tys[i]);
            let field_ty = match dict_ty.peel_alias() {
                Ty::Record(fields, _) => fields.get(field).cloned().unwrap_or(dict_ty),
                _ => dict_ty,
            };
            if let Some((root, path)) = self.resolve_dict(field, &field_ty, expr.span) {
                self.implicit_dict_args.insert(expr.span, (root, path));
            }
        }
        Some(result)
    }

    /// Find an in-scope RECORD supplying `field` at `field_ty`, for splicing
    /// as an implicit dictionary. Unlike `resolve_implicit_ref` (which returns
    /// the *field value* projection for `^field`), this returns the *record*
    /// that owns the field. Mirrors its search order: nearest scope first,
    /// then shallowest nesting, then sorted field order.
    ///
    /// - A named record `intOrd = {compare …}` resolves to `(intOrd, [path…])`.
    /// - A `with {compare …}` / `with intOrdDesc` frame resolves to the `with`
    ///   record value, bound by codegen under `\0withrec:<span>`; the path is
    ///   the field's nesting inside that record (minus the field itself).
    fn resolve_dict(&mut self, field: &str, field_ty: &Ty, span: Span) -> Option<(String, Vec<String>)> {
        // Candidate 0: an enclosing `with` frame that binds `field`. Snapshot
        // the frames first (immutable scan) so the speculative unify below can
        // borrow `self` mutably.
        let with_frames: Vec<(Span, Ty, bool)> = self
            .with_scope_stack
            .iter()
            .zip(self.scopes.iter())
            .rev()
            .filter_map(|(with_frame, scope)| {
                if let Some((with_span, field_schemes)) = with_frame
                    && let Some(scheme) = field_schemes.get(field)
                {
                    return Some((*with_span, scheme.ty.clone(), true));
                }
                if scope.contains_key(field) {
                    return Some((Span::new(0, 0), Ty::Error, false)); // shadow marker
                }
                None
            })
            .collect();
        for (with_span, scheme_ty, is_with) in with_frames {
            if !is_with {
                break;
            }
            let mut trial = self.subst.clone();
            std::mem::swap(&mut self.subst, &mut trial);
            let errs_before = self.errors.len();
            self.unify(&scheme_ty, field_ty, span);
            let ok = self.errors.len() == errs_before;
            self.errors.truncate(errs_before);
            std::mem::swap(&mut self.subst, &mut trial);
            if ok {
                self.subst = trial;
                let alias = format!("{WITH_RECORD_ALIAS_PREFIX}{}", with_span.start);
                // The `with` record itself is the dictionary (its `field` is
                // bound directly by the frame).
                return Some((alias, Vec::new()));
            }
        }

        // General case: BFS in-scope record bindings for one with a `field`
        // unifying with `field_ty`. The dict is the record projected along the
        // path to `field`, minus the field itself.
        let mut candidates: Vec<(String, Vec<String>, Ty)> = Vec::new();
        'scopes: for scope in self.scopes.iter().rev() {
            for (bind_name, scheme) in scope {
                let root_ty = self.apply(&scheme.ty);
                let mut frontier: Vec<(Vec<String>, Ty)> = match root_ty.peel_alias() {
                    Ty::Record(fields, _) => fields
                        .iter()
                        .map(|(f, t)| (vec![f.clone()], t.clone()))
                        .collect(),
                    _ => Vec::new(),
                };
                while !frontier.is_empty() {
                    let mut next: Vec<(Vec<String>, Ty)> = Vec::new();
                    for (path, fty) in frontier {
                        if *path.last().expect("non-empty path") == field {
                            candidates.push((bind_name.clone(), path.clone(), fty.clone()));
                        }
                        if let Ty::Record(sub, _) = self.apply(&fty).peel_alias().clone() {
                            for (f, t) in sub {
                                let mut p = path.clone();
                                p.push(f);
                                next.push((p, t));
                            }
                        }
                    }
                    if !candidates.is_empty() {
                        break;
                    }
                    frontier = next;
                }
            }
            if !candidates.is_empty() {
                break 'scopes;
            }
        }

        for (root, path, fty) in &candidates {
            let mut trial = self.subst.clone();
            std::mem::swap(&mut self.subst, &mut trial);
            let errs_before = self.errors.len();
            let fty = fty.clone();
            self.unify(&fty, field_ty, span);
            let ok = self.errors.len() == errs_before;
            self.errors.truncate(errs_before);
            std::mem::swap(&mut self.subst, &mut trial);
            if ok {
                self.subst = trial;
                let dict_path = if path.len() > 1 {
                    path[..path.len() - 1].to_vec()
                } else {
                    Vec::new()
                };
                return Some((root.clone(), dict_path));
            }
        }
        self.error(
            format!("no in-scope record supplies an implicit dictionary field '{field}'"),
            span,
        );
        None
    }

    /// Resolve a `^name` implicit field projection against `expected`.
    ///
    /// Searches the fields of in-scope RECORD bindings (only records — plain
    /// and function bindings are invisible) for a field named `name` whose
    /// type unifies with `expected`. Search order: nearest scope first,
    /// then shallowest record-nesting depth (a binding's own fields beat
    /// fields of nested records), then fields in sorted order (record types
    /// store fields in a `BTreeMap`, so source declaration order is
    /// unavailable). Each candidate is tested with a speculative unify
    /// against a throwaway clone of the real substitution; only the winning
    /// candidate's constraints are committed to `self`. The resolved
    /// (root binding, field path) is recorded in `implicit_refs` keyed by
    /// `span` so codegen can lower `^name` to a projection chain.
    fn resolve_implicit_ref(&mut self, name: &str, expected: &Ty, span: Span) -> Ty {
        // A `with` binds each of the record's fields DIRECTLY into its body
        // scope. The record-BFS below only finds fields nested inside
        // record-typed bindings, so it misses a `with` field whose value is
        // not itself a record (e.g. `with {show (\n -> …)}` binds `show : fn`,
        // and the BFS would fall through to an OUTER same-named record field —
        // resolving `^show` to a lexically-wrong dictionary: two sequential
        // `with` blocks would both hit the same outer record, and a nested
        // `with` could not shadow the outer). A direct `with`-field binding
        // for `name` therefore takes precedence: it is candidate 0, rooted at
        // the `with` site's unique alias (see `WITH_FIELD_ALIAS_PREFIX` and
        // codegen's `With` arm, which binds the field's value under that
        // alias), and the innermost such `with` wins (nested shadows, siblings
        // don't collide). A direct NON-`with` binding keeps its historical
        // meaning — the BFS field projection off that binding.
        let mut with_candidate: Option<(String, Vec<String>, Ty)> = None;
        for (with_frame, scope) in self
            .with_scope_stack
            .iter()
            .zip(self.scopes.iter())
            .rev()
        {
            if let Some((with_span, field_schemes)) = with_frame
                && let Some(scheme) = field_schemes.get(name)
            {
                let alias =
                    format!("{WITH_FIELD_ALIAS_PREFIX}{}@{name}", with_span.start);
                with_candidate = Some((alias, Vec::new(), scheme.ty.clone()));
                break;
            }
            if scope.contains_key(name) {
                // A non-`with` binding shadows any outer `with` field — the
                // BFS below projects `name` off it, as before.
                break;
            }
        }
        // Candidate search over an immutable view of the scopes. Walk
        // innermost-to-outermost (nearest scope wins across the whole
        // search) and BFS the record's fields shallowest-first; `fields` is
        // a `BTreeMap`, so within a level iteration is by sorted field name.
        let mut candidates: Vec<(String, Vec<String>, Ty)> =
            with_candidate.into_iter().collect();
        'scopes: for scope in self.scopes.iter().rev() {
            for (bind_name, scheme) in scope {
                // A `with` field's binding scheme is the field's own type —
                // no quantified vars to instantiate, so `scheme.ty` is the
                // binding's type as-is.
                let root_ty = self.apply(&scheme.ty);
                let mut frontier: Vec<(Vec<String>, Ty)> = match root_ty.peel_alias() {
                    Ty::Record(fields, _) => fields
                        .iter()
                        .map(|(f, t)| (vec![f.clone()], t.clone()))
                        .collect(),
                    _ => Vec::new(),
                };
                while !frontier.is_empty() {
                    let mut next: Vec<(Vec<String>, Ty)> = Vec::new();
                    for (path, field_ty) in frontier {
                        if *path.last().expect("non-empty path") == name {
                            candidates.push((bind_name.clone(), path.clone(), field_ty.clone()));
                        }
                        // Descend into nested record fields (without
                        // committing anything: `apply` is read-only).
                        if let Ty::Record(sub, _) = self.apply(&field_ty).peel_alias().clone() {
                            for (f, t) in sub {
                                let mut p = path.clone();
                                p.push(f);
                                next.push((p, t));
                            }
                        }
                    }
                    // Shallowest depth wins: if this depth produced any
                    // candidate, deeper nesting is never considered.
                    if !candidates.is_empty() {
                        break;
                    }
                    frontier = next;
                }
            }
            if !candidates.is_empty() {
                break 'scopes;
            }
        }

        // Speculatively unify each candidate against `expected` in order.
        // The speculative substitution CLONES the real one but points every
        // variable straight at its fully-resolved type, so bindings made
        // during the trial are all at fresh or resolved-root variables and
        // never reach a shared deeper chain — applying the winner's diff to
        // the real substitution is then a faithful replay.
        let mut searched: Vec<String> = Vec::new();
        for (root, path, field_ty) in &candidates {
            let mut trial: HashMap<TyVar, Ty> = HashMap::with_capacity(self.subst.len());
            for v in self.subst.keys() {
                let resolved = self.apply(&Ty::Var(*v));
                trial.insert(*v, resolved);
            }
            let mut trial_errors: Vec<(String, Span)> = Vec::new();
            std::mem::swap(&mut self.subst, &mut trial);
            std::mem::swap(&mut self.errors, &mut trial_errors);
            self.unify(&field_ty.clone(), expected, span);
            std::mem::swap(&mut self.subst, &mut trial);
            std::mem::swap(&mut self.errors, &mut trial_errors);
            // `trial` now holds the post-unify speculative substitution.
            if trial_errors.is_empty() {
                for (v, t) in trial {
                    self.subst.insert(v, t);
                }
                self.implicit_refs
                    .insert(span, (root.clone(), path.clone()));
                return field_ty.clone();
            }
            searched.push(format!("{}.{} : {}", root, path.join("."), self.display_ty(field_ty)));
        }

        let detail = if searched.is_empty() {
            "no in-scope record binding has a field with this name".to_string()
        } else {
            format!("searched: {}", searched.join(", "))
        };
        self.error(
            format!(
                "no in-scope record field '{name}' matches the expected type ({detail})"
            ),
            span,
        );
        Ty::Error
    }

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
                self.empty_spans.insert(expr.span);
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
                    // If this Var resolved to a field of a `with` that codegen
                    // binds in the CURRENT env frame, redirect codegen's `Var`
                    // lookup to that `with` site's unique alias slot. Codegen's
                    // runtime `Env` is a FLAT HashMap, so the bare field name
                    // is a single slot shared by every `with` whose body is
                    // being compiled — whichever `with` set it most recently at
                    // RUNTIME would win, ignoring lexical scope (e.g. two
                    // sequential `with {show …}` blocks both compiling
                    // `^show`'s root `Var("show")`, or a rebound/shadowing
                    // local clobbering a `with` field). The alias
                    // (`{PREFIX}{with_span}@{field}`, bound by codegen's `With`
                    // arm alongside the bare name) is unique per `with` site,
                    // so the emitted `Var(alias)` hits the lexically correct
                    // dictionary.
                    //
                    // Codegen's `Env` frames fork at every `With` arm, so an
                    // infer scope that binds `name` between the Var and the
                    // `with` frame (a lambda param, a do-block bind, …) is
                    // indistinguishable at runtime from the `with` frame itself
                    // — both live in the same flat env. The redirect therefore
                    // fires whenever the INNERMOST binder of `name` is a
                    // `with` frame, no matter how many scopes intervene.
                    //
                    // The one place codegen's env genuinely diverges from the
                    // infer scopes is a `with`'s OPERAND: codegen compiles it
                    // in an env derived from the ENCLOSING env with every
                    // `with` binding masked (nested shadowing), while infer
                    // pushes the new `with`'s frame before inferring the
                    // operand. So when the innermost enclosing `with` frame
                    // belongs to the `with` whose operand is currently being
                    // inferred, codegen's operand env has no alias slot for it
                    // — and a Var resolved to that frame must NOT redirect
                    // (its bare name reads the enclosing env's value, exactly
                    // as the old runtime-frame-popping behaviour produced).
                    // Deeper `with` frames are restored before the body compiles
                    // and their aliases ARE present in the operand env's
                    // prototype, so they still redirect.
                    let innermost_with_idx = self
                        .with_scope_stack
                        .iter()
                        .rposition(Option::is_some);
                    for (idx, (with_frame, scope)) in self
                        .with_scope_stack
                        .iter()
                        .zip(self.scopes.iter())
                        .enumerate()
                        .rev()
                    {
                        if let Some((with_span, field_schemes)) = with_frame
                            && let Some(scheme) = field_schemes.get(name)
                            && Some(idx) != innermost_with_idx
                        {
                            let alias = format!(
                                "{WITH_FIELD_ALIAS_PREFIX}{}@{name}",
                                with_span.start
                            );
                            self.implicit_refs.insert(expr.span, (alias, Vec::new()));
                            return scheme.ty.clone();
                        }
                        if scope.contains_key(name) {
                            break;
                        }
                    }
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
                if !self.is_builtin_ctor(name) && self.constructors.contains_key(name) {
                    // A USER-defined constructor referenced bare. Constructors
                    // are always qualified — require `Type.Ctor`. Built-ins
                    // (`True`, `Just`) fall through to the bare path below.
                    self.error(
                        format!(
                            "constructor '{}' must be qualified (e.g. `Type.{}`)",
                            name, name
                        ),
                        expr.span,
                    );
                    Ty::Error
                } else if let Some((data_ty, record_ty)) =
                    self.instantiate_ctor(name, expr.span)
                {
                    // Every constructor — including nullary ones — is a
                    // function from its record payload to its data type:
                    // `True : {} -> Bool`, `Just : {value: a} -> Maybe a`,
                    // `None : {} -> Maybe a`. A bare constructor reference is
                    // therefore a first-class function value; codegen
                    // eta-expands it into a closure when it isn't immediately
                    // applied (see the `App` arm and codegen's Constructor
                    // emission). This uniformity means `True False` parses and
                    // type-checks as applying `True` to the payload `False`
                    // (a type error, since `False : Bool` is not `{}`), and
                    // passing `True` to a higher-order function passes a
                    // closure.
                    Ty::Fun(Box::new(record_ty), Box::new(data_ty))
                } else {
                    // A capitalized name that isn't a constructor. Units are
                    // no longer declared, so there's no table to consult for
                    // the old value-literal hint — just report it plainly.
                    self.error(
                        format!("unknown constructor '{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::ImplicitRef(name) => {
                // `^name` — implicit field projection. Resolve against a
                // fresh expected-type variable; the first in-scope record
                // field named `name` that unifies wins, and the path is
                // recorded for codegen (see `resolve_implicit_ref`).
                let expected = self.fresh();
                self.resolve_implicit_ref(name, &expected, expr.span)
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
                // Detect duplicate field names — `BTreeMap::collect` would
                // silently keep only the last entry, masking a user error.
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for f in fields {
                    if !seen.insert(f.name.clone()) {
                        self.error(
                            format!("duplicate field '{}' in record literal", f.name),
                            f.value.span,
                        );
                    }
                }
                let field_tys: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|f| {
                        let val_ty = self.infer_expr(&f.value);
                        // A field with a standalone sig line (`name : Type`)
                        // must have a value whose type matches the sig —
                        // enforced exactly like an inline `(expr : Type)`
                        // ascription: lowercase unit names are polymorphic unit
                        // variables, then the value type is unified against the
                        // sig type at the value's span.
                        if let Some(sig) = &f.sig {
                            let saved_flag = self.in_type_annotation;
                            let saved_unit_vars = std::mem::take(&mut self.annotation_unit_vars);
                            self.in_type_annotation = true;
                            let sig_ty = self.ast_type_to_ty(&sig.ty);
                            self.in_type_annotation = saved_flag;
                            self.annotation_unit_vars = saved_unit_vars;
                            self.unify(&val_ty, &sig_ty, f.value.span);
                            (f.name.clone(), sig_ty)
                        } else {
                            (f.name.clone(), val_ty)
                        }
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
                // The base must be some record; `rv` captures its row.
                let rv = self.fresh_var();
                let constraint = Ty::Record(BTreeMap::new(), Some(rv));
                self.unify(&base_ty, &constraint, base.span);

                match self.apply(&Ty::Var(rv)) {
                    // The base's fields are fully known: overlay the updates
                    // on them. Fields the base already has keep their type;
                    // any others extend it — `{base | field: val}` is both
                    // update and extension syntax.
                    Ty::Record(base_fields, None) => {
                        let mut result = base_fields.clone();
                        for (name, update_ty) in update_fields {
                            if let Some(base_field_ty) = base_fields.get(&name)
                            {
                                let base_field_ty = base_field_ty.clone();
                                self.unify(
                                    &update_ty,
                                    &base_field_ty,
                                    base.span,
                                );
                            }
                            result.insert(name, update_ty);
                        }
                        Ty::Record(result, None)
                    }
                    // The base's row is still open, so we cannot tell whether
                    // it already holds the updated fields. Split the row into
                    // those fields and a `rest` tail carrying everything else,
                    // which requires the base to have them.
                    //
                    // Reusing the base's whole row as the result's tail (the
                    // previous approach) let a field sit in the tail *and* be
                    // named explicitly in the result. `{r | f: v}` and `r`
                    // then looked like they disagreed about `f`, so unifying
                    // the two — as `if c then {r | f: v} else r` does — failed
                    // with "record fields don't match".
                    _ => {
                        let rest = self.fresh_var();
                        let constraint =
                            Ty::Record(update_fields.clone(), Some(rest));
                        self.unify(&base_ty, &constraint, base.span);
                        Ty::Record(update_fields, Some(rest))
                    }
                }
            }

            ast::ExprKind::FieldAccess { expr: e, field } => {
                // Qualified constructor: `Color.Red`. The base parsed as a
                // `Constructor` (capitalized) but names a DATA TYPE; resolve
                // the field as that type's constructor, returning the ctor
                // function `payload -> DataTy`. Confined to `Color` — no
                // global/open-variant resolution.
                if let ast::ExprKind::Constructor(type_name) = &e.node
                    && self.data_types.contains_key(type_name)
                {
                    if let Some((data_ty, record_ty)) =
                        self.instantiate_qualified_ctor(type_name, field)
                    {
                        let ty = Ty::Fun(Box::new(record_ty), Box::new(data_ty));
                        self.field_accesses.push((expr.span, ty.clone()));
                        return ty;
                    }
                    self.error(
                        format!(
                            "data type '{}' has no constructor '{}'",
                            type_name, field
                        ),
                        expr.span,
                    );
                    return Ty::Error;
                }
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
                self.field_accesses.push((expr.span, field_ty.clone()));
                field_ty
            }

            ast::ExprKind::With { record, body } => {
                // Infer the record, then resolve its type to a concrete record
                // so the field names are known. Each field is bound as a local
                // variable for the body; the result is the body's type.
                //
                // For a RECORD-LITERAL operand, each field value is inferred
                // with only the ENCLOSING `with` frames' SAME-NAMED binding
                // masked (self-reference masking): `with` scopes a record's
                // fields over the BODY only, so a field's own value must not
                // capture an outer `with`'s same-named field (e.g. in
                // `with i (with {show (\n -> … show …)} …)` the inner lambda's
                // `show` must see the bindings that were in scope OUTSIDE the
                // outer `with` — the builtin — not i's `show`, else it would
                // produce "INNEROUTER1"). Other names stay fully visible:
                // argument-position references to outer `with` fields
                // (`with {ctor r.Pair}`, `with {xs (filter … xs)}` rebinding
                // `xs` from its outer value) keep working. Non-`with` scopes
                // are never masked, so ordinary locals stay visible too.
                let record_ty = if let ast::ExprKind::Record(field_exprs) =
                    &record.node
                {
                    let mut field_tys: Vec<(String, Ty)> =
                        Vec::with_capacity(field_exprs.len());
                    for f in field_exprs {
                        // Save any enclosing `with` frame that binds this
                        // field's name (innermost-to-outermost), masking it
                        // only while THIS field's value is inferred.
                        let mut masked: Vec<(
                            usize,
                            HashMap<String, Scheme>,
                            _,
                        )> = Vec::new();
                        for idx in (0..self.scopes.len()).rev() {
                            let is_with = self.with_scope_stack[idx]
                                .as_ref()
                                .is_some_and(|(_, fs)| fs.contains_key(&f.name));
                            if is_with {
                                let frame =
                                    self.with_scope_stack[idx].take().expect(
                                        "checked Some above",
                                    );
                                let scope =
                                    std::mem::take(&mut self.scopes[idx]);
                                masked.push((idx, scope, frame));
                            }
                        }
                        let val_ty = self.infer_expr(&f.value);
                        for (idx, scope, frame) in masked.into_iter().rev() {
                            self.scopes[idx] = scope;
                            self.with_scope_stack[idx] = Some(frame);
                        }
                        field_tys.push((f.name.clone(), val_ty));
                    }
                    Ty::Record(field_tys.into_iter().collect(), None)
                } else {
                    // Non-literal operand: nothing to mask (no field values
                    // visible), infer as-is.
                    self.infer_expr(record)
                };
                let resolved = self.apply(&record_ty);
                let fields = match &resolved {
                    Ty::Record(fields, _) => fields.clone(),
                    other => {
                        let shown = self.display_ty(other);
                        self.error(
                            format!("`with` requires a record, but this has type {shown}"),
                            record.span,
                        );
                        return Ty::Error;
                    }
                };
                self.push_scope();
                for (name, ty) in &fields {
                    self.bind(name, Scheme::mono(ty.clone()));
                }
                // Mark this scope as a `with` frame (span + field schemes) so
                // the `Var` arm can redirect a `with`-field reference to the
                // per-`with`-site alias codegen binds (lexical scoping in the
                // flat runtime `Env`).
                *self.with_scope_stack.last_mut().expect("just pushed") = Some((
                    expr.span,
                    fields
                        .iter()
                        .map(|(n, t)| (n.clone(), Scheme::mono(t.clone())))
                        .collect(),
                ));
                // Peel the record's embedded `type`/`data` declarations into a
                // scoped type env, confined to this `with` body. Only when the
                // record is a literal can we see the declarations; the bindings
                // vanish when the body ends (one layer — nested `with` pushes
                // its own frame).
                if let ast::ExprKind::Record(field_exprs) = &record.node {
                    let mut type_scope: HashMap<String, RecordTypeBinding> =
                        HashMap::new();
                    self.with_alias_saves.push(Vec::new());
                    for f in field_exprs {
                        match &f.value.node {
                            ast::ExprKind::TypeCtor { name, params, ty } => {
                                // Embedded `type` alias: resolve the body now and
                                // inject it into the global `aliases` map for the
                                // DURATION of this `with` body only (saved and
                                // restored below). This makes the confined alias
                                // behave byte-for-byte like a top-level alias.
                                // Parameterized aliases stay as TyCons.
                                if params.is_empty() {
                                    let resolved = self.ast_type_to_ty(ty);
                                    let save = (name.clone(), self.aliases.get(name).cloned());
                                    self.with_alias_saves
                                        .last_mut()
                                        .expect("frame just pushed")
                                        .push(save);
                                    self.aliases.insert(name.clone(), resolved);
                                } else {
                                    type_scope.insert(
                                        name.clone(),
                                        RecordTypeBinding::TyCon,
                                    );
                                }
                            }
                            ast::ExprKind::DataCtor { name, params, .. } => {
                                type_scope.insert(
                                    name.clone(),
                                    RecordTypeBinding::Data {
                                        params: params.clone(),
                                    },
                                );
                            }
                            _ => {}
                        }
                    }
                    self.record_type_scopes.push(type_scope);
                }
                // Register `with` field bindings in `let_bindings` (scoped to
                // this body) so `value_references_source` can fold through
                // them — same treatment `let` bindings got. If the record is
                // a literal we can record the field expressions precisely.
                let prev_let_bindings = self.let_bindings.clone();
                if let ast::ExprKind::Record(field_exprs) = &record.node {
                    for f in field_exprs {
                        self.let_bindings
                            .insert(f.name.clone(), f.value.clone());
                    }
                }
                let body_ty = self.infer_expr(body);
                self.let_bindings = prev_let_bindings;
                if let ast::ExprKind::Record(_) = &record.node {
                    self.record_type_scopes.pop();
                    // Restore any global aliases shadowed by this `with`'s
                    // embedded `type` decls, so nothing leaks past the body.
                    if let Some(frame) = self.with_alias_saves.pop() {
                        for (aname, saved) in frame {
                            match saved {
                                Some(prev) => {
                                    self.aliases.insert(aname, prev);
                                }
                                None => {
                                    self.aliases.remove(&aname);
                                }
                            }
                        }
                    }
                }
                self.pop_scope();
                self.with_fields
                    .push((expr.span, fields.keys().cloned().collect()));
                body_ty
            }

            ast::ExprKind::List(elems) => {
                let elem_ty = self.fresh();
                for e in elems {
                    let t = self.infer_expr(e);
                    self.unify(&elem_ty, &t, e.span);
                }
                Ty::Relation(Box::new(elem_ty))
            }

            ast::ExprKind::Lambda { params, ty_params, body } => {
                self.push_scope();
                // Type-witness params `\(T : Type)`: bind each to a rigid skolem
                // and record it in a type-param scope so `x : T` annotations in
                // the body resolve to the witness. The lambda's type prepends a
                // kind-`Type` arrow per witness, consumed at the call site by an
                // explicit type argument (erased at runtime).
                let mut ty_skolems: Vec<TyVar> = Vec::new();
                if !ty_params.is_empty() {
                    let mut scope = HashMap::new();
                    for tp in ty_params {
                        let s = self.fresh_var();
                        self.skolems.insert(s);
                        scope.insert(tp.name.clone(), s);
                        ty_skolems.push(s);
                    }
                    self.type_param_scopes.push(scope);
                }
                let mut param_types = Vec::new();
                for param in params {
                    let t = self.fresh();
                    self.check_pattern(param, &t);
                    param_types.push(t);
                }
                let body_ty = self.infer_expr(body);
                if !ty_params.is_empty() {
                    self.type_param_scopes.pop();
                }
                self.pop_scope();

                let mut result = body_ty;
                for pt in param_types.into_iter().rev() {
                    result = Ty::Fun(Box::new(pt), Box::new(result));
                }
                // Prepend the erased type-witness arrows (kind `Type`), one per
                // type param, so application consumes the type argument first,
                // then bind the witness skolems in a `Forall` so the caller
                // instantiates the exact witness var with the type argument.
                for _ in &ty_skolems {
                    result = Ty::Fun(
                        Box::new(Ty::Con("Type".into(), vec![])),
                        Box::new(result),
                    );
                }
                if !ty_skolems.is_empty() {
                    // The skolems are bound by this lambda; quantify them so the
                    // type is `∀ t. Type -> body`. They must not be treated as
                    // free rigid skolems from here on.
                    for s in &ty_skolems {
                        self.skolems.remove(s);
                    }
                    result = Ty::Forall(ty_skolems, Box::new(result));
                }
                result
            }

            ast::ExprKind::App { func, arg } => {
                // Special case: fully handle `fetch url (Ctor {..})` so the
                // response type can be resolved from route metadata.
                if let Some(ty) = self.try_infer_fetch(expr) {
                    return ty;
                }

                // Implicit-dictionary callsite: `clamp 0 10 42` where `clamp`
                // carries a `^`-field constraint. The function's scheme was
                // elaborated to take a leading dictionary record (see desugar);
                // here we resolve that dictionary from the in-scope records
                // (nearest scope wins, via the same search as `^field`), record
                // it for codegen to splice as the leading argument, and type
                // the application with the dictionary parameter consumed.
                if let Some(result) = self.try_infer_implicit_dict_app(expr) {
                    return result;
                }

                // `__result e` — a desugared do-block's final bare expression,
                // which is either `pure e` or `e` itself depending on whether
                // `e` is already an action in the block's monad. Neither the
                // desugarer nor a single HM type can decide that, so type it as
                // the block's `App(m, a)` and defer the choice to
                // `resolve_result_markers`, which reruns once `m` and `e`'s
                // type are known and then rewrites the AST accordingly.
                if let ast::ExprKind::Var(name) = &func.node
                    && name == crate::desugar::RESULT_MARKER
                {
                    let arg_ty = self.infer_expr(arg);
                    let m = self.fresh_var();
                    let a = self.fresh_var();
                    self.monad_vars.push((func.span, m));
                    self.result_markers.push(ResultMarker {
                        span: func.span,
                        monad: m,
                        elem: a,
                        arg: arg_ty,
                        arg_span: arg.span,
                        skolems: self.skolems.iter().copied().collect(),
                        effect_unions: self.pending_effect_unions.clone(),
                    });
                    return Ty::App(
                        Box::new(Ty::Var(m)),
                        Box::new(Ty::Var(a)),
                    );
                }

                // Constructor application `Ctor {fields}`: type the argument
                // against the constructor's field record and return the data
                // type directly. This is required now that bare nullary
                // constructors are values rather than `{} -> T` functions
                // (see the `Constructor` arm) — the generic application path
                // below would otherwise try to unify a value type with
                // `arg -> result`. Only the unambiguous record-payload case
                // is handled here; the ambiguous row-polymorphic-variant
                // constructor falls through to the generic path. A USER-defined
                // constructor applied bare is an error — constructors are
                // always qualified (`Color.Red {…}`); only built-ins
                // (`Just {…}`, `Nothing {}`) apply bare.
                if let ast::ExprKind::Constructor(name) = &func.node {
                    if !self.is_builtin_ctor(name) && self.constructors.contains_key(name) {
                        self.error(
                            format!(
                                "constructor '{}' must be qualified (e.g. `Type.{}`)",
                                name, name
                            ),
                            func.span,
                        );
                        return Ty::Error;
                    }
                    if let Some((data_ty, record_ty)) = self.instantiate_ctor(name, func.span)
                        && matches!(record_ty, Ty::Record(..))
                    {
                        self.check_expr(arg, &record_ty);
                        return data_ty;
                    }
                }

                // Let-binding: an immediately-applied single-variable lambda
                // `(\x -> body) arg` is semantically `let x = arg in body`.
                // The desugarer lowers pure-comprehension `do` `let`s to exactly
                // this shape, so generalize the binding here to preserve
                // let-polymorphism (e.g. `let g = \x -> x` usable at multiple
                // types), matching the non-desugared do-block `let` path. This
                // is sound: generalizing a let binding is always valid in a pure
                // language, and the bound name does not escape the body.
                if let ast::ExprKind::Lambda { params, body, .. } = &func.node
                    && params.len() == 1
                        && let ast::PatKind::Var(name) = &params[0].node {
                            let arg_ty = self.infer_expr(arg);
                            let applied = self.apply(&arg_ty);
                            let scheme = self.generalize(&applied);
                            self.push_scope();
                            self.bind(name, scheme);
                            self.binding_types.push((params[0].span, applied));
                            let body_ty = self.infer_expr(body);
                            self.pop_scope();
                            return body_ty;
                        }

                // Check lambda arguments LAST.
                //
                // `filter (\s -> isLocal s) names` is
                // `App(App(filter, lam), names)`. Inferring left to right lets
                // the lambda's body pin `filter`'s `a` to `isLocal`'s `Text`
                // before `names : [ServerName]` is ever looked at — the
                // refinement is thrown away, and the declared `[ServerName]`
                // result is then rejected. The *data* argument is the one that
                // knows the type, so pin the shared variables from it and
                // *check* the lambda against the parameter type it settled.
                //
                // Restricted to a named head with a two-argument signature, so
                // the shape is known before anything is inferred and there is
                // no half-inferred state to unwind.
                let lambda_last = if let ast::ExprKind::App { func: head, arg: lam } = &func.node
                    && matches!(&lam.node, ast::ExprKind::Lambda { .. })
                    && let ast::ExprKind::Var(head_name) = &head.node
                {
                    self.lookup(head_name)
                        .is_some_and(|s| takes_two_args(&s.ty))
                        .then(|| ((**head).clone(), (**lam).clone()))
                } else {
                    None
                };

                let (arg_ty, result_ty) = if let Some((head, lam)) = lambda_last {
                    let head_ty = self.infer_expr(&head);
                    let head_applied = self.apply(&head_ty);
                    let (p1, p2, ret) = match head_applied {
                        Ty::Fun(p1, rest) => match self.apply(&rest) {
                            Ty::Fun(p2, ret) => (*p1, *p2, *ret),
                            _ => unreachable!("takes_two_args checked the arity"),
                        },
                        _ => unreachable!("takes_two_args checked the arity"),
                    };
                    self.check_expr(arg, &p2);
                    self.check_expr(&lam, &p1);
                    let result_ty = self.fresh();
                    self.unify(&ret, &result_ty, expr.span);
                    (self.apply(&p2), result_ty)
                } else {
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
                            if let ast::ExprKind::Var(name) = &func.node
                                && name == "parseJson"
                                    && let Ty::Var(v) = &result_ty {
                                        self.from_json_calls.push((expr.span, *v));
                                    }
                            return result_ty;
                        }
                    }

                    // Π-lite explicit type argument: a type-witness lambda has
                    // type `∀ t. Type -> body`. An application `f Int` supplies
                    // the type argument `Int`, which is substituted for the
                    // bound witness var `t` throughout `body`, consuming the
                    // leading erased `Type` arrow. Runs BEFORE `infer_expr(arg)`
                    // so a bare uppercase type name is reinterpreted via
                    // `ast_type_to_ty` rather than erroring as a constructor.
                    if let Ty::Forall(_vars, body) = &func_applied {
                        let body_applied = self.apply(body);
                        if let Ty::Fun(witness_slot, _rest) = &body_applied
                            && matches!(self.apply(witness_slot), Ty::Con(ref n, _) if n == "Type")
                        {
                            // The parser glues `apply Int 42` into
                            // `apply (Int 42)` (constructor-application, like
                            // `Some 5`). Split the glued spine arity-aware:
                            // consume exactly one *complete* type (a head plus
                            // its `arity` type-arguments) and treat the rest as
                            // trailing value args. `Int 42` → type `Int`, value
                            // `42`; `const2 Int Text 99` → type `Int`, trailing
                            // `Text 99`; `f (Maybe Int) x` → type `Maybe Int`.
                            let flat = flatten_spine(arg);
                            if let Some((ty_ast, consumed)) = self.consume_type_arg(&flat) {
                                let type_span = ty_ast.span;
                                let mut pending: Vec<&ast::Expr> =
                                    flat.into_iter().skip(consumed).collect();
                                // Consume the (possibly several) leading type
                                // arguments: each bound witness var eats one
                                // complete type. `const2 Int Text 99` consumes
                                // `Int` for `A` then `Text` for `B`, leaving
                                // `99` as the sole value argument.
                                let mut cur_ty: Ty = func_applied.clone();
                                let mut first_ty: Option<ast::Type> = Some(ty_ast);
                                loop {
                                    let cur_applied = self.apply(&cur_ty);
                                    let (vars, body) = match &cur_applied {
                                        Ty::Forall(v, b) => (v, b),
                                        _ => break,
                                    };
                                    let body_applied = self.apply(body);
                                    let Some(witness_var) = vars.first().copied() else { break };
                                    let Ty::Fun(_, rest) = &body_applied else { break };
                                    let Some(ty_ast) = first_ty.take() else { break };
                                    let arg_ty = self.ast_type_to_ty(&ty_ast);
                                    self.type_arg_spans.insert(ty_ast.span);
                                    let mut mapping: HashMap<TyVar, Ty> = HashMap::new();
                                    mapping.insert(witness_var, arg_ty);
                                    let mut result = self.subst_ty(rest, &mapping);
                                    if vars.len() > 1 {
                                        result = Ty::Forall(vars[1..].to_vec(), Box::new(result));
                                    }
                                    cur_ty = result;
                                    // If the result is still a witness Forall
                                    // and a pending arg is a type, consume it
                                    // as the next type argument.
                                    if matches!(self.apply(&cur_ty), Ty::Forall(..))
                                        && !pending.is_empty()
                                    {
                                        let next_flat = flatten_spine(pending[0]);
                                        if let Some((next_ty, next_consumed)) =
                                            self.consume_type_arg(&next_flat)
                                        {
                                            first_ty = Some(next_ty);
                                            pending = next_flat
                                                .into_iter()
                                                .skip(next_consumed)
                                                .chain(pending.into_iter().skip(1))
                                                .collect();
                                            continue;
                                        }
                                    }
                                    break;
                                }
                                let mut result = self.apply(&cur_ty);
                                // Re-apply remaining value args left-to-right.
                                for a in pending {
                                    let a_ty = self.infer_expr(a);
                                    let res = self.fresh();
                                    let expected = Ty::Fun(Box::new(a_ty), Box::new(res.clone()));
                                    self.unify(&result, &expected, a.span);
                                    result = self.apply(&res);
                                }
                                let _ = type_span;
                                return result;
                            }
                        }
                    }

                    let arg_ty = self.infer_expr(arg);
                    let result_ty = self.fresh();
                    let expected = Ty::Fun(
                        Box::new(arg_ty.clone()),
                        Box::new(result_ty.clone()),
                    );
                    self.unify(&func_ty, &expected, arg.span);
                    (arg_ty, result_ty)
                };
                // Track parseJson calls for compile-time FromJSON dispatch
                if let ast::ExprKind::Var(name) = &func.node
                    && name == "parseJson"
                        && let Ty::Var(v) = &result_ty {
                            self.from_json_calls.push((expr.span, *v));
                        }

                // Track `show` calls so the argument's unit can be resolved
                // once inference finishes and handed to codegen — the unit is
                // erased before runtime, so this is the only chance to capture
                // it. Recorded unresolved: `show (a * b)` may not know its unit
                // until a later constraint solves the operands' unit vars.
                if let ast::ExprKind::Var(name) = &func.node
                    && name == "show" {
                        self.show_calls.push((expr.span, arg_ty.clone()));
                    }

                // Track full `traverse f rel` applications: the resolved
                // result type names the applicative, which codegen passes to
                // the runtime to pick the right `pure []` for empty inputs.
                if let ast::ExprKind::App { func: inner_f, .. } = &func.node
                    && matches!(&inner_f.node, ast::ExprKind::Var(n) if n == "traverse")
                        && let Ty::Var(res_v) = &result_ty {
                            let cont_v = self.fresh_var();
                            self.unify(&arg_ty, &Ty::Var(cont_v), arg.span);
                            self.traverse_calls.push((expr.span, *res_v, cont_v));
                        }

                // Track full `sum rel` applications: the resolved result type
                // says whether this is a Float sum, which codegen hands to the
                // runtime to pick the zero for an EMPTY relation (no summand
                // there to infer the numeric type from).
                if let ast::ExprKind::Var(n) = &func.node
                    && n == "sum"
                        && let Ty::Var(res_v) = &result_ty {
                            self.sum_calls.push((expr.span, *res_v));
                        }

                // Track `elem needle haystack` haystack types for SQL pushdown.
                // Curried: outer App's func is `App(Var("elem"), needle)`,
                // outer App's arg is the haystack. Record only when the
                // haystack's element type is a SQL-pushable scalar.
                if let ast::ExprKind::App { func: inner_f, .. } = &func.node
                    && let ast::ExprKind::Var(name) = &inner_f.node
                        && name == "elem" {
                            let resolved = self.apply(&arg_ty);
                            if self.is_elem_haystack_pushable(&resolved) {
                                self.elem_pushdown_ok.literal.insert(arg.span);
                                if self.is_elem_haystack_dynamic_pushable(&resolved) {
                                    self.elem_pushdown_ok.dynamic.insert(arg.span);
                                }
                            }
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
                        self.require_trait("Num", &operand_ty, operand.span);
                        self.degrade_refinement(operand_ty, operand.span)
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
                // Unify each branch against a fresh result var (the same
                // shape `case` uses) rather than unifying the branches
                // directly: when both branches are *concrete* `Ty::IO`s
                // with different closed effect sets (e.g. `*a` vs `*b`),
                // the var-rooted merge path in `unify` widens the result
                // to the union instead of rejecting the mismatch.
                let result_ty = self.fresh();
                let then_ty = self.infer_expr(then_branch);
                self.unify(&then_ty, &result_ty, then_branch.span);
                let else_ty = self.infer_expr(else_branch);
                self.unify(&else_ty, &result_ty, else_branch.span);
                // Merge IO effects from both branches — unify only checks
                // inner types and discards effect sets.
                let applied_then = self.apply(&then_ty);
                let applied_else = self.apply(&else_ty);
                match (&applied_then, &applied_else) {
                    (Ty::IO(e1, r1, inner), Ty::IO(e2, r2, _)) => {
                        let mut merged = e1.clone();
                        merged.extend(e2.iter().cloned());
                        // Both branches' effect-row tails must survive into the
                        // result. When both are still-open *distinct* row
                        // variables, `r1.or(r2)` would silently drop one, so
                        // effects later flowing into the dropped tail would
                        // vanish from the if-expression's type. Merge the two
                        // tails the same way a sequenced do-block does: a direct
                        // `unify` of two *rigid* signature skolems (as in a
                        // declared `IO {| r1 \/ r2}` union) fails with "cannot
                        // unify rigid type variables"; `merge_do_io_row` instead
                        // records a pending `\/` effect-union constraint (and
                        // still falls back to `unify` for the flexible cases).
                        let row = match (*r1, *r2) {
                            (Some(a), Some(b)) if a != b => {
                                let mut merged_row = Some(a);
                                self.merge_do_io_row(&mut merged_row, b, expr.span);
                                merged_row
                            }
                            (a, b) => a.or(b),
                        };
                        Ty::IO(merged, row, inner.clone())
                    }
                    // When one branch is IO and the other Relation, prefer IO.
                    // This handles functions whose IO nature wasn't detected
                    // due to declaration ordering (callee inferred after caller).
                    (Ty::IO(e, r, inner), Ty::Relation(_))
                    | (Ty::Relation(_), Ty::IO(e, r, inner)) => {
                        Ty::IO(e.clone(), *r, inner.clone())
                    }
                    _ => result_ty,
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
                    // Provided/actual side first (same shape `if` uses):
                    // the accumulator var plays the *required* role so the
                    // directional effect-widening path treats it as an
                    // accumulator rather than a closed expectation.
                    self.unify(&body_ty, &result_ty, arm.body.span);
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
                // mismatches highlight just the offending element. Every row
                // written is validated at runtime
                // (`knot_refinement_validate_relation`), so a raw base value
                // may flow into a refined field anywhere in this value — even
                // through plumbing like `union rows [newRow]`. Suppress the
                // refined-introduction check, but ONLY for refinements that the
                // source itself carries (those the runtime validates), not for
                // refined function parameters encountered inside the value.
                let source_refined = self.refined_names_in(&target_inner);
                let prev_suppress =
                    self.suppress_refine_intro.replace(source_refined);
                self.check_expr(value, &target_inner);
                self.suppress_refine_intro = prev_suppress;
                let mut effects = BTreeSet::new();
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    effects.insert(IoEffect::Writes(name.clone()));

                    // `set` is a read-modify-write only when the value actually
                    // reads the source. Relations require that reference (it's
                    // enforced below), so a valid relation `set` genuinely
                    // reads. But a scalar `*counter = 5` that references nothing
                    // reads nothing, so it must NOT carry a spurious `r *rel` —
                    // that would force an honest `{w *rel}` signature to widen
                    // to `{rw *rel}` (the same defect the `ReplaceSet` arm's
                    // comment below documents fixing there).
                    let references = value_references_source(
                        value,
                        name,
                        &self.source_var_binds,
                        &self.let_bindings,
                    );
                    if references {
                        effects.insert(IoEffect::Reads(name.clone()));
                    }

                    // Require `replace *rel = ...` when the value is a full
                    // replacement (doesn't reference *rel directly or via a
                    // local alias `xs <- *rel`). Skip views and scalar
                    // sources where the distinction is meaningless.
                    let is_view = self.view_names.contains(name);
                    let is_relation = matches!(
                        self.source_types.get(name),
                        Some(Ty::Relation(_))
                    );
                    if !is_view && is_relation && !references {
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
                // See the `Set` arm: writes are runtime-validated, so suppress
                // the refined-introduction check for this structural unify —
                // but only for the source's own refinements, not refined
                // function parameters used inside the value.
                let source_refined = self.refined_names_in(&target_inner);
                let prev_suppress =
                    self.suppress_refine_intro.replace(source_refined);
                self.check_expr(value, &target_inner);
                self.suppress_refine_intro = prev_suppress;
                let mut effects = BTreeSet::new();
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    // `replace *rel = v` blindly overwrites the relation — it
                    // does NOT read the existing contents (indeed, referencing
                    // `*rel` in the value is rejected below in favor of `set`).
                    // So it carries only a write effect, never a read; emitting
                    // a spurious `r *rel` here forced honest `{w *rel}`
                    // signatures to be widened to `{rw *rel}`.
                    effects.insert(IoEffect::Writes(name.clone()));

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

            // `2 seconds` is inference-identical to its desugared `2 * 1000`;
            // infer the wrapped multiplication directly.
            ast::ExprKind::TimeUnitLit { value, .. } => self.infer_expr(value),

            ast::ExprKind::Annot { expr: inner, ty } => {
                let inner_ty = self.infer_expr(inner);
                // Treat lowercase unit names in an inline ascription as
                // polymorphic unit variables (as in a signature), not as
                // concrete units — otherwise `(x : Float u)` would pin `u`
                // to a unit literally named `u` and reject valid code. Isolate
                // the unit-var map so these fresh vars don't collide with any
                // signature's unit vars.
                let saved_flag = self.in_type_annotation;
                let saved_unit_vars = std::mem::take(&mut self.annotation_unit_vars);
                self.in_type_annotation = true;
                let annot_ty = self.ast_type_to_ty(ty);
                self.in_type_annotation = saved_flag;
                self.annotation_unit_vars = saved_unit_vars;
                // An inline `forall` ascription in infer mode must not coerce a
                // monomorphic value to a polymorphic type: `(h : forall a. a -> a)`
                // where `h` is an unannotated lambda param would otherwise unify
                // the skolemised body against `h`'s flexible var, bind it toward
                // the skolems, and — the skolems being dropped right after —
                // generalise `g`'s inferred type into a lie (`g` usable at any
                // argument type). Skolemise the quantified vars, unify the inner
                // type against the skolemised body, then require the skolems to
                // stay out of the enclosing environment — mirroring the escape
                // check `check_expr` performs for an expected `forall`.
                if let Ty::Forall(vars, body) = self.apply(&annot_ty) {
                    let (skolemised, fresh_skolems) =
                        self.skolemise_forall_body(&vars, &body);
                    self.unify(&inner_ty, &skolemised, inner.span);
                    let env_vars = self.free_vars_in_env();
                    if fresh_skolems.iter().any(|s| env_vars.contains(s)) {
                        self.error(
                            "polymorphic type escapes its scope: this expression \
                             must work for every type, but its type leaked into \
                             the surrounding context — an inline `forall` \
                             annotation cannot make a monomorphic value \
                             polymorphic"
                                .into(),
                            expr.span,
                        );
                    }
                    for s in fresh_skolems {
                        self.skolems.remove(&s);
                    }
                } else {
                    self.unify(&inner_ty, &annot_ty, inner.span);
                }
                annot_ty
            }

            ast::ExprKind::Refine(inner) => {
                let inner_ty = self.infer_expr(inner);
                let alpha = self.fresh();
                // Deliberately do NOT unify alpha with inner_ty here: alpha
                // must stay free so the *context* can name the refined type
                // (e.g. a `Result RefinementError Nat` annotation binds
                // alpha to `Nat`). Eagerly unifying would collapse alpha to
                // the base type and lose the contextual target. Phase 6
                // (post-inference) resolves alpha — using the contextual
                // binding when present, falling back to a deterministic
                // base-type lookup otherwise — and checks the refined
                // value's type against the target's base type.
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

            ast::ExprKind::Serve { api, api_span, handlers } => {
                self.infer_serve(api, *api_span, handlers, expr.span)
            }

            ast::ExprKind::TypeCtor { name: _, params, .. } => {
                // A first-class (erased) type-constructor value from an
                // embedded `type` alias line. Statically its type is the alias's
                // KIND: `Type` (0 params), `Type -> Type` (1 param), …, one
                // `Type ->` per type parameter, ending in `Type`. The "Type"
                // here is the same opaque named type knot already accepts in
                // signatures like `f : Type -> Type` (i.e. `Ty::Con("Type", [])`).
                //
                // CONFINEMENT: nothing is registered into the global `aliases`
                // map. The alias name is reachable only via the record value
                // (`rec.Name`) or a `with` peel (scoped type env), so it never
                // leaks into the enclosing type namespace.
                let kind_type =
                    (0..params.len()).fold(Ty::Con("Type".into(), vec![]), |acc, _| {
                        Ty::Fun(
                            Box::new(Ty::Con("Type".into(), vec![])),
                            Box::new(acc),
                        )
                    });
                kind_type
            }

            ast::ExprKind::DataCtor { name, params, constructors } => {
                // A first-class (erased) `data` declaration embedded in a
                // record value literal. The field is fully erased at runtime
                // (compiles to unit), but statically its type is a RECORD of
                // constructor functions `{Ctor: payload -> Name, …}` so that
                // `rec.Name.Ctor` resolves via ordinary structural field
                // access.
                //
                // CONFINEMENT: unlike a top-level `data` decl, this registers
                // NOTHING into the global `constructors`/`data_types` maps.
                // The type `Name` and its constructors are reachable ONLY
                // through the record value (`rec.Name.Ctor`) or a `with` peel
                // (which pushes them into the scoped type env for the body).
                // The namespace record is built directly from the AST decl,
                // exactly as `instantiate_ctor` would, but self-contained.
                //
                // Freshen the type params (each use site gets its own vars).
                let saved_annotation_vars = self.annotation_vars.clone();
                self.annotation_vars.clear();
                let param_tys: Vec<Ty> = params
                    .iter()
                    .map(|p| {
                        let v = self.fresh_var();
                        self.annotation_vars.insert(p.clone(), v);
                        Ty::Var(v)
                    })
                    .collect();
                let data_ty = Ty::Con(name.clone(), param_tys);

                // Build the namespace record: each ctor maps to its function
                // type `payload -> data_ty` — including nullary ctors, which
                // keep the `{} -> data_ty` form because the applied syntax is
                // always `rec.Name.Ctor {}` (a record application), matching
                // how `Ctor {}` is typed through the App arm.
                let mut ns_fields = BTreeMap::new();
                for ctor in constructors {
                    let field_tys: BTreeMap<String, Ty> = ctor
                        .fields
                        .iter()
                        .map(|f| (f.name.clone(), self.ast_type_to_ty(&f.value)))
                        .collect();
                    let record_ty = Ty::Record(field_tys, None);
                    ns_fields.insert(
                        ctor.name.clone(),
                        Ty::Fun(Box::new(record_ty), Box::new(data_ty.clone())),
                    );
                }
                self.annotation_vars = saved_annotation_vars;
                Ty::Record(ns_fields, None)
            }

            ast::ExprKind::SourceDecl { name, ty, .. } => {
                // A persisted source-relation declaration embedded in a record
                // value literal (`{*todos : [Todo], …}`). Register the source
                // (by its bare field name — qualified-path registration is a
                // follow-up) so it participates in schema/migrations, and give
                // the field the type of a source READ (`IO {Reads name} [T]`)
                // so `db.*todos` resolves through ordinary field access.
                self.annotation_vars.clear();
                let resolved = self.ast_type_to_ty(ty);
                self.source_types.insert(name.clone(), resolved.clone());
                let mut effects = BTreeSet::new();
                effects.insert(IoEffect::Reads(name.clone()));
                Ty::IO(effects, None, Box::new(resolved))
            }
            ast::ExprKind::SubsetConstraint { .. } => {
                // A record-embedded subset constraint is a pure static marker
                // (registered via `TypeEnv::subset_constraints`); the field
                // has no meaningful value.
                Ty::unit()
            }
            ast::ExprKind::RouteDecl { name, entries } => {
                // A record-embedded route declaration. Like an embedded `data`
                // decl, its static type is a structural namespace record
                // `{Ctor: payload -> RouteTy, …}` so `rec.Api.Ctor` resolves via
                // ordinary field access. The endpoint type is the path-qualified
                // `Ty::Con("rec.Api")` produced by the hoisted `DeclKind::Route`
                // (desugar `hoist_record_routes`); the record value itself is
                // erased to unit at runtime.
                let mut ns_fields = BTreeMap::new();
                for entry in entries {
                    let input_ty = self.route_input_record_ty(entry);
                    ns_fields.insert(entry.constructor.clone(), Ty::Fun(Box::new(input_ty), Box::new(Ty::Con(name.clone(), vec![]))));
                }
                Ty::Record(ns_fields, None)
            }
            ast::ExprKind::RouteCompositeDecl { .. } => {
                // A route composite contributes no constructors of its own; it
                // merges other routes' endpoints. It carries no value namespace.
                Ty::unit()
            }
            ast::ExprKind::ViewDecl { name, ty, .. } => {
                // A view embedded in a record value literal (`{*openTodos = …}`).
                // The actual relation type is registered by the hoisted
                // top-level `DeclKind::View` (desugar `hoist_record_views`);
                // here the field reads through it, so type it as a view READ
                // (`IO {Reads name} [T]`). With an annotation use it, else a
                // fresh var the hoisted decl's check will pin down.
                self.annotation_vars.clear();
                let resolved = match ty {
                    Some(scheme) => self.ast_type_to_ty(&scheme.ty),
                    None => self.source_types.get(name).cloned().unwrap_or_else(|| self.fresh()),
                };
                self.source_types.insert(name.clone(), resolved.clone());
                self.view_names.insert(name.clone());
                let mut effects = BTreeSet::new();
                effects.insert(IoEffect::Reads(name.clone()));
                Ty::IO(effects, None, Box::new(resolved))
            }
            ast::ExprKind::DerivedDecl { name, ty, .. } => {
                // A derived relation embedded in a record value literal
                // (`{&seniors = …}`). The relation type is registered by the
                // hoisted top-level `DeclKind::Derived` (desugar
                // `hoist_record_views`); the field reads through it. Derived
                // reads aren't known at this site (mirrors `DerivedRef`) — the
                // effect-checker pass tracks them, so effects start empty.
                let resolved = match ty {
                    Some(scheme) => {
                        self.annotation_vars.clear();
                        self.ast_type_to_ty(&scheme.ty)
                    }
                    None => self.derived_types.get(name).cloned().unwrap_or_else(|| self.fresh()),
                };
                self.derived_types.insert(name.clone(), resolved.clone());
                Ty::IO(BTreeSet::new(), None, Box::new(resolved))
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
            // Escape check: a skolem must not leak into the enclosing
            // environment. Without this, an outer flexible var (e.g. the
            // type of an unannotated lambda param `h` in
            // `g = \h -> takesPoly h`) can be bound toward a skolem; once
            // the skolem is dropped from `self.skolems` it becomes an
            // ordinary var, gets generalized, and the wrapper accepts
            // monomorphic arguments where a polymorphic one was required.
            let env_vars = self.free_vars_in_env();
            if fresh_skolems.iter().any(|s| env_vars.contains(s)) {
                self.error(
                    "polymorphic type escapes its scope: this expression \
                     must work for every type, but its type leaked into \
                     the surrounding context — add an explicit `forall` \
                     annotation to keep the wrapper polymorphic"
                        .into(),
                    expr.span,
                );
            }
            for s in fresh_skolems {
                self.skolems.remove(&s);
            }
            return;
        }
        match &expr.node {
            ast::ExprKind::ImplicitRef(name) => {
                // `^name` — resolve against the EXPECTED type directly so a
                // concrete expectation (e.g. `println ^size` wanting `Text`)
                // disambiguates between same-named fields of different types.
                // `resolve_implicit_ref` already unifies the chosen field's
                // type with `expected`.
                let name = name.clone();
                self.resolve_implicit_ref(&name, expected, expr.span);
            }
            ast::ExprKind::Annot { expr: inner, ty } => {
                // See the infer-mode `Annot` arm: lowercase units in an inline
                // ascription must be polymorphic unit variables, not concrete.
                let saved_flag = self.in_type_annotation;
                let saved_unit_vars = std::mem::take(&mut self.annotation_unit_vars);
                self.in_type_annotation = true;
                let annot_ty = self.ast_type_to_ty(ty);
                self.in_type_annotation = saved_flag;
                self.annotation_unit_vars = saved_unit_vars;
                self.check_expr(inner, &annot_ty);
                self.unify(&annot_ty, expected, ty.span);
            }
            ast::ExprKind::Lambda { params, ty_params, body } => {
                // Lambdas with type-witness params have an inherent
                // `Fun(Con("Type"), …)` shape that `expected` may not supply
                // (e.g. a bare fresh Var for an un-annotated top-level def).
                // Synthesize via infer-mode (which builds the witness arrows)
                // and unify, rather than peeling.
                if !ty_params.is_empty() {
                    let inferred = self.infer_expr(expr);
                    // If `expected` is a bare unification var (an un-annotated
                    // definition like `apply = \(T : Type) -> …`), bind it
                    // directly to the inferred `∀ t. Type -> …` type. Routing
                    // through `unify_dir` would *instantiate* the Forall
                    // (provided side) and strip the quantifier, losing the
                    // witness binding the caller needs to supply the type arg.
                    if let Ty::Var(v) = self.apply(expected)
                        && self.subst.get(&v).is_none()
                    {
                        self.subst.insert(v, inferred);
                        return;
                    }
                    self.unify_dir(expected, &inferred, expr.span, false);
                    return;
                }
                // Peel `Fun(p, r)` off `expected` for each lambda param,
                // resolving substitutions as we go. If the expected type
                // turns out to have fewer arrows than the lambda has
                // params, fall back to synthesise + unify (mono).
                let mut current = self.apply(expected);
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
                    // `expected` is on the required side here (t1), so pass
                    // t1_provided=false for correct Forall polarity.
                    self.unify_dir(expected, &inferred, expr.span, false);
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
                self.unify_dir(expected, &inferred, do_result_span(stmts, expr.span), false);
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
                        // A field with a sig line is checked against its sig
                        // first; the sig type then stands as the field's type.
                        if let Some(sig) = &f.sig {
                            let saved_flag = self.in_type_annotation;
                            let saved_unit_vars =
                                std::mem::take(&mut self.annotation_unit_vars);
                            self.in_type_annotation = true;
                            let sig_ty = self.ast_type_to_ty(&sig.ty);
                            self.in_type_annotation = saved_flag;
                            self.annotation_unit_vars = saved_unit_vars;
                            self.check_expr(&f.value, &sig_ty);
                            field_tys.insert(f.name.clone(), sig_ty);
                        } else if let Some(exp_ty) = expected_fields.get(&f.name) {
                            self.check_expr(&f.value, exp_ty);
                            field_tys.insert(f.name.clone(), exp_ty.clone());
                        } else {
                            let val_ty = self.infer_expr(&f.value);
                            field_tys.insert(f.name.clone(), val_ty);
                        }
                    }
                    self.unify_dir(expected, &Ty::Record(field_tys, None), expr.span, false);
                } else {
                    let inferred = self.infer_expr(expr);
                    // `expected` is on the required side here (t1), so pass
                    // t1_provided=false for correct Forall polarity.
                    self.unify_dir(expected, &inferred, expr.span, false);
                }
            }
            ast::ExprKind::If { cond, then_branch, else_branch } => {
                // Push expected into both branches so a mismatch lights up
                // just the offending branch instead of the whole if.
                let cond_ty = self.infer_expr(cond);
                self.unify(&cond_ty, &Ty::Bool, cond.span);
                let then_ty = self.infer_expr(then_branch);
                self.unify_dir(expected, &then_ty, then_branch.span, false);
                let else_ty = self.infer_expr(else_branch);
                self.unify_dir(expected, &else_ty, else_branch.span, false);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                // Push expected into each arm body so a mismatch lights up
                // just the offending arm instead of the whole case.
                let scrut_ty = self.infer_expr(scrutinee);
                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify_dir(expected, &body_ty, arm.body.span, false);
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
                    // `expected` is on the required side here (t1), so pass
                    // t1_provided=false for correct Forall polarity.
                    self.unify_dir(expected, &inferred, expr.span, false);
                }
            }
            _ => {
                let inferred = self.infer_expr(expr);
                // Π-lite: an unannotated binding whose body infers to a
                // type-witness `Forall` (e.g. `step1 = const2 Int`, the
                // partial application of a `\(T : Type)` lambda) must keep
                // that `Forall` so later uses can still supply the remaining
                // type argument. Unifying against the unsolved fresh `expected`
                // var would *instantiate* the Forall and strip it, degrading
                // `step1` to `Type -> …` and breaking `step1 Text 99`. Bind the
                // var directly to the Forall instead — the standard
                // generalization of a principal (Forall) type.
                if let Ty::Forall(..) = &inferred
                    && let Ty::Var(v) = self.apply(expected)
                    && !self.subst.contains_key(&v)
                    && !self.skolems.contains(&v)
                {
                    self.subst.insert(v, inferred);
                    return;
                }
                // `expected` is on the required side here (t1), so pass
                // t1_provided=false for correct Forall polarity.
                self.unify_dir(expected, &inferred, expr.span, false);
            }
        }
    }

    /// Type-check a `serve Api where ...` expression, returning `Server Api _`
    /// (a row-variable tail when handlers have no concrete effects) or
    /// `Server Api {effects}` when handlers carry concrete effects.
    /// Internally the type is `Ty::Con("Server", [api, effect_row])` where
    /// the effect row collects every handler's IO effects and shares a row
    /// variable with `listen` for propagation.
    ///
    /// Each handler is checked against the type derived from its route entry:
    ///   - input: a record of (path params, query params, body fields,
    ///     request headers)
    ///   - output: the entry's declared response type, or
    ///     `{body: ResponseTy, headers: {h: T, ...}}` when response headers
    ///     are declared. The handler may also return `IO {effects} <output>`.
    ///
    /// Exhaustiveness and uniqueness are enforced: every constructor of the
    /// route ADT must be handled exactly once.
    fn infer_serve(
        &mut self,
        api: &str,
        api_span: Span,
        handlers: &[ast::ServeHandler],
        span: Span,
    ) -> Ty {
        // Each handler gets its own fresh row variable so the body's
        // (possibly closed) IO effects can bind it without constraining
        // sibling handlers. After checking, we resolve each row var,
        // accumulate the effects into a single set, and chain any leftover
        // open tails. A single shared row would force all handlers'
        // effects to be equal: the first handler that closes the row
        // (which any concrete IO body does) would reject every subsequent
        // handler with different effects.
        let mut accumulated: BTreeSet<IoEffect> = BTreeSet::new();
        let mut tail: Option<TyVar> = None;
        let entries = match self.route_entries_by_api.get(api).cloned() {
            Some(e) => e,
            None => {
                self.error(format!("'{}' is not a route type", api), api_span);
                // Still infer handlers so other diagnostics surface, then
                // return a fresh Server type.
                for h in handlers {
                    let _ = self.infer_expr(&h.body);
                }
                let a = self.fresh_var();
                let r = self.fresh_var();
                return Ty::Con(
                    "Server".into(),
                    vec![
                        Ty::Var(a),
                        Ty::EffectRow(BTreeSet::new(), Some(r)),
                    ],
                );
            }
        };

        let entry_by_ctor: std::collections::HashMap<String, ast::RouteEntry> = entries
            .iter()
            .cloned()
            .map(|e| (e.constructor.clone(), e))
            .collect();
        let mut seen: HashSet<String> = HashSet::new();

        for h in handlers {
            if !seen.insert(h.endpoint.clone()) {
                self.error(
                    format!(
                        "duplicate handler for endpoint '{}' in serve {}",
                        h.endpoint, api
                    ),
                    h.endpoint_span,
                );
                let _ = self.infer_expr(&h.body);
                continue;
            }
            let entry = match entry_by_ctor.get(&h.endpoint) {
                Some(e) => e,
                None => {
                    self.error(
                        format!(
                            "'{}' is not an endpoint of route {}",
                            h.endpoint, api
                        ),
                        h.endpoint_span,
                    );
                    let _ = self.infer_expr(&h.body);
                    continue;
                }
            };
            let handler_row = self.fresh_var();
            let expected = self.serve_handler_type(entry, handler_row);
            self.check_expr(&h.body, &expected);
            // Pull this handler's effects out of its row var and merge.
            // Any unresolved tail unifies into the rolling tail var so
            // later listen-side polymorphism still threads through.
            let (effects, leftover) =
                self.resolve_effect_row(BTreeSet::new(), Some(handler_row));
            accumulated.extend(effects);
            if let Some(rv) = leftover {
                match tail {
                    Some(existing) => {
                        self.unify(&Ty::Var(existing), &Ty::Var(rv), h.endpoint_span);
                    }
                    None => tail = Some(rv),
                }
            }
        }

        // Missing handlers
        for entry in &entries {
            if !seen.contains(&entry.constructor) {
                self.error(
                    format!(
                        "missing handler for endpoint '{}' in serve {}",
                        entry.constructor, api
                    ),
                    span,
                );
            }
        }

        Ty::Con(
            "Server".into(),
            vec![
                Ty::Con(api.to_string(), vec![]),
                Ty::EffectRow(accumulated, tail),
            ],
        )
    }

    /// Build the request-input record type for a route entry. Same record
    /// the handler receives (path params + query params + body fields +
    /// request headers) and the rate-limit `key` function's first argument.
    fn route_input_record_ty(&mut self, entry: &ast::RouteEntry) -> Ty {
        let mut input_fields: BTreeMap<String, Ty> = BTreeMap::new();
        for seg in &entry.path {
            if let ast::PathSegment::Param { name, ty } = seg {
                input_fields.insert(name.clone(), self.ast_type_to_ty(ty));
            }
        }
        for qp in &entry.query_params {
            input_fields.insert(qp.name.clone(), self.ast_type_to_ty(&qp.value));
        }
        for bf in &entry.body_fields {
            input_fields.insert(bf.name.clone(), self.ast_type_to_ty(&bf.value));
        }
        for hf in &entry.request_headers {
            input_fields.insert(hf.name.clone(), self.ast_type_to_ty(&hf.value));
        }
        Ty::Record(input_fields, None)
    }

    /// Build the expected type of a single endpoint handler.
    /// Input is a record of all request fields (path params, query params,
    /// body fields, request headers). Output is the declared response type
    /// wrapped in `IO {| r} _` where `r` is the per-handler row variable
    /// `infer_serve` allocates — its effects are extracted post-check and
    /// unioned into the resulting `Server`'s effect row.
    fn serve_handler_type(&mut self, entry: &ast::RouteEntry, handler_row: TyVar) -> Ty {
        let input = self.route_input_record_ty(entry);

        let response = match &entry.response_ty {
            Some(resp_ty) => {
                let resp = self.ast_type_to_ty(resp_ty);
                if entry.response_headers.is_empty() {
                    resp
                } else {
                    let hdrs = entry
                        .response_headers
                        .iter()
                        .map(|f| (f.name.clone(), self.ast_type_to_ty(&f.value)))
                        .collect();
                    Ty::Record(
                        BTreeMap::from([
                            ("body".into(), resp),
                            ("headers".into(), Ty::Record(hdrs, None)),
                        ]),
                        None,
                    )
                }
            }
            None => Ty::Var(self.fresh_var()),
        };
        // Handlers return `Result HttpError T` so they can pick custom
        // HTTP status codes via Err {error: {status, message}}. Use the
        // record-typed alias (not Ty::Con) so .status / .message access
        // works inside handler bodies.
        let http_error = self
            .aliases
            .get("HttpError")
            .cloned()
            .unwrap_or_else(|| Ty::Con("HttpError".into(), vec![]));
        let wrapped = Ty::Con("Result".into(), vec![http_error, response]);
        let output = Ty::IO(BTreeSet::new(), Some(handler_row), Box::new(wrapped));
        Ty::Fun(Box::new(input), Box::new(output))
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

        // The constructor must come from a `route` declaration — route
        // metadata drives URL construction and response typing, and
        // codegen has no entry to emit for a plain ADT constructor (it
        // would panic at `compile_fetch`). Reject here with a proper
        // diagnostic instead.
        let is_route_ctor = self
            .route_entries_by_api
            .values()
            .flat_map(|entries| entries.iter())
            .any(|e| e.constructor == ctor_name);
        if !is_route_ctor {
            self.error(
                format!(
                    "'{}' is not a route constructor — fetch/fetchWith \
                     require an endpoint constructor declared in a \
                     `route` block",
                    ctor_name
                ),
                expr.span,
            );
            return Some(Ty::Error);
        }

        // Infer URL argument (should be Text)
        let url_ty = self.infer_expr(args[0]);
        self.unify(&url_ty, &Ty::Text, args[0].span);

        // If fetchWith, check the options record. The shape must match
        // what codegen + the runtime consume: `compile_fetch` reads the
        // `headers` field with `knot_record_field` and
        // `knot_http_fetch_io` iterates it as rows of {name, value}
        // Text pairs — anything else compiles but panics at runtime.
        if is_fetch_with {
            let opts_ty = self.infer_expr(args[1]);
            let header_row = Ty::Record(
                BTreeMap::from([
                    ("name".into(), Ty::Text),
                    ("value".into(), Ty::Text),
                ]),
                None,
            );
            let expected_opts = Ty::Record(
                BTreeMap::from([(
                    "headers".into(),
                    Ty::Relation(Box::new(header_row)),
                )]),
                None,
            );
            self.unify(&opts_ty, &expected_opts, args[1].span);
        }

        // Infer the constructor's record payload (request fields only).
        // A bare nullary route constructor (`fetch url Ctor`) carries no
        // record argument — inferring the Constructor node as an expression
        // yields the ADT type, which would spuriously fail to unify against
        // the (empty) expected record. Skip payload unification for it; the
        // response type below is resolved from route metadata regardless.
        let ctor_arg = args.last().unwrap();
        let record_arg = match &ctor_arg.node {
            ast::ExprKind::App { arg, .. } => Some(arg.as_ref()),
            ast::ExprKind::Constructor(_) => None,
            _ => Some(*ctor_arg),
        };
        let record_ty = record_arg.map(|r| self.infer_expr(r));

        // Build the expected request fields from the route entry. Save and
        // restore annotation_vars so fetch inference doesn't corrupt the
        // enclosing declaration's type variable mapping.
        let saved_annotation_vars = self.annotation_vars.clone();
        if let Some(info) = self.constructors.get(ctor_name).and_then(|v| v.last()).cloned() {
            self.annotation_vars.clear();
            for p in &info.data_params {
                let v = self.fresh_var();
                self.annotation_vars.insert(p.clone(), v);
            }
            if let Some(record_ty) = &record_ty {
                let field_tys: BTreeMap<String, Ty> = info
                    .fields
                    .iter()
                    .map(|(name, ty)| (name.clone(), self.ast_type_to_ty(ty)))
                    .collect();
                let expected_record = Ty::Record(field_tys, None);
                self.unify(record_ty, &expected_record, ctor_arg.span);
            }
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

    /// Unify two operand types of a *symmetric* context — a binary operator or
    /// a literal pattern — where relating two existing values must not be
    /// treated as unsafely *introducing* a refined type. The directional
    /// refined-introduction guard in `unify_dir` exists for unchecked
    /// boundaries (function/assignment), where a bare `Int` claiming to be a
    /// `Nat` would skip the predicate. A binop does no such thing: neither
    /// operand is coerced into the other's refined type, and any refined result
    /// is degraded to its base via `degrade_refinement`. Without this, the
    /// guard fired asymmetrically — `n == 5` compiled but `5 == n` did not,
    /// depending only on which operand sat on the "required" side.
    ///
    /// Only the base↔refined introduction error is suppressed; two *different*
    /// refined types (`Nat` vs `Pos`) still fail to unify (handled by a
    /// separate guard in `unify_dir`), preserving nominal refinement.
    fn unify_symmetric(&mut self, t1: &Ty, t2: &Ty, span: Span) {
        let mut refined = match &self.suppress_refine_intro {
            Some(existing) => existing.clone(),
            None => HashSet::new(),
        };
        refined.extend(self.refined_names_in(t1));
        refined.extend(self.refined_names_in(t2));
        let prev = self.suppress_refine_intro.replace(refined);
        self.unify(t1, t2, span);
        self.suppress_refine_intro = prev;
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
            // Add/Sub/Mod: units must match, result has same unit
            ast::BinOp::Add | ast::BinOp::Sub | ast::BinOp::Mod => {
                let lhs_applied = self.apply(&lhs_ty);
                let rhs_applied = self.apply(&rhs_ty);
                // For unit-bearing types, unify normally (which checks unit
                // equality). Symmetric so `1 + n` and `n + 1` (n : Nat) agree.
                self.unify_symmetric(&lhs_applied, &rhs_applied, span);
                self.require_trait("Num", &lhs_applied, span);
                // Plain Int/Float are unit-agnostic, so `unitless + unit`
                // unifies without binding either side. Return the more
                // specific (unit-bearing) operand so the unit isn't
                // silently stripped when it appears on the RHS.
                let lhs_final = self.apply(&lhs_applied);
                let rhs_final = self.apply(&rhs_applied);
                let result = match (&lhs_final, &rhs_final) {
                    (l, _) if l.unit_of().is_some() => lhs_final,
                    (_, r) if r.unit_of().is_some() => rhs_final,
                    _ => lhs_final,
                };
                self.degrade_refinement(result, span)
            }
            // Mul/Div: units compose
            ast::BinOp::Mul | ast::BinOp::Div => {
                let lhs_applied = self.apply(&lhs_ty);
                let rhs_applied = self.apply(&rhs_ty);
                // Symmetric refined handling: `2 * n` and `n * 2` (n : Nat)
                // agree; the composed result is degraded to its base anyway.
                let prev = match &self.suppress_refine_intro {
                    Some(existing) => {
                        let mut r = existing.clone();
                        r.extend(self.refined_names_in(&lhs_applied));
                        r.extend(self.refined_names_in(&rhs_applied));
                        self.suppress_refine_intro.replace(r)
                    }
                    None => {
                        let mut r = self.refined_names_in(&lhs_applied);
                        r.extend(self.refined_names_in(&rhs_applied));
                        self.suppress_refine_intro.replace(r)
                    }
                };
                let result = self.unit_mul_div_ty(op, &lhs_applied, &rhs_applied, span, true);
                self.suppress_refine_intro = prev;
                self.degrade_refinement(result, span)
            }
            // Comparison: both same type, result Bool
            ast::BinOp::Eq | ast::BinOp::Neq => {
                self.unify_symmetric(&lhs_ty, &rhs_ty, span);
                Ty::Bool
            }
            ast::BinOp::Lt | ast::BinOp::Gt | ast::BinOp::Le | ast::BinOp::Ge => {
                self.unify_symmetric(&lhs_ty, &rhs_ty, span);
                Ty::Bool
            }
            // Boolean: both Bool, result Bool
            ast::BinOp::And | ast::BinOp::Or => {
                self.unify(&lhs_ty, &Ty::Bool, lhs.span);
                self.unify(&rhs_ty, &Ty::Bool, rhs.span);
                Ty::Bool
            }
            // Concat: both same type (Semigroup), result same type — but
            // degrade refinement, since `Short ++ Short` can exceed the
            // length bound (mirrors Add/Sub/Mod and Mul/Div above).
            ast::BinOp::Concat => {
                self.unify_symmetric(&lhs_ty, &rhs_ty, span);
                self.require_trait("Semigroup", &lhs_ty, span);
                self.degrade_refinement(lhs_ty, span)
            }
            // Pipe: a |> f  =  f a
            ast::BinOp::Pipe => {
                let result_ty = self.fresh();
                let fun_ty = Ty::Fun(
                    Box::new(lhs_ty.clone()),
                    Box::new(result_ty.clone()),
                );
                self.unify(&rhs_ty, &fun_ty, span);
                // `rel |> sum` reaches codegen as an application carrying
                // this pipe's span, so record it like the `sum rel` form.
                if let ast::ExprKind::Var(n) = &rhs.node
                    && n == "sum"
                        && let Ty::Var(res_v) = &result_ty {
                            self.sum_calls.push((span, *res_v));
                        }
                // `x |> show` desugars to `show x` carrying this pipe's span
                // (codegen.rs:4707-4713), so record the argument's unit like
                // the direct `show x` form — otherwise the pipe form drops the
                // unit suffix (`show (3.0 : Float M)` → "3.0 M" but
                // `(3.0 : Float M) |> show` → "3.0").
                if let ast::ExprKind::Var(name) = &rhs.node
                    && name == "show" {
                        self.show_calls.push((span, lhs_ty.clone()));
                    }
                result_ty
            }
        }
    }

    /// Result type of a `*`/`/` binop under unit composition. Both operand
    /// types must already be substitution-applied. When one side carries a
    /// concrete unit and the other is still an unresolved type variable,
    /// `allow_defer` controls the outcome: at the binop node (true) the
    /// check is deferred — the operand may resolve later, e.g. a field
    /// access on a lambda parameter whose record type is only pinned when
    /// the lambda unifies with its call site — and a fresh variable stands
    /// in for the result; at post-inference resolution (false) a still-
    /// unresolved operand is an error demanding an annotation.
    fn unit_mul_div_ty(
        &mut self,
        op: ast::BinOp,
        lhs_applied: &Ty,
        rhs_applied: &Ty,
        span: Span,
        allow_defer: bool,
    ) -> Ty {
        // Unit arithmetic uses helpers so it works for both plain and
        // unit-bearing numeric types.
        let same_numeric_class =
            |a: &Ty, b: &Ty| (a.is_int_like() && b.is_int_like()) || (a.is_float_like() && b.is_float_like());
        // Both operands have a unit and are the same numeric class → compose.
        if let (Some(u1), Some(u2)) = (lhs_applied.unit_of(), rhs_applied.unit_of()) {
            if same_numeric_class(lhs_applied, rhs_applied) {
                let u1 = self.apply_unit(u1);
                let u2 = self.apply_unit(u2);
                let result_unit = if op == ast::BinOp::Mul {
                    u1.mul(&u2)
                } else {
                    u1.div(&u2)
                };
                if result_unit.is_dimensionless() {
                    if lhs_applied.is_int_like() { return Ty::Int; } else { return Ty::Float; }
                }
                return if lhs_applied.is_int_like() {
                    Ty::int_with_unit(result_unit)
                } else {
                    Ty::float_with_unit(result_unit)
                };
            }
        }
        // One side carries a unit, the other is the plain form of the same
        // numeric class → preserve (and on `/`, invert the unit when the
        // *denominator* is the unit side).
        let one_unit: Option<(&UnitTy, bool, bool)> =
            match (lhs_applied, rhs_applied) {
                (a, b) if a.unit_of().is_some() && b.is_int_like() && a.is_int_like() && matches!(b, Ty::Int) => Some((a.unit_of().unwrap(), true,  false)),
                (a, b) if a.unit_of().is_some() && b.is_float_like() && a.is_float_like() && matches!(b, Ty::Float) => Some((a.unit_of().unwrap(), false, false)),
                (a, b) if b.unit_of().is_some() && a.is_int_like() && b.is_int_like() && matches!(a, Ty::Int) => Some((b.unit_of().unwrap(), true,  true)),
                (a, b) if b.unit_of().is_some() && a.is_float_like() && b.is_float_like() && matches!(a, Ty::Float) => Some((b.unit_of().unwrap(), false, true)),
                _ => None,
            };
        if let Some((u, is_int, rhs_has_unit)) = one_unit {
            let u = self.apply_unit(u);
            if op == ast::BinOp::Div && rhs_has_unit {
                // x / y<u> → x<1/u>
                let inv = u.pow(-1);
                if inv.is_dimensionless() {
                    return if is_int { Ty::Int } else { Ty::Float };
                }
                return if is_int { Ty::int_with_unit(inv) } else { Ty::float_with_unit(inv) };
            }
            // x<u> / y → x<u>; x<u> * y → x<u>; y * x<u> → x<u>
            if u.is_dimensionless() {
                return if is_int { Ty::Int } else { Ty::Float };
            }
            return if is_int { Ty::int_with_unit(u) } else { Ty::float_with_unit(u) };
        }
        // No units involved → default behavior
        {
            // Unit soundness: `*`/`/` *compose* units, but
            // composition is only computable when both operands'
            // units are known. If one side carries a concrete
            // unit while the other is still an unresolved type
            // variable (e.g. an unannotated lambda parameter),
            // unifying them would force both to the *same* unit
            // and type the product with that unit instead of its
            // square. Defer the check (the operand's type may be
            // pinned by a later unification), and at end of
            // inference reject conservatively rather than silently
            // inferring an unsound unit.
            // A unit is "known to be unit-bearing" when, after
            // resolving unit variables, it still has concrete
            // bases OR an unresolved unit variable. A bare unit
            // variable (e.g. the `u` in `Float u -> Float u`)
            // is just as unit-bearing as a concrete unit: typing
            // `x<u> * y` with `y` unresolved would unify `y`
            // with `x` and produce `u` where `u^2` is correct.
            let concrete_unit = |slf: &Self, t: &Ty| match t.unit_of() {
                Some(u) => {
                    let applied = slf.apply_unit(u);
                    if applied.is_dimensionless() {
                        None
                    } else {
                        Some(applied.display())
                    }
                }
                _ => None,
            };
                let lhs_is_var = matches!(lhs_applied, Ty::Var(_));
                let rhs_is_var = matches!(rhs_applied, Ty::Var(_));
                // BOTH operands unresolved: the composition can't be
                // computed yet AND unifying them would be unsound (it
                // types `w * h` as `w`'s unit instead of its square once
                // units appear, and falsely rejects `Float M * Float S`).
                // Defer the whole check and return a fresh result variable.
                // If the surrounding binding is generalized, `generalize`
                // captures this binop on the scheme so each instantiation
                // resolves its own composition (keeping `\x -> x * x`
                // unit-polymorphic); otherwise it is resolved once at
                // end-of-inference. If no units ever appear, that resolution
                // falls through to the plain `unify + Num` path below, so
                // dimensionless code is unaffected.
                if allow_defer && lhs_is_var && rhs_is_var {
                    let result = self.fresh_var();
                    self.deferred_unit_binops.push(DeferredUnitBinop {
                        op,
                        lhs: lhs_applied.clone(),
                        rhs: rhs_applied.clone(),
                        result,
                        span,
                    });
                    return Ty::Var(result);
                }
                let unit_side = if lhs_is_var {
                    concrete_unit(self, rhs_applied)
                } else if rhs_is_var {
                    concrete_unit(self, lhs_applied)
                } else {
                    None
                };
                if let Some(unit) = unit_side {
                    if allow_defer {
                        let result = self.fresh_var();
                        self.deferred_unit_binops.push(DeferredUnitBinop {
                            op,
                            lhs: lhs_applied.clone(),
                            rhs: rhs_applied.clone(),
                            result,
                            span,
                        });
                        return Ty::Var(result);
                    }
                    let op_name = if op == ast::BinOp::Mul { "*" } else { "/" };
                    self.error(
                        format!(
                            "cannot infer the unit of an operand of `{}`: one side has unit {} but the other side's type is not yet known — units compose under `{}`, so the unresolved operand needs an explicit annotation (e.g. `(x : Float ({}))`, or `(x : Float 1)` for a dimensionless value)",
                            op_name, unit, op_name, unit
                        ),
                        span,
                    );
                    return Ty::Error;
                }
                self.unify(lhs_applied, rhs_applied, span);
                self.require_trait("Num", lhs_applied, span);
                lhs_applied.clone()
        }
    }

    /// Resolve unit-composition checks deferred at `*`/`/` nodes. Runs after
    /// all declaration bodies are inferred, when an operand that was a bare
    /// type variable at the binop (e.g. a record field on a lambda param)
    /// may have been pinned to a concrete type. Re-running the composition
    /// with `allow_defer = false` either computes the result type — unified
    /// with the placeholder variable the binop returned — or emits the
    /// annotation-demanding error for operands that never resolved.
    fn resolve_deferred_unit_binops(&mut self) {
        let deferred = std::mem::take(&mut self.deferred_unit_binops);
        for d in &deferred {
            let lhs = self.apply(&d.lhs);
            let rhs = self.apply(&d.rhs);
            let result_ty = self.unit_mul_div_ty(d.op, &lhs, &rhs, d.span, false);
            if !matches!(result_ty, Ty::Error) {
                // Mirror the direct mul/div sites: a product/quotient of refined
                // operands is not itself refined (e.g. 9*9=81 isn't `Small`), so
                // strip the refinement before unifying to prevent laundering.
                let result_ty = self.degrade_refinement(result_ty, d.span);
                self.unify(&Ty::Var(d.result), &result_ty, d.span);
            }
        }
    }

    fn literal_type(&mut self, lit: &ast::Literal) -> Ty {
        match lit {
            // Numeric literals are unit-polymorphic: `1.5` has type
            // `Float <u>` for a fresh unit variable `u`, so it unifies with
            // whatever unit its context demands (`(1.5 : Float M)`, `sum
            // [Float M]`, a `Float` param) while remaining sound — the var
            // binds to that unit rather than laundering it away. When the
            // context leaves `u` unconstrained, codegen defaults it to
            // dimensionless.
            ast::Literal::Int(_) => Ty::int_with_unit(UnitTy::var(self.fresh_unit_var())),
            ast::Literal::Float(_) => Ty::float_with_unit(UnitTy::var(self.fresh_unit_var())),
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
                    Ty::Forall(vars, body) => {
                        // Collect deferred constraints/effect-unions/unit-binops
                        // that reference the quantified vars so they travel with
                        // the scheme and are re-registered at each instantiation.
                        let var_set: HashSet<TyVar> = vars.iter().copied().collect();
                        let mut constraints = Vec::new();
                        let deferred = std::mem::take(&mut self.deferred_constraints);
                        let mut remaining = Vec::with_capacity(deferred.len());
                        for dc in deferred {
                            match self.apply(&Ty::Var(dc.type_var)) {
                                Ty::Var(v) if var_set.contains(&v) => {
                                    constraints.push(TyConstraint {
                                        trait_name: dc.trait_name,
                                        type_var: v,
                                        span: dc.span,
                                    });
                                }
                                _ => remaining.push(dc),
                            }
                        }
                        self.deferred_constraints = remaining;
                        let mut effect_unions = Vec::new();
                        let pending = std::mem::take(&mut self.pending_effect_unions);
                        let mut remaining_eu = Vec::with_capacity(pending.len());
                        for u in pending {
                            match self.apply(&Ty::Var(u.result)) {
                                Ty::Var(v) if var_set.contains(&v) => {
                                    effect_unions.push(EffectUnion {
                                        result: v,
                                        sources: u.sources,
                                        declared: u.declared,
                                    });
                                }
                                _ => remaining_eu.push(u),
                            }
                        }
                        self.pending_effect_unions = remaining_eu;
                        let mut unit_binops = Vec::new();
                        let pending_ub = std::mem::take(&mut self.deferred_unit_binops);
                        let mut remaining_ub = Vec::with_capacity(pending_ub.len());
                        for b in pending_ub {
                            match self.apply(&Ty::Var(b.result)) {
                                Ty::Var(v) if var_set.contains(&v) => {
                                    unit_binops.push(DeferredUnitBinop {
                                        op: b.op,
                                        lhs: self.apply(&b.lhs),
                                        rhs: self.apply(&b.rhs),
                                        result: v,
                                        span: b.span,
                                    });
                                }
                                _ => remaining_ub.push(b),
                            }
                        }
                        self.deferred_unit_binops = remaining_ub;
                        Scheme {
                            vars,
                            unit_vars: vec![],
                            constraints,
                            effect_unions,
                            unit_binops,
                            ty: *body,
                        }
                    }
                    _ => Scheme::mono(expected.clone()),
                };
                self.bind(name, scheme);
                self.binding_types.push((pat.span, expected.clone()));
            }
            ast::PatKind::Wildcard => {}
            ast::PatKind::Constructor {
                name,
                payload,
                qualifier,
            } => {
                if let Some(q) = qualifier {
                    // Qualified pattern `Color.Red`: resolve `Red` within the
                    // nominal data type `Color` and unify the scrutinee with
                    // `Color` directly — NO row-polymorphic open variant.
                    match self.instantiate_qualified_ctor(q, name) {
                        Some((data_ty, record_ty)) => {
                            self.unify(&data_ty, expected, pat.span);
                            self.check_pattern(payload, &record_ty);
                        }
                        None => {
                            self.error(
                                format!(
                                    "data type '{}' has no constructor '{}' in pattern",
                                    q, name
                                ),
                                pat.span,
                            );
                        }
                    }
                } else if self.is_builtin_ctor(name) {
                    // Unqualified BUILT-IN constructor (`True`, `Just`): stays
                    // bare. Resolve nominally within its (single) built-in ADT.
                    if let Some((data_ty, record_ty)) =
                        self.instantiate_ctor(name, pat.span)
                    {
                        self.unify(&data_ty, expected, pat.span);
                        self.check_pattern(payload, &record_ty);
                    }
                } else {
                    // A USER-defined constructor used bare. Constructors are
                    // always qualified — require `Type.Ctor`.
                    self.error(
                        format!(
                            "constructor '{}' must be qualified (e.g. `Type.{}`)",
                            name, name
                        ),
                        pat.span,
                    );
                }
            }
            ast::PatKind::Record(field_pats) => {
                let mut field_types = BTreeMap::new();
                for fp in field_pats {
                    if field_types.contains_key(&fp.name) {
                        self.error(
                            format!("duplicate field '{}' in record pattern", fp.name),
                            fp.name_span,
                        );
                    }
                    let ft = self.fresh();
                    field_types.insert(fp.name.clone(), ft.clone());
                    if let Some(p) = &fp.pattern {
                        self.check_pattern(p, &ft);
                    } else {
                        // Punned: {name} → bind variable 'name' to field type.
                        // Record the binder under the field-name token's own
                        // span (not the whole record pattern's), so hover on one
                        // punned field resolves to that field's type instead of
                        // colliding with its siblings (smallest-span-wins).
                        self.bind(&fp.name, Scheme::mono(ft.clone()));
                        self.binding_types.push((fp.name_span, ft));
                    }
                }
                let row_var = self.fresh_var();
                let record_ty = Ty::Record(field_types, Some(row_var));
                self.unify(&record_ty, expected, pat.span);
            }
            ast::PatKind::Lit(lit) => {
                let lit_ty = self.literal_type(lit);
                // Matching a literal against a refined scrutinee (`case n of 0
                // -> …`, n : Nat) only tests the value; it introduces nothing,
                // so use symmetric unification (mirrors binary operators).
                self.unify_symmetric(&lit_ty, expected, pat.span);
            }
            ast::PatKind::List(pats) => {
                let elem_ty = self.fresh();
                for p in pats {
                    self.check_pattern(p, &elem_ty);
                }
                let list_ty = Ty::Relation(Box::new(elem_ty));
                self.unify(&list_ty, expected, pat.span);
            }
            ast::PatKind::Cons { head, tail } => {
                let elem_ty = self.fresh();
                let rel_ty = Ty::Relation(Box::new(elem_ty.clone()));
                self.unify(&rel_ty, expected, pat.span);
                self.check_pattern(head, &elem_ty);
                self.check_pattern(tail, &rel_ty);
            }
            ast::PatKind::Annot { pat: inner, ty } => {
                // `(pat : T)` — bind `pat` at the annotated type `T`, which
                // must match the expected type. Convert in type-annotation
                // mode (lowercase unit vars are polymorphic), then unify the
                // annotation with `expected` and check the inner pattern
                // against it. When `T` is a `forall`, checking a `Var` inner
                // pattern against it binds the var to a polymorphic Scheme —
                // this is rank-N lambda params `\(f : (forall a. a -> a))`.
                let saved_flag = self.in_type_annotation;
                let saved_unit_vars = std::mem::take(&mut self.annotation_unit_vars);
                self.in_type_annotation = true;
                let annot_ty = self.ast_type_to_ty(ty);
                self.in_type_annotation = saved_flag;
                self.annotation_unit_vars = saved_unit_vars;
                // Make the pattern's own type the annotation itself. For a
                // `forall` this keeps the quantifier on the lambda's parameter
                // slot, so at the call site the argument is checked against a
                // *required* Forall and skolemised (rank-N soundness) — a
                // monomorphic `Int->Int` is then rejected. (Unifying instead
                // would solve the skolems away and accept anything.)
                if let Ty::Var(v) = self.apply(expected) {
                    if !self.skolems.contains(&v) {
                        self.bind_var(v, annot_ty.clone(), ty.span);
                    }
                }
                self.check_pattern(inner, &annot_ty);
            }
        }
    }

    // ── Exhaustiveness checking ────────────────────────────────

    /// Whether a pattern matches *every* value of its type — i.e. it
    /// contains no refutable sub-pattern. Wildcards and variables are
    /// irrefutable; records are irrefutable when all their field
    /// sub-patterns are. Literals, nested constructors, and list/cons
    /// patterns match only a subset of values. (A nested constructor
    /// position could in principle be exhaustive across several arms;
    /// we conservatively do not attempt that analysis and require a
    /// wildcard or irrefutable pattern instead.)
    fn pattern_is_irrefutable(pat: &ast::Pat) -> bool {
        match &pat.node {
            ast::PatKind::Wildcard | ast::PatKind::Var(_) => true,
            ast::PatKind::Record(fields) => fields.iter().all(|f| match &f.pattern {
                Some(p) => Self::pattern_is_irrefutable(p),
                None => true, // field-name shorthand binds a variable
            }),
            ast::PatKind::Lit(_)
            | ast::PatKind::Constructor { .. }
            | ast::PatKind::List(_)
            | ast::PatKind::Cons { .. } => false,
            ast::PatKind::Annot { pat, .. } => Self::pattern_is_irrefutable(pat),
        }
    }

    /// Collect the constructors fully covered by `arms` (an arm covers its
    /// constructor only when its payload pattern is irrefutable) and the
    /// constructors that are only *partially* matched (refutable payloads —
    /// e.g. `Circle {radius: 1.0}` — which must not count as coverage).
    fn covered_constructors(
        arms: &[ast::CaseArm],
    ) -> (HashSet<&str>, HashSet<&str>) {
        let mut covered: HashSet<&str> = HashSet::new();
        let mut partial: HashSet<&str> = HashSet::new();
        for arm in arms {
            match &arm.pat.node {
                ast::PatKind::Constructor { name, payload, .. } => {
                    if Self::pattern_is_irrefutable(payload) {
                        covered.insert(name.as_str());
                    } else {
                        partial.insert(name.as_str());
                    }
                }
                ast::PatKind::Lit(ast::Literal::Bool(true)) => {
                    covered.insert("True");
                }
                ast::PatKind::Lit(ast::Literal::Bool(false)) => {
                    covered.insert("False");
                }
                _ => {}
            }
        }
        (covered, partial)
    }

    /// Format the standard non-exhaustiveness message; when some missing
    /// constructors are matched only with refutable sub-patterns, point
    /// the user toward a wildcard arm.
    fn non_exhaustive_msg(missing: &[&str], partial: &HashSet<&str>) -> String {
        let hint = if missing.iter().any(|c| partial.contains(c)) {
            " (some arms match these constructors only partially — \
             add a wildcard `_` case to cover the remaining values)"
        } else {
            ""
        };
        format!(
            "non-exhaustive pattern match — missing: {}{}",
            missing.join(", "),
            hint
        )
    }

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
                // A refined alias (`type Warm = Color where …`) stays
                // nominal as `Con("Warm", [])` and is stored only in
                // `refined_types`, not `data_types`. Without resolving the
                // refined alias to its base ADT, the lookup below returns
                // `None` and exhaustiveness is silently skipped.
                let name = if !self.data_types.contains_key(name) {
                    match self.resolve_refined_base(name, span) {
                        Some(Ty::Con(base, _)) => base,
                        _ => return,
                    }
                } else {
                    name.clone()
                };
                let data_info = match self.data_types.get(&name) {
                    Some(info) => info.clone(),
                    None => return,
                };

                let (covered, partial) = Self::covered_constructors(arms);

                let missing: Vec<&str> = data_info
                    .ctors
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .filter(|c| !covered.contains(c))
                    .collect();

                if !missing.is_empty() {
                    self.error(
                        Self::non_exhaustive_msg(&missing, &partial),
                        span,
                    );
                }
            }
            Ty::Variant(ctors, row) => {
                let (covered, partial) = Self::covered_constructors(arms);

                if let Some(rv) = row {
                    // Open variant — check if the covered constructors
                    // exhaust a known data type; if so, close the row.
                    //
                    // Constructor names may legally be shared across ADTs,
                    // so resolution must be set-valued: a candidate ADT is
                    // one whose constructor set contains EVERY covered
                    // constructor. (Looking each name up individually in
                    // `self.constructors` would resolve to whichever ADT
                    // registered the name last — declaring an unrelated
                    // `data B = X {} | Z {}` after `data A = X {} | Y {}`
                    // must not break matches on A.)
                    if !covered.is_empty() {
                        let mut candidates: Vec<String> = self
                            .data_types
                            .iter()
                            .filter(|(_, info)| {
                                covered.iter().all(|c| {
                                    info.ctors
                                        .iter()
                                        .any(|(n, _)| n.as_str() == *c)
                                })
                            })
                            .map(|(name, _)| name.clone())
                            .collect();
                        // Sort for deterministic candidate selection.
                        candidates.sort();
                        for dt in &candidates {
                            let dt_info = match self.data_types.get(dt) {
                                Some(info) => info.clone(),
                                None => continue,
                            };
                            let all_ctors: HashSet<&str> = dt_info
                                .ctors
                                .iter()
                                .map(|(n, _)| n.as_str())
                                .collect();
                            if covered == all_ctors {
                                // All constructors of a known type are
                                // covered — close the row var.  Use
                                // `bind_var` so the occurs- and skolem-
                                // checks that every other binding site
                                // enforces are applied here too.
                                let rv = *rv;
                                self.bind_var(
                                    rv,
                                    Ty::Variant(BTreeMap::new(), None),
                                    span,
                                );
                                return;
                            }
                        }
                        // No candidate is fully covered. If at least one
                        // ADT contains all covered constructors, report the
                        // one with the fewest missing constructors (ties
                        // broken by name order — deterministic).
                        let mut best: Option<(usize, &String)> = None;
                        for dt in &candidates {
                            if let Some(dt_info) = self.data_types.get(dt) {
                                let missing_count = dt_info
                                    .ctors
                                    .iter()
                                    .filter(|(n, _)| {
                                        !covered.contains(n.as_str())
                                    })
                                    .count();
                                if best
                                    .map(|(c, _)| missing_count < c)
                                    .unwrap_or(true)
                                {
                                    best = Some((missing_count, dt));
                                }
                            }
                        }
                        if let Some((_, dt)) = best {
                            let dt_info = self.data_types[dt].clone();
                            let missing: Vec<&str> = dt_info
                                .ctors
                                .iter()
                                .map(|(n, _)| n.as_str())
                                .filter(|c| !covered.contains(c))
                                .collect();
                            if !missing.is_empty() {
                                self.error(
                                    Self::non_exhaustive_msg(
                                        &missing, &partial,
                                    ),
                                    span,
                                );
                                return;
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
                            Self::non_exhaustive_msg(&missing, &partial),
                            span,
                        );
                    }
                }
            }
            // Bool is Ty::Bool (not Ty::Con), so handle it explicitly.
            Ty::Bool => {
                if let Some(data_info) = self.data_types.get("Bool").cloned() {
                    let (covered, partial) = Self::covered_constructors(arms);

                    let missing: Vec<&str> = data_info
                        .ctors
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .filter(|c| !covered.contains(c))
                        .collect();

                    if !missing.is_empty() {
                        self.error(
                            Self::non_exhaustive_msg(&missing, &partial),
                            span,
                        );
                    }
                }
            }
            // Relations: exhaustive iff `[]` and `Cons h t` (with
            // irrefutable head/tail — `Cons 1 rest` only matches lists
            // starting with 1) are both covered (or a wildcard is
            // present, handled above).
            Ty::Relation(_) => {
                let has_empty = arms.iter().any(|arm| matches!(
                    &arm.pat.node,
                    ast::PatKind::List(items) if items.is_empty()
                ));
                let has_cons = arms.iter().any(|arm| matches!(
                    &arm.pat.node,
                    ast::PatKind::Cons { head, tail }
                        if Self::pattern_is_irrefutable(head)
                            && Self::pattern_is_irrefutable(tail)
                ));
                let mut missing: Vec<&str> = Vec::new();
                if !has_empty {
                    missing.push("[]");
                }
                if !has_cons {
                    missing.push("Cons head tail");
                }
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
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Expr(expr) => {
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
                (crate::builtins::is_io_builtin(name) || name == "fork" || name == "race")
                || self.lookup(name).is_some_and(|scheme| {
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
            ast::ExprKind::Atomic(_) => true,
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
                    ast::StmtKind::Where { cond } => self.expr_is_io_prescan(cond),
                    ast::StmtKind::GroupBy { key } => self.expr_is_io_prescan(key),
                })
            }
            ast::ExprKind::With { record, body } => {
                self.expr_is_io_prescan(record) || self.expr_is_io_prescan(body)
            }
            ast::ExprKind::Lambda { body, .. } => self.expr_is_io_prescan(body),
            ast::ExprKind::TimeUnitLit { value, .. } => self.expr_is_io_prescan(value),
            ast::ExprKind::Annot { expr, .. } => self.expr_is_io_prescan(expr),
            ast::ExprKind::Refine(inner) => self.expr_is_io_prescan(inner),
            _ => false,
        }
    }

    /// Merge an IO statement's effect-row tail into the do-block's
    /// accumulated row. Ordinarily the rows are unified into a single
    /// polymorphic tail, but when both rows are *rigid* (signature
    /// skolems, e.g. `r1` and `r2` in
    /// `IO {| r1} {} -> IO {| r2} {} -> IO {| r1 \/ r2} {}`) the
    /// sequenced block's row is their *union*, not an equality —
    /// forcing them equal rejects every user-annotated `\/` function.
    /// Mirror the way `race`'s builtin registration types `\/`: a fresh
    /// result row bound by a pending effect-union constraint.
    ///
    /// The union is only available when the signature *declared* it. Two
    /// rigid rows the user never joined with `\/` stay unmergeable, so the
    /// `unify` fallback rejects them the same way it does everywhere else:
    /// a body sequencing `IO {| r1}` with `IO {| r2}` cannot be typed
    /// `IO {| r1}`, which would drop `r2`'s effects on the floor.
    fn merge_do_io_row(
        &mut self,
        io_row: &mut Option<TyVar>,
        rv: TyVar,
        span: Span,
    ) {
        let existing = match *io_row {
            None => {
                *io_row = Some(rv);
                return;
            }
            Some(e) => e,
        };
        if existing == rv {
            return;
        }
        let root_of = |slf: &Self, v: TyVar| -> Option<TyVar> {
            match slf.apply(&Ty::Var(v)) {
                Ty::Var(x) => Some(x),
                _ => None,
            }
        };
        if let (Some(e), Some(r)) =
            (root_of(self, existing), root_of(self, rv))
            && e != r {
                let e_rigid = self.skolems.contains(&e);
                let r_rigid = self.skolems.contains(&r);
                if e_rigid && r_rigid {
                    if !self.declared_union_sanctions(e, r) {
                        self.unify(&Ty::Var(existing), &Ty::Var(rv), span);
                        return;
                    }
                    let result = self.fresh_var();
                    self.pending_effect_unions.push(EffectUnion {
                        result,
                        sources: vec![e, r],
                        declared: false,
                    });
                    *io_row = Some(result);
                    return;
                }
                // The accumulated row may already be a union result var;
                // fold further rigid rows into its sources rather than
                // aliasing the union var to a skolem. Folding is subject to
                // the same rule as the merge above: the new row may only join
                // a union whose every other source the signature already
                // declared it unionable with, so a third rigid row can't slip
                // into a two-row `\/`.
                if r_rigid {
                    let idx = self.pending_effect_unions.iter().position(|u| self.var_chain_end(u.result) == e);
                    if let Some(idx) = idx
                        && self.union_admits_source(idx, r) {
                            let u = &mut self.pending_effect_unions[idx];
                            if !u.sources.contains(&r) {
                                u.sources.push(r);
                            }
                            return;
                        }
                }
                if e_rigid {
                    let idx = self.pending_effect_unions.iter().position(|u| self.var_chain_end(u.result) == r);
                    if let Some(idx) = idx
                        && self.union_admits_source(idx, e) {
                            let u = &mut self.pending_effect_unions[idx];
                            if !u.sources.contains(&e) {
                                u.sources.push(e);
                            }
                            *io_row = Some(rv);
                            return;
                        }
                }
            }
        self.unify(&Ty::Var(existing), &Ty::Var(rv), span);
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

                    // In a view body the do-block is a relation
                    // comprehension (codegen's `analyze_view`): a bind from
                    // an IO-wrapped relation iterates its ELEMENTS, so peel
                    // the IO wrapper and fall through to the relation-bind
                    // path below instead of treating it as an IO bind.
                    let (expr_ty, resolved) =
                        if self.in_view_comprehension {
                            match resolved {
                                Ty::IO(_, _, inner) => {
                                    let inner = (*inner).clone();
                                    let applied = self.apply(&inner);
                                    (inner, applied)
                                }
                                other => (expr_ty, other),
                            }
                        } else {
                            (expr_ty, resolved)
                        };

                    if let Ty::IO(ref effects, ref row, ref inner) = resolved {
                        // IO bind: x <- ioAction
                        is_io = true;
                        io_effects.extend(effects.iter().cloned());
                        if let Some(rv) = row {
                            let rv = *rv;
                            self.merge_do_io_row(&mut io_row, rv, expr.span);
                        }
                        let inner_applied = self.apply(inner);
                        if is_ctor_pat {
                            if let Ty::Relation(elem) =
                                inner_applied.peel_alias()
                            {
                                // `Ctor pat <- *rel` filters the relation to
                                // matching constructors and destructures each
                                // element — the pattern matches ELEMENTS, not
                                // the whole relation (same semantics as the
                                // two-step `rows <- *rel; Ctor pat <- rows`).
                                has_relation_bind = true;
                                let elem = (**elem).clone();
                                self.check_pattern(pat, &elem);
                            } else {
                                self.check_pattern(pat, &inner_applied);
                            }
                        } else {
                            self.check_pattern(pat, inner);
                        }
                    } else if self.in_io_do && matches!(&resolved, Ty::Var(_)) {
                        // In an IO do-block with an unresolved type variable —
                        // assume IO so we don't incorrectly unify with Relation.
                        // Use an OPEN effect row: the effects are unknown, not
                        // known-empty — a closed `IO {}` here would make the
                        // directional effect check reject effectful values
                        // (e.g. a callback parameter) later unified with it.
                        is_io = true;
                        let inner_ty = self.fresh();
                        let row = self.fresh_var();
                        self.unify(
                            &expr_ty,
                            &Ty::IO(BTreeSet::new(), Some(row), Box::new(inner_ty.clone())),
                            expr.span,
                        );
                        self.merge_do_io_row(&mut io_row, row, expr.span);
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
                    if let ast::PatKind::Var(var_name) = &pat.node
                        && let ast::ExprKind::SourceRef(source_name) = &expr.node {
                            self.source_var_binds
                                .insert(var_name.clone(), source_name.clone());
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
                            // Collect all variable names bound by this pattern
                            // (handles Var, Constructor, Record, List, Cons patterns).
                            let mut bound: Vec<String> = Vec::new();
                            collect_pat_bound_names(pat, &mut bound);
                            for name in &bound {
                                if let Some(scheme) = self.lookup(name).cloned() {
                                    let ty = self.instantiate_at(&scheme, key.span);
                                    if matches!(ty, Ty::IO(..)) {
                                        continue;
                                    }
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
                            self.unify(&inner_ty, &yt, expr.span);
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
                                let rv = *rv;
                                self.merge_do_io_row(&mut io_row, rv, expr.span);
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
                                // Open effect row: the effects are unknown,
                                // not known-empty (see the Bind arm above).
                                is_io = true;
                                let inner_ty = self.fresh();
                                let row = self.fresh_var();
                                self.unify(
                                    &expr_ty,
                                    &Ty::IO(BTreeSet::new(), Some(row), Box::new(inner_ty.clone())),
                                    expr.span,
                                );
                                self.merge_do_io_row(&mut io_row, row, expr.span);
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
        let has_group_by = stmts
            .iter()
            .any(|s| matches!(&s.node, ast::StmtKind::GroupBy { .. }));
        if promote_to_io {
            // An IO-promoted block that is still a *comprehension* — it
            // iterates plain relation binds (or groupBy groups) and
            // ACCUMULATES its yields — evaluates to the whole relation of
            // yielded values, not a single element. Codegen compiles such
            // blocks with a per-row loop pushing each yield into a result
            // relation (compile_io_bind_loop / the relational groupBy
            // path), so the type must be `IO [yield_ty]`, not
            // `IO yield_ty`. IO blocks without comprehension binds keep
            // `yield = pure` semantics (the yield value IS the result).
            if let Some(ty) = &yield_ty
                && (has_relation_bind || has_group_by) {
                    return Ty::IO(
                        io_effects,
                        io_row,
                        Box::new(Ty::Relation(Box::new(ty.clone()))),
                    );
                }
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

    fn collect_types(&mut self, program: &ast::Expr) {
        // First pass: type aliases (multi-pass to handle forward references)
        // Separate refined type aliases from regular ones.
        let mut alias_decls: Vec<(String, ast::Type, Span)> = Vec::new();
        let mut refined_alias_decls: Vec<(String, ast::Type, ast::Expr)> = Vec::new();
        for_each_type_ctor(program, &mut |name, params, ty, span| {
            if params.is_empty() {
                if let ast::TypeKind::Refined { base, predicate } = &ty.node {
                    refined_alias_decls.push((
                        name.to_string(),
                        (**base).clone(),
                        (**predicate).clone(),
                    ));
                } else {
                    alias_decls.push((name.to_string(), ty.clone(), span));
                }
            } else {
                // Parameterized alias: keep the AST body + param names so
                // applications (`Pair Int Text`) elaborate fresh each time
                // and substitute the actual arguments.
                self.param_aliases
                    .insert(name.to_string(), (params.to_vec(), ty.clone()));
            }
        });
        // Detect cyclic alias definitions (e.g. `type A = B; type B = A`)
        // before the fixpoint loop: each iteration would wrap another
        // `Ty::Alias` layer and never converge (stack overflow). A name is
        // cyclic when it can reach itself through alias references.
        let alias_names: HashSet<String> =
            alias_decls.iter().map(|(n, _, _)| n.clone()).collect();
        let mut alias_deps: HashMap<String, HashSet<String>> = HashMap::new();
        for (name, ty, _) in &alias_decls {
            let mut refs = HashSet::new();
            collect_alias_refs(ty, &alias_names, &mut refs);
            alias_deps.entry(name.clone()).or_default().extend(refs);
        }
        let mut cyclic_names: HashSet<String> = HashSet::new();
        for (name, _, span) in &alias_decls {
            if cyclic_names.contains(name) {
                continue;
            }
            let mut stack: Vec<String> =
                alias_deps[name].iter().cloned().collect();
            let mut visited: HashSet<String> = HashSet::new();
            let mut found = false;
            while let Some(n) = stack.pop() {
                if &n == name {
                    found = true;
                    break;
                }
                if visited.insert(n.clone())
                    && let Some(ds) = alias_deps.get(&n) {
                        stack.extend(ds.iter().cloned());
                    }
            }
            if found {
                cyclic_names.insert(name.clone());
                self.error(
                    format!(
                        "cyclic type alias '{}' — a type alias cannot refer to itself, directly or through other aliases",
                        name
                    ),
                    *span,
                );
                // Register an error type so dependents resolve to something
                // stable instead of diverging.
                self.aliases.insert(name.clone(), Ty::Error);
            }
        }

        // Iterate until alias resolutions stabilize (fixpoint).
        // Clear annotation_vars once before the loop so that type variable
        // names (e.g. `a` in `type T = a`) map to stable TyVars across
        // iterations — clearing inside would allocate fresh vars each time,
        // preventing convergence.
        self.annotation_vars.clear();
        // Safety bound: acyclic alias chains resolve in at most one pass per
        // alias; anything beyond that indicates an undetected divergence.
        let max_passes = alias_decls.len() + 1;
        let mut passes = 0;
        let saved_enforce = self.enforce_units;
        self.enforce_units = true;
        loop {
            let mut changed = false;
            for (name, ty, _) in &alias_decls {
                if cyclic_names.contains(name) {
                    continue;
                }
                let resolved = self.ast_type_to_ty(ty);
                if self.aliases.get(name) != Some(&resolved) {
                    self.aliases.insert(name.clone(), resolved);
                    changed = true;
                }
            }
            passes += 1;
            if !changed || passes > max_passes {
                break;
            }
        }

        // Populate refined types (after alias fixpoint so bases can reference aliases)
        for (name, base_ty_ast, predicate) in &refined_alias_decls {
            let base_ty = self.ast_type_to_ty(base_ty_ast);
            self.refined_types
                .insert(name.clone(), (base_ty, predicate.clone()));
        }
        self.enforce_units = saved_enforce;

        // Second pass: data types and constructors
        for_each_data_ctor(program, &mut |name, params, ctors, span| {
            {
                // Detect duplicate constructor names within the same `data`
                // declaration. Distinct ADTs may share a constructor name
                // (row-polymorphic variants — see comment below), but a
                // duplicate within one declaration is a user error that would
                // otherwise be silently accepted (last-write-wins for the
                // variant row, confusing downstream errors).
                {
                    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                    for ctor in ctors {
                        if !seen.insert(ctor.name.clone()) {
                            self.error(
                                format!(
                                    "duplicate constructor '{}' in data declaration '{}'",
                                    ctor.name, name
                                ),
                                span,
                            );
                        }
                    }
                }
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
                    // documented feature. We keep *every* declaring ADT so an
                    // overloaded name instantiates as an open variant rather
                    // than last-write-wins; open-variant dispatch goes through
                    // `knot_constructor_matches` at runtime which doesn't
                    // depend on this map.
                    self.constructors
                        .entry(ctor.name.clone())
                        .or_default()
                        .push(CtorInfo {
                            data_type: name.to_string(),
                            data_params: params.to_vec(),
                            fields: fields.clone(),
                        });
                    ctor_list.push((ctor.name.clone(), fields));
                }

                // Enforce unit annotations on every constructor field at
                // declaration time. Multi-variant fields are otherwise only
                // converted lazily at use sites (instantiate_ctor), so an
                // unused constructor with a bare `Int`/`Float` field would
                // slip through. Convert each field once here to surface the
                // error; the result is discarded (lazy conversion still runs).
                {
                    let saved_annotation_vars = self.annotation_vars.clone();
                    self.annotation_vars.clear();
                    for p in params {
                        let v = self.fresh_var();
                        self.annotation_vars.insert(p.clone(), v);
                    }
                    for ctor in ctors {
                        for f in &ctor.fields {
                            let _ = self.ast_type_to_ty(&f.value);
                        }
                    }
                    self.annotation_vars = saved_annotation_vars;
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
                            name.to_string(),
                            Ty::Record(field_tys, None),
                        );
                    }
                }

                self.data_types.insert(
                    name.to_string(),
                    DataInfo {
                        params: params.to_vec(),
                        ctors: ctor_list,
                    },
                );
            }
        });
    }

    // ── Source/view collection (phase 2) ──────────────────────────

    fn collect_sources(&mut self, program: &ast::Expr) {
        for_each_relation_marker(program, &mut |m| {
            match m {
                RelMarker::Source { name, ty } => {
                    self.annotation_vars.clear();
                    let resolved = self.ast_type_to_ty(ty);
                    self.source_types.insert(name.to_string(), resolved);
                }
                RelMarker::View { name, ty, .. } => {
                    let resolved = if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        self.ast_type_to_ty(&scheme.ty)
                    } else {
                        Ty::Relation(Box::new(self.fresh()))
                    };
                    self.source_types.insert(name.to_string(), resolved);
                    self.view_names.insert(name.to_string());
                }
                RelMarker::Derived { name, ty, .. } => {
                    let resolved = if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        self.ast_type_to_ty(&scheme.ty)
                    } else {
                        self.fresh()
                    };
                    self.derived_types.insert(name.to_string(), resolved);
                }
            }
        });
    }

    // ── Impl collection (phase 2b) ─────────────────────────────

    fn collect_impls(&mut self, _program: &ast::Expr) {
        // Traits are gone — no user impl declarations exist. The intrinsic
        // impls (`Eq`/`Ord`/`Num`/…) are registered unconditionally by
        // `check_inner`, so there is nothing to collect from the program.
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
            Ty::Uuid => Some("Uuid".into()),
            Ty::Relation(_) => Some("[]".into()),
            Ty::TyCon(name) => Some(name.clone()),
            // Refined nullary aliases (`type Nat = Int where ...`) are erased
            // at runtime, so trait impls on the base type satisfy constraints
            // on the refined type. Walk the refined chain to the base name.
            Ty::Con(name, args) if args.is_empty() && self.refined_types.contains_key(name) => {
                self.refined_base_type_name(name)
            }
            Ty::Con(name, _) => Some(name.clone()),
            Ty::IO(_, _, _) => Some("IO".into()),
            Ty::Fun(_, _) => Some("Fun".into()),
            Ty::Record(_, _) => Some("Record".into()),
            Ty::Variant(_, _) => Some("Variant".into()),
            Ty::App(_, _) => Some("App".into()),
            // Units are erased at runtime, so trait dispatch on a unit-typed
            // value resolves to the underlying primitive's impl. A unit-bearing
            // Int/Float is `Con("Int"/"Float", [Unit(_)])`; the general
            // `Ty::Con(name, _)` arm below already returns the name, so no
            // special arm is needed — the name IS "Int"/"Float".
            // An unresolved associated-type projection (`Elem c`, etc.) cannot
            // be reduced to a concrete type with trait impls. Returning None
            // here would silently drop trait constraints on such types
            // (e.g. `Display (Elem c)` would vanish). Surface a name so the
            // missing-impl error path fires with a clear diagnostic instead.
            Ty::Assoc(name, _) => Some(name.clone()),
            _ => None,
        }
    }

    /// Walk a refined nullary alias chain to its non-refined base, returning
    /// that base's `type_name_of`. Cycles (already diagnosed by unification)
    /// produce `None`.
    fn refined_base_type_name(&self, name: &str) -> Option<String> {
        let mut visited: Vec<&str> = vec![name];
        let mut current = &self.refined_types.get(name)?.0;
        loop {
            match current {
                Ty::Con(n, args) if args.is_empty() && self.refined_types.contains_key(n) => {
                    if visited.contains(&n.as_str()) {
                        return None;
                    }
                    visited.push(n.as_str());
                    current = &self.refined_types[n].0;
                }
                _ => return self.type_name_of(current),
            }
        }
    }

    // ── Pre-registration (phase 3) ───────────────────────────────

    fn pre_register(&mut self, program: &ast::Expr) {
        // Register built-in functions
        self.register_builtins();

        // Named functions are `with`-record fields with a signature and/or a
        // lambda value. Pre-register their schemes by name.
        for_each_named_fn(program, &mut |name, sig, _value| {
            if let Some(scheme) = sig {
                self.annotation_vars.clear();
                self.annotation_unit_vars.clear();
                self.in_type_annotation = true;
                // Convert AST constraints to internal constraints
                let mut constraints = Vec::new();
                for c in &scheme.constraints {
                    match c {
                        ast::Constraint::Trait { trait_name, args } => {
                            for arg in args {
                                if let ast::TypeKind::Var(var_name) = &arg.node {
                                    let v = self.annotation_var(var_name);
                                    constraints.push(TyConstraint {
                                        trait_name: trait_name.clone(),
                                        type_var: v,
                                        span: arg.span,
                                    });
                                }
                            }
                        }
                        ast::Constraint::ImplicitField { .. } => {
                            // Handled in the implicit-field pipeline.
                        }
                    }
                }
                let unions_before = self.pending_effect_unions.len();
                let raw_ty = self.ast_type_to_ty(&scheme.ty);
                self.in_type_annotation = false;
                let mut vars: Vec<TyVar> =
                    self.annotation_vars.values().copied().collect();
                let unit_vars: Vec<UnitVar> =
                    self.annotation_unit_vars.values().copied().collect();
                let ty = match raw_ty {
                    Ty::Forall(forall_vars, body) => {
                        vars.extend(forall_vars);
                        *body
                    }
                    other => other,
                };
                let effect_unions: Vec<EffectUnion> =
                    self.pending_effect_unions.split_off(unions_before);
                for u in &effect_unions {
                    if !vars.contains(&u.result) {
                        vars.push(u.result);
                    }
                    for s in &u.sources {
                        if !vars.contains(s) {
                            vars.push(*s);
                        }
                    }
                }
                self.bind_top(
                    name,
                    Scheme { vars, unit_vars, constraints, effect_unions, unit_binops: vec![], ty },
                );
            } else {
                let var = self.fresh();
                self.bind_top(name, Scheme::mono(var));
            }
        });

        // Routes: register by name/path.
        for_each_route_marker(program, &mut |name, entries| {
            if let Some(entries) = entries {
                self.route_types.insert(name.to_string());
                self.route_entries_by_api
                    .insert(name.to_string(), entries.to_vec());
                for entry in entries {
                    if let Some(ref resp_ty) = entry.response_ty {
                        self.fetch_response_types
                            .insert(entry.constructor.clone(), resp_ty.clone());
                    }
                    self.fetch_response_headers
                        .insert(entry.constructor.clone(), entry.response_headers.clone());
                }
            } else {
                self.route_types.insert(name.to_string());
            }
        });

        // Resolve composite routes: flatten their components' entries into
        // `route_entries_by_api` so `serve` can find them by composite name.
        // Composites may reference other composites declared in any order,
        // so resolve to a fixpoint: a composite is flattened once all of its
        // components have entries. Anything left after the fixpoint either
        // references an unknown route or participates in a cycle — both get
        // a diagnostic instead of silently dropping endpoints.
        let mut composites: Vec<(String, Vec<String>, Span)> = Vec::new();
        for_each_route_composite(program, &mut |name, components, span| {
            composites.push((name.to_string(), components.to_vec(), span));
        });
        let composite_names: HashSet<String> =
            composites.iter().map(|(n, _, _)| n.clone()).collect();
        let mut pending = composites;
        loop {
            let mut progressed = false;
            let mut still_pending = Vec::new();
            for (name, components, span) in pending {
                if components
                    .iter()
                    .all(|c| self.route_entries_by_api.contains_key(c))
                {
                    let mut combined = Vec::new();
                    for comp in &components {
                        if let Some(entries) =
                            self.route_entries_by_api.get(comp)
                        {
                            combined.extend(entries.iter().cloned());
                        }
                    }
                    self.route_entries_by_api.insert(name, combined);
                    progressed = true;
                } else {
                    still_pending.push((name, components, span));
                }
            }
            pending = still_pending;
            if !progressed || pending.is_empty() {
                break;
            }
        }
        for (name, components, span) in pending {
            let mut combined = Vec::new();
            for comp in &components {
                match self.route_entries_by_api.get(comp) {
                    Some(entries) => combined.extend(entries.iter().cloned()),
                    None => {
                        if composite_names.contains(comp) || *comp == name {
                            self.error(
                                format!(
                                    "cyclic route composition: route '{}' refers to '{}', which (directly or indirectly) refers back to it",
                                    name, comp
                                ),
                                span,
                            );
                        } else {
                            self.error(
                                format!(
                                    "route '{}' refers to '{}', which is not a declared route",
                                    name, comp
                                ),
                                span,
                            );
                        }
                    }
                }
            }
            // Register the entries we could resolve so downstream `serve`
            // checks produce fewer cascading errors.
            self.route_entries_by_api.insert(name, combined);
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
        // `parseJson : Text -> Maybe a` — a failure channel for malformed
        // input. The runtime decoder returns `Nothing` on parse error and
        // `Just decoded` on success, rather than aborting the program.
        let a = self.fresh_var();
        self.bind_top(
            "parseJson",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                ),
            ),
        );
    }

    fn register_builtins(&mut self) {
        // Built-in ADTs whose constructors stay referenceable bare.
        for n in ["Maybe", "Bool", "Result"] {
            self.builtin_data_types.insert(n.to_string());
        }
        // Built-in ADT: data Maybe a = Nothing {} | Just {value: a}
        let dummy_span = Span::new(0, 0);
        self.constructors.insert(
            "Nothing".into(),
            vec![CtorInfo {
                data_type: "Maybe".into(),
                data_params: vec!["a".into()],
                fields: vec![],
            }],
        );
        self.constructors.insert(
            "Just".into(),
            vec![CtorInfo {
                data_type: "Maybe".into(),
                data_params: vec!["a".into()],
                fields: vec![(
                    "value".into(),
                    ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span),
                )],
            }],
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
            vec![CtorInfo {
                data_type: "Bool".into(),
                data_params: vec![],
                fields: vec![],
            }],
        );
        self.constructors.insert(
            "False".into(),
            vec![CtorInfo {
                data_type: "Bool".into(),
                data_params: vec![],
                fields: vec![],
            }],
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
            vec![CtorInfo {
                data_type: "Result".into(),
                data_params: vec!["e".into(), "a".into()],
                fields: vec![(
                    "error".into(),
                    ast::Type::new(ast::TypeKind::Var("e".into()), dummy_span),
                )],
            }],
        );
        self.constructors.insert(
            "Ok".into(),
            vec![CtorInfo {
                data_type: "Result".into(),
                data_params: vec!["e".into(), "a".into()],
                fields: vec![(
                    "value".into(),
                    ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span),
                )],
            }],
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

        // Built-in type: HttpError = {status: Int 1, message: Text}
        // Used as the error type for serve handler return values: every
        // handler returns `Result HttpError T`, where Err carries a custom
        // HTTP status code and message.
        self.aliases.insert(
            "HttpError".into(),
            Ty::Record(
                BTreeMap::from([
                    ("status".into(), Ty::Int),
                    ("message".into(), Ty::Text),
                ]),
                None,
            ),
        );

        // Built-in type: RequestCtx — passed to a route's `rateLimit` key
        // function. Carries client metadata and a header lookup function.
        self.aliases.insert(
            "RequestCtx".into(),
            Ty::Record(
                BTreeMap::from([
                    ("clientIp".into(), Ty::Text),
                    ("receivedAt".into(), Ty::int_with_unit(UnitTy::named("Ms"))),
                    (
                        "header".into(),
                        Ty::Fun(
                            Box::new(Ty::Text),
                            Box::new(Ty::Con("Maybe".into(), vec![Ty::Text])),
                        ),
                    ),
                ]),
                None,
            ),
        );

        // ── strip / dress: top-level unit rebranding ────────────────────
        // `strip` removes a value's unit; `dress` attaches one. Both are
        // unconstrained top-level functions (no trait), identity at runtime.
        //   strip : ∀a u. a u -> a 1
        //   dress : ∀a u. a 1 -> a u
        // `a u` is `App(Var a, Unit u)`, which unifies with a concrete
        // unit-bearing `Con("Int"/"Float", [Unit M])` by decomposition
        // (a := TyCon "Int", u := M). The prelude cannot express `a 1`
        // (`1` is not a type), so these are registered here directly.
        for (method, from_dimless) in [("strip", false), ("dress", true)] {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let arg_unit = if from_dimless {
                UnitTy::dimensionless()
            } else {
                UnitTy::var(u)
            };
            let res_unit = if from_dimless {
                UnitTy::var(u)
            } else {
                UnitTy::dimensionless()
            };
            let a_ty = Ty::Var(a);
            let method_ty = Ty::Fun(
                Box::new(Ty::App(Box::new(a_ty.clone()), Box::new(Ty::Unit(arg_unit)))),
                Box::new(Ty::App(Box::new(a_ty), Box::new(Ty::Unit(res_unit)))),
            );
            self.bind_top(
                method,
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: method_ty,
                },
            );
        }

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

        // count : ∀a u. [a] -> Int u
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            self.bind_top(
                "count",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(int_u),
                    ),
                },
            );
        }

        // countWhere : ∀a u. (a -> Bool) -> [a] -> Int u
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            self.bind_top(
                "countWhere",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
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

        // now : IO {clock} Int Ms
        {
            let int_ms = Ty::int_with_unit(UnitTy::named("Ms"));
            self.bind_top("now", Scheme::mono(
                Ty::IO(BTreeSet::from([IoEffect::Clock]), None, Box::new(int_ms)),
            ));
        }

        // sleep : Int Ms -> IO {clock} {}
        {
            let int_ms = Ty::int_with_unit(UnitTy::named("Ms"));
            self.bind_top(
                "sleep",
                Scheme::mono(Ty::Fun(
                    Box::new(int_ms),
                    Box::new(Ty::IO(BTreeSet::from([IoEffect::Clock]), None, Box::new(Ty::unit()))),
                )),
            );
        }

        // randomInt : ∀u. Int u -> IO {random} Int u
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            self.bind_top(
                "randomInt",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u.clone()),
                        Box::new(Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(int_u))),
                    ),
                },
            );
        }

        // randomFloat : ∀u. IO {random} Float u
        {
            let u = self.fresh_unit_var();
            let float_u = Ty::float_with_unit(UnitTy::var(u));
            self.bind_top("randomFloat", Scheme {
                vars: vec![],
                unit_vars: vec![u],
                constraints: vec![],
                effect_unions: vec![],
                unit_binops: vec![],
                ty: Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(float_u)),
            });
        }

        // randomUuid : IO {random} Uuid (UUIDv7)
        self.bind_top("randomUuid", Scheme::mono(
            Ty::IO(BTreeSet::from([IoEffect::Random]), None, Box::new(Ty::Uuid)),
        ));

        // fork : ∀a r. IO r a -> IO r {}
        // Argument is any IO action (any effects, any result). The spawned
        // action's effect row propagates through fork to the caller, so a
        // program that forks an IO performing `println` is visibly typed
        // with `{console}` in its IO row.
        {
            let a = self.fresh_var();
            let r = self.fresh_var();
            self.bind_top(
                "fork",
                Scheme::poly(
                    vec![a, r],
                    Ty::Fun(
                        Box::new(Ty::IO(BTreeSet::new(), Some(r), Box::new(Ty::Var(a)))),
                        Box::new(Ty::IO(BTreeSet::new(), Some(r), Box::new(Ty::unit()))),
                    ),
                ),
            );
        }

        // race : ∀a b r1 r2. IO {| r1} a -> IO {| r2} b
        //                    -> IO {| r1 \/ r2} (Result a b)
        // Each arm carries its own effect row; the result's row is the
        // union of both via the `\/` operator. Built from an AST so the
        // type is plumbed through the same path user-level `\/` annotations
        // take: `ast_type_to_ty` registers the effect-union constraint,
        // `generalize` captures it on the scheme, and each instantiation
        // freshens the union. The winner is reported via the built-in
        // `Result a b` ADT — `Err {error: a}` when the left action wins,
        // `Ok {value: b}` when the right action wins.
        {
            let sp = Span::new(0, 0);
            let mk = |node| ast::Type { node, span: sp };
            let var = |n: &str| mk(ast::TypeKind::Var(n.to_string()));
            let io = |rest: Vec<String>, ty: ast::Type| mk(ast::TypeKind::IO {
                effects: vec![],
                rest,
                ty: Box::new(ty),
            });
            let result_ab = mk(ast::TypeKind::App {
                func: Box::new(mk(ast::TypeKind::App {
                    func: Box::new(mk(ast::TypeKind::Named("Result".into()))),
                    arg: Box::new(var("a")),
                })),
                arg: Box::new(var("b")),
            });
            let race_ast = mk(ast::TypeKind::Function {
                param: Box::new(io(vec!["r1".into()], var("a"))),
                result: Box::new(mk(ast::TypeKind::Function {
                    param: Box::new(io(vec!["r2".into()], var("b"))),
                    result: Box::new(io(
                        vec!["r1".into(), "r2".into()],
                        result_ab,
                    )),
                })),
            });
            let saved = std::mem::take(&mut self.annotation_vars);
            let race_ty = self.ast_type_to_ty(&race_ast);
            self.annotation_vars = saved;
            self.in_top_level_generalize = true;
            let scheme = self.generalize(&race_ty);
            self.in_top_level_generalize = false;
            self.bind_top("race", scheme);
        }

        // retry : ∀a. a (polymorphic bottom — usable in any context inside atomic)
        let a = self.fresh_var();
        self.bind_top("retry", Scheme::poly(vec![a], Ty::Var(a)));

        // __bind, __yield, __empty are handled as special cases in infer_expr
        // with polymorphic HKT types: ∀m a b. (a -> m b) -> m a -> m b, etc.
        // This allows do-block desugaring to work with any monad, not just [].

        // listen : ∀a u r. Int u -> Server a r -> IO {network | r} {}
        // The handler value must be a `Server a`, produced by the
        // `serve a where ...` expression. Each endpoint handler returns
        // its own response type; the runtime serializes the result based
        // on which endpoint matched. Server's effect-row arg shares the
        // row variable `r` with `listen`'s IO row, so the union of every
        // handler's IO effects flows out through the returned IO type — a
        // server whose handlers do `println` is
        // visibly typed `IO {network, console} {}`.
        {
            let a = self.fresh_var();
            let r = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            let server = Ty::Con(
                "Server".into(),
                vec![
                    Ty::Var(a),
                    Ty::EffectRow(BTreeSet::new(), Some(r)),
                ],
            );
            self.bind_top(
                "listen",
                Scheme {
                    vars: vec![a, r],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u),
                        Box::new(Ty::Fun(
                            Box::new(server),
                            Box::new(Ty::IO(
                                BTreeSet::from([IoEffect::Network]),
                                Some(r),
                                Box::new(Ty::unit()),
                            )),
                        )),
                    ),
                },
            );
        }

        // listenOn : ∀a u r. Text -> Int u -> Server a r -> IO {network | r} {}
        {
            let a = self.fresh_var();
            let r = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            let server = Ty::Con(
                "Server".into(),
                vec![
                    Ty::Var(a),
                    Ty::EffectRow(BTreeSet::new(), Some(r)),
                ],
            );
            self.bind_top(
                "listenOn",
                Scheme {
                    vars: vec![a, r],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Text),
                        Box::new(Ty::Fun(
                            Box::new(int_u),
                            Box::new(Ty::Fun(
                                Box::new(server),
                                Box::new(Ty::IO(
                                    BTreeSet::from([IoEffect::Network]),
                                    Some(r),
                                    Box::new(Ty::unit()),
                                )),
                            )),
                        )),
                    ),
                },
            );
        }

        // fetch : ∀a b. Text -> a -> IO {network} (Result {status: Int 1, message: Text} b)
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

        // map : ∀a b. (a -> b) -> [a] -> [b]  (builtin → knot_relation_map)
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "map",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(b)))),
                    )),
                ),
            ),
        );

        // forEach : ∀a r. [a] -> (a -> IO {|r} {}) -> IO {|r} {}  (builtin →
        // knot_relation_for_each). Relation-FIRST arg order (unlike map).
        // IO-effect iterator: runs `action` on each row for its side effects.
        // The action's effect row propagates through to the caller (like fork).
        {
            let a = self.fresh_var();
            let r = self.fresh_var();
            self.bind_top(
                "forEach",
                Scheme::poly(
                    vec![a, r],
                    Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Fun(
                                Box::new(Ty::Var(a)),
                                Box::new(Ty::IO(
                                    BTreeSet::new(),
                                    Some(r),
                                    Box::new(Ty::unit()),
                                )),
                            )),
                            Box::new(Ty::IO(
                                BTreeSet::new(),
                                Some(r),
                                Box::new(Ty::unit()),
                            )),
                        )),
                    ),
                ),
            );
        }

        // fold : ∀a b. (b -> a -> b) -> b -> [a] -> b  (builtin → knot_relation_fold)
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "fold",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(b)),
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    )),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(b)),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(Ty::Var(b)),
                        )),
                    )),
                ),
            ),
        );

        // bind : ∀a b. (a -> Maybe b) -> Maybe a -> Maybe b  (builtin → knot_relation_bind)
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "bind",
            Scheme::poly(
                vec![a, b],
                Ty::Fun(
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(a)),
                        Box::new(Ty::Relation(Box::new(Ty::Var(b)))),
                    )),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(b)))),
                    )),
                ),
            ),
        );

        // traverse : ∀a b f. (a -> f b) -> [a] -> f [b]  (builtin →
        // knot_relation_traverse_kind). The applicative `f` stays a type
        // variable applied via Ty::App; Phase 5b resolves each call site's
        // applicative kind from the result type and codegen hands it to the
        // runtime, which needs it ONLY to pick `pure []` for empty inputs
        // (non-empty inputs dispatch on the first mapped element).
        let a = self.fresh_var();
        let b = self.fresh_var();
        let f = self.fresh_var();
        self.bind_top(
            "traverse",
            Scheme::poly(
                vec![a, b, f],
                Ty::Fun(
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(a)),
                        Box::new(Ty::App(Box::new(Ty::Var(f)), Box::new(Ty::Var(b)))),
                    )),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::App(
                            Box::new(Ty::Var(f)),
                            Box::new(Ty::Relation(Box::new(Ty::Var(b)))),
                        )),
                    )),
                ),
            ),
        );

        // take / drop : ∀s. Int -> s -> s  — overloaded over Text and
        // relations (formerly the Sequence trait). The open `s` covers both
        // (`take 3 rows`, `take 1 s`); codegen's inner closure dispatches on
        // the second argument's runtime tag (knot_text_take/drop vs
        // knot_relation_take/drop). SQL/pipe special cases in codegen
        // intercept source-pipe calls first.
        for name in ["take", "drop"] {
            let s = self.fresh_var();
            self.bind_top(
                name,
                Scheme::poly(
                    vec![s],
                    Ty::Fun(
                        Box::new(Ty::Int),
                        Box::new(Ty::Fun(Box::new(Ty::Var(s)), Box::new(Ty::Var(s)))),
                    ),
                ),
            );
        }

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

        // sum : ∀a. Num a => [a] -> a
        // Direct aggregation over a relation of numerics — no projection. The
        // `Num a` bound rejects nonsensical aggregations such as summing a
        // `[Text]`, which would otherwise type-check and then panic at runtime
        // ("cannot add Int + Text"). Units/refined aliases resolve to their
        // base primitive via `type_name_of`, so `Num Int`/`Num Float` discharge
        // unit- and refinement-typed elements unchanged: `sum ([1,2,3] : [Int])
        // : Int 1` and `sum ([1.0 : Float M, ...]) : Float M`. To sum a
        // projection, map first: `sum (map (\r -> r.amount) rows)`.
        let a = self.fresh_var();
        self.bind_top(
            "sum",
            Scheme {
                vars: vec![a],
                unit_vars: vec![],
                constraints: vec![TyConstraint {
                    trait_name: "Num".to_string(),
                    type_var: a,
                    span: Span::new(0, 0),
                }],
                effect_unions: vec![],
                unit_binops: vec![],
                ty: Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Var(a)),
                ),
            },
        );

        // avg : ∀a u. (a -> Float u) -> [a] -> Float u
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let float_u = Ty::float_with_unit(UnitTy::var(u));
            self.bind_top(
                "avg",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
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

        // minOn : ∀a b. Ord b => (a -> b) -> [a] -> b
        // The `Ord b` bound rejects projecting to an unorderable type (e.g. a
        // record with no `Ord` impl), which would otherwise type-check and then
        // fail at runtime. `Ord Int`/`Ord Float`/`Ord Text` (plus `deriving
        // (Ord)` ADTs) discharge the common cases; units/refined aliases route
        // to their base primitive via `type_name_of`.
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "minOn",
            Scheme {
                vars: vec![a, b],
                unit_vars: vec![],
                constraints: vec![TyConstraint {
                    trait_name: "Ord".to_string(),
                    type_var: b,
                    span: Span::new(0, 0),
                }],
                effect_unions: vec![],
                unit_binops: vec![],
                ty: Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Var(b)),
                    )),
                ),
            },
        );

        // maxOn : ∀a b. Ord b => (a -> b) -> [a] -> b  (see `minOn`)
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "maxOn",
            Scheme {
                vars: vec![a, b],
                unit_vars: vec![],
                constraints: vec![TyConstraint {
                    trait_name: "Ord".to_string(),
                    type_var: b,
                    span: Span::new(0, 0),
                }],
                effect_unions: vec![],
                unit_binops: vec![],
                ty: Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Var(b)),
                    )),
                ),
            },
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

        // upsertBy : ∀a. (a -> Bool) -> a -> [a] -> [a]
        // Replace matching elements with the given value, or append it if
        // none match.
        let a = self.fresh_var();
        self.bind_top(
            "upsertBy",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(a)),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        )),
                    )),
                ),
            ),
        );

        // head : ∀a. [a] -> Maybe a  (builtin → knot_relation_head).
        // Relation-FIRST arg order. First element as `Just {value: x}`, or
        // `Nothing {}` on the empty relation.
        {
            let a = self.fresh_var();
            self.bind_top(
                "head",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                    ),
                ),
            );
        }

        // findFirst : ∀a. [a] -> (a -> Bool) -> Maybe a  (builtin →
        // knot_relation_find_first). Relation-FIRST arg order. First row
        // satisfying `pred` as `Just {value: x}`, else `Nothing {}`.
        {
            let a = self.fresh_var();
            self.bind_top(
                "findFirst",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                            Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                        )),
                    ),
                ),
            );
        }

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

        // any : ∀a. (a -> Bool) -> [a] -> Bool
        {
            let a = self.fresh_var();
            self.bind_top(
                "any",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(Ty::Bool),
                        )),
                    ),
                ),
            );
        }

        // all : ∀a. (a -> Bool) -> [a] -> Bool
        {
            let a = self.fresh_var();
            self.bind_top(
                "all",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                            Box::new(Ty::Bool),
                        )),
                    ),
                ),
            );
        }

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

        // length : ∀u. Text -> Int u
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            self.bind_top(
                "length",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
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

        // stripUnit : ∀u. Int u -> Int — drop the unit tag from an Int
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "stripUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::int_with_unit(UnitTy::var(u))),
                        Box::new(Ty::Int),
                    ),
                },
            );
        }

        // withUnit : ∀u. Int -> Int u — attach a unit (caller must annotate result)
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "withUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Int),
                        Box::new(Ty::int_with_unit(UnitTy::var(u))),
                    ),
                },
            );
        }

        // stripFloatUnit : ∀u. Float u -> Float
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "stripFloatUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::float_with_unit(UnitTy::var(u))),
                        Box::new(Ty::Float),
                    ),
                },
            );
        }

        // withFloatUnit : ∀u. Float -> Float u
        {
            let u = self.fresh_unit_var();
            self.bind_top(
                "withFloatUnit",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Float),
                        Box::new(Ty::float_with_unit(UnitTy::var(u))),
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

        // bytesLength : ∀u. Bytes -> Int u
        {
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            self.bind_top(
                "bytesLength",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(Box::new(Ty::Bytes), Box::new(int_u)),
                },
            );
        }

        // bytesSlice : ∀u1 u2. Int u1 -> Int u2 -> Bytes -> Bytes
        {
            let u1 = self.fresh_unit_var();
            let u2 = self.fresh_unit_var();
            let int_u1 = Ty::int_with_unit(UnitTy::var(u1));
            let int_u2 = Ty::int_with_unit(UnitTy::var(u2));
            self.bind_top(
                "bytesSlice",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u1, u2],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
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

        // hash : ∀a. a -> Bytes  (BLAKE3, returns 32 bytes; Bytes/Text hash
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

        // bytesGet : ∀u1 u2. Int u1 -> Bytes -> Maybe Int u2
        // `Maybe`, not a bare `Int`: the index is often attacker-supplied, so an
        // out-of-bounds read yields `Nothing {}` instead of aborting the process.
        {
            let u1 = self.fresh_unit_var();
            let u2 = self.fresh_unit_var();
            let int_u1 = Ty::int_with_unit(UnitTy::var(u1));
            let int_u2 = Ty::int_with_unit(UnitTy::var(u2));
            self.bind_top(
                "bytesGet",
                Scheme {
                    vars: vec![],
                    unit_vars: vec![u1, u2],
                    constraints: vec![],
                    effect_unions: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u1),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Bytes),
                            Box::new(Ty::Con("Maybe".into(), vec![int_u2])),
                        )),
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

        // The three fallible crypto primitives return `Maybe Bytes` rather than a
        // bare `Bytes`. Keys and ciphertexts routinely arrive from untrusted
        // sources, and a wrong-length key or tampered ciphertext must surface as
        // `Nothing {}` for the caller to handle — never as a process abort, which
        // in a server is a remote DoS. `verify` already returns `Bool` for the
        // same reason.
        let maybe_bytes = Ty::Con("Maybe".into(), vec![Ty::Bytes]);

        // encrypt : Bytes -> Bytes -> IO {random} (Maybe Bytes)
        self.bind_top(
            "encrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(
                    Box::new(Ty::Bytes),
                    Box::new(Ty::IO(
                        BTreeSet::from([IoEffect::Random]),
                        None,
                        Box::new(maybe_bytes.clone()),
                    )),
                )),
            )),
        );

        // decrypt : Bytes -> Bytes -> Maybe Bytes
        self.bind_top(
            "decrypt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(maybe_bytes.clone()))),
            )),
        );

        // sign : Bytes -> Bytes -> Maybe Bytes
        self.bind_top(
            "sign",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Bytes),
                Box::new(Ty::Fun(Box::new(Ty::Bytes), Box::new(maybe_bytes))),
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


    // ── Declaration inference (phase 4) ──────────────────────────

    fn infer_declarations(&mut self, program: &ast::Expr) {
        // Named functions: `with`-record fields with a lambda body.
        for_each_named_fn(program, &mut |name, ty, body| {
            let body = match body {
                Some(b) => b,
                None => return,
            };
            {
                let scheme = self.lookup(name).cloned();
                let (expected, fresh_skolems, fresh_unit_skolems) = match scheme {
                    Some(scheme) => {
                        self.skolemise_scheme(&scheme, body.span)
                    }
                    None => (self.fresh(), Vec::new(), Vec::new()),
                };
                self.check_expr(body, &expected);
                        // Record-field funs with `^`-field constraints: register
                        // each under its record path (`fns.greet`) so the
                        // callsite resolver can find it through a field-access
                        // head. The constraint field types are converted while
                        // the body's annotation vars are live, so they share the
                        // field's quantified vars.
                        if let ast::ExprKind::Record(fields) = &body.node {
                            for f in fields {
                                if let Some(sig) = &f.sig {
                                    let saved_flag = self.in_type_annotation;
                                    let saved_av = std::mem::take(&mut self.annotation_vars);
                                    let saved_auv = std::mem::take(&mut self.annotation_unit_vars);
                                    self.in_type_annotation = true;
                                    let fimplicit: Vec<(String, Ty)> = sig
                                        .constraints
                                        .iter()
                                        .filter_map(|c| match c {
                                            ast::Constraint::ImplicitField { field, ty } => {
                                                Some((field.clone(), self.ast_type_to_ty(&ty)))
                                            }
                                            _ => None,
                                        })
                                        .collect();
                                    self.in_type_annotation = saved_flag;
                                    self.annotation_vars = saved_av;
                                    self.annotation_unit_vars = saved_auv;
                                    if !fimplicit.is_empty() {
                                        self.implicit_dict_fns.insert(
                                            format!("{name}.{}", f.name),
                                            fimplicit,
                                        );
                                    }
                                }
                            }
                        }
                        for s in &fresh_skolems {
                            self.skolems.remove(s);
                        }
                        for u in &fresh_unit_skolems {
                            self.unit_skolems.remove(u);
                        }
                        let inferred = self.apply(&expected);

                        // Remove the old monomorphic binding before
                        // generalizing, so its free variables don't block
                        // quantification.
                        if let Some(scope) = self.scopes.first_mut() {
                            scope.remove(name);
                        }

                        // If the function has explicit constraints in its
                        // annotation, rebuild the scheme from the annotation.
                        // (We already verified the body matches via unification.)
                        let has_constraints = ty
                            .as_ref()
                            .is_some_and(|ts| !ts.constraints.is_empty());
                        if has_constraints {
                            let ts = ty.as_ref().unwrap();
                            self.annotation_vars.clear();
                            self.annotation_unit_vars.clear();
                            self.in_type_annotation = true;
                            let mut constraints = Vec::new();
                            for c in &ts.constraints {
                                match c {
                                    ast::Constraint::Trait { trait_name, args } => {
                                        for arg in args {
                                            if let ast::TypeKind::Var(var_name) =
                                                &arg.node
                                            {
                                                let v = self.annotation_var(var_name);
                                                constraints.push(TyConstraint {
                                                    trait_name: trait_name.clone(),
                                                    type_var: v,
                                                    span: arg.span,
                                                });
                                            }
                                        }
                                    }
                                    ast::Constraint::ImplicitField { .. } => {
                                        // Recorded into `implicit_dict_fns` below.
                                    }
                                }
                            }
                            let unions_before =
                                self.pending_effect_unions.len();
                            let ann_ty = self.ast_type_to_ty(&ts.ty);
                            // Record the implicit-field dictionaries this
                            // function takes, in declared order, so each
                            // callsite resolves them from scope. The field
                            // types are converted NOW (while `annotation_vars`
                            // is live) so they share the scheme's quantified
                            // vars and unify with the body's dictionary use.
                            let implicit: Vec<(String, Ty)> = ts
                                .constraints
                                .iter()
                                .filter_map(|c| match c {
                                    ast::Constraint::ImplicitField { field, ty } => {
                                        Some((field.clone(), self.ast_type_to_ty(&ty)))
                                    }
                                    _ => None,
                                })
                                .collect();
                            if !implicit.is_empty() {
                                self.implicit_dict_fns.insert(name.to_string(), implicit);
                            }
                            self.in_type_annotation = false;
                            let mut vars: Vec<TyVar> = self
                                .annotation_vars
                                .values()
                                .copied()
                                .collect();
                            let mut unit_vars: Vec<UnitVar> = self
                                .annotation_unit_vars
                                .values()
                                .copied()
                                .collect();
                            // Capture `\/` effect unions from the annotation
                            // into the scheme (see pre_register).
                            let effect_unions: Vec<EffectUnion> = self
                                .pending_effect_unions
                                .split_off(unions_before);
                            for u in &effect_unions {
                                if !vars.contains(&u.result) {
                                    vars.push(u.result);
                                }
                                for s in &u.sources {
                                    if !vars.contains(s) {
                                        vars.push(*s);
                                    }
                                }
                            }
                            // Capture deferred `*`/`/` unit-composition checks
                            // whose result var resolves to a skolemized
                            // annotation variable, so each call-site
                            // instantiation gets its own fresh composition
                            // (mirrors `generalize`).
                            //
                            // The scheme's type was just rebuilt from the
                            // annotation with *fresh* vars (`ann_ty`), so the
                            // body-check skolems these binops reference no
                            // longer occur anywhere in it. Build a
                            // skolem→fresh-var map by walking the skolemised
                            // body type (`inferred`) against `ann_ty` in
                            // parallel — they share the annotation's structure
                            // — and re-point each captured binop at the vars
                            // that actually appear in the scheme. Without this
                            // the binop's result floats free of the return type
                            // at instantiation and end-of-inference resolution
                            // degrades to a vacuous `unify`, silently
                            // mis-typing e.g. `scale 3.0 M 4.0 M` as
                            // `Float M` instead of the `Float (M^2)` that
                            // contradicts the `a -> a -> a` signature. (B12)
                            let skolem_set: HashSet<TyVar> = fresh_skolems.iter().copied().collect();
                            let unit_skolem_set: HashSet<UnitVar> = fresh_unit_skolems.iter().copied().collect();
                            let mut walk_ty: HashMap<TyVar, TyVar> = HashMap::new();
                            let mut walk_unit: HashMap<UnitVar, UnitVar> = HashMap::new();
                            correspond_vars(&inferred, &ann_ty, &mut walk_ty, &mut walk_unit);
                            // Restrict the remaps to the body-check skolems: only
                            // those vanish from the rebuilt type. Other vars in an
                            // operand are outer-scope and must be left untouched.
                            let skolem_ty_subst: HashMap<TyVar, Ty> = walk_ty
                                .iter()
                                .filter(|(k, _)| skolem_set.contains(k))
                                .map(|(k, v)| (*k, Ty::Var(*v)))
                                .collect();
                            let skolem_unit_subst: HashMap<UnitVar, UnitVar> = walk_unit
                                .iter()
                                .filter(|(k, _)| unit_skolem_set.contains(k))
                                .map(|(k, v)| (*k, *v))
                                .collect();
                            let remapped_unit_targets: HashSet<UnitVar> =
                                skolem_unit_subst.values().copied().collect();
                            let pending_binops = std::mem::take(&mut self.deferred_unit_binops);
                            let mut captured_binops: Vec<DeferredUnitBinop> = Vec::new();
                            for b in pending_binops {
                                let resolved_result = self.apply(&Ty::Var(b.result));
                                if let Ty::Var(v) = &resolved_result
                                    && skolem_set.contains(v)
                                    && let Some(Ty::Var(fresh_result)) =
                                        skolem_ty_subst.get(v).cloned() {
                                        let mut lhs = self.subst_ty(&self.apply(&b.lhs), &skolem_ty_subst);
                                        let mut rhs = self.subst_ty(&self.apply(&b.rhs), &skolem_ty_subst);
                                        if !skolem_unit_subst.is_empty() {
                                            lhs = self.subst_unit_vars_in_ty(&lhs, &skolem_unit_subst);
                                            rhs = self.subst_unit_vars_in_ty(&rhs, &skolem_unit_subst);
                                        }
                                        if !vars.contains(&fresh_result) {
                                            vars.push(fresh_result);
                                        }
                                        let mut all_uv = Vec::new();
                                        collect_unit_vars_ordered(&lhs, &mut all_uv);
                                        collect_unit_vars_ordered(&rhs, &mut all_uv);
                                        for uv in &all_uv {
                                            if remapped_unit_targets.contains(uv) && !unit_vars.contains(uv) {
                                                unit_vars.push(*uv);
                                            }
                                        }
                                        captured_binops.push(DeferredUnitBinop {
                                            op: b.op,
                                            lhs,
                                            rhs,
                                            result: fresh_result,
                                            span: b.span,
                                        });
                                        continue;
                                    }
                                self.deferred_unit_binops.push(b);
                            }
                            self.bind_top(
                                name,
                                Scheme { vars, unit_vars, constraints, effect_unions, unit_binops: captured_binops, ty: ann_ty },
                            );
                        } else {
                            let applied = self.apply(&inferred);
                            self.in_top_level_generalize = true;
                            let scheme = self.generalize(&applied);
                            self.in_top_level_generalize = false;
                            self.bind_top(name, scheme);
                        }
                    }
        });

        // Views and derived relations.
        for_each_relation_marker(program, &mut |m| {
            match m {
                RelMarker::View { name, body: Some(body), .. } => {
                    let expected =
                        self.source_types.get(name).cloned().unwrap_or_else(
                            || self.fresh(),
                        );
                    // View bodies are relation comprehensions (codegen's
                    // `analyze_view`): `*view = *src` aliases the source
                    // relation and `*view = do ...` iterates its elements.
                    // Relation reads are IO-typed everywhere else, so type
                    // the body in comprehension mode (do-binds iterate
                    // elements) and peel any remaining IO wrapper before
                    // unifying with the view's relation type `[T]`.
                    let prev = self.in_view_comprehension;
                    self.in_view_comprehension = true;
                    let inferred = self.infer_expr(body);
                    self.in_view_comprehension = prev;
                    let inferred = match self.apply(&inferred) {
                        Ty::IO(_, _, inner) => (*inner).clone(),
                        other => other,
                    };
                    self.unify(&inferred, &expected, body.span);
                }
                RelMarker::Derived { name, body: Some(body), .. } => {
                    let expected = self
                        .derived_types
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| self.fresh());
                    let inferred = self.infer_expr(body);
                    // The body computes the relation via IO-typed reads, but
                    // the derived relation itself IS the resulting relation
                    // (`&name` references re-wrap it in IO at each use, see
                    // `ExprKind::DerivedRef`) — peel the IO wrapper before
                    // unifying. For un-annotated deriveds this also binds
                    // the fresh var from `collect_sources` to the plain
                    // `[T]` instead of `IO {} [T]` (which made `&name`
                    // produce a nested `IO (IO [T])`).
                    let inferred = match self.apply(&inferred) {
                        Ty::IO(_, _, inner) => (*inner).clone(),
                        other => other,
                    };
                    self.unify(&inferred, &expected, body.span);
                }
                _ => {}
            }
        });

        // Routes: check field collisions and rate-limit exprs.
        for_each_route_marker(program, &mut |_name, entries| {
            if let Some(entries) = entries {
                for entry in entries {
                    self.check_route_field_collisions(entry);
                    if let Some(rate_limit_expr) = &entry.rate_limit {
                        self.check_rate_limit_expr(entry, rate_limit_expr);
                    }
                }
            }
        });
    }

    /// Reject a route endpoint whose request inputs (path params, query
    /// params, body fields, request headers) share a field name. The handler
    /// receives a single record merging all of them, so a collision would
    /// silently keep only one type (`route_input_record_ty` uses a `BTreeMap`)
    /// while the desugared constructor carries both fields — diverging the
    /// inferred input type from the runtime decode. Better a clear error.
    fn check_route_field_collisions(&mut self, entry: &ast::RouteEntry) {
        let mut seen: std::collections::HashMap<&str, &'static str> =
            std::collections::HashMap::new();
        let mut inputs: Vec<(&str, &'static str, Span)> = Vec::new();
        for seg in &entry.path {
            if let ast::PathSegment::Param { name, ty } = seg {
                inputs.push((name.as_str(), "path parameter", ty.span));
            }
        }
        for qp in &entry.query_params {
            inputs.push((qp.name.as_str(), "query parameter", qp.value.span));
        }
        for bf in &entry.body_fields {
            inputs.push((bf.name.as_str(), "body field", bf.value.span));
        }
        for hf in &entry.request_headers {
            inputs.push((hf.name.as_str(), "request header", hf.value.span));
        }
        for (name, kind, span) in inputs {
            match seen.get(name) {
                Some(prev_kind) => self.error(
                    format!(
                        "duplicate route input field `{}`: declared as both a {} and a {}",
                        name, prev_kind, kind
                    ),
                    span,
                ),
                None => {
                    seen.insert(name, kind);
                }
            }
        }
    }

    /// Type-check a route's `rateLimit <expr>` clause. The expression must
    /// have type `{key: input -> RequestCtx -> Maybe a, limit: {requests: Int 1, window: Int Ms}}`
    /// for some `a`, where `input` is the same record the handler receives
    /// (path/query/body/headers fields). The runtime serializes the key via
    /// `show`, so no trait constraint is needed on `a`.
    fn check_rate_limit_expr(&mut self, entry: &ast::RouteEntry, expr: &ast::Expr) {
        let alpha = self.fresh_var();
        let input_ty = self.route_input_record_ty(entry);
        let request_ctx = self
            .aliases
            .get("RequestCtx")
            .cloned()
            .unwrap_or_else(|| Ty::Con("RequestCtx".into(), vec![]));
        let key_ty = Ty::Fun(
            Box::new(input_ty),
            Box::new(Ty::Fun(
                Box::new(request_ctx),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(alpha)])),
            )),
        );
        let limit_ty = Ty::Record(
            BTreeMap::from([
                ("requests".into(), Ty::Int),
                ("window".into(), Ty::int_with_unit(UnitTy::named("Ms"))),
            ]),
            None,
        );
        let expected = Ty::Record(
            BTreeMap::from([
                ("key".into(), key_ty),
                ("limit".into(), limit_ty),
            ]),
            None,
        );
        self.check_expr(expr, &expected);
        // The runtime serializes the key via `show`, which works for all
        // types, so no trait constraint is needed on the key value type.
    }


    // ── Constraint checking ─────────────────────────────────────

    /// Record a trait requirement for `ty` arising at `span` (e.g. an `Ord`
    /// constraint at a `<` operator). Concrete types are checked immediately
    /// against `known_impls`; type variables are deferred — if they later
    /// resolve to a concrete type, `check_constraints` validates them.
    /// Skolem variables (signature-quantified) are validated once the
    /// function body is finished, in `check_skolem_constraints`.
    fn require_trait(&mut self, trait_name: &str, ty: &Ty, span: Span) {
        let resolved = self.apply(ty);
        match resolved.peel_alias() {
            Ty::Error => return,
            Ty::Var(v) => {
                let v = *v;
                let seq = self.next_constraint_seq();
                self.deferred_constraints.push(DeferredConstraint {
                    trait_name: trait_name.to_string(),
                    type_var: v,
                    span,
                    seq,
                });
                return;
            }
            _ => {}
        }
        if let Some(type_name) = self.type_name_of(&resolved) {
            let key = (trait_name.to_string(), type_name.clone());
            if !self.known_impls.contains(&key) {
                self.error(
                    format!(
                        "no implementation of trait '{}' for type '{}'",
                        trait_name, type_name
                    ),
                    span,
                );
            }
        }
    }

    /// After a function body is checked, any deferred constraint that
    /// resolves to one of the function's signature skolems must correspond
    /// to a constraint declared in the signature. Otherwise the body needs
    /// a polymorphism the signature didn't promise — e.g. using `<` on
    /// `a -> a -> a` without `Ord a =>`.
    /// Allocate the next push-order sequence number for a deferred constraint.
    fn next_constraint_seq(&mut self) -> u64 {
        let s = self.next_constraint_seq;
        self.next_constraint_seq += 1;
        s
    }

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
                    // All deferred constraints now carry a real call-site span
                    // (`instantiate_at` stamps the use site; the lone dummy-span
                    // producer was routed through it too), so report the missing
                    // impl unconditionally rather than silently dropping
                    // dummy-spanned obligations.
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
            // Local binding types are monomorphic (no `forall`-quantified unit
            // vars), so an unsolved unit var is one inference never pinned —
            // e.g. the literal `2.0` in `base * 2.0`, whose fresh var `Mul`
            // can't fold into the other operand's `M`. Runtime codegen already
            // defaults such vars to dimensionless; mirror that here so the
            // hint shows `Float M`, not a dangling `Float M*u`.
            let applied = default_free_unit_vars(&applied);
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
        Ty::Unit(u) => {
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

/// Walk two structurally-parallel types, recording how the type and unit
/// variables in `from` correspond to those in `to`. Both must derive from the
/// same annotation AST — they differ only in variable identities — as happens
/// when a scheme's type is rebuilt from its annotation with fresh vars: the
/// skolemised body-check type and the rebuilt type share their shape. Used to
/// re-point deferred unit-binops captured against body-check skolems onto the
/// rebuilt scheme's fresh vars (see the `has_constraints` branch of
/// `infer_declarations`, B12).
fn correspond_vars(
    from: &Ty,
    to: &Ty,
    ty_map: &mut HashMap<TyVar, TyVar>,
    unit_map: &mut HashMap<UnitVar, UnitVar>,
) {
    match (from, to) {
        (Ty::Var(a), Ty::Var(b)) => {
            ty_map.entry(*a).or_insert(*b);
        }
        (Ty::Fun(p1, r1), Ty::Fun(p2, r2)) => {
            correspond_vars(p1, p2, ty_map, unit_map);
            correspond_vars(r1, r2, ty_map, unit_map);
        }
        (Ty::Relation(a), Ty::Relation(b)) => {
            correspond_vars(a, b, ty_map, unit_map)
        }
        (Ty::Con(_, a1), Ty::Con(_, a2)) => {
            for (x, y) in a1.iter().zip(a2) {
                correspond_vars(x, y, ty_map, unit_map);
            }
        }
        (Ty::App(f1, a1), Ty::App(f2, a2)) => {
            correspond_vars(f1, f2, ty_map, unit_map);
            correspond_vars(a1, a2, ty_map, unit_map);
        }
        (Ty::Record(f1, r1), Ty::Record(f2, r2)) => {
            for (k, v) in f1 {
                if let Some(w) = f2.get(k) {
                    correspond_vars(v, w, ty_map, unit_map);
                }
            }
            if let (Some(a), Some(b)) = (r1, r2) {
                ty_map.entry(*a).or_insert(*b);
            }
        }
        (Ty::Variant(c1, r1), Ty::Variant(c2, r2)) => {
            for (k, v) in c1 {
                if let Some(w) = c2.get(k) {
                    correspond_vars(v, w, ty_map, unit_map);
                }
            }
            if let (Some(a), Some(b)) = (r1, r2) {
                ty_map.entry(*a).or_insert(*b);
            }
        }
        (Ty::IO(_, r1, i1), Ty::IO(_, r2, i2)) => {
            if let (Some(a), Some(b)) = (r1, r2) {
                ty_map.entry(*a).or_insert(*b);
            }
            correspond_vars(i1, i2, ty_map, unit_map);
        }
        (Ty::Unit(u1), Ty::Unit(u2)) => {
            correspond_unit_vars(u1, u2, unit_map);
        }
        (Ty::Forall(_, i1), Ty::Forall(_, i2)) => {
            correspond_vars(i1, i2, ty_map, unit_map)
        }
        (Ty::Alias(_, i1), Ty::Alias(_, i2)) => {
            correspond_vars(i1, i2, ty_map, unit_map)
        }
        // Look through a one-sided alias so shapes still line up.
        (Ty::Alias(_, i1), other) => {
            correspond_vars(i1, other, ty_map, unit_map)
        }
        (other, Ty::Alias(_, i2)) => {
            correspond_vars(other, i2, ty_map, unit_map)
        }
        (Ty::Assoc(_, i1), Ty::Assoc(_, i2)) => {
            correspond_vars(i1, i2, ty_map, unit_map)
        }
        _ => {}
    }
}

/// Pair the unit variables of two structurally-parallel units by exponent
/// (see `correspond_vars`). The common shape is a single variable per unit
/// (`Float u`), which pairs unambiguously; ties within one exponent pair in
/// `BTreeMap` iteration order.
fn correspond_unit_vars(
    from: &UnitTy,
    to: &UnitTy,
    unit_map: &mut HashMap<UnitVar, UnitVar>,
) {
    let mut targets_by_exp: BTreeMap<i32, Vec<UnitVar>> = BTreeMap::new();
    for (&v, &e) in &to.vars {
        targets_by_exp.entry(e).or_default().push(v);
    }
    let mut next: BTreeMap<i32, usize> = BTreeMap::new();
    for (&v, &e) in &from.vars {
        if let Some(candidates) = targets_by_exp.get(&e) {
            let idx = next.entry(e).or_insert(0);
            if let Some(&target) = candidates.get(*idx) {
                unit_map.entry(v).or_insert(target);
                *idx += 1;
            }
        }
    }
}

fn collect_vars_ordered(ty: &Ty, out: &mut Vec<TyVar>) {
    match ty {
        Ty::Var(v)
            if !out.contains(v) => {
                out.push(*v);
            }
        Ty::Fun(p, r) => {
            collect_vars_ordered(p, out);
            collect_vars_ordered(r, out);
        }
        Ty::Record(fields, row) => {
            for t in fields.values() {
                collect_vars_ordered(t, out);
            }
            if let Some(rv) = row
                && !out.contains(rv) {
                    out.push(*rv);
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
            if let Some(rv) = row
                && !out.contains(rv) {
                    out.push(*rv);
                }
        }
        Ty::App(f, a) => {
            collect_vars_ordered(f, out);
            collect_vars_ordered(a, out);
        }
        Ty::IO(_, row, inner) => {
            if let Some(rv) = row
                && !out.contains(rv) {
                    out.push(*rv);
                }
            collect_vars_ordered(inner, out);
        }
        Ty::EffectRow(_, row) => {
            if let Some(rv) = row
                && !out.contains(rv) {
                    out.push(*rv);
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
        Ty::Assoc(_, inner) => collect_vars_ordered(inner, out),
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
    _names: &HashMap<TyVar, usize>,
) -> String {
    let row_name = row.map(|_| "_".to_string());
    if effects.is_empty() {
        return match row_name {
            Some(name) => name,
            None => "{}".into(),
        };
    }
    let effects_str = format_io_effects_coalesced(effects).join(", ");
    match row_name {
        Some(name) => format!("{{{} | {}}}", effects_str, name),
        None => format!("{{{}}}", effects_str),
    }
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
        Ty::Text => "Text".into(),
        Ty::Bool => "Bool".into(),
        Ty::Bytes => "Bytes".into(),
        Ty::Uuid => "Uuid".into(),
        Ty::Assoc(name, inner) => {
            format!("{} {}", name, display_ty_clean_inner(inner, names, unit_names, true))
        }
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
            // Unit-bearing Int/Float → `Int u`/`Float u`, collapsing to
            // `Int`/`Float` when dimensionless.
            if (name == "Int" || name == "Float") && args.len() == 1 {
                if let Ty::Unit(u) = args[0].peel_alias() {
                    if u.is_dimensionless() {
                        return name.clone();
                    }
                    return format!("{} {}", name, display_unit_clean(u, unit_names));
                }
            }
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
            format!("IO {} {}", effects_str, display_ty_clean(inner, names, unit_names))
        }
        Ty::EffectRow(effects, row) => {
            display_effect_set_clean(effects, *row, names)
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
        // Standalone `Ty::Unit` only appears as the arg of a unit-bearing
        // Int/Float `Con`; the `Con` arm renders it. Defensive fallback:
        Ty::Unit(u) => format!("Unit<{}>", display_unit_clean(u, unit_names)),
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
        // `^x` reads a record field, never a source relation directly.
        ast::ExprKind::ImplicitRef(_) => false,
        ast::ExprKind::Var(name) => {
            if aliases.get(name).map(|s| s.as_str()) == Some(source_name) {
                return true;
            }
            // Fold through let bindings: `let foo = ...; *rel = foo`
            // counts as referencing the source if the body does.
            if visited.insert(name.clone())
                && let Some(body) = let_bindings.get(name) {
                    let result = value_references_source_inner(
                        body, source_name, aliases, let_bindings, visited,
                    );
                    visited.remove(name);
                    return result;
                }
            false
        }
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::DerivedRef(_) => false,
        ast::ExprKind::TypeCtor { .. } | ast::ExprKind::DataCtor { .. } | ast::ExprKind::SourceDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => false,
        ast::ExprKind::RouteDecl { .. } | ast::ExprKind::RouteCompositeDecl { .. } => false,
        ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
            value_references_source_inner(
                body, source_name, aliases, let_bindings, visited,
            )
        }
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
        ast::ExprKind::With { record, body } => {
            value_references_source_inner(record, source_name, aliases, let_bindings, visited)
                || value_references_source_inner(
                    body, source_name, aliases, let_bindings, visited,
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
        ast::ExprKind::TimeUnitLit { value, .. } => value_references_source_inner(
            value, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::Annot { expr, .. } => value_references_source_inner(
            expr, source_name, aliases, let_bindings, visited,
        ),
        ast::ExprKind::Serve { handlers, .. } => handlers.iter().any(|h| {
            value_references_source_inner(
                &h.body, source_name, aliases, let_bindings, visited,
            )
        }),
    }
}

// ── Public API ────────────────────────────────────────────────────

/// What `check` hands to the later passes: diagnostics, the inferred types
/// themselves, and the span-keyed facts codegen cannot re-derive on its own
/// (monad kinds, refine/parseJson targets, `elem` pushdown eligibility, `show`
/// units, `sum`'s numeric result type).
pub type CheckOutput = (
    Vec<Diagnostic>,
    MonadInfo,
    TypeInfo,
    LocalTypeInfo,
    RefineTargets,
    RefinedTypeInfoMap,
    FromJsonTargets,
    ElemPushdownOk,
    ShowUnitStrings,
    SumFloatSpans,
    RelationFieldSpans,
    WithFields,
    TypeArgSpans,
    ImplicitRefs,
    ImplicitDictArgs,
);

/// Run type inference on a parsed module. Returns diagnostics,
/// resolved monad info for desugared do-blocks, and inferred type info
/// mapping declaration names to their display type strings.
///
/// The module is taken by `&mut` because inference also *elaborates* it: the
/// desugarer emits `__result` markers for do-block final bare expressions,
/// and only the type checker can tell whether each one means `pure e` or `e`
/// (see `resolve_result_markers`). Every marker is rewritten away here, so
/// later passes never see one.
///
/// Runs on a grown stack: a desugared `do` block nests one `__bind` per
/// statement, and `infer_expr` recurses through every level.
pub fn check(program: &mut ast::Expr) -> CheckOutput {
    crate::stack::grow(|| check_inner(program))
}

fn check_inner(program: &mut ast::Expr) -> CheckOutput {
    let mut infer = Infer::new();

    // Every user-written numeric type must carry an explicit unit (bare
    // `Int`/`Float` is rejected). Value annotations already enforce this via
    // `in_type_annotation`; enable it globally so declaration-level types —
    // aliases, data fields, sources/views/derived, routes, trait methods,
    // impls — are checked too. Builtins are registered from Rust `Ty` (not
    // `ast_type_to_ty`) and the prelude is fully unit-annotated, so neither
    // is affected.
    infer.enforce_units = true;

    // Phase 1: Collect type aliases, data types, constructors
    infer.collect_types(program);

    // Phase 2: Register source/view/derived relation types
    infer.collect_sources(program);

    // Phase 2b: Collect known trait implementations
    infer.collect_impls(program);
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
    // Maybe's HKT impls are registered intrinsically in codegen
    // (`register_builtin_maybe_impls`), so the checker treats them as known.
    for trait_name in &["Functor", "Applicative", "Monad", "Alternative"] {
        infer
            .known_impls
            .insert((trait_name.to_string(), "Maybe".to_string()));
    }
    // Primitive impls registered intrinsically in codegen. `==`/`<` on these
    // dispatch to the runtime `knot_value_eq` / `knot_value_compare` fallbacks
    // (no user impl required). `knot_value_eq` compares records, variants,
    // relations, bytes and uuids structurally, so those get intrinsic `Eq`.
    // `Ord` stays minimal (matching the existing conservative design — e.g.
    // `Bool` is deliberately not orderable): ADTs that opt in via
    // `deriving (Ord)` are registered by `collect_impls` and ordered through
    // the structural recursion in the runtime's `compare_values`.
    for ty in &["Int", "Float", "Text", "Bool", "Bytes", "Uuid", "Record", "Variant", "[]"] {
        infer.known_impls.insert(("Eq".to_string(), ty.to_string()));
    }
    for ty in &["Int", "Float", "Text"] {
        infer.known_impls.insert(("Ord".to_string(), ty.to_string()));
    }
    for ty in &["Int", "Float"] {
        infer.known_impls.insert(("Num".to_string(), ty.to_string()));
    }
    infer.known_impls.insert(("Semigroup".to_string(), "Text".to_string()));
    infer.known_impls.insert(("Semigroup".to_string(), "[]".to_string()));
    infer.known_impls.insert(("Sequence".to_string(), "Text".to_string()));
    infer.known_impls.insert(("Sequence".to_string(), "[]".to_string()));

    // Phase 3: Pre-register top-level names (builtins, functions, trait methods)
    infer.pre_register(program);

    // Phase 4: Infer all declaration bodies
    infer.infer_declarations(program);

    // Phase 4a: Resolve any remaining effect-union (`r1 \/ r2`) constraints
    // so their result rows get bound to the union of their sources' effects.
    infer.resolve_pending_effect_unions();

    // Phase 4b: Resolve refine expression targets.
    // This must run BEFORE deferred-constraint checking (phase 4c) and
    // monad-kind resolution (phase 5): refine-target resolution unifies
    // type variables (the refined value against the target's base type)
    // that constraints and do-block monad vars may resolve through.
    // Running it later meant constraints on such vars were silently
    // skipped (`Ty::Var` skip in check_constraints) and monad kinds were
    // prematurely defaulted to Relation.
    //
    // The contextual binding of alpha (from an annotation or call site)
    // wins; otherwise fall back to matching the refined expression's type
    // against the declared refined types' base types — deterministically
    // (sorted by name) and erroring when several refined types share the
    // base.
    let mut refine_targets = RefineTargets::new();
    let refine_vars = infer.refine_vars.clone();
    for (span, var, inner_ty) in &refine_vars {
        let resolved = infer.apply(&Ty::Var(*var));
        if let Ty::Con(name, args) = &resolved
            && args.is_empty() && infer.refined_types.contains_key(name) {
                // Context named the refined type — check the refined value
                // against its *fully-resolved* base type (walking any chain of
                // refined aliases, e.g. Age → Nat → Int), then record the
                // target. Resolving to the ultimate base (not the immediate
                // one) matters now that subsumption is directional: the value
                // here is a raw base being introduced via `refine`, so an
                // immediate base that is itself refined (`Age = Nat where …`)
                // would otherwise look like an unchecked Int→Nat and be
                // rejected.
                let name = name.clone();
                match infer.resolve_refined_base(&name, *span) {
                    Some(base) => infer.unify(inner_ty, &base, *span),
                    None => continue, // cycle already reported
                }
                refine_targets.insert(*span, name);
                continue;
            }
        // Alpha is unconstrained or resolved to a base type (e.g. Int via
        // do-block subsumption). Match against refined types' base types.
        let key_ty = match &resolved {
            Ty::Var(_) => infer.apply(inner_ty),
            other => other.clone(),
        };
        // The refined expression may itself already have the refined type
        // (e.g. `refine (x : Nat)`).
        if let Ty::Con(name, args) = &key_ty
            && args.is_empty() && infer.refined_types.contains_key(name) {
                refine_targets.insert(*span, name.clone());
                continue;
            }
        let mut candidates: Vec<String> = infer
            .refined_types
            .iter()
            .filter(|(_, (base_ty, _))| {
                *base_ty == key_ty || infer.refined_base_compatible(base_ty, &key_ty)
            })
            .map(|(name, _)| name.clone())
            .collect();
        candidates.sort();
        match candidates.len() {
            1 => {
                let name = candidates.remove(0);
                refine_targets.insert(*span, name);
            }
            0 => {
                infer.errors.push((
                    format!(
                        "cannot infer refined type target for refine expression (got {}); use a context that constrains the type (e.g., pass to a function expecting a refined type)",
                        infer.display_ty(&resolved)
                    ),
                    *span,
                ));
            }
            _ => {
                infer.errors.push((
                    format!(
                        "ambiguous refined type target for refine expression: {} all refine {} — add a type annotation to pick one (e.g. `refine x : Result RefinementError {}`)",
                        candidates.join(", "),
                        infer.display_ty(&key_ty),
                        candidates[0]
                    ),
                    *span,
                ));
            }
        }
    }

    // Phase 4b2: Resolve unit-composition checks deferred at `*`/`/` nodes
    // (one operand was an unresolved type variable at the binop — e.g. a
    // record field on a lambda param pinned later by its call site). Must
    // run before check_constraints so the Num constraints it registers are
    // still checked.
    infer.resolve_deferred_unit_binops();

    // Phase 4b3: Settle desugared do-blocks' final bare expressions — `pure e`
    // or `e` itself — and rewrite the markers out of the AST. Runs before
    // check_constraints so the unifications it performs are visible to the
    // deferred trait checks.
    resolve_result_markers(&mut infer, program);

    // Phase 4c: Check deferred trait constraints
    infer.check_constraints();

    // Phase 4d: Compress substitution chains for faster resolution
    infer.compress_substitution();

    // Phase 5: Resolve monad types from desugared do-blocks
    let mut monad_info = MonadInfo::new();
    let monad_vars = infer.monad_vars.clone();
    let empty_spans = infer.empty_spans.clone();
    for (span, m_var) in &monad_vars {
        let resolved = infer.apply(&Ty::Var(*m_var));
        // When the monad type variable is still unresolved (a flexible
        // `Ty::Var` after full inference), codegen dispatches to
        // `knot_relation_bind` by default. This is correct for `main = do …`
        // and other top-level Relation do-blocks, but silently wrong for a
        // let-generalized monad-polymorphic function whose monad var was
        // quantified and never pinned to a concrete instance. We cannot
        // distinguish these two cases reliably after skolem cleanup, so we
        // keep the Relation default but emit a diagnostic *warning* (not an
        // error) so the user is alerted when the default may be wrong.
        if matches!(resolved.peel_alias(), Ty::Var(_)) {
            // Only warn for monad vars that were let-generalized (quantified
            // into a local let-binding's scheme), not for top-level do-blocks
            // where the Relation default is correct.
            if infer.generalized_monad_spans.contains(span) {
                infer.errors.push((
                    "do-block dispatches to Relation by default: the monad type \
                     variable was generalized and never resolved to a concrete \
                     monad. Add a type annotation to disambiguate."
                        .to_string(),
                    *span,
                ));
            }
            let kind = MonadKind::Relation;
            monad_info.insert(*span, kind);
            continue;
        }
        let kind = monad_kind_of(&resolved);
        // A `__empty` (from a `where` guard or `empty` in a comprehension)
        // dispatches through the monad's `Alternative` impl. `[]`, `Maybe`,
        // and `Result` always have one; a user-defined monad with only
        // Functor/Applicative/Monad does not, and would otherwise blow up with
        // a missing-impl panic in codegen. Surface it as a clean diagnostic.
        if empty_spans.contains(span) {
            let alt_ty = match &kind {
                MonadKind::Relation => Some("[]".to_string()),
                MonadKind::Adt(name) => Some(name.clone()),
                MonadKind::IO => Some("IO".to_string()),
            };
            if let Some(ty_name) = alt_ty
                && !infer.known_impls.contains(&("Alternative".to_string(), ty_name.clone())) {
                    infer.error(
                        format!(
                            "do-block uses a 'where' guard (or empty), which requires an \
                             Alternative impl, but '{}' has no Alternative instance",
                            ty_name
                        ),
                        *span,
                    );
                }
        }
        // Synthesized helper spans (globally unique, see desugar.rs) also
        // alias their originating do-block's real span — LSP monad inlay
        // hints look up `monad_info[do_span]`.
        if let Some(origin) = crate::desugar::synth_span_origin(*span) {
            monad_info.entry(origin).or_insert_with(|| kind.clone());
        }
        monad_info.insert(*span, kind);
    }

    // Phase 5b: Resolve applicative kinds for `traverse f rel` call sites
    // over relation containers, keyed by the call expression's span. Codegen
    // passes the kind to the runtime, which uses it ONLY to pick the
    // empty-input result (`pure []` in the right applicative) — the runtime
    // otherwise dispatches on the first mapped element, which doesn't exist
    // for empty inputs (the old behavior unconditionally returned the
    // Relation result `[[]]`).
    for (span, res_v, cont_v) in &infer.traverse_calls {
        let container = infer.apply(&Ty::Var(*cont_v));
        if !matches!(container.peel_alias(), Ty::Relation(_)) {
            continue; // other Traversables dispatch through their own impls
        }
        let resolved = infer.apply(&Ty::Var(*res_v));
        // Open variants from case-pattern unification name the constructors
        // rather than the ADT — recognize the built-in Maybe/Result shapes.
        let kind = match resolved.peel_alias() {
            Ty::Variant(ctors, _)
                if !ctors.is_empty()
                    && ctors.keys().all(|k| k == "Just" || k == "Nothing") =>
            {
                MonadKind::Adt("Maybe".into())
            }
            Ty::Variant(ctors, _)
                if !ctors.is_empty()
                    && ctors.keys().all(|k| k == "Ok" || k == "Err") =>
            {
                MonadKind::Adt("Result".into())
            }
            _ => monad_kind_of(&resolved),
        };
        monad_info.entry(*span).or_insert(kind);
    }

    // Phase 5c: Resolve the numeric type of each full `sum f rel` call, keyed
    // by the call span. Codegen passes it to the runtime, which uses it ONLY
    // for the EMPTY-input result: no summands means no value to take the type
    // from, and the zero must still be the one the program was checked against
    // (`Float 0.0`, not `Int 0`).
    let mut sum_float_spans = SumFloatSpans::new();
    for (span, res_v) in &infer.sum_calls {
        if infer.apply(&Ty::Var(*res_v)).peel_alias().is_float_like() {
            sum_float_spans.insert(*span);
        }
    }

    // Phase 5d: Sieve the field accesses whose field turned out to be a
    // relation. `t.members : [{who: Text}]` makes `m <- t.members` a relation
    // bind (inference types `m` as the ELEMENT), so codegen has to iterate the
    // rows there rather than bind the relation whole.
    let mut relation_fields = RelationFieldSpans::new();
    for (span, ty) in &infer.field_accesses {
        if matches!(infer.apply(ty).peel_alias(), Ty::Relation(_)) {
            relation_fields.insert(*span);
        }
    }

    // Export refined type predicates for codegen
    let refined_type_info: RefinedTypeInfoMap = infer
        .refined_types
        .iter()
        .map(|(name, (_, pred))| (name.clone(), pred.clone()))
        .collect();

    // Phase 7: Resolve parseJson call targets for compile-time FromJSON
    // dispatch and Maybe-aware wire decoding.
    let mut from_json_targets = FromJsonTargets::new();
    for (span, var) in &infer.from_json_calls {
        let resolved = infer.apply(&Ty::Var(*var));
        // `parseJson : Text -> Maybe a` — the JSON decodes to the inner type
        // `a`, which is then `Just`-wrapped (or `Nothing` on failure). The
        // type name and wire descriptor describe that inner type, not the
        // surrounding `Maybe`.
        let inner = match resolved.peel_alias() {
            Ty::Con(n, args) if n == "Maybe" && args.len() == 1 => args[0].clone(),
            other => other.clone(),
        };
        let type_name = ty_to_type_name(&inner);
        // Carry the wire schema whenever it constrains the shape at all (i.e.
        // anything other than the fully-opaque `*` catch-all). Beyond
        // normalizing Maybe positions (`?`), the typed decoder shape-checks the
        // decoded value: `null` for a required scalar and structurally-wrong
        // values (e.g. a forged `__knot_ctor` where a record is declared) fail
        // the parse instead of leaking a mistyped value into the `Just`. A bare
        // `*` (type var / non-record ADT) carries no schema, so ADT
        // round-tripping still flows through the schema-less decoder.
        let wire_schema =
            Some(ty_to_wire_descriptor(&inner)).filter(|d| d != "*");
        if type_name.is_some() || wire_schema.is_some() {
            from_json_targets.insert(*span, FromJsonTarget { type_name, wire_schema });
        }
    }

    // Phase 8: Resolve the unit of measure at each `show` call site. Units are
    // a compile-time overlay — fully erased by codegen — so a unit suffix can
    // only be printed if it is captured here and emitted as a constant.
    let mut show_unit_strings = ShowUnitStrings::new();
    for (span, ty) in std::mem::take(&mut infer.show_calls) {
        // Peel aliases so a refined/aliased numeric (`type Metres = Float M`)
        // still shows its unit.
        let resolved = infer.apply(&ty);
        // A *refined* alias (`type Pos = Metres where …`) is a nullary
        // `Con(name, [])`, not an `Alias`, so `peel_alias`/`unit_of` can't
        // see through it to the unit-bearing base. Resolve it to its refined
        // base (following stacked refined chains) before extracting the unit.
        let resolved_owned;
        let resolved = match resolved.peel_alias() {
            Ty::Con(name, args)
                if args.is_empty() && infer.refined_types.contains_key(name) =>
            {
                match infer.resolve_refined_base(name, span) {
                    Some(base) => {
                        resolved_owned = base;
                        &resolved_owned
                    }
                    None => continue,
                }
            }
            other => other,
        };
        let unit = match resolved.peel_alias().unit_of() {
            Some(u) => infer.apply_unit(u),
            None => continue,
        };
        // A unit still carrying variables is polymorphic — inside a unit-generic
        // function the concrete unit is not known at this call site, and DESIGN
        // specifies `show` prints just the number there. `apply` already folds a
        // dimensionless unit back to plain `Int`/`Float`, so the emptiness check
        // is only a guard against a hand-built dimensionless `Unit`.
        if !unit.vars.is_empty() || unit.is_dimensionless() {
            continue;
        }
        show_unit_strings.insert(span, unit.display());
    }

    let type_info = infer.extract_type_info();
    let local_type_info = infer.extract_local_type_info();
    let elem_pushdown_ok = infer.elem_pushdown_ok.clone();
    let with_fields: WithFields = infer.with_fields.iter().cloned().collect();
    let type_arg_spans: TypeArgSpans = infer.type_arg_spans.clone();
    let implicit_refs: ImplicitRefs = infer.implicit_refs.clone();
    let implicit_dict_args: ImplicitDictArgs = infer.implicit_dict_args.clone();

    (infer.to_diagnostics(), monad_info, type_info, local_type_info, refine_targets, refined_type_info, from_json_targets, elem_pushdown_ok, show_unit_strings, sum_float_spans, relation_fields, with_fields, type_arg_spans, implicit_refs, implicit_dict_args)
}


/// Whether a scheme's type is a two-argument function — the shape the
/// check-lambda-arguments-last path in `infer_expr`'s `App` arm relies on.
/// Peels `Forall` binders; nothing else, since the head's type comes straight
/// from its scheme and has not been substituted into yet.
fn takes_two_args(ty: &Ty) -> bool {
    match ty {
        Ty::Forall(_, body) => takes_two_args(body),
        Ty::Fun(_, rest) => matches!(rest.as_ref(), Ty::Fun(..)),
        _ => false,
    }
}

/// The number of leading `Ty::Fun` arrows in a type — a function's curried
/// arity. Peels `Forall` wrappers.
fn curry_arity(ty: &Ty) -> usize {
    match ty {
        Ty::Forall(_, body) => curry_arity(body),
        Ty::Fun(_, rest) => 1 + curry_arity(rest),
        _ => 0,
    }
}

/// Dotted field path for a `Var`-rooted field-access chain (`fns.greet` →
/// `Some("fns.greet")`), used to key record-field fun dictionaries. Returns
/// `None` for anything else (a `Var` head, or a non-`Var` base).
fn implicit_dict_head_path(expr: &ast::Expr) -> Option<String> {
    let mut fields = Vec::new();
    let mut cur = expr;
    loop {
        match &cur.node {
            ast::ExprKind::FieldAccess { expr: base, field } => {
                fields.push(field.as_str());
                cur = base;
            }
            ast::ExprKind::Var(root) => {
                fields.push(root.as_str());
                fields.reverse();
                return Some(fields.join("."));
            }
            _ => return None,
        }
    }
}

// ── Do-block final-expression resolution (`__result`) ─────────────

/// The monad a type is an action in, if it is one at all — the counterpart of
/// `monad_kind_of`, which classifies the *monad constructor*. Unlike that
/// function this one never guesses: anything that is not recognisably `m a`
/// (a plain `Int`, an unresolved var, a record) yields `None`.
fn action_monad_of(ty: &Ty) -> Option<MonadKind> {
    match ty.peel_alias() {
        Ty::IO(..) => Some(MonadKind::IO),
        Ty::Relation(_) => Some(MonadKind::Relation),
        // Saturated (`Maybe Int`) or partially applied (`Result e`) ADTs. A
        // nullary `Con(name, [])` is a plain data type, not an action.
        // Unit-bearing `Int`/`Float` (`Con("Int", [Unit(_)]`) are also plain
        // values, not actions — their single argument is a unit annotation,
        // not a payload. Without this guard, a `do` block whose final
        // expression is a unit-bearing number (e.g. `do { …; (5.0 : Float M)
        // }`) would misclassify the number as an `Adt("Int")` action and
        // try to treat it as the block's monad instead of wrapping in `pure`.
        Ty::Con(name, args)
            if !args.is_empty() && !(name == "Int" || name == "Float") =>
        {
            Some(MonadKind::Adt(name.clone()))
        }
        Ty::App(f, _) => action_monad_of(f).or_else(|| match f.peel_alias() {
            Ty::TyCon(name) if name == "[]" => Some(MonadKind::Relation),
            Ty::TyCon(name) if name == "IO" => Some(MonadKind::IO),
            Ty::TyCon(name) => Some(MonadKind::Adt(name.clone())),
            _ => None,
        }),
        // A constructor expression (`Just {value: x}`, `Ok {value: x}`) types
        // as an open variant rather than the ADT itself — recognise the
        // built-in monadic shapes, as `traverse` resolution already does.
        Ty::Variant(ctors, _) if !ctors.is_empty() => {
            if ctors.keys().all(|k| k == "Just" || k == "Nothing") {
                Some(MonadKind::Adt("Maybe".into()))
            } else if ctors.keys().all(|k| k == "Ok" || k == "Err") {
                Some(MonadKind::Adt("Result".into()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Settle every `__result e` marker the desugarer left behind, then rewrite it
/// out of the AST.
///
/// `do { …; e }` with a bare final `e` means one of two things, and only the
/// types tell them apart:
///
///   * `e` is already an action in the block's monad — `do { act x; loop rest }`
///     — and the block's result IS `e`. Wrapping it in `pure` would type the
///     block as `m (m a)`.
///   * `e` is a plain value — `do { x <- act; show x }` — and the block's
///     result is `pure e`.
///
/// So compare the block's monad `m` (fixed by the enclosing `__bind`, and by
/// the declaration's annotation) against the head of `e`'s type, and unify the
/// marker's `App(m, a)` accordingly. When `e`'s type is too unresolved to
/// classify, fall back to `pure` — the reading that makes an un-annotated
/// `do { x <- act; someValue }` work.
fn resolve_result_markers(infer: &mut Infer, program: &mut ast::Expr) {
    let markers = std::mem::take(&mut infer.result_markers);
    // Spans of the markers that turned out to mean `pure e`; the rest are the
    // identity and get replaced by their argument.
    let mut pure_spans: HashSet<Span> = HashSet::new();

    for m in &markers {
        let monad = infer.apply(&Ty::Var(m.monad));
        let arg = infer.apply(&m.arg);
        let block_kind = monad_kind_of(&monad);
        let is_action = action_monad_of(&arg) == Some(block_kind)
            // An unresolved monad var defaults to `Relation` in
            // `monad_kind_of`, which would misread `do { …; someList }` in an
            // otherwise-unconstrained block. Only trust the comparison when
            // the monad actually resolved to something.
            && !matches!(monad, Ty::Var(_));

        // Restore the rigidity context the marker was written in: this unify
        // stands in for a step of the do-block's body, and the body's rigid
        // signature vars (with whatever `\/` unions were declared over them)
        // must still constrain it. See `ResultMarker::skolems`.
        let saved_skolems = infer.skolems.clone();
        let saved_unions = std::mem::replace(
            &mut infer.pending_effect_unions,
            m.effect_unions.clone(),
        );
        infer.skolems.extend(m.skolems.iter().copied());

        if is_action {
            let action_ty = Ty::App(
                Box::new(Ty::Var(m.monad)),
                Box::new(Ty::Var(m.elem)),
            );
            infer.unify(&action_ty, &arg, m.arg_span);
        } else {
            pure_spans.insert(m.span);
            infer.unify(&Ty::Var(m.elem), &arg, m.arg_span);
        }

        infer.skolems = saved_skolems;
        infer.pending_effect_unions = saved_unions;
    }

    rewrite_result_markers(program, &pure_spans);
}

/// Replace each `__result` node: `pure`-classified markers become `__yield`
/// (keeping the Var's span, which `monad_info` is already keyed by), and the
/// rest collapse to their argument.
fn rewrite_result_markers(expr: &mut ast::Expr, pure_spans: &HashSet<Span>) {
    if let ast::ExprKind::App { func, arg } = &mut expr.node
        && matches!(&func.node, ast::ExprKind::Var(n) if n == crate::desugar::RESULT_MARKER)
    {
        if pure_spans.contains(&func.span) {
            func.node = ast::ExprKind::Var("__yield".into());
            rewrite_result_markers(arg, pure_spans);
        } else {
            let mut inner = (**arg).clone();
            rewrite_result_markers(&mut inner, pure_spans);
            *expr = inner;
        }
        return;
    }
    walk_expr_children_mut(expr, &mut |e| rewrite_result_markers(e, pure_spans));
}

/// Apply `f` to each direct sub-expression. Mirrors the AST shape walked by
/// `base::shift_expr_spans`; keep the two in sync when the AST grows a node.
fn walk_expr_children_mut(expr: &mut ast::Expr, f: &mut impl FnMut(&mut ast::Expr)) {
    use ast::ExprKind::*;
    match &mut expr.node {
        Lit(_) | Var(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) | ImplicitRef(_) => {}
        TypeCtor { .. } | DataCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } | RouteCompositeDecl { .. } => {}
        ViewDecl { body, .. } | DerivedDecl { body, .. } => f(body),
        Record(fields) => {
            for fl in fields {
                f(&mut fl.value);
            }
        }
        RecordUpdate { base, fields } => {
            f(base);
            for fl in fields {
                f(&mut fl.value);
            }
        }
        FieldAccess { expr, .. } => f(expr),
        List(items) => {
            for it in items {
                f(it);
            }
        }
        Lambda { body, .. } => f(body),
        App { func, arg } => {
            f(func);
            f(arg);
        }
        With { record, body } => {
            f(record);
            f(body);
        }
        BinOp { lhs, rhs, .. } => {
            f(lhs);
            f(rhs);
        }
        UnaryOp { operand, .. } => f(operand),
        If { cond, then_branch, else_branch } => {
            f(cond);
            f(then_branch);
            f(else_branch);
        }
        Case { scrutinee, arms } => {
            f(scrutinee);
            for arm in arms {
                f(&mut arm.body);
            }
        }
        Do(stmts) => {
            for s in stmts {
                match &mut s.node {
                    ast::StmtKind::Bind { expr, .. } => f(expr),
                    ast::StmtKind::Where { cond } => f(cond),
                    ast::StmtKind::GroupBy { key } => f(key),
                    ast::StmtKind::Expr(e) => f(e),
                }
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            f(target);
            f(value);
        }
        Atomic(inner) | Refine(inner) => f(inner),
        TimeUnitLit { value, .. } => f(value),
        Annot { expr, .. } => f(expr),
        Serve { handlers, .. } => {
            for h in handlers {
                f(&mut h.body);
            }
        }
    }
}

// ── Declaration-marker walkers (expression model) ──────────────────

/// A relation marker found in a record literal: a persisted source (`*name`),
/// view, or derived (`&name`) relation.
enum RelMarker<'a> {
    Source {
        name: &'a str,
        ty: &'a ast::Type,
    },
    View {
        name: &'a str,
        ty: Option<&'a ast::TypeScheme>,
        body: Option<&'a ast::Expr>,
    },
    Derived {
        name: &'a str,
        ty: Option<&'a ast::TypeScheme>,
        body: Option<&'a ast::Expr>,
    },
}

/// Read-only recursion over every sub-expression.
fn walk_exprs_read<'a>(e: &'a ast::Expr, f: &mut impl FnMut(&'a ast::Expr)) {
    f(e);
    use ast::ExprKind::*;
    match &e.node {
        App { func, arg } => {
            walk_exprs_read(func, f);
            walk_exprs_read(arg, f);
        }
        With { record, body } => {
            walk_exprs_read(record, f);
            walk_exprs_read(body, f);
        }
        Lambda { body, .. } => walk_exprs_read(body, f),
        BinOp { lhs, rhs, .. } => {
            walk_exprs_read(lhs, f);
            walk_exprs_read(rhs, f);
        }
        UnaryOp { operand, .. } => walk_exprs_read(operand, f),
        If { cond, then_branch, else_branch } => {
            walk_exprs_read(cond, f);
            walk_exprs_read(then_branch, f);
            walk_exprs_read(else_branch, f);
        }
        Case { scrutinee, arms } => {
            walk_exprs_read(scrutinee, f);
            for arm in arms {
                walk_exprs_read(&arm.body, f);
            }
        }
        Do(stmts) => {
            for s in stmts {
                match &s.node {
                    ast::StmtKind::Bind { expr, .. } => walk_exprs_read(expr, f),
                    ast::StmtKind::Where { cond } => walk_exprs_read(cond, f),
                    ast::StmtKind::GroupBy { key } => walk_exprs_read(key, f),
                    ast::StmtKind::Expr(x) => walk_exprs_read(x, f),
                }
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            walk_exprs_read(target, f);
            walk_exprs_read(value, f);
        }
        Atomic(x) | Refine(x) => walk_exprs_read(x, f),
        TimeUnitLit { value, .. } => walk_exprs_read(value, f),
        Record(fields) => {
            for fl in fields {
                walk_exprs_read(&fl.value, f);
            }
        }
        RecordUpdate { base, fields } => {
            walk_exprs_read(base, f);
            for fl in fields {
                walk_exprs_read(&fl.value, f);
            }
        }
        List(items) => {
            for it in items {
                walk_exprs_read(it, f);
            }
        }
        FieldAccess { expr, .. } | Annot { expr, .. } => walk_exprs_read(expr, f),
        Serve { handlers, .. } => {
            for h in handlers {
                walk_exprs_read(&h.body, f);
            }
        }
        ViewDecl { body, .. } | DerivedDecl { body, .. } => walk_exprs_read(body, f),
        _ => {}
    }
}

/// Visit every `TypeCtor` (`type` alias) marker in the program.
fn for_each_type_ctor<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, &'a [ast::Name], &'a ast::Type, Span),
) {
    walk_exprs_read(program, &mut |e| {
        if let ast::ExprKind::TypeCtor { name, params, ty } = &e.node {
            f(name, params, ty, e.span);
        }
    });
}

/// Visit every `DataCtor` (`data`) marker in the program.
fn for_each_data_ctor<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, &'a [ast::Name], &'a [ast::ConstructorDef], Span),
) {
    walk_exprs_read(program, &mut |e| {
        if let ast::ExprKind::DataCtor { name, params, constructors } = &e.node {
            f(name, params, constructors, e.span);
        }
    });
}

/// Visit every relation marker (`*source` / view / `&derived`).
fn for_each_relation_marker<'a>(program: &'a ast::Expr, f: &mut impl FnMut(RelMarker<'a>)) {
    walk_exprs_read(program, &mut |e| match &e.node {
        ast::ExprKind::SourceDecl { name, ty, .. } => {
            f(RelMarker::Source { name, ty });
        }
        ast::ExprKind::ViewDecl { name, ty, body } => {
            f(RelMarker::View {
                name,
                ty: ty.as_ref(),
                body: Some(body),
            });
        }
        ast::ExprKind::DerivedDecl { name, ty, body } => {
            f(RelMarker::Derived {
                name,
                ty: ty.as_ref(),
                body: Some(body),
            });
        }
        _ => {}
    });
}

/// Visit every named function binding: a record field whose value is a lambda
/// (or has a signature). Yields `(name, signature, body)`.
fn for_each_named_fn<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, Option<&'a ast::TypeScheme>, Option<&'a ast::Expr>),
) {
    walk_exprs_read(program, &mut |e| {
        if let ast::ExprKind::Record(fields) = &e.node {
            for fl in fields {
                let is_lambda = matches!(fl.value.node, ast::ExprKind::Lambda { .. });
                if is_lambda || fl.sig.is_some() {
                    f(&fl.name, fl.sig.as_ref(), Some(&fl.value));
                }
            }
        }
    });
}

/// Visit every route marker: `route Name = …` (with entries) and route
/// composites (`route Name = A | B`, `entries` = `None`).
fn for_each_route_marker<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, Option<&'a [ast::RouteEntry]>),
) {
    walk_exprs_read(program, &mut |e| match &e.node {
        ast::ExprKind::RouteDecl { name, entries } => {
            f(name, Some(entries));
        }
        ast::ExprKind::RouteCompositeDecl { name, .. } => {
            f(name, None);
        }
        _ => {}
    });
}

/// Visit every route composite (`route Name = A | B`):
/// `(name, components, span)`.
fn for_each_route_composite<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, &'a [String], Span),
) {
    walk_exprs_read(program, &mut |e| {
        if let ast::ExprKind::RouteCompositeDecl { name, components } = &e.node {
            f(name, components, e.span);
        }
    });
}

/// Classify a resolved monad/applicative type into a `MonadKind` for
/// codegen dispatch. Defaults unresolved types to Relation.
fn monad_kind_of(resolved: &Ty) -> MonadKind {
    match resolved.peel_alias() {
        Ty::TyCon(name) if name == "[]" => MonadKind::Relation,
        Ty::TyCon(name) if name == "IO" => MonadKind::IO,
        Ty::TyCon(name) => MonadKind::Adt(name.clone()),
        Ty::Relation(_) => MonadKind::Relation,
        Ty::IO(_, _, _) => MonadKind::IO,
        // Partially applied type constructor, e.g. Result e (App(TyCon("Result"), e))
        Ty::App(f, _) => match f.as_ref() {
            // IO applied to an effect row (App(TyCon("IO"), EffectRow))
            // is still the IO monad — classifying it as Adt("IO") would
            // dispatch to a nonexistent `Monad_IO_bind`.
            Ty::TyCon(name) if name == "IO" => MonadKind::IO,
            Ty::TyCon(name) if name == "[]" => MonadKind::Relation,
            Ty::TyCon(name) => MonadKind::Adt(name.clone()),
            _ => MonadKind::Relation,
        },
        // Saturated ADT used as monad, e.g. Con("Result", [Text]) from Result Text a
        Ty::Con(name, _) => MonadKind::Adt(name.clone()),
        _ => MonadKind::Relation, // default unresolved to Relation
    }
}

/// Collect the names of type aliases referenced by an AST type. Used for
/// cyclic-alias detection: only names present in `alias_names` are recorded.
fn collect_alias_refs(
    ty: &ast::Type,
    alias_names: &HashSet<String>,
    out: &mut HashSet<String>,
) {
    match &ty.node {
        ast::TypeKind::Named(name) => {
            if alias_names.contains(name) {
                out.insert(name.clone());
            }
        }
        ast::TypeKind::Var(_) | ast::TypeKind::Hole => {}
        ast::TypeKind::App { func, arg } => {
            collect_alias_refs(func, alias_names, out);
            collect_alias_refs(arg, alias_names, out);
        }
        ast::TypeKind::Record { fields, .. } => {
            for f in fields {
                collect_alias_refs(&f.value, alias_names, out);
            }
        }
        ast::TypeKind::Relation(inner) => {
            collect_alias_refs(inner, alias_names, out);
        }
        ast::TypeKind::Function { param, result } => {
            collect_alias_refs(param, alias_names, out);
            collect_alias_refs(result, alias_names, out);
        }
        ast::TypeKind::Variant { constructors, .. } => {
            for c in constructors {
                for f in &c.fields {
                    collect_alias_refs(&f.value, alias_names, out);
                }
            }
        }
        ast::TypeKind::Effectful { ty, .. } => {
            collect_alias_refs(ty, alias_names, out);
        }
        ast::TypeKind::IO { ty, .. } => {
            collect_alias_refs(ty, alias_names, out);
        }
        ast::TypeKind::UnitAnnotated { base, .. } => {
            collect_alias_refs(base, alias_names, out);
        }
        ast::TypeKind::Unit(_) => {}
        ast::TypeKind::Refined { base, .. } => {
            collect_alias_refs(base, alias_names, out);
        }
        ast::TypeKind::Forall { ty, .. } => {
            collect_alias_refs(ty, alias_names, out);
        }
    }
}

/// Build a wire type descriptor from a resolved type for Maybe-aware JSON
/// decoding: `?<inner>` marks Maybe positions (wire `null`/absent →
/// Nothing, present value → Just), `{name:ty,...}` records, `[ty]`
/// relations, scalar tokens for primitives, and `*` (leave unchanged) for
/// anything the decoder shouldn't touch.
fn ty_to_wire_descriptor(ty: &Ty) -> String {
    match ty.peel_alias() {
        t if t.is_int_like() => "int".to_string(),
        t if t.is_float_like() => "float".to_string(),
        Ty::Text => "text".to_string(),
        Ty::Bool => "bool".to_string(),
        Ty::Con(name, args) if name == "Maybe" && args.len() == 1 => {
            format!("?{}", ty_to_wire_descriptor(&args[0]))
        }
        // Open Maybe variants from case-pattern unification name the
        // constructors rather than the ADT; the inner type lives in Just's
        // payload record under `value`.
        Ty::Variant(ctors, _)
            if !ctors.is_empty() && ctors.keys().all(|k| k == "Just" || k == "Nothing") =>
        {
            let inner = ctors.get("Just").and_then(|payload| match payload.peel_alias() {
                Ty::Record(fields, _) => fields.get("value").map(ty_to_wire_descriptor),
                _ => None,
            });
            format!("?{}", inner.unwrap_or_else(|| "*".to_string()))
        }
        Ty::Record(fields, _) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|(n, t)| format!("{}:{}", n, ty_to_wire_descriptor(t)))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Ty::Relation(inner) => format!("[{}]", ty_to_wire_descriptor(inner)),
        _ => "*".to_string(),
    }
}

/// Extract a simple type name from a resolved type for trait dispatch purposes.
fn ty_to_type_name(ty: &Ty) -> Option<String> {
    match ty {
        t if t.is_int_like() => Some("Int".to_string()),
        t if t.is_float_like() => Some("Float".to_string()),
        Ty::Text => Some("Text".to_string()),
        Ty::Bool => Some("Bool".to_string()),
        Ty::Bytes => Some("Bytes".to_string()),
        Ty::Uuid => Some("Uuid".to_string()),
        Ty::Con(name, _) => Some(name.clone()),
        Ty::Relation(_) => Some("Relation".to_string()),
        Ty::Record(_, _) => Some("Record".to_string()),
        _ => None,
    }
}

/// Extract the constructor name from a `fetch url (Ctor {..})` or
/// `fetch url opts (Ctor {..})` expression tree.  Returns `None` if
/// the expression is not a fetch call with a constructor argument.
fn fetch_ctor_name(expr: &ast::Expr) -> Option<&str> {
    let ast::ExprKind::App { func, arg } = &expr.node else {
        return None;
    };
    // The last argument should be a constructor application. The constructor
    // may be a bare `Ctor` or a path into a record-embedded route namespace
    // (`rec.Api.Ctor`); the endpoint constructor is registered under its leaf
    // name, so a field path reduces to its final segment.
    let ctor_name = match &arg.node {
        ast::ExprKind::App { func: ctor_func, .. } => match &ctor_func.node {
            ast::ExprKind::Constructor(name) => name.as_str(),
            ast::ExprKind::FieldAccess { field, .. } => field.as_str(),
            _ => return None,
        },
        ast::ExprKind::Constructor(name) => name.as_str(),
        ast::ExprKind::FieldAccess { field, .. } => field.as_str(),
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
fn uncurry_fetch(expr: &ast::Expr) -> (&ast::Expr, Vec<&ast::Expr>) {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            let (f, mut args) = uncurry_fetch(func);
            args.push(arg);
            (f, args)
        }
        _ => (expr, Vec::new()),
    }
}

// ── Tests ─────────────────────────────────────────────────────────


