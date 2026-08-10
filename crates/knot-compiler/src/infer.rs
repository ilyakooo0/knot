//! Hindley-Milner type inference for the Knot language.
//!
//! Infers and checks types for all declarations. Reports type errors as
//! diagnostics. The runtime uses uniform pointer representation, so this
//! pass is purely for error detection — it does not affect code generation.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{BTreeMap, HashMap, HashSet};
use indexmap::IndexMap;

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
    /// The rigid signature vars in force where the marker was written.
    /// `resolve_result_markers` runs long after the enclosing declaration
    /// dropped them, but the unify it performs *is* the do-block's
    /// sequencing step.
    skolems: Vec<TyVar>,
}

/// Maps `refine` expression spans to their resolved refined type name.
pub type RefineTargets = HashMap<Span, String>;

/// A stdlib function that codegen may special-case (SQL pushdown, query
/// forms, …). Identified by RESOLUTION, not by name string, so user code that
/// shadows the name is never confused with the builtin.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum StdlibFn {
    Sum,
    Avg,
    MinOn,
    MaxOn,
    Count,
    CountWhere,
    Filter,
    Map,
    SortBy,
    SortByDesc,
    Take,
    Drop,
    FindFirst,
    Any,
    All,
    Elem,
    Head,
}

impl StdlibFn {
    /// Map a stdlib name to its identity; `None` for names codegen does not
    /// special-case.
    pub fn from_name(name: &str) -> Option<StdlibFn> {
        Some(match name {
            "sum" => StdlibFn::Sum,
            "avg" => StdlibFn::Avg,
            "minOn" => StdlibFn::MinOn,
            "maxOn" => StdlibFn::MaxOn,
            "count" => StdlibFn::Count,
            "countWhere" => StdlibFn::CountWhere,
            "filter" => StdlibFn::Filter,
            "map" => StdlibFn::Map,
            "sortBy" => StdlibFn::SortBy,
            "sortByDesc" => StdlibFn::SortByDesc,
            "take" => StdlibFn::Take,
            "drop" => StdlibFn::Drop,
            "findFirst" => StdlibFn::FindFirst,
            "any" => StdlibFn::Any,
            "all" => StdlibFn::All,
            "elem" => StdlibFn::Elem,
            "head" => StdlibFn::Head,
            _ => return None,
        })
    }
}

/// Which function a call-site head actually resolved to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FnIdentity {
    /// The compiler's own stdlib value-fn — safe to pattern-match for
    /// pushdown regardless of what else in the program shares its name.
    Stdlib(StdlibFn),
    /// Anything the user bound (top-level fn, `let`, lambda param, `with`
    /// field). Never eligible for pushdown.
    User,
}

/// Maps a call-site head span to the identity it resolved to during
/// inference. Codegen consults this to decide — positively, by resolution
/// rather than by name + shadowing guards — whether a call is the stdlib
/// function it can push down. A span with no entry fell through a path
/// inference did not classify; codegen treats that as "not provably stdlib"
/// and keeps the conservative (non-pushdown or name+guard) behaviour.
pub type ResolvedCalls = HashMap<Span, FnIdentity>;

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

/// Maps each `base.todo` / bare-`todo` reference span to the display string of
/// the type it was inferred to produce. Baked into the runtime hole report.
pub type TodoTypes = HashMap<Span, String>;

/// Maps each `base.todo` reference span to the local bindings visible at that
/// site — `(name, type-display-string)` pairs, innermost-first with shadowed
/// duplicates removed. Baked into the runtime hole report.
pub type TodoBindings = HashMap<Span, Vec<(String, String)>>;

/// Maps each `base.trace` reference span to the display string of the traced
/// value's type. Baked into the runtime trace report.
pub type TraceTypes = HashMap<Span, String>;

/// Maps each `base.trace` reference span to the local bindings visible at that
/// site — `(name, type-display-string)` pairs, innermost-first with shadowed
/// duplicates removed. Baked into the runtime trace report.
pub type TraceBindings = HashMap<Span, Vec<(String, String)>>;

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

/// Callsite resolutions for FOLD dictionaries: application span → the field
/// name plus the UNIQUE synthetic span of each collected `field` fragment's
/// `ImplicitRef` projection (innermost-first). Each synthetic span's
/// `(root, path)` lives in `implicit_refs`; codegen merges them with
/// `base.unify` from `{}` and splices the merged record (bare) as the leading
/// argument.
pub type FoldDictArgs = HashMap<Span, (String, Vec<Span>)>;

/// `<>name` collecting-fold resolutions: the `<>` head's span → the UNIQUE
/// synthetic span of each collected candidate's `ImplicitRef` projection
/// (innermost-first). Each synthetic span's `(root, path)` resolution lives in
/// `implicit_refs`; codegen emits an `ImplicitRef` node per span and unrolls
/// the `<>name folder init` spine into a left-nested fold over them.
pub type CollectRefs = HashMap<Span, Vec<Span>>;

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

/// Base offset for the synthetic spans `<>` mints for its per-candidate
/// `ImplicitRef` projections (registered in `implicit_refs`). Far above any
/// real source offset so synthetic spans never collide with genuine ones.
pub const COLLECT_SYNTH_BASE: usize = 1 << 40;

/// Base offset for the synthetic spans a `(<>field)` fold-constraint mints for
/// its per-candidate `ImplicitRef` projections at each callsite. A distinct
/// range from `COLLECT_SYNTH_BASE` so the two synthetic-span spaces never
/// collide.
pub const FOLD_DICT_SYNTH_BASE: usize = 1 << 41;

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

/// For each `base.compile src` call site whose context pins the result type
/// `a`, the expected type of the JIT-compiled snippet, as a descriptor string.
/// Keyed by the call's span. A call whose `a` stays a free variable (context
/// never constrains it) is absent — the runtime then accepts whatever type the
/// snippet produces. Codegen passes the descriptor to the runtime, which
/// rejects the call (`Nothing`) unless the snippet's own type subsumes it.
pub type CompileExpectedTypes = HashMap<Span, String>;

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
/// Field/constructor map for records and variants. `IndexMap` preserves source
/// definition order (records) / declaration order (variants) so consumers that
/// care about order — implicit-field `^name` resolution, `show` output, JSON
/// encoding — see a stable, source-faithful sequence instead of BTreeMap's
/// alphabetical sort. By-key access (`get`/`contains_key`/`insert`) is unchanged.
type FieldMap = IndexMap<String, Ty>;
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
    /// Fields keep source definition order (IndexMap) so implicit-field `^name`
    /// resolution can try them bottom-to-top; record-type equality is by-key,
    /// so order is not semantic.
    Record(FieldMap, Option<TyVar>),
    /// Relation (set) type: [T].
    Relation(Box<Ty>),
    /// Named algebraic data type with optional type arguments.
    Con(String, Vec<Ty>),
    /// Variant with named constructors and optional row variable (open variant).
    /// Each constructor maps to its field types as a Record.
    Variant(FieldMap, Option<TyVar>),
    /// Unapplied type constructor (e.g. `[]`, `Maybe`).
    /// Used for higher-kinded type polymorphism.
    TyCon(String),
    /// Type-level application (e.g. `f a` where `f` is a HK variable).
    App(Box<Ty>, Box<Ty>),
    /// IO monad. Effects are untracked: this is a plain unary type wrapper
    /// around the action's result type. Do-notation and `IO a` behave as
    /// before; there is no effect row or effect polymorphism.
    IO(Box<Ty>),
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
    #[allow(dead_code)] // associated-type projection not yet constructed
    Assoc(String, Box<Ty>),
    /// Error sentinel — suppresses cascading errors.
    Error,
}

impl Ty {
    fn unit() -> Ty {
        Ty::Record(IndexMap::new(), None)
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
        Ty::IO(inner) => Ty::IO(Box::new(default_free_unit_vars(inner))),
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
    /// Deferred `*`/`/` unit-composition checks captured during generalization
    /// (e.g. `\x -> x * x`). Each one references `vars`
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
            unit_binops: vec![],
            ty,
        }
    }

    fn poly(vars: Vec<TyVar>, ty: Ty) -> Self {
        Scheme {
            vars,
            unit_vars: vec![],
            constraints: vec![],
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
    #[allow(dead_code)] // reserved for constraint ordering
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

/// A deferred `unify` shape computation: one or both argument types was an
/// unresolved type variable at the call node (e.g. a lambda parameter whose
/// record shape is only pinned when the lambda unifies with its call site, as
/// in `map (\row -> unify row {defaults}) *items`). Re-checked at end-of-
/// inference, when the argument may have resolved to a closed record; the
/// merged field map is then unified with `result`, the fresh variable returned
/// as the application's type so inference could proceed.
#[derive(Debug, Clone)]
struct DeferredUnify {
    left: Ty,
    right: Ty,
    result: TyVar,
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

    /// Compiler-internal registry of stdlib schemes (`map`, `println`,
    /// `count`, …). These NEVER enter `scopes` — user code cannot name them
    /// bare (they are naturally undefined). `base`'s record type and the
    /// `base.<special-form>`/`base.<query-form>` arms instantiate from here,
    /// mirroring how codegen keeps stdlib closures in its internal `user_fns`
    /// registry rather than any source-level scope.
    stdlib_schemes: HashMap<String, Scheme>,

    /// True only while `register_builtins` runs. During that window, `bind_top`
    /// routes stdlib value-fn names into `stdlib_schemes` instead of `scopes`.
    /// User named fns are bound later (in `infer_declarations`), after the flag
    /// is cleared, so a user fn named `map` still lands in `scopes` and shadows
    /// correctly via the scopes-only `lookup`.
    in_register_builtins: bool,

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

    /// Tracks `compile src` application sites for the expected-type check.
    /// Each entry records (app_span, return_type_var); post-inference the var
    /// resolves to `Maybe a`, and the inner `a` is the type the caller expects
    /// the compiled snippet to have. Codegen hands that expected `a` to the
    /// runtime, which rejects the call (`Nothing`) unless the snippet's own
    /// type scheme subsumes it.
    compile_calls: Vec<(Span, TyVar)>,

    /// The inferred type of the program's file body (`main`), captured at the
    /// Phase-4z root inference. Surfaced to `base.compile`'s JIT so the runtime
    /// can check the snippet's type against the caller's expected `a`.
    file_body_ty: Option<Ty>,

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

    /// For each implicit-dict function, the subset of its constraint fields
    /// declared with the FOLD marker `(<>field)` (vs single-match `(^field)`).
    /// At the callsite a fold field's dictionary is the `base.unify`-merged
    /// fold of EVERY enclosing scope's `field` value (innermost wins), not the
    /// single innermost match.
    fold_dict_fields: HashMap<String, Vec<String>>,

    /// FOLD-dict callsite resolutions: application span → the field name plus
    /// the synthetic span of each collected `field` fragment's `ImplicitRef`
    /// (innermost-first). Codegen merges them with `base.unify` from `{}` and
    /// splices the merged record as the leading argument (bare — the fold dict
    /// is the merged value itself, not `{field : merged}`).
    fold_dict_args: HashMap<Span, (String, Vec<Span>)>,
    /// `<>name` collecting-fold resolutions: the `<>` head's span → every
    /// collected `(root, path)`, innermost-first (see `CollectRefs`).
    collect_refs: CollectRefs,

    /// Deferred trait constraint checks, resolved after inference.
    deferred_constraints: Vec<DeferredConstraint>,

    /// Next sequence number to stamp onto a pushed `DeferredConstraint`.
    next_constraint_seq: u64,

    /// Spans of local variable bindings and their types (for LSP hover).
    binding_types: Vec<(Span, Ty)>,

    /// Spans of `base.todo` (or `with base` → bare `todo`) references and the
    /// type each was inferred at. Recorded as raw `(Span, Ty)` pairs and
    /// applied/displayed at extraction time, mirroring `binding_types`, so the
    /// runtime `todo` hole can report the exact type it was expected to
    /// produce.
    todo_types: Vec<(Span, Ty)>,

    /// Spans of `base.todo` references and a snapshot of the local bindings
    /// visible at each site — `(name, Scheme)` pairs, innermost scope first,
    /// shadowed names already deduplicated. Resolved/displayed at extraction
    /// time so the runtime hole report can list every in-scope local binding.
    todo_scopes: Vec<(Span, Vec<(String, Scheme)>)>,

    /// Spans of `base.trace` references and the type each was inferred at,
    /// mirroring `todo_types` — the traced value's type, reported at runtime.
    trace_types: Vec<(Span, Ty)>,

    /// Spans of `base.trace` references and a snapshot of the local bindings
    /// visible at each site, mirroring `todo_scopes` — so the runtime trace
    /// report can list every in-scope local binding, like `todo` does.
    trace_scopes: Vec<(Span, Vec<(String, Scheme)>)>,

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
    /// Top-level constant literals from the `with` declaration record
    /// (`five 5`), so a constant used where a refined type is required can be
    /// checked against the predicate at compile time (allowing the implicit
    /// base→refined use, or failing the build when the predicate is violated).
    const_literals: HashMap<String, crate::codegen::CompileLit>,

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
    /// Stack of per-`with` constructor-import scopes (one frame per enclosing
    /// `with {Type …}` that names types). Each frame maps a constructor NAME to
    /// the data type it belongs to, so a bare `Just {value v}` inside the body
    /// resolves to `Maybe.Just`. Pushed before the body is inferred, popped
    /// after — the unqualified ctors are confined to the `with` body.
    with_ctor_imports: Vec<HashMap<String, String>>,
    /// Resolved `^name` implicit-field projections: span → (root binding,
    /// field path). Populated when an `ImplicitRef` is resolved; handed to
    /// codegen via `ImplicitRefs` so it can emit the projection chain.
    implicit_refs: ImplicitRefs,
    /// Call-site head spans mapped to the function identity they resolved to
    /// (stdlib vs user). Handed to codegen via `ResolvedCalls` so pushdown
    /// dispatch is by resolution, not name string.
    resolved_calls: ResolvedCalls,
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

    /// Expected type pushed by `check_expr` when it falls through to
    /// infer-mode for a `with` expression. The infer `With` arm pops this and
    /// CHECKS its body against it (rather than inferring), so the contextual
    /// type flows through the `with` to a `(^name) arg` application inside —
    /// letting the expected RESULT type disambiguate same-source-type morphs.
    /// A stack: nested checked `with` bodies push/pop in order.
    with_body_expected: Vec<Ty>,

    /// Names of USER top-level declarations (the `with`-record fields that act
    /// as named functions/values). These live in `scopes[0]`, which no longer
    /// contains stdlib value fns (those are in `stdlib_schemes`), but DOES
    /// still hold ctor/effect bindings — so `bound_in_user_scope` consults this
    /// set to tell a user decl named e.g. `map` apart from a builtin binding.
    user_top_level_names: HashSet<String>,

    /// Unit-composition checks for `*`/`/` deferred because one operand was
    /// still an unresolved type variable when the binop was inferred. When the
    /// enclosing binding is generalized, `generalize` moves the relevant
    /// entries onto the resulting `Scheme` (`Scheme::unit_binops`) so each
    /// instantiation re-arms its own copy; the rest are resolved once at
    /// end-of-inference by `resolve_deferred_unit_binops`.
    deferred_unit_binops: Vec<DeferredUnitBinop>,
    /// Deferred `unify` shape computations, resolved at end-of-inference by
    /// `resolve_deferred_unifies`.
    deferred_unifies: Vec<DeferredUnify>,

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
            stdlib_schemes: HashMap::new(),
            in_register_builtins: false,
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
            compile_calls: Vec::new(),
            file_body_ty: None,
            from_json_calls: Vec::new(),
            show_calls: Vec::new(),
            known_impls: HashSet::new(),
            implicit_dict_fns: HashMap::new(),
            fold_dict_fields: HashMap::new(),
            implicit_dict_args: HashMap::new(),
            fold_dict_args: HashMap::new(),
            collect_refs: HashMap::new(),
            deferred_constraints: Vec::new(),
            next_constraint_seq: 0,
            binding_types: Vec::new(),
            todo_types: Vec::new(),
            todo_scopes: Vec::new(),
            trace_types: Vec::new(),
            trace_scopes: Vec::new(),
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
            const_literals: HashMap::new(),
            field_accesses: Vec::new(),
            with_fields: Vec::new(),
            with_body_expected: Vec::new(),
            with_scope_stack: vec![None],
            with_ctor_imports: Vec::new(),
            implicit_refs: HashMap::new(),
            resolved_calls: HashMap::new(),
            suppress_refine_intro: None,
            user_top_level_names: HashSet::new(),
            deferred_unit_binops: Vec::new(),
            deferred_unifies: Vec::new(),
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
    /// Attempt to type an application `f <constant>` where `f`'s parameter is
    /// a refined type. When the argument is a compile-time constant we can
    /// check it against the predicate now: allow the implicit base→refined use
    /// if it holds, or fail the build if it's violated. Returns `Some((arg_ty,
    /// result_ty))` when handled, `None` to fall through to the normal path.
    fn try_const_refined_app(
        &mut self,
        func_ty: &Ty,
        arg: &ast::Expr,
        span: Span,
    ) -> Option<(Ty, Ty)> {
        // Only applies to a direct function `param -> ret`.
        let func_applied = self.apply(func_ty);
        let (param, ret) = match &func_applied {
            Ty::Fun(p, r) => (self.apply(p), self.apply(r)),
            _ => return None,
        };
        // The parameter must be a (non-generic) refined type.
        let refined_name = match &param {
            Ty::Con(name, args) if args.is_empty() && self.refined_types.contains_key(name) => {
                name.clone()
            }
            _ => return None,
        };
        // The argument must be a compile-time constant (literal or named const).
        let lit = crate::codegen::extract_literal_with_consts(arg, &self.const_literals)?;
        let pred = self.refined_types.get(&refined_name)?.1.clone();
        match crate::codegen::eval_refine_predicate_pub(&pred, &lit) {
            Some(true) => {
                // Predicate holds — allow the implicit use. Type the argument
                // against the *base* type (bypassing the introduction guard)
                // and return the function's result type.
                let base = self.resolve_refined_base(&refined_name, span)?;
                let arg_ty = self.infer_expr(arg);
                self.unify(&base, &arg_ty, arg.span);
                Some((self.apply(&param), ret))
            }
            Some(false) => {
                self.error(
                    format!(
                        "constant {} does not satisfy the predicate for refined type `{}`",
                        lit.display(),
                        refined_name
                    ),
                    arg.span,
                );
                Some((Ty::Error, Ty::Error))
            }
            // Not statically evaluable — leave it to the normal path, which
            // will require `refine`.
            None => None,
        }
    }

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

    /// Collect the predicate of a refined type *and every link in its declared
    /// chain*, outermost first. `Age = Nat where \x -> x <= 150` yields
    /// `[x <= 150, x >= 0]` — the effective predicate is their conjunction.
    /// Returns `None` on a cyclic definition (already diagnosed elsewhere).
    fn refined_chain_predicates(&self, name: &str) -> Option<Vec<ast::Expr>> {
        let mut preds = Vec::new();
        let mut visited: Vec<String> = vec![name.to_string()];
        let (mut base, pred) = self.refined_types.get(name)?.clone();
        preds.push(pred);
        loop {
            match &base {
                Ty::Con(n, args) if args.is_empty() && self.refined_types.contains_key(n) => {
                    if visited.iter().any(|v| v == n) {
                        return None;
                    }
                    visited.push(n.clone());
                    let (b, p) = self.refined_types[n].clone();
                    preds.push(p);
                    base = b;
                }
                _ => return Some(preds),
            }
        }
    }

    /// Is `src` a subtype of `dst` — may a `src` value be used where `dst` is
    /// required with no `refine`? True when both are refined types over the
    /// same numeric base and Z3 proves the source's (conjoined) predicate
    /// implies the destination's. Anything unprovable returns false.
    fn refined_subtype(&mut self, src: &str, dst: &str, span: Span) -> bool {
        // Resolve each to its non-refined base and require the same numeric kind.
        let src_base = match self.resolve_refined_base(src, span) {
            Some(b) => self.apply(&b),
            None => return false,
        };
        let dst_base = match self.resolve_refined_base(dst, span) {
            Some(b) => self.apply(&b),
            None => return false,
        };
        let kind = match (&src_base, &dst_base) {
            (a, b) if a == b => match base_kind(a) {
                Some(k) => k,
                None => return false, // non-numeric base: not SMT-encodable
            },
            _ => return false, // different bases: no widening
        };
        let src_preds = match self.refined_chain_predicates(src) {
            Some(p) => p,
            None => return false,
        };
        let dst_preds = match self.refined_chain_predicates(dst) {
            Some(p) => p,
            None => return false,
        };
        crate::refine_smt::implies(&src_preds, &dst_preds, kind)
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
            | Ty::IO(inner)
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
                let mut applied: FieldMap = fields
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
                let mut applied: FieldMap = ctors
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
                if (name == "Int" || name == "Float") && applied_args.len() == 1
                    && let Ty::Unit(u) = &applied_args[0] {
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
                Ty::Con(name.clone(), applied_args)
            }
            Ty::TyCon(_) => ty.clone(),
            Ty::App(f, a) => {
                let f = self.apply_impl(f, excluded);
                let a = self.apply_impl(a, excluded);
                Self::normalize_app(f, a)
            }
            Ty::IO(inner) => {
                let inner = self.apply_impl(inner, excluded);
                Ty::IO(Box::new(inner))
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
            Ty::TyCon(ref name) if name == "IO" => Ty::IO(Box::new(a)),
            Ty::App(..) => Ty::App(Box::new(f), Box::new(a)),
            Ty::TyCon(name) => Ty::Con(name, vec![a]),
            Ty::Con(name, mut args) => {
                args.push(a);
                Ty::Con(name, args)
            }
            _ => Ty::App(Box::new(f), Box::new(a)),
        }
    }

    // ── Effect-row helpers ───────────────────────────────────────

    // Walk an effect-row tail through the substitution, merging any
    // effects that have been bound to the chain. Returns the fully
    // resolved (effects, tail) pair.

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
            Ty::IO(inner) => self.occurs_in(var, inner),
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

    // ── Unification ──────────────────────────────────────────────

    /// `snippet ⊑ expected` — is the snippet's inferred type usable where the
    /// host's expected type is wanted? Checked on real `Ty`s with the
    /// language's own unifier, not a parallel string comparison.
    ///
    /// Mechanism: unify snippet against expected in a snapshot/restore of the
    /// substitution, succeeding iff unification produces no error. This reuses
    /// `unify`'s handling of record width, function contravariance, ADTs (via
    /// `Ty::Con` + the alias table, recursion included), and units. Free vars on
    /// EITHER side are unifiable (matching the existing contract that an
    /// expected type variable is unconstrained — a generic call site accepts
    /// whatever the snippet produces there).
    fn ty_subsumes(&mut self, snippet: &Ty, expected: &Ty, span: Span) -> bool {
        let subst_snapshot = self.subst.clone();
        let unit_subst_snapshot = self.unit_subst.clone();
        let errors_snapshot = self.errors.len();
        self.unify(snippet, expected, span);
        let ok = self.errors.len() == errors_snapshot;
        // Probe only: discard any bindings/errors the unification produced.
        self.subst = subst_snapshot;
        self.unit_subst = unit_subst_snapshot;
        self.errors.truncate(errors_snapshot);
        ok
    }

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
            (Ty::IO(x), Ty::IO(y)) => self.widen_refined_vars(x, y, depth + 1),
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
            // ── Refined-type SMT subsumption ─────────────────────────────
            // A value of refined type `S` flows where a *different*, looser
            // refined type `T` is required — with no `refine` — when Z3 proves
            // `S`'s predicate implies `T`'s predicate over a shared numeric
            // base. Anything unprovable (not a subtype, an un-encodable
            // predicate, a timeout) falls through to the mismatch arm, so the
            // user keeps the explicit-`refine` requirement. Same-name Cons are
            // the arm below; base-type flows are the refined/Con arms later.
            (Ty::Con(n1, a1), Ty::Con(n2, a2))
                if a1.is_empty()
                    && a2.is_empty()
                    && n1 != n2
                    && self.refined_types.contains_key(n1)
                    && self.refined_types.contains_key(n2) =>
            {
                // Provided vs required from polarity. `t1_provided` means t1
                // is the value being supplied and t2 the requirement.
                let (src, dst) = if t1_provided { (n1, n2) } else { (n2, n1) };
                if !self.refined_subtype(src, dst, span) {
                    let d1 = self.display_ty(&t1);
                    let d2 = self.display_ty(&t2);
                    let (exp, fnd) = if t1_provided { (d2, d1) } else { (d1, d2) };
                    self.error(
                        format!("type mismatch: expected {}, found {}", exp, fnd),
                        span,
                    );
                }
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
            // App(f, a) vs IO(b) → f = TyCon("IO"), a = b
            (Ty::App(f, a), Ty::IO(b)) => {
                self.unify_dir(f, &Ty::TyCon("IO".into()), span, t1_provided);
                self.unify_dir(a, b, span, t1_provided);
            }
            (Ty::IO(b), Ty::App(f, a)) => {
                self.unify_dir(f, &Ty::TyCon("IO".into()), span, !t1_provided);
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
            // ── IO monad ─────────────────────────────────────
            (Ty::IO(a), Ty::IO(b)) => {
                let a = a.clone();
                let b = b.clone();
                self.unify_dir(&a, &b, span, t1_provided);
            }
            // In IO do blocks, allow Relation types to unify with IO or
            // Unit types. Route handlers mix relational operations and
            // their declared response type in if/case branches. The plain
            // relation `[T]` stands for the *whole* result of the IO side,
            // so it unifies with the IO's inner type — not the relation's
            // element type with the inner (which produced nonsense
            // "expected {x: Int 1}, found [{x: Int 1}]" mismatches).
            (Ty::Relation(_), Ty::IO(b)) if self.in_io_do => {
                let b = (**b).clone();
                self.unify_dir(&t1, &b, span, t1_provided);
            }
            (Ty::IO(b), Ty::Relation(_)) if self.in_io_do => {
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
        fields: &FieldMap,
        row: Option<TyVar>,
        span: Span,
    ) -> (FieldMap, Option<TyVar>) {
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
        f1: &FieldMap,
        r1: Option<TyVar>,
        f2: &FieldMap,
        r2: Option<TyVar>,
        span: Span,
        t1_provided: bool,
    ) {
        // Unify common fields (IndexMap lookup is O(1), no HashSet needed)
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

        let only1: FieldMap = all1
            .iter()
            .filter(|(k, _)| !all2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let only2: FieldMap = all2
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
        c1: &FieldMap,
        r1: Option<TyVar>,
        c2: &FieldMap,
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

        let mut only1: FieldMap = c1
            .iter()
            .filter(|(k, _)| !c2.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut only2: FieldMap = c2
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
        let mut ctors = IndexMap::new();
        for (ctor_name, fields) in &info.ctors {
            let field_tys: FieldMap = fields
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
                let mut new_fields: FieldMap = fields
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
                let mut new_ctors: FieldMap = ctors
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
            Ty::IO(inner) => Ty::IO(Box::new(self.subst_ty(inner, mapping))),
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
            Ty::IO(inner) => Ty::IO(Box::new(self.subst_unit_vars_in_ty(inner, mapping))),
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
            Ty::IO(inner) => self.collect_free_unit_vars(inner, out),
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
        // Drain deferred `*`/`/` unit-composition checks whose result var is
        // generalized here, capturing them on the scheme (freshened per
        // instantiation). This is what lets a
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
            unit_binops,
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
            Ty::IO(inner) => self.collect_free_vars(inner, out),
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

    /// Bind `name` in the current scope, rejecting any binding that would
    /// SHADOW a name already visible in an enclosing lexical scope. Knot
    /// forbids shadowing outright: a name means exactly one thing for the
    /// whole region it is in scope, so a call head / var reference can never
    /// silently change which definition it resolves to (this is what makes
    /// SQL pushdown's name→SQL mapping sound without any shadowing guard).
    ///
    /// The check is purely lexical — only `scopes` matters. The stdlib names
    /// are NOT special: they live in `base` (an ordinary record) and in the
    /// compiler-internal `stdlib_schemes` registry, never in `scopes`, so they
    /// are simply not "in scope" until some `with` (e.g. `with base`) brings
    /// them in — at which point a later conflicting binding errors like any
    /// other. `with base` itself binds `base`'s fields exactly as `with` on
    /// any other record would.
    ///
    /// `span` is the new binding site, used for the error. Internal
    /// compiler-generated names (the `\0with:` alias prefix and friends) are
    /// exempt — they are not user-visible and are free to collide.
    fn bind_at(&mut self, name: &str, scheme: Scheme, span: Span) {
        if !name.starts_with('\0') {
            let shadows_enclosing =
                self.scopes.iter().rev().skip(1).any(|s| s.contains_key(name));
            if shadows_enclosing {
                self.error(
                    format!(
                        "`{name}` is already defined in an enclosing scope, and shadowing is not allowed"
                    ),
                    span,
                );
            }
        }
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    fn bind_top(&mut self, name: &str, scheme: Scheme) {
        // While `register_builtins` runs, stdlib value functions (`map`,
        // `println`, `count`, the server/query forms, …) go into the
        // compiler-internal `stdlib_schemes` registry, NOT `scopes` — so user
        // code cannot name them bare (they are naturally undefined, no gate
        // needed). `base`'s record type and the `base.<form>` arms instantiate
        // from the registry. The flag is cleared before `infer_declarations`,
        // so a USER fn named `map` binds into `scopes` here and shadows
        // correctly via the scopes-only `lookup`.
        if self.in_register_builtins
            && (crate::base::is_gated_stdlib(name)
                || crate::base::is_gated_special_form(name))
        {
            self.stdlib_schemes.insert(name.to_string(), scheme);
            return;
        }
        if let Some(scope) = self.scopes.first_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    /// Instantiate a stdlib scheme from the internal registry. Used by
    /// `bind_base_record` and the `base.<form>` arms, which must resolve a
    /// stdlib name that is deliberately absent from `scopes`.
    fn lookup_stdlib(&self, name: &str) -> Option<&Scheme> {
        self.stdlib_schemes.get(name)
    }

    fn lookup(&self, name: &str) -> Option<&Scheme> {
        for scope in self.scopes.iter().rev() {
            if let Some(scheme) = scope.get(name) {
                return Some(scheme);
            }
        }
        None
    }

    /// True when `name` is bound by a USER scope (any scope above the bottom
    /// builtin scope `scopes[0]`): a local lambda param, a do-block binder, a
    /// `with` field, a user top-level decl. The hard gate exempts such names —
    /// a user `map`/`println` binding shadows the gated builtin.
    fn bound_in_user_scope(&self, name: &str) -> bool {
        // A user top-level decl lives in `scopes[0]` alongside builtins, so it
        // won't be found by the scope-index scan below — check the set first.
        if self.user_top_level_names.contains(name) {
            return true;
        }
        if self
            .scopes
            .iter()
            .skip(1)
            .any(|scope| scope.contains_key(name))
        {
            return true;
        }
        // A `with`-opened field (`with base (map …)`, `with {show …} (…)`) is
        // a user binding too. It normally lands in `scopes[1..]` (caught
        // above), but the Phase-4z program-body re-inference runs with a
        // collapsed scope stack while the `with` frames still record the
        // opened fields — consult them so `with base`'s `map`/`show` aren't
        // misread as the gated builtin.
        self.with_scope_stack
            .iter()
            .flatten()
            .any(|(_, fields)| fields.contains_key(name))
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
    fn consume_type_arg(&self, spine: &[&ast::Expr]) -> Option<(ast::Type, usize)> {
        use knot::ast::TypeKind;
        let head = spine.first()?;
        let mut head_expr = *head;
        while let ast::ExprKind::Annot { expr: inner, .. } = &head_expr.node {
            head_expr = inner;
        }
        // `_` as a type argument: an inferrable hole. Maps to `TypeKind::Hole`
        // (→ a fresh unification variable), consumed as a single spine element.
        if matches!(&head_expr.node, ast::ExprKind::TypeHole) {
            return Some((
                knot::ast::Spanned {
                    node: TypeKind::Hole,
                    span: head.span,
                },
                1,
            ));
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
                    if self.param_aliases.contains_key(name)
                        && let Some(t) = self.expand_param_alias(ty) {
                            return t;
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
                let field_tys: FieldMap = fields
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
            // `?` — callsite-derived type. Maps to a fresh inference variable,
            // like `_`, but SEMANTICALLY distinct: it is not solved to one
            // concrete type at the definition; each callsite grounds it against
            // the resolved `<>` fold. For now it unifies as a fresh var; the
            // per-callsite grounding is driven by the fold-constraint machinery.
            ast::TypeKind::Callsite => self.fresh(),
            ast::TypeKind::Variant {
                constructors,
                rest,
            } => {
                let ctor_tys: FieldMap = constructors
                    .iter()
                    .map(|c| {
                        let field_tys: FieldMap = c
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
            ast::TypeKind::IO { ty: inner_ty } => {
                Ty::IO(Box::new(self.ast_type_to_ty(inner_ty)))
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
                if (name == "Int" || name == "Float") && args.len() == 1
                    && let Ty::Unit(u) = args[0].peel_alias() {
                        let u = self.apply_unit(u);
                        if u.is_dimensionless() {
                            return name.clone();
                        }
                        return format!("{} {}", name, u.display());
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
            Ty::IO(inner) => {
                format!("IO {}", self.display_ty(inner))
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

    /// Resolve a bare constructor name against the active `with {Type …}`
    /// constructor-import scopes (innermost-first). Returns the data type that
    /// owns it, so the bare `Just` can be instantiated as `Maybe.Just`. `None`
    /// when no enclosing `with` imports a type providing `name`.
    fn resolve_with_imported_ctor(&self, name: &str) -> Option<String> {
        for frame in self.with_ctor_imports.iter().rev() {
            if let Some(ty) = frame.get(name) {
                return Some(ty.clone());
            }
        }
        None
    }

    /// Push a `with {Type …}` constructor-import frame for `types`, validating
    /// each is a known data type and that no two imported ctors collide.
    /// Returns true when a frame was pushed (caller pops after the body). Used
    /// both by the `With` inference arm and by the top-level Phase-4z path,
    /// which infers a literal-record `with`'s body without visiting the arm.
    fn push_with_ctor_imports(&mut self, types: &[String], span: Span) -> bool {
        if types.is_empty() {
            return false;
        }
        let mut frame: HashMap<String, String> = HashMap::new();
        for tname in types {
            let Some(info) = self.data_types.get(tname).cloned() else {
                self.error(
                    format!("`with` type import `{tname}` is not a known data type"),
                    span,
                );
                continue;
            };
            for (ctor, _) in &info.ctors {
                if let Some(prev_ty) = frame.get(ctor) {
                    self.error(
                        format!(
                            "`with` type import `{tname}`: constructor `{ctor}` is also a constructor of `{prev_ty}` — use it qualified"
                        ),
                        span,
                    );
                    continue;
                }
                frame.insert(ctor.clone(), tname.clone());
            }
        }
        self.with_ctor_imports.push(frame);
        true
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

        let field_tys: FieldMap = info
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

        let field_tys: FieldMap = fields
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
            let path = implicit_dict_head_path(head)?;
            let dicts = self.implicit_dict_fns.get(&path)?.clone();
            let n_dicts = dicts.len();
            // Resolve the field's type structurally from the record root's
            // scheme (walking the path), so the leading dictionary params the
            // desugarer prepended are visible. `infer_expr(head)` would return
            // a fresh unification var, not the function type.
            let head_ty = self.resolve_field_path_ty(head)?;
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
            let fold_fields: Vec<String> = self
                .fold_dict_fields
                .get(&path)
                .cloned()
                .unwrap_or_default();
            for (i, (field, _)) in dicts.iter().enumerate() {
                if fold_fields.contains(field) {
                    self.resolve_fold_dict(field, expr.span);
                    continue;
                }
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
        // in-scope records and record the splice for codegen. A FOLD (`<>`)
        // field merges every in-scope fragment; a single-match (`^`) field
        // resolves the single innermost match.
        let fold_fields: Vec<String> = self
            .fold_dict_fields
            .get(name)
            .cloned()
            .unwrap_or_default();
        for (i, (field, _)) in dicts.iter().enumerate() {
            if fold_fields.contains(field) {
                self.resolve_fold_dict(field, expr.span);
                continue;
            }
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

    /// Type an application spine whose head is an `ImplicitRef` (`^name`) by
    /// its ARGUMENT types, so the candidate search in `resolve_implicit_ref`
    /// can discriminate between same-named fields of different structures
    /// (`base.list.map` vs `base.text.map`).
    ///
    /// `(^map) f xs` is `App(App(^map, f), xs)`. Inferring each argument and
    /// assembling the expected curried type `f_ty -> xs_ty -> ret` gives the
    /// resolver real type information to unify each candidate field against;
    /// the field whose type matches the arguments wins. Returns `None` when
    /// the head is not an `ImplicitRef` or the spine has no arguments (a bare
    /// `^name` falls through to the generic `ImplicitRef` arm, which keeps
    /// its fresh-var — first-in-scope — behavior).
    /// Peel an application spine whose head is `^name` into (name, args in
    /// application order, head span). Returns `None` when the expression is
    /// not a `^name` applied to at least one argument.
    fn peel_implicit_ref_app<'a>(
        expr: &'a ast::Expr,
    ) -> Option<(&'a str, Vec<&'a ast::Expr>, Span)> {
        let mut args: Vec<&ast::Expr> = Vec::new();
        let mut head = expr;
        while let ast::ExprKind::App { func, arg } = &head.node {
            args.push(arg);
            head = func;
        }
        args.reverse();
        let ast::ExprKind::ImplicitRef(name) = &head.node else {
            return None;
        };
        if args.is_empty() {
            return None;
        }
        Some((name.as_str(), args, head.span))
    }

    /// Resolve a `^name` application against a curried function type
    /// `arg1 -> … -> argN -> ret`, where `ret` is the caller-supplied result
    /// type. In infer mode `ret` is a fresh variable (argument types drive
    /// disambiguation); in check mode it is the CONTEXTUAL expected type, so
    /// the surrounding context's required result type also constrains which
    /// `^name` field is picked. Records the winning projection for codegen.
    fn resolve_implicit_ref_app(
        &mut self,
        name: &str,
        args: &[&ast::Expr],
        head_span: Span,
        app_span: Span,
        ret: Ty,
    ) -> Ty {
        let mut expected = ret;
        for a in args.iter().rev() {
            let arg_ty = self.infer_expr(a);
            expected = Ty::Fun(Box::new(arg_ty), Box::new(expected));
        }
        let field_ty = self.resolve_implicit_ref(name, &expected, head_span);
        self.unify(&field_ty, &expected, app_span);
        let mut result = self.apply(&expected);
        for _ in args {
            let Ty::Fun(_, rest) = result else {
                return Ty::Error;
            };
            result = self.apply(&rest);
        }
        result
    }

    fn try_infer_implicit_ref_app(&mut self, expr: &ast::Expr) -> Option<Ty> {
        let (name, args, head_span) = Self::peel_implicit_ref_app(expr)?;
        // Infer mode: the result type is a FRESH variable, so only the
        // argument types constrain which `^name` field is picked.
        let ret = self.fresh();
        Some(self.resolve_implicit_ref_app(name, &args, head_span, expr.span, ret))
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
    /// Resolve a `(<>field)` fold-constraint dictionary at a callsite: collect
    /// EVERY in-scope `field` fragment (innermost-first) via
    /// `collect_all_implicit_fields`, mint a unique synthetic span per
    /// fragment and register its `(root, path)` in `implicit_refs`, and record
    /// the fragment span list in `fold_dict_args` keyed by the callsite span.
    /// Codegen merges the fragments with `base.unify` from `{}` (innermost
    /// wins) and splices the merged record (bare) as the leading argument.
    fn resolve_fold_dict(&mut self, field: &str, span: Span) {
        let candidates = self.collect_all_implicit_fields(field);
        let mut frag_spans: Vec<Span> = Vec::with_capacity(candidates.len());
        for (i, (root, path, _ty)) in candidates.into_iter().enumerate() {
            let synth = Span::new(FOLD_DICT_SYNTH_BASE + span.start + i, 0);
            self.implicit_refs.insert(synth, (root, path));
            frag_spans.push(synth);
        }
        self.fold_dict_args.insert(span, (field.to_string(), frag_spans));
    }

    /// then shallowest record-nesting depth (a binding's own fields beat
    /// fields of nested records), then fields in source definition order
    /// bottom-to-top (record types store fields in an `IndexMap` preserving
    /// declaration order, iterated in reverse). Each candidate is tested
    /// with a speculative unify against a throwaway clone of the real
    /// substitution; only the winning
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
        // an `IndexMap` in source definition order, iterated in REVERSE so
        // within a level the bottom-defined field is tried first.
        let mut candidates: Vec<(String, Vec<String>, Ty)> =
            with_candidate.into_iter().collect();
        'scopes: for scope in self.scopes.iter().rev() {
            // Level-synchronized BFS across ALL record bindings in this
            // scope. Every binding's frontier advances one depth level per
            // round; we only stop descending once a given LEVEL (across all
            // bindings) has produced at least one candidate. This is the
            // "shallowest depth wins" rule — but a *shared* depth, so two
            // sibling records that both carry the field at the same nesting
            // (e.g. `int.morph.into` and `text.morph.into`) are BOTH
            // collected, letting the type-directed disambiguation below pick
            // between them. (The previous per-binding loop broke out of the
            // depth search as soon as the FIRST binding found a match at a
            // deeper level, so a later sibling's same-depth field was never
            // reached.)
            let mut frontiers: Vec<(String, Vec<(Vec<String>, Ty)>)> = Vec::new();
            for (bind_name, scheme) in scope {
                let root_ty = self.apply(&scheme.ty);
                let frontier: Vec<(Vec<String>, Ty)> = match root_ty.peel_alias() {
                    Ty::Record(fields, _) => fields
                        .iter()
                        .rev() // bottom-to-top: last-defined field tried first
                        .map(|(f, t)| (vec![f.clone()], t.clone()))
                        .collect(),
                    _ => Vec::new(),
                };
                if !frontier.is_empty() {
                    frontiers.push((bind_name.clone(), frontier));
                }
            }
            'depth: while frontiers.iter().any(|(_, f)| !f.is_empty()) {
                let found_before = candidates.len();
                // Advance every binding's frontier by one level.
                for (bind_name, frontier) in frontiers.iter_mut() {
                    let mut next: Vec<(Vec<String>, Ty)> = Vec::new();
                    for (path, field_ty) in frontier.drain(..) {
                        if *path.last().expect("non-empty path") == name {
                            candidates.push((bind_name.clone(), path.clone(), field_ty.clone()));
                        }
                        // Descend into nested record fields (without
                        // committing anything: `apply` is read-only).
                        if let Ty::Record(sub, _) = self.apply(&field_ty).peel_alias().clone() {
                            for (f, t) in sub.iter().rev() {
                                let mut p = path.clone();
                                p.push(f.clone());
                                next.push((p, t.clone()));
                            }
                        }
                    }
                    *frontier = next;
                }
                // Shallowest depth wins: if this LEVEL produced any candidate,
                // stop descending (deeper nesting is never considered).
                if candidates.len() > found_before {
                    break 'depth;
                }
            }
            if !candidates.is_empty() {
                break 'scopes;
            }
        }

        // Speculatively unify EACH candidate against `expected`. The
        // speculative substitution CLONES the real one but points every
        // variable straight at its fully-resolved type, so bindings made
        // during a trial are all at fresh or resolved-root variables and
        // never reach a shared deeper chain — applying a winner's diff to
        // the real substitution is then a faithful replay. Collect every
        // candidate that unifies: exactly one means a clean resolution;
        // more than one means the projection is genuinely ambiguous (two
        // in-scope fields of the same name and compatible type), which is a
        // hard error rather than a silent first-wins pick.
        let mut searched: Vec<String> = Vec::new();
        // (root binding, field path, field type, post-unify speculative subst)
        type Winner = (String, Vec<String>, Ty, HashMap<TyVar, Ty>);
        let mut winners: Vec<Winner> = Vec::new();
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
                winners.push((root.clone(), path.clone(), field_ty.clone(), trial));
            }
            searched.push(format!("{}.{} : {}", root, path.join("."), self.display_ty(field_ty)));
        }

        match winners.len() {
            1 => {
                let (root, path, field_ty, trial) = winners.pop().expect("one winner");
                for (v, t) in trial {
                    self.subst.insert(v, t);
                }
                self.implicit_refs.insert(span, (root, path));
                return field_ty;
            }
            n if n > 1 => {
                let options = winners
                    .iter()
                    .map(|(root, path, field_ty, _)| {
                        format!("{}.{} : {}", root, path.join("."), self.display_ty(field_ty))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                self.error(
                    format!(
                        "ambiguous projection '^{name}': {n} in-scope record fields match the expected type ({options}); qualify one explicitly"
                    ),
                    span,
                );
                return Ty::Error;
            }
            _ => {}
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

    /// Collect EVERY in-scope record field named `name`, across ALL enclosing
    /// scopes (innermost-first) — the `<>` counterpart to
    /// `resolve_implicit_ref`'s single-match search.
    ///
    /// Pure NAME collection, NO type filter: `<>` gathers by field name alone
    /// and unrolls the provided folder over every match. Shape compatibility
    /// is enforced by the fold itself (a mis-shaped fragment fails the fold's
    /// own `unify`/arithmetic at its precise site) — there is no silent
    /// skipping. The only deliberate difference from `^`'s search is NO
    /// early-exit at the first matching scope: `<>` wants the whole nested
    /// stack so inner and outer context both contribute.
    ///
    /// Each result is `(root_binding, field_path, field_ty)` — the same
    /// triple `implicit_refs` records for `^`, so codegen can emit the
    /// projection chain `root.path…name` unchanged.
    fn collect_all_implicit_fields(&mut self, name: &str) -> Vec<(String, Vec<String>, Ty)> {
        // Gather (root, path, ty) candidates from every scope, innermost
        // first. Mirrors the `with`-frame + record-BFS logic in
        // `resolve_implicit_ref`, but keeps walking outward.
        let mut candidates: Vec<(String, Vec<String>, Ty)> = Vec::new();

        // Pass 1: `with`-frame records. A `with {svcA {log …}}` frame binds
        // `svcA` as a field of the with-record. The runtime dictionary for a
        // `svcA` reference is the field value, bound in codegen under the
        // per-site FIELD alias `\0with:<span>@svcA` (see codegen's `With` arm)
        // — EXCEPT the innermost `with` frame, whose fields are read via the
        // shared bare-name slot (inference's `Var` arm deliberately does not
        // redirect the innermost; see its comment). So: innermost frame →
        // bare field-name root; deeper frames → `\0with:<span>@field` alias
        // root. BFS into record-typed fields for a nested `…log` at any
        // depth, rooting the projection at that field root and projecting the
        // REMAINING path: `svcA.log` → root `…@svcA`, path `[log]`.
        // Root a with-frame field reference the way inference's `Var` arm does:
        // the INNERMOST `with` frame reads via the shared bare-name slot (its
        // fields are bound directly by the frame); DEEPER frames must use the
        // per-site field alias `\0with:<span>@field`, which codegen binds and
        // which stays lexically correct across nesting where the bare slot is
        // runtime-order-dependent. Codegen emits each candidate as an
        // `ImplicitRef` (the `^` path), which compiles `Var(root)` directly.
        let innermost_with_idx = self
            .with_scope_stack
            .iter()
            .rposition(Option::is_some);
        for (idx, (with_frame, _scope)) in self
            .with_scope_stack
            .iter()
            .zip(self.scopes.iter())
            .enumerate()
            .rev()
        {
            let Some((with_span, field_schemes)) = with_frame else {
                continue;
            };
            let field_root = |fname: &str| -> String {
                if Some(idx) == innermost_with_idx {
                    fname.to_string()
                } else {
                    format!("{WITH_FIELD_ALIAS_PREFIX}{}@{fname}", with_span.start)
                }
            };
            // Direct field binding `with {log …}`: `log` is a top-level field.
            if let Some(scheme) = field_schemes.get(name) {
                candidates.push((field_root(name), Vec::new(), scheme.ty.clone()));
            }
            // BFS into record-typed fields for a nested `…log` at any depth.
            let mut frontier: Vec<(Vec<String>, Ty)> = field_schemes
                .iter()
                .map(|(f, s)| (vec![f.clone()], self.apply(&s.ty).clone()))
                .collect();
            loop {
                let mut next: Vec<(Vec<String>, Ty)> = Vec::new();
                let mut descended = false;
                for (path, field_ty) in &frontier {
                    // Depth-1 paths are the with's DIRECT fields, already
                    // collected by the `field_schemes.get(name)` lookup above —
                    // re-collecting them here would double-count. Only nested
                    // (`outer.parts`, depth ≥ 2) matches belong to the BFS.
                    if path.len() > 1 && *path.last().expect("non-empty path") == name {
                        // Root at the FIRST path element's BARE field name,
                        // matching `^`'s record-BFS root (`bind_name`): the
                        // `with` binds each field into the flat `Env`, which
                        // prototypes into nested bodies, so a bare `Var(app)`
                        // resolves the outer record correctly across nesting.
                        let root = path[0].clone();
                        let rest: Vec<String> = path[1..].to_vec();
                        candidates.push((root, rest, field_ty.clone()));
                    }
                    if let Ty::Record(sub, _) = self.apply(field_ty).peel_alias().clone() {
                        for (f, t) in sub.iter() {
                            let mut p = path.clone();
                            p.push(f.clone());
                            next.push((p, t.clone()));
                            descended = true;
                        }
                    }
                }
                if !descended {
                    break;
                }
                frontier = next;
            }
        }

        // Pass 2: record-BFS over every scope's bindings, innermost-first,
        // descending into nested record fields (no shallowest-depth early
        // exit — `<>` collects at any depth in any scope). SKIP bindings that
        // are `with`-record fields: those were already collected in Pass 1
        // rooted at the reliable `\0withrec:` alias, and re-collecting them
        // via the shared bare-name slot would double-count AND risk the
        // unreliable bare resolution.
        let with_field_names: std::collections::HashSet<&str> = self
            .with_scope_stack
            .iter()
            .flatten()
            .flat_map(|(_, fm)| fm.keys().map(String::as_str))
            .collect();
        for scope in self.scopes.iter().rev() {
            // (path-to-current-record, record_fields) frontier per binding.
            let mut frontier: Vec<(String, Vec<String>, Ty)> = Vec::new();
            for (bind_name, scheme) in scope {
                if with_field_names.contains(bind_name.as_str()) {
                    continue;
                }
                let root_ty = self.apply(&scheme.ty).clone();
                // A scope binding whose NAME is the sought field is a direct
                // candidate at depth 0, whatever its type (`parts = 5`, `logCtx
                // = {…}`, `tag = True`): the `<>` fold is general, not
                // record-only. Mirrors `resolve_dict`'s
                // `scope.contains_key(field)` candidate and also covers a
                // `with` field surfaced as a decl in the Phase-4z body pass
                // (where the `with` frame is not on `with_scope_stack`).
                if bind_name == name {
                    candidates.push((bind_name.clone(), Vec::new(), root_ty.clone()));
                }
                if let Ty::Record(fields, _) = root_ty.peel_alias() {
                    for (f, t) in fields.iter().rev() {
                        frontier.push((bind_name.clone(), vec![f.clone()], t.clone()));
                    }
                }
            }
            // BFS descend until no frontier entry has a record-typed field.
            loop {
                let mut next: Vec<(String, Vec<String>, Ty)> = Vec::new();
                let mut descended = false;
                for (root, path, field_ty) in &frontier {
                    if *path.last().expect("non-empty path") == name {
                        candidates.push((root.clone(), path.clone(), field_ty.clone()));
                    }
                    if let Ty::Record(sub, _) = self.apply(field_ty).peel_alias().clone() {
                        for (f, t) in sub.iter().rev() {
                            let mut p = path.clone();
                            p.push(f.clone());
                            next.push((root.clone(), p, t.clone()));
                            descended = true;
                        }
                    }
                }
                if !descended {
                    break;
                }
                frontier = next;
            }
        }

        candidates
    }

    /// Detect `<>name folder init` (an app spine headed by `CollectFold` with
    /// exactly two args) and rewrite it IN PLACE to the unrolled fold
    /// `folder (folder … (folder init proj1) … projN)`, where each `projK` is
    /// the explicit projection `rootK.pathK…name` for the Kth collected
    /// candidate. Returns the rewritten expression's type, or `None` if this
    /// isn't a `<>`-fold spine.
    ///
    /// The synthetic chain is ordinary `App`/`FieldAccess`/`Var`, and each
    /// projection's field type is known at its own site, so heterogeneous
    /// fragment shapes typecheck (the whole reason `<>` folds rather than
    /// listing). The collected `(root, path)` list is recorded in
    /// `collect_refs` keyed by the `<>` head's span so codegen unrolls the
    /// same fold over the ORIGINAL spine.
    fn try_infer_collect_fold(&mut self, expr: &ast::Expr) -> Option<Ty> {
        // Peel `App(App(CollectFold(name), folder), init)`.
        let ast::ExprKind::App { func: f1, arg: init } = &expr.node else {
            return None;
        };
        let ast::ExprKind::App { func: f0, arg: folder } = &f1.node else {
            return None;
        };
        let ast::ExprKind::CollectFold(name) = &f0.node else {
            return None;
        };
        let name = name.clone();
        let head_span = f0.span;
        let span = expr.span;

        // Collect candidates purely by field name — no type filter. Shape
        // compatibility is enforced by the unrolled fold itself at each site.
        let candidates = self.collect_all_implicit_fields(&name);

        // Register each candidate in `implicit_refs` under a UNIQUE synthetic
        // span, and record those spans (innermost-first) in `collect_refs` for
        // codegen. Codegen emits each projection as an `ImplicitRef` node with
        // its span, so it compiles through the SAME `^` path that resolves
        // outer `with`-records lexically across nesting (a bare `Var(root)`
        // would read the runtime-order-dependent shared slot instead).
        let synth_spans: Vec<Span> = candidates
            .iter()
            .enumerate()
            .map(|(i, (root, path, _))| {
                // Reserved high range, unique per (head span, index).
                let synth = Span {
                    start: COLLECT_SYNTH_BASE + head_span.start * 64 + i,
                    end: COLLECT_SYNTH_BASE + head_span.start * 64 + i + 1,
                };
                self.implicit_refs
                    .insert(synth, (root.clone(), path.clone()));
                synth
            })
            .collect();
        self.collect_refs.insert(head_span, synth_spans.clone());

        // Build the projection expr for one candidate: an `ImplicitRef` node
        // with the candidate's unique synthetic span (its resolution is in
        // `implicit_refs`). Inference's `ImplicitRef` arm reads that entry.
        // Build the projection expr for one candidate's TYPE: `Var(root).f1…name`.
        // Inference only needs the field's type here; codegen separately emits
        // each candidate as an `ImplicitRef` node (using the synthetic spans in
        // `collect_refs`) so nested `with`-records resolve lexically.
        let proj = |i: usize, span: Span| -> ast::Expr {
            let (root, path, _ty) = &candidates[i];
            let mut e = ast::Expr {
                node: ast::ExprKind::Var(root.clone()),
                span,
            };
            for field in path {
                e = ast::Expr {
                    node: ast::ExprKind::FieldAccess {
                        expr: Box::new(e),
                        field: field.clone(),
                    },
                    span,
                };
            }
            e
        };

        // Unroll outermost-first so that, with the right-biased `unify` folder
        // `(\acc frag -> base.unify frag acc)`, the INNERMOST fragment is
        // applied last and wins per-field.
        //
        // When the folder is a lambda LITERAL `\p1 p2 -> body`, beta-reduce
        // `folder acc p` to `body[p1:=acc, p2:=p]` at each site. This keeps
        // every `base.unify` in the body applied to CONCRETE argument types —
        // reusing one lambda at N sites would instead infer its body once
        // with unresolved params and trip the deferred-`unify` gap ("unify
        // expects record arguments, got a non-record type").
        let apply_folder = |folder: &ast::Expr, a: ast::Expr, b: ast::Expr| -> ast::Expr {
            if let ast::ExprKind::Lambda { params, body, .. } = &folder.node
                && params.len() == 2
                && let ast::PatKind::Var(p1) = &params[0].node
                && let ast::PatKind::Var(p2) = &params[1].node
            {
                let mut substituted = (**body).clone();
                subst_var(&mut substituted, p1, &a);
                subst_var(&mut substituted, p2, &b);
                return substituted;
            }
            // Fallback: ordinary curried application `folder a b`.
            ast::Expr {
                node: ast::ExprKind::App {
                    func: Box::new(ast::Expr {
                        node: ast::ExprKind::App {
                            func: Box::new(folder.clone()),
                            arg: Box::new(a),
                        },
                        span,
                    }),
                    arg: Box::new(b),
                },
                span,
            }
        };

        // Fold innermost-first (candidates are already innermost-first). The
        // folder `\acc frag -> base.unify frag acc` puts `frag` on the LEFT and
        // `acc` on the RIGHT, and `unify` is right-biased — so wrapping the
        // NEXT-outer fragment around the accumulated inner merge keeps the
        // INNERMOST fragment on the right of the outermost `unify`, i.e. it
        // wins per-field. Result: `unify outer (unify … (unify inner init))`.
        let mut acc = (**init).clone();
        for (i, _) in candidates.iter().enumerate() {
            let p = proj(i, span);
            acc = apply_folder(folder, acc, p);
        }

        Some(self.infer_expr(&acc))
    }

    fn infer_expr(&mut self, expr: &ast::Expr) -> Ty {
        let ty = self.infer_expr_inner(expr);
        // Record the inferred type of every `base.todo` (or `with base` → bare
        // `todo`) hole so codegen can report the exact type it was expected to
        // produce at runtime. Detection is purely syntactic on the reference
        // shape; the recorded `Ty` is applied/displayed at extraction time.
        if expr_is_todo_ref(expr) {
            self.todo_types.push((expr.span, ty.clone()));
            self.todo_scopes
                .push((expr.span, self.visible_bindings()));
        }
        // Same capture for `base.trace`: the traced value's type and the local
        // bindings in scope, so the runtime trace report mirrors `todo`'s.
        if expr_is_trace_ref(expr) {
            self.trace_types.push((expr.span, ty.clone()));
            self.trace_scopes
                .push((expr.span, self.visible_bindings()));
        }
        ty
    }

    fn infer_expr_inner(&mut self, expr: &ast::Expr) -> Ty {
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
                // `base` is the ONLY stdlib value in scope. Bare stdlib
                // value-function names (`map`, `println`, …) are NOT bound in
                // `scopes` at all (they live in the internal `stdlib_schemes`
                // registry), so a bare reference falls through to the natural
                // "undefined variable" path below — no masking gate needed.
                // A user binding that shadows the name (a local `map`, …) IS
                // in `scopes` and resolves normally. Prelude-internal uses
                // resolve via the temporary stdlib scope `bind_base_record`
                // pushes. `retry` is handled by its own `atomic` check above.
                if let Some(ty) = self.lookup_instantiate_at(name, expr.span) {
                    // This bare Var resolved through `scopes` — i.e. to a user
                    // binding (top-level fn, `let`, lambda param, or `with`
                    // field), never to a stdlib value-fn (those live only in
                    // `stdlib_schemes`, absent from `scopes`). Record the
                    // identity so codegen knows this call head is NOT the
                    // builtin even though the name may collide with one.
                    if StdlibFn::from_name(name).is_some() {
                        self.resolved_calls.insert(expr.span, FnIdentity::User);
                    }
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
                // A constructor named by an enclosing `with {Type …}` import is
                // in scope UNQUALIFIED: resolve `Just` to `Maybe.Just` and
                // instantiate it via the confined qualified path, yielding the
                // same `record -> data` function type as a normal ctor.
                if let Some(data_name) = self.resolve_with_imported_ctor(name)
                    && let Some((data_ty, record_ty)) =
                        self.instantiate_qualified_ctor(&data_name, name)
                    {
                        return Ty::Fun(Box::new(record_ty), Box::new(data_ty));
                    }
                // A bare BUILT-IN constructor in user code must be qualified
                // by its data type, exactly like a user constructor
                // (`Maybe.Just`, `Result.Ok`, `Bool.True`, …). Prelude-internal
                // uses (shifted spans) and pattern positions (which go through
                // a different arm) are exempt.
                if self.is_builtin_ctor(name)
                    && expr.span.start < crate::base::PRELUDE_SPAN_OFFSET
                {
                    let ty = self
                        .constructors
                        .get(name)
                        .and_then(|i| i.first())
                        .map(|i| i.data_type.as_str())
                        .unwrap_or("Type");
                    self.error(
                        format!(
                            "constructor '{name}' must be qualified (e.g. `{ty}.{name}`)"
                        ),
                        expr.span,
                    );
                    Ty::Error
                } else if !self.is_builtin_ctor(name) && self.constructors.contains_key(name) {
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

            ast::ExprKind::CollectFold(name) => {
                // A bare `<>name` not applied to `folder init` — the fold is
                // only meaningful applied. (`<>name folder init` is handled
                // in the App arm via `try_infer_collect_fold`.)
                self.error(
                    format!(
                        "`<>{name}` must be applied to a folder and an initial value: `<>{name} folder init`"
                    ),
                    expr.span,
                );
                Ty::Error
            }

            ast::ExprKind::TypeHole => {
                // `_` in VALUE position (a TypeHole that reached general
                // inference rather than being consumed as a type argument):
                // behaves like `base.todo` — a polymorphic `∀a. a` placeholder.
                // The `infer_expr` wrapper records the expected type + scope
                // (via `expr_is_todo_ref`) for the runtime hole report.
                self.fresh()
            }

            ast::ExprKind::SourceRef { name, .. } => {
                if let Some(ty) = self.source_types.get(name).cloned() {
                    Ty::IO(Box::new(ty))
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
                    Ty::IO(Box::new(ty))
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
                let field_tys: FieldMap = fields
                    .iter()
                    .map(|f| {
                        // A signature-only field (`name : Type`, no `=`) is a
                        // required CLI constant: the parser gave it an
                        // empty-record placeholder value that must NOT be
                        // checked against the sig (it would fail). Take the
                        // sig type as the field type and skip the value.
                        let is_required_const = f.sig.is_some()
                            && matches!(&f.value.node, ast::ExprKind::Record(fs) if fs.is_empty());
                        if is_required_const {
                            let sig = f.sig.as_ref().unwrap();
                            let saved_flag = self.in_type_annotation;
                            let saved_unit_vars = std::mem::take(&mut self.annotation_unit_vars);
                            self.in_type_annotation = true;
                            let sig_ty = self.ast_type_to_ty(&sig.ty);
                            self.in_type_annotation = saved_flag;
                            self.annotation_unit_vars = saved_unit_vars;
                            return (f.name.clone(), sig_ty);
                        }
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
                // `base.<server-form>` — `fetch`/`fetchWith`/`listen`/`listenOn`
                // are compile-time special forms (route-table / HTTP macros),
                // not `base` record fields, so the generic record-read below
                // would fail. Type the access by instantiating the bare
                // builtin's scheme; codegen dispatches the same name through
                // `server_form_name`. (`retry` is deliberately excluded: it is
                // the STM primitive, only valid inside `atomic`, never a value.)
                //
                // `base.<query-form>` — `count`/`union`/`sum`/`bind` ARE real
                // record fields (codegen registers curried values for them),
                // but their schemes carry `unit_vars` (`count : [a] -> Int u`)
                // or higher-rank shapes that the infer-from-prelude record type
                // in `bind_base_record` mangles during generalize. Resolve them
                // by instantiating the field's OWN scheme from scope, exactly
                // like the server forms — the runtime value is the same, only
                // the typing path differs.
                // `base.retry` — the STM primitive reached through `base`. It
                // has no record field; instantiate its polymorphic scheme, but
                // keep the same "only inside `atomic`" constraint as the bare
                // form (the bare `Var` arm checks this at its own site).
                if let ast::ExprKind::Var(n) = &e.node
                    && n == "base"
                    && field == "retry"
                {
                    if !self.in_atomic {
                        self.error(
                            "'retry' can only be used inside an 'atomic' block".to_string(),
                            expr.span,
                        );
                    }
                    if let Some(scheme) = self.lookup_stdlib(field).cloned() {
                        let ty = self.instantiate_at(&scheme, expr.span);
                        self.field_accesses.push((expr.span, ty.clone()));
                        return ty;
                    }
                    return Ty::Error;
                }
                // `base.todo` — the unimplemented hole. Dispatch-only (no
                // `base` record field holds it, since `todo : ∀a. a` can never
                // produce a value eagerly), so the generic record-read below
                // would report "unexpected field `todo`". Instantiate its
                // polymorphic scheme like `base.retry`; the expected type it
                // unifies with is captured for the runtime report via
                // `todo_types` (recorded in `record_expr_types`).
                if let ast::ExprKind::Var(n) = &e.node
                    && n == "base"
                    && field == "todo"
                {
                    if let Some(scheme) = self.lookup_stdlib(field).cloned() {
                        let ty = self.instantiate_at(&scheme, expr.span);
                        self.field_accesses.push((expr.span, ty.clone()));
                        return ty;
                    }
                    return Ty::Error;
                }
                // `base.trace` — the tracer. Also dispatch-only in the `base`
                // namespace: it compiles to a closure-creating call, not a
                // record read. Type it as `∀a. a -> a`; the traced value's type
                // and the in-scope bindings are captured for the runtime report
                // via `trace_types` (recorded in `record_expr_types`).
                if let ast::ExprKind::Var(n) = &e.node
                    && n == "base"
                    && field == "trace"
                {
                    if let Some(scheme) = self.lookup_stdlib(field).cloned() {
                        let ty = self.instantiate_at(&scheme, expr.span);
                        self.field_accesses.push((expr.span, ty.clone()));
                        return ty;
                    }
                    return Ty::Error;
                }
                if let ast::ExprKind::Var(n) = &e.node
                    && n == "base"
                    && matches!(
                        field.as_str(),
                        "fetch" | "fetchWith" | "listen" | "listenOn"
                            | "count" | "union" | "sum" | "bind"
                    )
                    && let Some(scheme) = self.lookup_stdlib(field).cloned()
                {
                    if let Some(sf) = StdlibFn::from_name(field) {
                        self.resolved_calls.insert(expr.span, FnIdentity::Stdlib(sf));
                    }
                    let ty = self.instantiate_at(&scheme, expr.span);
                    self.field_accesses.push((expr.span, ty.clone()));
                    return ty;
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
                // `base.<field>` where `field` is a stdlib value-fn codegen
                // special-cases: `base` is always the compiler's own record
                // (user code cannot rebind it), so the field unambiguously
                // names the builtin. Record the identity by resolution so
                // codegen's pushdown dispatch need not re-derive it by name.
                if let ast::ExprKind::Var(n) = &e.node
                    && n == "base"
                    && let Some(sf) = StdlibFn::from_name(field)
                {
                    self.resolved_calls.insert(expr.span, FnIdentity::Stdlib(sf));
                }
                let field_ty = self.fresh();
                let rv = self.fresh_var();
                let constraint = Ty::Record(
                    IndexMap::from([(field.clone(), field_ty.clone())]),
                    Some(rv),
                );
                self.unify(&base_ty, &constraint, e.span);
                self.field_accesses.push((expr.span, field_ty.clone()));
                field_ty
            }

            ast::ExprKind::With { record, body, types } => {
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
                        // Required CLI constant (sig-only field, empty-record
                        // placeholder value): take the sig type as the field
                        // type and skip the placeholder value entirely.
                        if let Some(sig) = &f.sig
                            && matches!(&f.value.node, ast::ExprKind::Record(fs) if fs.is_empty())
                        {
                            let saved_flag = self.in_type_annotation;
                            let saved_unit_vars =
                                std::mem::take(&mut self.annotation_unit_vars);
                            self.in_type_annotation = true;
                            let sig_ty = self.ast_type_to_ty(&sig.ty);
                            self.in_type_annotation = saved_flag;
                            self.annotation_unit_vars = saved_unit_vars;
                            field_tys.push((f.name.clone(), sig_ty));
                            continue;
                        }
                        // A `with` field named `base` collides with the
                        // compiler-owned stdlib record, which is bound in
                        // `scopes[0]` — a genuine lexical conflict (knot has
                        // no shadowing). Reject here and skip the field
                        // entirely: the error is the clean shadowing message,
                        // and omitting the field keeps the `with` record from
                        // unifying `base`'s value against the global record
                        // type (which would add a redundant, unreadable
                        // type-mismatch error on top).
                        if f.name == "base" {
                            self.error(
                                "`base` is already defined in an enclosing scope, and shadowing is not allowed".to_string(),
                                f.value.span,
                            );
                            continue;
                        }
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
                    // `fields` comes from the resolved record TYPE (no per-field
                    // span), so point the shadow error at the whole `with` record.
                    self.bind_at(name, Scheme::mono(ty.clone()), record.span);
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
                // Type imports (`with {Maybe …} body`): bring each named data
                // type's constructors into scope UNQUALIFIED for the body,
                // confined to the body (see push_with_ctor_imports).
                let pushed_ctor_imports = self.push_with_ctor_imports(types, record.span);
                // If `check_expr` fell through to us on this `with` with a
                // pending contextual expected type, CHECK the body against it
                // (rather than inferring) so the expected type flows to a
                // `(^name) arg` application inside — letting the required
                // RESULT type disambiguate same-source-type morphs.
                let body_ty = if let Some(expected) = self.with_body_expected.pop() {
                    self.check_expr(body, &expected);
                    expected
                } else {
                    self.infer_expr(body)
                };
                if pushed_ctor_imports {
                    self.with_ctor_imports.pop();
                }
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

                // Special case: `unify a b` — record merge, right-biased. The
                // result type is shape-dependent (the union of the two
                // argument field maps), which no single forall-quantified
                // scheme can express without a row-union type operator, so it
                // is computed here from the two argument types. Both arguments
                // must be closed, statically-known records (a free row tail
                // makes the union underdetermined → type error).
                if let Some(ty) = self.try_infer_unify(expr) {
                    return ty;
                }

                // Collecting fold: `<>name folder init`. Unrolled to a chain
                // of explicit projections so heterogeneous fragment shapes
                // typecheck; see `try_infer_collect_fold`.
                if let Some(ty) = self.try_infer_collect_fold(expr) {
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

                // Type-directed `^name` projection. A bare `^name` resolves
                // against a FRESH expected var (see the `ImplicitRef` arm),
                // so the candidate search's backtracking has nothing to
                // discriminate on and the shallowest/first field always wins.
                // When `^name` heads an application spine (`(^map) f xs`),
                // the argument types ARE the discriminating information:
                // infer them first, build the expected curried function type
                // `arg1 -> … -> argN -> ret`, and resolve `^name` against it —
                // the candidate whose field type unifies (e.g. `list.map` for
                // a list argument, `text.map` for a text argument) wins, and
                // genuinely ambiguous overlaps are reported by the resolver.
                if let Some(result) = self.try_infer_implicit_ref_app(expr) {
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
                    // A constructor named by an enclosing `with {Type …}`
                    // import applies UNQUALIFIED: `Just {value 5}` resolves to
                    // `Maybe.Just`. Check the payload against the qualified
                    // record type and return the data type.
                    if let Some(data_name) = self.resolve_with_imported_ctor(name)
                        && let Some((data_ty, record_ty)) =
                            self.instantiate_qualified_ctor(&data_name, name)
                        {
                            self.check_expr(arg, &record_ty);
                            return data_ty;
                        }
                    // An applied BUILT-IN constructor in user code must be
                    // qualified by its data type (`Maybe.Just {…}`).
                    if self.is_builtin_ctor(name)
                        && func.span.start < crate::base::PRELUDE_SPAN_OFFSET
                    {
                        let ty = self
                            .constructors
                            .get(name)
                            .and_then(|i| i.first())
                            .map(|i| i.data_type.as_str())
                            .unwrap_or("Type");
                        self.error(
                            format!(
                                "constructor '{name}' must be qualified (e.g. `{ty}.{name} {{…}}`)"
                            ),
                            func.span,
                        );
                        return Ty::Error;
                    }
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
                            self.bind_at(name, scheme, params[0].span);
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
                    // Respect the `base`-only gate: a bare stdlib head at a
                    // user span is undefined (the Var arm reports it), so don't
                    // pre-compute the lambda-last shape for it — `infer_expr`
                    // on the head would yield `Ty::Error` and trip the
                    // `unreachable!` arity assumption below.
                    let gated = head.span.start < crate::base::PRELUDE_SPAN_OFFSET
                        && crate::base::is_gated_stdlib(head_name)
                        && !self.bound_in_user_scope(head_name);
                    if gated {
                        None
                    } else {
                        self.lookup(head_name)
                            .is_some_and(|s| takes_two_args(&s.ty))
                            .then(|| ((**head).clone(), (**lam).clone()))
                    }
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

                    // Constant into a refined parameter: `f 5` / `f five`
                    // where `f : Nat -> …`. If the argument is a compile-time
                    // constant, check it against the predicate now — allow the
                    // implicit use when it holds (no `refine`, no runtime
                    // check), fail the build when it's violated. Anything not
                    // statically determinable falls through to the normal path
                    // (which still requires `refine`).
                    if let Some(pair) = self.try_const_refined_app(&func_ty, arg, expr.span) {
                        pair
                    } else {
                        let arg_ty = self.infer_expr(arg);
                        let result_ty = self.fresh();
                        let expected = Ty::Fun(
                            Box::new(arg_ty.clone()),
                            Box::new(result_ty.clone()),
                        );
                        self.unify(&func_ty, &expected, arg.span);
                        (arg_ty, result_ty)
                    }
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

                // Track `compile src` applications: the result type is
                // `Maybe a`; the inner `a` is the type the caller expects the
                // JIT-compiled snippet to have. Codegen hands it to the
                // runtime, which rejects the call (`Nothing`) unless the
                // snippet's own type subsumes it. `compile`'s result unifies
                // directly with `Maybe a` (not a bare var), so peel the `Maybe`
                // and record the inner `a`'s var. Matches both bare `compile`
                // and the namespaced `base.compile`.
                let is_compile_app = match &func.node {
                    ast::ExprKind::Var(n) => n == "compile",
                    ast::ExprKind::FieldAccess { expr: ns, field } => {
                        field == "compile"
                            && matches!(&ns.node, ast::ExprKind::Var(n) if n == "base")
                    }
                    _ => false,
                };
                if is_compile_app
                        && let Ty::Con(mn, margs) = self.apply(&result_ty).peel_alias()
                        && mn == "Result"
                        && margs.len() == 2
                        && let Ty::Var(inner_v) = self.apply(&margs[1]).peel_alias() {
                            self.compile_calls.push((expr.span, *inner_v));
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

            ast::ExprKind::Case { scrutinee, arms } => {
                let scrut_ty = self.infer_expr(scrutinee);
                let result_ty = self.fresh();

                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify(&body_ty, &result_ty, arm.body.span);
                    self.pop_scope();
                }

                self.check_exhaustiveness(&scrut_ty, arms, expr.span);

                result_ty
            }

            ast::ExprKind::Do(stmts) => self.infer_do(stmts, expr.span),

            ast::ExprKind::Set { target, value } => {
                let target_ty = self.infer_expr(target);
                let target_applied = self.apply(&target_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(inner) => (**inner).clone(),
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
                if let ast::ExprKind::SourceRef { name, .. } = &target.node {
                    // `set` is a read-modify-write only when the value actually
                    // reads the source. Relations require that reference (it's
                    // enforced below), so a valid relation `set` genuinely
                    // reads. But a scalar `*counter = 5` that references nothing
                    // reads nothing.
                    let references = value_references_source(
                        value,
                        name,
                        &self.source_var_binds,
                        &self.let_bindings,
                    );
                    // Require `full *rel = ...` when the value is a full
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
                                 use `full *{name} = ...` for a full replacement"
                            ),
                            expr.span,
                        );
                    }
                }
                Ty::IO(Box::new(Ty::unit()))
            }

            ast::ExprKind::FullSet { target, value } => {
                let target_ty = self.infer_expr(target);
                let target_applied = self.apply(&target_ty);
                let unwrap_io = |ty: &Ty| match ty {
                    Ty::IO(inner) => (**inner).clone(),
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
                if let ast::ExprKind::SourceRef { name, .. } = &target.node {
                    // Reject `full *rel = ...` when the value references
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
                                "`full *{name} = ...` is unnecessary when \
                                 the value references `*{name}` \
                                 (directly or via a `<- *{name}` bind); \
                                 use `*{name} = ...` instead"
                            ),
                            expr.span,
                        );
                    }
                }
                Ty::IO(Box::new(Ty::unit()))
            }

            ast::ExprKind::Atomic(inner) => {
                let prev = self.in_atomic;
                self.in_atomic = true;
                let inner_ty = self.infer_expr(inner);
                self.in_atomic = prev;
                // atomic : IO {} a -> IO {} a
                let inner_applied = self.apply(&inner_ty);
                match &inner_applied {
                    Ty::IO(_) => inner_applied,
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
                
                (0..params.len()).fold(Ty::Con("Type".into(), vec![]), |acc, _| {
                        Ty::Fun(
                            Box::new(Ty::Con("Type".into(), vec![])),
                            Box::new(acc),
                        )
                    })
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
                let mut ns_fields = IndexMap::new();
                for ctor in constructors {
                    let field_tys: FieldMap = ctor
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
                Ty::IO(Box::new(resolved))
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
                let mut ns_fields = IndexMap::new();
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
                Ty::IO(Box::new(resolved))
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
                Ty::IO(Box::new(resolved))
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
            ast::ExprKind::App { .. }
                if matches!(
                    Self::peel_implicit_ref_app(expr),
                    Some((_, args, _)) if !args.is_empty()
                ) =>
            {
                // `(^name) arg…` in a CHECKING context: thread the contextual
                // expected type in as the application's RESULT type, so the
                // surrounding context's required type participates in
                // disambiguation (e.g. `x : Float 1 = (^into) "hi"` picks the
                // `Text -> Float 1` morph, not any other `Text -> _` one).
                // Without this the result var is fresh and only the argument
                // types constrain the pick, leaving same-source-type morphs
                // ambiguous.
                let (name, args, head_span) =
                    Self::peel_implicit_ref_app(expr).expect("matched above");
                self.resolve_implicit_ref_app(
                    name,
                    &args,
                    head_span,
                    expr.span,
                    expected.clone(),
                );
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
                        && !self.subst.contains_key(&v)
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
                if matches!(resolved_expected, Ty::IO(_)) {
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
                    let mut field_tys = IndexMap::new();
                    for f in fields {
                        // Required CLI constant (sig-only field, empty-record
                        // placeholder value): take the sig type, skip the value.
                        let is_required_const = f.sig.is_some()
                            && matches!(&f.value.node, ast::ExprKind::Record(fs) if fs.is_empty());
                        if is_required_const {
                            let sig = f.sig.as_ref().unwrap();
                            let saved_flag = self.in_type_annotation;
                            let saved_unit_vars =
                                std::mem::take(&mut self.annotation_unit_vars);
                            self.in_type_annotation = true;
                            let sig_ty = self.ast_type_to_ty(&sig.ty);
                            self.in_type_annotation = saved_flag;
                            self.annotation_unit_vars = saved_unit_vars;
                            field_tys.insert(f.name.clone(), sig_ty);
                            continue;
                        }
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
            ast::ExprKind::With { .. } => {
                // Propagate the contextual expected type INTO the `with` body:
                // push it so the infer `With` arm CHECKS its body against it
                // (see `with_body_expected`), letting a `(^name) arg` inside
                // resolve by the required RESULT type. `infer_expr` returns
                // that same expected type, so the final unify is a no-op.
                self.with_body_expected.push(expected.clone());
                let inferred = self.infer_expr(expr);
                // Defensive: if the infer path didn't consume it (e.g. an
                // error bailed early), don't leak the pending expected.
                self.with_body_expected.retain(|e| e != expected);
                self.unify_dir(expected, &inferred, expr.span, false);
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

    /// Type-check a `serve Api where ...` expression, returning `Server Api`.
    /// Internally the type is `Ty::Con("Server", [api])`.
    ///
    /// Each handler is checked against the type derived from its route entry:
    ///   - input: a record of (path params, query params, body fields,
    ///     request headers)
    ///   - output: the entry's declared response type, or
    ///     `{body: ResponseTy, headers: {h: T, ...}}` when response headers
    ///     are declared. The handler may also return `IO <output>`.
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
                return Ty::Con("Server".into(), vec![Ty::Var(a)]);
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
            let expected = self.serve_handler_type(entry);
            self.check_expr(&h.body, &expected);
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

        Ty::Con("Server".into(), vec![Ty::Con(api.to_string(), vec![])])
    }

    /// Build the request-input record type for a route entry. Same record
    /// the handler receives (path params + query params + body fields +
    /// request headers) and the rate-limit `key` function's first argument.
    fn route_input_record_ty(&mut self, entry: &ast::RouteEntry) -> Ty {
        let mut input_fields: FieldMap = IndexMap::new();
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
    fn serve_handler_type(&mut self, entry: &ast::RouteEntry) -> Ty {
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
                        IndexMap::from([
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
        let output = Ty::IO(Box::new(wrapped));
        Ty::Fun(Box::new(input), Box::new(output))
    }

    /// Try to infer a `fetch` call. Returns `Some(ty)` if the expression
    /// is `fetch url (Ctor {..})` or `fetch url opts (Ctor {..})`.
    /// This skips the constructor's `respond` field and resolves the
    /// response type from route metadata.
    /// Infer `unify a b` (record merge, right-biased). Peel the 2-arg spine;
    /// the head must be the `unify` builtin (bare or `base.`-qualified). Both
    /// arguments must infer to closed records (no free row tail — a tail makes
    /// the field-set underdetermined). The result field map is the union with
    /// the right argument's type winning on a name conflict.
    fn try_infer_unify(&mut self, expr: &ast::Expr) -> Option<Ty> {
        let mut args: Vec<&ast::Expr> = Vec::new();
        let mut head = expr;
        while let ast::ExprKind::App { func, arg } = &head.node {
            args.push(arg);
            head = func;
        }
        args.reverse();
        if args.len() != 2 {
            return None;
        }
        let is_unify = match &head.node {
            ast::ExprKind::Var(n) => n == "unify",
            ast::ExprKind::FieldAccess { expr: base, field } => {
                field == "unify" && matches!(&base.node, ast::ExprKind::Var(n) if n == "base")
            }
            _ => false,
        };
        if !is_unify {
            return None;
        }

        let left_ty = self.infer_expr(args[0]);
        let right_ty = self.infer_expr(args[1]);
        let left_res = self.apply(&left_ty).clone();
        let right_res = self.apply(&right_ty).clone();

        // If either argument is still an unresolved type variable (a lambda
        // parameter whose record shape is pinned only when the lambda unifies
        // with its call site), defer the shape computation to end-of-inference
        // and return a fresh result variable so inference can proceed.
        if matches!(left_res.peel_alias(), Ty::Var(_))
            || matches!(right_res.peel_alias(), Ty::Var(_))
        {
            let result = self.fresh_var();
            self.deferred_unifies.push(DeferredUnify {
                left: left_res,
                right: right_res,
                result,
                span: expr.span,
            });
            return Some(Ty::Var(result));
        }

        // Right-biased merge over possibly-open records. The common case is
        // `unify base {updates}` — an open left (a lambda parameter / relation
        // row whose full shape isn't pinned) merged with a closed right (a
        // literal of updates). Overlay the right's fields on the left, keeping
        // the left's row tail so the rest of the base's fields flow through —
        // the same right-biased overlay record-update syntax used. A genuinely
        // non-record argument is the only hard error.
        match (
            Self::record_fields(&left_res),
            Self::record_fields(&right_res),
        ) {
            (Ok((f1, None)), Ok((f2, tail2))) => {
                // Closed left: overlay the right's fields, keep the right's tail
                // (a closed left contributes no tail).
                let mut merged = f1;
                for (k, v) in f2 {
                    merged.insert(k, v);
                }
                Some(Ty::Record(merged, tail2))
            }
            (Ok((f1, Some(_))), Ok((f2, tail2))) => {
                // Open left: the base's other fields live in its row tail, so we
                // cannot just name the right's fields explicitly AND keep the
                // base's tail — a field would sit in both, and unifying the
                // result with the base (as `case … -> unify r {..} ; _ -> r`
                // does) would then report "record fields don't match". Split
                // the row instead (record-update's approach): the result names
                // the merged fields and a FRESH tail, and the base is
                // constrained to have the right's fields, so its own tail
                // becomes that fresh tail with the named fields excluded.
                let rest = self.fresh_var();
                let mut merged = f1;
                for (k, v) in &f2 {
                    merged.insert(k.clone(), v.clone());
                }
                let result = Ty::Record(merged.clone(), Some(rest));
                // Constrain the base: it must contain the merged known fields,
                // with the same fresh tail (so its other fields flow into
                // `rest`).
                let base_constraint = Ty::Record(merged, Some(rest));
                let base_ty = self.apply(&left_ty).clone();
                self.unify(&base_ty, &base_constraint, args[0].span);
                // A right tail beyond the fresh one is not expressible here;
                // the common `unify base {literal-updates}` case has none.
                let _ = tail2;
                Some(result)
            }
            (Err(msg), _) | (_, Err(msg)) => {
                self.error(msg, expr.span);
                // Return Unit to stop the generic application path emitting a
                // second, confusing mismatch on top of ours.
                Some(Ty::Unit(UnitTy::dimensionless()))
            }
        }
    }

    /// Extract a record's field map and row tail (`None` = closed). The only
    /// error is a genuinely non-record argument.
    fn record_fields(
        ty: &Ty,
    ) -> Result<(FieldMap, Option<TyVar>), String> {
        match ty.peel_alias() {
            Ty::Record(fields, tail) => Ok((fields.clone(), *tail)),
            other => Err(format!(
                "unify expects record arguments, got {}",
                match other {
                    Ty::Int => "Int".to_string(),
                    Ty::Float => "Float".to_string(),
                    Ty::Text => "Text".to_string(),
                    Ty::Bool => "Bool".to_string(),
                    Ty::Relation(_) => "a relation".to_string(),
                    _ => "a non-record type".to_string(),
                }
            )),
        }
    }

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
                IndexMap::from([
                    ("name".into(), Ty::Text),
                    ("value".into(), Ty::Text),
                ]),
                None,
            );
            let expected_opts = Ty::Record(
                IndexMap::from([(
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
                let field_tys: FieldMap = info
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
                    IndexMap::from([
                        ("body".into(), raw_body_ty),
                        ("headers".into(), headers_ty),
                    ]),
                    None,
                )
            }
            _ => raw_body_ty,
        };
        let err_ty = Ty::Record(
            IndexMap::from([
                ("message".into(), Ty::Text),
                ("status".into(), Ty::Int),
            ]),
            None,
        );
        let result_adt = Ty::Con("Result".into(), vec![err_ty, ok_ty]);
        self.annotation_vars = saved_annotation_vars;
        Some(Ty::IO(Box::new(result_adt)))
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
        if let (Some(u1), Some(u2)) = (lhs_applied.unit_of(), rhs_applied.unit_of())
            && same_numeric_class(lhs_applied, rhs_applied) {
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

    /// Resolve `unify` shape computations deferred when an argument was an
    /// unresolved type variable at the call node. Runs after all declaration
    /// bodies are inferred, when a lambda parameter's record shape may have
    /// been pinned by its call site. Re-applies both argument types; if both
    /// are now closed records the merged field map is unified with the
    /// placeholder result variable, otherwise the shape error is emitted.
    fn resolve_deferred_unifies(&mut self) {
        let deferred = std::mem::take(&mut self.deferred_unifies);
        for d in &deferred {
            let left = self.apply(&d.left).clone();
            let right = self.apply(&d.right).clone();
            match (
                Self::record_fields(&left),
                Self::record_fields(&right),
            ) {
                (Ok((f1, tail1)), Ok((f2, tail2))) => {
                    let mut merged = f1;
                    for (k, v) in f2 {
                        merged.insert(k, v);
                    }
                    let merged = Ty::Record(merged, tail1.or(tail2));
                    self.unify(&Ty::Var(d.result), &merged, d.span);
                }
                (Err(msg), _) | (_, Err(msg)) => {
                    self.error(msg, d.span);
                }
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
                            unit_binops,
                            ty: *body,
                        }
                    }
                    _ => Scheme::mono(expected.clone()),
                };
                self.bind_at(name, scheme, pat.span);
                self.binding_types.push((pat.span, expected.clone()));
            }
            ast::PatKind::Wildcard => {}
            ast::PatKind::Constructor {
                name,
                payload,
                qualifier,
            } => {
                // A constructor named by an enclosing `with {Type …}` import is
                // in scope UNQUALIFIED in patterns too: `Just {value v}`
                // resolves to `Maybe.Just`. Takes precedence over the
                // must-qualify errors below.
                if qualifier.is_none()
                    && let Some(data_name) = self.resolve_with_imported_ctor(name)
                {
                    match self.instantiate_qualified_ctor(&data_name, name) {
                        Some((data_ty, record_ty)) => {
                            self.unify(&data_ty, expected, pat.span);
                            self.check_pattern(payload, &record_ty);
                        }
                        None => {
                            self.error(
                                format!(
                                    "data type '{data_name}' has no constructor '{name}' in pattern"
                                ),
                                pat.span,
                            );
                        }
                    }
                    return;
                }
                if let Some(q) = qualifier {
                    if q == "base" {
                        // `base.Ctor` is no longer a thing — constructors are
                        // qualified by their DATA TYPE (`Maybe.Just`), not the
                        // `base` namespace, matching user-defined ctors.
                        self.error(
                            format!(
                                "constructors are qualified by their data type, not `base` — write `{name}`'s type (e.g. `Maybe.Just`, `Result.Ok`)"
                            ),
                            pat.span,
                        );
                    } else {
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
                    }
                } else if self.is_builtin_ctor(name) {
                    // Unqualified BUILT-IN constructor in a PATTERN. User code
                    // must qualify by data type (`Maybe.Just`); prelude-internal
                    // patterns (shifted spans) stay bare.
                    if pat.span.start < crate::base::PRELUDE_SPAN_OFFSET {
                        let ty = self
                            .constructors
                            .get(name)
                            .and_then(|i| i.first())
                            .map(|i| i.data_type.as_str())
                            .unwrap_or("Type");
                        self.error(
                            format!(
                                "constructor '{name}' must be qualified (e.g. `{ty}.{name}`)"
                            ),
                            pat.span,
                        );
                    } else if let Some((data_ty, record_ty)) =
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
                let mut field_types = IndexMap::new();
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
                        self.bind_at(&fp.name, Scheme::mono(ft.clone()), fp.name_span);
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
                if let Ty::Var(v) = self.apply(expected)
                    && !self.skolems.contains(&v) {
                        self.bind_var(v, annot_ty.clone(), ty.span);
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
                                    Ty::Variant(IndexMap::new(), None),
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
                            Ty::IO(_) => true,
                            Ty::Fun(_, ret) => returns_io(ret),
                            _ => false,
                        }
                    }
                    let resolved = self.apply(&scheme.ty);
                    returns_io(&resolved)
                })
            }
            ast::ExprKind::SourceRef { .. } | ast::ExprKind::DerivedRef(_) => true,
            ast::ExprKind::Set { .. } | ast::ExprKind::FullSet { .. } => true,
            ast::ExprKind::Atomic(_) => true,
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.expr_is_io_prescan(lhs) || self.expr_is_io_prescan(rhs)
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.expr_is_io_prescan(operand),
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
            // `base.<io-builtin>` — the standard library now lives under the
            // `base` record, so an IO-producing call reaches us as a
            // FieldAccess on `base` rather than a bare `Var`. Treat it exactly
            // like the bare-builtin case above so a do-block whose only IO
            // step is `base.println …` is still classified as an IO block.
            ast::ExprKind::FieldAccess { expr, field } => {
                matches!(&expr.node, ast::ExprKind::Var(n) if n == "base")
                    && crate::builtins::is_io_builtin(field)
            }
            ast::ExprKind::With { record, body, .. } => {
                self.expr_is_io_prescan(record) || self.expr_is_io_prescan(body)
            }
            ast::ExprKind::Lambda { body, .. } => self.expr_is_io_prescan(body),
            ast::ExprKind::TimeUnitLit { value, .. } => self.expr_is_io_prescan(value),
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
                                Ty::IO(inner) => {
                                    let inner = (*inner).clone();
                                    let applied = self.apply(&inner);
                                    (inner, applied)
                                }
                                other => (expr_ty, other),
                            }
                        } else {
                            (expr_ty, resolved)
                        };

                    if let Ty::IO(ref inner) = resolved {
                        // IO bind: x <- ioAction
                        is_io = true;
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
                        is_io = true;
                        let inner_ty = self.fresh();
                        self.unify(
                            &expr_ty,
                            &Ty::IO(Box::new(inner_ty.clone())),
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
                    if let ast::PatKind::Var(var_name) = &pat.node
                        && let ast::ExprKind::SourceRef { name: source_name, .. } = &expr.node {
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
                        if let Ty::IO(ref inner) = resolved {
                            is_io = true;
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
                                    &Ty::IO(Box::new(inner_ty.clone())),
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
                    return Ty::IO(Box::new(Ty::Relation(Box::new(ty.clone()))),
                    );
                }
            let inner = yield_ty.or(last_expr_ty).unwrap_or_else(Ty::unit);
            Ty::IO(Box::new(inner))
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

        // Same-scope duplicate type declarations: two `data` decls with the
        // same name at the SAME `with` nesting depth are a compile error.
        // Without this they silently clobber each other in the global type env
        // (last-write-wins), producing "no constructor" errors far from the
        // real mistake. Nested scopes (different depths) may reuse a name —
        // those are distinct types. Builtin data types are exempt (they are
        // not user `data` decls and never appear here).
        {
            let mut seen: HashMap<(String, usize), Span> = HashMap::new();
            for_each_data_ctor_scoped(program, &mut |name, _params, _ctors, span, depth| {
                let key = (name.to_string(), depth);
                if seen.insert(key, span).is_some() {
                    self.error(
                        format!("duplicate type declaration '{name}' in the same scope"),
                        span,
                    );
                }
            });
        }

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
                    let field_tys: FieldMap = ctors[0]
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
            Ty::IO(_) => Some("IO".into()),
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
        for_each_named_fn(program, &mut |name, sig, value| {
            // A declaration record field named `base` collides with the
            // compiler-owned stdlib record, which IS bound in `scopes[0]`
            // (via `bind_top`) — a genuine lexical conflict, so reject it.
            // Decl fields bind through `bind_top` (pre-registration into
            // `scopes[0]`), bypassing the `bind_at` check, so reject the
            // collision here at the source to get the clean shadowing error
            // instead of a type mismatch against the full `base` record type.
            //
            // stdlib value-fn names (`map`, `count`, …) are NOT in `scopes` —
            // they live in `base` / the `stdlib_schemes` registry — so a decl
            // field named after one is NOT a lexical conflict and is allowed
            // (pure-lexical scoping: `base` is an ordinary record).
            if name == "base" {
                let span = value.map(|v| v.span).unwrap_or(Span { start: 0, end: 0 });
                self.error(
                    format!(
                        "`{name}` is already defined in an enclosing scope, and shadowing is not allowed"
                    ),
                    span,
                );
            }
            self.user_top_level_names.insert(name.to_string());
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
                        ast::Constraint::CollectField { .. } => {
                            // Handled in the implicit-field pipeline (folds <>).
                        }
                    }
                }
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
                self.bind_top(
                    name,
                    Scheme { vars, unit_vars, constraints, unit_binops: vec![], ty },
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
        // These are stdlib value fns: they go into `stdlib_schemes` (never
        // `scopes`) like every other stdlib name — this runs outside the
        // `register_builtins` window, so bind the registry directly.
        let a = self.fresh_var();
        self.stdlib_schemes.insert(
            "toJson".to_string(),
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );
        // `parseJson : Text -> Maybe a` — a failure channel for malformed
        // input. The runtime decoder returns `Nothing` on parse error and
        // `Just decoded` on success, rather than aborting the program.
        let a = self.fresh_var();
        self.stdlib_schemes.insert(
            "parseJson".to_string(),
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                ),
            ),
        );

        // Bind the global `base` record's type LAST, once every stdlib name
        // (including `toJson`/`parseJson` above) is in `stdlib_schemes`.
        self.bind_base_record();
    }

    fn register_builtins(&mut self) {
        // Route stdlib value-fn `bind_top` calls into `stdlib_schemes` (not
        // `scopes`) for the duration of this function. Cleared on exit, before
        // user named fns are bound in `infer_declarations`.
        self.in_register_builtins = true;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.register_builtins_inner();
        }));
        self.in_register_builtins = false;
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    fn register_builtins_inner(&mut self) {
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

        // Built-in ADT: data Level = Debug {} | Info {} | Warn {} | Error {}
        // (log severity; first-class value — pass/compute/pattern-match it).
        for ctor in ["Debug", "Info", "Warn", "Error"] {
            self.constructors.insert(
                ctor.into(),
                vec![CtorInfo {
                    data_type: "Level".into(),
                    data_params: vec![],
                    fields: vec![],
                }],
            );
        }
        self.data_types.insert(
            "Level".into(),
            DataInfo {
                params: vec![],
                ctors: vec![
                    ("Debug".into(), vec![]),
                    ("Info".into(), vec![]),
                    ("Warn".into(), vec![]),
                    ("Error".into(), vec![]),
                ],
            },
        );
        self.builtin_data_types.insert("Level".into());

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

        // Built-in ADT: data List a = Nil {} | Cons {head: a, tail: List a}
        // A singly-linked list. `tail` is self-referential (`List a`), so the
        // recursion lives in the type, not in codegen. Registered as an
        // intrinsic (like `Maybe`/`Result`) because the prelude's `base` record
        // bypasses the desugar hoist that would otherwise lift a user
        // `with`-record's embedded `data` into the global type env.
        let list_a_ty = || ast::Type::new(
            ast::TypeKind::App {
                func: Box::new(ast::Type::new(ast::TypeKind::Named("List".into()), dummy_span)),
                arg: Box::new(ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span)),
            },
            dummy_span,
        );
        let a_var = || ast::Type::new(ast::TypeKind::Var("a".into()), dummy_span);
        self.constructors.insert(
            "Nil".into(),
            vec![CtorInfo {
                data_type: "List".into(),
                data_params: vec!["a".into()],
                fields: vec![],
            }],
        );
        self.constructors.insert(
            "Cons".into(),
            vec![CtorInfo {
                data_type: "List".into(),
                data_params: vec!["a".into()],
                fields: vec![("head".into(), a_var()), ("tail".into(), list_a_ty())],
            }],
        );
        self.data_types.insert(
            "List".into(),
            DataInfo {
                params: vec!["a".into()],
                ctors: vec![
                    ("Nil".into(), vec![]),
                    ("Cons".into(), vec![("head".into(), a_var()), ("tail".into(), list_a_ty())]),
                ],
            },
        );

        // Built-in type: RefinementError = {typeName: Text, violations: [{field: Maybe Text, message: Text}]}
        // Register as a type alias so field access (e.typeName) works.
        self.aliases.insert(
            "RefinementError".into(),
            Ty::Record(
                IndexMap::from([
                    ("typeName".into(), Ty::Text),
                    ("violations".into(), Ty::Relation(Box::new(Ty::Record(
                        IndexMap::from([
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
                IndexMap::from([
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
                IndexMap::from([
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
                Box::new(Ty::IO(Box::new(Ty::unit()))),
            )),
        );

        // print : ∀a. a -> IO {console} {}
        let a = self.fresh_var();
        self.bind_top(
            "print",
            Scheme::poly(vec![a], Ty::Fun(
                Box::new(Ty::Var(a)),
                Box::new(Ty::IO(Box::new(Ty::unit()))),
            )),
        );

        // `logInfo`/`logWarn`/`logError`/`logDebug` are deprecated prelude
        // `base` record fields with a `(<>logCtx)` constraint — NOT top-level
        // polymorphic stdlib fns. No binding here; the prelude record supplies
        // them (and threads the caller's logCtx).

        // emitLog : ∀c. Level -> Text -> c -> IO {console} {} — `base.log`'s
        // runtime target. `c` is the (already merged) logCtx record; kept
        // polymorphic since the merged shape is callsite-dependent.
        let c = self.fresh_var();
        self.bind_top(
            "emitLog",
            Scheme::poly(vec![c], Ty::Fun(
                Box::new(Ty::Con("Level".into(), vec![])),
                Box::new(Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Var(c)),
                        Box::new(Ty::IO(Box::new(Ty::unit()))),
                    )),
                )),
            )),
        );

        // readLine : IO {console} Text
        self.bind_top("readLine", Scheme::mono(
            Ty::IO(Box::new(Ty::Text)),
        ));

        // show : ∀a. a -> Text
        let a = self.fresh_var();
        self.bind_top(
            "show",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );

        // extract : ∀a. a -> Text — render any value as evaluable Knot source,
        // collecting closure/IO dependencies into a `with` block.
        let a = self.fresh_var();
        self.bind_top(
            "extract",
            Scheme::poly(vec![a], Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text))),
        );

        // compile : ∀a. Text -> Result Text a — JIT-compile+eval a knot source
        // string in-process against the host runtime. `Ok {value}` on success
        // (the value forced at compile time, typed as `a`); `Err {error: Text}`
        // carrying the compile-error message on any compile error, or when the
        // program's type doesn't match `a`.
        let a = self.fresh_var();
        self.bind_top(
            "compile",
            Scheme::poly(
                vec![a],
                Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::Con(
                        "Result".into(),
                        vec![Ty::Text, Ty::Var(a)],
                    )),
                ),
            ),
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
                Box::new(Ty::IO(Box::new(Ty::unit()))),
            )),
        );

        // todo : a  — the unimplemented hole. Fully polymorphic so it unifies
        // with whatever type its position expects (including `IO _`); the
        // expected type is recorded per reference (see `todo_types`) and the
        // runtime aborts with that context. Never produces a value.
        {
            let a = self.fresh_var();
            self.bind_top("todo", Scheme::poly(vec![a], Ty::Var(a)));
        }

        // trace : ∀a. a -> a  — print the traced value (with source context and
        // in-scope bindings, like `todo`) and return it unchanged. Fully
        // polymorphic on the value type; the per-reference type is recorded in
        // `trace_types` for the runtime report.
        {
            let a = self.fresh_var();
            self.bind_top(
                "trace",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(a))),
                ),
            );
        }

        // now : IO {clock} Int Ms
        {
            let int_ms = Ty::int_with_unit(UnitTy::named("Ms"));
            self.bind_top("now", Scheme::mono(
                Ty::IO(Box::new(int_ms)),
            ));
        }

        // sleep : Int Ms -> IO {clock} {}
        {
            let int_ms = Ty::int_with_unit(UnitTy::named("Ms"));
            self.bind_top(
                "sleep",
                Scheme::mono(Ty::Fun(
                    Box::new(int_ms),
                    Box::new(Ty::IO(Box::new(Ty::unit()))),
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
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u.clone()),
                        Box::new(Ty::IO(Box::new(int_u))),
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
                unit_binops: vec![],
                ty: Ty::IO(Box::new(float_u)),
            });
        }

        // randomUuid : IO {random} Uuid (UUIDv7)
        self.bind_top("randomUuid", Scheme::mono(
            Ty::IO(Box::new(Ty::Uuid)),
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
                        Box::new(Ty::IO(Box::new(Ty::Var(a)))),
                        Box::new(Ty::IO(Box::new(Ty::unit()))),
                    ),
                ),
            );
        }

        // race : ∀a b. IO a -> IO b -> IO (Result a b)
        // The winner is reported via the built-in `Result a b` ADT —
        // `Err {error: a}` when the left action wins, `Ok {value: b}` when
        // the right action wins.
        {
            let sp = Span::new(0, 0);
            let mk = |node| ast::Type { node, span: sp };
            let var = |n: &str| mk(ast::TypeKind::Var(n.to_string()));
            let io = |ty: ast::Type| mk(ast::TypeKind::IO { ty: Box::new(ty) });
            let result_ab = mk(ast::TypeKind::App {
                func: Box::new(mk(ast::TypeKind::App {
                    func: Box::new(mk(ast::TypeKind::Named("Result".into()))),
                    arg: Box::new(var("a")),
                })),
                arg: Box::new(var("b")),
            });
            let race_ast = mk(ast::TypeKind::Function {
                param: Box::new(io(var("a"))),
                result: Box::new(mk(ast::TypeKind::Function {
                    param: Box::new(io(var("b"))),
                    result: Box::new(io(result_ab)),
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

        // listen : ∀a u. Int u -> Server a -> IO {}
        // The handler value must be a `Server a`, produced by the
        // `serve a where ...` expression. Each endpoint handler returns
        // its own response type; the runtime serializes the result based
        // on which endpoint matched.
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            let server = Ty::Con("Server".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listen",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(int_u),
                        Box::new(Ty::Fun(
                            Box::new(server),
                            Box::new(Ty::IO(Box::new(Ty::unit()))),
                        )),
                    ),
                },
            );
        }

        // listenOn : ∀a u. Text -> Int u -> Server a -> IO {}
        {
            let a = self.fresh_var();
            let u = self.fresh_unit_var();
            let int_u = Ty::int_with_unit(UnitTy::var(u));
            let server = Ty::Con("Server".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listenOn",
                Scheme {
                    vars: vec![a],
                    unit_vars: vec![u],
                    constraints: vec![],
                    unit_binops: vec![],
                    ty: Ty::Fun(
                        Box::new(Ty::Text),
                        Box::new(Ty::Fun(
                            Box::new(int_u),
                            Box::new(Ty::Fun(
                                Box::new(server),
                                Box::new(Ty::IO(Box::new(Ty::unit()))),
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
                IndexMap::from([
                    ("message".into(), Ty::Text),
                    ("status".into(), Ty::Int),
                ]),
                None,
            );
            let result_ty = Ty::Con("Result".into(), vec![err_ty.clone(), Ty::Var(b)]);
            let io_ty = Ty::IO(Box::new(result_ty));
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
            let io_ty2 = Ty::IO(Box::new(result_ty2));
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

        // distinct : ∀a. [a] -> [a]  (builtin → knot_relation_dedup). Relations
        // are already sets, so the in-memory path is a dedup no-op; the value
        // is the SQL pushdown — `distinct (map f *src)` → SELECT DISTINCT.
        {
            let a = self.fresh_var();
            self.bind_top(
                "distinct",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    ),
                ),
            );
        }

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
                                Box::new(Ty::IO(Box::new(Ty::unit()),
                                )),
                            )),
                            Box::new(Ty::IO(Box::new(Ty::unit()),
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

        // bind : ∀a b. (a -> [b]) -> [a] -> [b]  (builtin → knot_relation_bind)
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

        // sortByDesc : ∀a b. (a -> b) -> [a] -> [a]
        let a = self.fresh_var();
        let b = self.fresh_var();
        self.bind_top(
            "sortByDesc",
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

        // ── List ADT builtins (`base.list.*`) ─────────────────────────────
        // Runtime-implemented (knot_list_*); the prelude record can't
        // self-reference for recursion, so these are codegen builtins. Names
        // are `list`-prefixed to avoid clashing with the flat relation
        // builtins (`map`/`length`/`head`/`reverse` operate on `[a]`).
        // List is a sequence: order-preserving, duplicates allowed.
        {
            // listCons : ∀a. a -> List a -> List a
            let a = self.fresh_var();
            let list_a = || Ty::Con("List".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listCons",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Var(a)),
                        Box::new(Ty::Fun(Box::new(list_a()), Box::new(list_a()))),
                    ),
                ),
            );
        }
        // listNil : ∀a b. b -> List a   (exposed as a 1-arg builtin that
        // ignores its argument, so the bare function value has the standard
        // 1-param shape. The param type `b` is independent of the element
        // type `a`, so applying it to any value — e.g. `nil {}` — leaves `a`
        // free to unify per call site, keeping `nil` polymorphic.)
        {
            let a = self.fresh_var();
            let b = self.fresh_var();
            self.bind_top(
                "listNil",
                Scheme::poly(
                    vec![a, b],
                    Ty::Fun(
                        Box::new(Ty::Var(b)),
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                    ),
                ),
            );
        }
        // listIsNil : ∀a. List a -> Bool
        {
            let a = self.fresh_var();
            self.bind_top(
                "listIsNil",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                        Box::new(Ty::Bool),
                    ),
                ),
            );
        }
        // listHead : ∀a. List a -> Maybe a
        {
            let a = self.fresh_var();
            self.bind_top(
                "listHead",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                        Box::new(Ty::Con("Maybe".into(), vec![Ty::Var(a)])),
                    ),
                ),
            );
        }
        // listTail : ∀a. List a -> Maybe (List a)
        {
            let a = self.fresh_var();
            let list_a = || Ty::Con("List".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listTail",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(list_a()),
                        Box::new(Ty::Con("Maybe".into(), vec![list_a()])),
                    ),
                ),
            );
        }
        // listLength : ∀a. List a -> Int  (dimensionless)
        {
            let a = self.fresh_var();
            self.bind_top(
                "listLength",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                        Box::new(Ty::int_with_unit(UnitTy::dimensionless())),
                    ),
                ),
            );
        }
        // listMap : ∀a b. (a -> b) -> List a -> List b
        {
            let a = self.fresh_var();
            let b = self.fresh_var();
            self.bind_top(
                "listMap",
                Scheme::poly(
                    vec![a, b],
                    Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Var(b)))),
                        Box::new(Ty::Fun(
                            Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                            Box::new(Ty::Con("List".into(), vec![Ty::Var(b)])),
                        )),
                    ),
                ),
            );
        }
        // listFilter : ∀a. (a -> Bool) -> List a -> List a
        {
            let a = self.fresh_var();
            let list_a = || Ty::Con("List".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listFilter",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Bool))),
                        Box::new(Ty::Fun(Box::new(list_a()), Box::new(list_a()))),
                    ),
                ),
            );
        }
        // listFold : ∀a b. (b -> a -> b) -> b -> List a -> b
        {
            let a = self.fresh_var();
            let b = self.fresh_var();
            self.bind_top(
                "listFold",
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
                                Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                                Box::new(Ty::Var(b)),
                            )),
                        )),
                    ),
                ),
            );
        }
        // listReverse : ∀a. List a -> List a
        {
            let a = self.fresh_var();
            let list_a = || Ty::Con("List".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listReverse",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(Box::new(list_a()), Box::new(list_a())),
                ),
            );
        }
        // listAppend : ∀a. List a -> List a -> List a
        {
            let a = self.fresh_var();
            let list_a = || Ty::Con("List".into(), vec![Ty::Var(a)]);
            self.bind_top(
                "listAppend",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(list_a()),
                        Box::new(Ty::Fun(Box::new(list_a()), Box::new(list_a()))),
                    ),
                ),
            );
        }
        // listFromRelation : ∀a. [a] -> List a
        {
            let a = self.fresh_var();
            self.bind_top(
                "listFromRelation",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                    ),
                ),
            );
        }
        // listToRelation : ∀a. List a -> [a]
        {
            let a = self.fresh_var();
            self.bind_top(
                "listToRelation",
                Scheme::poly(
                    vec![a],
                    Ty::Fun(
                        Box::new(Ty::Con("List".into(), vec![Ty::Var(a)])),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
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

        // trimAscii / ltrimAscii / rtrimAscii : Text -> Text
        for name in ["trimAscii", "ltrimAscii", "rtrimAscii"] {
            self.bind_top(
                name,
                Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
            );
        }

        // byteLength : Text -> Int
        self.bind_top(
            "byteLength",
            Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Int))),
        );

        // toAsciiLower / toAsciiUpper : Text -> Text
        for name in ["toAsciiLower", "toAsciiUpper"] {
            self.bind_top(
                name,
                Scheme::mono(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Text))),
            );
        }

        // contains : Text -> Text -> Bool
        self.bind_top(
            "contains",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bool))),
            )),
        );

        // startsWith : Text -> Text -> Bool
        self.bind_top(
            "startsWith",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(Box::new(Ty::Text), Box::new(Ty::Bool))),
            )),
        );

        // endsWith : Text -> Text -> Bool
        self.bind_top(
            "endsWith",
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
                Box::new(Ty::IO(Box::new(Ty::Text))),
            )),
        );

        // writeFile : Text -> Text -> IO {fs} {}
        self.bind_top(
            "writeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Fun(
                    Box::new(Ty::Text),
                    Box::new(Ty::IO(Box::new(Ty::unit()))),
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
                    Box::new(Ty::IO(Box::new(Ty::unit()))),
                )),
            )),
        );

        // fileExists : Text -> IO {fs} Bool
        self.bind_top(
            "fileExists",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(Box::new(Ty::Bool))),
            )),
        );

        // removeFile : Text -> IO {fs} {}
        self.bind_top(
            "removeFile",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(Box::new(Ty::unit()))),
            )),
        );

        // listDir : Text -> IO {fs} [Text]
        self.bind_top(
            "listDir",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::IO(Box::new(Ty::Relation(Box::new(Ty::Text))))),
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

        // floor : Float -> Int  (round toward negative infinity)
        self.bind_top(
            "floor",
            Scheme::mono(Ty::Fun(Box::new(Ty::Float), Box::new(Ty::Int))),
        );

        // intToFloat : Int -> Float  (lossy past 2^53)
        self.bind_top(
            "intToFloat",
            Scheme::mono(Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Float))),
        );

        // abs : Int -> Int  (pushable to ABS)
        self.bind_top("abs", Scheme::mono(Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Int))));
        // intMin / intMax : Int -> Int -> Int  (pushable to scalar min/max)
        let int2 = Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Int))));
        self.bind_top("intMin", Scheme::mono(int2.clone()));
        self.bind_top("intMax", Scheme::mono(int2));
        // clamp : Int -> Int -> Int -> Int  (pushable to min(max(x,lo),hi))
        let int3 = Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Fun(Box::new(Ty::Int), Box::new(Ty::Int))))));
        self.bind_top("clamp", Scheme::mono(int3));

        // unify : {r1} -> {r2} -> {r1 ∪ r2} — record merge, right-biased. The
        // result type is shape-dependent (a function of both arguments' field
        // names), which no single forall-scheme can express without a row-union
        // type operator, so the real typing is a special case in
        // `try_infer_unify`. This placeholder scheme exists only so the name
        // resolves in scope; the special case overrides it for full
        // applications, and the fully-polymorphic body is harmless when the
        // special case doesn't fire (e.g. partial application, which is
        // shape-dependent and thus not precisely typeable anyway).
        let ua = self.fresh_var();
        let ub = self.fresh_var();
        let uc = self.fresh_var();
        self.bind_top(
            "unify",
            Scheme::poly(
                vec![ua, ub, uc],
                Ty::Fun(
                    Box::new(Ty::Var(ua)),
                    Box::new(Ty::Fun(Box::new(Ty::Var(ub)), Box::new(Ty::Var(uc)))),
                ),
            ),
        );

        // textToInt : Text -> Maybe Int  (Nothing on malformed input)
        self.bind_top(
            "textToInt",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Int])),
            )),
        );

        // textToFloat : Text -> Maybe Float  (Nothing on malformed input)
        self.bind_top(
            "textToFloat",
            Scheme::mono(Ty::Fun(
                Box::new(Ty::Text),
                Box::new(Ty::Con("Maybe".into(), vec![Ty::Float])),
            )),
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
            IndexMap::from([
                ("privateKey".into(), Ty::Bytes),
                ("publicKey".into(), Ty::Bytes),
            ]),
            None,
        );
        self.bind_top("generateKeyPair", Scheme::mono(
            Ty::IO(Box::new(key_pair_record.clone())),
        ));

        // generateSigningKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
        self.bind_top("generateSigningKeyPair", Scheme::mono(
            Ty::IO(Box::new(key_pair_record)),
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
                    Box::new(Ty::IO(Box::new(maybe_bytes.clone()),
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

    /// Bind the global `base` record's TYPE. Its field types come from the
    /// stdlib schemes bound during `pre_register` (`map`, `filter`, `toJson`,
    /// …) plus the prelude's own polymorphic helpers (`min`/`max`/`when`/
    /// `unless`). Binding `base` as a top-level global — rather than only as a
    /// `with` field — makes `base.X` resolve inside nested `with` field-values
    /// and decl bodies, which compile in fresh envs that never see the
    /// injected prelude `with`. Constructors (`Just`/…) and the server forms
    /// are NOT fields here; dedicated `base.Ctor` / `base.<form>` arms route
    /// them.
    ///
    /// The record's scheme is generalized over every free type variable in the
    /// field types, so each field access re-instantiates them (a field like
    /// `map : (a -> b) -> [a] -> [b]` stays polymorphic per use, just as the
    /// bare stdlib binding is).
    ///
    /// Must run AFTER every stdlib `bind_top` in `pre_register` — including
    /// the `toJson`/`parseJson` re-bind at the end — or those names are absent
    /// from the record type.
    fn bind_base_record(&mut self) {
        // Infer the prelude `base` record's type directly from its source.
        // The stdlib `Var(name)` fields must resolve, but stdlib names are
        // deliberately absent from `scopes` (they live in `stdlib_schemes`).
        // Push a temporary scope holding the stdlib schemes so the record's
        // `map: Var(map)` fields resolve; the polymorphic helper lambdas
        // (`min`/`max`/`when`/`unless`) infer with CORRECT effect-row
        // generalization — hand-building their schemes gets the `IO {| e}`
        // rows wrong (rigid instead of unifiable). The scope is popped before
        // returning, so user code never sees these names. Generalize the
        // inferred record type so each `base.X` access re-instantiates.
        let base_record = crate::base::prelude_base_record();
        // Register the `base` record's dictionary constraints (`^`/`<>`
        // fields) under their dotted `base.<name>` paths, exactly as
        // `infer_declarations` does for user namespaced records, so a
        // `base.log` (or any constrained `base.*` fn) splices its dictionary
        // at the user's callsite the same way a user-defined constrained fn
        // does. `bind_base_record` types the record directly and never runs
        // `infer_declarations`, so without this the constraint would not fire.
        if let ast::ExprKind::Record(base_fields) = &base_record.node {
            for f in base_fields {
                let Some(sig) = &f.sig else { continue };
                let saved_flag = self.in_type_annotation;
                let saved_av = std::mem::take(&mut self.annotation_vars);
                let saved_auv = std::mem::take(&mut self.annotation_unit_vars);
                self.in_type_annotation = true;
                let implicit: Vec<(String, Ty)> = sig
                    .constraints
                    .iter()
                    .filter_map(|c| match c {
                        ast::Constraint::ImplicitField { field, ty } => {
                            Some((field.clone(), self.ast_type_to_ty(ty)))
                        }
                        ast::Constraint::CollectField { field, .. } => {
                            Some((field.clone(), self.fresh()))
                        }
                        _ => None,
                    })
                    .collect();
                let folds: Vec<String> = sig
                    .constraints
                    .iter()
                    .filter_map(|c| match c {
                        ast::Constraint::CollectField { field, .. } => Some(field.clone()),
                        _ => None,
                    })
                    .collect();
                self.in_type_annotation = saved_flag;
                self.annotation_vars = saved_av;
                self.annotation_unit_vars = saved_auv;
                if !implicit.is_empty() {
                    self.implicit_dict_fns.insert(format!("base.{}", f.name), implicit);
                }
                if !folds.is_empty() {
                    self.fold_dict_fields.insert(format!("base.{}", f.name), folds);
                }
            }
        }
        self.scopes.push(self.stdlib_schemes.clone());
        self.push_scope();
        let inferred = self.infer_expr(&base_record);
        let resolved = self.apply(&inferred);
        self.pop_scope();
        self.scopes.pop(); // the temporary stdlib scope
        let scheme = self.generalize(&resolved);
        self.bind_top("base", scheme);
    }


    // ── Declaration inference (phase 4) ──────────────────────────

    /// Walk the declaration record and record each `name <literal>` field as a
    /// compile-time constant, for constant-checking at refined-type boundaries.
    /// Only plain literal values are collected (`five 5`, `name "Ada"`); lambdas,
    /// relations, ADTs, and decl forms are ignored.
    fn collect_const_literals(&mut self, program: &ast::Expr) {
        fn decl_record(e: &ast::Expr) -> Option<&ast::Expr> {
            match &e.node {
                ast::ExprKind::With { record, body, .. } => {
                    if matches!(body.node, ast::ExprKind::With { .. }) {
                        decl_record(body)
                    } else {
                        Some(record)
                    }
                }
                ast::ExprKind::Record(_) => Some(e),
                _ => None,
            }
        }
        if let Some(record) = decl_record(program)
            && let ast::ExprKind::Record(fields) = &record.node
        {
            for fl in fields {
                if let Some(lit) = crate::codegen::extract_literal(&fl.value) {
                    self.const_literals.insert(fl.name.clone(), lit);
                }
            }
        }
    }

    fn infer_declarations(&mut self, program: &ast::Expr) {
        // Collect top-level constant literals (`five 5`) from the declaration
        // record so a constant flowing into a refined type can be checked
        // against the predicate at compile time.
        self.collect_const_literals(program);
        // Named functions: `with`-record fields with a lambda body.
        for_each_named_fn(program, &mut |name, ty, body| {
            let body = match body {
                Some(b) => b,
                None => return,
            };
            // Required CLI constant (sig-only field, empty-record placeholder
            // body): there is no value to check against the sig. Skip — codegen
            // registers the startup `--name=value` lookup and the field's type
            // is the sig (set during pre-registration).
            if ty.is_some()
                && matches!(&body.node, ast::ExprKind::Record(fs) if fs.is_empty())
            {
                return;
            }
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
                                                Some((field.clone(), self.ast_type_to_ty(ty)))
                                            }
                                            ast::Constraint::CollectField { field, .. } => {
                                                // `(<>field)` fold: dict collected+merged
                                                // at the callsite; type is a fresh var.
                                                Some((field.clone(), self.fresh()))
                                            }
                                            _ => None,
                                        })
                                        .collect();
                                    let ffolds: Vec<String> = sig
                                        .constraints
                                        .iter()
                                        .filter_map(|c| match c {
                                            ast::Constraint::CollectField { field, .. } => {
                                                Some(field.clone())
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
                                    if !ffolds.is_empty() {
                                        self.fold_dict_fields
                                            .insert(format!("{name}.{}", f.name), ffolds);
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
                                    ast::Constraint::CollectField { .. } => {
                                        // Recorded into `implicit_dict_fns` below
                                        // (as a fold).
                                    }
                                }
                            }
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
                                        Some((field.clone(), self.ast_type_to_ty(ty)))
                                    }
                                    ast::Constraint::CollectField { field, .. } => {
                                        // `(<>field)` fold constraint: the dict is
                                        // collected+merged at the callsite from the
                                        // in-scope `field` values (like `^` but
                                        // merging all). Its type is a fresh var —
                                        // the merged shape is callsite-dependent.
                                        Some((field.clone(), self.fresh()))
                                    }
                                    _ => None,
                                })
                                .collect();
                            if !implicit.is_empty() {
                                self.implicit_dict_fns.insert(name.to_string(), implicit);
                            }
                            // Track which of this fn's dict fields are FOLDS
                            // (`<>`) vs single-match (`^`), so the callsite splice
                            // merges all fragments for a fold instead of resolving
                            // a single match.
                            let fold_fields: Vec<String> = ts
                                .constraints
                                .iter()
                                .filter_map(|c| match c {
                                    ast::Constraint::CollectField { field, .. } => {
                                        Some(field.clone())
                                    }
                                    _ => None,
                                })
                                .collect();
                            if !fold_fields.is_empty() {
                                self.fold_dict_fields.insert(name.to_string(), fold_fields);
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
                            // `(<>field)` fold constraints elaborate to a leading
                            // dict param whose type is a fresh var (Hole/Callsite),
                            // NOT a named annotation var — so it isn't in
                            // `annotation_vars` and would otherwise stay
                            // MONOMORPHIC across callsites (every call shares the
                            // one dict var, so two calls with differently-shaped
                            // contexts collide: "record fields don't match").
                            // Quantify it (and any `_` hole in the signature) so
                            // each callsite instantiates a fresh dict var. Only
                            // quantify vars that are free in the rebuilt
                            // annotation type but NOT free in the enclosing env
                            // (outer-scope vars must stay shared) and not
                            // skolems (rigid body-check vars).
                            let env_free = self.free_vars_in_env();
                            let mut extra: Vec<TyVar> = self
                                .free_vars(&ann_ty)
                                .into_iter()
                                .filter(|v| {
                                    !vars.contains(v)
                                        && !self.skolems.contains(v)
                                        && !env_free.contains(v)
                                })
                                .collect();
                            extra.sort_unstable();
                            vars.append(&mut extra);
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
                                Scheme { vars, unit_vars, constraints, unit_binops: captured_binops, ty: ann_ty },
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
                        Ty::IO(inner) => (*inner).clone(),
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
                        Ty::IO(inner) => (*inner).clone(),
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
            IndexMap::from([
                ("requests".into(), Ty::Int),
                ("window".into(), Ty::int_with_unit(UnitTy::named("Ms"))),
            ]),
            None,
        );
        let expected = Ty::Record(
            IndexMap::from([
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

    fn extract_todo_types(&self) -> TodoTypes {
        let mut info = TodoTypes::new();
        for (span, ty) in &self.todo_types {
            let applied = self.apply(ty);
            let applied = default_free_unit_vars(&applied);
            let var_map = var_map_for(&applied);
            let unit_var_map = unit_var_map_for(&applied);
            info.insert(*span, display_ty_clean(&applied, &var_map, &unit_var_map));
        }
        info
    }

    /// Snapshot the local bindings visible at the current point, innermost
    /// scope first. `scopes` is a stack of per-frame `HashMap`s; walking it in
    /// reverse and skipping names already seen yields each binding under its
    /// innermost (shadowing) definition, deduplicated. Stdlib value-fns are
    /// not in `scopes` (they live in `stdlib_schemes`), so this captures only
    /// genuine user/lambda/`with`/do-block bindings.
    fn visible_bindings(&self) -> Vec<(String, Scheme)> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for scope in self.scopes.iter().rev() {
            for (name, scheme) in scope {
                // `base` is the namespaced stdlib prelude record — dumping its
                // full type into every report is noise. It's always reachable
                // via `base.` anyway, so it's not a genuine local.
                if name == "base" {
                    continue;
                }
                if seen.insert(name.clone()) {
                    out.push((name.clone(), scheme.clone()));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn extract_todo_bindings(&self) -> TodoBindings {
        let mut out = TodoBindings::new();
        for (span, bindings) in &self.todo_scopes {
            let rendered = bindings
                .iter()
                .map(|(name, scheme)| (name.clone(), self.display_scheme(scheme)))
                .collect();
            out.insert(*span, rendered);
        }
        out
    }

    /// Extract `base.trace` spans → traced value type display strings, mirroring
    /// `extract_todo_types`.
    fn extract_trace_types(&self) -> TraceTypes {
        let mut info = TraceTypes::new();
        for (span, ty) in &self.trace_types {
            let applied = self.apply(ty);
            let applied = default_free_unit_vars(&applied);
            let var_map = var_map_for(&applied);
            let unit_var_map = unit_var_map_for(&applied);
            info.insert(*span, display_ty_clean(&applied, &var_map, &unit_var_map));
        }
        info
    }

    /// Extract `base.trace` spans → in-scope local bindings, mirroring
    /// `extract_todo_bindings`.
    fn extract_trace_bindings(&self) -> TraceBindings {
        let mut out = TraceBindings::new();
        for (span, bindings) in &self.trace_scopes {
            let rendered = bindings
                .iter()
                .map(|(name, scheme)| (name.clone(), self.display_scheme(scheme)))
                .collect();
            out.insert(*span, rendered);
        }
        out
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
        Ty::IO(inner) => collect_unit_vars_ordered(inner, out),
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
        (Ty::IO(i1), Ty::IO(i2)) => {
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
        Ty::IO(inner) => {
            collect_vars_ordered(inner, out);
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
    display_ty_clean_inner(ty, names, unit_names, false, false)
}

/// Render a type for the `base.compile` expected-type WIRE: identical to
/// `display_ty_clean` except dimensionless `Int`/`Float` keep their unit as
/// `Int 1`/`Float 1`, so the string round-trips through `parser::parse_type_str`
/// (the JIT's expected-type parser requires an explicit unit; bare `Int` would
/// parse to `Ty::Error` and wrongly reject a dimensionless-Int snippet).
fn display_ty_wire(
    ty: &Ty,
    names: &HashMap<TyVar, usize>,
    unit_names: &HashMap<UnitVar, usize>,
) -> String {
    display_ty_clean_inner(ty, names, unit_names, false, true)
}

fn display_ty_clean_inner(
    ty: &Ty,
    names: &HashMap<TyVar, usize>,
    unit_names: &HashMap<UnitVar, usize>,
    in_fun: bool,
    wire: bool,
) -> String {
    match ty {
        Ty::Var(v) => var_letter(names.get(v).copied().unwrap_or(*v as usize)),
        // Primitive `Int`/`Float` are dimensionless; on the wire render them
        // with the explicit `1` unit so they re-parse (bare `Int`/`Float` is a
        // unit-annotation error in the JIT's expected-type parser).
        Ty::Int => if wire { "Int 1".into() } else { "Int".into() },
        Ty::Float => if wire { "Float 1".into() } else { "Float".into() },
        Ty::Text => "Text".into(),
        Ty::Bool => "Bool".into(),
        Ty::Bytes => "Bytes".into(),
        Ty::Uuid => "Uuid".into(),
        Ty::Assoc(name, inner) => {
            format!("{} {}", name, display_ty_clean_inner(inner, names, unit_names, true, wire))
        }
        Ty::Fun(p, r) => {
            let s = format!(
                "{} -> {}",
                display_ty_clean_inner(p, names, unit_names, true, wire),
                display_ty_clean_inner(r, names, unit_names, false, wire)
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
                .map(|(n, t)| format!("{}: {}", n, display_ty_clean_inner(t, names, unit_names, false, wire)))
                .collect();
            if let Some(rv) = row {
                parts.push(format!("| {}", var_letter(names.get(rv).copied().unwrap_or(*rv as usize))));
            }
            format!("{{{}}}", parts.join(", "))
        }
        Ty::Relation(inner) => format!("[{}]", display_ty_clean_inner(inner, names, unit_names, false, wire)),
        Ty::Con(name, args) => {
            // Unit-bearing Int/Float → `Int u`/`Float u`, collapsing to
            // `Int`/`Float` when dimensionless. On the compile-expected WIRE
            // keep the dimensionless unit as `Int 1`/`Float 1` so the string
            // re-parses (bare `Int` has no unit and would become `Ty::Error`).
            if (name == "Int" || name == "Float") && args.len() == 1
                && let Ty::Unit(u) = args[0].peel_alias() {
                    if u.is_dimensionless() {
                        return if wire { format!("{} 1", name) } else { name.clone() };
                    }
                    return format!("{} {}", name, display_unit_clean(u, unit_names));
                }
            if args.is_empty() {
                name.clone()
            } else {
                let args_str: Vec<String> =
                    args.iter().map(|a| display_ty_clean_inner(a, names, unit_names, false, wire)).collect();
                format!("{} {}", name, args_str.join(" "))
            }
        }
        Ty::Variant(ctors, row) => {
            let mut parts: Vec<String> = ctors
                .iter()
                .map(|(name, ft)| format!("{} {}", name, display_ty_clean_inner(ft, names, unit_names, false, wire)))
                .collect();
            if let Some(rv) = row {
                parts.push(var_letter(names.get(rv).copied().unwrap_or(*rv as usize)));
            }
            format!("<{}>", parts.join(" | "))
        }
        Ty::TyCon(name) => name.clone(),
        Ty::App(f, a) => format!(
            "({} {})",
            display_ty_clean_inner(f, names, unit_names, false, wire),
            display_ty_clean_inner(a, names, unit_names, false, wire)
        ),
        Ty::IO(inner) => {
            format!("IO {}", display_ty_clean_inner(inner, names, unit_names, false, wire))
        }
        Ty::Forall(vars, inner) => {
            if vars.is_empty() {
                display_ty_clean_inner(inner, names, unit_names, in_fun, wire)
            } else {
                let bound: Vec<String> = vars
                    .iter()
                    .map(|v| var_letter(names.get(v).copied().unwrap_or(*v as usize)))
                    .collect();
                let s = format!(
                    "forall {}. {}",
                    bound.join(" "),
                    display_ty_clean_inner(inner, names, unit_names, false, wire)
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
/// from full replacement (which requires `full *rel = ...`).
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
        ast::ExprKind::SourceRef { name, .. } => name == source_name,
        // `^x` reads a record field, never a source relation directly.
        ast::ExprKind::ImplicitRef(_) => false,
        // `<>x` likewise reads record fields, never a source relation.
        ast::ExprKind::CollectFold(_) => false,
        // `_` hole reads nothing.
        ast::ExprKind::TypeHole => false,
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
        ast::ExprKind::With { record, body, .. } => {
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
        | ast::ExprKind::FullSet { target, value } => {
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
    FoldDictArgs,
    CollectRefs,
    ResolvedCalls,
    TodoTypes,
    TodoBindings,
    TraceTypes,
    TraceBindings,
    CompileExpectedTypes,
    Option<String>,
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
    crate::stack::grow(|| check_inner(program, None))
}

// ── Compile-snippet subsumption (Option A) ─────────────────────────────────
//
// When the JIT compiles a `base.compile` snippet, the HOST's expected type
// travels to it as a source-syntax type-annotation string. To check "the
// snippet's type is usable where the expected type is wanted" on REAL types
// (not a lossy string comparison), the expected string is passed INTO `check`
// as a parameter (a thread-local would NOT survive `stack::grow`'s thread
// hop), and — while the snippet's `Infer` is still alive at the end of
// `check_inner` — parsed into a `Ty` in that same context (so the snippet's
// own `data` ADT definitions resolve) and checked with `ty_subsumes` against
// the snippet's inferred file-body type. The verdict is returned via a
// shared cell so the 25-field `CheckOutput` tuple is untouched.
static SUBSUMPTION_VERDICT: std::sync::Mutex<Option<bool>> = std::sync::Mutex::new(None);

/// Like `check`, but additionally subsumes the program's body type against
/// `expected_src` (a knot source type-annotation string, possibly prefixed by
/// the host's `data` decls). The verdict is retrievable via
/// `take_subsumption_verdict` after this returns.
pub fn check_with_expected(program: &mut ast::Expr, expected_src: &str) -> CheckOutput {
    let src = expected_src.to_string();
    crate::stack::grow(|| check_inner(program, Some(&src)))
}

/// Read (and clear) the verdict produced by the last `check_with_expected`.
/// `Some(true)` = snippet subsumes expected; `Some(false)` = it doesn't;
/// `None` = the expected type failed to parse, or the snippet had no
/// inferrable body type.
pub fn take_subsumption_verdict() -> Option<bool> {
    SUBSUMPTION_VERDICT.lock().unwrap_or_else(|e| e.into_inner()).take()
}

/// Is this expression a reference to the `todo` hole, `base.todo`? Purely
/// syntactic on the reference shape; the type checker has already resolved
/// that the field means the builtin. (Bare `todo` is not a `base` record
/// field, so it is never in scope — only the namespaced form exists.)
fn expr_is_todo_ref(expr: &ast::Expr) -> bool {
    // `_` in value position is a TypeHole: it behaves exactly like `base.todo`
    // (polymorphic placeholder that warns at compile time, errors at runtime),
    // so it is detected here too and shares the whole todo pipeline.
    if matches!(&expr.node, ast::ExprKind::TypeHole) {
        return true;
    }
    matches!(
        &expr.node,
        ast::ExprKind::FieldAccess { expr: base, field }
            if field == "todo" && matches!(&base.node, ast::ExprKind::Var(n) if n == "base")
    )
}

/// Is this expression a reference to the tracer, `base.trace`? Same purely
/// syntactic shape check as `expr_is_todo_ref`. (Bare `trace` is not a `base`
/// record field, so only the namespaced form exists.)
fn expr_is_trace_ref(expr: &ast::Expr) -> bool {
    matches!(
        &expr.node,
        ast::ExprKind::FieldAccess { expr: base, field }
            if field == "trace" && matches!(&base.node, ast::ExprKind::Var(n) if n == "base")
    )
}

/// Split a `base.compile` expected-type payload into the host's prepended
/// `data` declarations and the trailing type. The host emits
/// `data A = ... \n data B = ... \n <type>`; the decls let the JIT compare
/// constructor sets structurally. Returns `(name → ctor-name-set, type_src)`.
/// With no leading `data` lines the set is empty and the whole string is the
/// type.
fn split_host_data_decls(
    src: &str,
) -> (HashMap<String, Vec<(String, Vec<(String, String)>)>>, String) {
    let mut sets: HashMap<String, Vec<(String, Vec<(String, String)>)>> = HashMap::new();
    let mut rest = src;
    loop {
        let trimmed = rest.trim_start();
        if !trimmed.starts_with("data ") {
            return (sets, trimmed.to_string());
        }
        // Consume one line: `data Name = Ctor {} | Ctor {f: T} | ...`.
        let (line, next) = match trimmed.find('\n') {
            Some(i) => (&trimmed[..i], &trimmed[i + 1..]),
            None => (trimmed, ""),
        };
        if let Some((name, ctors)) = parse_data_decl_ctors(line) {
            sets.insert(name, ctors);
        }
        rest = next;
        if rest.trim().is_empty() {
            return (sets, String::new());
        }
    }
}

/// Parse `data Name = Ctor1 {f: T, ..} | Ctor2 {..}` into
/// `(Name, [(Ctor, [(field, field_src)])])`. Field types are kept as source
/// substrings (e.g. `Int 1`, `Text`) so the JIT can re-parse them into real
/// `Ty`s and unify against the snippet's payload types. Returns `None` if the
/// line isn't a well-formed `data` decl.
fn parse_data_decl_ctors(
    line: &str,
) -> Option<(String, Vec<(String, Vec<(String, String)>)>)> {
    let body = line.strip_prefix("data ")?.trim();
    let (name, rhs) = body.split_once('=')?;
    let name = name.trim().to_string();
    if name.is_empty() {
        return None;
    }
    let ctors = rhs
        .split('|')
        .map(|arm| {
            let arm = arm.trim();
            // `Ctor {}` or `Ctor {f: T, g: U}` — split ctor name from the
            // brace body (space-separated, not comma, per knot's ctor syntax).
            let cname = arm.split_whitespace().next().unwrap_or("").to_string();
            let fields = match (arm.find('{'), arm.rfind('}')) {
                (Some(open), Some(close)) if close > open => {
                    let inner = &arm[open + 1..close];
                    inner
                        .split(',')
                        .filter_map(|f| {
                            let f = f.trim();
                            if f.is_empty() {
                                return None;
                            }
                            let (fname, fty) = f.split_once(':')?;
                            Some((fname.trim().to_string(), fty.trim().to_string()))
                        })
                        .collect()
                }
                _ => Vec::new(),
            };
            (cname, fields)
        })
        .collect();
    Some((name, ctors))
}

/// The polarity of a type position, for variance-aware constructor checking.
/// `Co` = the snippet produces and the host consumes (return types, covariant
/// fields); `Contra` = the host produces and the snippet consumes (function
/// parameters); `Inv` = both (an ADT appearing at mixed polarity), which
/// requires the constructor sets to be exactly equal.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variance {
    Co,
    Contra,
    Inv,
}

impl Variance {
    /// Flip across a function-parameter (contravariant) position. `Inv`
    /// absorbs: once a position is invariant, nesting can't recover variance.
    fn flip(self) -> Self {
        match self {
            Variance::Co => Variance::Contra,
            Variance::Contra => Variance::Co,
            Variance::Inv => Variance::Inv,
        }
    }
}

/// Are the snippet's ADT constructors usable where the host's are expected,
/// at the correct VARIANCE for each occurrence? The host is the consumer of
/// the snippet's result; the subset direction that keeps it sound flips with
/// polarity:
///  - Covariant (the snippet's return): snippet ctors ⊆ host ctors — the
///    host's `case` must cover everything the snippet can produce.
///  - Contravariant (a function parameter the host fills): host ctors ⊆
///    snippet ctors — the snippet must accept everything the host passes in.
///  - Invariant (mixed positions): the two ctor sets must be equal.
/// Payload field types recurse at the same variance (a `List Priority` return
/// is covariant in `Priority`); field subsumption flips direction under
/// `Contra`. ADTs only one side declares are unconstrained.
fn ctor_sets_variance_ok(
    infer: &mut Infer,
    expected: &Ty,
    host: &HashMap<String, Vec<(String, Vec<(String, String)>)>>,
    snippet_data: &HashMap<String, DataInfo>,
    span: Span,
) -> bool {
    walk_expected(infer, expected, Variance::Co, host, snippet_data, span)
}

fn walk_expected(
    infer: &mut Infer,
    ty: &Ty,
    var: Variance,
    host: &HashMap<String, Vec<(String, Vec<(String, String)>)>>,
    snippet_data: &HashMap<String, DataInfo>,
    span: Span,
) -> bool {
    // A NAMED single-variant ADT arrives as `Alias(name, Record)` (the record
    // bridge), which `peel_alias` would strip to a bare `Record` — losing the
    // name and skipping the ctor/field check entirely. Run that check on the
    // name FIRST (it compares the snippet's `DataInfo` payload field types
    // against the host's `data` decl), then peel and recurse as usual. This is
    // what makes a single-variant `data Wrap = W {n: Text}` snippet reject
    // against a host expecting `W {n: Int 1}`.
    if let Ty::Alias(name, _) = ty {
        if !check_adt_ctors(infer, name, var, host, snippet_data, span) {
            return false;
        }
    }
    match ty.peel_alias() {
        Ty::Fun(p, r) => {
            // Parameter is contravariant, result keeps the current variance.
            walk_expected(infer, p, var.flip(), host, snippet_data, span)
                && walk_expected(infer, r, var, host, snippet_data, span)
        }
        Ty::Con(name, args) => {
            // `Int`/`Float` carry a Unit arg, not an ADT — skip the ctor check
            // for them, but still recurse (harmless; their args are Units).
            let is_numeric = (name == "Int" || name == "Float") && args.len() == 1;
            let adt_ok = is_numeric
                || check_adt_ctors(infer, name, var, host, snippet_data, span);
            adt_ok
                && args
                    .iter()
                    .all(|a| walk_expected(infer, a, var, host, snippet_data, span))
        }
        Ty::Record(fields, _) | Ty::Variant(fields, _) => fields
            .values()
            .all(|f| walk_expected(infer, f, var, host, snippet_data, span)),
        Ty::Relation(inner) | Ty::IO(inner) => {
            walk_expected(infer, inner, var, host, snippet_data, span)
        }
        Ty::App(f, a) => {
            walk_expected(infer, f, var, host, snippet_data, span)
                && walk_expected(infer, a, var, host, snippet_data, span)
        }
        Ty::Forall(_, inner) => walk_expected(infer, inner, var, host, snippet_data, span),
        // Scalars, type variables, units, TyCon: no constructors to compare.
        _ => true,
    }
}

/// Compare the constructor sets of one shared ADT `name` at variance `var`,
/// and recurse into payload field types with matching direction.
fn check_adt_ctors(
    infer: &mut Infer,
    name: &str,
    var: Variance,
    host: &HashMap<String, Vec<(String, Vec<(String, String)>)>>,
    snippet_data: &HashMap<String, DataInfo>,
    span: Span,
) -> bool {
    let Some(host_ctors) = host.get(name) else {
        return true; // host didn't declare this ADT — nothing to compare
    };
    let Some(info) = snippet_data.get(name) else {
        return true; // snippet doesn't declare it either
    };
    let snippet_has = |c: &str| info.ctors.iter().any(|(cn, _)| cn == c);
    let host_has = |c: &str| host_ctors.iter().any(|(cn, _)| cn == c);
    let sets_ok = match var {
        // Covariant: snippet produces ⊆ host consumes (no snippet-only ctor).
        Variance::Co => info.ctors.iter().all(|(cn, _)| host_has(cn)),
        // Contravariant: host produces ⊆ snippet consumes (no host-only ctor
        // the snippet couldn't match on).
        Variance::Contra => host_ctors.iter().all(|(cn, _)| snippet_has(cn)),
        // Invariant: exact equality.
        Variance::Inv => {
            info.ctors.iter().all(|(cn, _)| host_has(cn))
                && host_ctors.iter().all(|(cn, _)| snippet_has(cn))
        }
    };
    if !sets_ok {
        return false;
    }
    // Payload field types, at matching variance. The CONSUMED side destructures
    // the value, so every field it binds must be PRESENT in the produced side
    // with a subsumable type; extra fields on the produced side are ignored.
    // Under Co the host consumes (host fields ⊆ snippet fields, snippet field
    // type ⊆ host field type); under Contra the snippet consumes (the mirror);
    // under Inv the field sets must be equal and each type mutually subsumable.
    info.ctors.iter().all(|(cname, snippet_fields)| {
        let Some((_, host_fields)) = host_ctors.iter().find(|(hc, _)| hc == cname) else {
            // A ctor only on the produced/consumed side — already handled by
            // the set check above; nothing to compare field-wise here.
            return true;
        };
        // Field PRESENCE: the consumed side's fields must all exist in the
        // produced side, else the consumer binds a field that isn't there
        // (e.g. host reads `n` but the snippet produced `count` — a runtime
        // "field not found" panic). Co: consumed=host; Contra: consumed=snippet;
        // Inv: both directions.
        let snippet_has = |f: &str| snippet_fields.iter().any(|(n, _)| n == f);
        let host_has = |f: &str| host_fields.iter().any(|(n, _)| n == f);
        let presence_ok = match var {
            Variance::Co => host_fields.iter().all(|(hf, _)| snippet_has(hf)),
            Variance::Contra => snippet_fields.iter().all(|(sf, _)| host_has(sf)),
            Variance::Inv => {
                host_fields.iter().all(|(hf, _)| snippet_has(hf))
                    && snippet_fields.iter().all(|(sf, _)| host_has(sf))
            }
        };
        if !presence_ok {
            return false;
        }
        snippet_fields.iter().all(|(fname, snippet_fty)| {
            let Some((_, host_fty_src)) = host_fields.iter().find(|(hf, _)| hf == fname) else {
                // Field present only on the snippet (produced) side. Sound iff
                // the host's ctor doesn't bind it — under Co the host ignores
                // it; presence (above) already guarantees the host's own fields
                // exist, so a snippet-only field is fine here.
                return true;
            };
            let snippet_ty = infer.ast_type_to_ty(snippet_fty);
            let Some(host_ast) = knot::parser::parse_type_str(host_fty_src) else {
                return true; // unparseable host field — conservative accept
            };
            let host_ty = infer.ast_type_to_ty(&host_ast);
            // Name/subsumption check of the field type, in the direction this
            // variance demands…
            let names_ok = match var {
                Variance::Co => infer.ty_subsumes(&snippet_ty, &host_ty, span),
                Variance::Contra => infer.ty_subsumes(&host_ty, &snippet_ty, span),
                // Invariant fields: require subsumption both ways.
                Variance::Inv => {
                    infer.ty_subsumes(&snippet_ty, &host_ty, span)
                        && infer.ty_subsumes(&host_ty, &snippet_ty, span)
                }
            };
            if !names_ok {
                return false;
            }
            // …and if the field is itself a host-declared ADT, recurse the
            // CONSTRUCTOR-SET check into it at the same variance. Name
            // subsumption (`Con("Priority") == Con("Priority")`) can't see that
            // the snippet's `Priority` has an extra ctor the host can't read.
            walk_expected(infer, &host_ty, var, host, snippet_data, span)
        })
    })
}

fn check_inner(program: &mut ast::Expr, expected_src: Option<&str>) -> CheckOutput {
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

    // Phase 4z: Infer the program's body so nested expressions get
    // `with_fields` entries and do-block monad resolution, and record the
    // user's top-level `with` fields. The program root is not a declaration,
    // so `infer_declarations` never visits it — without this a top-level
    // `with {x …} (…x…)` or a nested `with` inside the body compiles to
    // "undefined variable". After prelude injection the program is
    // `with {prelude} <user program>`; the user's `with` (when theirs is one)
    // is the prelude-with's body. We infer the user's BODY (not the record,
    // whose fields are already decls), and register the with's field names.
    {
        let mut cur: &ast::Expr = program;
        // Scopes/frames pushed for collapsed outer literal `with`s below; kept
        // live until the innermost body is inferred, then popped in reverse.
        let mut collapsed_withs: Vec<ast::Span> = Vec::new();
        loop {
            match &cur.node {
                ast::ExprKind::With { record, body, .. }
                    if matches!(body.node, ast::ExprKind::With { .. }) =>
                {
                    // A nested literal-record `with`: the collapse below skips
                    // the normal `With` arm, which is what pushes this `with`'s
                    // field scope and `with_scope_stack` frame. Without that the
                    // innermost body's `<>`/`^` collection sees only the
                    // INNERMOST `with`'s fields — an outer `with {outer {logCtx
                    // …}}` is invisible, so `base.info` never merges
                    // `outer.logCtx`. Push the scope + frame here (mirroring the
                    // `With` arm) so every collapsed outer `with` stays visible
                    // to the innermost body.
                    if let ast::ExprKind::Record(field_exprs) = &record.node {
                        let mut field_tys: Vec<(String, Ty)> =
                            Vec::with_capacity(field_exprs.len());
                        for f in field_exprs {
                            let val_ty = infer.infer_expr(&f.value);
                            field_tys.push((f.name.clone(), val_ty));
                        }
                        infer.with_fields.push((
                            cur.span,
                            field_tys.iter().map(|(n, _)| n.clone()).collect(),
                        ));
                        infer.push_scope();
                        for (name, ty) in &field_tys {
                            infer.bind_at(name, Scheme::mono(ty.clone()), record.span);
                        }
                        *infer.with_scope_stack.last_mut().expect("just pushed") =
                            Some((
                                cur.span,
                                field_tys
                                    .iter()
                                    .map(|(n, t)| (n.clone(), Scheme::mono(t.clone())))
                                    .collect(),
                            ));
                        collapsed_withs.push(cur.span);
                    }
                    cur = body;
                }
                ast::ExprKind::With { record, body, types } => {
                    if let ast::ExprKind::Record(fields) = &record.node {
                        // Literal-record `with`: the fields are decls already
                        // inferred by `infer_declarations`, so infer only the
                        // body (its field references resolve via the alias
                        // mechanism) and record the field names.
                        infer.with_fields.push((
                            cur.span,
                            fields.iter().map(|f| f.name.clone()).collect(),
                        ));
                        // The `With` inference arm is skipped here, so push its
                        // type-import scope (`with {Maybe …}`) around the body
                        // inference ourselves — else bare ctors never resolve.
                        let pushed = infer.push_with_ctor_imports(types, record.span);
                        let bt = infer.infer_expr(body);
                        infer.file_body_ty = Some(bt);
                        if pushed {
                            infer.with_ctor_imports.pop();
                        }
                    } else {
                        // Non-literal operand (`with base …`, `with someRecord …`):
                        // the fields are NOT decls, so there is nothing to skip
                        // — infer the whole `with` so its arm pushes the field
                        // scope and the body's bare field references (`map`,
                        // `show`) resolve. Inferring only `body` would leave
                        // them unbound in this pass.
                        let bt = infer.infer_expr(cur);
                        infer.file_body_ty = Some(bt);
                    }
                    break;
                }
                _ => {
                    let bt = infer.infer_expr(cur);
                    infer.file_body_ty = Some(bt);
                    break;
                }
            }
        }
        // Pop the scopes/frames pushed for collapsed outer `with`s above so the
        // compiler's scope stack returns to its prior depth for later phases.
        // (`pop_scope` pops both `scopes` and `with_scope_stack` together.)
        for _ in &collapsed_withs {
            infer.pop_scope();
        }
    }

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
    infer.resolve_deferred_unifies();

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

    // Phase 5c': Resolve the expected `a` of each `compile src` call, keyed by
    // the call span. The result type is `Maybe a`; the inner `a` is what the
    // caller expects the JIT-compiled snippet to be. Codegen hands it to the
    // runtime, which rejects the call (`Nothing`) unless the snippet's type
    // subsumes it. A call whose `a` stays a free variable (context never pins
    // it — e.g. under polymorphic `show`) carries NO expected type and accepts
    // whatever the snippet produces.
    let mut compile_expected_types = CompileExpectedTypes::new();
    for (span, res_v) in &infer.compile_calls {
        // `res_v` is the inner `a` of the call's `Maybe a` result (the caller's
        // expected type for the snippet), resolved now that inference is done.
        let inner = infer.apply(&Ty::Var(*res_v));
        if std::env::var("KNOT_DEBUG_CC").is_ok() {
            eprintln!("[cc] inner={:?}", inner);
        }
        // Skip unconstrained `a`: a bare type var means the caller accepts any
        // type, so there is nothing to check against.
        if matches!(inner.peel_alias(), Ty::Var(_)) {
            continue;
        }
        // Default free unit variables to dimensionless, as `local_type_info`
        // does — `Int u` (unit unconstrained) means plain `Int`, and the
        // snippet's dimensionless `Int` must match it.
        let inner = default_free_unit_vars(&inner);
        let var_map = var_map_for(&inner);
        let unit_var_map = unit_var_map_for(&inner);
        // Render for the WIRE (round-trippable): dimensionless `Int`/`Float`
        // keep their `1` unit so the JIT's `parse_type_str` re-parses them.
        compile_expected_types.insert(
            *span,
            display_ty_wire(&inner, &var_map, &unit_var_map),
        );
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

    // The program's file-body (`main`) type, surfaced for `base.compile`'s
    // runtime check: the JIT'd snippet's body type is compared against the
    // caller's expected `a`. Rendered with the same clean display as
    // `local_type_info` (free unit vars defaulted to dimensionless).
    // Option A: if an expected type was threaded in (JIT compile-snippet path),
    // parse it into a real `Ty` in THIS live inference context (so the
    // snippet's own `data` ADT names resolve) and check the body type subsumes
    // it — on real types, not strings. The verdict rides back via the static;
    // the probe leaves `infer` untouched.
    if let Some(src) = expected_src {
        // The host prepends its `data` declarations for ADTs the expected
        // type references (so ctor SETS can be compared, not just names).
        // Split them off: leading `data Name = ...` lines, then the type.
        let (host_ctor_sets, type_src) = split_host_data_decls(src);
        let verdict = match knot::parser::parse_type_str(&type_src) {
            Some(ast_ty) => {
                let expected = infer.ast_type_to_ty(&ast_ty);
                match infer.file_body_ty.clone() {
                    Some(body) => {
                        let body = infer.apply(&body);
                        // Name-based subsumption first, then a VARIANCE-AWARE
                        // constructor check over the expected type: at each ADT
                        // occurrence the snippet/host ctor-set relation must
                        // hold in the direction that position's polarity
                        // demands (return = snippet ⊆ host; parameter =
                        // host ⊆ snippet; mixed = equal).
                        let data_types = infer.data_types.clone();
                        Some(
                            infer.ty_subsumes(&body, &expected, ast_ty.span)
                                && ctor_sets_variance_ok(
                                    &mut infer,
                                    &expected,
                                    &host_ctor_sets,
                                    &data_types,
                                    ast_ty.span,
                                ),
                        )
                    }
                    None => None,
                }
            }
            None => None,
        };
        *SUBSUMPTION_VERDICT.lock().unwrap_or_else(|e| e.into_inner()) = verdict;
    }

    let file_body_type = infer.file_body_ty.as_ref().map(|ty| {
        let applied = infer.apply(ty);
        let applied = default_free_unit_vars(&applied);
        let var_map = var_map_for(&applied);
        let unit_var_map = unit_var_map_for(&applied);
        display_ty_clean(&applied, &var_map, &unit_var_map)
    });
    let elem_pushdown_ok = infer.elem_pushdown_ok.clone();
    let with_fields: WithFields = infer.with_fields.iter().cloned().collect();
    let type_arg_spans: TypeArgSpans = infer.type_arg_spans.clone();
    let implicit_refs: ImplicitRefs = infer.implicit_refs.clone();
    let implicit_dict_args: ImplicitDictArgs = infer.implicit_dict_args.clone();
    let fold_dict_args: FoldDictArgs = infer.fold_dict_args.clone();
    let collect_refs: CollectRefs = infer.collect_refs.clone();
    let todo_types = infer.extract_todo_types();
    let todo_bindings = infer.extract_todo_bindings();
    let trace_types = infer.extract_trace_types();
    let trace_bindings = infer.extract_trace_bindings();

    (infer.to_diagnostics(), monad_info, type_info, local_type_info, refine_targets, refined_type_info, from_json_targets, elem_pushdown_ok, show_unit_strings, sum_float_spans, relation_fields, with_fields, type_arg_spans, implicit_refs, implicit_dict_args, fold_dict_args, collect_refs, infer.resolved_calls.clone(), todo_types, todo_bindings, trace_types, trace_bindings, compile_expected_types, file_body_type)
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
            if !(args.is_empty() || name == "Int" || name == "Float") =>
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
        // signature vars must still constrain it. See `ResultMarker::skolems`.
        let saved_skolems = infer.skolems.clone();
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
        Lit(_) | Var(_) | Constructor(_) | SourceRef { .. } | DerivedRef(_) | ImplicitRef(_) | CollectFold(_) => {}
        TypeHole => {}
        TypeCtor { .. } | DataCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } | RouteCompositeDecl { .. } => {}
        ViewDecl { body, .. } | DerivedDecl { body, .. } => f(body),
        Record(fields) => {
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
        With { record, body, .. } => {
            f(record);
            f(body);
        }
        BinOp { lhs, rhs, .. } => {
            f(lhs);
            f(rhs);
        }
        UnaryOp { operand, .. } => f(operand),
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
        Set { target, value } | FullSet { target, value } => {
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

/// Substitute every free occurrence of `var` in `expr` with `replacement`.
/// Used by `<>`'s fold unroll to beta-reduce a lambda-literal folder at each
/// projection site, so the body's `base.unify` sees concrete argument types.
/// Naive (no alpha-renaming): safe here because the folder is a 2-param lambda
/// applied to a projection/init that references no binder shadowed inside.
pub(crate) fn subst_var(expr: &mut ast::Expr, var: &str, replacement: &ast::Expr) {
    // Replace this node if it's the variable, then recurse into children.
    if let ast::ExprKind::Var(n) = &expr.node
        && n == var
    {
        *expr = replacement.clone();
        return;
    }
    use ast::ExprKind::*;
    match &mut expr.node {
        App { func, arg } => {
            subst_var(func, var, replacement);
            subst_var(arg, var, replacement);
        }
        FieldAccess { expr: e, .. } => subst_var(e, var, replacement),
        Lambda { body, params, .. } => {
            // Do not substitute under a re-binding of `var` (shadowing).
            let rebinds = params
                .iter()
                .flat_map(pat_bound_names_pub)
                .any(|n| n == var);
            if !rebinds {
                subst_var(body, var, replacement);
            }
        }
        Record(fields) => {
            for fl in fields {
                subst_var(&mut fl.value, var, replacement);
            }
        }
        With { record, body, .. } => {
            subst_var(record, var, replacement);
            subst_var(body, var, replacement);
        }
        BinOp { lhs, rhs, .. } => {
            subst_var(lhs, var, replacement);
            subst_var(rhs, var, replacement);
        }
        UnaryOp { operand, .. } => subst_var(operand, var, replacement),
        Case { scrutinee, arms } => {
            subst_var(scrutinee, var, replacement);
            for arm in arms {
                subst_var(&mut arm.body, var, replacement);
            }
        }
        Do(stmts) => {
            for s in stmts {
                match &mut s.node {
                    ast::StmtKind::Bind { expr: e, .. } => subst_var(e, var, replacement),
                    ast::StmtKind::Where { cond } => subst_var(cond, var, replacement),
                    ast::StmtKind::GroupBy { key } => subst_var(key, var, replacement),
                    ast::StmtKind::Expr(e) => subst_var(e, var, replacement),
                    _ => {}
                }
            }
        }
        Set { target, value } | FullSet { target, value } => {
            subst_var(target, var, replacement);
            subst_var(value, var, replacement);
        }
        Atomic(inner) | Refine(inner) => subst_var(inner, var, replacement),
        TimeUnitLit { value, .. } => subst_var(value, var, replacement),
        Annot { expr: e, .. } => subst_var(e, var, replacement),
        List(items) => {
            for i in items {
                subst_var(i, var, replacement);
            }
        }
        _ => {}
    }
}

/// Names bound by a pattern (public helper for `subst_var`'s shadowing check).
fn pat_bound_names_pub(pat: &ast::Pat) -> Vec<String> {
    let mut out = Vec::new();
    collect_pat_names(pat, &mut out);
    out
}

fn collect_pat_names(pat: &ast::Pat, out: &mut Vec<String>) {
    use ast::PatKind::*;
    match &pat.node {
        Var(n) => out.push(n.clone()),
        Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => collect_pat_names(p, out),
                    None => out.push(f.name.clone()), // punned `{name}`
                }
            }
        }
        Constructor { payload, .. } => collect_pat_names(payload, out),
        List(items) => {
            for i in items {
                collect_pat_names(i, out);
            }
        }
        _ => {}
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
pub(crate) fn walk_exprs_read<'a>(e: &'a ast::Expr, f: &mut impl FnMut(&'a ast::Expr)) {
    f(e);
    use ast::ExprKind::*;
    match &e.node {
        App { func, arg } => {
            walk_exprs_read(func, f);
            walk_exprs_read(arg, f);
        }
        With { record, body, .. } => {
            walk_exprs_read(record, f);
            walk_exprs_read(body, f);
        }
        Lambda { body, .. } => walk_exprs_read(body, f),
        BinOp { lhs, rhs, .. } => {
            walk_exprs_read(lhs, f);
            walk_exprs_read(rhs, f);
        }
        UnaryOp { operand, .. } => walk_exprs_read(operand, f),
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
        Set { target, value } | FullSet { target, value } => {
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

/// Visit every `DataCtor` (`data`) marker, additionally yielding the `with`
/// nesting depth at which it is declared (0 = top level, +1 per enclosing
/// `with`). Used to detect SAME-SCOPE duplicate type declarations: two `data`
/// decls with the same name at the SAME depth are a compile error (they would
/// otherwise silently clobber each other in the global type env). Nested
/// scopes (different depths) may reuse a name — those are distinct types.
fn for_each_data_ctor_scoped<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, &'a [ast::Name], &'a [ast::ConstructorDef], Span, usize),
) {
    fn walk<'a>(
        e: &'a ast::Expr,
        depth: usize,
        f: &mut impl FnMut(&'a str, &'a [ast::Name], &'a [ast::ConstructorDef], Span, usize),
    ) {
        use ast::ExprKind::*;
        if let DataCtor { name, params, constructors } = &e.node {
            f(name, params, constructors, e.span, depth);
            return; // do not descend into the decl's own subexpressions
        }
        let d = match &e.node {
            With { .. } => depth + 1,
            _ => depth,
        };
        match &e.node {
            App { func, arg } => {
                walk(func, d, f);
                walk(arg, d, f);
            }
            With { record, body, .. } => {
                walk(record, d, f);
                walk(body, d, f);
            }
            Lambda { body, .. } => walk(body, d, f),
            BinOp { lhs, rhs, .. } => {
                walk(lhs, d, f);
                walk(rhs, d, f);
            }
            UnaryOp { operand, .. } => walk(operand, d, f),
            Case { scrutinee, arms } => {
                walk(scrutinee, d, f);
                for arm in arms {
                    walk(&arm.body, d, f);
                }
            }
            Do(stmts) => {
                for s in stmts {
                    match &s.node {
                        ast::StmtKind::Bind { expr, .. } => walk(expr, d, f),
                        ast::StmtKind::Where { cond } => walk(cond, d, f),
                        ast::StmtKind::GroupBy { key } => walk(key, d, f),
                        ast::StmtKind::Expr(x) => walk(x, d, f),
                    }
                }
            }
            Set { target, value } | FullSet { target, value } => {
                walk(target, d, f);
                walk(value, d, f);
            }
            Atomic(x) | Refine(x) => walk(x, d, f),
            TimeUnitLit { value, .. } => walk(value, d, f),
            Record(fields) => {
                for fl in fields {
                    walk(&fl.value, d, f);
                }
            }
            List(items) => {
                for it in items {
                    walk(it, d, f);
                }
            }
            FieldAccess { expr, .. } | Annot { expr, .. } => walk(expr, d, f),
            Serve { handlers, .. } => {
                for h in handlers {
                    walk(&h.body, d, f);
                }
            }
            ViewDecl { body, .. } | DerivedDecl { body, .. } => walk(body, d, f),
            _ => {}
        }
    }
    walk(program, 0, f);
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

/// Classify a resolved, non-refined base type as integer or real for the SMT
/// encoding; `None` for non-numeric bases (Text, Bool, records, ADTs), which
/// Z3's arithmetic fragment can't reason about. Unit-bearing numerics count —
/// but only when dimensionless, since `Nat M` and `Nat s` must not widen into
/// each other (unit mismatch is caught by the earlier `==` on the base `Ty`).
fn base_kind(ty: &Ty) -> Option<crate::refine_smt::BaseKind> {
    match ty {
        Ty::Float => Some(crate::refine_smt::BaseKind::Real),
        Ty::Int => Some(crate::refine_smt::BaseKind::Int),
        Ty::Con(name, _) if name == "Float" => Some(crate::refine_smt::BaseKind::Real),
        Ty::Con(name, _) if name == "Int" => Some(crate::refine_smt::BaseKind::Int),
        _ => None,
    }
}

/// Visit every named function binding: a record field whose value is a lambda
/// (or has a signature). Yields `(name, signature, body)`.
fn for_each_named_fn<'a>(
    program: &'a ast::Expr,
    f: &mut impl FnMut(&'a str, Option<&'a ast::TypeScheme>, Option<&'a ast::Expr>),
) {
    // The user's declaration record is the record of the INNERMOST `with` in
    // the `with {r1} (with {r2} (…))` chain — after prelude injection the
    // program is `with {prelude} <user program>`, so when the user's program
    // is itself a `with`, its record holds the declarations. When the user
    // program is a bare record or a non-`with` expression, that's the record
    // (or there are no decls). Nested record *values* (a record literal in a
    // field) are NOT declarations.
    fn decl_record(e: &ast::Expr) -> Option<&ast::Expr> {
        match &e.node {
            ast::ExprKind::With { record, body, .. } => {
                // The user's program is this `with`'s body. If that body is
                // itself a `with`, the user's decls live in ITS record.
                if matches!(body.node, ast::ExprKind::With { .. }) {
                    decl_record(body)
                } else {
                    Some(record)
                }
            }
            ast::ExprKind::Record(_) => Some(e),
            _ => None,
        }
    }
    if let Some(record) = decl_record(program)
        && let ast::ExprKind::Record(fields) = &record.node
    {
        for fl in fields {
            if !matches!(
                fl.value.node,
                ast::ExprKind::DataCtor { .. }
                    | ast::ExprKind::TypeCtor { .. }
                    | ast::ExprKind::SourceDecl { .. }
                    | ast::ExprKind::ViewDecl { .. }
                    | ast::ExprKind::DerivedDecl { .. }
                    | ast::ExprKind::RouteDecl { .. }
                    | ast::ExprKind::RouteCompositeDecl { .. }
                    | ast::ExprKind::SubsetConstraint { .. }
            ) {
                f(&fl.name, fl.sig.as_ref(), Some(&fl.value));
            }
        }
    }
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
        Ty::IO(_) => MonadKind::IO,
        // Partially applied type constructor, e.g. Result e (App(TyCon("Result"), e))
        Ty::App(f, _) => match f.as_ref() {
            // A bare `IO` constructor application is still the IO monad —
            // classifying it as Adt("IO") would dispatch to a nonexistent
            // `Monad_IO_bind`.
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
        ast::TypeKind::Var(_) | ast::TypeKind::Hole | ast::TypeKind::Callsite => {}
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

