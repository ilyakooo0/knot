//! Abstract Syntax Tree for the Knot language.

// ── Spans ──────────────────────────────────────────────────────────

/// Byte-offset span in source code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// A value annotated with its source location.
#[derive(Debug, Clone)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl<T> Spanned<T> {
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }
}

// ── Names ──────────────────────────────────────────────────────────

/// An identifier. Could be interned later for performance.
pub type Name = String;

/// A compiler-generated name for an implicit-dictionary alias. Distinct from
/// every user-written identifier by construction (a user can't name a `with`
/// span or this variant); the span makes it unique per `with` site. Only
/// produced by inference — never parsed from or formatted to source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum InternalName {
    /// Per-`with`-site, per-`field` alias.
    WithField { span_start: usize, field: String },
    /// Per-`with`-site alias for the whole record value.
    WithRecord { span_start: usize },
}

/// A variable reference: either a user-written name or a compiler-generated
/// internal alias. Keeping them as one enum means a `Var` never needs to
/// mangle an internal alias into the user-name string space.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Binding {
    /// A user-written variable name.
    User(Name),
    /// A compiler-generated internal alias.
    Internal(InternalName),
}

impl Binding {
    /// The user name, if this is a user binding.
    pub fn as_user(&self) -> Option<&str> {
        match self {
            Binding::User(name) => Some(name),
            Binding::Internal(_) => None,
        }
    }

    /// True if this is the user binding with the given name.
    pub fn is_user(&self, name: &str) -> bool {
        matches!(self, Binding::User(n) if n == name)
    }

    /// The name as a string slice (the `Display` form). User names pass
    /// through; internal aliases use their span-qualified form. Use this where
    /// a `Var` name is consumed as a string (error messages, map keys).
    pub fn as_str(&self) -> &str {
        match self {
            Binding::User(name) => name.as_str(),
            // Internal aliases never reach string consumers in a well-formed
            // pipeline (they're looked up as `Binding`), but if one does, the
            // span-qualified form is a safe, unique fallback.
            Binding::Internal(_) => "",
        }
    }
}

impl std::fmt::Display for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Binding::User(name) => f.write_str(name),
            Binding::Internal(InternalName::WithField { span_start, field }) => {
                write!(f, "with{span_start}@{field}")
            }
            Binding::Internal(InternalName::WithRecord { span_start }) => {
                write!(f, "withrec{span_start}")
            }
        }
    }
}

// A `Binding` compares equal to a string only for the `User` variant — an
// internal alias never equals any user name. This lets existing
// `name == "lit"` / `map.get(name)`-style checks keep working where `name`
// is now a `Binding`.
impl PartialEq<str> for Binding {
    fn eq(&self, other: &str) -> bool {
        matches!(self, Binding::User(n) if n == other)
    }
}
impl PartialEq<&str> for Binding {
    fn eq(&self, other: &&str) -> bool {
        matches!(self, Binding::User(n) if n == other)
    }
}
impl PartialEq<Binding> for str {
    fn eq(&self, other: &Binding) -> bool {
        other == self
    }
}
impl PartialEq<Binding> for &str {
    fn eq(&self, other: &Binding) -> bool {
        other == self
    }
}
impl PartialEq<String> for Binding {
    fn eq(&self, other: &String) -> bool {
        matches!(self, Binding::User(n) if n == other)
    }
}
impl PartialEq<Binding> for String {
    fn eq(&self, other: &Binding) -> bool {
        other == self
    }
}

// ── Convenience aliases ────────────────────────────────────────────

pub type Expr = Spanned<ExprKind>;
pub type Pat = Spanned<PatKind>;
pub type Type = Spanned<TypeKind>;
pub type Stmt = Spanned<StmtKind>;

// ── Units of Measure ──────────────────────────────────────────────

/// A unit-of-measure expression (compile-time only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitExpr {
    /// Dimensionless: `1`
    Dimensionless,
    /// A named unit or unit variable: `m`, `s`, `u`
    Named(Name),
    /// Product: `u1 * u2`
    Mul(Box<UnitExpr>, Box<UnitExpr>),
    /// Quotient: `u1 / u2`
    Div(Box<UnitExpr>, Box<UnitExpr>),
    /// Power: `u ^ n` (integer exponent)
    Pow(Box<UnitExpr>, i32),
    /// `_` — unit hole: a fresh unit variable, bound by unification (like a
    /// lowercase unit variable, but each occurrence is independent).
    Hole,
}

// ── Expressions ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ExprKind {
    /// `42`, `3.14`, `"hello"`
    Lit(Literal),

    /// `x`, `formatTitle` — lowercase identifier.
    Var(Binding),

    /// `Circle`, `Open` — PascalCase constructor reference.
    Constructor(Name),

    /// `*people` — reference to a source relation.
    SourceRef { name: Name },

    /// `^name` — implicit field projection: DFS the fields of in-scope record
    /// bindings (innermost scope first, shallowest, earliest field) for a
    /// field named `name` whose type unifies with the expected type.
    ImplicitRef(Name),

    /// `<>name` — collecting fold head. When applied as `<>name folder init`,
    /// folds every in-scope record field named `name` (all enclosing
    /// `with`-scopes, innermost-first, type-filtered to the fold's element
    /// type) through `folder`, starting from `init`. The compiler unrolls the
    /// fold per candidate, so heterogeneous fragment shapes are fine.
    CollectFold(Name),

    /// `_` in expression position — a HOLE. In a type-argument slot (consumed
    /// by `consume_type_arg`) it is an inferrable type hole (`TypeKind::Hole`
    /// → a fresh unification variable). In any other (value) position it
    /// behaves like `base.todo`: a polymorphic `∀a. a` placeholder that warns
    /// at compile time and errors at runtime with the expected type + scope.
    TypeHole,

    /// `{name: "Alice", age: 30}`
    Record(Vec<RecordField>),

    /// `t.name`
    FieldAccess { expr: Box<Expr>, field: Name },

    /// `["Alice", "Bob"]` or `[]`
    List(Vec<Expr>),

    /// `\x -> expr` or `\x y -> expr` — or, with leading type-witness params,
    /// `\(T : Type) -> \x -> expr` (Π-lite explicit type arguments).
    Lambda {
        params: Vec<Pat>,
        /// Leading type-witness parameters `\(T : Type)`. Each is an erased
        /// type argument: at runtime it has no representation, and at a call
        /// site the corresponding argument is a *type* (disambiguated by the
        /// parameter's `Type` kind), not a value. Empty for ordinary lambdas.
        ty_params: Vec<TyParam>,
        body: Box<Expr>,
    },

    /// `f x` — function application.
    App { func: Box<Expr>, arg: Box<Expr> },

    /// `with record body` — every field of `record` (which must have a known
    /// record type) is in scope as a variable inside `body`. The result is
    /// `body`. Generalizes `let … in` for record-shaped bindings.
    ///
    /// `types` lists the data types named as uppercase "fields" in the `with`
    /// record (`with {Maybe} body`). Their constructors are in scope
    /// UNQUALIFIED inside `body` (`Just {value v}` instead of `Maybe.Just
    /// {value v}`). Empty for an ordinary value-only `with`.
    With {
        record: Box<Expr>,
        body: Box<Expr>,
        types: Vec<String>,
    },

    /// `a + b`, `x == y`, `xs |> filter f`
    BinOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },

    /// `-x`, `not cond`
    UnaryOp { op: UnaryOp, operand: Box<Expr> },

    /// `case expr of { Pat -> Expr, ... }`
    Case {
        scrutinee: Box<Expr>,
        arms: Vec<CaseArm>,
    },

    /// `do { stmts }`
    Do(Vec<Stmt>),

    /// `*rel = expr` — update a source relation (must match an optimized pattern).
    Set { target: Box<Expr>, value: Box<Expr> },

    /// `full *rel = expr` — full table replacement (DELETE + INSERT).
    FullSet { target: Box<Expr>, value: Box<Expr> },

    /// `atomic expr` — transactional boundary.
    Atomic(Box<Expr>),

    /// `2 seconds`, `5 ms` — time-unit sugar. `value` holds the desugared
    /// form (a `BinOp::Mul` of the literal and its millisecond factor, so
    /// inference/codegen treat it identically to that multiplication);
    /// `unit_name` preserves the original unit word so the formatter can
    /// re-render the surface syntax instead of the raw multiplication.
    TimeUnitLit { value: Box<Expr>, unit_name: Name },

    /// `the (Type) expr` — type annotation on expression.
    Annot { expr: Box<Expr>, ty: Type },

    /// `refine expr` — runtime refinement check, returns Result.
    Refine(Box<Expr>),

    /// A first-class (erased) TYPE CONSTRUCTOR value, produced by a `type`
    /// alias declaration inside a record value literal:
    ///   {type Pair a b = {fst: a, snd: b}  Pair  ...}
    /// The field named `name` has this as its value. Statically its type is
    /// the alias's kind (`Type`, `Type -> Type`, …, one `Type ->` per param,
    /// ending in `Type`); the alias `name` is brought into type scope so it can
    /// be applied in annotations (`x : Pair Int Text`). Fully ERASED at runtime
    /// (compiles to unit) — there is no reified type value.
    TypeCtor {
        name: Name,
        params: Vec<Name>,
        ty: Type,
    },

    /// A persisted source-relation declaration embedded in a record value
    /// literal (`{*todos : [Todo], …}`). The record field is literally named
    /// `*todos` (the `*` is part of the field NAME, not a prefix operator).
    /// Reading `db.*todos` yields the relation value `[Todo]`; writing
    /// `db.*todos = …` is a source write. The source's qualified identity is
    /// `<record>.<field>` (e.g. `db.*todos`), used for the schema lockfile,
    /// migrations, effects, and the physical table name. Like `TypeCtor`, the
    /// record field itself is a marker — the source is registered statically
    /// and resolved by path, not carried as a runtime value.
    SourceDecl {
        /// Field name WITHOUT the leading `*` (e.g. `todos`).
        name: Name,
        ty: Type,
        /// Migrations attached to the source:
        /// `*todos : [Todo] migrate from A to B using f migrate from B to C using g`.
        /// Mirrors top-level `migrate` decls (cumulative — all historical
        /// migrations are kept) but hangs off the source field itself.
        migrations: Vec<SourceMigration>,
    },

    /// A subset constraint embedded in a record value literal:
    /// `{…, *orders.customer <= *people.name, …}` (or the whole-relation form
    /// `*a <= *b`). Mirrors a top-level subset constraint; the
    /// field is a pure marker — it contributes no runtime value, the
    /// constraint is registered statically alongside top-level ones.
    SubsetConstraint {
        sub: RelationPath,
        sup: RelationPath,
    },

    /// A `route Name where …` declaration embedded in a record value literal
    /// (`{route Api where …, …}`). Mirrors a top-level route declaration.
    /// The field is a pure marker — it contributes no runtime value (erased
    /// like `TypeCtor`); the route's entries and endpoint constructors are
    /// registered statically under the record path (`rec.Api`) and resolved
    /// by path at `serve rec.Api` / `fetch url (rec.Api.Ctor …)` call sites.
    RouteDecl {
        /// Route name (e.g. `Api`), also the record field name.
        name: Name,
        entries: Vec<RouteEntry>,
    },

    /// `serve Api where E1 = expr1; E2 = expr2; ...` — typed server value.
    /// Each handler is bound to a route endpoint constructor; the whole
    /// expression has type `Server Api _` (a row variable when no handler
    /// has concrete effects) or `Server Api {effects}` when handlers carry
    /// concrete IO effects.
    Serve {
        api: Name,
        api_span: Span,
        handlers: Vec<ServeHandler>,
    },
}

/// A single endpoint binding inside a `serve` expression.
#[derive(Debug, Clone)]
pub struct ServeHandler {
    pub endpoint: Name,
    pub endpoint_span: Span,
    pub body: Expr,
}

impl ExprKind {
    /// If this is `yield arg` (i.e. `App(Var("yield"), arg)`), return the argument.
    pub fn as_yield_arg(&self) -> Option<&Expr> {
        if let ExprKind::App { func, arg } = self
            && let ExprKind::Var(name) = &func.node
            && name.is_user("yield")
        {
            return Some(arg);
        }
        None
    }
}

// ── Literals ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Literal {
    Int(String),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

// ── Operators ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,    // +
    Sub,    // -
    Mul,    // *
    Div,    // /
    Mod,    // %
    Eq,     // ==
    Neq,    // !=
    Lt,     // <
    Gt,     // >
    Le,     // <=
    Ge,     // >=
    And,    // &&
    Or,     // ||
    Concat, // ++
    Pipe,   // |>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg, // - (numeric negation)
    Not, // not
}

// ── Patterns ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PatKind {
    /// `x` — bind a variable.
    Var(Name),

    /// `_` — match anything.
    Wildcard,

    /// `Circle {radius}`, `Open {}`, `Circle c` — constructor + payload.
    /// `qualifier` is the data-type path in a qualified pattern `Color.Red`
    /// (`Some("Color")`); `name` is always the bare constructor tag (`Red`).
    Constructor {
        name: Name,
        payload: Box<Pat>,
        qualifier: Option<Name>,
    },

    /// `{name: n, age}` — record destructure.
    Record(Vec<FieldPat>),

    /// `42`, `"hello"` — literal value.
    Lit(Literal),

    /// `[]`, `[{name: n}]` — relation/list pattern.
    List(Vec<Pat>),

    /// `Cons head tail` — non-empty relation pattern. `head` binds the
    /// first element, `tail` binds the remainder as a relation.
    Cons { head: Box<Pat>, tail: Box<Pat> },

    /// `(x : T)` — a type-annotated pattern. Binds `x` at the annotated type.
    /// On a lambda param this enables rank-N: `\(f : (forall a. a -> a)) -> …`
    /// gives `f` a polymorphic type inside the body.
    Annot { pat: Box<Pat>, ty: Box<Type> },
}

/// A field in a record pattern.
#[derive(Debug, Clone)]
pub struct FieldPat {
    pub name: Name,
    /// Span of the field-name token. For a punned field (`pattern: None`) this
    /// is also the binder's span — tooling (hover/inference) relies on it to
    /// give each punned binder its own span rather than sharing the whole
    /// record pattern's span.
    pub name_span: Span,
    /// `None` means punned: `{name}` is shorthand for `{name: name}`.
    pub pattern: Option<Pat>,
}

/// A type-witness parameter in a lambda: `\(T : Type)`. The witness is erased
/// at runtime (no value representation); its only role is to let the caller
/// pass a *type* explicitly that later parameters/annotations reference.
#[derive(Debug, Clone)]
pub struct TyParam {
    pub name: Name,
    pub span: Span,
}

// ── Do-block statements ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum StmtKind {
    /// `pat <- expr` — monadic bind.
    Bind { pat: Pat, expr: Expr },

    /// `where cond` — guard / filter (requires `Alternative`).
    Where { cond: Expr },

    /// `groupBy expr` — group rows by key expression.
    GroupBy { key: Expr },

    /// Bare expression (including `yield expr`).
    Expr(Expr),
}

// ── Case arms ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CaseArm {
    pub pat: Pat,
    pub body: Expr,
}

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum TypeKind {
    /// `Int`, `Text`, `Person` — named type.
    Named(Name),

    /// `a`, `b` — type variable.
    Var(Name),

    /// `Maybe a`, `Result e a` — type application.
    App { func: Box<Type>, arg: Box<Type> },

    /// `{name Text age Int}` or `{name Text | r}` — record type.
    Record {
        fields: Vec<Field<Type>>,
        rest: Option<Name>,
    },

    /// `[T]` — relation (set) type.
    Relation(Box<Type>),

    /// `a -> b` — function type.
    Function { param: Box<Type>, result: Box<Type> },

    /// `<Open {} | InProgress {assignee: Text}>` — inline variant type.
    Variant {
        constructors: Vec<ConstructorDef>,
        rest: Option<Name>,
    },

    /// `IO a` — IO monad type. Effects are untracked.
    IO { ty: Box<Type> },

    /// `_` — type hole, inferred by the type checker.
    Hole,

    /// `?` — callsite-derived type. Concretely known at each CALLSITE, not at
    /// the definition: used in a `(<>field : ?) =>` fold-constraint, where the
    /// collected fragments' merged type grounds it per call. Maps to an open
    /// row variable the body constrains via field access and each callsite
    /// grounds against its merged record type.
    Callsite,

    /// `Float M`, `Float (M / S^2)`, `Float u` — a type-level unit
    /// expression, appearing as the argument of a type application to
    /// `Int`/`Float`. Carries the compile-time unit algebra (`*`, `/`, `^`).
    /// A bare `Named(n)` unit is a concrete unit (`M`) when `n` is uppercase
    /// or a unit variable (`u`) when lowercase.
    Unit(UnitExpr),

    /// `Float M` / `Int Usd` / `Float (M / S^2)` — numeric type with unit.
    /// Kept as a dedicated node (rather than desugared to `App(Named "Float",
    /// Unit u)`) so inference can recognise the shape without peeling
    /// application spines. The `base` is `Named "Int"`/`Named "Float"`.
    UnitAnnotated { base: Box<Type>, unit: UnitExpr },

    /// `T where \x -> predicate` — refined type.
    Refined {
        base: Box<Type>,
        predicate: Box<Expr>,
    },

    /// `forall a b. T` — explicit higher-rank quantifier.
    Forall { vars: Vec<Name>, ty: Box<Type> },
}

/// A type with optional trait constraints: `Display a => [a] -> [Text]`.
#[derive(Debug, Clone)]
pub struct TypeScheme {
    pub constraints: Vec<Constraint>,
    pub ty: Type,
}

/// A signature constraint: either a trait constraint (`Display a`) or an
/// implicit-field constraint (`^compare : a -> a -> Ordering`).
#[derive(Debug, Clone)]
pub enum Constraint {
    /// `Display a`, `Num n` — a trait constraint.
    Trait { trait_name: Name, args: Vec<Type> },
    /// `(^field : Type)` — an implicit-field constraint. The function takes a
    /// hidden dictionary argument; callsites resolve it by searching scope for
    /// a record providing `field` at `Type`.
    ImplicitField { field: Name, ty: Type },
    /// `(<>field)` — an implicit-field FOLD constraint. Name-only: asserts the
    /// callsite must SUPPLY a `field` value, produced by an explicit `<>` fold
    /// (`<>field folder init`) passed as the dict argument. No type annotation:
    /// the dict's type is inferred at the CALLSITE from the fold, not declared
    /// here. `ty` is currently always `None` (the dict type is derived at the
    /// callsite). Used by `base.log` to merge all `logCtx` scopes.
    CollectField { field: Name, ty: Option<Type> },
}

impl Constraint {
    /// The constraint's display name (trait name or `^field`).
    pub fn name(&self) -> &str {
        match self {
            Constraint::Trait { trait_name, .. } => trait_name,
            Constraint::ImplicitField { field, .. } => field,
            Constraint::CollectField { field, .. } => field,
        }
    }

    /// All types mentioned by the constraint (trait args, or the field type).
    pub fn types(&self) -> Vec<&Type> {
        match self {
            Constraint::Trait { args, .. } => args.iter().collect(),
            Constraint::ImplicitField { ty, .. } => vec![ty],
            Constraint::CollectField { ty, .. } => ty.iter().collect(),
        }
    }
}

// ── Shared structures ──────────────────────────────────────────────

/// A field in a record expression or record type.
#[derive(Debug, Clone)]
pub struct Field<T> {
    pub name: Name,
    pub value: T,
}

/// A field in a record VALUE literal `{name value, ...}` — like `Field<Expr>`
/// but may carry an optional standalone type signature from a preceding
/// type-first `Type  name` sig line:
///   {Text  name
///    name "a"}
/// The sig (when present) is enforced against the value's type. It is a full
/// type scheme so a field function can take implicit-field constraints:
/// `{(Text  ^name) => {} -> Text  greet  greet \_ -> ^name}`.
#[derive(Debug, Clone)]
pub struct RecordField {
    pub name: Name,
    pub value: Expr,
    pub sig: Option<TypeScheme>,
    /// Markdown documentation attached via `---` doc comments immediately
    /// preceding this field. `None` when the field has no doc.
    pub doc: Option<String>,
}

/// A migration attached to a record-embedded source field:
/// `*todos : [Todo] migrate from Old to New using f`.
#[derive(Debug, Clone)]
pub struct SourceMigration {
    pub from_ty: Type,
    pub to_ty: Type,
    pub using_fn: Expr,
}

/// A constructor in a `data` declaration: `Circle {radius: Float}`.
#[derive(Debug, Clone)]
pub struct ConstructorDef {
    pub name: Name,
    pub fields: Vec<Field<Type>>,
}

// ── Routes ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

/// A single route entry (parser flattens any path-prefix nesting).
#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub method: HttpMethod,
    pub path: Vec<PathSegment>,
    pub body_fields: Vec<Field<Type>>,
    pub query_params: Vec<Field<Type>>,
    pub request_headers: Vec<Field<Type>>,
    pub response_ty: Option<Type>,
    pub response_headers: Vec<Field<Type>>,
    pub rate_limit: Option<Expr>,
    pub constructor: Name,
}

/// A segment of a route path.
#[derive(Debug, Clone)]
pub enum PathSegment {
    /// `/todos` — literal segment.
    Literal(String),
    /// `/{id: Int}` — typed path parameter.
    Param { name: Name, ty: Type },
}

// ── Subset constraints ─────────────────────────────────────────────

/// A path like `*orders.customer` or just `*users` (for uniqueness).
#[derive(Debug, Clone)]
pub struct RelationPath {
    pub relation: Name,
    pub field: Option<Name>,
}
