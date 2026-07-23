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

// ── Convenience aliases ────────────────────────────────────────────

pub type Expr = Spanned<ExprKind>;
pub type Pat = Spanned<PatKind>;
pub type Type = Spanned<TypeKind>;
#[derive(Debug, Clone)]
pub struct Decl {
    pub node: DeclKind,
    pub span: Span,
    pub exported: bool,
}
pub type Stmt = Spanned<StmtKind>;

// ── Module ─────────────────────────────────────────────────────────

/// A complete Knot source file.
#[derive(Debug, Clone)]
pub struct Module {
    pub imports: Vec<Import>,
    pub decls: Vec<Decl>,
}

/// `import ./path` or `import ./path (A, b)`
#[derive(Debug, Clone)]
pub struct Import {
    pub path: String,
    pub items: Option<Vec<ImportItem>>,
    pub span: Span,
}

/// A single item in a selective import list.
#[derive(Debug, Clone)]
pub struct ImportItem {
    pub name: Name,
    pub span: Span,
}

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

// ── Declarations ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum DeclKind {
    /// `data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}`
    Data {
        name: Name,
        params: Vec<Name>,
        constructors: Vec<ConstructorDef>,
        deriving: Vec<Name>,
    },

    /// `type Person = {name: Text, age: Int}`
    TypeAlias {
        name: Name,
        params: Vec<Name>,
        ty: Type,
    },

    /// `*people : [Person]` — persisted, mutable, no body.
    Source {
        name: Name,
        ty: Type,
    },

    /// `*openTodos = expr` — query over sources, settable.
    View {
        name: Name,
        ty: Option<TypeScheme>,
        body: Expr,
    },

    /// `&seniors = expr` — computed from sources, read-only.
    Derived {
        name: Name,
        ty: Option<TypeScheme>,
        body: Expr,
    },

    /// `add = \x y -> x + y` — constant binding (functions are lambdas).
    /// `body` is `None` for signature-only declarations (e.g. `f : Int -> Int`).
    Fun {
        name: Name,
        ty: Option<TypeScheme>,
        body: Option<Expr>,
    },

    /// `route Api where ...`
    Route {
        name: Name,
        entries: Vec<RouteEntry>,
    },

    /// `route Api = TodoApi | AdminApi`
    RouteComposite {
        name: Name,
        components: Vec<Name>,
    },

    /// `migrate *rel from T1 to T2 using f`
    Migrate {
        relation: Name,
        from_ty: Type,
        to_ty: Type,
        using_fn: Expr,
    },

    /// `*orders.customer <= *people.name`
    SubsetConstraint {
        sub: RelationPath,
        sup: RelationPath,
    },
}

// ── Expressions ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ExprKind {
    /// `42`, `3.14`, `"hello"`
    Lit(Literal),

    /// `x`, `formatTitle` — lowercase identifier.
    Var(Name),

    /// `Circle`, `Open` — PascalCase constructor reference.
    Constructor(Name),

    /// `*people` — reference to a source relation.
    SourceRef(Name),

    /// `&seniors` — reference to a derived relation.
    DerivedRef(Name),

    /// `^name` — implicit field projection: DFS the fields of in-scope record
    /// bindings (innermost scope first, shallowest, earliest field) for a
    /// field named `name` whose type unifies with the expected type.
    ImplicitRef(Name),

    /// `{name: "Alice", age: 30}`
    Record(Vec<RecordField>),

    /// `{t | age: t.age + 1}`
    RecordUpdate {
        base: Box<Expr>,
        fields: Vec<Field<Expr>>,
    },

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
    With { record: Box<Expr>, body: Box<Expr> },

    /// `a + b`, `x == y`, `xs |> filter f`
    BinOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },

    /// `-x`, `not cond`
    UnaryOp { op: UnaryOp, operand: Box<Expr> },

    /// `if cond then a else b`
    If {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },

    /// `case expr of { Pat -> Expr, ... }`
    Case {
        scrutinee: Box<Expr>,
        arms: Vec<CaseArm>,
    },

    /// `do { stmts }`
    Do(Vec<Stmt>),

    /// `*rel = expr` — update a source relation (must match an optimized pattern).
    Set { target: Box<Expr>, value: Box<Expr> },

    /// `replace *rel = expr` — full table replacement (DELETE + INSERT).
    ReplaceSet { target: Box<Expr>, value: Box<Expr> },

    /// `atomic expr` — transactional boundary.
    Atomic(Box<Expr>),

    /// `2 seconds`, `5 ms` — time-unit sugar. `value` holds the desugared
    /// form (a `BinOp::Mul` of the literal and its millisecond factor, so
    /// inference/codegen treat it identically to that multiplication);
    /// `unit_name` preserves the original unit word so the formatter can
    /// re-render the surface syntax instead of the raw multiplication.
    TimeUnitLit {
        value: Box<Expr>,
        unit_name: Name,
    },

    /// `(expr : Type)` — type annotation on expression.
    Annot {
        expr: Box<Expr>,
        ty: Type,
    },

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

    /// A first-class, erased `data` declaration embedded in a record value
    /// literal (`{data Status = Open {} | Done {}, …}`). The record field
    /// `Status` is fully ERASED at runtime (compiles to unit, like `TypeCtor`),
    /// but its constructors are reachable in value scope as `rec.Status.Open`,
    /// `rec.Status.Done`, … and the data type name enters type scope so it can
    /// be referenced in annotations (`x : Status`).
    DataCtor {
        name: Name,
        params: Vec<Name>,
        constructors: Vec<ConstructorDef>,
    },

    /// A persisted source-relation declaration embedded in a record value
    /// literal (`{*todos : [Todo], …}`). The record field is literally named
    /// `*todos` (the `*` is part of the field NAME, not a prefix operator).
    /// Reading `db.*todos` yields the relation value `[Todo]`; writing
    /// `db.*todos = …` is a source write. The source's qualified identity is
    /// `<record>.<field>` (e.g. `db.*todos`), used for the schema lockfile,
    /// migrations, effects, and the physical table name. Like `DataCtor`, the
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

    /// `*openTodos = expr` (or `*openTodos : Type = expr`) inside a record
    /// literal — an embedded view declaration. Mirrors the top-level
    /// `DeclKind::View`; the field is literally named `*name` and its value is
    /// a marker (the view is registered statically and resolved by path).
    ViewDecl {
        /// Field name WITHOUT the leading `*` (e.g. `openTodos`).
        name: Name,
        ty: Option<TypeScheme>,
        body: Box<Expr>,
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
                && name == "yield" {
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
    Bool(bool),
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
    Constructor { name: Name, payload: Box<Pat> },

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

    /// `{name: Text, age: Int}` or `{name: Text | r}` — record type.
    Record {
        fields: Vec<Field<Type>>,
        rest: Option<Name>,
    },

    /// `[T]` — relation (set) type.
    Relation(Box<Type>),

    /// `a -> b` — function type.
    Function {
        param: Box<Type>,
        result: Box<Type>,
    },

    /// `<Open {} | InProgress {assignee: Text}>` — inline variant type.
    Variant {
        constructors: Vec<ConstructorDef>,
        rest: Option<Name>,
    },

    /// `{rw *people} Text -> {}` — effectful type.
    Effectful {
        effects: Vec<Effect>,
        ty: Box<Type>,
    },

    /// `IO {effects} a` or `IO {effects | r} a` — IO monad type with effect set.
    /// `rest` is the row-variable tail. Empty Vec = closed row. One element =
    /// single row variable. Multiple elements = `r1 \/ r2 \/ ...` row-union —
    /// the tail row is the union of each named row variable's effects.
    IO {
        effects: Vec<Effect>,
        rest: Vec<Name>,
        ty: Box<Type>,
    },

    /// `_` — type hole, inferred by the type checker.
    Hole,

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
    UnitAnnotated {
        base: Box<Type>,
        unit: UnitExpr,
    },

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
    /// `Display a`, `Num n` — a trait constraint (legacy; traits removed).
    Trait { trait_name: Name, args: Vec<Type> },
    /// `(^field : Type)` — an implicit-field constraint. The function takes a
    /// hidden dictionary argument; callsites resolve it by searching scope for
    /// a record providing `field` at `Type`.
    ImplicitField { field: Name, ty: Type },
}

impl Constraint {
    /// The constraint's display name (trait name or `^field`).
    pub fn name(&self) -> &str {
        match self {
            Constraint::Trait { trait_name, .. } => trait_name,
            Constraint::ImplicitField { field, .. } => field,
        }
    }

    /// All types mentioned by the constraint (trait args, or the field type).
    pub fn types(&self) -> Vec<&Type> {
        match self {
            Constraint::Trait { args, .. } => args.iter().collect(),
            Constraint::ImplicitField { ty, .. } => vec![ty],
        }
    }
}

// ── Effects ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Effect {
    Reads(Name),
    Writes(Name),
    Console,
    Network,
    Fs,
    Clock,
    Random,
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
/// `name : Type` sig line:
///   {name : Text
///    name "a"}
/// The sig (when present) is enforced against the value's type.
#[derive(Debug, Clone)]
pub struct RecordField {
    pub name: Name,
    pub value: Expr,
    pub sig: Option<Type>,
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
