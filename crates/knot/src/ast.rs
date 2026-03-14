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
pub type Decl = Spanned<DeclKind>;
pub type Stmt = Spanned<StmtKind>;

// ── Module ─────────────────────────────────────────────────────────

/// A complete Knot source file.
#[derive(Debug, Clone)]
pub struct Module {
    pub name: Option<Name>,
    pub decls: Vec<Decl>,
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
        history: bool,
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
    Fun {
        name: Name,
        ty: Option<TypeScheme>,
        body: Expr,
    },

    /// `trait Functor (f : Type -> Type) where ...`
    Trait {
        name: Name,
        params: Vec<TraitParam>,
        supertraits: Vec<Constraint>,
        items: Vec<TraitItem>,
    },

    /// `impl Functor [] where ...`
    Impl {
        trait_name: Name,
        args: Vec<Type>,
        constraints: Vec<Constraint>,
        items: Vec<ImplItem>,
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

    /// `{name: "Alice", age: 30}`
    Record(Vec<Field<Expr>>),

    /// `{t | age: t.age + 1}`
    RecordUpdate {
        base: Box<Expr>,
        fields: Vec<Field<Expr>>,
    },

    /// `t.name`
    FieldAccess { expr: Box<Expr>, field: Name },

    /// `["Alice", "Bob"]` or `[]`
    List(Vec<Expr>),

    /// `\x -> expr` or `\x y -> expr`
    Lambda {
        params: Vec<Pat>,
        body: Box<Expr>,
    },

    /// `f x` — function application.
    App { func: Box<Expr>, arg: Box<Expr> },

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

    /// `yield expr` — `Applicative.yield`.
    Yield(Box<Expr>),

    /// `set *rel = expr` — update a source relation (must match an optimized pattern).
    Set { target: Box<Expr>, value: Box<Expr> },

    /// `full set *rel = expr` — full table replacement (DELETE + INSERT).
    FullSet { target: Box<Expr>, value: Box<Expr> },

    /// `atomic expr` — transactional boundary.
    Atomic(Box<Expr>),

    /// `*employees @(now - 365 days)` — temporal query.
    At {
        relation: Box<Expr>,
        time: Box<Expr>,
    },
}

// ── Literals ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Literal {
    Int(i64),
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
}

/// A field in a record pattern.
#[derive(Debug, Clone)]
pub struct FieldPat {
    pub name: Name,
    /// `None` means punned: `{name}` is shorthand for `{name: name}`.
    pub pattern: Option<Pat>,
}

// ── Do-block statements ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum StmtKind {
    /// `pat <- expr` — monadic bind.
    Bind { pat: Pat, expr: Expr },

    /// `let pat = expr` — local binding.
    Let { pat: Pat, expr: Expr },

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

    /// `{reads *people, writes *people} Text -> {}` — effectful type.
    Effectful {
        effects: Vec<Effect>,
        ty: Box<Type>,
    },

    /// `_` — type hole, inferred by the type checker.
    Hole,
}

/// A type with optional trait constraints: `Display a => [a] -> [Text]`.
#[derive(Debug, Clone)]
pub struct TypeScheme {
    pub constraints: Vec<Constraint>,
    pub ty: Type,
}

/// A trait constraint: `Display a`, `Num n`.
#[derive(Debug, Clone)]
pub struct Constraint {
    pub trait_name: Name,
    pub args: Vec<Type>,
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

/// A constructor in a `data` declaration: `Circle {radius: Float}`.
#[derive(Debug, Clone)]
pub struct ConstructorDef {
    pub name: Name,
    pub fields: Vec<Field<Type>>,
}

// ── Traits ─────────────────────────────────────────────────────────

/// A type parameter in a trait declaration, with optional kind annotation.
#[derive(Debug, Clone)]
pub struct TraitParam {
    pub name: Name,
    /// e.g. `Type -> Type` for higher-kinded params. `None` = inferred.
    pub kind: Option<Type>,
}

/// An item inside a `trait` block.
#[derive(Debug, Clone)]
pub enum TraitItem {
    /// A method signature with optional default body.
    Method {
        name: Name,
        ty: TypeScheme,
        default_params: Vec<Pat>,
        default_body: Option<Expr>,
    },
    /// `type Item c` — associated type declaration.
    AssociatedType { name: Name, params: Vec<Name> },
}

/// An item inside an `impl` block.
#[derive(Debug, Clone)]
pub enum ImplItem {
    /// A method implementation.
    Method {
        name: Name,
        params: Vec<Pat>,
        body: Expr,
    },
    /// `type Item [a] = a` — associated type definition.
    AssociatedType {
        name: Name,
        args: Vec<Type>,
        ty: Type,
    },
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
    pub response_ty: Option<Type>,
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
