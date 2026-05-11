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

    /// `unit m` or `unit N = kg * m / s^2`
    UnitDecl {
        name: Name,
        definition: Option<UnitExpr>,
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

    /// `*rel = expr` — update a source relation (must match an optimized pattern).
    Set { target: Box<Expr>, value: Box<Expr> },

    /// `replace *rel = expr` — full table replacement (DELETE + INSERT).
    ReplaceSet { target: Box<Expr>, value: Box<Expr> },

    /// `atomic expr` — transactional boundary.
    Atomic(Box<Expr>),

    /// `42.0<m>`, `999<usd>` — numeric literal with unit annotation.
    UnitLit {
        value: Box<Expr>,
        unit: UnitExpr,
    },

    /// `(expr : Type)` — type annotation on expression.
    Annot {
        expr: Box<Expr>,
        ty: Type,
    },

    /// `refine expr` — runtime refinement check, returns Result.
    Refine(Box<Expr>),

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
        if let ExprKind::App { func, arg } = self {
            if let ExprKind::Var(name) = &func.node {
                if name == "yield" {
                    return Some(arg);
                }
            }
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

    /// `Float<m>`, `Int<usd>`, `Float<m / s^2>` — numeric type with unit.
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
        /// Span of the method name token; used by editor tooling to point
        /// document symbols and go-to-definition at the method itself
        /// rather than the enclosing trait.
        name_span: Span,
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
        /// Span of the method name token; used by editor tooling.
        name_span: Span,
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
