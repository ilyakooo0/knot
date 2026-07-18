//! Cranelift-based code generator for the Knot language.
//!
//! Compiles a Knot AST into a native object file. All Knot values are
//! represented at the machine level as pointers to heap-allocated tagged
//! values (managed by the runtime). The generated code calls into runtime
//! functions for value construction, operations, and SQLite persistence.

use crate::infer::{MonadInfo, MonadKind};
use crate::types::{ResolvedType, TypeEnv};
use knot::ast::Span;
use cranelift_codegen::ir::condcodes::IntCC;
use cranelift_codegen::ir::types;
use cranelift_codegen::ir::{AbiParam, InstBuilder, StackSlotData, StackSlotKind, Value};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_codegen::Context;
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{DataDescription, DataId, FuncId, Linkage, Module};
use cranelift_object::{ObjectBuilder, ObjectModule};
use knot::ast;
use std::collections::{HashMap, HashSet};


// ── Codegen state ─────────────────────────────────────────────────

/// One invalidation event for the `source_var_binds` map (see the field
/// docs on [`Codegen`]). Logged in statement order so nested do-block
/// scopes can replay invalidations onto their restored snapshots.
#[derive(Clone)]
enum SourceBindInvalidation {
    /// A variable was rebound by a later `let`/bind — drop its entry.
    Rebind(String),
    /// A specific source table was written — drop entries reading it.
    SourceWrite(String),
    /// A write happened whose target couldn't be identified statically
    /// (view target, or a call into possibly-writing code) — drop all.
    AllSources,
}

pub struct Codegen {
    module: ObjectModule,
    ctx: Context,
    builder_ctx: FunctionBuilderContext,
    /// Accumulates .eh_frame entries for every generated function so the
    /// system unwinder can walk Cranelift frames — required for the
    /// runtime's catch_unwind recovery (HTTP 500s, race cancellation) to
    /// work when a panic crosses generated code. See crate::unwind.
    unwind: crate::unwind::UnwindContext,
    ptr_type: types::Type,

    // Interned string constants
    strings: HashMap<String, DataId>,
    string_counter: usize,

    // Per-text-literal caching slot.  Each unique text literal has an
    // 8-byte zero-initialized data slot; first use calls
    // `knot_value_text_intern(ptr, len, slot)` which atomically
    // populates the slot, subsequent uses load directly.  Replaces the
    // runtime-side LRU for text literals — no hashing, no eviction,
    // O(1) load on the hot path.
    text_literal_slots: HashMap<String, DataId>,

    // Runtime function declarations (imported)
    runtime_fns: HashMap<String, FuncId>,

    // User function declarations: name -> (func_id, param_count)
    user_fns: HashMap<String, (FuncId, usize)>,

    // Names registered as stdlib builtins (skip user redefinitions)
    stdlib_fns: HashSet<String>,

    // Stdlib names the user re-defines at top level. A user-defined function
    // OVERRIDES a same-named builtin (see compile_app's shadowing rule), so the
    // stdlib version is neither registered nor defined for these names — the
    // user's declaration flows through the normal function path instead.
    user_shadowed_stdlib: HashSet<String>,

    // Source relation schemas: name -> schema descriptor
    source_schemas: HashMap<String, String>,
    /// Variables bound from source reads: var_name → source_name.
    /// Populated by compile_io_do_eager when it processes `x <- *source`.
    /// Enables SQL optimization for `do { m <- x; where ...; yield m }`.
    source_var_binds: HashMap<String, String>,
    /// Invalidations applied to `source_var_binds` (and `let_bindings`)
    /// during the current function body, in statement order. Do-block
    /// compilers snapshot the log length on entry and replay the suffix
    /// after restoring their saved maps, so an invalidation that happened
    /// inside a nested scope (e.g. a write inside an `atomic` body) still
    /// kills stale entries in the enclosing scope. Without this, a
    /// pushed-down `count xs` after `*items = ...` would re-query the
    /// table instead of using the pre-write binding.
    source_bind_invalidations: Vec<SourceBindInvalidation>,

    /// User function bodies: name → AST body.
    /// Enables cross-function-boundary SQL optimization by resolving named
    /// predicates and partial applications to their definitions.
    fun_bodies: HashMap<String, ast::Expr>,

    /// In-scope `let pat = expr` bindings inside the current do-block.
    /// Populated by `compile_do` / `compile_io_do_eager` as they walk
    /// statements; saved/restored around each do-block scope.  Lets the
    /// SQL pushdown matchers fold through `let foo = union *rel new`
    /// before pattern-matching on `foo`.  Local entries shadow
    /// `fun_bodies` during inlining.
    let_bindings: HashMap<String, ast::Expr>,

    /// Names bound inside the current IO do-block whose value is statically
    /// known to be a relation (let-bound comprehensions — including groupBy
    /// blocks — source reads, list literals, relation-returning stdlib
    /// calls). A later `pat <- name` bind in the same IO do-block iterates
    /// the relation per row (comprehension semantics) instead of binding
    /// the whole relation value. Saved/restored around each IO do-block
    /// scope by `compile_io_do_eager`.
    io_relation_vars: HashSet<String>,

    // Constructor info: ctor_name -> [(field_name, field_type_str)]
    constructors: HashMap<String, Vec<(String, String)>>,

    // Counter for generating unique lambda names
    lambda_counter: usize,

    // Pending lambda definitions: (func_id, params, body, free_vars)
    pending_lambdas: Vec<PendingLambda>,

    // Pending IO do-block thunks: deferred compilation of IO do-block bodies
    pending_io_thunks: Vec<PendingIoThunk>,

    // Pending trampolines for multi-param user functions (curry chains)
    pending_trampolines: Vec<PendingTrampoline>,

    // Counter for generating unique IO thunk names
    io_thunk_counter: usize,

    // Database path baked into the compiled binary
    db_path: String,

    // Migration schemas: relation_name -> Vec<(old_schema, new_schema)>
    migrate_schemas: HashMap<String, Vec<(String, String)>>,

    // View declarations: view_name -> provenance info
    views: HashMap<String, ViewInfo>,

    // Collected diagnostics
    diagnostics: Vec<knot::diagnostic::Diagnostic>,

    // Trait support: method_name -> dispatch info
    trait_methods: HashMap<String, TraitMethodInfo>,

    // Trait definitions: trait_name -> TraitDef (for default method lookup)
    trait_defs: HashMap<String, TraitDef>,

    // Data type -> constructor names (for ADT trait dispatch)
    data_constructors: HashMap<String, Vec<String>>,

    // Trait method dispatcher function IDs (method_name -> func_id)
    trait_dispatcher_fns: HashMap<String, FuncId>,

    // Derived method bodies to define (from `deriving` clauses)
    derived_methods: Vec<DerivedMethodDef>,

    // Supertrait relationships: trait_name -> direct supertrait names
    trait_supertraits: HashMap<String, Vec<String>>,

    // Track which types implement which trait: trait_name -> [(type_name, impl_span)]
    trait_impl_types: HashMap<String, Vec<(String, knot::ast::Span)>>,

    // Subset constraints: (sub, sup) relation paths
    subset_constraints: Vec<(knot::ast::RelationPath, knot::ast::RelationPath)>,

    // Names of derived relations that are self-referencing (recursive)
    recursive_derived: HashSet<String>,

    // Body function IDs for recursive derived relations: name -> func_id
    recursive_body_fns: HashMap<String, FuncId>,

    // Route entries: route_name -> entries (for HTTP codegen)
    route_entries: HashMap<String, Vec<ast::RouteEntry>>,

    // Route entry chosen for each fetch constructor: ctor_name -> entry.
    // Populated last-wins in source declaration order so distinct routes that
    // legally share a constructor name resolve to the SAME entry that infer's
    // fetch_response_types/fetch_response_headers picked — otherwise a fetch
    // could typecheck against one route and compile the HTTP call against
    // another (B38).
    fetch_route_entries: HashMap<String, ast::RouteEntry>,

    // Type aliases for resolving response types in OpenAPI descriptors
    type_aliases: HashMap<String, ResolvedType>,

    // Trampolines for user functions used as values: fn_name -> trampoline_func_id
    user_fn_trampolines: HashMap<String, FuncId>,

    // Do-blocks sitting in the value position of a `set`/`replace`, keyed by
    // span. These are relational comprehensions even when they bind from a
    // source (`x <- *rel`), which `is_io_do_block` would otherwise read as IO.
    // A do-block directly under the `=` is handled by `compile_set_value_expr`,
    // but one nested inside an `if`/`case` branch is reached through the
    // generic `compile_expr` path, which has no way to know it is producing the
    // relation being written — hence the span set.
    relational_do_spans: HashSet<ast::Span>,

    // Resolved monad types for desugared do-blocks (from type inference)
    monad_info: MonadInfo,
    /// Static dispatch type per trait-method occurrence, from inference.
    trait_call_targets: crate::infer::TraitCallTargets,
    /// Trait method name → the trait that declares it.
    trait_method_traits: HashMap<String, String>,
    /// Trait method occurrences left to the runtime dispatcher because the site
    /// is polymorphic. Checked for constructor-tag ambiguity after codegen.
    dynamic_dispatch_sites: Vec<(String, Span)>,

    // Builtin relation impls that were actually registered (not already provided by user/prelude)
    registered_builtin_impls: HashSet<String>,

    // Nullable-encoded ADTs: ctor_name -> NullableInfo
    // Types isomorphic to Maybe (one nullary ctor, one non-nullary ctor)
    // are encoded as nullable pointers: null = none variant, bare payload = some variant.
    nullable_ctors: HashMap<String, NullableRole>,

    // User-defined functions whose bodies (transitively) produce IO values
    io_functions: HashSet<String>,

    // User-defined functions whose bodies (transitively) perform a relation
    // write (Set/ReplaceSet or a nested call to another writing function).
    // Used to skip the SAVEPOINT in read-only `atomic` blocks — the version-
    // snapshot retry machinery doesn't need transactional rollback when no
    // SQL writes can happen.
    write_functions: HashSet<String>,
    /// Functions that may RETURN one of their arguments unapplied, to be run
    /// by the caller (`when`/`unless`/`id`, or user helpers of that shape).
    /// The write-analysis can't see a write performed by such a returned
    /// action from the call syntax alone, so an application of one of these
    /// to an opaque IO value is treated as possibly-writing (otherwise a
    /// write laundered through `when act` slips past the `atomic` SAVEPOINT).
    passthrough_functions: HashSet<String>,
    /// Names of all top-level function declarations (used by the
    /// write-analysis: calls to anything NOT in this set and not a builtin
    /// are conservatively treated as possibly-writing).
    top_fn_names: HashSet<String>,

    // Scalar sources: source names whose type is a bare primitive (e.g. `*counter : Int 1`)
    // rather than a relation of records. These get automatic wrap/unwrap of `_value` field.
    scalar_sources: HashSet<String>,

    // Overridable constants: name -> type string ("Int", "Float", "Text", "Bool")
    overridable_constants: HashMap<String, String>,

    // Default-value display strings for overridable constants whose body is a
    // simple, displayable expression (literal, negated literal, Just/Nothing).
    // Used by `--help` to show the actual default rather than a generic
    // "default from source" placeholder. Constants with complex bodies are
    // absent from this map.
    overridable_defaults: HashMap<String, String>,

    // Compile-time constant overrides: name -> raw string value (parsed during codegen)
    compile_time_overrides: HashMap<String, String>,

    // Body-less top-level constants that must be supplied as CLI arguments at run time.
    required_constants: Vec<RequiredConstant>,

    // Whether we are inside compile_io_do_eager — when true, Yield compiles to
    // the raw inner value rather than wrapping in knot_relation_singleton.
    in_io_eager: bool,

    // Current atomic retry block — when compiling inside an `atomic` body,
    // this is the block that `retry` jumps to (rollback + wait + loop).
    // Used to short-circuit execution on retry instead of flag-based checking.
    atomic_retry_block: Option<cranelift_codegen::ir::Block>,
    // Innermost IO comprehension loop's row-skip block — when compiling the
    // statements that follow a `pat <- relation` bind inside an IO do-block,
    // `where` guard failures and pattern-bind mismatches jump here so the
    // current row is SKIPPED (no value pushed into the loop's result),
    // instead of to the do-block's done_block (which would push unit).
    // Saved/restored around each loop's rest and cleared for fresh nested
    // do-blocks and new function contexts.
    io_loop_skip_block: Option<cranelift_codegen::ir::Block>,
    // Number of arena frames pushed (and not yet popped) since the innermost
    // atomic loop head. The direct `retry` jump to atomic_retry_block must
    // emit this many extra knot_arena_pop_frame calls first: retry_block
    // itself pops only the atomic's own frame, so frames opened by nested
    // do-blocks (or bind-expression frame isolation) between the loop head
    // and the `retry` would otherwise leak one frame per retry iteration.
    atomic_arena_frames: usize,

    // Pre-built hash-join index values (`knot_relation_build_index` results)
    // that are currently live. These are system-heap `Box<HashIndex>`
    // allocations, NOT arena-managed, so the only reclaim path is an explicit
    // `knot_relation_index_free`. `compile_do` pushes its indices here after it
    // pre-builds them and pops them on exit; the `retry` handler reads this
    // stack and frees every live index before jumping to the atomic retry
    // block — otherwise each retry iteration leaks one `Box<HashIndex>` per
    // active join (the normal-exit free loop is bypassed by the retry jump).
    pending_index_frees: Vec<Value>,

    // Refined type predicates: type_name -> predicate AST expression
    refined_types: HashMap<String, knot::ast::Expr>,
    // Declared (unresolved) alias bodies: alias_name -> AST type. Unlike
    // `type_aliases`, these keep their `where` refinements, so
    // `collect_type_refinements` can follow `events: [GossipEvent]` into
    // `GossipEvent`'s own refined fields.
    alias_ast: HashMap<String, knot::ast::Type>,
    // Source refinements: source_name -> [(field_name_or_none, type_name, predicate_expr)]
    source_refinements: HashMap<String, Vec<(Option<String>, String, knot::ast::Expr)>>,
    // Refine expression targets: expr_span -> refined type name
    refine_targets: HashMap<knot::ast::Span, String>,
    // Compiled predicate function values: type_name -> func_id
    #[allow(dead_code)]
    refined_predicate_fns: HashMap<String, FuncId>,
    // parseJson call targets: app_span -> resolved type name (for compile-time
    // FromJSON dispatch) + wire schema (for Maybe-aware decoding)
    from_json_targets: crate::infer::FromJsonTargets,

    // Spans of `elem` haystack args whose element type is SQL-pushable
    // Spans of `elem` haystacks whose element type is SQL-pushable, split by
    // path: `literal` (the `IN (?, …)` list form) and `dynamic` (the
    // `IN (SELECT value FROM json_each(?))` form). See `infer::ElemPushdownOk`.
    elem_pushdown_ok: crate::infer::ElemPushdownOk,

    // `show` call sites whose argument has a concrete unit of measure:
    // app_span -> canonical unit string (e.g. "M", "M/S^2"). Units are erased
    // here, so the string from inference is the only carrier of the unit into
    // the emitted code.
    show_unit_strings: crate::infer::ShowUnitStrings,

    // Spans of full `sum f rel` calls whose result is a Float. Passed to the
    // runtime as `is_float` so an EMPTY relation sums to `Float 0.0` rather
    // than `Int 0`. See `infer::SumFloatSpans`.
    sum_float_spans: crate::infer::SumFloatSpans,

    // Spans of field accesses whose field type is a relation (`t.members` where
    // `members : [{who: Text}]`). A record's field types are unreachable from
    // the AST, so this is the only way codegen can tell a nested-relation field
    // from a scalar one. See `infer::RelationFieldSpans`.
    relation_fields: crate::infer::RelationFieldSpans,

    // Set by `compile_io_do_eager` when the statements it compiled ended in an
    // iterating bind, i.e. its value is that loop's relation of per-row results
    // rather than one row's value. An enclosing `compile_io_bind_loop` reads it
    // right after compiling its body to decide whether to splice those rows into
    // its own result (`m <- t.members` under `t <- *teams` yields one flat
    // relation of members) or push the value as a single row.
    io_do_tail_iterated: bool,
}

/// A top-level constant declared as a signature with no body
/// (e.g. `port : Int 1` or `host : Text`). The value must be supplied
/// at run time via a `--<name>=<value>` CLI argument.
#[derive(Clone)]
struct RequiredConstant {
    name: String,
    /// Base scalar type for argv parsing: "Int" / "Float" / "Text" / "Bool".
    base_type: String,
    /// Optional refinement predicate to validate the parsed value against.
    refinement: Option<RequiredRefinement>,
}

#[derive(Clone)]
struct RequiredRefinement {
    /// Display label for the refinement (the refined type's name, or the
    /// constant's own name for inline `Int where ...` annotations).
    type_label: String,
    /// Predicate AST — typically a lambda like `\x -> x > 0`.
    predicate: ast::Expr,
}

/// Role of a constructor in a nullable-encoded ADT.
/// Currently unused — nullable encoding is disabled to avoid representation
/// mismatch with SQLite-read values (see collect_declarations).
#[derive(Clone, Debug)]
#[allow(dead_code)]
enum NullableRole {
    /// The nullary constructor (e.g. Nothing) — encoded as null pointer
    None,
    /// The constructor with fields (e.g. Just) — encoded as bare payload
    Some,
}

/// Provenance info for a view declaration, extracted at compile time.
#[derive(Clone)]
#[allow(dead_code)]
struct ViewInfo {
    /// The underlying source relation name.
    source_name: String,
    /// Source columns: (yield_field_name, source_field_name).
    source_columns: Vec<(String, String)>,
    /// Constant columns: (field_name, constant_expr).
    constant_columns: Vec<(String, ast::Expr)>,
    /// The full view body expression (for read compilation).
    body: ast::Expr,
}

struct PendingLambda {
    func_id: FuncId,
    params: Vec<String>,
    /// The original parameter pattern (for destructuring bind in the lambda body).
    param_pat: Option<ast::Pat>,
    body: ast::Expr,
    free_vars: Vec<String>,
}

/// A deferred IO do-block body compiled as a thunk function.
/// The thunk has signature `(db, env) -> result` matching the IO convention.
struct PendingIoThunk {
    func_id: FuncId,
    stmts: Vec<ast::Stmt>,
    free_vars: Vec<String>,
}

/// A deferred trampoline for a multi-param user function.
/// Generates a curry chain that directly calls the user function,
/// avoiding the infinite recursion that occurs when trampolines
/// resolve back through user_fns.
struct PendingTrampoline {
    trampoline_id: FuncId,
    user_fn_name: String,
    n_params: usize,
}

/// Information about a trait method for runtime dispatch.
struct TraitMethodInfo {
    param_count: usize,
    /// Which parameter to dispatch on (index into params, after db).
    /// None means dispatch is impossible (e.g. `yield : a -> f a` where
    /// the type constructor only appears in the return type).
    dispatch_index: Option<usize>,
    impls: Vec<ImplEntry>,
}

struct ImplEntry {
    type_name: String,
    func_id: FuncId,
    /// True for intrinsic codegen impls registered by the compiler itself
    /// (primitive Eq/Ord/Num/…, built-in []/IO HKT impls). These delegate to
    /// the same runtime functions the operator fast paths call, so operators
    /// only need to dispatch through the trait when a non-builtin (user or
    /// prelude) impl exists.
    is_builtin: bool,
}

/// Default method definition from a trait declaration.
#[derive(Clone)]
struct DefaultMethod {
    params: Vec<ast::Pat>,
    body: ast::Expr,
}

/// Info about a trait declaration (methods with optional defaults).
struct TraitDef {
    defaults: HashMap<String, DefaultMethod>,
    /// Names of associated types declared in this trait.
    #[allow(dead_code)]
    associated_types: Vec<String>,
}

/// Tracks pending derived method definitions (mangled_name -> default method).
struct DerivedMethodDef {
    mangled: String,
    default: DefaultMethod,
}

// ── Variable environment ──────────────────────────────────────────

#[derive(Clone)]
struct Env {
    bindings: HashMap<String, Value>,
}

impl Env {
    fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    /// Look up a binding. Returns `None` for an unbound name — callers turn that
    /// into a codegen diagnostic (via `Codegen::push_codegen_error`) rather than
    /// panicking, since a well-typed program never reaches an unbound variable
    /// but malformed input still could.
    fn get(&self, name: &str) -> Option<Value> {
        self.bindings.get(name).copied()
    }

    fn set(&mut self, name: &str, val: Value) {
        self.bindings.insert(name.to_string(), val);
    }
}

// ── Loop tracking for do-block compilation ────────────────────────

struct LoopInfo {
    header: cranelift_codegen::ir::Block,
    continue_blk: cranelift_codegen::ir::Block,
    exit: cranelift_codegen::ir::Block,
    index_var: Value,
    where_skips: Vec<cranelift_codegen::ir::Block>,
    /// Arena mark set at loop body entry — used for per-iteration reset.
    arena_mark: Value,
}

/// Per-method trait dispatch metadata:
/// `(method_name, dispatcher func id, param count, optional dispatch-arg index,
/// `(type_name, impl func id)` list)`.
type DispatcherInfo = Vec<(String, FuncId, usize, Option<usize>, Vec<(String, FuncId)>)>;

/// Matched conditional-update shape:
/// `(bind var, condition expr, `(field, value-expr)` update assignments)`.
type ConditionalUpdateMatch<'a> = (String, &'a ast::Expr, Vec<(&'a str, &'a ast::Expr)>);

/// Resolve a constant's inferred type, as rendered by inference, to the scalar
/// type the runtime knows how to parse from argv. Returns one of the eight
/// strings `define_user_function` maps to a type tag, or `None` when the
/// constant is not CLI-overridable.
///
/// A constant annotated with an alias of a scalar (`type Host = Text`) renders
/// as the alias name, so matching the rendered string alone would drop it and
/// leave the constant with no `knot_override_lookup` call — the flag would be
/// accepted and silently ignored. Resolving through `type_aliases` keeps this
/// in step with `classify_required_constant_type`, which already does so for
/// body-less constants.
///
/// Refined aliases are deliberately excluded: nothing here runs their
/// predicate, so honouring the flag would let a CLI value in through a check
/// the type promises. (Body-less constants take the refined path instead,
/// which validates via `knot_override_refinement_check`.)
fn scalar_override_type(
    ty_str: &str,
    type_aliases: &HashMap<String, ResolvedType>,
    refined_types: &HashMap<String, ast::Expr>,
) -> Option<String> {
    let (prefix, base) = match ty_str.strip_prefix("Maybe ") {
        Some(rest) => ("Maybe ", rest),
        None => ("", ty_str),
    };
    let resolved = match base {
        "Int" | "Float" | "Text" | "Bool" => base,
        name => {
            if refined_types.contains_key(name) {
                return None;
            }
            match type_aliases.get(name)? {
                ResolvedType::Int => "Int",
                ResolvedType::Float => "Float",
                ResolvedType::Text => "Text",
                ResolvedType::Bool => "Bool",
                _ => return None,
            }
        }
    };
    Some(format!("{prefix}{resolved}"))
}

/// Classify the type annotation on a body-less constant declaration.
///
/// Returns `Some((base_type_str, refinement))` when the type resolves to a
/// scalar primitive (or a refined alias of one); the `base_type_str` is
/// `"Int"`, `"Float"`, `"Text"`, or `"Bool"` for argv parsing. Returns `None`
/// when the type is unsupported (e.g. a record, relation, or unknown alias).
fn classify_required_constant_type(
    ty: &ast::Type,
    const_name: &str,
    type_aliases: &HashMap<String, ResolvedType>,
    refined_types: &HashMap<String, ast::Expr>,
) -> Option<(String, Option<RequiredRefinement>)> {
    match &ty.node {
        ast::TypeKind::Refined { base, predicate } => {
            // Inline refinement: `name : Int 1 where \x -> x > 0`. The refined type
            // has no name of its own, so we use the constant's name as the label.
            let (base_type, _) = classify_required_constant_type(
                base,
                const_name,
                type_aliases,
                refined_types,
            )?;
            Some((
                base_type,
                Some(RequiredRefinement {
                    type_label: const_name.to_string(),
                    predicate: (**predicate).clone(),
                }),
            ))
        }
        ast::TypeKind::Named(name) => match name.as_str() {
            "Int" => Some(("Int".to_string(), None)),
            "Float" => Some(("Float".to_string(), None)),
            "Text" => Some(("Text".to_string(), None)),
            "Bool" => Some(("Bool".to_string(), None)),
            _ => {
                let resolved = type_aliases.get(name)?;
                let base_type = match resolved {
                    ResolvedType::Int => "Int",
                    ResolvedType::Float => "Float",
                    ResolvedType::Text => "Text",
                    ResolvedType::Bool => "Bool",
                    _ => return None,
                }
                .to_string();
                let refinement = refined_types.get(name).map(|pred| RequiredRefinement {
                    type_label: name.clone(),
                    predicate: pred.clone(),
                });
                Some((base_type, refinement))
            }
        },
        _ => None,
    }
}

/// Render a constant's default-value expression as a short string for
/// `--help` output. Only handles simple, unambiguous shapes: literals,
/// negated numeric literals, `True {}`/`False {}`, `Nothing {}`, and
/// `Just {value: <literal>}`. Returns `None` for anything else (the help
/// text will fall back to a generic placeholder).
fn format_default_value_display(expr: &ast::Expr) -> Option<String> {
    match &expr.node {
        ast::ExprKind::Lit(lit) => format_literal_display(lit),
        ast::ExprKind::UnaryOp { op: ast::UnaryOp::Neg, operand } => {
            if let ast::ExprKind::Lit(lit) = &operand.node {
                format_literal_display(lit).map(|s| format!("-{}", s))
            } else {
                None
            }
        }
        ast::ExprKind::Constructor(name) if name == "Nothing" => {
            Some("Nothing".to_string())
        }
        ast::ExprKind::Constructor(name) if name == "True" => Some("true".to_string()),
        ast::ExprKind::Constructor(name) if name == "False" => Some("false".to_string()),
        ast::ExprKind::App { func, arg } => {
            if let ast::ExprKind::Constructor(name) = &func.node {
                if name == "Nothing" {
                    return Some("Nothing".to_string());
                }
                if (name == "True" || name == "False")
                    && let ast::ExprKind::Record(fields) = &arg.node
                        && fields.is_empty() {
                            return Some(if name == "True" { "true" } else { "false" }.to_string());
                        }
                if name == "Just" {
                    // `Just {value: <lit>}` — Knot constructors take a record.
                    if let ast::ExprKind::Record(fields) = &arg.node
                        && fields.len() == 1 && fields[0].name == "value" {
                            return format_default_value_display(&fields[0].value)
                                .map(|s| format!("Just {}", s));
                        }
                }
            }
            None
        }
        _ => None,
    }
}

fn format_literal_display(lit: &ast::Literal) -> Option<String> {
    match lit {
        ast::Literal::Int(s) => Some(s.clone()),
        ast::Literal::Float(f) => Some(format!("{}", f)),
        ast::Literal::Text(s) => {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            Some(format!("\"{}\"", escaped))
        }
        ast::Literal::Bool(b) => Some(if *b { "true" } else { "false" }.to_string()),
        ast::Literal::Bytes(_) => None,
    }
}

// ── Public API ────────────────────────────────────────────────────

// Entry point threads every inference artifact into codegen; grouping them
// into a struct would obscure more than it helps.
///
/// Runs on a grown stack — `compile_expr` recurses through the `__bind` chain
/// a desugared `do` block expands into, one level per statement.
#[allow(clippy::too_many_arguments)]
pub fn compile(
    module: &ast::Module,
    type_env: &TypeEnv,
    source_file: &str,
    monad_info: &MonadInfo,
    refine_targets: &crate::infer::RefineTargets,
    refined_types: &crate::infer::RefinedTypeInfoMap,
    from_json_targets: &crate::infer::FromJsonTargets,
    type_info: &crate::infer::TypeInfo,
    elem_pushdown_ok: &crate::infer::ElemPushdownOk,
    trait_call_targets: &crate::infer::TraitCallTargets,
    show_unit_strings: &crate::infer::ShowUnitStrings,
    sum_float_spans: &crate::infer::SumFloatSpans,
    relation_fields: &crate::infer::RelationFieldSpans,
    compile_time_overrides: &HashMap<String, String>,
) -> Result<Vec<u8>, Vec<knot::diagnostic::Diagnostic>> {
    crate::stack::grow(|| {
        compile_inner(
            module,
            type_env,
            source_file,
            monad_info,
            refine_targets,
            refined_types,
            from_json_targets,
            type_info,
            elem_pushdown_ok,
            trait_call_targets,
            show_unit_strings,
            sum_float_spans,
            relation_fields,
            compile_time_overrides,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn compile_inner(
    module: &ast::Module,
    type_env: &TypeEnv,
    source_file: &str,
    monad_info: &MonadInfo,
    refine_targets: &crate::infer::RefineTargets,
    refined_types: &crate::infer::RefinedTypeInfoMap,
    from_json_targets: &crate::infer::FromJsonTargets,
    type_info: &crate::infer::TypeInfo,
    elem_pushdown_ok: &crate::infer::ElemPushdownOk,
    trait_call_targets: &crate::infer::TraitCallTargets,
    show_unit_strings: &crate::infer::ShowUnitStrings,
    sum_float_spans: &crate::infer::SumFloatSpans,
    relation_fields: &crate::infer::RelationFieldSpans,
    compile_time_overrides: &HashMap<String, String>,
) -> Result<Vec<u8>, Vec<knot::diagnostic::Diagnostic>> {
    let mut cg = Codegen::new();
    // Derive database path from source filename: "foo.knot" → "foo.db"
    let stem = std::path::Path::new(source_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("knot");
    cg.db_path = format!("{}.db", stem);
    cg.source_schemas = type_env.source_schemas.clone();
    for (name, schema) in &type_env.source_schemas {
        if schema.starts_with("_value:") {
            cg.scalar_sources.insert(name.clone());
        }
    }
    cg.migrate_schemas = type_env.migrate_schemas.clone();
    cg.type_aliases = type_env.aliases.clone();
    cg.subset_constraints = type_env.subset_constraints.clone();
    cg.monad_info = monad_info.clone();
    cg.trait_call_targets = trait_call_targets.clone();
    cg.refine_targets = refine_targets.clone();
    cg.refined_types = refined_types.clone();
    cg.alias_ast = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            ast::DeclKind::TypeAlias { name, params, ty } if params.is_empty() => {
                Some((name.clone(), ty.clone()))
            }
            _ => None,
        })
        .collect();
    cg.from_json_targets = from_json_targets.clone();
    cg.elem_pushdown_ok = elem_pushdown_ok.clone();
    cg.show_unit_strings = show_unit_strings.clone();
    cg.sum_float_spans = sum_float_spans.clone();
    cg.relation_fields = relation_fields.clone();
    cg.source_refinements = type_env.source_refinements.clone();
    for (name, fields) in &type_env.constructors {
        let field_strs: Vec<(String, String)> = fields
            .iter()
            .map(|(n, _)| (n.clone(), "unknown".into()))
            .collect();
        cg.constructors.insert(name.clone(), field_strs);
    }
    cg.declare_runtime_fns();
    // Collect view declarations and analyze provenance
    for decl in &module.decls {
        if let ast::DeclKind::View { name, body, .. } = &decl.node {
            match analyze_view(body) {
                Ok(Some(info)) => {
                    cg.views.insert(name.clone(), info);
                }
                Ok(None) => {}
                Err((span, msg)) => {
                    cg.diagnostics.push(
                        knot::diagnostic::Diagnostic::error(msg)
                            .label(span, "unsupported view filter"),
                    );
                }
            }
        }
    }
    cg.collect_declarations(module);
    // Register body-less top-level constants as required CLI arguments.
    // Each becomes a 0-param user_fn whose body reads --<name>=value at startup,
    // exits if missing, and runs any attached refinement predicate.
    for decl in &module.decls {
        if let ast::DeclKind::Fun { name, body: None, ty: Some(ts), .. } = &decl.node {
            if name == "main" {
                continue;
            }
            // Already registered (e.g. by a duplicate decl) — skip.
            if cg.user_fns.contains_key(name.as_str()) {
                continue;
            }
            let classified = classify_required_constant_type(
                &ts.ty,
                name,
                &cg.type_aliases,
                &cg.refined_types,
            );
            let (base_type, refinement) = match classified {
                Some(v) => v,
                None => {
                    cg.diagnostics.push(knot::diagnostic::Diagnostic::error(
                        format!(
                            "constant '{}' has no body, so it must be supplied as a CLI argument; \
                             this requires a scalar type (Int, Float, Text, Bool) or a refined alias of one",
                            name
                        ),
                    ).label(decl.span, "unsupported type for required CLI constant"));
                    continue;
                }
            };
            // Register a 0-param user function for this constant so references resolve.
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.returns.push(AbiParam::new(cg.ptr_type));
            let func_name = format!("knot_user_{}", name);
            let func_id = cg
                .module
                .declare_function(&func_name, Linkage::Local, &sig)
                .unwrap();
            cg.user_fns.insert(name.clone(), (func_id, 0));
            cg.overridable_constants.insert(name.clone(), base_type.clone());
            cg.required_constants.push(RequiredConstant {
                name: name.clone(),
                base_type,
                refinement,
            });
        }
    }
    // Compute overridable constants: 0-param Fun declarations with scalar types
    for decl in &module.decls {
        if let ast::DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
            if name == "main" {
                continue;
            }
            if let Some((_, 0)) = cg.user_fns.get(name.as_str())
                && let Some(ty_str) = type_info.get(name.as_str())
                && let Some(base_type) =
                    scalar_override_type(ty_str, &cg.type_aliases, &cg.refined_types)
            {
                cg.overridable_constants.insert(name.clone(), base_type);
                if let Some(disp) = format_default_value_display(body) {
                    cg.overridable_defaults.insert(name.clone(), disp);
                }
            }
        }
    }
    // Validate and store compile-time overrides
    for (name, val) in compile_time_overrides {
        if let Some(ty_str) = cg.overridable_constants.get(name) {
            let is_maybe = ty_str.starts_with("Maybe ");
            // `Nothing` is a valid override for any Maybe-typed constant
            // (emitted as the null none-encoding by `emit_override_literal`),
            // so skip the inner-type parse check for it.
            if !(is_maybe && val == "Nothing") {
                let base_type = match ty_str.as_str() {
                    "Int" | "Maybe Int" => "Int",
                    "Float" | "Maybe Float" => "Float",
                    "Text" | "Maybe Text" => "Text",
                    "Bool" | "Maybe Bool" => "Bool",
                    _ => continue,
                };
                match base_type {
                    "Int"
                        if val.parse::<i64>().is_err() => {
                            cg.diagnostics.push(knot::diagnostic::Diagnostic::error(
                                format!("invalid compile-time override '{}' for --{} (expected Int)", val, name),
                            ));
                            continue;
                        }
                    "Float"
                        if val.parse::<f64>().is_err() => {
                            cg.diagnostics.push(knot::diagnostic::Diagnostic::error(
                                format!("invalid compile-time override '{}' for --{} (expected Float)", val, name),
                            ));
                            continue;
                        }
                    "Bool"
                        if !matches!(val.as_str(), "true" | "True" | "false" | "False" | "0" | "1") => {
                            cg.diagnostics.push(knot::diagnostic::Diagnostic::error(
                                format!("invalid compile-time override '{}' for --{} (expected true or false)", val, name),
                            ));
                            continue;
                        }
                    _ => {} // Text always valid
                }
            }
            cg.compile_time_overrides.insert(name.clone(), val.clone());
        } else {
            cg.diagnostics.push(knot::diagnostic::Diagnostic::error(
                format!("unknown constant '{}' for compile-time override", name),
            ));
        }
    }
    cg.define_functions(module, type_env);
    cg.generate_main(module);
    // Drain lambdas and IO thunks created by generate_main (e.g., migration functions)
    while !cg.pending_lambdas.is_empty() || !cg.pending_io_thunks.is_empty() || !cg.pending_trampolines.is_empty() {
        let lambdas: Vec<PendingLambda> = std::mem::take(&mut cg.pending_lambdas);
        for lambda in lambdas {
            cg.define_lambda_function(&lambda);
        }
        let thunks: Vec<PendingIoThunk> = std::mem::take(&mut cg.pending_io_thunks);
        for thunk in thunks {
            cg.define_io_thunk_function(&thunk);
        }
        let trampolines: Vec<PendingTrampoline> = std::mem::take(&mut cg.pending_trampolines);
        for tramp in &trampolines {
            cg.define_trampoline(tramp);
        }
    }
    cg.check_ambiguous_dynamic_dispatch();
    if !cg.diagnostics.is_empty() {
        return Err(cg.diagnostics);
    }
    Ok(cg.finish())
}

// ── Constructor ───────────────────────────────────────────────────

impl Codegen {
    fn new() -> Self {
        let mut flag_builder = settings::builder();
        flag_builder.set("is_pic", "true").unwrap();
        let mut isa_builder =
            cranelift_native::builder().expect("failed to detect host CPU");
        // cranelift-native enables return-address signing (PAC) on Apple
        // Silicon. The resulting RA_SIGN_STATE DWARF expressions in our
        // .eh_frame are rejected by Apple's libunwind when it steps through
        // generated frames, turning every panic that crosses generated code
        // into a fatal "failed to initiate panic" abort — which defeats the
        // runtime's catch_unwind recovery (HTTP 500s, race cancellation).
        // Plain arm64 macOS binaries don't sign return addresses anyway.
        let _ = isa_builder.set("sign_return_address", "false");
        let _ = isa_builder.set("sign_return_address_with_bkey", "false");
        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .expect("failed to build ISA");
        let ptr_type = isa.pointer_type();

        let builder = ObjectBuilder::new(
            isa,
            "knot_program",
            cranelift_module::default_libcall_names(),
        )
        .expect("failed to create ObjectBuilder");
        let mut module = ObjectModule::new(builder);
        // is_pic is set above, so .eh_frame pointers use PC-relative encoding.
        let unwind = crate::unwind::UnwindContext::new(&mut module, true);

        Self {
            ctx: module.make_context(),
            module,
            builder_ctx: FunctionBuilderContext::new(),
            unwind,
            ptr_type,
            strings: HashMap::new(),
            string_counter: 0,
            text_literal_slots: HashMap::new(),
            runtime_fns: HashMap::new(),
            user_fns: HashMap::new(),
            stdlib_fns: HashSet::new(),
            user_shadowed_stdlib: HashSet::new(),
            source_schemas: HashMap::new(),
            source_var_binds: HashMap::new(),
            source_bind_invalidations: Vec::new(),
            fun_bodies: HashMap::new(),
            let_bindings: HashMap::new(),
            io_relation_vars: HashSet::new(),
            constructors: HashMap::new(),
            lambda_counter: 0,
            pending_lambdas: Vec::new(),
            pending_io_thunks: Vec::new(),
            pending_trampolines: Vec::new(),
            io_thunk_counter: 0,
            db_path: String::new(),
            migrate_schemas: HashMap::new(),
            views: HashMap::new(),
            diagnostics: Vec::new(),
            trait_methods: HashMap::new(),
            trait_defs: HashMap::new(),
            data_constructors: HashMap::new(),
            trait_dispatcher_fns: HashMap::new(),
            derived_methods: Vec::new(),
            trait_supertraits: HashMap::new(),
            trait_impl_types: HashMap::new(),
            subset_constraints: Vec::new(),
            recursive_derived: HashSet::new(),
            recursive_body_fns: HashMap::new(),
            route_entries: HashMap::new(),
            fetch_route_entries: HashMap::new(),
            type_aliases: HashMap::new(),
            user_fn_trampolines: HashMap::new(),
            relational_do_spans: HashSet::new(),
            monad_info: HashMap::new(),
            trait_call_targets: HashMap::new(),
            trait_method_traits: HashMap::new(),
            dynamic_dispatch_sites: Vec::new(),
            registered_builtin_impls: HashSet::new(),
            nullable_ctors: HashMap::new(),
            io_functions: HashSet::new(),
            write_functions: HashSet::new(),
            passthrough_functions: HashSet::new(),
            top_fn_names: HashSet::new(),
            scalar_sources: HashSet::new(),
            overridable_constants: HashMap::new(),
            overridable_defaults: HashMap::new(),
            compile_time_overrides: HashMap::new(),
            required_constants: Vec::new(),
            in_io_eager: false,
            atomic_retry_block: None,
            io_loop_skip_block: None,
            atomic_arena_frames: 0,
            pending_index_frees: Vec::new(),
            refined_types: HashMap::new(),
            alias_ast: HashMap::new(),
            refine_targets: HashMap::new(),
            refined_predicate_fns: HashMap::new(),
            source_refinements: HashMap::new(),
            from_json_targets: HashMap::new(),
            elem_pushdown_ok: crate::infer::ElemPushdownOk::default(),
            show_unit_strings: HashMap::new(),
            sum_float_spans: crate::infer::SumFloatSpans::new(),
            relation_fields: crate::infer::RelationFieldSpans::new(),
            io_do_tail_iterated: false,
        }
    }

    // ── Runtime function declarations ─────────────────────────────

    fn declare_runtime_fns(&mut self) {
        let p = self.ptr_type;

        // Value constructors
        self.declare_rt("knot_value_int", &[types::I64], &[p]);
        self.declare_rt("knot_value_int_from_str", &[p, p], &[p]);
        self.declare_rt("knot_value_float", &[types::F64], &[p]);
        self.declare_rt("knot_value_text", &[p, p], &[p]);
        self.declare_rt("knot_value_text_cached", &[p, p], &[p]);
        self.declare_rt("knot_value_text_intern", &[p, p, p], &[p]);
        self.declare_rt("knot_value_bool", &[types::I32], &[p]);
        self.declare_rt("knot_value_unit", &[], &[p]);
        self.declare_rt("knot_value_function", &[p, p, p, p], &[p]);
        self.declare_rt("knot_value_constructor", &[p, p, p], &[p]);

        // Value accessors
        self.declare_rt("knot_value_get_int", &[p], &[types::I64]);
        self.declare_rt("knot_value_get_float", &[p], &[types::F64]);
        self.declare_rt("knot_value_get_bool", &[p], &[types::I32]);

        // Record operations
        self.declare_rt("knot_record_empty", &[p], &[p]);
        self.declare_rt("knot_record_set_field", &[p, p, p, p], &[]);
        self.declare_rt("knot_record_field", &[p, p, p], &[p]);
        self.declare_rt("knot_record_field_by_index", &[p, p], &[p]);
        self.declare_rt("knot_record_from_pairs", &[p, p], &[p]);
        self.declare_rt("knot_record_update", &[p], &[p]);
        self.declare_rt("knot_record_update_batch", &[p, p, p], &[p]);

        // Relation operations
        self.declare_rt("knot_relation_empty", &[], &[p]);
        self.declare_rt("knot_relation_with_capacity", &[p], &[p]);
        self.declare_rt("knot_relation_dedup", &[p], &[p]);
        self.declare_rt("knot_relation_singleton", &[p], &[p]);
        self.declare_rt("knot_scalar_source_unwrap", &[p], &[p]);
        self.declare_rt("knot_scalar_source_wrap", &[p], &[p]);
        self.declare_rt("knot_relation_push", &[p, p], &[]);
        self.declare_rt("knot_relation_extend", &[p, p], &[]);
        self.declare_rt("knot_relation_len", &[p], &[p]);
        self.declare_rt("knot_relation_get", &[p, p], &[p]);
        self.declare_rt("knot_relation_tail", &[p], &[p]);
        self.declare_rt("knot_relation_union", &[p, p, p], &[p]);

        // Binary operations
        self.declare_rt("knot_value_add", &[p, p], &[p]);
        self.declare_rt("knot_value_sub", &[p, p], &[p]);
        self.declare_rt("knot_value_mul", &[p, p], &[p]);
        self.declare_rt("knot_value_div", &[p, p], &[p]);
        self.declare_rt("knot_value_mod", &[p, p], &[p]);
        self.declare_rt("knot_value_eq", &[p, p], &[p]);
        self.declare_rt("knot_value_neq", &[p, p], &[p]);
        self.declare_rt("knot_value_lt", &[p, p], &[p]);
        self.declare_rt("knot_value_gt", &[p, p], &[p]);
        self.declare_rt("knot_value_le", &[p, p], &[p]);
        self.declare_rt("knot_value_ge", &[p, p], &[p]);
        self.declare_rt("knot_value_eq_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_neq_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_lt_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_gt_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_le_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_ge_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_compare_ord", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_and", &[p, p], &[p]);
        self.declare_rt("knot_value_or", &[p, p], &[p]);
        self.declare_rt("knot_value_and_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_or_i32", &[p, p], &[types::I32]);
        self.declare_rt("knot_value_concat", &[p, p], &[p]);

        // Comparison (returns Ordering ADT)
        self.declare_rt("knot_value_compare", &[p, p], &[p]);
        self.declare_rt("knot_ordering_tag_i32", &[p], &[types::I32]);

        // Unary operations
        self.declare_rt("knot_value_negate", &[p], &[p]);
        self.declare_rt("knot_value_not", &[p], &[p]);

        // Function calls
        self.declare_rt("knot_value_call", &[p, p, p], &[p]);

        // Printing / reading / show
        self.declare_rt("knot_read_line", &[], &[p]);
        self.declare_rt("knot_print", &[p], &[p]);
        self.declare_rt("knot_println", &[p], &[p]);
        self.declare_rt("knot_value_show", &[p], &[p]);
        self.declare_rt("knot_value_show_unit", &[p, p, p], &[p]);
        self.declare_rt("knot_guard_failed", &[], &[]);

        // Constructor declaration order (backs structural `Ord` on ADTs)
        self.declare_rt("knot_register_ctor_order", &[p, p, p, p], &[]);

        // Database
        self.declare_rt("knot_db_open", &[p, p], &[p]);
        self.declare_rt("knot_db_close", &[p], &[]);
        self.declare_rt("knot_db_exec", &[p, p, p], &[]);
        self.declare_rt("knot_source_init", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_source_read", &[p, p, p, p, p], &[p]);
        self.declare_rt("knot_source_count", &[p, p, p], &[p]);
        self.declare_rt("knot_source_query_count", &[p, p, p, p], &[p]);
        self.declare_rt("knot_source_read_where", &[p, p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_source_query", &[p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_source_query_float", &[p, p, p, p], &[p]);
        self.declare_rt("knot_source_query_sum", &[p, p, p, p, types::I64], &[p]);
        self.declare_rt("knot_source_query_value", &[p, p, p, p, types::I64], &[p]);
        self.declare_rt("knot_source_fold", &[p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_source_query_fold", &[p, p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_source_write", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_append", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_diff_write", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_delete_where", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_update_where", &[p, p, p, p, p, p, p, p], &[]);

        // Schema tracking
        self.declare_rt("knot_schema_init", &[p], &[]);
        self.declare_rt("knot_source_migrate", &[p, p, p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_migrate_preview", &[p, p, p, p, p, p, p, p], &[p]);

        // Debug
        self.declare_rt("knot_debug_init", &[], &[]);

        // HTTP configuration (--http-max-body-bytes)
        self.declare_rt("knot_http_config_init", &[], &[]);

        // CLI constant overrides
        self.declare_rt("knot_override_lookup", &[p, p, types::I32], &[p]);
        self.declare_rt("knot_override_required_lookup", &[p, p, types::I32], &[p]);
        self.declare_rt("knot_override_refinement_check", &[p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_override_check_help", &[p, p], &[]);

        // STM tracking
        self.declare_rt("knot_stm_track_read", &[p, p], &[]);
        self.declare_rt("knot_stm_track_read_pred", &[p, p, p, p, p], &[]);

        // Transactions
        self.declare_rt("knot_atomic_begin", &[p], &[]);
        self.declare_rt("knot_atomic_commit", &[p], &[]);
        self.declare_rt("knot_atomic_rollback", &[p], &[]);

        // View operations
        self.declare_rt("knot_view_read", &[p, p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_relation_add_fields", &[p, p], &[p]);
        self.declare_rt("knot_relation_rename_columns", &[p, p, p], &[p]);
        self.declare_rt("knot_view_write", &[p, p, p, p, p, p, p, p, p], &[]);

        // Constructor matching
        self.declare_rt("knot_constructor_matches", &[p, p, p], &[types::I32]);
        self.declare_rt("knot_constructor_payload", &[p], &[p]);
        self.declare_rt("knot_constructor_tag_ptr", &[p], &[p]);
        self.declare_rt("knot_constructor_tag_len", &[p], &[p]);
        self.declare_rt("knot_str_eq", &[p, p, p, p], &[types::I32]);
        self.declare_rt("knot_ensure_relation", &[p], &[p]);

        // Trait dispatch error
        self.declare_rt("knot_trait_no_impl", &[p, p, p], &[p]);

        // Type tag inspection (for trait dispatch)
        self.declare_rt("knot_value_get_tag", &[p], &[types::I32]);

        // Random number generation
        self.declare_rt("knot_random_int", &[p], &[p]);
        self.declare_rt("knot_random_float", &[], &[p]);
        self.declare_rt("knot_random_uuid", &[], &[p]);

        // Elliptic curve cryptography
        self.declare_rt("knot_crypto_generate_key_pair", &[], &[p]);
        self.declare_rt("knot_crypto_generate_signing_key_pair", &[], &[p]);
        self.declare_rt("knot_crypto_generate_key_pair_io", &[], &[p]);
        self.declare_rt("knot_crypto_generate_signing_key_pair_io", &[], &[p]);
        self.declare_rt("knot_crypto_encrypt", &[p, p], &[p]);
        self.declare_rt("knot_crypto_encrypt_io", &[p, p], &[p]);
        self.declare_rt("knot_crypto_decrypt", &[p, p], &[p]);
        self.declare_rt("knot_crypto_sign", &[p, p], &[p]);
        self.declare_rt("knot_crypto_verify", &[p, p, p, p], &[p]);

        self.declare_rt("knot_now", &[], &[p]);

        // Subset constraints
        self.declare_rt("knot_constraint_register", &[p, p, p, p, p, p, p, p, p], &[]);
        // Refinement validation
        // Result monad
        self.declare_rt("knot_result_bind", &[p, p, p], &[p]);
        self.declare_rt("knot_result_yield", &[p], &[p]);
        self.declare_rt("knot_result_empty", &[], &[p]);
        self.declare_rt("knot_refinement_validate_relation", &[p, p, p, p, p, p, p], &[]);
        self.declare_rt("knot_route_set_field_refinement", &[p, p, p, p, p, p, p, p], &[]);
        self.declare_rt("knot_route_set_rate_limit", &[p, p, p, p], &[]);

        // Monadic bind for relations (do-desugaring)
        self.declare_rt("knot_relation_bind", &[p, p, p], &[p]);

        // GroupBy: group relation by key columns using SQLite ORDER BY
        self.declare_rt("knot_relation_group_by", &[p, p, p, p, p, p], &[p]);

        // Standard library: relation operations
        self.declare_rt("knot_relation_filter", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_match", &[p, p], &[p]);
        self.declare_rt("knot_relation_sort_by", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_take", &[p, p], &[p]);
        self.declare_rt("knot_relation_drop", &[p, p], &[p]);
        self.declare_rt("knot_source_match", &[p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_relation_map", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_ap", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_fold", &[p, p, p, p], &[p]);
        self.declare_rt("knot_relation_traverse", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_traverse_kind", &[p, p, p, p, p], &[p]);
        self.declare_rt("knot_relation_single", &[p], &[p]);
        self.declare_rt("knot_relation_any", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_all", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_diff", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_inter", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_sum", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_sum_typed", &[p, p, p, types::I64], &[p]);
        self.declare_rt("knot_relation_sum_direct", &[p, p, types::I64], &[p]);
        self.declare_rt("knot_relation_avg", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_min", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_max", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_count_where", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_upsert_by", &[p, p, p, p], &[p]);

        // Standard library: text operations
        self.declare_rt("knot_text_to_upper", &[p], &[p]);
        self.declare_rt("knot_text_to_lower", &[p], &[p]);
        self.declare_rt("knot_text_take", &[p, p], &[p]);
        self.declare_rt("knot_text_drop", &[p, p], &[p]);
        self.declare_rt("knot_text_length", &[p], &[p]);
        self.declare_rt("knot_text_trim", &[p], &[p]);
        self.declare_rt("knot_text_contains", &[p, p], &[p]);
        self.declare_rt("knot_list_elem", &[p, p], &[p]);
        self.declare_rt("knot_text_reverse", &[p], &[p]);
        self.declare_rt("knot_text_chars", &[p], &[p]);

        // Standard library: utility
        self.declare_rt("knot_value_id", &[p], &[p]);
        self.declare_rt("knot_value_not_fn", &[p], &[p]);

        // Standard library: JSON
        self.declare_rt("knot_json_encode", &[p], &[p]);
        self.declare_rt("knot_json_encode_with", &[p, p, p], &[p]);
        self.declare_rt("knot_json_decode", &[p], &[p]);
        self.declare_rt("knot_json_decode_typed", &[p, p, p], &[p]);
        self.declare_rt("knot_json_decode_maybe", &[p], &[p]);
        self.declare_rt("knot_json_decode_typed_maybe", &[p, p, p], &[p]);
        self.declare_rt("knot_register_to_json", &[p], &[]);
        self.declare_rt("knot_register_ord_compare", &[p], &[]);

        // Bytes value constructor and standard library
        self.declare_rt("knot_value_bytes", &[p, p], &[p]);
        self.declare_rt("knot_bytes_length", &[p], &[p]);
        self.declare_rt("knot_bytes_concat", &[p, p], &[p]);
        self.declare_rt("knot_bytes_slice", &[p, p, p, p], &[p]);
        self.declare_rt("knot_text_to_bytes", &[p], &[p]);
        self.declare_rt("knot_bytes_to_text", &[p], &[p]);
        self.declare_rt("knot_bytes_to_hex", &[p], &[p]);
        self.declare_rt("knot_bytes_from_hex", &[p], &[p]);
        self.declare_rt("knot_bytes_get", &[p, p], &[p]);
        self.declare_rt("knot_hash", &[p], &[p]);

        // Standard library: file system operations
        self.declare_rt("knot_fs_read_file", &[p], &[p]);
        self.declare_rt("knot_fs_write_file", &[p, p], &[p]);
        self.declare_rt("knot_fs_append_file", &[p, p], &[p]);
        self.declare_rt("knot_fs_file_exists", &[p], &[p]);
        self.declare_rt("knot_fs_remove_file", &[p], &[p]);
        self.declare_rt("knot_fs_list_dir", &[p], &[p]);

        // Hash index for equi-join optimization
        self.declare_rt("knot_relation_build_index", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_index_lookup", &[p, p], &[p]);
        self.declare_rt("knot_relation_index_free", &[p], &[]);

        // Fixpoint iteration for recursive derived relations
        self.declare_rt("knot_relation_fixpoint", &[p, p, p], &[p]);

        // IO monad
        self.declare_rt("knot_io_wrap", &[p, p], &[p]);
        self.declare_rt("knot_io_new", &[p, p], &[p]);
        self.declare_rt("knot_io_pure", &[p], &[p]);
        self.declare_rt("knot_io_run", &[p, p], &[p]);
        self.declare_rt("knot_io_bind", &[p, p], &[p]);
        self.declare_rt("knot_io_then", &[p, p], &[p]);
        self.declare_rt("knot_io_map", &[p, p], &[p]);

        // IO wrappers for effectful builtins
        self.declare_rt("knot_println_io", &[p], &[p]);
        self.declare_rt("knot_print_io", &[p], &[p]);
        self.declare_rt("knot_log_info_io", &[p], &[p]);
        self.declare_rt("knot_log_warn_io", &[p], &[p]);
        self.declare_rt("knot_log_error_io", &[p], &[p]);
        self.declare_rt("knot_log_debug_io", &[p], &[p]);
        self.declare_rt("knot_read_line_io", &[], &[p]);
        self.declare_rt("knot_fs_read_file_io", &[p], &[p]);
        self.declare_rt("knot_fs_write_file_io", &[p, p], &[p]);
        self.declare_rt("knot_fs_append_file_io", &[p, p], &[p]);
        self.declare_rt("knot_fs_file_exists_io", &[p], &[p]);
        self.declare_rt("knot_fs_remove_file_io", &[p], &[p]);
        self.declare_rt("knot_fs_list_dir_io", &[p], &[p]);
        self.declare_rt("knot_now_io", &[], &[p]);
        self.declare_rt("knot_sleep_io", &[p], &[p]);
        self.declare_rt("knot_random_int_io", &[p], &[p]);
        self.declare_rt("knot_random_float_io", &[], &[p]);
        self.declare_rt("knot_random_uuid_io", &[], &[p]);

        // Spawn / threading
        self.declare_rt("knot_fork_io", &[p], &[p]);
        self.declare_rt("knot_race_io", &[p, p], &[p]);
        self.declare_rt("knot_threads_join", &[], &[]);

        // STM retry
        self.declare_rt("knot_stm_retry", &[], &[p]);
        self.declare_rt("knot_stm_check_and_clear", &[], &[types::I32]);
        self.declare_rt("knot_stm_skip", &[], &[p]);
        self.declare_rt("knot_stm_check_skip_and_clear", &[], &[types::I32]);
        self.declare_rt("knot_stm_snapshot", &[], &[types::I64]);
        self.declare_rt("knot_stm_wait", &[types::I64], &[]);
        self.declare_rt("knot_stm_push", &[], &[]);
        self.declare_rt("knot_stm_pop_merge", &[], &[]);

        // HTTP server (routes)
        self.declare_rt("knot_route_table_new", &[], &[p]);
        // (table, method, method_len, path, path_len, ctor, ctor_len,
        //  body, body_len, query, query_len, resp, resp_len,
        //  req_hdrs, req_hdrs_len, resp_hdrs, resp_hdrs_len)
        self.declare_rt(
            "knot_route_table_add",
            &[p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p],
            &[],
        );
        self.declare_rt("knot_http_listen", &[p, p, p, p], &[p]);
        // (db, host_value, port_value, route_table, handler) — host is a Value::Text.
        self.declare_rt("knot_http_listen_on", &[p, p, p, p, p], &[p]);
        // IO-thunk builders: same shapes, but return an IO value instead of
        // entering the serve loop (so `fork (listen ...)` works).
        self.declare_rt("knot_http_listen_io", &[p, p, p, p], &[p]);
        self.declare_rt("knot_http_listen_on_io", &[p, p, p, p, p], &[p]);

        // HTTP client (fetch)
        // (base_url, method_ptr, method_len, path_ptr, path_len, payload,
        //  body_ptr, body_len, query_ptr, query_len, resp_ptr, resp_len,
        //  headers, req_hdrs_ptr, req_hdrs_len, resp_hdrs_ptr, resp_hdrs_len)
        self.declare_rt(
            "knot_http_fetch_io",
            &[p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p],
            &[p],
        );

        // OpenAPI / api command
        self.declare_rt("knot_api_register", &[p, p, p], &[]);
        self.declare_rt("knot_api_handle", &[types::I32, p], &[types::I32]);

        // DB explorer TUI
        self.declare_rt("knot_db_handle", &[types::I32, p, p, p], &[types::I32]);

        // Arena GC — per-iteration reset and frame-based isolation
        self.declare_rt("knot_arena_mark", &[], &[p]);
        self.declare_rt("knot_arena_reset_to", &[p], &[]);
        self.declare_rt("knot_arena_promote", &[p], &[p]);
        self.declare_rt("knot_arena_push_frame", &[], &[]);
        self.declare_rt("knot_arena_pop_frame", &[], &[]);
        self.declare_rt("knot_arena_pop_frame_promote", &[p], &[p]);
    }

    fn declare_rt(&mut self, name: &str, params: &[types::Type], returns: &[types::Type]) {
        let mut sig = self.module.make_signature();
        for p in params {
            sig.params.push(AbiParam::new(*p));
        }
        for r in returns {
            sig.returns.push(AbiParam::new(*r));
        }
        let id = self
            .module
            .declare_function(name, Linkage::Import, &sig)
            .unwrap();
        self.runtime_fns.insert(name.to_string(), id);
    }

    /// Register a standard library function as a user_fn.
    /// All stdlib functions are registered as 1-param so they curry properly.
    fn register_stdlib_fn(&mut self, name: &str) {
        // A user redefinition of this name wins — skip the stdlib registration
        // so `user_fns[name]` is free for the user's own declaration.
        if self.user_shadowed_stdlib.contains(name) {
            return;
        }
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // arg1
        sig.returns.push(AbiParam::new(self.ptr_type));
        let func_name = format!("knot_user_{}", name);
        let func_id = self
            .module
            .declare_function(&func_name, Linkage::Local, &sig)
            .unwrap();
        self.user_fns.insert(name.into(), (func_id, 1));
        self.stdlib_fns.insert(name.into());
    }

    /// Declare a helper closure function with the standard (db, env, arg) -> result signature.
    fn declare_closure_fn(&mut self, name: &str) -> FuncId {
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env
        sig.params.push(AbiParam::new(self.ptr_type)); // arg
        sig.returns.push(AbiParam::new(self.ptr_type));
        self.module
            .declare_function(name, Linkage::Local, &sig)
            .unwrap()
    }

    /// Define a 1-param stdlib function that directly delegates to a runtime function.
    fn define_stdlib_fn_1(&mut self, name: &str, rt_name: &str) {
        // User redefinition overrides the stdlib version (never registered).
        if self.user_shadowed_stdlib.contains(name) {
            return;
        }
        let (func_id, _) = self.user_fns[name];
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // arg
        sig.returns.push(AbiParam::new(self.ptr_type));

        let rt_name = rt_name.to_string();
        self.build_function(func_id, sig, |cg, builder, entry| {
            let arg = builder.block_params(entry)[1];
            let result = cg.call_rt(builder, &rt_name, &[arg]);
            builder.ins().return_(&[result]);
        });
    }

    /// Define `sum : [a] -> a` (direct aggregation, no projection). The bare
    /// `sum` value is a 1-param closure over `knot_relation_sum_direct` with
    /// `is_float = 0`; the common `sum rel` application is intercepted at the
    /// call site so the statically inferred element type supplies the
    /// EMPTY-relation zero (Int vs Float). A bare `sum` value passed around
    /// (e.g. `map sum rels`) only hits empty relations as Int, which is the
    /// right zero for the overwhelmingly common `[Int]`/non-empty case.
    fn define_stdlib_sum(&mut self) {
        if self.user_shadowed_stdlib.contains("sum") {
            return;
        }
        let (func_id, _) = self.user_fns["sum"];
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // rel
        sig.returns.push(AbiParam::new(self.ptr_type));

        self.build_function(func_id, sig, |cg, builder, entry| {
            let db = builder.block_params(entry)[0];
            let rel = builder.block_params(entry)[1];
            let is_float = builder.ins().iconst(types::I64, 0);
            let result = cg.call_rt(builder, "knot_relation_sum_direct", &[db, rel, is_float]);
            builder.ins().return_(&[result]);
        });
    }

    /// Define a 2-param stdlib function using currying:
    /// outer(db, arg1) -> Function(inner, arg1, name)  — arg1 passed directly as env
    /// inner(db, env=arg1, arg2) -> rt_fn(db, arg1, arg2)
    fn define_stdlib_fn_2(
        &mut self,
        name: &str,
        rt_name: &str,
        rt_needs_db: bool,
    ) {
        // User redefinition overrides the stdlib version (never registered).
        if self.user_shadowed_stdlib.contains(name) {
            return;
        }
        let inner_id = self.declare_closure_fn(&format!("__stdlib_{}_apply", name));

        // Define the outer function: passes arg1 directly as env (no record allocation)
        let (func_id, _) = self.user_fns[name];
        let mut outer_sig = self.module.make_signature();
        outer_sig.params.push(AbiParam::new(self.ptr_type)); // db
        outer_sig.params.push(AbiParam::new(self.ptr_type)); // arg1
        outer_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(func_id, outer_sig, |cg, builder, entry| {
            let arg1 = builder.block_params(entry)[1];

            // Pass arg1 directly as env — no record wrapping needed
            let inner_ref = cg.module.declare_func_in_func(inner_id, builder.func);
            let fn_addr = builder.ins().func_addr(cg.ptr_type, inner_ref);
            let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
            let result =
                cg.call_rt(builder, "knot_value_function", &[fn_addr, arg1, src_ptr, src_len]);
            builder.ins().return_(&[result]);
        });

        // Define the inner closure: env IS arg1 directly (no record extraction)
        let mut inner_sig = self.module.make_signature();
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // db
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // env = arg1
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // arg2
        inner_sig.returns.push(AbiParam::new(self.ptr_type));

        let rt_name = rt_name.to_string();
        self.build_function(inner_id, inner_sig, |cg, builder, entry| {
            let db = builder.block_params(entry)[0];
            let arg1 = builder.block_params(entry)[1]; // env IS arg1
            let arg2 = builder.block_params(entry)[2];

            let result = if rt_needs_db {
                cg.call_rt(builder, &rt_name, &[db, arg1, arg2])
            } else {
                cg.call_rt(builder, &rt_name, &[arg1, arg2])
            };
            builder.ins().return_(&[result]);
        });
    }

    /// Define a 3-param stdlib function using double currying:
    /// outer(db, arg1) -> Function(middle, {arg1})
    /// middle(db, {arg1}, arg2) -> Function(inner, {arg1, arg2})
    /// inner(db, {arg1, arg2}, arg3) -> rt_fn(db, arg1, arg2, arg3)
    fn define_stdlib_fn_3(
        &mut self,
        name: &str,
        rt_name: &str,
    ) {
        // User redefinition overrides the stdlib version (never registered).
        if self.user_shadowed_stdlib.contains(name) {
            return;
        }
        let middle_id = self.declare_closure_fn(&format!("__stdlib_{}_mid", name));
        let inner_id = self.declare_closure_fn(&format!("__stdlib_{}_apply", name));

        // Outer: passes arg1 directly as env (no record allocation)
        let (func_id, _) = self.user_fns[name];
        let mut outer_sig = self.module.make_signature();
        outer_sig.params.push(AbiParam::new(self.ptr_type));
        outer_sig.params.push(AbiParam::new(self.ptr_type));
        outer_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(func_id, outer_sig, |cg, builder, entry| {
            let arg1 = builder.block_params(entry)[1];

            // Pass arg1 directly as env — no record wrapping needed
            let mid_ref = cg.module.declare_func_in_func(middle_id, builder.func);
            let fn_addr = builder.ins().func_addr(cg.ptr_type, mid_ref);
            let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
            let result =
                cg.call_rt(builder, "knot_value_function", &[fn_addr, arg1, src_ptr, src_len]);
            builder.ins().return_(&[result]);
        });

        // Middle: env IS arg1 directly; captures (arg1, arg2) in record for inner
        let mut mid_sig = self.module.make_signature();
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(middle_id, mid_sig, |cg, builder, entry| {
            let arg1 = builder.block_params(entry)[1]; // env IS arg1
            let arg2 = builder.block_params(entry)[2];

            // Build new env with both args (keys "0","1" are pre-sorted)
            let ptr_bytes = cg.ptr_type.bytes() as i32;
            let slot = builder.create_sized_stack_slot(
                StackSlotData::new(StackSlotKind::ExplicitSlot, (6 * ptr_bytes) as u32, 3),
            );
            let (k0_ptr, k0_len) = cg.string_ptr(builder, "0");
            builder.ins().stack_store(k0_ptr, slot, 0);
            builder.ins().stack_store(k0_len, slot, ptr_bytes);
            builder.ins().stack_store(arg1, slot, 2 * ptr_bytes);
            let (k1_ptr, k1_len) = cg.string_ptr(builder, "1");
            builder.ins().stack_store(k1_ptr, slot, 3 * ptr_bytes);
            builder.ins().stack_store(k1_len, slot, 4 * ptr_bytes);
            builder.ins().stack_store(arg2, slot, 5 * ptr_bytes);
            let data_ptr = builder.ins().stack_addr(cg.ptr_type, slot, 0);
            let count = builder.ins().iconst(cg.ptr_type, 2);
            let env = cg.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count]);

            let inner_ref = cg.module.declare_func_in_func(inner_id, builder.func);
            let fn_addr = builder.ins().func_addr(cg.ptr_type, inner_ref);
            let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
            let result =
                cg.call_rt(builder, "knot_value_function", &[fn_addr, env, src_ptr, src_len]);
            builder.ins().return_(&[result]);
        });

        // Inner: extracts arg1, arg2 from env, calls runtime with all 3 + db
        let mut inner_sig = self.module.make_signature();
        inner_sig.params.push(AbiParam::new(self.ptr_type));
        inner_sig.params.push(AbiParam::new(self.ptr_type));
        inner_sig.params.push(AbiParam::new(self.ptr_type));
        inner_sig.returns.push(AbiParam::new(self.ptr_type));

        let rt_name = rt_name.to_string();
        self.build_function(inner_id, inner_sig, |cg, builder, entry| {
            let db = builder.block_params(entry)[0];
            let env = builder.block_params(entry)[1];
            let arg3 = builder.block_params(entry)[2];

            let idx0 = builder.ins().iconst(cg.ptr_type, 0);
            let arg1 = cg.call_rt(builder, "knot_record_field_by_index", &[env, idx0]);
            let idx1 = builder.ins().iconst(cg.ptr_type, 1);
            let arg2 = cg.call_rt(builder, "knot_record_field_by_index", &[env, idx1]);

            let result = cg.call_rt(builder, &rt_name, &[db, arg1, arg2, arg3]);
            builder.ins().return_(&[result]);
        });
    }

    // ── Declaration collection ────────────────────────────────────

    fn collect_declarations(&mut self, module: &ast::Module) {
        // Register built-in ADT constructors so trait dispatchers can find their
        // ctor lists. Inference treats these as built-in types (see
        // `register_builtins` in infer.rs), so they don't appear as user-source
        // `data` decls. Without this, `impl Functor Maybe` etc. would be silently
        // dropped from the dispatcher lookup at codegen time.
        self.data_constructors.insert(
            "Maybe".into(),
            vec!["Nothing".into(), "Just".into()],
        );
        self.data_constructors.insert(
            "Result".into(),
            vec!["Err".into(), "Ok".into()],
        );
        self.data_constructors.insert(
            "Bool".into(),
            vec!["True".into(), "False".into()],
        );

        // __bind/__yield/__empty are desugared do-block operations that dispatch
        // through Monad/Applicative/Alternative trait impls (see compile_app,
        // compile_monadic_yield, compile_monadic_empty). No standalone user
        // function is registered — dispatch is compile-time via monad_info.

        // A user-defined top-level function shadows any same-named stdlib
        // builtin (compile_app documents this rule). Collect those names up
        // front so we neither register nor define the stdlib version for them —
        // the user's declaration then flows through the normal function path
        // and `user_fns[name]` ends up pointing at the user's code. Without
        // this, the program type-checks against the user's semantics (inference
        // binds the user decl after `register_builtins`) but silently runs the
        // stdlib's.
        self.user_shadowed_stdlib = module
            .decls
            .iter()
            .filter_map(|decl| match &decl.node {
                ast::DeclKind::Fun { name, body: Some(_), .. } => Some(name.clone()),
                _ => None,
            })
            .collect();

        // Register standard library functions (all as 1-param for proper currying)
        // map and fold are now trait methods (Functor.map, Foldable.fold)
        // with [] impls registered directly in register_builtin_relation_impls.
        let stdlib_names = [
            "filter", "match", "single", "any", "all", "diff", "inter", "sum", "avg",
            "minOn", "maxOn", "countWhere",
            "toUpper", "toLower", "sortBy",
            "length", "trim", "contains", "elem", "reverse",
            "chars", "id", "not",
            "stripUnit", "withUnit", "stripFloatUnit", "withFloatUnit",
            "bytesLength", "bytesSlice", "bytesConcat",
            "textToBytes", "bytesToText", "bytesToHex", "bytesFromHex", "hexDecode",
            "bytesGet", "hash",
            "readFile", "writeFile", "appendFile",
            "fileExists", "removeFile", "listDir",
            "randomInt", "sleep", "fork", "race",
            "encrypt", "decrypt", "sign", "verify",
            "upsertBy",
        ];
        for name in &stdlib_names {
            self.register_stdlib_fn(name);
        }

        // Composite route declarations (`route Name = A | B`), resolved to a
        // fixpoint after the declaration loop (see below).
        let mut composite_routes: Vec<(String, Vec<String>)> = Vec::new();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body: Some(body), .. } => {
                    {
                        // Skip user functions that shadow stdlib builtins —
                        // the stdlib version is already registered.
                        if self.user_fns.contains_key(name.as_str()) {
                            continue;
                        }
                        // If the body is a lambda, extract its params for direct-call optimization.
                        let n_params = match &body.node {
                            ast::ExprKind::Lambda { params, .. } => params.len(),
                            _ => 0,
                        };
                        let mut sig = self.module.make_signature();
                        sig.params.push(AbiParam::new(self.ptr_type)); // db
                        for _ in 0..n_params {
                            sig.params.push(AbiParam::new(self.ptr_type));
                        }
                        sig.returns.push(AbiParam::new(self.ptr_type));
                        let func_name = format!("knot_user_{}", name);
                        let func_id = self
                            .module
                            .declare_function(&func_name, Linkage::Local, &sig)
                            .unwrap();
                        self.user_fns.insert(name.clone(), (func_id, n_params));
                        self.fun_bodies.insert(name.clone(), body.clone());
                    }
                }
                ast::DeclKind::Derived { name, body, .. } => {
                    // Derived relations are 0-param functions (only db param)
                    let mut sig = self.module.make_signature();
                    sig.params.push(AbiParam::new(self.ptr_type)); // db
                    sig.returns.push(AbiParam::new(self.ptr_type));
                    let func_name = format!("knot_user_{}", name);
                    let func_id = self
                        .module
                        .declare_function(&func_name, Linkage::Local, &sig)
                        .unwrap();
                    self.user_fns.insert(name.clone(), (func_id, 0));

                    // Detect self-referencing (recursive) derived relations
                    if expr_contains_derived_ref(body, name) {
                        self.recursive_derived.insert(name.clone());

                        // Declare body function: (db, self_val) -> result
                        let mut body_sig = self.module.make_signature();
                        body_sig.params.push(AbiParam::new(self.ptr_type)); // db
                        body_sig.params.push(AbiParam::new(self.ptr_type)); // self_val
                        body_sig.returns.push(AbiParam::new(self.ptr_type));
                        let body_func_name = format!("knot_user_{}_body", name);
                        let body_func_id = self
                            .module
                            .declare_function(&body_func_name, Linkage::Local, &body_sig)
                            .unwrap();
                        self.recursive_body_fns.insert(name.clone(), body_func_id);
                    }
                }
                ast::DeclKind::Data {
                    name,
                    constructors: ctors,
                    ..
                } => {
                    let ctor_names: Vec<String> =
                        ctors.iter().map(|c| c.name.clone()).collect();
                    self.data_constructors.insert(name.clone(), ctor_names);

                    // NOTE: nullable pointer encoding for Maybe-isomorphic types
                    // (2 constructors, one nullary, one with fields) is disabled.
                    // The runtime reconstructs ADT values from SQLite as
                    // Constructor(tag, payload) which is always non-null, creating
                    // a representation mismatch with null-encoded in-memory values
                    // that breaks equality and pattern matching.
                }
                ast::DeclKind::Trait {
                    name: trait_name,
                    params,
                    supertraits,
                    items,
                } => {
                    // Extract HKT param name (e.g., "f" from `(f : Type -> Type)`)
                    let hkt_param_name: Option<String> = params.iter().find_map(|p| {
                        if p.kind.is_some() {
                            Some(p.name.clone())
                        } else {
                            None
                        }
                    });
                    // Extract regular type param name (e.g., "a" from `Eq a`)
                    let type_param_name: Option<String> = params.iter().find_map(|p| {
                        if p.kind.is_none() {
                            Some(p.name.clone())
                        } else {
                            None
                        }
                    });
                    // Store supertrait relationships
                    let supertrait_names: Vec<String> = supertraits
                        .iter()
                        .map(|c| c.trait_name.clone())
                        .collect();
                    self.trait_supertraits
                        .insert(trait_name.clone(), supertrait_names);

                    let mut defaults = HashMap::new();
                    let mut assoc_type_names = Vec::new();
                    for item in items {
                        match item {
                            ast::TraitItem::Method {
                                name: method_name,
                                ty,
                                default_params,
                                default_body,
                                ..
                            } => {
                                // Defaults written as `m = \a b -> ...` carry
                                // their params on the lambda — unwrap so the
                                // dispatcher signature matches call sites.
                                let norm_default: Option<(Vec<ast::Pat>, ast::Expr)> =
                                    default_body.as_ref().map(|body| {
                                        let (p, b) =
                                            method_params_body(default_params, body);
                                        (p, b.clone())
                                    });
                                let param_count = if let Some((p, _)) = &norm_default {
                                    p.len()
                                } else {
                                    count_fn_params(&ty.ty)
                                };
                                let dispatch_index = find_dispatch_index(
                                    hkt_param_name.as_deref(),
                                    type_param_name.as_deref(),
                                    &ty.ty,
                                );
                                self.trait_method_traits.insert(
                                    method_name.clone(),
                                    trait_name.clone(),
                                );
                                self.trait_methods
                                    .entry(method_name.clone())
                                    .and_modify(|info| {
                                        info.param_count = param_count;
                                        info.dispatch_index = dispatch_index;
                                    })
                                    .or_insert(TraitMethodInfo {
                                        param_count,
                                        dispatch_index,
                                        impls: Vec::new(),
                                    });
                                if let Some((params, body)) = norm_default {
                                    defaults.insert(
                                        method_name.clone(),
                                        DefaultMethod { params, body },
                                    );
                                }
                            }
                            ast::TraitItem::AssociatedType {
                                name, ..
                            } => {
                                assoc_type_names.push(name.clone());
                            }
                        }
                    }
                    self.trait_defs.insert(
                        trait_name.clone(),
                        TraitDef {
                            defaults,
                            associated_types: assoc_type_names,
                        },
                    );
                }
                ast::DeclKind::Impl {
                    trait_name,
                    args,
                    items,
                    ..
                } => {
                    if let Some(type_name) = impl_type_name(args) {
                        // Collect names of methods explicitly provided in this impl
                        let provided_methods: Vec<String> = items
                            .iter()
                            .filter_map(|item| {
                                if let ast::ImplItem::Method { name, .. } = item {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for item in items {
                            if let ast::ImplItem::Method {
                                name: method_name,
                                params,
                                body,
                                ..
                            } = item
                            {
                                // Methods written as constants bound to
                                // lambdas (`eq = \a b -> ...`) must count the
                                // lambda's params, mirroring top-level Funs.
                                let (params, _) = method_params_body(params, body);
                                let n_params = params.len();
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, method_name
                                );
                                let mut sig = self.module.make_signature();
                                sig.params.push(AbiParam::new(self.ptr_type));
                                for _ in 0..n_params {
                                    sig.params.push(AbiParam::new(self.ptr_type));
                                }
                                sig.returns.push(AbiParam::new(self.ptr_type));
                                let func_name = format!("knot_user_{}", mangled);
                                let func_id = self
                                    .module
                                    .declare_function(
                                        &func_name,
                                        Linkage::Local,
                                        &sig,
                                    )
                                    .unwrap();
                                self.user_fns
                                    .insert(mangled.clone(), (func_id, n_params));

                                self.trait_methods
                                    .entry(method_name.clone())
                                    .or_insert(TraitMethodInfo {
                                        param_count: n_params,
                                        dispatch_index: None,
                                        impls: Vec::new(),
                                    })
                                    .impls
                                    .push(ImplEntry {
                                        type_name: type_name.clone(),
                                        func_id,
                                        is_builtin: false,
                                    });
                            }
                        }

                        // Auto-declare functions for default methods not provided
                        if let Some(trait_def) = self.trait_defs.get(trait_name) {
                            let defaults_to_add: Vec<(String, DefaultMethod)> = trait_def
                                .defaults
                                .iter()
                                .filter(|(method_name, _)| {
                                    !provided_methods.contains(method_name)
                                })
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            for (method_name, default) in defaults_to_add {
                                let n_params = default.params.len();
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, method_name
                                );
                                let mut sig = self.module.make_signature();
                                sig.params.push(AbiParam::new(self.ptr_type));
                                for _ in 0..n_params {
                                    sig.params.push(AbiParam::new(self.ptr_type));
                                }
                                sig.returns.push(AbiParam::new(self.ptr_type));
                                let func_name = format!("knot_user_{}", mangled);
                                let func_id = self
                                    .module
                                    .declare_function(
                                        &func_name,
                                        Linkage::Local,
                                        &sig,
                                    )
                                    .unwrap();
                                self.user_fns
                                    .insert(mangled.clone(), (func_id, n_params));

                                self.trait_methods
                                    .entry(method_name.clone())
                                    .or_insert(TraitMethodInfo {
                                        param_count: n_params,
                                        dispatch_index: None,
                                        impls: Vec::new(),
                                    })
                                    .impls
                                    .push(ImplEntry {
                                        type_name: type_name.clone(),
                                        func_id,
                                        is_builtin: false,
                                    });
                            }
                        }

                        // Track this impl for supertrait validation
                        self.trait_impl_types
                            .entry(trait_name.clone())
                            .or_default()
                            .push((type_name.clone(), decl.span));
                    }
                }
                ast::DeclKind::Route { name, entries } => {
                    self.route_entries.insert(name.clone(), entries.clone());
                    // Last route entry (in source declaration order) with a
                    // given constructor name wins, matching infer's fetch
                    // metadata resolution so typecheck and codegen agree on
                    // which route a fetch compiles against (B38).
                    for entry in entries {
                        self.fetch_route_entries
                            .insert(entry.constructor.clone(), entry.clone());
                    }
                }
                ast::DeclKind::RouteComposite { name, components } => {
                    // Deferred: resolved to a fixpoint after the loop so
                    // composition is order-independent (a composite may
                    // reference a route declared later in the file).
                    composite_routes.push((name.clone(), components.clone()));
                }
                _ => {}
            }
        }

        // Resolve composite routes to a fixpoint. The type checker already
        // rejects unknown components and cycles; the bounded loop below
        // simply stops when no further composite can be expanded.
        let mut passes = composite_routes.len() + 1;
        while !composite_routes.is_empty() && passes > 0 {
            passes -= 1;
            let mut still_pending = Vec::new();
            for (name, components) in composite_routes {
                if components
                    .iter()
                    .all(|c| self.route_entries.contains_key(c))
                {
                    let mut all = Vec::new();
                    for comp in &components {
                        all.extend_from_slice(&self.route_entries[comp]);
                    }
                    self.route_entries.insert(name, all);
                } else {
                    still_pending.push((name, components));
                }
            }
            composite_routes = still_pending;
        }
        // Anything left references unknown or cyclic routes (already
        // diagnosed by inference) — register what resolves so downstream
        // lookups don't panic.
        for (name, components) in composite_routes {
            let mut all = Vec::new();
            for comp in &components {
                if let Some(entries) = self.route_entries.get(comp) {
                    all.extend_from_slice(entries);
                }
            }
            self.route_entries.insert(name, all);
        }

        // Detect user functions that produce IO values (fixed-point iteration)
        self.detect_io_functions(&module.decls);
        self.detect_passthrough_functions(&module.decls);
        self.detect_write_functions(&module.decls);

        // Process deriving clauses: auto-generate impl methods from trait defaults
        for decl in &module.decls {
            if let ast::DeclKind::Data {
                name: type_name,
                deriving,
                ..
            } = &decl.node
            {
                for trait_name in deriving {
                    if let Some(trait_def) = self.trait_defs.get(trait_name) {
                        let defaults_to_derive: Vec<(&String, &DefaultMethod)> = trait_def
                            .defaults
                            .iter()
                            .collect();
                        for (method_name, default) in defaults_to_derive {
                            let mangled = format!(
                                "{}_{}_{}", trait_name, type_name, method_name
                            );
                            // Skip if already declared (explicit impl takes priority)
                            if self.user_fns.contains_key(&mangled) {
                                continue;
                            }
                            let n_params = default.params.len();
                            let mut sig = self.module.make_signature();
                            sig.params.push(AbiParam::new(self.ptr_type));
                            for _ in 0..n_params {
                                sig.params.push(AbiParam::new(self.ptr_type));
                            }
                            sig.returns.push(AbiParam::new(self.ptr_type));
                            let func_name = format!("knot_user_{}", mangled);
                            let func_id = self
                                .module
                                .declare_function(&func_name, Linkage::Local, &sig)
                                .unwrap();
                            self.user_fns
                                .insert(mangled.clone(), (func_id, n_params));

                            self.trait_methods
                                .entry(method_name.clone())
                                .or_insert(TraitMethodInfo {
                                    param_count: n_params,
                                    dispatch_index: None,
                                    impls: Vec::new(),
                                })
                                .impls
                                .push(ImplEntry {
                                    type_name: type_name.clone(),
                                    func_id,
                                    is_builtin: false,
                                });

                            self.derived_methods.push(DerivedMethodDef {
                                mangled,
                                default: default.clone(),
                            });
                        }
                    }

                    // Track derived impl for supertrait validation
                    self.trait_impl_types
                        .entry(trait_name.clone())
                        .or_default()
                        .push((type_name.clone(), decl.span));
                }
            }
        }

        // Register built-in [] impls for HKT traits (Functor, Applicative, Monad, Foldable)
        // These are registered directly in codegen to avoid span collision issues
        // with base-parsed source.
        self.register_builtin_relation_impls();

        // Register built-in IO impls for Functor, Applicative, Monad
        self.register_builtin_io_impls();

        // Register built-in primitive impls for Eq, Ord, Num traits.
        // These delegate to runtime functions to avoid circular dependencies
        // (e.g. `impl Eq Int where eq a b = a == b` would loop if == dispatches through eq).
        self.register_builtin_primitive_impls();

        // Validate supertrait constraints
        self.validate_supertraits();

        // Create dispatcher functions for trait methods
        // (skip methods that collide with user-defined functions)
        let dispatchers: Vec<(String, usize)> = self
            .trait_methods
            .iter()
            .filter(|(name, info)| {
                let has_fallback = has_trait_fallback(name);
                (!info.impls.is_empty() || has_fallback) && !self.user_fns.contains_key(name.as_str())
            })
            .map(|(name, info)| (name.clone(), info.param_count))
            .collect();
        for (method_name, param_count) in dispatchers {
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..param_count {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));
            let func_name = format!("knot_user_{}", method_name);
            let func_id = self
                .module
                .declare_function(&func_name, Linkage::Local, &sig)
                .unwrap();
            self.user_fns
                .insert(method_name.clone(), (func_id, param_count));
            self.trait_dispatcher_fns
                .insert(method_name, func_id);
        }
    }

    // ── Built-in [] impls for HKT traits ─────────────────────────

    /// Register mangled functions for Functor/Applicative/Monad/Alternative/Foldable [] impls.
    /// Called from `collect_declarations` after user impls are processed.
    fn register_builtin_relation_impls(&mut self) {
        // (mangled_name, trait_method_name, n_user_params)
        let impls = [
            ("Functor_Relation_map", "map", 2),
            ("Applicative_Relation_yield", "yield", 1),
            ("Applicative_Relation_ap", "ap", 2),
            ("Monad_Relation_bind", "bind", 2),
            ("Alternative_Relation_empty", "empty", 0),
            ("Alternative_Relation_alt", "alt", 2),
            ("Foldable_Relation_fold", "fold", 3),
            ("Traversable_Relation_traverse", "traverse", 2),
            ("Semigroup_Relation_append", "append", 2),
            ("Sequence_Relation_take", "take", 2),
            ("Sequence_Relation_drop", "drop", 2),
        ];
        for (mangled, method_name, n_params) in &impls {
            // Don't register if already defined (by user impl or prelude)
            if self.user_fns.contains_key(*mangled) {
                continue;
            }
            let already_has_relation_impl = self
                .trait_methods
                .get(*method_name)
                .map(|info| info.impls.iter().any(|e| e.type_name == "Relation"))
                .unwrap_or(false);
            if already_has_relation_impl {
                continue;
            }

            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..*n_params {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));
            let func_name = format!("knot_user_{}", mangled);
            let func_id = self
                .module
                .declare_function(&func_name, Linkage::Local, &sig)
                .unwrap();
            self.user_fns
                .insert(mangled.to_string(), (func_id, *n_params));
            self.registered_builtin_impls.insert(mangled.to_string());

            self.trait_methods
                .entry(method_name.to_string())
                .or_insert(TraitMethodInfo {
                    param_count: *n_params,
                    dispatch_index: None,
                    impls: Vec::new(),
                })
                .impls
                .push(ImplEntry {
                    type_name: "Relation".to_string(),
                    func_id,
                    is_builtin: true,
                });

            // Track for supertrait validation
            self.trait_impl_types
                .entry(match *method_name {
                    "map" => "Functor".to_string(),
                    "yield" | "ap" => "Applicative".to_string(),
                    "bind" => "Monad".to_string(),
                    "empty" | "alt" => "Alternative".to_string(),
                    "fold" => "Foldable".to_string(),
                    "traverse" => "Traversable".to_string(),
                    "append" => "Semigroup".to_string(),
                    "take" | "drop" => "Sequence".to_string(),
                    _ => continue,
                })
                .or_default()
                .push(("Relation".to_string(), ast::Span { start: 0, end: 0 }));
        }
    }

    // ── Built-in IO impls for HKT traits ─────────────────────────

    /// Register mangled functions for Functor/Applicative/Monad IO impls.
    fn register_builtin_io_impls(&mut self) {
        let impls = [
            ("Functor_IO_map", "map", 2),
            ("Applicative_IO_yield", "yield", 1),
            ("Monad_IO_bind", "bind", 2),
        ];
        for (mangled, method_name, n_params) in &impls {
            if self.user_fns.contains_key(*mangled) {
                continue;
            }
            let already_has_io_impl = self
                .trait_methods
                .get(*method_name)
                .map(|info| info.impls.iter().any(|e| e.type_name == "IO"))
                .unwrap_or(false);
            if already_has_io_impl {
                continue;
            }

            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..*n_params {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));
            let func_name = format!("knot_user_{}", mangled);
            let func_id = self
                .module
                .declare_function(&func_name, Linkage::Local, &sig)
                .unwrap();
            self.user_fns
                .insert(mangled.to_string(), (func_id, *n_params));
            self.registered_builtin_impls.insert(mangled.to_string());

            self.trait_methods
                .entry(method_name.to_string())
                .or_insert(TraitMethodInfo {
                    param_count: *n_params,
                    dispatch_index: None,
                    impls: Vec::new(),
                })
                .impls
                .push(ImplEntry {
                    type_name: "IO".to_string(),
                    func_id,
                    is_builtin: true,
                });

            self.trait_impl_types
                .entry(match *method_name {
                    "map" => "Functor".to_string(),
                    "yield" | "ap" => "Applicative".to_string(),
                    "bind" => "Monad".to_string(),
                    _ => continue,
                })
                .or_default()
                .push(("IO".to_string(), ast::Span { start: 0, end: 0 }));
        }
    }

    /// Register built-in primitive impls for Eq, Ord, Num traits.
    /// These delegate directly to runtime functions, avoiding circular dependencies.
    fn register_builtin_primitive_impls(&mut self) {
        // (mangled_name, trait_method_name, type_name, n_user_params, trait_name)
        let impls = [
            // Eq impls
            ("Eq_Int_eq", "eq", "Int", 2, "Eq"),
            ("Eq_Float_eq", "eq", "Float", 2, "Eq"),
            ("Eq_Text_eq", "eq", "Text", 2, "Eq"),
            ("Eq_Bool_eq", "eq", "Bool", 2, "Eq"),
            // Ord impls
            ("Ord_Int_compare", "compare", "Int", 2, "Ord"),
            ("Ord_Float_compare", "compare", "Float", 2, "Ord"),
            ("Ord_Text_compare", "compare", "Text", 2, "Ord"),
            // Num impls
            ("Num_Int_add", "add", "Int", 2, "Num"),
            ("Num_Int_sub", "sub", "Int", 2, "Num"),
            ("Num_Int_mul", "mul", "Int", 2, "Num"),
            ("Num_Int_div", "div", "Int", 2, "Num"),
            ("Num_Int_mod", "mod", "Int", 2, "Num"),
            ("Num_Int_negate", "negate", "Int", 1, "Num"),
            ("Num_Float_add", "add", "Float", 2, "Num"),
            ("Num_Float_sub", "sub", "Float", 2, "Num"),
            ("Num_Float_mul", "mul", "Float", 2, "Num"),
            ("Num_Float_div", "div", "Float", 2, "Num"),
            ("Num_Float_mod", "mod", "Float", 2, "Num"),
            ("Num_Float_negate", "negate", "Float", 1, "Num"),
            // Semigroup impls
            ("Semigroup_Text_append", "append", "Text", 2, "Semigroup"),
            // Sequence impls
            ("Sequence_Text_take", "take", "Text", 2, "Sequence"),
            ("Sequence_Text_drop", "drop", "Text", 2, "Sequence"),
        ];
        for (mangled, method_name, type_name, n_params, trait_name) in &impls {
            // Don't register if already defined (by user impl or prelude)
            if self.user_fns.contains_key(*mangled) {
                continue;
            }
            let already_has_impl = self
                .trait_methods
                .get(*method_name)
                .map(|info| info.impls.iter().any(|e| e.type_name == *type_name))
                .unwrap_or(false);
            if already_has_impl {
                continue;
            }

            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..*n_params {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));
            let func_name = format!("knot_user_{}", mangled);
            let func_id = self
                .module
                .declare_function(&func_name, Linkage::Local, &sig)
                .unwrap();
            self.user_fns
                .insert(mangled.to_string(), (func_id, *n_params));
            self.registered_builtin_impls.insert(mangled.to_string());

            self.trait_methods
                .entry(method_name.to_string())
                .or_insert(TraitMethodInfo {
                    param_count: *n_params,
                    dispatch_index: None,
                    impls: Vec::new(),
                })
                .impls
                .push(ImplEntry {
                    type_name: type_name.to_string(),
                    func_id,
                    is_builtin: true,
                });

            // Track for supertrait validation
            self.trait_impl_types
                .entry(trait_name.to_string())
                .or_default()
                .push((type_name.to_string(), ast::Span { start: 0, end: 0 }));
        }
    }

    /// Define Cranelift IR bodies for built-in [] impls of HKT traits.
    /// Only defines impls that were actually registered by `register_builtin_relation_impls`
    /// (not those already provided by user code or the prelude).
    fn define_builtin_relation_impls(&mut self) {
        // Helper macro: only define if this impl was registered by the builtin path
        macro_rules! define_if_registered {
            ($name:expr, $body:expr) => {
                if self.registered_builtin_impls.contains($name) {
                    if let Some(&(func_id, _)) = self.user_fns.get($name) {
                        $body(self, func_id);
                    }
                }
            };
        }

        // Functor_Relation_map(db, f, rel) → knot_relation_map(db, f, rel)
        define_if_registered!("Functor_Relation_map", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let f = builder.block_params(entry)[1];
                let rel = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_map", &[db, f, rel]);
                builder.ins().return_(&[result]);
            });
        });

        // Applicative_Relation_yield(db, x) → knot_relation_singleton(x)
        define_if_registered!("Applicative_Relation_yield", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // x
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let x = builder.block_params(entry)[1];
                let result = cg.call_rt(builder, "knot_relation_singleton", &[x]);
                builder.ins().return_(&[result]);
            });
        });

        // Applicative_Relation_ap(db, fs, xs) → knot_relation_ap(db, fs, xs)
        define_if_registered!("Applicative_Relation_ap", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // fs
            sig.params.push(AbiParam::new(cg.ptr_type)); // xs
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let fs = builder.block_params(entry)[1];
                let xs = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_ap", &[db, fs, xs]);
                builder.ins().return_(&[result]);
            });
        });

        // Monad_Relation_bind(db, f, rel) → knot_relation_bind(db, f, rel)
        define_if_registered!("Monad_Relation_bind", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let f = builder.block_params(entry)[1];
                let rel = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_bind", &[db, f, rel]);
                builder.ins().return_(&[result]);
            });
        });

        // Alternative_Relation_empty(db) → knot_relation_empty()
        define_if_registered!("Alternative_Relation_empty", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, _entry| {
                let result = cg.call_rt(builder, "knot_relation_empty", &[]);
                builder.ins().return_(&[result]);
            });
        });

        // Alternative_Relation_alt(db, a, b) → knot_relation_union(db, a, b)
        define_if_registered!("Alternative_Relation_alt", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // a
            sig.params.push(AbiParam::new(cg.ptr_type)); // b
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let a = builder.block_params(entry)[1];
                let b = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_union", &[db, a, b]);
                builder.ins().return_(&[result]);
            });
        });

        // Foldable_Relation_fold(db, f, init, rel) → knot_relation_fold(db, f, init, rel)
        define_if_registered!("Foldable_Relation_fold", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // init
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let f = builder.block_params(entry)[1];
                let init = builder.block_params(entry)[2];
                let rel = builder.block_params(entry)[3];
                let result = cg.call_rt(builder, "knot_relation_fold", &[db, f, init, rel]);
                builder.ins().return_(&[result]);
            });
        });

        // Traversable_Relation_traverse(db, f, rel) → knot_relation_traverse(db, f, rel)
        define_if_registered!("Traversable_Relation_traverse", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let f = builder.block_params(entry)[1];
                let rel = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_traverse", &[db, f, rel]);
                builder.ins().return_(&[result]);
            });
        });

        // Semigroup_Relation_append(db, a, b) → knot_relation_union(db, a, b)
        define_if_registered!("Semigroup_Relation_append", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // a
            sig.params.push(AbiParam::new(cg.ptr_type)); // b
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let a = builder.block_params(entry)[1];
                let b = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_union", &[db, a, b]);
                builder.ins().return_(&[result]);
            });
        });

        // Sequence_Relation_take(db, n, rel) → knot_relation_take(n, rel)
        define_if_registered!("Sequence_Relation_take", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // n
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let n = builder.block_params(entry)[1];
                let rel = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_take", &[n, rel]);
                builder.ins().return_(&[result]);
            });
        });

        // Sequence_Relation_drop(db, n, rel) → knot_relation_drop(n, rel)
        define_if_registered!("Sequence_Relation_drop", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // n
            sig.params.push(AbiParam::new(cg.ptr_type)); // rel
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let n = builder.block_params(entry)[1];
                let rel = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_relation_drop", &[n, rel]);
                builder.ins().return_(&[result]);
            });
        });
    }

    /// Define Cranelift IR bodies for built-in IO impls of HKT traits.
    fn define_builtin_io_impls(&mut self) {
        macro_rules! define_if_registered {
            ($name:expr, $body:expr) => {
                if self.registered_builtin_impls.contains($name) {
                    if let Some(&(func_id, _)) = self.user_fns.get($name) {
                        $body(self, func_id);
                    }
                }
            };
        }

        // Functor_IO_map(db, f, io) → knot_io_map(f, io)
        define_if_registered!("Functor_IO_map", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // io
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let f = builder.block_params(entry)[1];
                let io = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_io_map", &[f, io]);
                builder.ins().return_(&[result]);
            });
        });

        // Applicative_IO_yield(db, x) → knot_io_pure(x)
        define_if_registered!("Applicative_IO_yield", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // x
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let x = builder.block_params(entry)[1];
                let result = cg.call_rt(builder, "knot_io_pure", &[x]);
                builder.ins().return_(&[result]);
            });
        });

        // Monad_IO_bind(db, f, io) → knot_io_bind(io, f)
        define_if_registered!("Monad_IO_bind", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // f
            sig.params.push(AbiParam::new(cg.ptr_type)); // io
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let f = builder.block_params(entry)[1];
                let io = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_io_bind", &[io, f]);
                builder.ins().return_(&[result]);
            });
        });
    }

    /// Define Cranelift IR bodies for built-in primitive impls (Eq, Ord, Num).
    /// Each impl delegates to the corresponding runtime function.
    fn define_builtin_primitive_impls(&mut self) {
        macro_rules! define_if_registered {
            ($name:expr, $body:expr) => {
                if self.registered_builtin_impls.contains($name) {
                    if let Some(&(func_id, _)) = self.user_fns.get($name) {
                        $body(self, func_id);
                    }
                }
            };
        }

        // Helper: define a 2-param impl that delegates to a runtime function
        // Signature: (db, a, b) → rt_fn(a, b)
        macro_rules! define_binop_impl {
            ($mangled:expr, $rt_fn:expr) => {
                define_if_registered!($mangled, |cg: &mut Self, func_id: FuncId| {
                    let mut sig = cg.module.make_signature();
                    sig.params.push(AbiParam::new(cg.ptr_type)); // db
                    sig.params.push(AbiParam::new(cg.ptr_type)); // a
                    sig.params.push(AbiParam::new(cg.ptr_type)); // b
                    sig.returns.push(AbiParam::new(cg.ptr_type));
                    cg.build_function(func_id, sig, |cg, builder, entry| {
                        let a = builder.block_params(entry)[1];
                        let b = builder.block_params(entry)[2];
                        let result = cg.call_rt(builder, $rt_fn, &[a, b]);
                        builder.ins().return_(&[result]);
                    });
                });
            };
        }

        // Helper: define a 1-param impl that delegates to a runtime function
        // Signature: (db, a) → rt_fn(a)
        macro_rules! define_unop_impl {
            ($mangled:expr, $rt_fn:expr) => {
                define_if_registered!($mangled, |cg: &mut Self, func_id: FuncId| {
                    let mut sig = cg.module.make_signature();
                    sig.params.push(AbiParam::new(cg.ptr_type)); // db
                    sig.params.push(AbiParam::new(cg.ptr_type)); // a
                    sig.returns.push(AbiParam::new(cg.ptr_type));
                    cg.build_function(func_id, sig, |cg, builder, entry| {
                        let a = builder.block_params(entry)[1];
                        let result = cg.call_rt(builder, $rt_fn, &[a]);
                        builder.ins().return_(&[result]);
                    });
                });
            };
        }

        // Eq impls: eq(a, b) → knot_value_eq(a, b)
        define_binop_impl!("Eq_Int_eq", "knot_value_eq");
        define_binop_impl!("Eq_Float_eq", "knot_value_eq");
        define_binop_impl!("Eq_Text_eq", "knot_value_eq");
        define_binop_impl!("Eq_Bool_eq", "knot_value_eq");

        // Ord impls: compare(a, b) → knot_value_compare(a, b)
        define_binop_impl!("Ord_Int_compare", "knot_value_compare");
        define_binop_impl!("Ord_Float_compare", "knot_value_compare");
        define_binop_impl!("Ord_Text_compare", "knot_value_compare");

        // Num impls: add/sub/mul/div(a, b) → knot_value_add/sub/mul/div(a, b)
        define_binop_impl!("Num_Int_add", "knot_value_add");
        define_binop_impl!("Num_Int_sub", "knot_value_sub");
        define_binop_impl!("Num_Int_mul", "knot_value_mul");
        define_binop_impl!("Num_Int_div", "knot_value_div");
        define_binop_impl!("Num_Int_mod", "knot_value_mod");
        define_unop_impl!("Num_Int_negate", "knot_value_negate");

        define_binop_impl!("Num_Float_add", "knot_value_add");
        define_binop_impl!("Num_Float_sub", "knot_value_sub");
        define_binop_impl!("Num_Float_mul", "knot_value_mul");
        define_binop_impl!("Num_Float_div", "knot_value_div");
        define_binop_impl!("Num_Float_mod", "knot_value_mod");
        define_unop_impl!("Num_Float_negate", "knot_value_negate");

        // Semigroup impls: append(a, b) → knot_value_concat(a, b)
        define_binop_impl!("Semigroup_Text_append", "knot_value_concat");

        // Sequence impls for Text: take/drop(n, text) → knot_text_take/drop(n, text)
        define_binop_impl!("Sequence_Text_take", "knot_text_take");
        define_binop_impl!("Sequence_Text_drop", "knot_text_drop");
    }

    // ── Supertrait validation ────────────────────────────────────

    /// Check that every impl (including derived) satisfies its supertrait
    /// constraints. If `trait A => B`, then `impl B T` requires `impl A T`.
    fn validate_supertraits(&mut self) {
        // Build a set of (trait_name, type_name) for O(1) lookup
        let impl_set: HashSet<(&str, &str)> = self
            .trait_impl_types
            .iter()
            .flat_map(|(trait_name, types)| {
                types
                    .iter()
                    .map(move |(type_name, _)| (trait_name.as_str(), type_name.as_str()))
            })
            .collect();

        // Collect diagnostics separately to avoid borrow conflict with self
        let mut diags = Vec::new();

        for (trait_name, types) in &self.trait_impl_types {
            if let Some(required) = self.trait_supertraits.get(trait_name) {
                for supertrait in required {
                    for (type_name, span) in types {
                        if !impl_set.contains(&(supertrait.as_str(), type_name.as_str())) {
                            diags.push(
                                knot::diagnostic::Diagnostic::error(format!(
                                    "impl `{trait_name}` for `{type_name}` requires `{supertrait}` \
                                     to be implemented for `{type_name}`"
                                ))
                                .label(
                                    *span,
                                    format!("this impl requires `{supertrait}`"),
                                )
                                .note(format!(
                                    "add `impl {supertrait} {type_name} where ...` \
                                     or derive it with `deriving ({supertrait})`"
                                )),
                            );
                        }
                    }
                }
            }
        }
        self.diagnostics.extend(diags);
    }

    // ── Function definitions ──────────────────────────────────────

    fn define_functions(&mut self, module: &ast::Module, _type_env: &TypeEnv) {
        // __bind is no longer a standalone function — it dispatches through
        // Monad_{type}_bind trait impls (see compile_app).

        // Define standard library functions
        // 1-param: direct delegation to runtime
        self.define_stdlib_fn_1("single", "knot_relation_single");
        self.define_stdlib_fn_1("toUpper", "knot_text_to_upper");
        self.define_stdlib_fn_1("toLower", "knot_text_to_lower");
        self.define_stdlib_fn_1("length", "knot_text_length");
        self.define_stdlib_fn_1("trim", "knot_text_trim");
        self.define_stdlib_fn_1("reverse", "knot_text_reverse");
        self.define_stdlib_fn_1("chars", "knot_text_chars");
        self.define_stdlib_fn_1("id", "knot_value_id");
        self.define_stdlib_fn_1("not", "knot_value_not_fn");
        self.define_stdlib_fn_1("stripUnit", "knot_value_id");
        self.define_stdlib_fn_1("withUnit", "knot_value_id");
        self.define_stdlib_fn_1("stripFloatUnit", "knot_value_id");
        self.define_stdlib_fn_1("withFloatUnit", "knot_value_id");

        // 2-param: curried (outer captures arg1, inner calls runtime)
        self.define_stdlib_fn_2("filter", "knot_relation_filter", true);
        self.define_stdlib_fn_2("match", "knot_relation_match", false);
        self.define_stdlib_fn_2("sortBy", "knot_relation_sort_by", true);
        self.define_stdlib_fn_2("contains", "knot_text_contains", false);
        self.define_stdlib_fn_2("elem", "knot_list_elem", false);
        self.define_stdlib_fn_2("diff", "knot_relation_diff", true);
        self.define_stdlib_fn_2("inter", "knot_relation_inter", true);
        self.define_stdlib_sum();
        self.define_stdlib_fn_2("avg", "knot_relation_avg", true);
        self.define_stdlib_fn_2("minOn", "knot_relation_min", true);
        self.define_stdlib_fn_2("maxOn", "knot_relation_max", true);
        self.define_stdlib_fn_2("countWhere", "knot_relation_count_where", true);
        self.define_stdlib_fn_2("any", "knot_relation_any", true);
        self.define_stdlib_fn_2("all", "knot_relation_all", true);

        // Bytes: 1-param
        self.define_stdlib_fn_1("bytesLength", "knot_bytes_length");
        self.define_stdlib_fn_1("textToBytes", "knot_text_to_bytes");
        self.define_stdlib_fn_1("bytesToText", "knot_bytes_to_text");
        self.define_stdlib_fn_1("bytesToHex", "knot_bytes_to_hex");
        self.define_stdlib_fn_1("bytesFromHex", "knot_bytes_from_hex");
        self.define_stdlib_fn_1("hexDecode", "knot_bytes_from_hex");
        self.define_stdlib_fn_1("hash", "knot_hash");

        // Bytes: 2-param (curried)
        self.define_stdlib_fn_2("bytesConcat", "knot_bytes_concat", false);
        self.define_stdlib_fn_2("bytesGet", "knot_bytes_get", false);

        // Bytes: 3-param (double-curried)
        self.define_stdlib_fn_3("bytesSlice", "knot_bytes_slice");

        // 3-param: double-curried (outer captures arg1, middle captures arg2, inner calls runtime)
        self.define_stdlib_fn_3("upsertBy", "knot_relation_upsert_by");

        // Random: 1-param (IO-returning)
        self.define_stdlib_fn_1("randomInt", "knot_random_int_io");

        // Sleep: 1-param (IO-returning)
        self.define_stdlib_fn_1("sleep", "knot_sleep_io");

        // Spawn (IO-returning)
        self.define_stdlib_fn_1("fork", "knot_fork_io");

        // Race two IO actions concurrently (IO-returning).
        self.define_stdlib_fn_2("race", "knot_race_io", false);

        // Crypto: 2-param (curried)
        self.define_stdlib_fn_2("encrypt", "knot_crypto_encrypt_io", false);
        self.define_stdlib_fn_2("decrypt", "knot_crypto_decrypt", false);
        self.define_stdlib_fn_2("sign", "knot_crypto_sign", false);

        // Crypto: 3-param (double-curried)
        self.define_stdlib_fn_3("verify", "knot_crypto_verify");

        // File system: 1-param (IO-returning)
        self.define_stdlib_fn_1("readFile", "knot_fs_read_file_io");
        self.define_stdlib_fn_1("fileExists", "knot_fs_file_exists_io");
        self.define_stdlib_fn_1("removeFile", "knot_fs_remove_file_io");
        self.define_stdlib_fn_1("listDir", "knot_fs_list_dir_io");

        // File system: 2-param (curried, IO-returning)
        self.define_stdlib_fn_2("writeFile", "knot_fs_write_file_io", false);
        self.define_stdlib_fn_2("appendFile", "knot_fs_append_file_io", false);

        // Define built-in [] impls for HKT traits
        self.define_builtin_relation_impls();

        // Define built-in IO impls for HKT traits
        self.define_builtin_io_impls();

        // Define built-in primitive impls for Eq, Ord, Num traits
        self.define_builtin_primitive_impls();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body: Some(body), .. } => {
                    {
                        // Skip user functions that shadow stdlib builtins —
                        // the stdlib version is already defined above.
                        if self.stdlib_fns.contains(name.as_str()) {
                            continue;
                        }
                        // If body is a lambda, extract its params for direct compilation.
                        match &body.node {
                            ast::ExprKind::Lambda { params, body: lambda_body } => {
                                self.define_user_function(name, params, lambda_body);
                            }
                            _ => {
                                self.define_user_function(name, &[], body);
                            }
                        }
                    }
                }
                ast::DeclKind::Derived { name, body, .. } => {
                    if self.recursive_derived.contains(name) {
                        self.define_recursive_derived(name, body);
                    } else {
                        self.define_user_function(name, &[], body);
                    }
                }
                ast::DeclKind::Impl {
                    trait_name,
                    args,
                    items,
                    ..
                } => {
                    if let Some(type_name) = impl_type_name(args) {
                        let provided_methods: HashSet<&str> = items
                            .iter()
                            .filter_map(|item| {
                                if let ast::ImplItem::Method { name, .. } = item {
                                    Some(name.as_str())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for item in items {
                            if let ast::ImplItem::Method { name, params, body, .. } =
                                item
                            {
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, name
                                );
                                // Unwrap lambda-bodied methods the same way
                                // the collection pass counted their params.
                                let (params, body) = method_params_body(params, body);
                                self.define_user_function(&mangled, &params, body);
                            }
                        }

                        // Define default method bodies for methods not in this impl.
                        // Collect (name, params, body) to avoid holding borrow on self.trait_defs
                        // across self.define_user_function calls.
                        let defaults_to_define: Vec<(String, Vec<ast::Pat>, ast::Expr)> =
                            self.trait_defs.get(trait_name)
                                .map(|td| td.defaults.iter()
                                    .filter(|(m, _)| !provided_methods.contains(m.as_str()))
                                    .map(|(k, v)| (k.clone(), v.params.clone(), v.body.clone()))
                                    .collect())
                                .unwrap_or_default();
                        for (method_name, params, body) in &defaults_to_define {
                            let mangled = format!(
                                "{}_{}_{}", trait_name, type_name, method_name
                            );
                            self.define_user_function(
                                &mangled,
                                params,
                                body,
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        // Define derived method bodies
        let derived = std::mem::take(&mut self.derived_methods);
        for dm in &derived {
            self.define_user_function(&dm.mangled, &dm.default.params, &dm.default.body);
        }

        // Compile any pending lambdas and IO thunks (may generate more)
        while !self.pending_lambdas.is_empty() || !self.pending_io_thunks.is_empty() || !self.pending_trampolines.is_empty() {
            let lambdas: Vec<PendingLambda> =
                std::mem::take(&mut self.pending_lambdas);
            for lambda in lambdas {
                self.define_lambda_function(&lambda);
            }
            let thunks: Vec<PendingIoThunk> =
                std::mem::take(&mut self.pending_io_thunks);
            for thunk in thunks {
                self.define_io_thunk_function(&thunk);
            }
            let trampolines: Vec<PendingTrampoline> =
                std::mem::take(&mut self.pending_trampolines);
            for tramp in &trampolines {
                self.define_trampoline(tramp);
            }
        }

        // Define trait dispatcher function bodies
        self.define_trait_dispatchers();

        // Compile any pending lambdas/thunks from dispatchers
        while !self.pending_lambdas.is_empty() || !self.pending_io_thunks.is_empty() || !self.pending_trampolines.is_empty() {
            let lambdas: Vec<PendingLambda> =
                std::mem::take(&mut self.pending_lambdas);
            for lambda in lambdas {
                self.define_lambda_function(&lambda);
            }
            let thunks: Vec<PendingIoThunk> =
                std::mem::take(&mut self.pending_io_thunks);
            for thunk in thunks {
                self.define_io_thunk_function(&thunk);
            }
            let trampolines: Vec<PendingTrampoline> =
                std::mem::take(&mut self.pending_trampolines);
            for tramp in &trampolines {
                self.define_trampoline(tramp);
            }
        }

        // Body-less top-level constants — supplied at run time as CLI args.
        let required: Vec<RequiredConstant> = self.required_constants.clone();
        for constant in &required {
            self.define_required_constant(constant);
        }
    }

    // ── Trait dispatcher code generation ─────────────────────────

    /// Generate runtime dispatch function bodies for trait methods.
    /// Each dispatcher checks the runtime type tag of the first argument
    /// and calls the appropriate impl method.
    fn define_trait_dispatchers(&mut self) {
        // (method_name, dispatcher_id, param_count, dispatch_index, impls)
        let dispatcher_info: DispatcherInfo =
            self.trait_dispatcher_fns
                .iter()
                .filter_map(|(method_name, &dispatcher_id)| {
                    let info = self.trait_methods.get(method_name)?;
                    let impls: Vec<(String, FuncId)> = info
                        .impls
                        .iter()
                        .map(|e| (e.type_name.clone(), e.func_id))
                        .collect();
                    Some((
                        method_name.clone(),
                        dispatcher_id,
                        info.param_count,
                        info.dispatch_index,
                        impls,
                    ))
                })
                .collect();

        let data_ctors = std::rc::Rc::new(self.data_constructors.clone());
        let nullable_ctors = std::rc::Rc::new(self.nullable_ctors.clone());

        for (method_name, dispatcher_id, param_count, dispatch_index, impls) in dispatcher_info {
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..param_count {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));

            let data_ctors_ref = data_ctors.clone();
            let nullable_ctors_ref = nullable_ctors.clone();

            self.build_function(dispatcher_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let mut all_params: Vec<Value> = Vec::new();
                for i in 0..param_count {
                    all_params.push(builder.block_params(entry)[i + 1]);
                }

                // 0-param methods (e.g. `empty : c`) can't dispatch at runtime;
                // call the single impl directly
                if param_count == 0
                    && let Some((_, impl_func_id)) = impls.first() {
                        let impl_ref = cg
                            .module
                            .declare_func_in_func(*impl_func_id, builder.func);
                        let call = builder.ins().call(impl_ref, &[db]);
                        let result = builder.inst_results(call)[0];
                        builder.ins().return_(&[result]);
                        return;
                    }

                let dispatch_arg = match dispatch_index {
                    Some(idx) => all_params[idx],
                    None => {
                        // No parameter carries the type variable — can't dispatch
                        // at runtime. Use fallback if available, else first impl.
                        let fallback_rt = trait_method_fallback(&method_name);
                        if let Some(rt_fn) = fallback_rt {
                            let result = cg.call_rt(builder, rt_fn, &all_params);
                            builder.ins().return_(&[result]);
                            return;
                        }
                        if let Some((_, impl_func_id)) = impls.first() {
                            let impl_ref = cg
                                .module
                                .declare_func_in_func(*impl_func_id, builder.func);
                            let mut args = vec![db];
                            args.extend_from_slice(&all_params);
                            let call = builder.ins().call(impl_ref, &args);
                            let result = builder.inst_results(call)[0];
                            builder.ins().return_(&[result]);
                            return;
                        }
                        // No impls at all — unreachable in valid programs
                        builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                        return;
                    }
                };

                let merge_block = builder.create_block();
                merge_block_param(builder, merge_block, cg.ptr_type);

                // Separate primitive, normal ADT, and nullable ADT impls
                let mut primitive_impls: Vec<(i64, FuncId)> = Vec::new();
                let mut adt_impls: Vec<(Vec<String>, FuncId)> = Vec::new();
                let mut nullable_adt_impls: Vec<FuncId> = Vec::new();
                for (type_name, impl_func_id) in &impls {
                    if let Some(runtime_tag) = type_name_to_tag(type_name) {
                        primitive_impls.push((runtime_tag, *impl_func_id));
                    } else if let Some(ctors) = data_ctors_ref.get(type_name) {
                        let is_nullable = ctors.iter().any(|c| nullable_ctors_ref.contains_key(c));
                        if is_nullable {
                            nullable_adt_impls.push(*impl_func_id);
                        } else {
                            adt_impls.push((ctors.clone(), *impl_func_id));
                        }
                    }
                }

                // For nullable ADTs: check null first (before dereferencing)
                let tag_block = builder.create_block();
                if !nullable_adt_impls.is_empty() {
                    let nullable_impl_block = builder.create_block();
                    let is_null = builder.ins().icmp_imm(
                        IntCC::Equal,
                        dispatch_arg,
                        0,
                    );
                    builder.ins().brif(
                        is_null,
                        nullable_impl_block,
                        &[],
                        tag_block,
                        &[],
                    );

                    // Null → dispatch to the nullable ADT impl
                    builder.switch_to_block(nullable_impl_block);
                    builder.seal_block(nullable_impl_block);
                    let impl_ref = cg
                        .module
                        .declare_func_in_func(nullable_adt_impls[0], builder.func);
                    let mut call_args = vec![db];
                    call_args.extend(&all_params);
                    let call = builder.ins().call(impl_ref, &call_args);
                    let result = builder.inst_results(call)[0];
                    builder.ins().jump(merge_block, &[result.into()]);
                } else {
                    builder.ins().jump(tag_block, &[]);
                }

                builder.switch_to_block(tag_block);
                builder.seal_block(tag_block);

                // Get value tag for dispatch (safe: value is non-null here)
                let tag = cg.call_rt_typed(
                    builder,
                    "knot_value_get_tag",
                    &[dispatch_arg],
                    types::I32,
                );

                // Generate primitive type checks
                for (runtime_tag, impl_func_id) in &primitive_impls {
                    let impl_block = builder.create_block();
                    let next_block = builder.create_block();

                    let tag_const =
                        builder.ins().iconst(types::I32, *runtime_tag);
                    let is_match =
                        builder.ins().icmp(IntCC::Equal, tag, tag_const);
                    // Unit (tag 4) can appear where Relation (tag 6) is expected
                    // (empty relation operations). Route Unit to Relation impls.
                    let is_match = if *runtime_tag == 6 {
                        let unit_tag = builder.ins().iconst(types::I32, 4);
                        let is_unit = builder.ins().icmp(IntCC::Equal, tag, unit_tag);
                        builder.ins().bor(is_match, is_unit)
                    } else {
                        is_match
                    };
                    builder.ins().brif(
                        is_match,
                        impl_block,
                        &[],
                        next_block,
                        &[],
                    );

                    builder.switch_to_block(impl_block);
                    builder.seal_block(impl_block);
                    let impl_ref = cg
                        .module
                        .declare_func_in_func(*impl_func_id, builder.func);
                    let mut call_args = vec![db];
                    call_args.extend(&all_params);
                    let call = builder.ins().call(impl_ref, &call_args);
                    let result = builder.inst_results(call)[0];
                    builder.ins().jump(merge_block, &[result.into()]);

                    builder.switch_to_block(next_block);
                    builder.seal_block(next_block);
                }

                // Generate normal ADT type checks (Constructor tag + constructor name)
                for (ctors, impl_func_id) in &adt_impls {
                    if ctors.is_empty() {
                        continue;
                    }

                    let impl_block = builder.create_block();
                    let ctor_check_block = builder.create_block();
                    let next_adt_block = builder.create_block();

                    // Check if value is a Constructor (tag == 7)
                    let tag_7 = builder.ins().iconst(types::I32, 7);
                    let is_ctor =
                        builder.ins().icmp(IntCC::Equal, tag, tag_7);
                    builder.ins().brif(
                        is_ctor,
                        ctor_check_block,
                        &[],
                        next_adt_block,
                        &[],
                    );

                    // Check each constructor name — extract tag once, compare with knot_str_eq
                    builder.switch_to_block(ctor_check_block);
                    builder.seal_block(ctor_check_block);
                    // We know dispatch_arg is a Constructor (checked tag == 7 above),
                    // so extract the tag string pointer+length once for all comparisons
                    let ctor_tag_ptr = cg.call_rt(builder, "knot_constructor_tag_ptr", &[dispatch_arg]);
                    let ctor_tag_len = cg.call_rt(builder, "knot_constructor_tag_len", &[dispatch_arg]);
                    for (j, ctor_name) in ctors.iter().enumerate() {
                        let (expected_ptr, expected_len) =
                            cg.string_ptr(builder, ctor_name);
                        let matches = cg.call_rt_typed(
                            builder,
                            "knot_str_eq",
                            &[ctor_tag_ptr, ctor_tag_len, expected_ptr, expected_len],
                            types::I32,
                        );
                        let is_match = builder
                            .ins()
                            .icmp_imm(IntCC::NotEqual, matches, 0);

                        if j < ctors.len() - 1 {
                            let next_ctor = builder.create_block();
                            builder.ins().brif(
                                is_match,
                                impl_block,
                                &[],
                                next_ctor,
                                &[],
                            );
                            builder.switch_to_block(next_ctor);
                            builder.seal_block(next_ctor);
                        } else {
                            builder.ins().brif(
                                is_match,
                                impl_block,
                                &[],
                                next_adt_block,
                                &[],
                            );
                        }
                    }

                    // Impl block: call the impl function
                    builder.switch_to_block(impl_block);
                    builder.seal_block(impl_block);
                    let impl_ref = cg
                        .module
                        .declare_func_in_func(*impl_func_id, builder.func);
                    let mut call_args = vec![db];
                    call_args.extend(&all_params);
                    let call = builder.ins().call(impl_ref, &call_args);
                    let result = builder.inst_results(call)[0];
                    builder.ins().jump(merge_block, &[result.into()]);

                    builder.switch_to_block(next_adt_block);
                    builder.seal_block(next_adt_block);
                }

                // Nullable ADT "Some" dispatch: non-null bare payload
                // (value didn't match any Constructor-based ADT).
                // Use the first nullable impl — at runtime, null vs non-null
                // is the only distinction we can make for nullable ADTs.
                if let Some(impl_func_id) = nullable_adt_impls.first() {
                    let impl_block = builder.create_block();
                    let next_block = builder.create_block();

                    // Non-null, non-Constructor → must be a nullable Some variant
                    let tag_7 = builder.ins().iconst(types::I32, 7);
                    let is_not_ctor =
                        builder.ins().icmp(IntCC::NotEqual, tag, tag_7);
                    builder.ins().brif(
                        is_not_ctor,
                        impl_block,
                        &[],
                        next_block,
                        &[],
                    );

                    builder.switch_to_block(impl_block);
                    builder.seal_block(impl_block);
                    let impl_ref = cg
                        .module
                        .declare_func_in_func(*impl_func_id, builder.func);
                    let mut call_args = vec![db];
                    call_args.extend(&all_params);
                    let call = builder.ins().call(impl_ref, &call_args);
                    let result = builder.inst_results(call)[0];
                    builder.ins().jump(merge_block, &[result.into()]);

                    builder.switch_to_block(next_block);
                    builder.seal_block(next_block);
                }

                // Fallback: for operator-mapped trait methods, delegate to the
                // runtime function (handles types without explicit impls like
                // Record == Record). For other traits, panic with no-impl error.
                let fallback_rt = trait_method_fallback(&method_name);
                if let Some(rt_fn) = fallback_rt {
                    if method_name == "toJson" {
                        // Special case: toJson fallback passes the dispatcher function
                        // pointer so the runtime can call back for nested values,
                        // respecting custom ToJSON impls inside compound types.
                        let self_ref = cg.module.declare_func_in_func(dispatcher_id, builder.func);
                        let self_addr = builder.ins().func_addr(cg.ptr_type, self_ref);
                        let result = cg.call_rt(builder, "knot_json_encode_with", &[db, all_params[0], self_addr]);
                        builder.ins().jump(merge_block, &[result.into()]);
                    } else {
                        let result = cg.call_rt(builder, rt_fn, &all_params);
                        builder.ins().jump(merge_block, &[result.into()]);
                    }
                } else {
                    let (name_ptr, name_len) =
                        cg.string_ptr(builder, &method_name);
                    let err = cg.call_rt(
                        builder,
                        "knot_trait_no_impl",
                        &[name_ptr, name_len, dispatch_arg],
                    );
                    builder.ins().jump(merge_block, &[err.into()]);
                }

                builder.switch_to_block(merge_block);
                builder.seal_block(merge_block);
                let result = builder.block_params(merge_block)[0];
                builder.ins().return_(&[result]);
            });
        }
    }

    /// Temporarily move ctx/builder_ctx out of self so the FunctionBuilder
    /// doesn't borrow self, allowing self.method() calls during building.
    fn build_function<F>(&mut self, func_id: FuncId, sig: cranelift_codegen::ir::Signature, f: F)
    where
        F: FnOnce(&mut Self, &mut FunctionBuilder, cranelift_codegen::ir::Block),
    {
        let mut ctx = std::mem::replace(&mut self.ctx, self.module.make_context());
        let mut fb_ctx =
            std::mem::replace(&mut self.builder_ctx, FunctionBuilderContext::new());
        // Block references never cross function boundaries — clear the
        // per-function loop-skip target for the new builder context.
        let prev_io_loop_skip = self.io_loop_skip_block.take();

        ctx.func.signature = sig;

        {
            let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
            let entry = builder.create_block();
            builder.append_block_params_for_function_params(entry);
            builder.switch_to_block(entry);
            builder.seal_block(entry);

            f(self, &mut builder, entry);

            builder.finalize();
        }

        self.module.define_function(func_id, &mut ctx).unwrap();
        // Record unwind info while ctx still holds the compiled code.
        self.unwind.add_function(&mut self.module, func_id, &ctx);
        self.builder_ctx = fb_ctx;
        self.ctx = ctx;
        self.module.clear_context(&mut self.ctx);
        self.io_loop_skip_block = prev_io_loop_skip;
    }

    fn define_user_function(
        &mut self,
        name: &str,
        params: &[ast::Pat],
        body: &ast::Expr,
    ) {
        let (func_id, _) = self.user_fns[name];
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        for _ in params {
            sig.params.push(AbiParam::new(self.ptr_type));
        }
        sig.returns.push(AbiParam::new(self.ptr_type));

        let params_owned: Vec<ast::Pat> = params.to_vec();
        let body_owned = body.clone();
        let override_type = if params.is_empty() {
            self.overridable_constants.get(name).cloned()
        } else {
            None
        };
        let compile_time_val = self.compile_time_overrides.get(name).cloned();
        let name_owned = name.to_string();

        self.build_function(func_id, sig, |cg, builder, entry| {
            let mut env = Env::new();
            let db = builder.block_params(entry)[0];
            for (i, pat) in params_owned.iter().enumerate() {
                let val = builder.block_params(entry)[i + 1];
                cg.bind_io_pattern(builder, pat, val, &mut env, None);
            }

            // Compile-time override: emit the value directly, skip body entirely
            if let (Some(val_str), Some(type_str)) = (&compile_time_val, &override_type) {
                let val = cg.emit_override_literal(builder, val_str, type_str);
                builder.ins().return_(&[val]);
                return;
            }

            // For overridable constants, check CLI override before evaluating body
            if let Some(type_str) = &override_type {
                let type_tag: Option<i64> = match type_str.as_str() {
                    "Int" => Some(0),
                    "Float" => Some(1),
                    "Text" => Some(2),
                    "Bool" => Some(3),
                    "Maybe Int" => Some(4),
                    "Maybe Float" => Some(5),
                    "Maybe Text" => Some(6),
                    "Maybe Bool" => Some(7),
                    // Bytes/Uuid and other types have no CLI-override parser in
                    // the runtime (knot_override_lookup only handles the scalar
                    // types above). Skip the override for this constant rather
                    // than crashing codegen.
                    _ => {
                        eprintln!(
                            "knot: warning: constant '{}' of type '{}' does not support \
                             command-line overrides; ignoring override for this constant",
                            name_owned, type_str
                        );
                        None
                    }
                };
                if let Some(type_tag) = type_tag {
                    let (name_ptr, name_len) = cg.string_ptr(builder, &name_owned);
                    let tag = builder.ins().iconst(types::I32, type_tag);
                    let override_val =
                        cg.call_rt(builder, "knot_override_lookup", &[name_ptr, name_len, tag]);

                    let default_block = builder.create_block();
                    let override_block = builder.create_block();
                    let is_null = builder.ins().icmp_imm(IntCC::Equal, override_val, 0);
                    builder.ins().brif(is_null, default_block, &[], override_block, &[]);

                    builder.switch_to_block(override_block);
                    builder.seal_block(override_block);
                    builder.ins().return_(&[override_val]);

                    builder.switch_to_block(default_block);
                    builder.seal_block(default_block);
                }
            }

            let result = cg.compile_expr(builder, &body_owned, &mut env, db);
            builder.ins().return_(&[result]);
        });
    }

    /// Generate the body of a body-less top-level constant. The function reads
    /// the value from the matching `--<name>=value` CLI argument, exits with
    /// a clear error if the argument is missing, and runs any attached
    /// refinement predicate before returning.
    fn define_required_constant(&mut self, constant: &RequiredConstant) {
        let (func_id, _) = self.user_fns[constant.name.as_str()];
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.returns.push(AbiParam::new(self.ptr_type));

        let name_owned = constant.name.clone();
        let base_type = constant.base_type.clone();
        let refinement = constant.refinement.clone();
        let compile_time_val = self.compile_time_overrides.get(&constant.name).cloned();

        self.build_function(func_id, sig, |cg, builder, entry| {
            let db = builder.block_params(entry)[0];

            // Compile-time override: emit the value directly, skip the lookup.
            if let Some(val_str) = &compile_time_val {
                let val = cg.emit_override_literal(builder, val_str, &base_type);
                if let Some(refine) = &refinement {
                    let mut env = Env::new();
                    let pred_fn =
                        cg.compile_expr(builder, &refine.predicate, &mut env, db);
                    let (name_ptr, name_len) = cg.string_ptr(builder, &name_owned);
                    let (label_ptr, label_len) =
                        cg.string_ptr(builder, &refine.type_label);
                    let checked = cg.call_rt(
                        builder,
                        "knot_override_refinement_check",
                        &[db, val, pred_fn, name_ptr, name_len, label_ptr, label_len],
                    );
                    builder.ins().return_(&[checked]);
                } else {
                    builder.ins().return_(&[val]);
                }
                return;
            }

            // Runtime: look up the required CLI argument (exits if missing).
            let type_tag: i64 = match base_type.as_str() {
                "Int" => 0,
                "Float" => 1,
                "Text" => 2,
                "Bool" => 3,
                _ => unreachable!("base_type must be a primitive scalar"),
            };
            let (name_ptr, name_len) = cg.string_ptr(builder, &name_owned);
            let tag = builder.ins().iconst(types::I32, type_tag);
            let val = cg.call_rt(
                builder,
                "knot_override_required_lookup",
                &[name_ptr, name_len, tag],
            );

            if let Some(refine) = &refinement {
                let mut env = Env::new();
                let pred_fn =
                    cg.compile_expr(builder, &refine.predicate, &mut env, db);
                let (label_ptr, label_len) =
                    cg.string_ptr(builder, &refine.type_label);
                let checked = cg.call_rt(
                    builder,
                    "knot_override_refinement_check",
                    &[db, val, pred_fn, name_ptr, name_len, label_ptr, label_len],
                );
                builder.ins().return_(&[checked]);
            } else {
                builder.ins().return_(&[val]);
            }
        });
    }

    /// Define a recursive derived relation using fixpoint iteration.
    /// Generates two functions:
    /// - `knot_user_<name>_body(db, self_val)`: the body with self-references
    ///   reading from `self_val` instead of recursing
    /// - `knot_user_<name>(db)`: wrapper that calls `knot_relation_fixpoint`
    fn define_recursive_derived(&mut self, name: &str, body: &ast::Expr) {
        let body_func_id = self.recursive_body_fns[name];
        let name_owned = name.to_string();
        let body_owned = body.clone();

        // 1. Define the body function: (db, self_val) -> result
        {
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            sig.params.push(AbiParam::new(self.ptr_type)); // self_val
            sig.returns.push(AbiParam::new(self.ptr_type));

            let self_key = format!("__derived_self_{}", name_owned);

            self.build_function(body_func_id, sig, |cg, builder, entry| {
                let mut env = Env::new();
                let db = builder.block_params(entry)[0];
                let self_val = builder.block_params(entry)[1];
                // Inject self-reference into env so DerivedRef uses it
                env.set(&self_key, self_val);

                let result = cg.compile_expr(builder, &body_owned, &mut env, db);
                builder.ins().return_(&[result]);
            });
        }

        // 2. Define the wrapper function: (db) -> result
        //    Calls knot_relation_fixpoint(db, body_fn_ptr, empty_relation)
        {
            let (wrapper_func_id, _) = self.user_fns[name];
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            sig.returns.push(AbiParam::new(self.ptr_type));

            self.build_function(wrapper_func_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let initial = cg.call_rt(builder, "knot_relation_empty", &[]);
                let body_ref = cg.module.declare_func_in_func(body_func_id, builder.func);
                let body_addr = builder.ins().func_addr(cg.ptr_type, body_ref);
                let result = cg.call_rt(
                    builder,
                    "knot_relation_fixpoint",
                    &[db, body_addr, initial],
                );
                builder.ins().return_(&[result]);
            });
        }
    }

    fn define_lambda_function(&mut self, lambda: &PendingLambda) {
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env
        sig.params.push(AbiParam::new(self.ptr_type)); // arg
        sig.returns.push(AbiParam::new(self.ptr_type));

        let func_id = lambda.func_id;
        let params = lambda.params.clone();
        let param_pat = lambda.param_pat.clone();
        let body = lambda.body.clone();
        let free_vars = lambda.free_vars.clone();

        self.build_function(func_id, sig, |cg, builder, entry| {
            let mut env = Env::new();
            let db = builder.block_params(entry)[0];
            let closure_env = builder.block_params(entry)[1];
            let arg = builder.block_params(entry)[2];

            // Unpack free variables from closure env
            if free_vars.len() == 1 {
                // Single capture: env IS the value directly (no record)
                env.set(&free_vars[0], closure_env);
            } else {
                // Multi-capture: env is a record, extract by index (sorted order)
                let mut sorted_vars: Vec<&str> = free_vars.iter().map(|s| s.as_str()).collect();
                sorted_vars.sort();
                for (i, var_name) in sorted_vars.iter().enumerate() {
                    let idx = builder.ins().iconst(cg.ptr_type, i as i64);
                    let field_val =
                        cg.call_rt(builder, "knot_record_field_by_index", &[closure_env, idx]);
                    env.set(var_name, field_val);
                }
            }

            // Bind parameter — use the original pattern for destructuring
            if let Some(ref pat) = param_pat {
                match &pat.node {
                    ast::PatKind::Var(name) => env.set(name, arg),
                    _ => cg.bind_io_pattern(builder, pat, arg, &mut env, None),
                }
            } else if params.len() == 1 {
                env.set(&params[0], arg);
            }

            // If the body is an IO do-block, compile it eagerly inline
            // instead of creating a deferred thunk. This avoids the
            // variable capture mechanism: binds within the do-block
            // create SSA values directly in this function, so later
            // statements can use them without going through a closure env.
            // The lambda executes IO when called; the caller's knot_io_run
            // on the result is a no-op (returns non-IO values as-is).
            let result = if let ast::ExprKind::Do(stmts) = &body.node {
                if cg.is_io_do_block(stmts) {
                    cg.compile_io_do_eager(builder, stmts, &mut env, db)
                } else {
                    cg.compile_expr(builder, &body, &mut env, db)
                }
            } else {
                cg.compile_expr(builder, &body, &mut env, db)
            };
            builder.ins().return_(&[result]);
        });
    }

    /// Compile a pending IO do-block thunk function.
    /// Signature: (db, env) -> result. Runs IO actions eagerly inside the thunk.
    fn define_io_thunk_function(&mut self, thunk: &PendingIoThunk) {
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env
        sig.returns.push(AbiParam::new(self.ptr_type));

        let func_id = thunk.func_id;
        let stmts = thunk.stmts.clone();
        let free_vars = thunk.free_vars.clone();

        self.build_function(func_id, sig, |cg, builder, entry| {
            let mut env = Env::new();
            let db = builder.block_params(entry)[0];
            let closure_env = builder.block_params(entry)[1];

            // Unpack free variables from closure env (same pattern as lambdas)
            if free_vars.len() == 1 {
                env.set(&free_vars[0], closure_env);
            } else if free_vars.len() > 1 {
                let mut sorted_vars: Vec<&str> = free_vars.iter().map(|s| s.as_str()).collect();
                sorted_vars.sort();
                for (i, var_name) in sorted_vars.iter().enumerate() {
                    let idx = builder.ins().iconst(cg.ptr_type, i as i64);
                    let field_val =
                        cg.call_rt(builder, "knot_record_field_by_index", &[closure_env, idx]);
                    env.set(var_name, field_val);
                }
            }

            // Run IO do-block eagerly inside the thunk
            let result = cg.compile_io_do_eager(builder, &stmts, &mut env, db);
            builder.ins().return_(&[result]);
        });
    }

    /// Get or create a trampoline function that wraps a user function with the
    /// standard lambda calling convention (db, env, arg) -> result.
    /// For 1-param user functions: trampoline(db, env, arg) calls user_fn(db, arg).
    /// For n-param: generates a curry chain that directly calls the user function,
    /// avoiding the infinite recursion that would occur if the trampoline tried
    /// to partially apply itself through compile_app.
    fn get_or_create_trampoline(&mut self, name: &str, n_params: usize) -> FuncId {
        if let Some(&id) = self.user_fn_trampolines.get(name) {
            return id;
        }
        let trampoline_name = format!("__trampoline_{}", name);
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env
        sig.params.push(AbiParam::new(self.ptr_type)); // arg
        sig.returns.push(AbiParam::new(self.ptr_type));
        let trampoline_id = self
            .module
            .declare_function(&trampoline_name, Linkage::Local, &sig)
            .unwrap();

        if n_params <= 1 {
            // For 1-param functions: body is App(Var(name), Var(param)) — direct call
            let dummy_span = ast::Span::new(0, 0);
            let body = ast::Spanned::new(
                ast::ExprKind::App {
                    func: Box::new(ast::Spanned::new(
                        ast::ExprKind::Var(name.to_string()),
                        dummy_span,
                    )),
                    arg: Box::new(ast::Spanned::new(
                        ast::ExprKind::Var("__trampoline_arg".into()),
                        dummy_span,
                    )),
                },
                dummy_span,
            );

            self.pending_lambdas.push(PendingLambda {
                func_id: trampoline_id,
                params: vec!["__trampoline_arg".to_string()],
                param_pat: None,
                body,
                free_vars: vec![],
            });
        } else {
            // For multi-param functions: generate curry chain via build_function
            self.pending_trampolines.push(PendingTrampoline {
                trampoline_id,
                user_fn_name: name.to_string(),
                n_params,
            });
        }

        self.user_fn_trampolines.insert(name.to_string(), trampoline_id);
        trampoline_id
    }

    /// Define a multi-param trampoline as a curry chain.
    /// For n_params=2: trampoline(db,env,arg1) → Function(inner,arg1)
    ///                  inner(db,arg1,arg2)     → user_fn(db,arg1,arg2)
    /// For n_params=3: trampoline(db,env,arg1) → Function(mid,arg1)
    ///                  mid(db,arg1,arg2)       → Function(inner,{arg1,arg2})
    ///                  inner(db,env,arg3)      → user_fn(db,arg1,arg2,arg3)
    /// General pattern builds n_params-1 curry stages.
    fn define_trampoline(&mut self, tramp: &PendingTrampoline) {
        let (user_fn_id, _) = self.user_fns[&tramp.user_fn_name];
        let n_params = tramp.n_params;
        let fn_name = tramp.user_fn_name.clone();

        // Declare all inner curry stage functions upfront
        // Stage i (0-indexed) takes (db, env, arg_{i+1}) and either:
        //   - returns the final user_fn call (if i == n_params-2, i.e. last stage)
        //   - returns a Function wrapping the next stage
        let mut stage_ids: Vec<FuncId> = Vec::new();
        for i in 0..n_params - 1 {
            let stage_name = format!("__tramp_{}_{}", fn_name, i + 1);
            stage_ids.push(self.declare_closure_fn(&stage_name));
        }

        // Stage 0: the trampoline itself — captures arg1, returns Function(stage1, arg1)
        {
            let next_stage_id = stage_ids[0];
            let trampoline_id = tramp.trampoline_id;
            let fn_name = fn_name.clone();
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            sig.params.push(AbiParam::new(self.ptr_type)); // env (unused)
            sig.params.push(AbiParam::new(self.ptr_type)); // arg1
            sig.returns.push(AbiParam::new(self.ptr_type));
            self.build_function(trampoline_id, sig, |cg, builder, entry| {
                let arg1 = builder.block_params(entry)[2];
                let next_ref = cg.module.declare_func_in_func(next_stage_id, builder.func);
                let fn_addr = builder.ins().func_addr(cg.ptr_type, next_ref);
                let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
                let result =
                    cg.call_rt(builder, "knot_value_function", &[fn_addr, arg1, src_ptr, src_len]);
                builder.ins().return_(&[result]);
            });
        }

        // Intermediate + final stages
        for stage_idx in 0..stage_ids.len() {
            let stage_fn_id = stage_ids[stage_idx];
            let is_last = stage_idx == stage_ids.len() - 1;
            let next_stage_id = if !is_last { Some(stage_ids[stage_idx + 1]) } else { None };
            let fn_name = fn_name.clone();

            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            sig.params.push(AbiParam::new(self.ptr_type)); // env (captured args)
            sig.params.push(AbiParam::new(self.ptr_type)); // new arg
            sig.returns.push(AbiParam::new(self.ptr_type));

            let total_args = stage_idx + 2; // args accumulated after this stage
            let _n_params = n_params;

            self.build_function(stage_fn_id, sig, |cg, builder, entry| {
                let db = builder.block_params(entry)[0];
                let env = builder.block_params(entry)[1]; // captured args
                let new_arg = builder.block_params(entry)[2];

                if is_last {
                    // Final stage: extract all captured args, call user function directly
                    let mut call_args = vec![db];
                    if total_args == 2 {
                        // env is arg1 directly (single capture)
                        call_args.push(env);
                    } else {
                        // env is a record of previous args
                        for i in 0..total_args - 1 {
                            let idx = builder.ins().iconst(cg.ptr_type, i as i64);
                            let arg_val = cg.call_rt(
                                builder,
                                "knot_record_field_by_index",
                                &[env, idx],
                            );
                            call_args.push(arg_val);
                        }
                    }
                    call_args.push(new_arg);

                    let func_ref =
                        cg.module.declare_func_in_func(user_fn_id, builder.func);
                    let call = builder.ins().call(func_ref, &call_args);
                    let result = builder.inst_results(call)[0];
                    builder.ins().return_(&[result]);
                } else {
                    // Intermediate stage: pack args into record, return Function(next_stage, record)
                    let next_id = next_stage_id.unwrap();

                    let new_env = if total_args == 2 {
                        // Going from 1 captured arg (env=arg1) + new_arg to record of 2
                        let ptr_bytes = cg.ptr_type.bytes() as i32;
                        let slot = builder.create_sized_stack_slot(
                            StackSlotData::new(StackSlotKind::ExplicitSlot, (6 * ptr_bytes) as u32, 3),
                        );
                        let (k0_ptr, k0_len) = cg.string_ptr(builder, &tramp_arg_key(0));
                        builder.ins().stack_store(k0_ptr, slot, 0);
                        builder.ins().stack_store(k0_len, slot, ptr_bytes);
                        builder.ins().stack_store(env, slot, 2 * ptr_bytes);
                        let (k1_ptr, k1_len) = cg.string_ptr(builder, &tramp_arg_key(1));
                        builder.ins().stack_store(k1_ptr, slot, 3 * ptr_bytes);
                        builder.ins().stack_store(k1_len, slot, 4 * ptr_bytes);
                        builder.ins().stack_store(new_arg, slot, 5 * ptr_bytes);
                        let data_ptr = builder.ins().stack_addr(cg.ptr_type, slot, 0);
                        let count = builder.ins().iconst(cg.ptr_type, 2i64);
                        cg.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count])
                    } else {
                        // env is already a record, append new_arg
                        let prev_count = total_args - 1;
                        let new_count = total_args;
                        let ptr_bytes = cg.ptr_type.bytes() as i32;
                        let slot_size = (3u32)
                            .checked_mul(new_count as u32)
                            .and_then(|n| n.checked_mul(ptr_bytes as u32))
                            .expect("knot codegen: trampoline slot size overflow");
                        let slot = builder.create_sized_stack_slot(
                            StackSlotData::new(StackSlotKind::ExplicitSlot, slot_size, 3),
                        );
                        // Copy existing fields
                        for i in 0..prev_count {
                            let idx = builder.ins().iconst(cg.ptr_type, i as i64);
                            let val = cg.call_rt(
                                builder,
                                "knot_record_field_by_index",
                                &[env, idx],
                            );
                            let key_str = tramp_arg_key(i);
                            let (kp, kl) = cg.string_ptr(builder, &key_str);
                            let base = (i as i32) * (3 * ptr_bytes);
                            builder.ins().stack_store(kp, slot, base);
                            builder.ins().stack_store(kl, slot, base + ptr_bytes);
                            builder.ins().stack_store(val, slot, base + 2 * ptr_bytes);
                        }
                        // Add new arg
                        let key_str = tramp_arg_key(prev_count);
                        let (kp, kl) = cg.string_ptr(builder, &key_str);
                        let base = (prev_count as i32) * (3 * ptr_bytes);
                        builder.ins().stack_store(kp, slot, base);
                        builder.ins().stack_store(kl, slot, base + ptr_bytes);
                        builder.ins().stack_store(new_arg, slot, base + 2 * ptr_bytes);
                        let data_ptr = builder.ins().stack_addr(cg.ptr_type, slot, 0);
                        let count = builder.ins().iconst(cg.ptr_type, new_count as i64);
                        cg.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count])
                    };

                    let next_ref = cg.module.declare_func_in_func(next_id, builder.func);
                    let fn_addr = builder.ins().func_addr(cg.ptr_type, next_ref);
                    let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
                    let result = cg.call_rt(
                        builder,
                        "knot_value_function",
                        &[fn_addr, new_env, src_ptr, src_len],
                    );
                    builder.ins().return_(&[result]);
                }
            });
        }
    }

    // ── Main function generation ──────────────────────────────────

    fn generate_main(&mut self, module: &ast::Module) {
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(types::I32)); // argc
        sig.params.push(AbiParam::new(self.ptr_type)); // argv
        sig.returns.push(AbiParam::new(types::I32));
        let main_id = self
            .module
            .declare_function("main", Linkage::Export, &sig)
            .unwrap();

        let decls: Vec<ast::Decl> = module.decls.clone();
        let user_main = self.user_fns.get("main").copied();
        let all_routes: Vec<(String, Vec<ast::RouteEntry>)> =
            self.route_entries.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let to_json_dispatcher_id = self.trait_dispatcher_fns.get("toJson").copied();
        // Only when a user `Ord` impl exists, matching `compile_comparison`'s
        // gate for `<`/`>`. Without one the runtime's structural comparison is
        // both the same answer and one indirect call cheaper.
        let ord_compare_dispatcher_id = self
            .has_user_impls("compare")
            .then(|| self.trait_dispatcher_fns.get("compare").copied())
            .flatten();
        let alias_ast = self.alias_ast.clone();

        self.build_function(main_id, sig, |cg, builder, entry| {
            let argc = builder.block_params(entry)[0];
            let argv = builder.block_params(entry)[1];

            // Register route tables for the api command
            let mut route_tables: Vec<(Value, Vec<ast::RouteEntry>)> = Vec::new();
            for (route_name, entries) in &all_routes {
                let table = cg.call_rt(builder, "knot_route_table_new", &[]);
                for route_entry in entries {
                    let method_str = match route_entry.method {
                        ast::HttpMethod::Get => "GET",
                        ast::HttpMethod::Post => "POST",
                        ast::HttpMethod::Put => "PUT",
                        ast::HttpMethod::Delete => "DELETE",
                        ast::HttpMethod::Patch => "PATCH",
                    };
                    let (method_ptr, method_len) = cg.string_ptr(builder, method_str);
                    let path_pattern = path_segments_to_pattern(&route_entry.path, &cg.type_aliases);
                    let (path_ptr, path_len) = cg.string_ptr(builder, &path_pattern);
                    let (ctor_ptr, ctor_len) = cg.string_ptr(builder, &route_entry.constructor);
                    let body_desc = fields_to_descriptor(&route_entry.body_fields, &cg.type_aliases);
                    let (body_ptr, body_len) = cg.string_ptr(builder, &body_desc);
                    let query_desc = fields_to_descriptor(&route_entry.query_params, &cg.type_aliases);
                    let (query_ptr, query_len) = cg.string_ptr(builder, &query_desc);
                    let resp_desc = response_type_descriptor(&route_entry.response_ty, &cg.type_aliases);
                    let (resp_ptr, resp_len) = cg.string_ptr(builder, &resp_desc);
                    let req_hdrs_desc = fields_to_descriptor(&route_entry.request_headers, &cg.type_aliases);
                    let (req_hdrs_ptr, req_hdrs_len) = cg.string_ptr(builder, &req_hdrs_desc);
                    let resp_hdrs_desc = fields_to_descriptor(&route_entry.response_headers, &cg.type_aliases);
                    let (resp_hdrs_ptr, resp_hdrs_len) = cg.string_ptr(builder, &resp_hdrs_desc);
                    cg.call_rt_void(
                        builder,
                        "knot_route_table_add",
                        &[
                            table, method_ptr, method_len, path_ptr, path_len,
                            ctor_ptr, ctor_len, body_ptr, body_len, query_ptr,
                            query_len, resp_ptr, resp_len,
                            req_hdrs_ptr, req_hdrs_len, resp_hdrs_ptr, resp_hdrs_len,
                        ],
                    );
                }
                let (name_ptr, name_len) = cg.string_ptr(builder, route_name);
                cg.call_rt_void(builder, "knot_api_register", &[name_ptr, name_len, table]);
                route_tables.push((table, entries.clone()));
            }

            // Check if this is an "api" command
            let api_result = {
                let func_id = cg.runtime_fns["knot_api_handle"];
                let func_ref = cg.module.declare_func_in_func(func_id, builder.func);
                let call = builder.ins().call(func_ref, &[argc, argv]);
                builder.inst_results(call)[0]
            };

            let normal_block = builder.create_block();
            let api_exit_block = builder.create_block();
            builder.ins().brif(api_result, api_exit_block, &[], normal_block, &[]);

            builder.switch_to_block(api_exit_block);
            builder.seal_block(api_exit_block);
            let zero = builder.ins().iconst(types::I32, 0);
            builder.ins().return_(&[zero]);

            builder.switch_to_block(normal_block);
            builder.seal_block(normal_block);

            // Check if this is a "db" command (TUI explorer)
            let db_path = cg.db_path.clone();
            let (db_path_ptr_pre, db_path_len_pre) = cg.string_ptr(builder, &db_path);
            let db_result = {
                let func_id = cg.runtime_fns["knot_db_handle"];
                let func_ref = cg.module.declare_func_in_func(func_id, builder.func);
                let call = builder.ins().call(func_ref, &[argc, argv, db_path_ptr_pre, db_path_len_pre]);
                builder.inst_results(call)[0]
            };

            let normal_block2 = builder.create_block();
            let db_exit_block = builder.create_block();
            builder.ins().brif(db_result, db_exit_block, &[], normal_block2, &[]);

            builder.switch_to_block(db_exit_block);
            builder.seal_block(db_exit_block);
            let zero2 = builder.ins().iconst(types::I32, 0);
            builder.ins().return_(&[zero2]);

            builder.switch_to_block(normal_block2);
            builder.seal_block(normal_block2);

            // Check --debug flag
            let debug_init_ref = cg.import_rt(builder, "knot_debug_init");
            builder.ins().call(debug_init_ref, &[]);

            // Apply --http-max-body-bytes if present (must run before any
            // listen/fetch so the cap is in effect on the first request).
            let http_init_ref = cg.import_rt(builder, "knot_http_config_init");
            builder.ins().call(http_init_ref, &[]);

            // Check --help for overridable constants (exclude compile-time overrides).
            // Entry format:
            //   `name:type:!`           — required (no default)
            //   `name:type:=<value>`    — has displayable default (escape `\` and `,`
            //                             as `\\` and `\c` so the entry separator is
            //                             unambiguous; colons inside the value are
            //                             preserved by `splitn(3, ':')` at runtime)
            //   `name:type`             — has default but not displayable
            let required_names: HashSet<String> = cg
                .required_constants
                .iter()
                .map(|c| c.name.clone())
                .collect();
            let descriptor = {
                let mut entries: Vec<String> = cg.overridable_constants.iter()
                    .filter(|(name, _)| !cg.compile_time_overrides.contains_key(*name))
                    .map(|(name, ty)| {
                        if required_names.contains(name) {
                            format!("{}:{}:!", name, ty)
                        } else if let Some(def) = cg.overridable_defaults.get(name) {
                            let escaped = def.replace('\\', "\\\\").replace(',', "\\c");
                            format!("{}:{}:={}", name, ty, escaped)
                        } else {
                            format!("{}:{}", name, ty)
                        }
                    })
                    .collect();
                entries.sort();
                entries.join(",")
            };
            let (desc_ptr, desc_len) = cg.string_ptr(builder, &descriptor);
            cg.call_rt_void(builder, "knot_override_check_help", &[desc_ptr, desc_len]);

            // Open database
            let db_path = cg.db_path.clone();
            let (db_path_ptr, db_path_len) = cg.string_ptr(builder, &db_path);
            let db_open_ref = cg.import_rt(builder, "knot_db_open");
            let db_open_call =
                builder.ins().call(db_open_ref, &[db_path_ptr, db_path_len]);
            let db = builder.inst_results(db_open_call)[0];

            // Initialize schema tracking
            cg.call_rt_void(builder, "knot_schema_init", &[db]);

            // Register toJson dispatcher so the runtime can use custom ToJSON impls
            if let Some(dispatcher_id) = to_json_dispatcher_id {
                let func_ref = cg.module.declare_func_in_func(dispatcher_id, builder.func);
                let func_addr = builder.ins().func_addr(cg.ptr_type, func_ref);
                cg.call_rt_void(builder, "knot_register_to_json", &[func_addr]);
            }

            // Register constructor declaration order for every `data`
            // declaration. Structural `Ord` on an ADT follows the order the
            // constructors were written in, and a `Value::Constructor` carries
            // only its tag, so the runtime needs the order handed to it.
            for decl in &decls {
                if let ast::DeclKind::Data { name, constructors, .. } = &decl.node {
                    let ctor_list = constructors
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    let (name_ptr, name_len) = cg.string_ptr(builder, name);
                    let (ctors_ptr, ctors_len) = cg.string_ptr(builder, &ctor_list);
                    cg.call_rt_void(
                        builder,
                        "knot_register_ctor_order",
                        &[name_ptr, name_len, ctors_ptr, ctors_len],
                    );
                }
            }

            // Register compare dispatcher so sortBy/minOn/maxOn order keys
            // through user Ord impls, as `<`/`>` already do
            if let Some(dispatcher_id) = ord_compare_dispatcher_id {
                let func_ref = cg.module.declare_func_in_func(dispatcher_id, builder.func);
                let func_addr = builder.ins().func_addr(cg.ptr_type, func_ref);
                cg.call_rt_void(builder, "knot_register_ord_compare", &[func_addr]);
            }

            // Apply pending migrations (before source init)
            let migrate_schemas = cg.migrate_schemas.clone();
            let mut migrate_counters: HashMap<String, usize> = HashMap::new();
            for decl in &decls {
                if let ast::DeclKind::Migrate {
                    relation,
                    using_fn,
                    ..
                } = &decl.node
                    && let Some(migrations) = migrate_schemas.get(relation) {
                        let idx = migrate_counters.entry(relation.clone()).or_insert(0);
                        if let Some((old_schema, new_schema)) = migrations.get(*idx) {
                            let (name_ptr, name_len) = cg.string_ptr(builder, relation);
                            let (old_ptr, old_len) = cg.string_ptr(builder, old_schema);
                            let (new_ptr, new_len) = cg.string_ptr(builder, new_schema);

                            // Compile the using expression (typically a lambda)
                            let mut env = Env::new();
                            let migrate_fn_val =
                                cg.compile_expr(builder, using_fn, &mut env, db);

                            // Validate refinements on the transformed rows
                            // before committing the migration. Every other
                            // write path (set/replace/append/view/scalar)
                            // calls emit_refinement_checks; migrate was the
                            // sole exception, so a `migrate … using` could
                            // persist values violating a refined type (e.g.
                            // negative into a Nat column). The runtime's
                            // knot_source_migrate_preview returns the
                            // transformed rows without writing, in the same
                            // value shape set/replace validate; if there are
                            // no refinements on this source the check is a
                            // no-op.
                            if cg.source_refinements.contains_key(relation) {
                                let preview = cg.call_rt(
                                    builder,
                                    "knot_source_migrate_preview",
                                    &[
                                        db, name_ptr, name_len, old_ptr, old_len,
                                        new_ptr, new_len, migrate_fn_val,
                                    ],
                                );
                                cg.emit_refinement_checks(
                                    builder,
                                    relation,
                                    preview,
                                    &mut env,
                                    db,
                                );
                            }

                            cg.call_rt_void(
                                builder,
                                "knot_source_migrate",
                                &[
                                    db, name_ptr, name_len, old_ptr, old_len, new_ptr,
                                    new_len, migrate_fn_val,
                                ],
                            );
                            *idx += 1;
                        }
                    }
            }

            // Initialize source tables
            for decl in &decls {
                if let ast::DeclKind::Source { name, .. } = &decl.node {
                    let schema = cg
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();
                    let (name_ptr, name_len) = cg.string_ptr(builder, name);
                    let (schema_ptr, schema_len) = cg.string_ptr(builder, &schema);
                    let init_ref = cg.import_rt(builder, "knot_source_init");
                    builder.ins().call(
                        init_ref,
                        &[db, name_ptr, name_len, schema_ptr, schema_len],
                    );
                }
            }

            // Register subset constraints
            let constraints = cg.subset_constraints.clone();
            for (sub, sup) in &constraints {
                let (sub_rel_ptr, sub_rel_len) = cg.string_ptr(builder, &sub.relation);
                let sub_field_str = sub.field.as_deref().unwrap_or("");
                let (sub_field_ptr, sub_field_len) = cg.string_ptr(builder, sub_field_str);
                let (sup_rel_ptr, sup_rel_len) = cg.string_ptr(builder, &sup.relation);
                let sup_field_str = sup.field.as_deref().unwrap_or("");
                let (sup_field_ptr, sup_field_len) = cg.string_ptr(builder, sup_field_str);
                cg.call_rt_void(
                    builder,
                    "knot_constraint_register",
                    &[
                        db, sub_rel_ptr, sub_rel_len, sub_field_ptr, sub_field_len,
                        sup_rel_ptr, sup_rel_len, sup_field_ptr, sup_field_len,
                    ],
                );
            }

            // Register refinement predicates for route body fields, including
            // the ones nested inside lists and records (see
            // `collect_type_refinements`).
            for (table, entries) in &route_tables {
                for route_entry in entries {
                    let (ctor_ptr, ctor_len) = cg.string_ptr(builder, &route_entry.constructor);
                    for field in &route_entry.body_fields {
                        let mut found = Vec::new();
                        collect_type_refinements(
                            &field.value,
                            &field.name,
                            &alias_ast,
                            &mut Vec::new(),
                            &mut found,
                        );
                        for (path, type_name, pred_expr) in found {
                            let mut pred_env = Env::new();
                            let pred_fn = cg.compile_expr(builder, &pred_expr, &mut pred_env, db);
                            let (fn_ptr, fn_len) = cg.string_ptr(builder, &path);
                            let (tn_ptr, tn_len) = cg.string_ptr(builder, &type_name);
                            cg.call_rt_void(
                                builder,
                                "knot_route_set_field_refinement",
                                &[*table, ctor_ptr, ctor_len, fn_ptr, fn_len, pred_fn, tn_ptr, tn_len],
                            );
                        }
                    }
                }
            }

            // Register rate limit configurations for route entries.
            // The expression compiles to a `{key, limit}` Value handed
            // straight to the runtime, which unpacks the fields.
            for (table, entries) in &route_tables {
                for route_entry in entries {
                    if let Some(rate_limit_expr) = &route_entry.rate_limit {
                        let (ctor_ptr, ctor_len) = cg.string_ptr(builder, &route_entry.constructor);
                        let mut rl_env = Env::new();
                        let rl_val = cg.compile_expr(builder, rate_limit_expr, &mut rl_env, db);
                        cg.call_rt_void(
                            builder,
                            "knot_route_set_rate_limit",
                            &[*table, ctor_ptr, ctor_len, rl_val],
                        );
                    }
                }
            }

            // Call user's main function if it exists.
            //
            // Isolate main's body in a child arena frame so any values it
            // `knot_arena_promote`s are pinned in the child, not the root.
            // The root frame is never popped (see Arena::pop_frame), so
            // any pinned values there would leak for the life of the
            // process — a real concern for any program whose top-level
            // do-block promotes values.
            if let Some((main_fn_id, n_params)) = user_main {
                if n_params == 0 {
                    cg.call_rt_void(builder, "knot_arena_push_frame", &[]);

                    let user_main_ref =
                        cg.module.declare_func_in_func(main_fn_id, builder.func);
                    let call = builder.ins().call(user_main_ref, &[db]);
                    let result = builder.inst_results(call)[0];

                    // Run IO if result is an IO value, then print
                    let io_run_ref = cg.import_rt(builder, "knot_io_run");
                    let call2 = builder.ins().call(io_run_ref, &[db, result]);
                    let executed = builder.inst_results(call2)[0];

                    let println_ref = cg.import_rt(builder, "knot_println");
                    builder.ins().call(println_ref, &[executed]);

                    // Pop main's frame, discarding everything it allocated.
                    // The printed value has already been written to stdout,
                    // so we don't need to promote anything up.
                    cg.call_rt_void(builder, "knot_arena_pop_frame", &[]);
                } else {
                    cg.diagnostics.push(
                        knot::diagnostic::Diagnostic::error(
                            "'main' must be a zero-parameter declaration, but it takes arguments"
                        )
                    );
                }
            }

            // Join all spawned threads before closing
            let threads_join_ref = cg.import_rt(builder, "knot_threads_join");
            builder.ins().call(threads_join_ref, &[]);

            // Close database
            let db_close_ref = cg.import_rt(builder, "knot_db_close");
            builder.ins().call(db_close_ref, &[db]);

            let zero = builder.ins().iconst(types::I32, 0);
            builder.ins().return_(&[zero]);
        });
    }

    // ── Finish ────────────────────────────────────────────────────

    fn finish(self) -> Vec<u8> {
        let mut product = self.module.finish();
        self.unwind.emit(&mut product);
        product.emit().unwrap()
    }

    // ── Expression compilation ────────────────────────────────────

    fn compile_expr(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        match &expr.node {
            ast::ExprKind::Lit(lit) => self.compile_lit(builder, lit),

            ast::ExprKind::Var(name) if name == "__empty" => {
                self.compile_monadic_empty(builder, expr.span, db)
            }

            ast::ExprKind::Var(name) => {
                // A local or captured binding shadows any builtin of the same
                // name (e.g. a lambda param `\now -> now`, or `retry <- ...`).
                // The applied-call path already consults `env` first; the bare
                // builtin special-cases below must do the same or they would
                // hijack the binding (and, for `retry`, emit STM control flow
                // instead of reading the variable).
                if let Some(&val) = env.bindings.get(name.as_str()) {
                    return val;
                }
                // A user-defined top-level declaration whose name collides with
                // one of the zero-arg builtins below (`now`, `randomFloat`,
                // `retry`, …) shadows the builtin. None of those names are
                // stdlib functions, so a `user_fns` hit here is always a genuine
                // user declaration, never a stdlib registration. The applied-call
                // path already consults `user_fns` before its builtin special
                // cases; the bare reference must too, or the user's declaration
                // is compiled but never referenced — e.g. `now = 5` would emit
                // `knot_now_io` here, producing an `IO` value where the type
                // checker inferred `Int` (a runtime panic when later used).
                // A trait method referenced as a value (`map area shapes`)
                // boxes the impl its static type selects, not the runtime tag
                // dispatcher — the tag cannot tell two ADTs apart when they
                // share a constructor name.
                let static_impl = self.resolve_trait_call(name, expr.span);
                let fn_name: &str =
                    static_impl.as_deref().unwrap_or(name.as_str());
                if let Some((func_id, n_params)) = self.user_fns.get(fn_name).copied() {
                    if n_params == 0 {
                        // 0-param function is a constant — call it directly
                        let func_ref =
                            self.module.declare_func_in_func(func_id, builder.func);
                        let call = builder.ins().call(func_ref, &[db]);
                        return builder.inst_results(call)[0];
                    } else {
                        // Create a trampoline that bridges (db, env, arg) calling
                        // convention to the user function's (db, arg1, ...) convention.
                        let trampoline_id = self.get_or_create_trampoline(fn_name, n_params);
                        let func_ref =
                            self.module.declare_func_in_func(trampoline_id, builder.func);
                        let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);
                        let null = builder.ins().iconst(self.ptr_type, 0);
                        let (src_ptr, src_len) = self.string_ptr(builder, fn_name);
                        return self.call_rt(builder, "knot_value_function", &[fn_addr, null, src_ptr, src_len]);
                    }
                }
                if name == "now" {
                    return self.call_rt(builder, "knot_now_io", &[]);
                }
                if name == "randomFloat" {
                    return self.call_rt(builder, "knot_random_float_io", &[]);
                }
                if name == "randomUuid" {
                    return self.call_rt(builder, "knot_random_uuid_io", &[]);
                }
                if name == "generateKeyPair" {
                    return self.call_rt(builder, "knot_crypto_generate_key_pair_io", &[]);
                }
                if name == "generateSigningKeyPair" {
                    return self.call_rt(builder, "knot_crypto_generate_signing_key_pair_io", &[]);
                }
                if name == "readLine" {
                    return self.call_rt(builder, "knot_read_line_io", &[]);
                }
                if name == "retry" {
                    if let Some(retry_block) = self.atomic_retry_block {
                        // Pop any arena frames opened since the atomic loop
                        // head (nested do-block frames, bind-expression
                        // isolation frames). retry_block only pops the
                        // atomic's own frame — skipping these pops would
                        // leak one frame per retry iteration.
                        for _ in 0..self.atomic_arena_frames {
                            self.call_rt_void(builder, "knot_arena_pop_frame", &[]);
                        }
                        // Free every live pre-built hash-join index. These are
                        // heap `Box<HashIndex>` allocations (not arena-managed),
                        // so the arena-pop above does not reclaim them, and the
                        // normal-exit free loop in `compile_do` is unreachable
                        // from here. Without this each retry iteration leaks one
                        // box per active join. The stack is left untouched (it
                        // is balanced by `compile_do`'s own push/pop) — we only
                        // read it to know which SSA values are live at this site.
                        let live_indices = self.pending_index_frees.clone();
                        for idx in live_indices {
                            self.call_rt_void(builder, "knot_relation_index_free", &[idx]);
                        }
                        // Jump directly to the retry path, short-circuiting
                        // all subsequent code in the atomic body.
                        builder.ins().jump(retry_block, &[]);
                        // Create an unreachable block so subsequent codegen
                        // has somewhere to emit instructions (dead code).
                        let dead = builder.create_block();
                        builder.switch_to_block(dead);
                        builder.seal_block(dead);
                        return self.call_rt(builder, "knot_value_unit", &[]);
                    }
                    return self.call_rt(builder, "knot_stm_retry", &[]);
                }
                // `env` and `user_fns` were both consulted above; anything
                // reaching here is a genuinely undefined variable.
                self.push_codegen_error(
                    builder,
                    expr.span,
                    format!("codegen: undefined variable '{}'", name),
                )
            }

            ast::ExprKind::Constructor(name) => {
                if name == "True" || name == "False" {
                    let val = if name == "True" { 1i64 } else { 0i64 };
                    let arg = builder.ins().iconst(cranelift_codegen::ir::types::I32, val);
                    self.call_rt(builder, "knot_value_bool", &[arg])
                } else if matches!(self.nullable_ctors.get(name), Some(NullableRole::None)) {
                    // Nullable none: encode as null pointer
                    builder.ins().iconst(self.ptr_type, 0)
                } else {
                    // Bare constructor reference — return as a unit constructor
                    let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    self.call_rt(builder, "knot_value_constructor", &[tag_ptr, tag_len, unit])
                }
            }

            ast::ExprKind::SourceRef(name) => {
                // Check if this is a view reference
                let view_info = self.views.get(name).cloned();
                if let Some(view) = view_info {
                    if view.constant_columns.is_empty() && view.source_columns.is_empty() {
                        // Simple alias: read the underlying source directly
                        let schema = self
                            .source_schemas
                            .get(&view.source_name)
                            .cloned()
                            .unwrap_or_default();
                        let (name_ptr, name_len) =
                            self.string_ptr(builder, &view.source_name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt(
                            builder,
                            "knot_source_read",
                            &[db, name_ptr, name_len, schema_ptr, schema_len],
                        )
                    } else {
                        // Filtered/projected view: SELECT source columns WHERE constants match
                        let view_schema = self.compute_view_schema(&view);
                        let (src_to_view, _) = Self::compute_view_renames(&view);
                        let (filter_where, constant_cols) =
                            self.compute_view_filter(&view);

                        let filter_params = self.compile_view_filter_params(
                            builder,
                            &constant_cols,
                            env,
                            db,
                        );

                        let (name_ptr, name_len) =
                            self.string_ptr(builder, &view.source_name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &view_schema);
                        let (filter_ptr, filter_len) =
                            self.string_ptr(builder, &filter_where);

                        let result = self.call_rt(
                            builder,
                            "knot_view_read",
                            &[
                                db,
                                name_ptr,
                                name_len,
                                schema_ptr,
                                schema_len,
                                filter_ptr,
                                filter_len,
                                filter_params,
                            ],
                        );
                        // Rename source columns → view columns if any differ
                        if src_to_view.is_empty() {
                            result
                        } else {
                            let (map_ptr, map_len) = self.string_ptr(builder, &src_to_view);
                            self.call_rt(builder, "knot_relation_rename_columns", &[result, map_ptr, map_len])
                        }
                    }
                } else {
                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();
                    let (name_ptr, name_len) = self.string_ptr(builder, name);
                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                    let rel = self.call_rt(
                        builder,
                        "knot_source_read",
                        &[db, name_ptr, name_len, schema_ptr, schema_len],
                    );
                    if self.scalar_sources.contains(name) {
                        // Scalar source: unwrap first row's _value field,
                        // or return a default if the relation is empty.
                        self.call_rt(builder, "knot_scalar_source_unwrap", &[rel])
                    } else {
                        rel
                    }
                }
            }

            ast::ExprKind::DerivedRef(name) => {
                // For recursive derived relations, self-references use the
                // current accumulator value passed via the environment.
                let self_key = format!("__derived_self_{}", name);
                if let Some(&self_val) = env.bindings.get(&self_key) {
                    self_val
                } else if let Some((func_id, 0)) = self.user_fns.get(name).copied() {
                    let func_ref =
                        self.module.declare_func_in_func(func_id, builder.func);
                    let call = builder.ins().call(func_ref, &[db]);
                    builder.inst_results(call)[0]
                } else {
                    self.push_codegen_error(
                        builder,
                        expr.span,
                        format!("codegen: undefined derived relation '&{}'", name),
                    )
                }
            }

            ast::ExprKind::Record(fields) => {
                let n = fields.len();
                if n == 0 {
                    let n_val = builder.ins().iconst(self.ptr_type, 0);
                    self.call_rt(builder, "knot_record_empty", &[n_val])
                } else {
                    // Compile all field values (preserving evaluation order)
                    let mut compiled: Vec<(&str, Value)> = Vec::with_capacity(n);
                    for f in fields {
                        let val = self.compile_expr(builder, &f.value, env, db);
                        compiled.push((&f.name, val));
                    }
                    // Sort by field name at compile time (pre-sorted for runtime)
                    compiled.sort_by_key(|(name, _)| *name);

                    let ptr_bytes = self.ptr_type.bytes() as i32;
                    let slot_size = (3 * n as u32) * ptr_bytes as u32;
                    let slot = builder.create_sized_stack_slot(
                        StackSlotData::new(StackSlotKind::ExplicitSlot, slot_size, 3),
                    );
                    for (i, (name, val)) in compiled.iter().enumerate() {
                        let (key_ptr, key_len) = self.string_ptr(builder, name);
                        let base = (i as i32) * (3 * ptr_bytes);
                        builder.ins().stack_store(key_ptr, slot, base);
                        builder.ins().stack_store(key_len, slot, base + ptr_bytes);
                        builder.ins().stack_store(*val, slot, base + 2 * ptr_bytes);
                    }
                    let data_ptr = builder.ins().stack_addr(self.ptr_type, slot, 0);
                    let count = builder.ins().iconst(self.ptr_type, n as i64);
                    self.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count])
                }
            }

            ast::ExprKind::RecordUpdate { base, fields } => {
                let base_val = self.compile_expr(builder, base, env, db);
                let n = fields.len();
                // Compile and sort update fields for batch merge
                let mut compiled: Vec<(&str, Value)> = Vec::with_capacity(n);
                for f in fields {
                    let val = self.compile_expr(builder, &f.value, env, db);
                    compiled.push((&f.name, val));
                }
                compiled.sort_by_key(|(name, _)| *name);

                let ptr_bytes = self.ptr_type.bytes() as i32;
                let slot_size = (3 * n as u32) * ptr_bytes as u32;
                let slot = builder.create_sized_stack_slot(
                    StackSlotData::new(StackSlotKind::ExplicitSlot, slot_size, 3),
                );
                for (i, (name, val)) in compiled.iter().enumerate() {
                    let (key_ptr, key_len) = self.string_ptr(builder, name);
                    let base_off = (i as i32) * (3 * ptr_bytes);
                    builder.ins().stack_store(key_ptr, slot, base_off);
                    builder.ins().stack_store(key_len, slot, base_off + ptr_bytes);
                    builder.ins().stack_store(*val, slot, base_off + 2 * ptr_bytes);
                }
                let data_ptr = builder.ins().stack_addr(self.ptr_type, slot, 0);
                let count = builder.ins().iconst(self.ptr_type, n as i64);
                self.call_rt(builder, "knot_record_update_batch", &[base_val, data_ptr, count])
            }

            ast::ExprKind::FieldAccess { expr, field } => {
                let val = self.compile_expr(builder, expr, env, db);
                let (key_ptr, key_len) = self.string_ptr(builder, field);
                self.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len])
            }

            ast::ExprKind::List(elems) => {
                let rel = if elems.is_empty() {
                    self.call_rt(builder, "knot_relation_empty", &[])
                } else {
                    let cap = builder.ins().iconst(self.ptr_type, elems.len() as i64);
                    self.call_rt(builder, "knot_relation_with_capacity", &[cap])
                };
                for elem in elems {
                    let val = self.compile_expr(builder, elem, env, db);
                    self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
                }
                if elems.len() > 1 {
                    // A relation is a set: `[1, 1, 2]` holds two rows, not
                    // three. Every other path that builds a relation (union,
                    // map, comprehensions, writes) already dedups.
                    self.call_rt(builder, "knot_relation_dedup", &[rel])
                } else {
                    rel
                }
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                if matches!(op, ast::BinOp::Pipe) {
                    // Check for: source |> match Constructor → SQL-level match.
                    // Only when `match` is NOT shadowed — a locally-bound name
                    // (lambda param, `let`, do-bind, captured free var) or a
                    // user-declared top-level `match` must win over the builtin,
                    // mirroring `compile_app` (env-locals first) and the non-pipe
                    // `match` special form (guarded on `user_shadows_special`).
                    // Without this, `\match -> xs |> match Ctor` would call the
                    // builtin instead of the local value. When shadowed, fall
                    // through to `try_compile_pipe_sql`/`compile_app` below, which
                    // resolve the shadowing name correctly.
                    if let ast::ExprKind::App { func: match_fn, arg: match_arg } = &rhs.node
                        && let (ast::ExprKind::Var(fn_name), ast::ExprKind::Constructor(ctor_name)) = (&match_fn.node, &match_arg.node)
                            && fn_name == "match"
                            && !env.bindings.contains_key(fn_name)
                            && !(self.top_fn_names.contains(fn_name) && self.user_fns.contains_key(fn_name)) {
                                if let ast::ExprKind::SourceRef(source_name) = &lhs.node
                                    && let Some(schema) = self.source_schemas.get(source_name).cloned() {
                                        let (name_ptr, name_len) =
                                            self.string_ptr(builder, source_name);
                                        let (schema_ptr, schema_len) =
                                            self.string_ptr(builder, &schema);
                                        let (tag_ptr, tag_len) =
                                            self.string_ptr(builder, ctor_name);
                                        return self.call_rt(
                                            builder,
                                            "knot_source_match",
                                            &[
                                                db, name_ptr, name_len, schema_ptr,
                                                schema_len, tag_ptr, tag_len,
                                            ],
                                        );
                                    }
                                // Non-source: value-level match
                                let rel = self.compile_expr(builder, lhs, env, db);
                                let ctor = self.compile_expr(builder, match_arg, env, db);
                                return self.call_rt(
                                    builder,
                                    "knot_relation_match",
                                    &[ctor, rel],
                                );
                            }
                    // Try to compile the entire pipe chain to a single SQL query
                    if let Some(val) = self.try_compile_pipe_sql(builder, expr, env, db) {
                        return val;
                    }
                    // lhs |> rhs  =>  rhs(lhs); route through compile_app so
                    // application-form special cases (bare `count`, stdlib
                    // aggregates, per-op SQL pushdown) apply on the fallback
                    // path too — a raw knot_value_call can't resolve names
                    // like `count` that only exist as compile_app intrinsics.
                    let app = ast::Spanned::new(
                        ast::ExprKind::App {
                            func: rhs.clone(),
                            arg: lhs.clone(),
                        },
                        expr.span,
                    );
                    self.compile_app(builder, &app, env, db)
                } else if matches!(op, ast::BinOp::And | ast::BinOp::Or) {
                    // Short-circuit boolean ops: don't evaluate RHS if LHS determines result
                    let l = self.compile_expr(builder, lhs, env, db);
                    let l_bool = self.call_rt_typed(builder, "knot_value_get_bool", &[l], types::I32);
                    let l_true = builder.ins().icmp_imm(IntCC::NotEqual, l_bool, 0);

                    let rhs_block = builder.create_block();
                    let merge_block = builder.create_block();
                    merge_block_param(builder, merge_block, self.ptr_type);

                    if matches!(op, ast::BinOp::And) {
                        // &&: if l is false, short-circuit with l (false)
                        builder.ins().brif(l_true, rhs_block, &[], merge_block, &[l.into()]);
                    } else {
                        // ||: if l is true, short-circuit with l (true)
                        builder.ins().brif(l_true, merge_block, &[l.into()], rhs_block, &[]);
                    }

                    builder.switch_to_block(rhs_block);
                    builder.seal_block(rhs_block);
                    let r = self.compile_expr(builder, rhs, env, db);
                    builder.ins().jump(merge_block, &[r.into()]);

                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    builder.block_params(merge_block)[0]
                } else {
                    let l = self.compile_expr(builder, lhs, env, db);
                    let r = self.compile_expr(builder, rhs, env, db);
                    match op {
                        // Arithmetic: dispatch through Num trait
                        ast::BinOp::Add => self.compile_trait_binop(builder, "add", l, r, db, "knot_value_add"),
                        ast::BinOp::Sub => self.compile_trait_binop(builder, "sub", l, r, db, "knot_value_sub"),
                        ast::BinOp::Mul => self.compile_trait_binop(builder, "mul", l, r, db, "knot_value_mul"),
                        ast::BinOp::Div => self.compile_trait_binop(builder, "div", l, r, db, "knot_value_div"),
                        ast::BinOp::Mod => self.compile_trait_binop(builder, "mod", l, r, db, "knot_value_mod"),
                        // Equality: dispatch through Eq trait
                        ast::BinOp::Eq => self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq"),
                        ast::BinOp::Neq => {
                            let eq_result = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            self.call_rt(builder, "knot_value_not", &[eq_result])
                        },
                        // Comparison: dispatch through Ord trait (compare → Ordering)
                        ast::BinOp::Lt => self.compile_comparison(builder, l, r, db, "LT", false),
                        ast::BinOp::Gt => self.compile_comparison(builder, l, r, db, "GT", false),
                        ast::BinOp::Le => self.compile_comparison(builder, l, r, db, "GT", true),
                        ast::BinOp::Ge => self.compile_comparison(builder, l, r, db, "LT", true),
                        // Semigroup: dispatch through Semigroup trait
                        ast::BinOp::Concat => self.compile_trait_binop(builder, "append", l, r, db, "knot_value_concat"),
                        ast::BinOp::And | ast::BinOp::Or => unreachable!(),
                        ast::BinOp::Pipe => unreachable!(),
                    }
                }
            }

            ast::ExprKind::UnaryOp { op, operand } => {
                let val = self.compile_expr(builder, operand, env, db);
                match op {
                    // Negation: dispatch through Num trait
                    ast::UnaryOp::Neg => self.compile_trait_unop(builder, "negate", val, db, "knot_value_negate"),
                    // Boolean not: no trait dispatch
                    ast::UnaryOp::Not => self.call_rt(builder, "knot_value_not", &[val]),
                }
            }

            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let cond_i32 = self.compile_condition(builder, cond, env, db);
                let is_true =
                    builder.ins().icmp_imm(IntCC::NotEqual, cond_i32, 0);

                let then_block = builder.create_block();
                let else_block = builder.create_block();
                let merge_block = builder.create_block();
                merge_block_param(builder, merge_block, self.ptr_type);

                builder
                    .ins()
                    .brif(is_true, then_block, &[], else_block, &[]);

                builder.switch_to_block(then_block);
                builder.seal_block(then_block);
                let then_val =
                    self.compile_expr(builder, then_branch, &mut env.clone(), db);
                builder.ins().jump(merge_block, &[then_val.into()]);

                builder.switch_to_block(else_block);
                builder.seal_block(else_block);
                let else_val =
                    self.compile_expr(builder, else_branch, &mut env.clone(), db);
                builder.ins().jump(merge_block, &[else_val.into()]);

                builder.switch_to_block(merge_block);
                builder.seal_block(merge_block);
                builder.block_params(merge_block)[0]
            }

            ast::ExprKind::Lambda { params, body } => {
                self.compile_lambda(builder, params, body, env, db)
            }

            ast::ExprKind::App { func, arg } => {
                // Check for monadic yield: __yield(e) or yield(e)
                if let ast::ExprKind::Var(name) = &func.node
                    && (name == "__yield" || name == "yield") {
                        let val = self.compile_expr(builder, arg, env, db);
                        if self.in_io_eager {
                            return val;
                        }
                        return self.compile_monadic_yield(builder, val, func.span, db);
                    }
                self.compile_app(builder, expr, env, db)
            }

            ast::ExprKind::Do(stmts) => {
                if self.relational_do_spans.contains(&expr.span) {
                    // Produces the relation written by an enclosing
                    // set/replace, even if it binds from a source.
                    self.compile_do(builder, stmts, env, db)
                } else if self.is_io_do_block(stmts) {
                    self.compile_io_do(builder, stmts, env, db)
                } else if self.in_io_eager
                    && !stmts.iter().any(|s| matches!(&s.node, ast::StmtKind::Bind { .. }))
                {
                    // Pure do-block with no binds (no loops) nested inside
                    // an IO eager context: compile eagerly so that `yield`
                    // returns values directly instead of wrapping them in a
                    // relation.  Bind-free do-blocks are just sequential
                    // let/yield/where, which compile_io_do_eager handles
                    // correctly.
                    self.compile_io_do_eager(builder, stmts, env, db)
                } else {
                    self.compile_do(builder, stmts, env, db)
                }
            }

            ast::ExprKind::Set { target, value } => {
                // target should be a SourceRef (source or view)
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    // Check if target is a view
                    let view_info = self.views.get(name).cloned();
                    if let Some(view) = view_info {
                        return self.compile_view_set(builder, &view, name, value, env, db);
                    }

                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();

                    // Scalar source: wrap value as [{_value: val}] and do a full write
                    if self.scalar_sources.contains(name) {
                        let val = self.compile_set_value_expr(builder, value, env, db);
                        // Validate refinements on the raw value (wrap as [val] for the check)
                        if self.source_refinements.contains_key(name) {
                            let singleton = self.call_rt(builder, "knot_relation_singleton", &[val]);
                            self.emit_refinement_checks(builder, name, singleton, env, db);
                        }
                        let wrapped = self.call_rt(builder, "knot_scalar_source_wrap", &[val]);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, wrapped],
                        );
                        return self.call_rt(builder, "knot_value_unit", &[]);
                    }

                    // Inline let-bindings and named-function bodies before
                    // running shape matchers, so a let-bound `union`,
                    // filter-only do-block, or conditional update is
                    // recognised even when the value is a bare `Var`.
                    let inlined =
                        beta_reduce(value, &self.fun_bodies, &self.let_bindings);
                    let match_value: &ast::Expr = &inlined;

                    if let Some(new_rows_expr) = self.match_union_append(name, match_value) {
                        // 1. Append: union *rel <new> → INSERT only.
                        // Refinement checks must run here too: appended data
                        // is only pre-validated when it came through a route
                        // handler (HTTP body fields) — locally constructed
                        // rows (e.g. `union rows [{v: -5}]`) would otherwise
                        // bypass refined-type validation entirely.
                        let mut new_rows = self.compile_expr(builder, new_rows_expr, env, db);
                        // A do-block that binds from a source is classified as
                        // IO, so compile_expr produced a deferred Value::IO
                        // thunk rather than a Relation. knot_source_append needs
                        // a concrete Relation, so force the thunk with
                        // knot_io_run first (identity on non-IO values).
                        if let ast::ExprKind::Do(stmts) = &new_rows_expr.node
                            && self.is_io_do_block(stmts)
                        {
                            new_rows = self.call_rt(builder, "knot_io_run", &[db, new_rows]);
                        }
                        self.emit_refinement_checks(builder, name, new_rows, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_append",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, new_rows],
                        );
                    } else if !Self::references_source(match_value, name) {
                        // 2. Full replace: value doesn't read the source
                        let val = self.compile_set_value_expr(builder, value, env, db);
                        self.emit_refinement_checks(builder, name, val, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                        );
                    } else if !schema.contains('[')
                        && !self.source_refinements.contains_key(name)
                        && let Some((bind_var, cond, update_fields)) =
                            Self::match_conditional_update(name, match_value)
                    {
                        // 3. Conditional update: do { t <- *rel; yield (if cond then {t | ...} else t) }
                        //    Try SQL UPDATE WHERE (skip for nested relations — child tables need full rewrite)
                        //    Skip when source has refinements — SQL bypasses Knot-level validation
                        let where_frag = self.try_compile_sql_expr(&bind_var, cond, &schema);
                        let set_frag = where_frag.as_ref().and_then(|_| {
                            let mut parts = Vec::new();
                            let mut params = Vec::new();
                            for (field_name, field_val) in &update_fields {
                                match &field_val.node {
                                    ast::ExprKind::Lit(lit) => {
                                        parts.push(format!("{} = ?", quote_sql_ident(field_name)));
                                        params.push(SqlParamSource::Literal(lit.clone()));
                                    }
                                    ast::ExprKind::Var(name) => {
                                        parts.push(format!("{} = ?", quote_sql_ident(field_name)));
                                        params.push(SqlParamSource::Var(name.clone()));
                                    }
                                    _ => {
                                        // Try computed expression (e.g., p.price * 0.9)
                                        let atom = Self::try_compile_single_table_atom(
                                            &bind_var, field_val,
                                        )?;
                                        parts.push(format!(
                                            "{} = {}",
                                            quote_sql_ident(field_name),
                                            atom.sql
                                        ));
                                        params.extend(atom.params);
                                    }
                                }
                            }
                            Some(SqlFragment {
                                sql: parts.join(", "),
                                params,
                            })
                        });

                        if let (Some(wf), Some(sf)) = (where_frag, set_frag) {
                            // SQL compilation succeeded → UPDATE WHERE
                            let mut all_params = sf.params;
                            all_params.extend(wf.params);
                            let params_rel =
                                self.compile_sql_params(builder, &all_params, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let set_sql = sf.sql;
                            let where_sql = wf.sql;
                            let (set_ptr, set_len) =
                                self.string_ptr(builder, &set_sql);
                            let (where_ptr, where_len) =
                                self.string_ptr(builder, &where_sql);
                            self.call_rt_void(
                                builder,
                                "knot_source_update_where",
                                &[
                                    db, name_ptr, name_len, set_ptr, set_len,
                                    where_ptr, where_len, params_rel,
                                ],
                            );
                        } else {
                            // SQL compilation failed → map with no filter → full write
                            let val = self.compile_set_value_expr(builder, value, env, db);
                            self.emit_refinement_checks(builder, name, val, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            self.call_rt_void(
                                builder,
                                "knot_source_write",
                                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                            );
                        }
                    } else if !schema.contains('[')
                        && let Some((bind_var, conditions)) =
                            Self::match_filter_only(name, match_value)
                    {
                        // 4. Filter only: do { t <- *rel; where cond; yield t }
                        //    Try SQL DELETE WHERE (skip for nested relations — child tables need full rewrite)
                        let combined_sql: Option<SqlFragment> = {
                            let mut frags = Vec::new();
                            let mut all_ok = true;
                            for cond in &conditions {
                                if let Some(f) =
                                    self.try_compile_sql_expr(&bind_var, cond, &schema)
                                {
                                    frags.push(f);
                                } else {
                                    all_ok = false;
                                    break;
                                }
                            }
                            if all_ok && !frags.is_empty() {
                                let sql = frags
                                    .iter()
                                    .map(|f| format!("({})", f.sql))
                                    .collect::<Vec<_>>()
                                    .join(" AND ");
                                let params: Vec<SqlParamSource> = frags
                                    .into_iter()
                                    .flat_map(|f| f.params)
                                    .collect();
                                Some(SqlFragment { sql, params })
                            } else {
                                None
                            }
                        };

                        if let Some(frag) = combined_sql {
                            // SQL compilation succeeded → DELETE WHERE NOT (cond)
                            let params_rel =
                                self.compile_sql_params(builder, &frag.params, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let where_sql = frag.sql;
                            let (where_ptr, where_len) =
                                self.string_ptr(builder, &where_sql);
                            self.call_rt_void(
                                builder,
                                "knot_source_delete_where",
                                &[db, name_ptr, name_len, where_ptr, where_len, params_rel],
                            );
                        } else {
                            // SQL compilation failed → fall back to diff-write
                            let val = self.compile_set_value_expr(builder, value, env, db);
                            self.emit_refinement_checks(builder, name, val, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            self.call_rt_void(
                                builder,
                                "knot_source_diff_write",
                                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                            );
                        }
                    } else if Self::match_map_no_filter(name, match_value) {
                        // 5. Map without filter: every row transformed, no filtering
                        //    Full write is safe and avoids diff overhead.
                        let val = self.compile_set_value_expr(builder, value, env, db);
                        self.emit_refinement_checks(builder, name, val, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                        );
                    } else {
                        // 6. Fallback: diff-based write
                        let val = self.compile_set_value_expr(builder, value, env, db);
                        self.emit_refinement_checks(builder, name, val, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_diff_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                        );
                    }
                    self.call_rt(builder, "knot_value_unit", &[])
                } else {
                    self.push_codegen_error(
                        builder,
                        target.span,
                        "codegen: set target must be a source reference",
                    )
                }
            }

            ast::ExprKind::ReplaceSet { target, value } => {
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    // Check if target is a view
                    let view_info = self.views.get(name).cloned();
                    if let Some(view) = view_info {
                        return self.compile_view_set(builder, &view, name, value, env, db);
                    }

                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();

                    // Scalar source: wrap value as [{_value: val}] and do a full write
                    if self.scalar_sources.contains(name) {
                        let val = self.compile_set_value_expr(builder, value, env, db);
                        // Validate refinements on the raw value (wrap as [val] for the check)
                        if self.source_refinements.contains_key(name) {
                            let singleton = self.call_rt(builder, "knot_relation_singleton", &[val]);
                            self.emit_refinement_checks(builder, name, singleton, env, db);
                        }
                        let wrapped = self.call_rt(builder, "knot_scalar_source_wrap", &[val]);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, wrapped],
                        );
                        return self.call_rt(builder, "knot_value_unit", &[]);
                    }

                    let val = self.compile_set_value_expr(builder, value, env, db);
                    self.emit_refinement_checks(builder, name, val, env, db);
                    let (name_ptr, name_len) = self.string_ptr(builder, name);
                    let (schema_ptr, schema_len) =
                        self.string_ptr(builder, &schema);
                    self.call_rt_void(
                        builder,
                        "knot_source_write",
                        &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                    );
                    self.call_rt(builder, "knot_value_unit", &[])
                } else {
                    self.push_codegen_error(
                        builder,
                        target.span,
                        "codegen: replace target must be a source reference",
                    )
                }
            }

            ast::ExprKind::Atomic(inner) => {
                let is_nested = self.atomic_retry_block.is_some();
                // Whether the body might issue a SQL write (Set/ReplaceSet,
                // direct or via a user fn). If not, the SAVEPOINT can be
                // skipped — the version-snapshot retry machinery already
                // provides the consistency guarantees we need for a
                // read-only body, and skipping avoids a WAL write per retry.
                let body_writes = Self::expr_contains_writes(
                    inner,
                    &self.write_functions,
                    &self.top_fn_names,
                    &self.passthrough_functions,
                );

                // For nested atomics, save outer STM tracking before the loop
                // so inner snapshot/retry doesn't destroy outer read/write sets.
                if is_nested {
                    self.call_rt_void(builder, "knot_stm_push", &[]);
                }

                // Retry loop: if `retry` is called inside, rollback and wait for changes
                let loop_block = builder.create_block();
                let retry_block = builder.create_block();
                let done_block = builder.create_block();
                builder.append_block_param(done_block, self.ptr_type);

                builder.ins().jump(loop_block, &[]);
                // loop_block sealed after retry jump is emitted (two predecessors)
                builder.switch_to_block(loop_block);

                // Arena frame: each retry iteration pushes a fresh frame so
                // allocations from the body (SQL reads including large blobs)
                // are freed on retry instead of accumulating unboundedly.
                self.call_rt_void(builder, "knot_arena_push_frame", &[]);

                // Snapshot change counter before executing the body
                let snapshot = self.call_rt(builder, "knot_stm_snapshot", &[]);
                if body_writes {
                    self.call_rt_void(builder, "knot_atomic_begin", &[db]);
                }

                // Set retry block so `retry` keyword can jump directly here,
                // short-circuiting execution instead of using a flag.
                let prev_retry_block = self.atomic_retry_block;
                self.atomic_retry_block = Some(retry_block);
                // Frames pushed inside the body are counted relative to THIS
                // atomic's loop head (its own frame is popped by retry_block).
                let prev_atomic_arena_frames = self.atomic_arena_frames;
                self.atomic_arena_frames = 0;

                // Guard failures inside the atomic body must flow to ITS
                // done_block (so the skip flag triggers rollback) — never
                // to an enclosing bind-loop's skip block, which would jump
                // straight past the commit/rollback machinery and leak the
                // savepoint.
                let prev_io_loop_skip = self.io_loop_skip_block.take();

                // Compile inner IO eagerly so side effects run inside the transaction.
                // If the inner is an IO do-block, we must run it inline rather than
                // creating a deferred thunk (which would execute after commit).
                let val = if let ast::ExprKind::Do(stmts) = &inner.node {
                    if self.is_io_do_block(stmts) {
                        self.compile_io_do_eager(builder, stmts, env, db)
                    } else {
                        self.compile_expr(builder, inner, env, db)
                    }
                } else {
                    // Non-do inner (e.g. function call returning IO): compile it,
                    // then run the resulting IO thunk eagerly inside the transaction
                    let io_val = self.compile_expr(builder, inner, env, db);
                    self.call_rt(builder, "knot_io_run", &[db, io_val])
                };

                self.io_loop_skip_block = prev_io_loop_skip;
                self.atomic_retry_block = prev_retry_block;
                self.atomic_arena_frames = prev_atomic_arena_frames;

                // After the body completes without an explicit retry, three
                // outcomes are possible:
                //   1. retry was set     → rollback, wait for changes, loop
                //   2. skip flag was set → rollback, exit atomic with unit
                //                          (a constructor-pattern bind or
                //                          `where` guard failed inside the
                //                          body; partial writes must NOT be
                //                          committed)
                //   3. normal completion → commit and return body's value
                let retry_flag = self.call_rt(builder, "knot_stm_check_and_clear", &[]);
                let post_retry_block = builder.create_block();
                builder.append_block_param(post_retry_block, self.ptr_type);
                builder.ins().brif(retry_flag, retry_block, &[], post_retry_block, &[val.into()]);

                // (1) Retry path
                builder.switch_to_block(retry_block);
                builder.seal_block(retry_block);
                if body_writes {
                    self.call_rt_void(builder, "knot_atomic_rollback", &[db]);
                }
                self.call_rt_void(builder, "knot_arena_pop_frame", &[]);
                self.call_rt_void(builder, "knot_stm_wait", &[snapshot]);
                builder.ins().jump(loop_block, &[]);
                builder.seal_block(loop_block);

                // After the body returns without retry, check the skip flag.
                builder.switch_to_block(post_retry_block);
                builder.seal_block(post_retry_block);
                let post_val = builder.block_params(post_retry_block)[0];
                let skip_flag = self.call_rt(builder, "knot_stm_check_skip_and_clear", &[]);
                let skip_block = builder.create_block();
                let commit_block = builder.create_block();
                builder.ins().brif(skip_flag, skip_block, &[], commit_block, &[]);

                // (2) Skip path: rollback the savepoint (no commit), pop arena,
                // jump to done with unit so the surrounding IO continues.
                builder.switch_to_block(skip_block);
                builder.seal_block(skip_block);
                if body_writes {
                    self.call_rt_void(builder, "knot_atomic_rollback", &[db]);
                }
                self.call_rt_void(builder, "knot_arena_pop_frame", &[]);
                let unit_after_skip = self.call_rt(builder, "knot_value_unit", &[]);
                builder.ins().jump(done_block, &[unit_after_skip.into()]);

                // (3) Commit path: promote body's allocations to parent frame
                // (so they survive the frame pop), pop body frame, commit.
                builder.switch_to_block(commit_block);
                builder.seal_block(commit_block);
                let promoted = self.call_rt(builder, "knot_arena_pop_frame_promote", &[post_val]);
                if body_writes {
                    self.call_rt_void(builder, "knot_atomic_commit", &[db]);
                }
                builder.ins().jump(done_block, &[promoted.into()]);

                // Done: both skip and commit paths converge here.
                builder.switch_to_block(done_block);
                builder.seal_block(done_block);
                let result = builder.block_params(done_block)[0];

                // For nested atomics, restore outer tracking and merge inner
                if is_nested {
                    self.call_rt_void(builder, "knot_stm_pop_merge", &[]);
                }

                result
            }

            ast::ExprKind::Case {
                scrutinee,
                arms,
            } => self.compile_case(builder, scrutinee, arms, env, db),

            // `2 seconds` compiles exactly like its desugared `2 * 1000`.
            ast::ExprKind::TimeUnitLit { value, .. } => {
                self.compile_expr(builder, value, env, db)
            }

            ast::ExprKind::Annot { expr, .. } => {
                self.compile_expr(builder, expr, env, db)
            }

            ast::ExprKind::Refine(inner) => {
                self.compile_refine(builder, inner, expr.span, env, db)
            }

            ast::ExprKind::Serve { api, handlers, .. } => {
                self.compile_serve(builder, api, handlers, expr.span, env, db)
            }
        }
    }

    // ── View compilation ─────────────────────────────────────────

    /// Compute the view schema: subset of source schema for source columns only.
    /// Uses SOURCE column names (for correct SQL against the source table).
    fn compute_view_schema(&self, view: &ViewInfo) -> String {
        let source_schema = self
            .source_schemas
            .get(&view.source_name)
            .cloned()
            .unwrap_or_default();
        let src_col_set: std::collections::HashSet<&str> = view
            .source_columns
            .iter()
            .map(|(_, src_col)| src_col.as_str())
            .collect();
        split_schema_fields(&source_schema)
            .into_iter()
            .filter(|part| {
                let src_name = part.split(':').next().unwrap_or("");
                src_col_set.contains(src_name)
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Compute rename mapping strings for views that rename columns.
    /// Returns `(src_to_view, view_to_src)` — empty strings when no renames.
    fn compute_view_renames(view: &ViewInfo) -> (String, String) {
        let renames: Vec<(&str, &str)> = view
            .source_columns
            .iter()
            .filter(|(view_col, src_col)| view_col != src_col)
            .map(|(view_col, src_col)| (src_col.as_str(), view_col.as_str()))
            .collect();
        if renames.is_empty() {
            return (String::new(), String::new());
        }
        let src_to_view = renames
            .iter()
            .map(|(s, v)| format!("{}>{}",s, v))
            .collect::<Vec<_>>()
            .join(",");
        let view_to_src = renames
            .iter()
            .map(|(s, v)| format!("{}>{}",v, s))
            .collect::<Vec<_>>()
            .join(",");
        (src_to_view, view_to_src)
    }

    /// Compute the WHERE clause and constant column expressions for a view.
    fn compute_view_filter(&self, view: &ViewInfo) -> (String, Vec<(String, ast::Expr)>) {
        let filter_parts: Vec<String> = view
            .constant_columns
            .iter()
            .enumerate()
            .map(|(i, (name, _))| format!("{} = ?{}", quote_sql_ident(name), i + 1))
            .collect();
        let filter_where = filter_parts.join(" AND ");
        (filter_where, view.constant_columns.clone())
    }

    fn compile_view_set(
        &mut self,
        builder: &mut FunctionBuilder,
        view: &ViewInfo,
        view_name: &str,
        value: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        let source_name = view.source_name.clone();
        let source_schema = self
            .source_schemas
            .get(&source_name)
            .cloned()
            .unwrap_or_default();

        // Compute the view-filtered schema (only columns the view selects).
        // Uses source column names for correct SQL against the source table.
        let view_schema = if view.source_columns.is_empty() {
            source_schema.clone()
        } else {
            self.compute_view_schema(view)
        };

        // Compute rename mapping: view→source for writing (records have view names)
        let (_, view_to_src) = Self::compute_view_renames(view);

        // Check for append optimization: set *view = union *view newRows.
        // Inline let/fun bindings first so the union shape is visible
        // even when value is a let-bound `Var`.
        let inlined_value = beta_reduce(value, &self.fun_bodies, &self.let_bindings);
        if let Some(new_rows_expr) = self.match_union_append(view_name, &inlined_value) {
            let new_rows_expr = new_rows_expr.clone();
            let mut new_rows = self.compile_expr(builder, &new_rows_expr, env, db);
            // Rename view columns → source columns before writing
            if !view_to_src.is_empty() {
                let (map_ptr, map_len) = self.string_ptr(builder, &view_to_src);
                new_rows = self.call_rt(builder, "knot_relation_rename_columns", &[new_rows, map_ptr, map_len]);
            }
            let augmented =
                self.compile_view_augment(builder, new_rows, &view.constant_columns, env, db);
            let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
            // Augmented rows have view columns + constants, which may be a
            // subset of the full source schema for projected views.  Pass a
            // schema that covers exactly the columns present in the rows.
            let append_schema = if view.source_columns.is_empty() {
                source_schema.clone()
            } else {
                let mut present: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for (_, src_col) in &view.source_columns {
                    present.insert(src_col.as_str());
                }
                for (col_name, _) in &view.constant_columns {
                    present.insert(col_name.as_str());
                }
                split_schema_fields(&source_schema)
                    .into_iter()
                    .filter(|part| {
                        let name = part.split(':').next().unwrap_or("");
                        present.contains(name)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let (schema_ptr, schema_len) = self.string_ptr(builder, &append_schema);
            // Validate refined types on the rows actually written to the
            // underlying source (post-rename, post-constant-augment) — view
            // writes must not bypass the source's refinement predicates.
            let written_cols = schema_col_names(&append_schema);
            self.emit_refinement_checks_filtered(
                builder, &source_name, augmented, Some(&written_cols), env, db,
            );
            self.call_rt_void(
                builder,
                "knot_source_append",
                &[db, name_ptr, name_len, schema_ptr, schema_len, augmented],
            );
        } else if view.constant_columns.is_empty() {
            // No constant columns — use diff-write on underlying source
            // view_schema uses source column names for correct SQL.
            let mut val = self.compile_set_value_expr(builder, value, env, db);
            // Rename view columns → source columns before writing
            if !view_to_src.is_empty() {
                let (map_ptr, map_len) = self.string_ptr(builder, &view_to_src);
                val = self.call_rt(builder, "knot_relation_rename_columns", &[val, map_ptr, map_len]);
            }
            let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
            let (schema_ptr, schema_len) = self.string_ptr(builder, &view_schema);
            // Validate refined types on the rows written through the view
            // (post-rename) — view writes must not bypass the underlying
            // source's refinement predicates.
            let written_cols = schema_col_names(&view_schema);
            self.emit_refinement_checks_filtered(
                builder, &source_name, val, Some(&written_cols), env, db,
            );
            self.call_rt_void(
                builder,
                "knot_source_diff_write",
                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
            );
        } else {
            // General case: delete matching rows, insert new rows with constants
            let mut new_val = self.compile_set_value_expr(builder, value, env, db);
            // Rename view columns → source columns before writing
            if !view_to_src.is_empty() {
                let (map_ptr, map_len) = self.string_ptr(builder, &view_to_src);
                new_val = self.call_rt(builder, "knot_relation_rename_columns", &[new_val, map_ptr, map_len]);
            }
            let augmented =
                self.compile_view_augment(builder, new_val, &view.constant_columns, env, db);

            // Build filter WHERE clause
            let filter_parts: Vec<String> = view
                .constant_columns
                .iter()
                .enumerate()
                .map(|(i, (name, _))| format!("{} = ?{}", quote_sql_ident(name), i + 1))
                .collect();
            let filter_where = filter_parts.join(" AND ");

            // Build filter params
            let constant_cols = view.constant_columns.clone();
            let filter_params =
                self.compile_view_filter_params(builder, &constant_cols, env, db);

            let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
            // Augmented rows have view columns + constants, which may be a
            // subset of the full source schema for projected views.  Pass a
            // schema that covers exactly the columns present in the rows.
            let write_schema = if view.source_columns.is_empty() {
                source_schema.clone()
            } else {
                let mut present: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for (_, src_col) in &view.source_columns {
                    present.insert(src_col.as_str());
                }
                for (col_name, _) in &view.constant_columns {
                    present.insert(col_name.as_str());
                }
                split_schema_fields(&source_schema)
                    .into_iter()
                    .filter(|part| {
                        let name = part.split(':').next().unwrap_or("");
                        present.contains(name)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let (schema_ptr, schema_len) = self.string_ptr(builder, &write_schema);
            let (filter_ptr, filter_len) = self.string_ptr(builder, &filter_where);

            // Validate refined types on the rows actually written to the
            // underlying source (post-rename, post-constant-augment) — view
            // writes must not bypass the source's refinement predicates.
            let written_cols = schema_col_names(&write_schema);
            self.emit_refinement_checks_filtered(
                builder, &source_name, augmented, Some(&written_cols), env, db,
            );
            self.call_rt_void(
                builder,
                "knot_view_write",
                &[
                    db,
                    name_ptr,
                    name_len,
                    schema_ptr,
                    schema_len,
                    filter_ptr,
                    filter_len,
                    filter_params,
                    augmented,
                ],
            );
        }

        self.call_rt(builder, "knot_value_unit", &[])
    }

    /// Augment each row in a relation with constant column values.
    fn compile_view_augment(
        &mut self,
        builder: &mut FunctionBuilder,
        relation: Value,
        constant_columns: &[(String, ast::Expr)],
        env: &mut Env,
        db: Value,
    ) -> Value {
        if constant_columns.is_empty() {
            return relation;
        }

        // Build extra fields record
        let n = constant_columns.len();
        let n_val = builder.ins().iconst(self.ptr_type, n as i64);
        let extra = self.call_rt(builder, "knot_record_empty", &[n_val]);
        for (name, expr) in constant_columns {
            let val = self.compile_expr(builder, expr, env, db);
            let (key_ptr, key_len) = self.string_ptr(builder, name);
            self.call_rt_void(
                builder,
                "knot_record_set_field",
                &[extra, key_ptr, key_len, val],
            );
        }

        self.call_rt(builder, "knot_relation_add_fields", &[relation, extra])
    }

    /// Build a flat relation of SQL parameter values from constant column expressions.
    fn compile_view_filter_params(
        &mut self,
        builder: &mut FunctionBuilder,
        constant_columns: &[(String, ast::Expr)],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let rel = self.call_rt(builder, "knot_relation_empty", &[]);
        for (_, expr) in constant_columns {
            let val = self.compile_expr(builder, expr, env, db);
            self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
        }
        rel
    }

    // ── Application compilation ───────────────────────────────────

    /// Apply one invalidation to the live `source_var_binds` map (and, for
    /// rebinds, the `let_bindings` map) and append it to the log so
    /// enclosing scopes replay it after restoring their snapshots.
    fn apply_source_bind_invalidation(&mut self, inv: SourceBindInvalidation) {
        Self::replay_source_bind_invalidation(
            &inv,
            &mut self.source_var_binds,
            &mut self.let_bindings,
        );
        self.source_bind_invalidations.push(inv);
    }

    fn replay_source_bind_invalidation(
        inv: &SourceBindInvalidation,
        source_var_binds: &mut HashMap<String, String>,
        let_bindings: &mut HashMap<String, ast::Expr>,
    ) {
        match inv {
            SourceBindInvalidation::Rebind(name) => {
                source_var_binds.remove(name);
                let_bindings.remove(name);
            }
            SourceBindInvalidation::SourceWrite(src) => {
                source_var_binds.retain(|_, s| s != src);
            }
            SourceBindInvalidation::AllSources => {
                source_var_binds.clear();
            }
        }
    }

    /// Replay invalidations logged since `mark` onto the (just-restored)
    /// `source_var_binds`/`let_bindings` maps. Called when a do-block scope
    /// exits: the scope restored its entry snapshots, but writes and
    /// rebinds that happened inside the scope must still kill matching
    /// outer-scope entries.
    fn replay_source_bind_invalidations_since(&mut self, mark: usize) {
        let suffix: Vec<SourceBindInvalidation> =
            self.source_bind_invalidations[mark..].to_vec();
        for inv in &suffix {
            Self::replay_source_bind_invalidation(
                inv,
                &mut self.source_var_binds,
                &mut self.let_bindings,
            );
        }
    }

    /// Invalidate `source_var_binds` entries for every name bound by `pat`:
    /// a rebind means the variable no longer holds the rows read from its
    /// previous source, so SQL pushdown must not re-query the table for it.
    fn invalidate_rebound_pattern(&mut self, pat: &ast::Pat) {
        for name in pat_bound_names(pat) {
            if self.source_var_binds.contains_key(&name)
                || self.let_bindings.contains_key(&name)
            {
                self.apply_source_bind_invalidation(
                    SourceBindInvalidation::Rebind(name),
                );
            }
        }
    }

    /// Collect the write targets inside `expr` into `out`. Returns `false`
    /// when some write cannot be attributed to a plain source — a view or
    /// dynamic `Set` target, a reference to a possibly-writing user
    /// function, or a call through an unknown callee. The conservatism
    /// mirrors `expr_contains_writes` exactly: whenever that function
    /// reports "may write" for a reason other than a direct
    /// `Set`/`ReplaceSet`, this returns `false` so the caller invalidates
    /// every source binding.
    fn collect_direct_write_targets(
        &self,
        expr: &ast::Expr,
        out: &mut Vec<String>,
    ) -> bool {
        use ast::ExprKind::*;
        let name_is_known_write_free = |name: &str| -> bool {
            !self.write_functions.contains(name)
                && (self.top_fn_names.contains(name)
                    || is_builtin_name(name)
                    || matches!(name, "yield" | "__bind" | "__yield" | "__empty"))
        };
        match &expr.node {
            Set { target, value } | ReplaceSet { target, value } => {
                let target_ok = match &target.node {
                    SourceRef(name) if !self.views.contains_key(name) => {
                        out.push(name.clone());
                        true
                    }
                    _ => false,
                };
                target_ok && self.collect_direct_write_targets(value, out)
            }
            // A bare reference to a possibly-writing function: it may be
            // invoked later through a value we can't track.
            Var(name) => !self.write_functions.contains(name),
            Atomic(inner) | Refine(inner) => {
                self.collect_direct_write_targets(inner, out)
            }
            UnaryOp { operand, .. } => self.collect_direct_write_targets(operand, out),
            TimeUnitLit { value, .. } => self.collect_direct_write_targets(value, out),
            Annot { expr: e, .. } => self.collect_direct_write_targets(e, out),
            FieldAccess { expr: e, .. } => self.collect_direct_write_targets(e, out),
            App { func, arg } => {
                // Mirror `expr_contains_writes`: a call through an unknown
                // callee (parameter, do-local lambda, trait dispatcher,
                // computed expression) may write anything.
                let (head, _) = uncurry_app(expr);
                let head_attributable = match &strip_expr_wrappers(head).node {
                    Var(name) => name_is_known_write_free(name),
                    Constructor(_) => true,
                    Lambda { .. } => true, // body covered by recursion below
                    _ => false,
                };
                head_attributable
                    && self.collect_direct_write_targets(func, out)
                    && self.collect_direct_write_targets(arg, out)
            }
            BinOp { lhs, rhs, .. } => {
                self.collect_direct_write_targets(lhs, out)
                    && self.collect_direct_write_targets(rhs, out)
            }
            If { cond, then_branch, else_branch } => {
                self.collect_direct_write_targets(cond, out)
                    && self.collect_direct_write_targets(then_branch, out)
                    && self.collect_direct_write_targets(else_branch, out)
            }
            Case { scrutinee, arms } => {
                self.collect_direct_write_targets(scrutinee, out)
                    && arms
                        .iter()
                        .all(|a| self.collect_direct_write_targets(&a.body, out))
            }
            Do(stmts) => stmts.iter().all(|s| match &s.node {
                // Bind/expression statements RUN their value when it is an
                // IO action — an IO value of unknown provenance may write
                // anything (mirrors `expr_contains_writes`).
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Expr(expr) => {
                    let unknown_io = matches!(
                        &strip_expr_wrappers(expr).node,
                        Var(name) if !name_is_known_write_free(name)
                    );
                    !unknown_io && self.collect_direct_write_targets(expr, out)
                }
                ast::StmtKind::Let { expr, .. } => {
                    self.collect_direct_write_targets(expr, out)
                }
                ast::StmtKind::Where { cond } => {
                    self.collect_direct_write_targets(cond, out)
                }
                ast::StmtKind::GroupBy { key } => {
                    self.collect_direct_write_targets(key, out)
                }
            }),
            Record(fields) => fields
                .iter()
                .all(|f| self.collect_direct_write_targets(&f.value, out)),
            RecordUpdate { base, fields } => {
                self.collect_direct_write_targets(base, out)
                    && fields
                        .iter()
                        .all(|f| self.collect_direct_write_targets(&f.value, out))
            }
            List(items) => items
                .iter()
                .all(|e| self.collect_direct_write_targets(e, out)),
            Lambda { body, .. } => self.collect_direct_write_targets(body, out),
            Serve { handlers, .. } => handlers
                .iter()
                .all(|h| self.collect_direct_write_targets(&h.body, out)),
            Lit(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) => true,
        }
    }

    /// After compiling a do-block statement, invalidate any
    /// `source_var_binds` entries whose source the statement may have
    /// written. Direct `*src = ...` writes invalidate just that source;
    /// writes we can't attribute (view writes, calls into possibly-writing
    /// functions) invalidate everything. Invalidation only disables the
    /// SQL pushdown optimization — the general in-memory path stays
    /// correct — so being conservative here is safe.
    fn invalidate_after_possible_writes(&mut self, expr: &ast::Expr) {
        if !Self::expr_contains_writes(expr, &self.write_functions, &self.top_fn_names, &self.passthrough_functions) {
            return;
        }
        let mut direct: Vec<String> = Vec::new();
        if self.collect_direct_write_targets(expr, &mut direct) {
            for name in direct {
                self.apply_source_bind_invalidation(
                    SourceBindInvalidation::SourceWrite(name),
                );
            }
        } else {
            self.apply_source_bind_invalidation(SourceBindInvalidation::AllSources);
        }
    }

    /// Resolve an expression to a source relation name.
    /// Handles both `*source` (SourceRef) and bound variables from `x <- *source`.
    fn resolve_source(&self, expr: &ast::Expr) -> Option<String> {
        match &expr.node {
            ast::ExprKind::SourceRef(name) => Some(name.clone()),
            ast::ExprKind::Var(name) => self.source_var_binds.get(name).cloned(),
            _ => None,
        }
    }

    fn compile_app(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        // Uncurry nested applications
        let (func_expr, args) = uncurry_app(expr);

        // A user-defined top-level function shadows any same-named builtin,
        // stdlib function, or SQL-pushdown special form. When present, skip all
        // the name-based special-case dispatch below and fall through to the
        // normal `user_fns` call arm in the match — mirroring the `traverse`
        // special form, which already guards on `top_fn_names`. Without this a
        // top-level `count = \xs -> …` would silently be replaced by the
        // built-in `knot_source_query_count` SQL pushdown. (None of the
        // special-cased names — count/single/fold/filter/… — are prelude
        // functions, so this never spuriously suppresses a special form.)
        let user_shadows_special = matches!(
            &func_expr.node,
            ast::ExprKind::Var(name)
                if self.top_fn_names.contains(name) && self.user_fns.contains_key(name)
        );

        // A locally-bound name (lambda param, `let`, do-bind, captured free
        // var) shadows any same-named top-level function, builtin, stdlib
        // function, or SQL-pushdown special form. Resolve it dynamically so
        // the local value wins, BEFORE any of the name-based special-case
        // dispatch below — mirroring the bare-`Var` path in `compile_expr`,
        // which already consults `env` first. Without this, e.g.
        // `\helper -> helper 5` (shadowing a top-level `helper`) or
        // `\count -> count xs` (shadowing the stdlib `count`) would call the
        // global instead of the local value.
        if let ast::ExprKind::Var(name) = &func_expr.node
            && env.bindings.contains_key(name) {
                let compiled_args: Vec<Value> = args
                    .iter()
                    .map(|a| self.compile_arg_expr(builder, a, env, db))
                    .collect();
                let func_val = self.compile_expr(builder, func_expr, env, db);
                let mut result = func_val;
                for arg in &compiled_args {
                    result = self.call_rt(
                        builder,
                        "knot_value_call",
                        &[db, result, *arg],
                    );
                }
                return result;
            }

        // Special case: count *rel → SQL COUNT(*)
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "count" && args.len() == 1 && !user_shadows_special {
                if let Some(source_name) = self.resolve_source(args[0]) {
                    // Only for actual sources, not views
                    if !self.views.contains_key(&source_name)
                        && self.source_schemas.contains_key(&source_name)
                    {
                        self.emit_stm_track_read(builder, &source_name);
                        let (name_ptr, name_len) =
                            self.string_ptr(builder, &source_name);
                        return self.call_rt(
                            builder,
                            "knot_source_count",
                            &[db, name_ptr, name_len],
                        );
                    }
                }

                // count (filter f *source) → SELECT COUNT(*) FROM ... WHERE ...
                if let Some((source_name, filter_bind, filter_body)) =
                    extract_filter_on_source(args[0], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                {
                    let source_name: &str = &source_name;
                    let filter_body: &ast::Expr = &filter_body;
                    if !self.views.contains_key(source_name)
                        && let Some(schema) = self.source_schemas.get(source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[')
                                && let Some(frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                    let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                    let sql = format!("SELECT COUNT(*) FROM {} WHERE {}", table, frag.sql);
                                    self.emit_stm_track_read(builder, source_name);
                                    let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                    return self.call_rt(
                                        builder,
                                        "knot_source_query_count",
                                        &[db, sql_ptr, sql_len, params_rel],
                                    );
                                }
                }

                // count (do { x <- *source; where ...; yield x }) → SELECT COUNT(*) FROM ... WHERE ...
                if let ast::ExprKind::Do(stmts) = &args[0].node
                    && let Some(plan) = self.analyze_sql_plan(stmts, env) {
                        let tables_sql: Vec<String> = plan.tables.iter().map(|t| {
                            format!("{} AS {}", quote_sql_ident(&format!("_knot_{}", t.source_name)), t.alias)
                        }).collect();
                        let from = tables_sql.join(", ");
                        let sql = if plan.conditions.is_empty() {
                            format!("SELECT COUNT(*) FROM {}", from)
                        } else {
                            format!("SELECT COUNT(*) FROM {} WHERE {}", from, join_sql_conditions(&plan.conditions))
                        };
                        self.emit_stm_track_reads_for_plan(builder, &plan);
                        let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                        return self.call_rt(
                            builder,
                            "knot_source_query_count",
                            &[db, sql_ptr, sql_len, params_rel],
                        );
                    }
            }

        // Special case: single *rel / single (filter f *rel) / single (do {...}) → LIMIT 2
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "single" && args.len() == 1 && !user_shadows_special {
                // single *source → SELECT ... LIMIT 2 then knot_relation_single
                if let Some(source_name) = self.resolve_source(args[0])
                    && !self.views.contains_key(&source_name)
                        && let Some(schema) = self.source_schemas.get(&source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[') {
                                let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                let cols = parse_schema_columns(&schema).iter()
                                    .map(|(name, _)| quote_sql_ident(name))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                let sql = format!("SELECT {} FROM {} LIMIT 2", cols, table);
                                let params_rel = self.compile_sql_params(builder, &[], env, db);
                                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                let (tn_ptr, tn_len) = self.string_ptr(builder, &source_name);
                                self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                                let rel = self.call_rt(
                                    builder,
                                    "knot_source_query",
                                    &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                );
                                return self.call_rt(builder, "knot_relation_single", &[rel]);
                            }

                // single (filter f *source) → SELECT ... WHERE ... LIMIT 2
                if let Some((source_name, filter_bind, filter_body)) =
                    extract_filter_on_source(args[0], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                {
                    let filter_body: &ast::Expr = &filter_body;
                    if !self.views.contains_key(&source_name)
                        && let Some(schema) = self.source_schemas.get(&source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[')
                                && let Some(frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                    let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                    let cols = parse_schema_columns(&schema).iter()
                                        .map(|(name, _)| quote_sql_ident(name))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    let sql = format!("SELECT {} FROM {} WHERE {} LIMIT 2", cols, table, frag.sql);
                                    let preds = try_extract_field_preds(&filter_bind, filter_body);
                                    let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                    let (tn_ptr, tn_len) = self.string_ptr(builder, &source_name);
                                    self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                                    self.emit_stm_track_pred(builder, tn_ptr, tn_len, &preds, env, db);
                                    let rel = self.call_rt(
                                        builder,
                                        "knot_source_query",
                                        &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                    );
                                    return self.call_rt(builder, "knot_relation_single", &[rel]);
                                }
                }

                // single (do { x <- *source; where ...; yield x }) → SQL plan + LIMIT 2
                if let ast::ExprKind::Do(stmts) = &args[0].node
                    && let Some(plan) = self.analyze_sql_plan(stmts, env) {
                        let mut sql = plan.build_sql();
                        sql.push_str(" LIMIT 2");
                        let result_schema = plan.build_result_schema();
                        let preds = try_extract_preds_for_single_table_plan(stmts, &plan);
                        let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                        for table in &plan.tables {
                            let (tn_ptr, tn_len) = self.string_ptr(builder, &table.source_name);
                            self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                            if plan.tables.len() == 1 {
                                self.emit_stm_track_pred(builder, tn_ptr, tn_len, &preds, env, db);
                            }
                        }
                        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                        let rel = self.call_rt(
                            builder,
                            "knot_source_query",
                            &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                        );
                        return self.call_rt(builder, "knot_relation_single", &[rel]);
                    }
            }

        // Special case: fold f init <relation expression> → stream rows from SQLite
        // one-by-one through the fold function instead of materializing the whole
        // relation in memory.  Restricted to flat record sources (no ADTs, no
        // nested relation fields); other shapes fall back to knot_relation_fold.
        // We deliberately skip the bound-variable case (`fold f init sales` after
        // `sales <- *sales`) — the bind already materialised the rows, so
        // streaming would do a redundant SQL pass.  The wins are in the filter
        // and do-block paths, which would otherwise build an intermediate Vec.
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "fold" && args.len() == 3 && !user_shadows_special {
                // fold f init *source → SELECT cols FROM table; stream
                if let ast::ExprKind::SourceRef(source_name) = &args[2].node
                    && !self.views.contains_key(source_name)
                        && let Some(schema) = self.source_schemas.get(source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[') {
                                let source_name = source_name.clone();
                                let f = self.compile_expr(builder, args[0], env, db);
                                let init = self.compile_expr(builder, args[1], env, db);
                                let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
                                let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                return self.call_rt(
                                    builder,
                                    "knot_source_fold",
                                    &[db, f, init, name_ptr, name_len, schema_ptr, schema_len],
                                );
                            }

                // fold f init (filter g *source) → SELECT cols FROM table WHERE ...; stream
                if let Some((source_name, filter_bind, filter_body)) =
                    extract_filter_on_source(args[2], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                {
                    let filter_body: &ast::Expr = &filter_body;
                    if !self.views.contains_key(&source_name)
                        && let Some(schema) = self.source_schemas.get(&source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[')
                                && let Some(frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                    let f = self.compile_expr(builder, args[0], env, db);
                                    let init = self.compile_expr(builder, args[1], env, db);
                                    let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                    let cols = parse_schema_columns(&schema).iter()
                                        .map(|(name, _)| quote_sql_ident(name))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    let sql = format!("SELECT {} FROM {} WHERE {}", cols, table, frag.sql);
                                    let preds = try_extract_field_preds(&filter_bind, filter_body);
                                    let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                    let (tn_ptr, tn_len) = self.string_ptr(builder, &source_name);
                                    self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                                    self.emit_stm_track_pred(builder, tn_ptr, tn_len, &preds, env, db);
                                    return self.call_rt(
                                        builder,
                                        "knot_source_query_fold",
                                        &[db, f, init, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                    );
                                }
                }

                // fold f init (do { ... }) → SQL plan; stream
                if let ast::ExprKind::Do(stmts) = &args[2].node
                    && let Some(plan) = self.analyze_sql_plan(stmts, env) {
                        let f = self.compile_expr(builder, args[0], env, db);
                        let init = self.compile_expr(builder, args[1], env, db);
                        let sql = plan.build_sql();
                        let result_schema = plan.build_result_schema();
                        let preds = try_extract_preds_for_single_table_plan(stmts, &plan);
                        let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                        for table in &plan.tables {
                            let (tn_ptr, tn_len) = self.string_ptr(builder, &table.source_name);
                            self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                            if plan.tables.len() == 1 {
                                self.emit_stm_track_pred(builder, tn_ptr, tn_len, &preds, env, db);
                            }
                        }
                        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                        return self.call_rt(
                            builder,
                            "knot_source_query_fold",
                            &[db, f, init, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                        );
                    }
            }

        // Special case: match Constructor SourceRef → SQL-level filtered read
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "match" && args.len() == 2 && !user_shadows_special
                && let ast::ExprKind::Constructor(ctor_name) = &args[0].node {
                    if let ast::ExprKind::SourceRef(source_name) = &args[1].node
                        && let Some(schema) = self.source_schemas.get(source_name).cloned() {
                            let source_name = source_name.clone();
                            self.emit_stm_track_read(builder, &source_name);
                            let (name_ptr, name_len) =
                                self.string_ptr(builder, &source_name);
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            let (tag_ptr, tag_len) =
                                self.string_ptr(builder, ctor_name);
                            return self.call_rt(
                                builder,
                                "knot_source_match",
                                &[
                                    db, name_ptr, name_len, schema_ptr,
                                    schema_len, tag_ptr, tag_len,
                                ],
                            );
                        }
                    // Non-source relation: compile and use value-level match
                    let rel = self.compile_expr(builder, args[1], env, db);
                    let ctor = self.compile_expr(builder, args[0], env, db);
                    return self.call_rt(
                        builder,
                        "knot_relation_match",
                        &[ctor, rel],
                    );
                }

        // Special case: filter/sum/avg with lambda on source → SQL
        if let ast::ExprKind::Var(name) = &func_expr.node
            && args.len() == 2 && !user_shadows_special {
                if let Some(source_name) = self.resolve_source(args[1])
                    && !self.views.contains_key(&source_name)
                        && let Some(schema) = self.source_schemas.get(&source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[')
                                && let Some(result) = self.try_compile_app_sql(
                                    builder, name, args[0], &source_name, &schema, env, db,
                                ) {
                                    return result;
                                }

                // sum/avg/min/max lambda (filter f *source) → SQL aggregate with WHERE
                if let Some((sql_func, rt_fn)) = aggregate_sql_func_runtime(name) {
                    if let Some((source_name, filter_bind, filter_body)) =
                        extract_filter_on_source(args[1], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                    {
                        let source_name: &str = &source_name;
                        let filter_body: &ast::Expr = &filter_body;
                        if !self.views.contains_key(source_name)
                            && let Some(schema) = self.source_schemas.get(source_name).cloned()
                                && !schema.starts_with('#') && !schema.contains('[')
                                    && let Some((agg_bind, agg_body)) = extract_single_param_lambda(args[0], &self.fun_bodies, &self.let_bindings) {
                                        let agg_body: &ast::Expr = &agg_body;
                                        // MIN/MAX over non-numeric columns must
                                        // stay in memory (see minmax_pushdown_type_ok).
                                        let minmax_ok = !matches!(name.as_str(), "minOn" | "maxOn")
                                            || minmax_pushdown_type_ok(&agg_bind, agg_body, &schema);
                                        if let (true, Some(col_sql)) = (minmax_ok, extract_sql_field_access(&agg_bind, agg_body, "", &schema))
                                            && let Some(frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                                let arg_sql = if matches!(name.as_str(), "minOn" | "maxOn") {
                                                    col_sql_for_minmax(&col_sql, &agg_bind, agg_body, &schema)
                                                } else {
                                                    col_sql
                                                };
                                                let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                                let sql = format!("SELECT {}({}) FROM {} WHERE {}", sql_func, arg_sql, table, frag.sql);
                                                self.emit_stm_track_read(builder, source_name);
                                                let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                                                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                if rt_fn == "knot_source_query_value" {
                                                    let is_text = builder.ins().iconst(
                                                        types::I64,
                                                        minmax_result_is_text(&agg_bind, agg_body, &schema) as i64,
                                                    );
                                                    return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_text]);
                                                }
                                                if rt_fn == "knot_source_query_sum" {
                                                    let is_float = builder.ins().iconst(
                                                        types::I64,
                                                        sum_result_is_float(&agg_bind, agg_body, &schema) as i64,
                                                    );
                                                    return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_float]);
                                                }
                                                return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel]);
                                            }
                                    }
                    }

                    // sum/avg/min/max lambda (do { ... }) → SQL aggregate from plan.
                    // The lambda sees the do-block's *projected* rows, so its
                    // field must be resolved through the yield projection —
                    // `sum (\x -> x.amt) (do i <- *t; yield {amt: i.qty})`
                    // aggregates the underlying `qty` column. Computed or
                    // unmappable projections fall back to in-memory.
                    if let ast::ExprKind::Do(stmts) = &args[1].node
                        && let Some(plan) = self.analyze_sql_plan(stmts, env)
                            && let Some((agg_bind, agg_body)) = extract_single_param_lambda(args[0], &self.fun_bodies, &self.let_bindings) {
                                let agg_body: &ast::Expr = &agg_body;
                                // Resolve the aggregate column: a plain
                                // `\x -> x.field` maps through the projection;
                                // arithmetic bodies are only allowed when the
                                // projection is the identity (single table).
                                let col_info: Option<(String, String)> = match &agg_body.node {
                                    ast::ExprKind::FieldAccess { expr: fa_inner, field }
                                        if matches!(&fa_inner.node, ast::ExprKind::Var(v) if v == &agg_bind) =>
                                    {
                                        plan_projection_column(&plan, field)
                                    }
                                    _ if plan.tables.len() == 1
                                        && plan_projection_is_identity(&plan) =>
                                    {
                                        let alias = &plan.tables[0].alias;
                                        let schema = self
                                            .source_schemas
                                            .get(&plan.tables[0].source_name)
                                            .cloned()
                                            .unwrap_or_default();
                                        // MIN/MAX over an Int-typed CASE loses
                                        // the KNOT_INT collation, and Float
                                        // MIN/MAX diverges from total_cmp —
                                        // keep both in memory (see
                                        // minmax_pushdown_type_ok).
                                        let case_ok = !matches!(name.as_str(), "minOn" | "maxOn")
                                            || minmax_pushdown_type_ok(&agg_bind, agg_body, &schema);
                                        if case_ok {
                                            extract_sql_field_access(&agg_bind, agg_body, alias, &schema)
                                                .map(|col_sql| {
                                                    let ty = infer_sql_expr_type(&agg_bind, agg_body, &schema)
                                                        .unwrap_or_else(|| "float".to_string());
                                                    (col_sql, ty)
                                                })
                                        } else {
                                            None
                                        }
                                    }
                                    _ => None,
                                };
                                let col_info = col_info.filter(|(_, col_ty)| {
                                    // MIN/MAX over Float projected columns must
                                    // stay in memory (total_cmp divergence, see
                                    // minmax_pushdown_type_ok): SQLite stores NaN
                                    // as NULL and skips it, and conflates ±0.0,
                                    // both diverging from Knot's `total_cmp`. Only
                                    // Int and Text push down — the runtime's
                                    // `is_text` flag keeps Text results from being
                                    // re-parsed as Int.
                                    !matches!(name.as_str(), "minOn" | "maxOn")
                                        || col_ty == "int"
                                        || col_ty == "text"
                                });
                                if let Some((col_sql, col_ty)) = col_info {
                                    let col_is_text = col_ty == "text";
                                    let arg_sql = if matches!(name.as_str(), "minOn" | "maxOn")
                                        && col_ty == "int"
                                    {
                                        format!("{} COLLATE KNOT_INT", col_sql)
                                    } else {
                                        col_sql
                                    };
                                    let tables_sql: Vec<String> = plan.tables.iter().map(|t| {
                                        format!("{} AS {}", quote_sql_ident(&format!("_knot_{}", t.source_name)), t.alias)
                                    }).collect();
                                    let from = tables_sql.join(", ");
                                    let sql = if plan.conditions.is_empty() {
                                        format!("SELECT {}({}) FROM {}", sql_func, arg_sql, from)
                                    } else {
                                        format!("SELECT {}({}) FROM {} WHERE {}", sql_func, arg_sql, from, join_sql_conditions(&plan.conditions))
                                    };
                                    self.emit_stm_track_reads_for_plan(builder, &plan);
                                    let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                    if rt_fn == "knot_source_query_value" {
                                        let is_text = builder.ins().iconst(types::I64, col_is_text as i64);
                                        return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_text]);
                                    }
                                    if rt_fn == "knot_source_query_sum" {
                                        let is_float = builder.ins().iconst(types::I64, (col_ty == "float") as i64);
                                        return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_float]);
                                    }
                                    return self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel]);
                                }
                            }
                }

                // countWhere predicate (filter f *source) → SELECT COUNT(*) FROM ... WHERE pred AND filter
                if name == "countWhere" {
                    if let Some((source_name, filter_bind, filter_body)) =
                        extract_filter_on_source(args[1], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                    {
                        let source_name: &str = &source_name;
                        let filter_body: &ast::Expr = &filter_body;
                        if !self.views.contains_key(source_name)
                            && let Some(schema) = self.source_schemas.get(source_name).cloned()
                                && !schema.starts_with('#') && !schema.contains('[')
                                    && let Some((pred_bind, pred_body)) = extract_single_param_lambda(args[0], &self.fun_bodies, &self.let_bindings) {
                                        let pred_body: &ast::Expr = &pred_body;
                                        if let Some(pred_frag) = self.try_compile_sql_expr(&pred_bind, pred_body, &schema)
                                            && let Some(filter_frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                                let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                                let sql = format!(
                                                    "SELECT COUNT(*) FROM {} WHERE ({}) AND ({})",
                                                    table, filter_frag.sql, pred_frag.sql,
                                                );
                                                let mut all_params = filter_frag.params;
                                                all_params.extend(pred_frag.params);
                                                self.emit_stm_track_read(builder, source_name);
                                                let params_rel = self.compile_sql_params(builder, &all_params, env, db);
                                                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                return self.call_rt(
                                                    builder,
                                                    "knot_source_query_count",
                                                    &[db, sql_ptr, sql_len, params_rel],
                                                );
                                            }
                                    }
                    }

                    // countWhere predicate (do { ... }) → SELECT COUNT(*) FROM plan WHERE pred.
                    // The predicate sees the do-block's *projected* rows, so
                    // its field references must be rewritten through the
                    // yield projection before compiling against the base
                    // table; multi-table plans and computed projections fall
                    // back to in-memory evaluation.
                    if let ast::ExprKind::Do(stmts) = &args[1].node
                        && let Some(plan) = self.analyze_sql_plan(stmts, env)
                            && plan.tables.len() == 1
                                && let Some((pred_bind, pred_body)) = extract_single_param_lambda(args[0], &self.fun_bodies, &self.let_bindings) {
                                    let pred_body: &ast::Expr = &pred_body;
                                    if let Some(rewritten) =
                                        rewrite_body_through_projection(&plan, &pred_bind, pred_body)
                                    {
                                        let mut bind_aliases: HashMap<String, String> = HashMap::new();
                                        bind_aliases.insert(pred_bind.clone(), plan.tables[0].alias.clone());
                                        let mut bind_schemas: HashMap<String, String> = HashMap::new();
                                        if let Some(schema) =
                                            self.source_schemas.get(&plan.tables[0].source_name)
                                        {
                                            bind_schemas.insert(pred_bind.clone(), schema.clone());
                                        }
                                        if let Some(pred_frag) = Self::try_compile_multi_table_sql_expr(
                                            &bind_aliases, &bind_schemas, &rewritten, env, &HashMap::new(),
                                        ) {
                                            let tables_sql: Vec<String> = plan.tables.iter().map(|t| {
                                                format!("{} AS {}", quote_sql_ident(&format!("_knot_{}", t.source_name)), t.alias)
                                            }).collect();
                                            let from = tables_sql.join(", ");
                                            let mut all_conditions = plan.conditions.clone();
                                            all_conditions.push(pred_frag.sql);
                                            let sql = format!(
                                                "SELECT COUNT(*) FROM {} WHERE {}",
                                                from,
                                                join_sql_conditions(&all_conditions),
                                            );
                                            self.emit_stm_track_reads_for_plan(builder, &plan);
                                            let mut all_params = plan.params.clone();
                                            all_params.extend(pred_frag.params);
                                            let params_rel = self.compile_sql_params(builder, &all_params, env, db);
                                            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                            return self.call_rt(
                                                builder,
                                                "knot_source_query_count",
                                                &[db, sql_ptr, sql_len, params_rel],
                                            );
                                        }
                                    }
                                }
                }

                // filter/sortBy lambda (do { ... }) → merge into SQL plan.
                // The lambda sees the do-block's *projected* rows, so its
                // field references are rewritten through the yield
                // projection before being compiled against the base table;
                // computed projections fall back to in-memory evaluation.
                if matches!(name.as_str(), "filter" | "sortBy")
                    && let ast::ExprKind::Do(stmts) = &args[1].node
                        && let Some(mut plan) = self.analyze_sql_plan(stmts, env)
                            && let Some((bind_var, body)) = extract_single_param_lambda(args[0], &self.fun_bodies, &self.let_bindings) {
                                let body: &ast::Expr = &body;
                                match name.as_str() {
                                    "filter" => {
                                        // For single-table plans, the bind var maps to the table alias
                                        if plan.tables.len() == 1
                                            && let Some(rewritten) =
                                                rewrite_body_through_projection(&plan, &bind_var, body)
                                            {
                                                let mut ba = HashMap::new();
                                                ba.insert(bind_var.clone(), plan.tables[0].alias.clone());
                                                let mut bs = HashMap::new();
                                                if let Some(schema) =
                                                    self.source_schemas.get(&plan.tables[0].source_name)
                                                {
                                                    bs.insert(bind_var.clone(), schema.clone());
                                                }
                                                if let Some(frag) = Self::try_compile_multi_table_sql_expr(
                                                    &ba, &bs, &rewritten, env, &HashMap::new(),
                                                ) {
                                                    plan.conditions.push(frag.sql);
                                                    plan.params.extend(frag.params);
                                                    let sql = plan.build_sql();
                                                    let result_schema = plan.build_result_schema();
                                                    self.emit_stm_track_reads_for_plan(builder, &plan);
                                                    let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                                                    return self.call_rt(
                                                        builder,
                                                        "knot_source_query",
                                                        &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                                    );
                                                }
                                            }
                                    }
                                    "sortBy"
                                        if plan.tables.len() == 1 => {
                                            let alias = plan.tables[0].alias.clone();
                                            let schema = self.source_schemas
                                                .get(&plan.tables[0].source_name)
                                                .cloned()
                                                .unwrap_or_default();
                                            if let Some(rewritten) =
                                                rewrite_body_through_projection(&plan, &bind_var, body)
                                                // Same ORDER BY guards as every other sortBy path:
                                                // no Int CASE (KNOT_INT collation is lost through CASE)
                                                // and no Float key (SQLite conflates -0.0/+0.0 and sorts
                                                // NaN as NULL, diverging from in-memory total_cmp) —
                                                // fall back to in-memory otherwise.
                                                && sortby_projection_pushable(&bind_var, &rewritten, &schema)
                                                && let Some(col_sql) = extract_sql_field_access(
                                                    &bind_var, &rewritten, &alias, &schema,
                                                ) {
                                                    plan.order_by.push(col_sql);
                                                    let sql = plan.build_sql();
                                                    let result_schema = plan.build_result_schema();
                                                    self.emit_stm_track_reads_for_plan(builder, &plan);
                                                    let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                                                    return self.call_rt(
                                                        builder,
                                                        "knot_source_query",
                                                        &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                                    );
                                                }
                                        }
                                    _ => {}
                                }
                            }
            }

        // Special case: takeRelation N (sortBy f *source) → SQL ORDER BY + LIMIT
        if let ast::ExprKind::Var(name) = &func_expr.node
            && (name == "takeRelation" || name == "take") && args.len() == 2 && !user_shadows_special {
                // args[0] = N, args[1] = sortBy f *source (or just *source)
                if let ast::ExprKind::App { func: sort_func, arg: sort_source } = &args[1].node
                    && let ast::ExprKind::App { func: sort_name_expr, arg: sort_lambda } = &sort_func.node
                        && let ast::ExprKind::Var(sort_name) = &sort_name_expr.node
                            && sort_name == "sortBy"
                                && let Some((sort_bind, sort_body)) = extract_single_param_lambda(sort_lambda, &self.fun_bodies, &self.let_bindings) {
                                    let sort_body: &ast::Expr = &sort_body;
                                    // Case 1: sortBy f *source → SQL ORDER BY + LIMIT
                                    if let ast::ExprKind::SourceRef(source_name) = &sort_source.node
                                        && !self.views.contains_key(source_name)
                                            && let Some(schema) = self.source_schemas.get(source_name).cloned()
                                                && !schema.starts_with('#') && !schema.contains('[') {
                                                    // Same ORDER BY guards as the other sortBy paths:
                                                    // no Int CASE (collation loss), no Float (total_cmp
                                                    // divergence) — fall back to in-memory otherwise.
                                                    if sortby_projection_pushable(&sort_bind, sort_body, &schema)
                                                    && let Some(col_sql) = extract_sql_field_access(&sort_bind, sort_body, "", &schema) {
                                                        let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                                        let cols = parse_schema_columns(&schema).iter()
                                                            .map(|(n, _)| quote_sql_ident(n))
                                                            .collect::<Vec<_>>()
                                                            .join(", ");
                                                        let sql = format!("SELECT {} FROM {} ORDER BY {} LIMIT MAX(CAST(? AS INTEGER), 0)", cols, table, col_sql);
                                                        let source_name = source_name.clone();
                                                        self.emit_stm_track_read(builder, &source_name);
                                                        let n_val = self.compile_expr(builder, args[0], env, db);
                                                        let params_rel = self.call_rt(builder, "knot_relation_singleton", &[n_val]);
                                                        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                        let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                                        return self.call_rt(
                                                            builder,
                                                            "knot_source_query",
                                                            &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                                        );
                                                    }
                                                }
                                    // Case 2: sortBy f (do { m <- *source; where ...; yield m })
                                    // → SQL WHERE + ORDER BY + LIMIT
                                    if let ast::ExprKind::Do(do_stmts) = &sort_source.node
                                        && let Some(mut plan) = self.analyze_sql_plan(do_stmts, env)
                                            && plan.tables.len() == 1 {
                                                let alias = plan.tables[0].alias.clone();
                                                let schema = self.source_schemas
                                                    .get(&plan.tables[0].source_name)
                                                    .cloned()
                                                    .unwrap_or_default();
                                                // Sort lambda sees projected rows: map its field
                                                // refs through the yield projection (fall back on
                                                // computed projections).
                                                let rewritten_sort =
                                                    rewrite_body_through_projection(&plan, &sort_bind, sort_body);
                                                if let Some(col_sql) = rewritten_sort.as_ref().and_then(|rb| {
                                                    // Same ORDER BY guards as the other sortBy paths:
                                                    // no Int CASE collation loss, no Float total_cmp
                                                    // divergence.
                                                    if !sortby_projection_pushable(&sort_bind, rb, &schema) {
                                                        return None;
                                                    }
                                                    extract_sql_field_access(&sort_bind, rb, &alias, &schema)
                                                }) {
                                                    plan.order_by.push(col_sql);
                                                    let n_param = SqlParamSource::Var("__limit__".into());
                                                    plan.limit = Some(n_param);
                                                    let sql = plan.build_sql();
                                                    let result_schema = plan.build_result_schema();
                                                    let preds = try_extract_preds_for_single_table_plan(do_stmts, &plan);
                                                    // Track reads for STM (so retry wakes on changes)
                                                    for table in &plan.tables {
                                                        let (tn_ptr, tn_len) = self.string_ptr(builder, &table.source_name);
                                                        self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
                                                        self.emit_stm_track_pred(builder, tn_ptr, tn_len, &preds, env, db);
                                                    }
                                                    // Compile SQL params + the limit value
                                                    let n_val = self.compile_expr(builder, args[0], env, db);
                                                    let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
                                                    // Append limit to the params relation
                                                    self.call_rt_void(builder, "knot_relation_push", &[params_rel, n_val]);
                                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                                                    return self.call_rt(
                                                        builder,
                                                        "knot_source_query",
                                                        &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                                    );
                                                }
                                            }
                                }

                // takeRelation N *source → SQL LIMIT (no ORDER BY)
                if let Some(source_name) = self.resolve_source(args[1])
                    && !self.views.contains_key(&source_name)
                        && let Some(schema) = self.source_schemas.get(&source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[') {
                                self.emit_stm_track_read(builder, &source_name);
                                let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                let cols = parse_schema_columns(&schema).iter()
                                    .map(|(n, _)| quote_sql_ident(n))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                let sql = format!("SELECT {} FROM {} LIMIT MAX(CAST(? AS INTEGER), 0)", cols, table);
                                let n_val = self.compile_expr(builder, args[0], env, db);
                                let params_rel = self.call_rt(builder, "knot_relation_singleton", &[n_val]);
                                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                return self.call_rt(
                                    builder,
                                    "knot_source_query",
                                    &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                );
                            }

                // takeRelation N (filter f *source) → SQL WHERE + LIMIT
                if let Some((source_name, filter_bind, filter_body)) =
                    extract_filter_on_source(args[1], &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
                {
                    let source_name: &str = &source_name;
                    let filter_body: &ast::Expr = &filter_body;
                    if !self.views.contains_key(source_name)
                        && let Some(schema) = self.source_schemas.get(source_name).cloned()
                            && !schema.starts_with('#') && !schema.contains('[')
                                && let Some(frag) = self.try_compile_sql_expr(&filter_bind, filter_body, &schema) {
                                    let table = quote_sql_ident(&format!("_knot_{}", source_name));
                                    let cols = parse_schema_columns(&schema).iter()
                                        .map(|(n, _)| quote_sql_ident(n))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    let sql = format!("SELECT {} FROM {} WHERE {} LIMIT MAX(CAST(? AS INTEGER), 0)", cols, table, frag.sql);
                                    self.emit_stm_track_read(builder, source_name);
                                    let n_val = self.compile_expr(builder, args[0], env, db);
                                    let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                                    self.call_rt_void(builder, "knot_relation_push", &[params_rel, n_val]);
                                    let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                                    return self.call_rt(
                                        builder,
                                        "knot_source_query",
                                        &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                                    );
                                }
                }
            }

        // SQL set operations: diff/inter/union on two source relations
        if let ast::ExprKind::Var(name) = &func_expr.node
            && !user_shadows_special {
            let sql_op = match name.as_str() {
                "diff" => Some("EXCEPT"),
                "inter" => Some("INTERSECT"),
                "union" => Some("UNION"),
                _ => None,
            };
            if let Some(sql_op) = sql_op
                && args.len() == 2 {
                    // Try to compile each side to a SQL subquery
                    if let (Some(sub_a), Some(sub_b)) = (
                        self.try_set_op_subquery(args[0], env),
                        self.try_set_op_subquery(args[1], env),
                    ) {
                        // SQLite's EXCEPT/INTERSECT/UNION match columns
                        // POSITIONALLY. The two sides can have different SELECT
                        // column orders (a bare/filtered source uses schema
                        // order, a do-block uses yield-record field order), so
                        // align sub_b to sub_a's schema column order by name
                        // before combining — otherwise the set op compares
                        // mismatched columns and silently returns wrong rows.
                        // Output columns are aliased to their field names in
                        // both subquery forms, so a name-based reprojection is
                        // safe. The result is read positionally as sub_a.schema.
                        let b_sql = if sub_b.schema == sub_a.schema {
                            sub_b.sql
                        } else {
                            let order_cols = parse_schema_columns(&sub_a.schema)
                                .iter()
                                .map(|(n, _)| quote_sql_ident(n))
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("SELECT {} FROM ({})", order_cols, sub_b.sql)
                        };
                        let sql = format!("{} {} {}", sub_a.sql, sql_op, b_sql);
                        let result_schema = sub_a.schema;
                        for table in sub_a.tables.iter().chain(sub_b.tables.iter()) {
                            let table = table.clone();
                            self.emit_stm_track_read(builder, &table);
                        }
                        let mut all_params = sub_a.params;
                        all_params.extend(sub_b.params);
                        let params_rel = self.compile_sql_params(builder, &all_params, env, db);
                        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
                        return self.call_rt(
                            builder,
                            "knot_source_query",
                            &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                        );
                    }
                }
        }

        // Special case: fetch/fetchWith
        if let ast::ExprKind::Var(name) = &func_expr.node
            && ((name == "fetch" && args.len() == 2)
                || (name == "fetchWith" && args.len() == 3))
            {
                return self.compile_fetch(builder, &args, name == "fetchWith", env, db);
            }

        // Special case: `traverse f rel` over a relation with a statically
        // known applicative — pass the kind so an EMPTY input produces
        // `pure []` in the right applicative instead of unconditionally the
        // Relation result `[[]]` (the runtime otherwise dispatches on the
        // first mapped element, which doesn't exist for empty inputs).
        // Inference records a monad_info entry keyed by the call span only
        // for relation containers; locally bound or user-defined `traverse`
        // names skip this and use normal dispatch.
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "traverse"
                && args.len() == 2
                && !env.bindings.contains_key("traverse")
                && !self.top_fn_names.contains("traverse")
                && let Some(kind) = self.monad_info.get(&expr.span).cloned() {
                    let kind_str = match &kind {
                        MonadKind::IO => "io".to_string(),
                        MonadKind::Relation => "relation".to_string(),
                        MonadKind::Adt(n) => n.clone(),
                    };
                    let f_val = self.compile_expr(builder, args[0], env, db);
                    let rel_val = self.compile_expr(builder, args[1], env, db);
                    let (k_ptr, k_len) = self.string_ptr(builder, &kind_str);
                    return self.call_rt(
                        builder,
                        "knot_relation_traverse_kind",
                        &[db, f_val, rel_val, k_ptr, k_len],
                    );
                }

        // Special case: `sum rel` that did not push down to SQL above — pass
        // the statically inferred element type so an EMPTY relation sums to the
        // right zero. The runtime otherwise takes the type from the summands,
        // of which there are none, and returns `Int 0` even for a `[Float]`
        // (the SQL pushdown paths pass the same `is_float` flag, derived from
        // the column type). Inference records the span only when the result is
        // a Float; a user-defined `sum` skips this and dispatches normally.
        if let ast::ExprKind::Var(name) = &func_expr.node
            && name == "sum"
                && args.len() == 1
                && !user_shadows_special {
                    let rel_val = self.compile_expr(builder, args[0], env, db);
                    let is_float = builder.ins().iconst(
                        types::I64,
                        self.sum_float_spans.contains(&expr.span) as i64,
                    );
                    return self.call_rt(
                        builder,
                        "knot_relation_sum_direct",
                        &[db, rel_val, is_float],
                    );
                }

        let compiled_args: Vec<Value> = args
            .iter()
            .map(|a| self.compile_arg_expr(builder, a, env, db))
            .collect();

        match &func_expr.node {
            // Monadic bind: __bind(f, m) — dispatch based on monad type
            ast::ExprKind::Var(name) if name == "__bind" => {
                // Dispatch through Monad trait impls based on resolved monad type
                if compiled_args.len() == 2 {
                    let type_name = match self.monad_info.get(&func_expr.span) {
                        Some(MonadKind::Adt(name)) => name.clone(),
                        Some(MonadKind::IO) => "IO".to_string(),
                        _ => "Relation".to_string(),
                    };
                    // Built-in Result monad bind
                    if type_name == "Result" {
                        return self.call_rt(
                            builder,
                            "knot_result_bind",
                            &[db, compiled_args[0], compiled_args[1]],
                        );
                    }
                    let bind_fn = format!("Monad_{}_bind", type_name);
                    if let Some(&(func_id, _)) = self.user_fns.get(&bind_fn) {
                        let func_ref = self
                            .module
                            .declare_func_in_func(func_id, builder.func);
                        let call = builder.ins().call(
                            func_ref,
                            &[db, compiled_args[0], compiled_args[1]],
                        );
                        return builder.inst_results(call)[0];
                    }
                }
                // Ultimate fallback: direct runtime call
                self.call_rt(
                    builder,
                    "knot_relation_bind",
                    &[db, compiled_args[0], compiled_args[1]],
                )
            }

            // Compile-time FromJSON dispatch: parseJson(text) → FromJSON_Type_parseJson
            // when the return type is known and a FromJSON impl exists for that type
            ast::ExprKind::Var(name) if name == "parseJson" && compiled_args.len() == 1 => {
                let target = self.from_json_targets.get(&expr.span).cloned();
                if let Some(type_name) = target.as_ref().and_then(|t| t.type_name.as_deref()) {
                    let impl_fn = format!("FromJSON_{}_parseJson", type_name);
                    if let Some(&(func_id, _)) = self.user_fns.get(&impl_fn) {
                        let func_ref = self
                            .module
                            .declare_func_in_func(func_id, builder.func);
                        let call = builder.ins().call(
                            func_ref,
                            &[db, compiled_args[0]],
                        );
                        return builder.inst_results(call)[0];
                    }
                }
                // `parseJson : Text -> Maybe a` — the decoders return `Nothing`
                // on malformed input and `Just decoded` on success. When the
                // inner type carries Maybe positions, pass its wire schema so
                // `null`/absent normalizes to Nothing and present values are
                // Just-wrapped inside the decoded value.
                if let Some(schema) = target.as_ref().and_then(|t| t.wire_schema.as_deref()) {
                    let (sptr, slen) = self.string_ptr(builder, schema);
                    return self.call_rt(
                        builder,
                        "knot_json_decode_typed_maybe",
                        &[compiled_args[0], sptr, slen],
                    );
                }
                // Fall through to generic parseJson (dispatcher or runtime)
                if let Some(&(func_id, expected_params)) = self.user_fns.get("parseJson")
                    && compiled_args.len() == expected_params {
                        let func_ref = self
                            .module
                            .declare_func_in_func(func_id, builder.func);
                        let call = builder.ins().call(func_ref, &[db, compiled_args[0]]);
                        return builder.inst_results(call)[0];
                    }
                self.call_rt(builder, "knot_json_decode_maybe", &[compiled_args[0]])
            }

            // Direct call to a known user function
            ast::ExprKind::Var(name)
                if self.user_fns.contains_key(name) =>
            {
                // A trait method call resolves to the impl its static type
                // selects; only genuinely polymorphic sites are left to the
                // runtime tag dispatcher.
                let static_impl =
                    self.resolve_trait_call(name, func_expr.span);
                let fn_name: &str =
                    static_impl.as_deref().unwrap_or(name.as_str());
                let (func_id, expected_params) = self.user_fns[fn_name];
                if compiled_args.len() == expected_params {
                    let func_ref = self
                        .module
                        .declare_func_in_func(func_id, builder.func);
                    let mut call_args = vec![db];
                    call_args.extend(&compiled_args);
                    let call = builder.ins().call(func_ref, &call_args);
                    builder.inst_results(call)[0]
                } else {
                    // Partial application or over-application — use dynamic call
                    let func_val =
                        self.compile_expr(builder, func_expr, env, db);
                    let mut result = func_val;
                    for arg in &compiled_args {
                        result = self.call_rt(
                            builder,
                            "knot_value_call",
                            &[db, result, *arg],
                        );
                    }
                    result
                }
            }

            // Built-in functions (IO-returning)
            ast::ExprKind::Var(name) if name == "println" || name == "putLine" => {
                if compiled_args.len() == 1 {
                    self.call_rt(builder, "knot_println_io", &[compiled_args[0]])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "print" => {
                if compiled_args.len() == 1 {
                    self.call_rt(builder, "knot_print_io", &[compiled_args[0]])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name)
                if name == "logInfo" || name == "logWarn"
                    || name == "logError" || name == "logDebug" =>
            {
                let rt = match name.as_str() {
                    "logInfo" => "knot_log_info_io",
                    "logWarn" => "knot_log_warn_io",
                    "logError" => "knot_log_error_io",
                    "logDebug" => "knot_log_debug_io",
                    _ => unreachable!(),
                };
                if compiled_args.len() == 1 {
                    self.call_rt(builder, rt, &[compiled_args[0]])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "show" => {
                if compiled_args.len() == 1 {
                    // A `show` whose argument's type carried a concrete unit
                    // gets the unit appended: `show 42.0 M` → "42.0 M". The
                    // unit is erased from the value, so inference resolved it
                    // per call site and it is emitted as a string constant.
                    match self.show_unit_strings.get(&expr.span).cloned() {
                        Some(unit) => {
                            let (unit_ptr, unit_len) = self.string_ptr(builder, &unit);
                            self.call_rt(
                                builder,
                                "knot_value_show_unit",
                                &[compiled_args[0], unit_ptr, unit_len],
                            )
                        }
                        None => self.call_rt(builder, "knot_value_show", &[compiled_args[0]]),
                    }
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "union" => {
                if compiled_args.len() == 2 {
                    self.call_rt(
                        builder,
                        "knot_relation_union",
                        &[db, compiled_args[0], compiled_args[1]],
                    )
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "count" => {
                if compiled_args.len() == 1 {
                    // knot_relation_len returns raw usize, pass directly to knot_value_int
                    let len =
                        self.call_rt(builder, "knot_relation_len", &[compiled_args[0]]);
                    self.call_rt(builder, "knot_value_int", &[len])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "listen" || name == "listenOn" => {
                let is_listen_on = name == "listenOn";
                let expected_arity = if is_listen_on { 3 } else { 2 };
                if compiled_args.len() == expected_arity {
                    // listen port handler  /  listenOn host port handler
                    // Build route table from known route declarations
                    let table = self.call_rt(builder, "knot_route_table_new", &[]);

                    // Identify which API this server actually serves so only
                    // that API's route entries are registered on its table.
                    // Registering every declared route would dispatch requests
                    // for unserved routes into `compile_serve`'s case (which
                    // has no arm for them) — a 500/abort instead of a 404.
                    // The server value is the last argument: either a literal
                    // `serve Api where ...` or a name resolving to one.
                    let server_arg = &args[expected_arity - 1];
                    let served_api: Option<String> = {
                        let reduced = beta_reduce(
                            server_arg,
                            &self.fun_bodies,
                            &self.let_bindings,
                        );
                        match &strip_expr_wrappers(&reduced).node {
                            ast::ExprKind::Serve { api, .. } => Some(api.clone()),
                            _ => None,
                        }
                    };

                    // Collect the served API's route entries (composed
                    // `route Name = A | B` declarations are already flattened
                    // in route_entries), deduplicating by constructor name.
                    // If the server value can't be traced to a `serve`
                    // expression statically (e.g. it came through a function
                    // parameter), fall back to registering every declared
                    // route — over-registration only risks 500s on unserved
                    // routes, never missed dispatch of served ones.
                    let mut seen = std::collections::HashSet::new();
                    let mut entries: Vec<ast::RouteEntry> = Vec::new();
                    match served_api
                        .as_ref()
                        .and_then(|api| self.route_entries.get(api))
                    {
                        Some(api_entries) => {
                            for entry in api_entries {
                                if seen.insert(entry.constructor.clone()) {
                                    entries.push(entry.clone());
                                }
                            }
                        }
                        None => {
                            for route_entries in self.route_entries.values() {
                                for entry in route_entries {
                                    if seen.insert(entry.constructor.clone()) {
                                        entries.push(entry.clone());
                                    }
                                }
                            }
                        }
                    }

                    for entry in &entries {
                        let method_str = match entry.method {
                            ast::HttpMethod::Get => "GET",
                            ast::HttpMethod::Post => "POST",
                            ast::HttpMethod::Put => "PUT",
                            ast::HttpMethod::Delete => "DELETE",
                            ast::HttpMethod::Patch => "PATCH",
                        };
                        let (method_ptr, method_len) =
                            self.string_ptr(builder, method_str);

                        let path_pattern = path_segments_to_pattern(&entry.path, &self.type_aliases);
                        let (path_ptr, path_len) =
                            self.string_ptr(builder, &path_pattern);

                        let (ctor_ptr, ctor_len) =
                            self.string_ptr(builder, &entry.constructor);

                        let body_desc = fields_to_descriptor(&entry.body_fields, &self.type_aliases);
                        let (body_ptr, body_len) =
                            self.string_ptr(builder, &body_desc);

                        let query_desc = fields_to_descriptor(&entry.query_params, &self.type_aliases);
                        let (query_ptr, query_len) =
                            self.string_ptr(builder, &query_desc);

                        let resp_desc = response_type_descriptor(&entry.response_ty, &self.type_aliases);
                        let (resp_ptr, resp_len) =
                            self.string_ptr(builder, &resp_desc);

                        let req_hdrs_desc = fields_to_descriptor(&entry.request_headers, &self.type_aliases);
                        let (req_hdrs_ptr, req_hdrs_len) =
                            self.string_ptr(builder, &req_hdrs_desc);

                        let resp_hdrs_desc = fields_to_descriptor(&entry.response_headers, &self.type_aliases);
                        let (resp_hdrs_ptr, resp_hdrs_len) =
                            self.string_ptr(builder, &resp_hdrs_desc);

                        self.call_rt_void(
                            builder,
                            "knot_route_table_add",
                            &[
                                table, method_ptr, method_len, path_ptr, path_len,
                                ctor_ptr, ctor_len, body_ptr, body_len, query_ptr,
                                query_len, resp_ptr, resp_len,
                                req_hdrs_ptr, req_hdrs_len, resp_hdrs_ptr, resp_hdrs_len,
                            ],
                        );
                    }

                    // Register refinement predicates for route body fields
                    // on this table. The main_init registration targets the
                    // tables built there, not this one — the serve loop
                    // dispatches from this table, so refinements must be
                    // registered here for the HTTP 400 auto-validation to
                    // fire.
                    for entry in &entries {
                        let (ctor_ptr, ctor_len) =
                            self.string_ptr(builder, &entry.constructor);
                        for field in &entry.body_fields {
                            let mut found = Vec::new();
                            collect_type_refinements(
                                &field.value,
                                &field.name,
                                &self.alias_ast,
                                &mut Vec::new(),
                                &mut found,
                            );
                            for (path, type_name, pred_expr) in found {
                                let mut pred_env = Env::new();
                                let pred_fn =
                                    self.compile_expr(builder, &pred_expr, &mut pred_env, db);
                                let (fn_ptr, fn_len) = self.string_ptr(builder, &path);
                                let (tn_ptr, tn_len) = self.string_ptr(builder, &type_name);
                                self.call_rt_void(
                                    builder,
                                    "knot_route_set_field_refinement",
                                    &[
                                        table, ctor_ptr, ctor_len, fn_ptr, fn_len,
                                        pred_fn, tn_ptr, tn_len,
                                    ],
                                );
                            }
                        }
                    }

                    // Register rate-limit configurations on this table.
                    // The compiled `rateLimit` expression is a record value
                    // `{key, limit}` that the runtime unpacks.
                    for entry in &entries {
                        if let Some(rate_limit_expr) = &entry.rate_limit {
                            let (ctor_ptr, ctor_len) =
                                self.string_ptr(builder, &entry.constructor);
                            let mut rl_env = Env::new();
                            let rl_val =
                                self.compile_expr(builder, rate_limit_expr, &mut rl_env, db);
                            self.call_rt_void(
                                builder,
                                "knot_route_set_rate_limit",
                                &[table, ctor_ptr, ctor_len, rl_val],
                            );
                        }
                    }

                    // `listen` produces an IO *value*; starting the server
                    // happens when the value is run (knot_io_run for a bare
                    // statement, or on the spawned thread for
                    // `fork (listen port api)`). Calling the serve loop
                    // directly here would block the evaluating thread before
                    // `fork` ever saw the value.
                    if is_listen_on {
                        self.call_rt(
                            builder,
                            "knot_http_listen_on_io",
                            &[db, compiled_args[0], compiled_args[1], table, compiled_args[2]],
                        )
                    } else {
                        self.call_rt(
                            builder,
                            "knot_http_listen_io",
                            &[db, compiled_args[0], table, compiled_args[1]],
                        )
                    }
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }

            // Constructor application: `Circle {radius: 3.14}`
            ast::ExprKind::Constructor(name) if name == "True" || name == "False" => {
                let val = if name == "True" { 1i64 } else { 0i64 };
                let arg = builder.ins().iconst(cranelift_codegen::ir::types::I32, val);
                self.call_rt(builder, "knot_value_bool", &[arg])
            }
            ast::ExprKind::Constructor(name) => {
                match self.nullable_ctors.get(name).cloned() {
                    Some(NullableRole::None) => {
                        // Nullable none: ignore args, return null
                        builder.ins().iconst(self.ptr_type, 0)
                    }
                    Some(NullableRole::Some) => {
                        // Nullable some: return bare payload (no Constructor wrapper)
                        if compiled_args.len() == 1 {
                            compiled_args[0]
                        } else {
                            self.call_rt(builder, "knot_value_unit", &[])
                        }
                    }
                    None => {
                        let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                        let payload = if compiled_args.len() == 1 {
                            compiled_args[0]
                        } else {
                            self.call_rt(builder, "knot_value_unit", &[])
                        };
                        self.call_rt(
                            builder,
                            "knot_value_constructor",
                            &[tag_ptr, tag_len, payload],
                        )
                    }
                }
            }

            // Dynamic call through a function value
            _ => {
                let func_val =
                    self.compile_expr(builder, func_expr, env, db);
                let mut result = func_val;
                for arg in &compiled_args {
                    result = self.call_rt(
                        builder,
                        "knot_value_call",
                        &[db, result, *arg],
                    );
                }
                result
            }
        }
    }

    // ── HTTP fetch compilation ────────────────────────────────────

    /// Compile `fetch url (Ctor {..})` or `fetchWith url opts (Ctor {..})`.
    fn compile_fetch(
        &mut self,
        builder: &mut FunctionBuilder,
        args: &[&ast::Expr],
        with_opts: bool,
        env: &mut Env,
        db: Value,
    ) -> Value {
        let base_url = self.compile_expr(builder, args[0], env, db);

        let (headers, ctor_expr) = if with_opts {
            // fetchWith url opts (Ctor {..})
            let opts = self.compile_expr(builder, args[1], env, db);
            let (h_ptr, h_len) = self.string_ptr(builder, "headers");
            let headers =
                self.call_rt(builder, "knot_record_field", &[opts, h_ptr, h_len]);
            (headers, args[2])
        } else {
            // fetch url (Ctor {..})
            let null = builder.ins().iconst(self.ptr_type, 0);
            (null, args[1])
        };

        // Extract constructor name and record argument from the AST
        let (ctor_name, record_expr) = match &ctor_expr.node {
            ast::ExprKind::App { func, arg } => {
                if let ast::ExprKind::Constructor(name) = &func.node {
                    (name.clone(), Some(arg.as_ref()))
                } else {
                    return self.push_codegen_error(
                        builder,
                        ctor_expr.span,
                        "fetch: expected constructor application as last argument",
                    );
                }
            }
            ast::ExprKind::Constructor(name) => (name.clone(), None),
            _ => {
                return self.push_codegen_error(
                    builder,
                    ctor_expr.span,
                    "fetch: expected constructor application as last argument",
                );
            }
        };

        // Compile just the record payload (skip the Constructor wrapper)
        let payload = match record_expr {
            Some(expr) => self.compile_expr(builder, expr, env, db),
            None => self.call_rt(builder, "knot_value_unit", &[]),
        };

        // Look up the route entry for this constructor. Resolved last-wins in
        // source declaration order (see `fetch_route_entries`) so it matches
        // the entry infer typechecked against — iterating `route_entries`
        // (a HashMap) and taking the first match would pick a nondeterministic
        // route when distinct routes legally share a constructor name (B38).
        let entry = match self.fetch_route_entries.get(&ctor_name).cloned() {
            Some(e) => e,
            None => {
                return self.push_codegen_error(
                    builder,
                    ctor_expr.span,
                    format!("fetch: no route entry found for constructor '{}'", ctor_name),
                );
            }
        };

        let method_str = match entry.method {
            ast::HttpMethod::Get => "GET",
            ast::HttpMethod::Post => "POST",
            ast::HttpMethod::Put => "PUT",
            ast::HttpMethod::Delete => "DELETE",
            ast::HttpMethod::Patch => "PATCH",
        };
        let path_pattern = path_segments_to_pattern(&entry.path, &self.type_aliases);
        let body_desc = fields_to_descriptor(&entry.body_fields, &self.type_aliases);
        let query_desc = fields_to_descriptor(&entry.query_params, &self.type_aliases);
        let resp_desc =
            response_type_descriptor(&entry.response_ty, &self.type_aliases);
        let req_hdrs_desc = fields_to_descriptor(&entry.request_headers, &self.type_aliases);
        let resp_hdrs_desc = fields_to_descriptor(&entry.response_headers, &self.type_aliases);

        let (method_ptr, method_len) = self.string_ptr(builder, method_str);
        let (path_ptr, path_len) = self.string_ptr(builder, &path_pattern);
        let (body_ptr, body_len) = self.string_ptr(builder, &body_desc);
        let (query_ptr, query_len) = self.string_ptr(builder, &query_desc);
        let (resp_ptr, resp_len) = self.string_ptr(builder, &resp_desc);
        let (req_hdrs_ptr, req_hdrs_len) = self.string_ptr(builder, &req_hdrs_desc);
        let (resp_hdrs_ptr, resp_hdrs_len) = self.string_ptr(builder, &resp_hdrs_desc);

        self.call_rt(
            builder,
            "knot_http_fetch_io",
            &[
                base_url, method_ptr, method_len, path_ptr, path_len, payload,
                body_ptr, body_len, query_ptr, query_len, resp_ptr, resp_len,
                headers, req_hdrs_ptr, req_hdrs_len, resp_hdrs_ptr, resp_hdrs_len,
            ],
        )
    }

    // ── Refine expression compilation ─────────────────────────────

    /// Compile `serve Api where E1 = h1; ... En = hn` into a function value
    /// that dispatches on the route ADT constructor and applies the matching
    /// handler to the constructor's payload record.
    ///
    /// We desugar to a synthetic AST `\req -> case req of Ei pi -> hi pi`
    /// and reuse the existing lambda + case codegen.  At runtime the resulting
    /// `Server Api` value is just a `Value::Function` — `listen` invokes it
    /// like any other 1-arg function and serializes the returned body.
    fn compile_serve(
        &mut self,
        builder: &mut FunctionBuilder,
        _api: &str,
        handlers: &[ast::ServeHandler],
        span: ast::Span,
        env: &mut Env,
        db: Value,
    ) -> Value {
        let req_name = format!("__serve_req_{}", self.lambda_counter);
        let payload_name = format!("__serve_payload_{}", self.lambda_counter);

        let arms: Vec<ast::CaseArm> = handlers
            .iter()
            .map(|h| {
                let payload_pat = ast::Spanned::new(
                    ast::PatKind::Var(payload_name.clone()),
                    h.endpoint_span,
                );
                let arm_pat = ast::Spanned::new(
                    ast::PatKind::Constructor {
                        name: h.endpoint.clone(),
                        payload: Box::new(payload_pat),
                    },
                    h.endpoint_span,
                );
                let payload_var = ast::Spanned::new(
                    ast::ExprKind::Var(payload_name.clone()),
                    h.endpoint_span,
                );
                let arm_body = ast::Spanned::new(
                    ast::ExprKind::App {
                        func: Box::new(h.body.clone()),
                        arg: Box::new(payload_var),
                    },
                    h.body.span,
                );
                ast::CaseArm { pat: arm_pat, body: arm_body }
            })
            .collect();

        let req_var = ast::Spanned::new(ast::ExprKind::Var(req_name.clone()), span);
        let case_expr = ast::Spanned::new(
            ast::ExprKind::Case {
                scrutinee: Box::new(req_var),
                arms,
            },
            span,
        );
        let req_pat = ast::Spanned::new(ast::PatKind::Var(req_name), span);
        self.compile_lambda(builder, &[req_pat], &case_expr, env, db)
    }

    fn compile_refine(
        &mut self,
        builder: &mut FunctionBuilder,
        inner: &ast::Expr,
        span: knot::ast::Span,
        env: &mut Env,
        db: Value,
    ) -> Value {
        // Look up which refined type this refine targets
        let type_name = match self.refine_targets.get(&span) {
            Some(name) => name.clone(),
            None => {
                // No target resolved — wrap in Ok {value: val} as pass-through
                let val = self.compile_expr(builder, inner, env, db);
                return self.build_ok_result(builder, val);
            }
        };

        // Compile the predicate expression (a lambda) in the current env
        let predicate_expr = match self.refined_types.get(&type_name) {
            Some(pred) => pred.clone(),
            None => {
                let val = self.compile_expr(builder, inner, env, db);
                return self.build_ok_result(builder, val);
            }
        };

        // Const-fold: if the inner expression is a literal, evaluate the
        // refinement predicate at compile time and skip the runtime check.
        if let Some(lit) = extract_literal(inner) {
            match eval_refine_predicate(&predicate_expr, &lit) {
                Some(true) => {
                    let val = self.compile_expr(builder, inner, env, db);
                    return self.build_ok_result(builder, val);
                }
                Some(false) => {
                    self.diagnostics.push(
                        knot::diagnostic::Diagnostic::error(
                            format!(
                                "refine: value {} does not satisfy predicate for type `{}`",
                                lit.display(),
                                type_name
                            ),
                        ).label(span, "refinement predicate fails at compile time"),
                    );
                    let _ = self.compile_expr(builder, inner, env, db);
                    return self.build_refinement_err(builder, &type_name);
                }
                None => { /* fall through to runtime check */ }
            }
        }

        let val = self.compile_expr(builder, inner, env, db);
        let pred_fn = self.compile_expr(builder, &predicate_expr, env, db);

        // Call the predicate: pred(val) -> Bool
        let pred_result = self.call_rt(builder, "knot_value_call", &[db, pred_fn, val]);
        let is_true = self.call_rt_typed(
            builder,
            "knot_value_get_bool",
            &[pred_result],
            cranelift_codegen::ir::types::I32,
        );

        // Branch: if true -> Ok {value: val}, else -> Err {error: RefinementError{...}}
        let ok_block = builder.create_block();
        let err_block = builder.create_block();
        let merge_block = builder.create_block();
        builder.append_block_param(merge_block, self.ptr_type);

        builder.ins().brif(is_true, ok_block, &[], err_block, &[]);

        // Ok path
        builder.switch_to_block(ok_block);
        builder.seal_block(ok_block);
        let ok_val = self.build_ok_result(builder, val);
        builder.ins().jump(merge_block, &[ok_val.into()]);

        // Err path
        builder.switch_to_block(err_block);
        builder.seal_block(err_block);
        let err_val = self.build_refinement_err(builder, &type_name);
        builder.ins().jump(merge_block, &[err_val.into()]);

        builder.switch_to_block(merge_block);
        builder.seal_block(merge_block);
        builder.block_params(merge_block)[0]
    }

    /// Build Ok {value: val} constructor value
    fn build_ok_result(&mut self, builder: &mut FunctionBuilder, val: Value) -> Value {
        let cap = builder.ins().iconst(self.ptr_type, 1);
        let rec = self.call_rt(builder, "knot_record_empty", &[cap]);
        let (key_ptr, key_len) = self.string_ptr(builder, "value");
        self.call_rt_void(builder, "knot_record_set_field", &[rec, key_ptr, key_len, val]);
        let (tag_ptr, tag_len) = self.string_ptr(builder, "Ok");
        self.call_rt(builder, "knot_value_constructor", &[tag_ptr, tag_len, rec])
    }

    /// Emit refinement validation calls for a source relation before writing.
    fn emit_refinement_checks(
        &mut self,
        builder: &mut FunctionBuilder,
        source_name: &str,
        relation_val: Value,
        env: &mut Env,
        db: Value,
    ) {
        self.emit_refinement_checks_filtered(builder, source_name, relation_val, None, env, db);
    }

    /// Emit refinement validation calls for rows about to be written to
    /// `source_name`, restricted to the columns the rows actually carry.
    /// `written_cols: None` means the rows carry the full source schema.
    /// With `Some(cols)`:
    ///   - field-level refinements are emitted only for fields in `cols`
    ///     (a projected view that doesn't select the field can't violate it);
    ///   - whole-element refinements are emitted only when `cols` covers
    ///     every source column (otherwise the predicate could access a
    ///     missing field and the check can't be evaluated on partial rows).
    fn emit_refinement_checks_filtered(
        &mut self,
        builder: &mut FunctionBuilder,
        source_name: &str,
        relation_val: Value,
        written_cols: Option<&HashSet<String>>,
        env: &mut Env,
        db: Value,
    ) {
        // Refinement checks run inside atomic blocks too: a program can build
        // rows locally and `set` them inside `atomic do { … }`, and the
        // documented guarantee is that every write to a refined source is
        // validated (and panics on violation). Inside atomic a violation panic
        // rolls back the savepoint, so the transaction is not committed with
        // invalid data. (Route handlers also validate at the HTTP boundary, but
        // that path does not cover locally constructed rows.)
        let refinements = match self.source_refinements.get(source_name) {
            Some(r) => r.clone(),
            None => return,
        };
        let covers_all = match written_cols {
            None => true,
            Some(cols) => self
                .source_schemas
                .get(source_name)
                .map(|full| {
                    split_schema_fields(full).iter().all(|part| {
                        let name = part.split(':').next().unwrap_or("");
                        cols.contains(name)
                    })
                })
                .unwrap_or(false),
        };
        for (field_name, type_name, predicate_expr) in &refinements {
            match (field_name, written_cols) {
                (Some(f), Some(cols)) if !cols.contains(f.as_str()) => continue,
                (None, _) if !covers_all => continue,
                _ => {}
            }
            let pred_fn = self.compile_expr(builder, predicate_expr, env, db);
            let (tn_ptr, tn_len) = self.string_ptr(builder, type_name);
            let field_str = field_name.as_deref().unwrap_or("");
            let (fn_ptr, fn_len) = self.string_ptr(builder, field_str);
            self.call_rt_void(
                builder,
                "knot_refinement_validate_relation",
                &[db, relation_val, pred_fn, tn_ptr, tn_len, fn_ptr, fn_len],
            );
        }
    }

    /// Build Err {error: {typeName: ..., violations: [...]}} constructor value
    fn build_refinement_err(&mut self, builder: &mut FunctionBuilder, type_name: &str) -> Value {
        // Build {typeName: type_name, violations: [{field: Nothing {}, message: "..."}]}
        let cap2 = builder.ins().iconst(self.ptr_type, 2);
        let error_rec = self.call_rt(builder, "knot_record_empty", &[cap2]);

        let (tn_key_ptr, tn_key_len) = self.string_ptr(builder, "typeName");
        let (tn_val_ptr, tn_val_len) = self.string_ptr(builder, type_name);
        let type_name_val = self.call_rt(builder, "knot_value_text", &[tn_val_ptr, tn_val_len]);
        self.call_rt_void(builder, "knot_record_set_field", &[error_rec, tn_key_ptr, tn_key_len, type_name_val]);

        // Build violation record
        let violation_rec = self.call_rt(builder, "knot_record_empty", &[cap2]);

        let (f_key_ptr, f_key_len) = self.string_ptr(builder, "field");
        let (nothing_tag_ptr, nothing_tag_len) = self.string_ptr(builder, "Nothing");
        let nothing_unit = self.call_rt(builder, "knot_value_unit", &[]);
        let nothing_val = self.call_rt(builder, "knot_value_constructor", &[nothing_tag_ptr, nothing_tag_len, nothing_unit]);
        self.call_rt_void(builder, "knot_record_set_field", &[violation_rec, f_key_ptr, f_key_len, nothing_val]);

        let (m_key_ptr, m_key_len) = self.string_ptr(builder, "message");
        let msg_str = format!("value does not satisfy '{}' predicate", type_name);
        let (msg_ptr, msg_len) = self.string_ptr(builder, &msg_str);
        let msg_val = self.call_rt(builder, "knot_value_text", &[msg_ptr, msg_len]);
        self.call_rt_void(builder, "knot_record_set_field", &[violation_rec, m_key_ptr, m_key_len, msg_val]);

        let violations = self.call_rt(builder, "knot_relation_singleton", &[violation_rec]);
        let (v_key_ptr, v_key_len) = self.string_ptr(builder, "violations");
        self.call_rt_void(builder, "knot_record_set_field", &[error_rec, v_key_ptr, v_key_len, violations]);

        // Wrap in Err {error: error_rec}
        let cap1 = builder.ins().iconst(self.ptr_type, 1);
        let err_wrapper = self.call_rt(builder, "knot_record_empty", &[cap1]);
        let (err_key_ptr, err_key_len) = self.string_ptr(builder, "error");
        self.call_rt_void(builder, "knot_record_set_field", &[err_wrapper, err_key_ptr, err_key_len, error_rec]);
        let (err_tag_ptr, err_tag_len) = self.string_ptr(builder, "Err");
        self.call_rt(builder, "knot_value_constructor", &[err_tag_ptr, err_tag_len, err_wrapper])
    }

    // ── Case expression compilation ───────────────────────────────

    fn compile_case(
        &mut self,
        builder: &mut FunctionBuilder,
        scrutinee: &ast::Expr,
        arms: &[ast::CaseArm],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let scrut = self.compile_expr(builder, scrutinee, env, db);
        let merge_block = builder.create_block();
        merge_block_param(builder, merge_block, self.ptr_type);

        // Count non-nullable constructor arms to decide whether to extract tag once.
        // Exclude True/False — they compile to Value::Bool, not Value::Constructor,
        // so calling knot_constructor_tag_ptr on them would panic.
        let non_nullable_ctor_count = arms.iter().filter(|a| {
            if let ast::PatKind::Constructor { name, .. } = &a.pat.node {
                !self.nullable_ctors.contains_key(name) && name != "True" && name != "False"
            } else {
                false
            }
        }).count();

        // Only cache the tag when there are no wildcard/var catch-all arms.
        // With a catch-all, the scrutinee could (defensively) be a non-Constructor
        // value, and calling knot_constructor_tag_ptr would panic.
        let has_catchall = arms.iter().any(|a| matches!(
            &a.pat.node,
            ast::PatKind::Wildcard | ast::PatKind::Var(_)
        ));

        // Extract constructor tag pointer+length once if multiple constructor arms
        let cached_tag = if non_nullable_ctor_count >= 2 && !has_catchall {
            let tag_ptr = self.call_rt(builder, "knot_constructor_tag_ptr", &[scrut]);
            let tag_len = self.call_rt(builder, "knot_constructor_tag_len", &[scrut]);
            Some((tag_ptr, tag_len))
        } else {
            None
        };

        for (i, arm) in arms.iter().enumerate() {
            let is_last = i == arms.len() - 1;
            let arm_block = builder.create_block();

            // For unconditional patterns on the last arm, use merge_block
            // as next_block. For conditional patterns, always create a
            // separate block (merge_block has a parameter that brif can't
            // provide). A pattern is only unconditional when it is
            // irrefutable all the way down — nested literals, constructor
            // tags, and list shapes all emit runtime tests that can fail.
            let is_unconditional = case_pattern_is_irrefutable(&arm.pat);
            let next_block = if is_last && is_unconditional {
                merge_block
            } else {
                builder.create_block()
            };

            // Test the pattern
            match &arm.pat.node {
                ast::PatKind::Wildcard | ast::PatKind::Var(_) => {
                    // Always matches
                    builder.ins().jump(arm_block, &[]);
                }
                ast::PatKind::Constructor { name, .. } if name == "True" || name == "False" => {
                    let bool_val = self.call_rt_typed(builder, "knot_value_get_bool", &[scrut], types::I32);
                    let expected = if name == "True" { 1i64 } else { 0i64 };
                    let is_match = builder.ins().icmp_imm(IntCC::Equal, bool_val, expected);
                    builder.ins().brif(
                        is_match,
                        arm_block,
                        &[],
                        next_block,
                        &[],
                    );
                }
                ast::PatKind::Constructor { name, .. } => {
                    match self.nullable_ctors.get(name).cloned() {
                        Some(NullableRole::None) => {
                            // Nullable none: check if scrutinee is null
                            let is_null = builder.ins().icmp_imm(
                                IntCC::Equal,
                                scrut,
                                0,
                            );
                            builder.ins().brif(
                                is_null,
                                arm_block,
                                &[],
                                next_block,
                                &[],
                            );
                        }
                        Some(NullableRole::Some) => {
                            // Nullable some: check if scrutinee is non-null
                            let is_some = builder.ins().icmp_imm(
                                IntCC::NotEqual,
                                scrut,
                                0,
                            );
                            builder.ins().brif(
                                is_some,
                                arm_block,
                                &[],
                                next_block,
                                &[],
                            );
                        }
                        None => {
                            if let Some((tag_ptr, tag_len)) = cached_tag {
                                // Use pre-extracted tag for fast string comparison
                                let (expected_ptr, expected_len) =
                                    self.string_ptr(builder, name);
                                let matches = self.call_rt_typed(
                                    builder,
                                    "knot_str_eq",
                                    &[tag_ptr, tag_len, expected_ptr, expected_len],
                                    types::I32,
                                );
                                let is_match = builder
                                    .ins()
                                    .icmp_imm(IntCC::NotEqual, matches, 0);
                                builder.ins().brif(
                                    is_match,
                                    arm_block,
                                    &[],
                                    next_block,
                                    &[],
                                );
                            } else {
                                let (tag_ptr, tag_len) =
                                    self.string_ptr(builder, name);
                                let matches = self.call_rt_typed(
                                    builder,
                                    "knot_constructor_matches",
                                    &[scrut, tag_ptr, tag_len],
                                    types::I32,
                                );
                                let is_match = builder
                                    .ins()
                                    .icmp_imm(IntCC::NotEqual, matches, 0);
                                builder.ins().brif(
                                    is_match,
                                    arm_block,
                                    &[],
                                    next_block,
                                    &[],
                                );
                            }
                        }
                    }
                }
                ast::PatKind::Lit(lit) => {
                    let lit_val = self.compile_lit(builder, lit);
                    let eq_i32 =
                        self.call_rt_typed(builder, "knot_value_eq_i32", &[scrut, lit_val], types::I32);
                    let is_eq =
                        builder.ins().icmp_imm(IntCC::NotEqual, eq_i32, 0);
                    builder
                        .ins()
                        .brif(is_eq, arm_block, &[], next_block, &[]);
                }
                ast::PatKind::List(pats) => {
                    // Check if relation length matches the number of patterns
                    let len = self.call_rt(builder, "knot_relation_len", &[scrut]);
                    let expected =
                        builder.ins().iconst(self.ptr_type, pats.len() as i64);
                    let is_match =
                        builder.ins().icmp(IntCC::Equal, len, expected);
                    builder.ins().brif(
                        is_match,
                        arm_block,
                        &[],
                        next_block,
                        &[],
                    );
                }
                ast::PatKind::Cons { .. } => {
                    // Match any non-empty relation: len > 0.
                    let len = self.call_rt(builder, "knot_relation_len", &[scrut]);
                    let is_match =
                        builder.ins().icmp_imm(IntCC::NotEqual, len, 0);
                    builder.ins().brif(
                        is_match,
                        arm_block,
                        &[],
                        next_block,
                        &[],
                    );
                }
                ast::PatKind::Record(_) => {
                    // Record patterns always match (no top-level guard)
                    builder.ins().jump(arm_block, &[]);
                }
            }

            builder.switch_to_block(arm_block);
            builder.seal_block(arm_block);

            // Bind pattern variables. Refutable patterns may carry nested
            // sub-patterns (literals, constructor tags, list shapes) that
            // the top-level discriminant test above did not cover — bind
            // them through the testing variant, which branches to
            // next_block on a sub-pattern mismatch so the next arm is
            // tried. Irrefutable patterns can't fail and bind directly
            // (next_block may be merge_block, which carries a param that
            // a mismatch branch couldn't provide).
            let mut arm_env = env.clone();
            if is_unconditional {
                self.bind_case_pattern(builder, &arm.pat, scrut, &mut arm_env);
            } else {
                self.bind_case_pattern_checked(
                    builder, &arm.pat, scrut, &mut arm_env, next_block,
                );
            }

            let arm_val = if self.in_io_eager {
                self.compile_io_expr_eager(builder, &arm.body, &mut arm_env, db)
            } else {
                self.compile_expr(builder, &arm.body, &mut arm_env, db)
            };
            builder.ins().jump(merge_block, &[arm_val.into()]);

            if is_last && !is_unconditional {
                // Last arm was conditional — no arm matched at runtime; panic.
                builder.switch_to_block(next_block);
                builder.seal_block(next_block);
                self.call_rt_void(builder, "knot_guard_failed", &[]);
                builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
            } else if !is_last {
                builder.switch_to_block(next_block);
                builder.seal_block(next_block);
            }
        }

        builder.switch_to_block(merge_block);
        builder.seal_block(merge_block);
        builder.block_params(merge_block)[0]
    }

    fn bind_case_pattern(
        &mut self,
        builder: &mut FunctionBuilder,
        pat: &ast::Pat,
        val: Value,
        env: &mut Env,
    ) {
        match &pat.node {
            ast::PatKind::Var(name) => env.set(name, val),
            ast::PatKind::Wildcard => {}
            ast::PatKind::Constructor { name, payload } => {
                if name == "True" || name == "False" {
                    // Bool is represented as Value::Bool, not Value::Constructor —
                    // calling knot_constructor_payload would panic. The payload is
                    // conceptually unit (Bool has no fields).
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    self.bind_case_pattern(builder, payload, unit, env);
                } else if matches!(self.nullable_ctors.get(name), Some(NullableRole::None)) {
                    // Nullable none: payload is conceptually unit
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    self.bind_case_pattern(builder, payload, unit, env);
                } else if matches!(self.nullable_ctors.get(name), Some(NullableRole::Some)) {
                    // Nullable some: val is the bare payload
                    self.bind_case_pattern(builder, payload, val, env);
                } else {
                    let inner = self.call_rt(builder, "knot_constructor_payload", &[val]);
                    self.bind_case_pattern(builder, payload, inner, env);
                }
            }
            ast::PatKind::Record(fields) => {
                for fp in fields {
                    let (key_ptr, key_len) = self.string_ptr(builder, &fp.name);
                    let field_val =
                        self.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len]);
                    if let Some(inner_pat) = &fp.pattern {
                        self.bind_case_pattern(builder, inner_pat, field_val, env);
                    } else {
                        // Punned: {name} means {name: name}
                        env.set(&fp.name, field_val);
                    }
                }
            }
            ast::PatKind::Lit(_) => {
                // Literal patterns don't bind anything
            }
            ast::PatKind::List(pats) => {
                for (idx, elem_pat) in pats.iter().enumerate() {
                    let index = builder.ins().iconst(self.ptr_type, idx as i64);
                    let elem =
                        self.call_rt(builder, "knot_relation_get", &[val, index]);
                    self.bind_case_pattern(builder, elem_pat, elem, env);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                let zero = builder.ins().iconst(self.ptr_type, 0);
                let head_val =
                    self.call_rt(builder, "knot_relation_get", &[val, zero]);
                let tail_val =
                    self.call_rt(builder, "knot_relation_tail", &[val]);
                self.bind_case_pattern(builder, head, head_val, env);
                self.bind_case_pattern(builder, tail, tail_val, env);
            }
        }
    }

    /// Extract a constructor's payload value for pattern binding
    /// (Bool and nullable constructors have special representations).
    fn case_ctor_payload(
        &mut self,
        builder: &mut FunctionBuilder,
        name: &str,
        val: Value,
    ) -> Value {
        if name == "True" || name == "False" {
            // Bool is represented as Value::Bool, not Value::Constructor —
            // calling knot_constructor_payload would panic. The payload is
            // conceptually unit (Bool has no fields).
            self.call_rt(builder, "knot_value_unit", &[])
        } else if matches!(self.nullable_ctors.get(name), Some(NullableRole::None)) {
            // Nullable none: payload is conceptually unit
            self.call_rt(builder, "knot_value_unit", &[])
        } else if matches!(self.nullable_ctors.get(name), Some(NullableRole::Some)) {
            // Nullable some: val is the bare payload
            val
        } else {
            self.call_rt(builder, "knot_constructor_payload", &[val])
        }
    }

    /// Bind a case-arm pattern whose TOP-LEVEL discriminant was already
    /// tested by `compile_case`, emitting runtime tests for refutable
    /// SUB-patterns (nested literals, constructor tags, list shapes).
    /// On a sub-pattern mismatch, control branches to `fail_block` (the
    /// next arm's test, or the no-match panic block for the last arm).
    fn bind_case_pattern_checked(
        &mut self,
        builder: &mut FunctionBuilder,
        pat: &ast::Pat,
        val: Value,
        env: &mut Env,
        fail_block: cranelift_codegen::ir::Block,
    ) {
        match &pat.node {
            ast::PatKind::Var(name) => env.set(name, val),
            ast::PatKind::Wildcard => {}
            // Top-level literal equality was tested by compile_case.
            ast::PatKind::Lit(_) => {}
            ast::PatKind::Constructor { name, payload } => {
                // Tag already tested at top level — test+bind the payload.
                let inner = self.case_ctor_payload(builder, name, val);
                self.test_and_bind_case_subpattern(builder, payload, inner, env, fail_block);
            }
            ast::PatKind::Record(fields) => {
                for fp in fields {
                    let (key_ptr, key_len) = self.string_ptr(builder, &fp.name);
                    let field_val =
                        self.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len]);
                    if let Some(inner_pat) = &fp.pattern {
                        self.test_and_bind_case_subpattern(
                            builder, inner_pat, field_val, env, fail_block,
                        );
                    } else {
                        // Punned: {name} means {name: name}
                        env.set(&fp.name, field_val);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                // Length already tested at top level — test+bind elements.
                for (idx, elem_pat) in pats.iter().enumerate() {
                    let index = builder.ins().iconst(self.ptr_type, idx as i64);
                    let elem =
                        self.call_rt(builder, "knot_relation_get", &[val, index]);
                    self.test_and_bind_case_subpattern(builder, elem_pat, elem, env, fail_block);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                // Non-emptiness already tested at top level.
                let zero = builder.ins().iconst(self.ptr_type, 0);
                let head_val =
                    self.call_rt(builder, "knot_relation_get", &[val, zero]);
                let tail_val =
                    self.call_rt(builder, "knot_relation_tail", &[val]);
                self.test_and_bind_case_subpattern(builder, head, head_val, env, fail_block);
                self.test_and_bind_case_subpattern(builder, tail, tail_val, env, fail_block);
            }
        }
    }

    /// Test a NESTED case sub-pattern against `val`, branching to
    /// `fail_block` on mismatch, then bind its variables (recursively
    /// testing deeper sub-patterns the same way).
    fn test_and_bind_case_subpattern(
        &mut self,
        builder: &mut FunctionBuilder,
        pat: &ast::Pat,
        val: Value,
        env: &mut Env,
        fail_block: cranelift_codegen::ir::Block,
    ) {
        // Emit `brif test, cont, fail_block` and continue in cont.
        let branch_on = |builder: &mut FunctionBuilder, is_match: Value| {
            let cont = builder.create_block();
            builder.ins().brif(is_match, cont, &[], fail_block, &[]);
            builder.switch_to_block(cont);
            builder.seal_block(cont);
        };
        match &pat.node {
            ast::PatKind::Var(name) => env.set(name, val),
            ast::PatKind::Wildcard => {}
            ast::PatKind::Lit(lit) => {
                let lit_val = self.compile_lit(builder, lit);
                let eq_i32 = self.call_rt_typed(
                    builder,
                    "knot_value_eq_i32",
                    &[val, lit_val],
                    types::I32,
                );
                let is_eq = builder.ins().icmp_imm(IntCC::NotEqual, eq_i32, 0);
                branch_on(builder, is_eq);
            }
            ast::PatKind::Constructor { name, payload } => {
                let is_match = if name == "True" || name == "False" {
                    let bool_val = self.call_rt_typed(
                        builder,
                        "knot_value_get_bool",
                        &[val],
                        types::I32,
                    );
                    let expected = if name == "True" { 1i64 } else { 0i64 };
                    builder.ins().icmp_imm(IntCC::Equal, bool_val, expected)
                } else {
                    match self.nullable_ctors.get(name).cloned() {
                        Some(NullableRole::None) => {
                            builder.ins().icmp_imm(IntCC::Equal, val, 0)
                        }
                        Some(NullableRole::Some) => {
                            builder.ins().icmp_imm(IntCC::NotEqual, val, 0)
                        }
                        None => {
                            let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                            let matches = self.call_rt_typed(
                                builder,
                                "knot_constructor_matches",
                                &[val, tag_ptr, tag_len],
                                types::I32,
                            );
                            builder.ins().icmp_imm(IntCC::NotEqual, matches, 0)
                        }
                    }
                };
                branch_on(builder, is_match);
                let inner = self.case_ctor_payload(builder, name, val);
                self.test_and_bind_case_subpattern(builder, payload, inner, env, fail_block);
            }
            ast::PatKind::Record(fields) => {
                for fp in fields {
                    let (key_ptr, key_len) = self.string_ptr(builder, &fp.name);
                    let field_val =
                        self.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len]);
                    if let Some(inner_pat) = &fp.pattern {
                        self.test_and_bind_case_subpattern(
                            builder, inner_pat, field_val, env, fail_block,
                        );
                    } else {
                        env.set(&fp.name, field_val);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                let len = self.call_rt(builder, "knot_relation_len", &[val]);
                let expected = builder.ins().iconst(self.ptr_type, pats.len() as i64);
                let is_match = builder.ins().icmp(IntCC::Equal, len, expected);
                branch_on(builder, is_match);
                for (idx, elem_pat) in pats.iter().enumerate() {
                    let index = builder.ins().iconst(self.ptr_type, idx as i64);
                    let elem =
                        self.call_rt(builder, "knot_relation_get", &[val, index]);
                    self.test_and_bind_case_subpattern(builder, elem_pat, elem, env, fail_block);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                let len = self.call_rt(builder, "knot_relation_len", &[val]);
                let is_match = builder.ins().icmp_imm(IntCC::NotEqual, len, 0);
                branch_on(builder, is_match);
                let zero = builder.ins().iconst(self.ptr_type, 0);
                let head_val =
                    self.call_rt(builder, "knot_relation_get", &[val, zero]);
                let tail_val =
                    self.call_rt(builder, "knot_relation_tail", &[val]);
                self.test_and_bind_case_subpattern(builder, head, head_val, env, fail_block);
                self.test_and_bind_case_subpattern(builder, tail, tail_val, env, fail_block);
            }
        }
    }

    // ── Monadic operation compilation ─────────────────────────────

    /// Compile `__yield(val)` / bare `yield val` — dispatches through
    /// Applicative trait impl based on monad_info.
    fn compile_monadic_yield(
        &mut self,
        builder: &mut FunctionBuilder,
        val: Value,
        span: ast::Span,
        db: Value,
    ) -> Value {
        if let Some(MonadKind::IO) = self.monad_info.get(&span) {
            // IO yield: wrap value in IO thunk via knot_io_pure
            return self.call_rt(builder, "knot_io_pure", &[val]);
        }
        let type_name = match self.monad_info.get(&span) {
            Some(MonadKind::Adt(name)) => name.clone(),
            _ => "Relation".to_string(),
        };
        // Built-in Result yield (pure/return)
        if type_name == "Result" {
            return self.call_rt(builder, "knot_result_yield", &[val]);
        }
        let yield_fn = format!("Applicative_{}_yield", type_name);
        if let Some(&(func_id, _)) = self.user_fns.get(&yield_fn) {
            let func_ref = self
                .module
                .declare_func_in_func(func_id, builder.func);
            let call = builder.ins().call(func_ref, &[db, val]);
            return builder.inst_results(call)[0];
        }
        // Ultimate fallback: direct runtime call
        self.call_rt(builder, "knot_relation_singleton", &[val])
    }

    /// Compile `__empty` — dispatches through Alternative trait impl.
    fn compile_monadic_empty(
        &mut self,
        builder: &mut FunctionBuilder,
        span: ast::Span,
        db: Value,
    ) -> Value {
        let type_name = match self.monad_info.get(&span) {
            Some(MonadKind::Adt(name)) => name.clone(),
            Some(MonadKind::IO) => "IO".to_string(),
            _ => "Relation".to_string(),
        };
        // Built-in Result empty
        if type_name == "Result" {
            return self.call_rt(builder, "knot_result_empty", &[]);
        }
        let empty_fn = format!("Alternative_{}_empty", type_name);
        if let Some(&(func_id, _)) = self.user_fns.get(&empty_fn) {
            let func_ref = self
                .module
                .declare_func_in_func(func_id, builder.func);
            let call = builder.ins().call(func_ref, &[db]);
            return builder.inst_results(call)[0];
        }
        // Ultimate fallback: direct runtime call
        self.call_rt(builder, "knot_relation_empty", &[])
    }

    // ── Do-block compilation ──────────────────────────────────────

    /// Check if a do-block should be compiled as IO (contains IO-producing builtins).
    /// Compile an expression that will be used as the value of a `set`/`replace`.
    /// Do-blocks in set-value position are always relational comprehensions,
    /// even when they contain SourceRef/DerivedRef binds (which would normally
    /// cause `is_io_do_block` to classify them as IO).
    fn compile_set_value_expr(
        &mut self,
        builder: &mut FunctionBuilder,
        value: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        match &value.node {
            ast::ExprKind::Do(stmts) => self.compile_do(builder, stmts, env, db),
            // Unwrap wrapper expressions to find the do-block inside.
            // E.g. `set *rel = (do { ... } : [T])` wraps the Do in Annot.
            ast::ExprKind::TimeUnitLit { value: inner, .. }
            | ast::ExprKind::Annot { expr: inner, .. } => {
                self.compile_set_value_expr(builder, inner, env, db)
            }
            ast::ExprKind::Refine(inner) => {
                self.compile_set_value_expr(builder, inner, env, db)
            }
            // `if`/`case` in set-value position: each branch is itself a
            // set value, so a do-block in a branch is a relational
            // comprehension too. The branches are compiled by the generic
            // `compile_expr` path, so record their do-block spans first and
            // let the `Do` arm of `compile_expr` consult the set.
            ast::ExprKind::If { .. } | ast::ExprKind::Case { .. } => {
                Self::collect_relational_do_spans(value, &mut self.relational_do_spans);
                self.compile_expr(builder, value, env, db)
            }
            _ => self.compile_expr(builder, value, env, db),
        }
    }

    /// Record the spans of do-blocks that a set/replace value produces its
    /// relation from. Result position extends through type/unit wrappers and
    /// through `if`/`case` branches (which may nest arbitrarily), so the walk
    /// mirrors `compile_set_value_expr`'s own recursion.
    fn collect_relational_do_spans(value: &ast::Expr, spans: &mut HashSet<ast::Span>) {
        match &value.node {
            ast::ExprKind::Do(_) => {
                spans.insert(value.span);
            }
            ast::ExprKind::TimeUnitLit { value: inner, .. }
            | ast::ExprKind::Annot { expr: inner, .. }
            | ast::ExprKind::Refine(inner) => Self::collect_relational_do_spans(inner, spans),
            ast::ExprKind::If {
                then_branch,
                else_branch,
                ..
            } => {
                Self::collect_relational_do_spans(then_branch, spans);
                Self::collect_relational_do_spans(else_branch, spans);
            }
            ast::ExprKind::Case { arms, .. } => {
                for arm in arms {
                    Self::collect_relational_do_spans(&arm.body, spans);
                }
            }
            _ => {}
        }
    }

    fn is_io_do_block(&self, stmts: &[ast::Stmt]) -> bool {
        // Do-blocks with groupBy always need relational iteration (compile_do),
        // even if they contain IO-like expressions, because groupBy requires
        // the loop-based collection/grouping phase that compile_io_do_eager
        // cannot provide.
        if stmts.iter().any(|s| matches!(&s.node, ast::StmtKind::GroupBy { .. })) {
            return false;
        }
        stmts.iter().any(|stmt| match &stmt.node {
            ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => self.expr_is_io(expr),
            ast::StmtKind::Expr(expr) => self.expr_is_io(expr),
            ast::StmtKind::Where { cond } => self.expr_is_io(cond),
            ast::StmtKind::GroupBy { .. } => false,
        })
    }

    /// Detect user functions whose bodies (transitively) produce IO values.
    /// Uses fixed-point iteration to handle transitive IO (e.g., genToken calls randomInt).
    fn detect_io_functions(&mut self, decls: &[ast::Decl]) {
        let io_builtins: HashSet<&str> = crate::builtins::EFFECTFUL_BUILTINS
            .iter()
            .filter(|n| **n != "retry")
            .copied()
            .collect();

        // Collect function bodies for analysis
        let mut fun_bodies: Vec<(String, &ast::Expr)> = Vec::new();
        for decl in decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body: Some(body), ty: Some(ts), .. } => {
                    // Seed IO functions from type annotations (same as desugar's fun_sig_io).
                    // Functions like `forEach` whose IO comes from trait-method calls
                    // (yield) are not detected by body scan alone.
                    if Self::type_returns_io_codegen(&ts.ty) {
                        self.io_functions.insert(name.clone());
                    }
                    fun_bodies.push((name.clone(), body));
                }
                ast::DeclKind::Fun { name, body: Some(body), .. } => {
                    fun_bodies.push((name.clone(), body));
                }
                // Trait methods are called by bare name (through the
                // dispatcher) — if any impl body produces IO, calls of the
                // method do too, so `tick 1; tick 2` in a do-block must be
                // classified as IO sequencing rather than a comprehension.
                ast::DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, body, .. } = item {
                            fun_bodies.push((name.clone(), body));
                        }
                    }
                }
                // Same for trait default method bodies.
                ast::DeclKind::Trait { items, .. } => {
                    for item in items {
                        if let ast::TraitItem::Method {
                            name,
                            default_body: Some(body),
                            ..
                        } = item
                        {
                            fun_bodies.push((name.clone(), body));
                        }
                    }
                }
                _ => {}
            }
        }

        // Fixed-point: keep iterating until no new IO functions are found
        loop {
            let mut changed = false;
            for (name, body) in &fun_bodies {
                if self.io_functions.contains(name) {
                    continue;
                }
                if Self::expr_contains_io(body, &io_builtins, &self.io_functions) {
                    self.io_functions.insert(name.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// Whether a declared type's final return type is `IO ...` (walking through
    /// curried function arrows). Mirrors desugar's `type_returns_io`.
    fn type_returns_io_codegen(ty: &ast::Type) -> bool {
        match &ty.node {
            ast::TypeKind::Function { result, .. } => Self::type_returns_io_codegen(result),
            ast::TypeKind::IO { .. } => true,
            _ => false,
        }
    }

    /// Check if an expression contains IO calls (builtins or known IO user functions).
    fn expr_contains_io(expr: &ast::Expr, builtins: &HashSet<&str>, io_fns: &HashSet<String>) -> bool {
        match &expr.node {
            ast::ExprKind::Var(name) => builtins.contains(name.as_str()) || io_fns.contains(name),
            ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => true,
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. } => true,
            ast::ExprKind::Atomic(_) => true,
            ast::ExprKind::App { func, arg } => {
                Self::expr_contains_io(func, builtins, io_fns)
                    || Self::expr_contains_io(arg, builtins, io_fns)
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                Self::expr_contains_io(lhs, builtins, io_fns)
                    || Self::expr_contains_io(rhs, builtins, io_fns)
            }
            ast::ExprKind::UnaryOp { operand, .. } => {
                Self::expr_contains_io(operand, builtins, io_fns)
            }
            ast::ExprKind::Do(stmts) => {
                stmts.iter().any(|s| match &s.node {
                    ast::StmtKind::Bind { expr, .. } => Self::expr_contains_io(expr, builtins, io_fns),
                    ast::StmtKind::Expr(expr) => Self::expr_contains_io(expr, builtins, io_fns),
                    ast::StmtKind::Let { expr, .. } => Self::expr_contains_io(expr, builtins, io_fns),
                    ast::StmtKind::Where { cond } => Self::expr_contains_io(cond, builtins, io_fns),
                    ast::StmtKind::GroupBy { key } => Self::expr_contains_io(key, builtins, io_fns),
                })
            }
            ast::ExprKind::Lambda { body, .. } => Self::expr_contains_io(body, builtins, io_fns),
            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                Self::expr_contains_io(cond, builtins, io_fns)
                    || Self::expr_contains_io(then_branch, builtins, io_fns)
                    || Self::expr_contains_io(else_branch, builtins, io_fns)
            }
            ast::ExprKind::Case { scrutinee, arms, .. } => {
                Self::expr_contains_io(scrutinee, builtins, io_fns)
                    || arms.iter().any(|arm| Self::expr_contains_io(&arm.body, builtins, io_fns))
            }
            // Records, lists, field access are data constructors/accessors —
            // they don't produce IO even if they contain IO values as
            // subexpressions. A function like `f x = {result: println x}`
            // returns a record, not IO.
            ast::ExprKind::TimeUnitLit { value, .. } => Self::expr_contains_io(value, builtins, io_fns),
            ast::ExprKind::Annot { expr, .. } => Self::expr_contains_io(expr, builtins, io_fns),
            ast::ExprKind::Refine(inner) => Self::expr_contains_io(inner, builtins, io_fns),
            ast::ExprKind::Record(_)
            | ast::ExprKind::RecordUpdate { .. }
            | ast::ExprKind::FieldAccess { .. }
            | ast::ExprKind::List(_) => false,
            _ => false,
        }
    }

    /// Detect user functions whose bodies (transitively) perform a relation
    /// write. Mirror of `detect_io_functions`. The result lets `compile_atomic`
    /// skip the SAVEPOINT entirely for read-only atomic bodies — the version-
    /// snapshot retry machinery doesn't need transactional rollback when no
    /// SQL write can occur.
    /// Populate `passthrough_functions`: top-level functions whose body may
    /// evaluate to one of their own parameters *unapplied* (so the caller runs
    /// it), directly (`id = \x -> x`) or via branches (`when = \c a -> if c
    /// then a else yield {}`), or by forwarding a parameter into another
    /// passthrough function. Fixed-point to capture forwarding chains.
    fn detect_passthrough_functions(&mut self, decls: &[ast::Decl]) {
        let mut fun_bodies: Vec<(String, &ast::Expr)> = Vec::new();
        for decl in decls {
            if let ast::DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                fun_bodies.push((name.clone(), body));
            }
        }
        loop {
            let mut changed = false;
            for (name, body) in &fun_bodies {
                if self.passthrough_functions.contains(name) {
                    continue;
                }
                // Collect the (possibly curried) parameter names, then test
                // whether the innermost body returns one of them unapplied.
                let mut params: HashSet<String> = HashSet::new();
                let mut cur = strip_expr_wrappers(body);
                while let ast::ExprKind::Lambda { params: ps, body: inner } = &cur.node {
                    for p in ps {
                        collect_pat_var_names(p, &mut params);
                    }
                    cur = strip_expr_wrappers(inner);
                }
                if params.is_empty() {
                    continue;
                }
                if Self::tail_returns_param(cur, &params, &self.passthrough_functions) {
                    self.passthrough_functions.insert(name.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// True when `expr`, in value/tail position, may evaluate to one of `params`
    /// unapplied — as a bare reference, through `if`/`case` branches, or by
    /// forwarding a param into a known passthrough function.
    fn tail_returns_param(
        expr: &ast::Expr,
        params: &HashSet<String>,
        passthrough_fns: &HashSet<String>,
    ) -> bool {
        match &strip_expr_wrappers(expr).node {
            ast::ExprKind::Var(name) => params.contains(name),
            ast::ExprKind::If { then_branch, else_branch, .. } => {
                Self::tail_returns_param(then_branch, params, passthrough_fns)
                    || Self::tail_returns_param(else_branch, params, passthrough_fns)
            }
            ast::ExprKind::Case { arms, .. } => arms
                .iter()
                .any(|arm| Self::tail_returns_param(&arm.body, params, passthrough_fns)),
            ast::ExprKind::App { .. } => {
                // `g <args>` where g is a passthrough fn and some argument is one
                // of our params passed unapplied → we forward that param onward.
                let (head, spine_args) = uncurry_app(expr);
                match &strip_expr_wrappers(head).node {
                    ast::ExprKind::Var(hname) if passthrough_fns.contains(hname) => spine_args
                        .iter()
                        .any(|a| matches!(&strip_expr_wrappers(a).node,
                            ast::ExprKind::Var(n) if params.contains(n))),
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn detect_write_functions(&mut self, decls: &[ast::Decl]) {
        let mut fun_bodies: Vec<(String, &ast::Expr)> = Vec::new();
        for decl in decls {
            if let ast::DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                fun_bodies.push((name.clone(), body));
            }
        }
        self.top_fn_names = fun_bodies.iter().map(|(n, _)| n.clone()).collect();
        loop {
            let mut changed = false;
            for (name, body) in &fun_bodies {
                if self.write_functions.contains(name) {
                    continue;
                }
                if Self::expr_contains_writes(body, &self.write_functions, &self.top_fn_names, &self.passthrough_functions) {
                    self.write_functions.insert(name.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// True if `expr` may perform a relation write at runtime. Used to decide
    /// whether an `atomic` body needs a `SAVEPOINT`.
    ///
    /// Pessimistic on lambdas: we treat any Lambda body as potentially
    /// containing writes only if its body literally does, since the lambda
    /// itself is a value (no side effects on construction); but applications
    /// of an unknown function (e.g. through a parameter) are conservatively
    /// treated as writes by the App fallthrough. A nested `Atomic` is treated
    /// as writing because we can't rule out writes within it without re-running
    /// the analysis on the inner body — and even a read-only inner atomic
    /// gates on the same SAVEPOINT decision at the outer level: the inner
    /// retry machinery is self-contained either way.
    fn expr_contains_writes(
        expr: &ast::Expr,
        write_fns: &HashSet<String>,
        known_fns: &HashSet<String>,
        passthrough_fns: &HashSet<String>,
    ) -> bool {
        // A callee/IO-value name is "known write-free" only when it is a
        // top-level function (the fixed-point analysis covers it) or a
        // builtin. Anything else — a parameter, a do-local lambda binding,
        // a trait-method dispatcher — could perform a write at runtime, so
        // it must be treated as possibly-writing (emitting the SAVEPOINT is
        // always safe; skipping it is not).
        let name_is_known_write_free = |name: &str| -> bool {
            !write_fns.contains(name)
                && (known_fns.contains(name)
                    || is_builtin_name(name)
                    || matches!(name, "yield" | "__bind" | "__yield" | "__empty"))
        };
        // True when evaluating/running `e` could write through a value of
        // unknown provenance (e.g. an IO action received as a parameter).
        let unknown_io_value = |e: &ast::Expr| -> bool {
            match &strip_expr_wrappers(e).node {
                ast::ExprKind::Var(name) => !name_is_known_write_free(name),
                _ => false,
            }
        };
        match &expr.node {
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. } => true,
            ast::ExprKind::Atomic(inner) => Self::expr_contains_writes(inner, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::Var(name) => write_fns.contains(name),
            ast::ExprKind::App { func, arg } => {
                // Conservatively treat applications of unknown callees as
                // possibly-writing: a lambda received as a parameter
                // (`runAtomic = \act -> atomic (do rows <- *t; act rows)`)
                // can perform a write the static analysis can't see.
                let (head, spine_args) = uncurry_app(expr);
                let head_unknown = match &strip_expr_wrappers(head).node {
                    ast::ExprKind::Var(name) => !name_is_known_write_free(name),
                    ast::ExprKind::Constructor(_) => false,
                    // A literal lambda's body is covered by recursion below.
                    ast::ExprKind::Lambda { .. } => false,
                    // Computed callee (field access, case result, ...) —
                    // can't tell what it does.
                    _ => true,
                };
                // A passthrough combinator (`when`/`unless`/`id`, …) returns an
                // argument unapplied to be run by the caller. If that argument
                // is an opaque IO value, the returned action may write — a fact
                // the head-name check misses because the passthrough itself is
                // "known write-free". Treat such applications as possibly-writing.
                let passthrough_arg_writes = matches!(
                    &strip_expr_wrappers(head).node,
                    ast::ExprKind::Var(name) if passthrough_fns.contains(name)
                ) && spine_args.iter().any(|a| unknown_io_value(a));
                head_unknown
                    || passthrough_arg_writes
                    || Self::expr_contains_writes(func, write_fns, known_fns, passthrough_fns)
                    || Self::expr_contains_writes(arg, write_fns, known_fns, passthrough_fns)
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                Self::expr_contains_writes(lhs, write_fns, known_fns, passthrough_fns)
                    || Self::expr_contains_writes(rhs, write_fns, known_fns, passthrough_fns)
            }
            ast::ExprKind::UnaryOp { operand, .. } => Self::expr_contains_writes(operand, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
                // Bind/expression statements RUN their value when it is an
                // IO action — a bare `io` of unknown provenance may write.
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Expr(expr) => {
                    unknown_io_value(expr)
                        || Self::expr_contains_writes(expr, write_fns, known_fns, passthrough_fns)
                }
                ast::StmtKind::Let { expr, .. } => Self::expr_contains_writes(expr, write_fns, known_fns, passthrough_fns),
                ast::StmtKind::Where { cond } => Self::expr_contains_writes(cond, write_fns, known_fns, passthrough_fns),
                ast::StmtKind::GroupBy { key } => Self::expr_contains_writes(key, write_fns, known_fns, passthrough_fns),
            }),
            ast::ExprKind::Lambda { body, .. } => Self::expr_contains_writes(body, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                Self::expr_contains_writes(cond, write_fns, known_fns, passthrough_fns)
                    || Self::expr_contains_writes(then_branch, write_fns, known_fns, passthrough_fns)
                    || Self::expr_contains_writes(else_branch, write_fns, known_fns, passthrough_fns)
            }
            ast::ExprKind::Case { scrutinee, arms, .. } => {
                Self::expr_contains_writes(scrutinee, write_fns, known_fns, passthrough_fns)
                    || arms.iter().any(|arm| Self::expr_contains_writes(&arm.body, write_fns, known_fns, passthrough_fns))
            }
            ast::ExprKind::TimeUnitLit { value, .. } => Self::expr_contains_writes(value, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::Annot { expr, .. } => Self::expr_contains_writes(expr, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::Refine(inner) => Self::expr_contains_writes(inner, write_fns, known_fns, passthrough_fns),
            ast::ExprKind::Record(fields) => fields
                .iter()
                .any(|f| Self::expr_contains_writes(&f.value, write_fns, known_fns, passthrough_fns)),
            ast::ExprKind::RecordUpdate { base, fields } => {
                Self::expr_contains_writes(base, write_fns, known_fns, passthrough_fns)
                    || fields.iter().any(|f| Self::expr_contains_writes(&f.value, write_fns, known_fns, passthrough_fns))
            }
            ast::ExprKind::FieldAccess { expr, .. } => {
                Self::expr_contains_writes(expr, write_fns, known_fns, passthrough_fns)
            }
            ast::ExprKind::List(items) => {
                items.iter().any(|e| Self::expr_contains_writes(e, write_fns, known_fns, passthrough_fns))
            }
            _ => false,
        }
    }

    /// Check if an expression produces an IO value (calls an IO-returning builtin
    /// or a user-defined IO function).
    fn expr_is_io(&self, expr: &ast::Expr) -> bool {
        match &expr.node {
            ast::ExprKind::App { func, arg } => {
                self.expr_is_io(func) || self.expr_is_io(arg)
            }
            ast::ExprKind::Var(name) => {
                crate::builtins::is_io_builtin(name)
                || matches!(
                    name.as_str(),
                    "fork" | "race"
                ) || self.io_functions.contains(name)
            }
            ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => true,
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. } => true,
            ast::ExprKind::Atomic(_) => true,
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.expr_is_io(lhs) || self.expr_is_io(rhs)
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.expr_is_io(operand),
            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                self.expr_is_io(cond)
                    || self.expr_is_io(then_branch)
                    || self.expr_is_io(else_branch)
            }
            ast::ExprKind::Case { scrutinee, arms, .. } => {
                self.expr_is_io(scrutinee)
                    || arms.iter().any(|arm| self.expr_is_io(&arm.body))
            }
            ast::ExprKind::Do(stmts) => {
                stmts.iter().any(|s| match &s.node {
                    ast::StmtKind::Bind { expr, .. } => self.expr_is_io(expr),
                    ast::StmtKind::Expr(expr) => self.expr_is_io(expr),
                    ast::StmtKind::Let { expr, .. } => self.expr_is_io(expr),
                    ast::StmtKind::Where { cond } => self.expr_is_io(cond),
                    ast::StmtKind::GroupBy { key } => self.expr_is_io(key),
                })
            }
            ast::ExprKind::Lambda { body, .. } => self.expr_is_io(body),
            ast::ExprKind::TimeUnitLit { value, .. } => self.expr_is_io(value),
            ast::ExprKind::Annot { expr, .. } => self.expr_is_io(expr),
            ast::ExprKind::Refine(inner) => self.expr_is_io(inner),
            _ => false,
        }
    }

    /// Check whether a do-block has relation-comprehension shape: every
    /// statement before the last is a Bind/Where/Let and the last statement
    /// is a `yield`. Such blocks have per-row loop semantics (`where` is a
    /// row filter, `yield` accumulates) rather than sequential IO semantics
    /// (`where` is a guard).
    fn do_block_is_comprehension(stmts: &[ast::Stmt]) -> bool {
        let Some((last, init)) = stmts.split_last() else {
            return false;
        };
        let last_is_yield = matches!(
            &last.node,
            ast::StmtKind::Expr(e) if e.node.as_yield_arg().is_some()
        );
        last_is_yield
            && init.iter().all(|s| {
                matches!(
                    &s.node,
                    ast::StmtKind::Bind { .. }
                        | ast::StmtKind::Where { .. }
                        | ast::StmtKind::Let { .. }
                )
            })
    }

    /// Does a `where` in these statements filter on `name`'s FIELDS?
    ///
    /// Only a per-row reading gives such a `where` any meaning: as a guard over
    /// the whole relation, `u.age` is the FIRST row's age (field access on a
    /// relation delegates to its first element), so the guard either waves every
    /// row through or drops the block entirely. Seeing one therefore settles
    /// `x <- *rel` as a comprehension bind even when the name is also used as a
    /// whole value — `yield u` yields the ROW.
    ///
    /// A `where` that uses `name` as a value (`where count people > 3`) is a
    /// real guard on the relation and is deliberately not counted. The scan
    /// stops at a statement that rebinds `name`, past which the uses belong to
    /// a different binding.
    fn where_filters_row_fields(stmts: &[ast::Stmt], name: &str) -> bool {
        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Where { cond } => {
                    if expr_refs_var(cond, name) && !expr_uses_var_as_value(cond, name) {
                        return true;
                    }
                }
                ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. }
                    if pat_bound_names(pat).iter().any(|n| n == name) => {
                    return false;
                }
                _ => {}
            }
        }
        false
    }

    /// Like `do_block_is_comprehension`, but also admits `groupBy`
    /// statements: a do-block ending in `yield` whose other statements are
    /// all Bind/Where/Let/GroupBy compiles through the relational loop
    /// paths and therefore produces a relation value.
    fn do_block_is_relational_shape(stmts: &[ast::Stmt]) -> bool {
        let Some((last, init)) = stmts.split_last() else {
            return false;
        };
        let last_is_yield = matches!(
            &last.node,
            ast::StmtKind::Expr(e) if e.node.as_yield_arg().is_some()
        );
        last_is_yield
            && init.iter().all(|s| {
                matches!(
                    &s.node,
                    ast::StmtKind::Bind { .. }
                        | ast::StmtKind::Where { .. }
                        | ast::StmtKind::Let { .. }
                        | ast::StmtKind::GroupBy { .. }
                )
            })
    }

    /// Check whether an expression's IO-ness involves *external* effects
    /// (console/fs/network/clock/random builtins, fork/race, atomic blocks,
    /// relation writes, user IO functions) rather than plain relation reads.
    /// Relation-only IO (`IO {} [T]` produced by SourceRef/DerivedRef
    /// comprehensions) is treated by inference as the underlying relation
    /// when let-bound, so codegen must run it eagerly; external-effect IO
    /// bound by `let` must stay deferred and run at its use sites.
    /// Mirrors the traversal of `expr_is_io`.
    fn expr_has_external_io(&self, expr: &ast::Expr) -> bool {
        match &expr.node {
            ast::ExprKind::App { func, arg } => {
                self.expr_has_external_io(func) || self.expr_has_external_io(arg)
            }
            ast::ExprKind::Var(name) => {
                crate::builtins::is_io_builtin(name)
                    || matches!(name.as_str(), "fork" | "race")
                    || self.io_functions.contains(name)
            }
            // Relation reads are the "pure DB" IO that inference lets flow
            // as the relation value itself.
            ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => false,
            // Writes and atomic blocks must not run at `let` time.
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. } => true,
            ast::ExprKind::Atomic(_) => true,
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.expr_has_external_io(lhs) || self.expr_has_external_io(rhs)
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.expr_has_external_io(operand),
            ast::ExprKind::If { cond, then_branch, else_branch, .. } => {
                self.expr_has_external_io(cond)
                    || self.expr_has_external_io(then_branch)
                    || self.expr_has_external_io(else_branch)
            }
            ast::ExprKind::Case { scrutinee, arms, .. } => {
                self.expr_has_external_io(scrutinee)
                    || arms.iter().any(|arm| self.expr_has_external_io(&arm.body))
            }
            ast::ExprKind::Do(stmts) => {
                stmts.iter().any(|s| match &s.node {
                    ast::StmtKind::Bind { expr, .. } => self.expr_has_external_io(expr),
                    ast::StmtKind::Expr(expr) => self.expr_has_external_io(expr),
                    ast::StmtKind::Let { expr, .. } => self.expr_has_external_io(expr),
                    ast::StmtKind::Where { cond } => self.expr_has_external_io(cond),
                    ast::StmtKind::GroupBy { key } => self.expr_has_external_io(key),
                })
            }
            ast::ExprKind::Lambda { body, .. } => self.expr_has_external_io(body),
            ast::ExprKind::TimeUnitLit { value, .. } => self.expr_has_external_io(value),
            ast::ExprKind::Annot { expr, .. } => self.expr_has_external_io(expr),
            ast::ExprKind::Refine(inner) => self.expr_has_external_io(inner),
            _ => false,
        }
    }

    /// Compile an IO do-block: builds IO thunk that sequences actions.
    /// Inside the thunk, IO binds run their action and bind the result.
    fn compile_io_do(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Value {
        // Build the entire do-block as an IO thunk using a helper function.
        // The helper function, when called, runs each IO action with knot_io_run.
        self.compile_io_do_as_thunk(builder, stmts, env, db)
    }

    /// Compile IO do-block as a deferred thunk.
    /// Creates a separate Cranelift function for the do-block body and returns
    /// an IO value `IO(fn_ptr, env)` that, when run via `knot_io_run`, executes
    /// the IO actions. This ensures side effects are deferred until the IO value
    /// is actually executed (important for `fork`, storing IO values, etc.).
    fn compile_io_do_as_thunk(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        _db: Value,
    ) -> Value {
        let thunk_name = format!("knot_io_thunk_{}", self.io_thunk_counter);
        self.io_thunk_counter += 1;

        // Find free variables in the do-block statements
        let dummy_span = ast::Span::new(0, 0);
        let do_expr = ast::Spanned::new(ast::ExprKind::Do(stmts.to_vec()), dummy_span);
        // Capture everything bound in the enclosing local scope — including
        // names that shadow top-level functions/constants or builtins (the
        // local binding wins; the previous user_fns filter silently resolved
        // such names to the global inside the thunk). Names not in env are
        // unshadowed globals/builtins, resolved inside the thunk body.
        let free_vars: Vec<String> = find_free_vars(&do_expr, &[])
            .into_iter()
            .filter(|v| env.bindings.contains_key(v))
            .collect();

        // Declare the thunk function: (db, env) -> result
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env (captured vars)
        sig.returns.push(AbiParam::new(self.ptr_type));
        let func_id = self
            .module
            .declare_function(&thunk_name, Linkage::Local, &sig)
            .unwrap();

        // Queue for later compilation
        self.pending_io_thunks.push(PendingIoThunk {
            func_id,
            stmts: stmts.to_vec(),
            free_vars: free_vars.clone(),
        });

        // Build the closure env: capture free variables (same pattern as lambdas)
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);

        let env_val = if free_vars.is_empty() {
            builder.ins().iconst(self.ptr_type, 0) // null env
        } else if free_vars.len() == 1 {
            match env.get(&free_vars[0]) {
                Some(v) => v,
                None => {
                    let msg =
                        format!("codegen: undefined captured variable '{}'", free_vars[0]);
                    self.push_codegen_error(builder, ast::Span::new(0, 0), msg)
                }
            }
        } else {
            let n = free_vars.len();
            let mut sorted_vars: Vec<&str> = free_vars.iter().map(|s| s.as_str()).collect();
            sorted_vars.sort();

            let ptr_bytes = self.ptr_type.bytes() as i32;
            let slot_size = (3 * n as u32) * ptr_bytes as u32;
            let slot = builder.create_sized_stack_slot(
                StackSlotData::new(StackSlotKind::ExplicitSlot, slot_size, 3),
            );
            for (i, var_name) in sorted_vars.iter().enumerate() {
                let val = match env.get(var_name) {
                    Some(v) => v,
                    None => {
                        let msg =
                            format!("codegen: undefined captured variable '{}'", var_name);
                        self.push_codegen_error(builder, ast::Span::new(0, 0), msg)
                    }
                };
                let (key_ptr, key_len) = self.string_ptr(builder, var_name);
                let base = (i as i32) * (3 * ptr_bytes);
                builder.ins().stack_store(key_ptr, slot, base);
                builder.ins().stack_store(key_len, slot, base + ptr_bytes);
                builder.ins().stack_store(val, slot, base + 2 * ptr_bytes);
            }
            let data_ptr = builder.ins().stack_addr(self.ptr_type, slot, 0);
            let count = builder.ins().iconst(self.ptr_type, n as i64);
            self.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count])
        };

        // Create IO value: IO(fn_ptr, env)
        self.call_rt(builder, "knot_io_new", &[fn_addr, env_val])
    }

    /// Compile an expression in *argument* position, where the value is handed
    /// to a callee instead of being executed here.
    ///
    /// `Set`/`ReplaceSet` are typed `IO {} {}`, but their `compile_expr` arms
    /// emit the write inline. That is correct for a do-block *statement* (the
    /// write is meant to run at that point) and wrong for an argument: in
    /// `when False (*rel = …)` the argument is compiled while building the call,
    /// so the write fired no matter what the guard said — the callee only ever
    /// received the already-performed write's unit result. Wrap the write in a
    /// single-statement IO thunk instead, which is exactly what a `do`-block
    /// argument (`when False do *rel = …`) already compiles to, and why that
    /// form was unaffected. The callee now decides: `when False` drops the
    /// thunk, `when True` returns it and the caller's `knot_io_run` executes it.
    ///
    /// Mirrors the `Atomic`-in-`let` case above, which defers for the same
    /// reason.
    fn compile_arg_expr(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        if matches!(
            &expr.node,
            ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. }
        ) {
            let do_stmts = vec![ast::Spanned::new(
                ast::StmtKind::Expr(expr.clone()),
                expr.span,
            )];
            return self.compile_io_do_as_thunk(builder, &do_stmts, env, db);
        }
        self.compile_expr(builder, expr, env, db)
    }

    /// Compile IO do-block body eagerly (runs IO actions inline).
    /// Used inside IO thunk bodies where laziness is already provided by the
    /// thunk wrapper. Returns the raw result value (not wrapped in IO).
    fn compile_io_do_eager(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let prev_io_eager = self.in_io_eager;
        self.in_io_eager = true;
        // Cleared here and set only by the bind-loop hand-off below, so the
        // value an enclosing bind loop reads after this call describes THIS
        // block (see the field's doc comment).
        self.io_do_tail_iterated = false;
        // Intermediate sub-compilations (a nested IO do-block in an if/case
        // branch, a bound expression, …) can clobber `io_do_tail_iterated`. Track
        // whether THIS block actually ended in the iterating-bind hand-off in a
        // local, and write it back once at the very end — otherwise a clobbered
        // `true` would leak to the enclosing `compile_io_bind_loop`, which reads
        // the flag right after this call to choose splice-vs-push.
        let mut ended_in_tail_iteration = false;
        // Save source_var_binds to restore after — but DON'T clear, so
        // inner do-blocks (else branches, nested blocks) inherit entries
        // from the enclosing scope.
        let prev_source_var_binds = self.source_var_binds.clone();
        // Save let_bindings so the in-scope set we accumulate here is
        // restored on exit.  We DO clear new entries on a per-name basis
        // (via the saved snapshot) but keep outer-scope entries visible
        // while compiling inner do-blocks.
        let prev_let_bindings = self.let_bindings.clone();
        // Save relation-valued binding names for the same scoping reasons.
        let prev_io_relation_vars = self.io_relation_vars.clone();
        // Mark the invalidation log so writes/rebinds inside this scope are
        // replayed onto the restored snapshots when the scope exits.
        let invalidation_mark = self.source_bind_invalidations.len();
        let mut last_val = self.call_rt(builder, "knot_value_unit", &[]);

        // Create a done block for early exit on guard/pattern failures.
        // Failed guards jump here with unit instead of panicking.
        let done_block = builder.create_block();
        let done_param = builder.append_block_param(done_block, self.ptr_type);

        for (stmt_idx, stmt) in stmts.iter().enumerate() {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    // A bind shadows any previous source/let tracking for the
                    // names it binds — drop stale entries before (re)inserting.
                    self.invalidate_rebound_pattern(pat);
                    // Comprehension-style bind over a plain (non-IO) relation
                    // value (`b <- [1, 2]; tick b`): inference types this as
                    // iteration (b : element), so run the remaining
                    // statements once per row — IO actions execute per
                    // element. Restricted to expressions statically known to
                    // be relations; other non-IO binds (single-value pattern
                    // matches like `InProgress ip <- t.status`) keep
                    // bind-the-value semantics.
                    // A refutable pattern (Constructor/List/Cons/Lit) bound
                    // from a relation source iterates per row, mirroring the
                    // type checker which types `Circle c <- *shapes` as a
                    // comprehension bind (c : element, per-row filter). Without
                    // this, the IO source falls through to bind_io_pattern
                    // against the *whole* relation value, which always
                    // mismatches and silently skips the rest of the do-block
                    // (returning {}). Plain (non-IO) relation values are
                    // already covered by the second disjunct below.
                    let pat_filters_rows = matches!(
                        &pat.node,
                        ast::PatKind::Constructor { .. }
                            | ast::PatKind::List(_)
                            | ast::PatKind::Cons { .. }
                            | ast::PatKind::Lit(_)
                    );
                    let rhs_is_io_relation_source = matches!(
                        &expr.node,
                        ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_)
                    );
                    // A comprehension TAIL inside a sequential IO block:
                    //
                    //   replace *people = [...]      -- IO statement
                    //   p <- *people                 -- ← this bind
                    //   where p.age > 27
                    //   yield p.name
                    //
                    // The bind and every statement after it form a relation
                    // comprehension, so the bind must iterate ROWS — `where`
                    // filters each row and `yield` accumulates the matches.
                    // Taking the whole relation instead gives `where` GUARD
                    // semantics, under which `p.age` silently means "the FIRST
                    // row's age": examples/hello.knot printed only "Alice"
                    // (row 1 passed the guard, so its single yield became the
                    // whole result) and never "Carol". PR #63 fixed this same
                    // root cause for the `<-`-bound form (`xs <- do { r <- *rel;
                    // where …; yield … }`); this is the same comprehension
                    // written directly as the block's trailing statements.
                    //
                    // Both readings of `x <- *rel` are legal, so the bound
                    // name's USES pick between them: DESIGN.md's
                    // `&seniors = do { people <- *people;
                    //                  yield (filter (\p -> p.age > 65) people) }`
                    // passes the name on as the whole relation, while the
                    // comprehension above only ever reads fields off it. Iterate
                    // when every use is a field access on the name — referenced,
                    // but never as a value — which is meaningless under
                    // whole-relation semantics anyway ("first row's age").
                    //
                    // That rule alone misses the most ordinary comprehension of
                    // all, `yield x` (yield the ROW):
                    //
                    //   &adults = do { u <- *users; where u.age >= 25; yield u }
                    //
                    // `yield u` uses the name as a value, so the block fell back
                    // to whole-relation semantics and the `where` became a guard
                    // on the FIRST row: `u.age >= 25` was true for row 1, the
                    // guard passed, and `yield u` handed back the WHOLE relation
                    // — every user, filter silently ignored. Flip the predicate
                    // to one row 1 fails and the guard drops the whole block to
                    // `{}`, so the caller's `count` panicked on a non-relation.
                    //
                    // A `where` that reads FIELDS off the bound name is itself
                    // the proof that the bind iterates: as a whole-relation guard
                    // it can only mean "the first row's field", which is never
                    // what a filter means. So it settles the reading regardless
                    // of how `yield` uses the name. A `where` over the name as a
                    // VALUE (`where count people > 3`) is a genuine guard on the
                    // relation and keeps the whole-relation reading.
                    let comprehension_tail = rhs_is_io_relation_source
                        && Self::do_block_is_comprehension(&stmts[stmt_idx..])
                        && match &pat.node {
                            ast::PatKind::Var(name) => {
                                let tail = ast::Spanned::new(
                                    ast::ExprKind::Do(stmts[stmt_idx + 1..].to_vec()),
                                    stmt.span,
                                );
                                (expr_refs_var(&tail, name)
                                    && !expr_uses_var_as_value(&tail, name))
                                    || Self::where_filters_row_fields(
                                        &stmts[stmt_idx + 1..],
                                        name,
                                    )
                            }
                            _ => false,
                        };
                    let rhs_iterates = (!self.expr_is_io(expr)
                        && (matches!(&expr.node, ast::ExprKind::List(_))
                            || self.expr_is_known_relation(expr)
                            || self.expr_is_relation_var(expr)
                            // A pure comprehension over the relation monad is
                            // desugared before codegen into an `App` spine of
                            // `__bind`/`__yield` (no longer an `ExprKind::Do`),
                            // and inference types the bind pattern as the
                            // ELEMENT type. The Let arm recognizes this shape
                            // (via `desugared_monad_kind`, ~line 8434) to mark
                            // the binding relation-valued; mirror it here so
                            // `x <- do { a <- [1,2,3]; yield a }` iterates
                            // per-row (x : element) instead of binding the whole
                            // relation value (which prints the list once and
                            // panics on field access).
                            || self.desugared_monad_kind(expr)
                                == Some(MonadKind::Relation)))
                        || (pat_filters_rows && rhs_is_io_relation_source)
                        || comprehension_tail;
                    // Names (re)bound by this pattern are rows from here on,
                    // not the relation-valued lets they may have shadowed.
                    self.io_relation_vars.retain(|n| !pat_binds(pat, n));
                    if rhs_iterates {
                        last_val = self.compile_io_bind_loop(
                            builder,
                            pat,
                            expr,
                            &stmts[stmt_idx + 1..],
                            env,
                            db,
                        );
                        // The remaining statements were consumed by the loop, so
                        // this block's value is the loop's relation of per-row
                        // results — tell an enclosing loop to splice, not nest.
                        // Recorded in the local and written to the field at exit.
                        ended_in_tail_iteration = true;
                        break;
                    }
                    // Track source read bindings for SQL optimization:
                    // `x <- *source` records x → source so inner do-blocks
                    // like `do { m <- x; where ...; yield m }` can compile to SQL.
                    if let ast::PatKind::Var(var_name) = &pat.node
                        && let ast::ExprKind::SourceRef(source_name) = &expr.node {
                            self.source_var_binds.insert(var_name.clone(), source_name.clone());
                        }
                    // A comprehension over relation sources (`xs <- do { r <-
                    // *rel; where …; yield … }`) is IO only because of the
                    // relation reads, and inference types it as the relation
                    // itself. Its `where` is a per-row FILTER, but the generic
                    // `compile_expr` path would route the do-block through
                    // `compile_io_do` (is_io_do_block sees the SourceRef bind),
                    // where `where` is a GUARD over the whole relation value and
                    // `r.v` silently means "first row's v" — a false guard then
                    // skips the rest of the block and binds `{}`, losing every
                    // row. Compile it through the relational loop path instead,
                    // exactly as the Let arm does for `let xs = do { … }`.
                    let relation_only_comprehension = matches!(
                        &expr.node,
                        ast::ExprKind::Do(do_stmts)
                            if Self::do_block_is_comprehension(do_stmts)
                    ) && self.expr_is_io(expr)
                        && !self.expr_has_external_io(expr);
                    let io_val = match &expr.node {
                        ast::ExprKind::Do(do_stmts) if relation_only_comprehension => {
                            self.compile_do(builder, do_stmts, env, db)
                        }
                        _ => self.compile_expr(builder, expr, env, db),
                    };
                    // Run the IO action to get the result. `knot_io_run` is the
                    // identity on non-IO values, so the relation produced above
                    // passes through untouched.
                    let result = self.call_rt(builder, "knot_io_run", &[db, io_val]);
                    // Bind the result to the pattern. Inside a bind-loop's
                    // rest, a mismatch skips the current row instead of
                    // pushing unit into the loop result.
                    let mismatch_target =
                        self.io_loop_skip_block.unwrap_or(done_block);
                    self.bind_io_pattern(builder, pat, result, env, Some(mismatch_target));
                    // Running the bound action may have written relations —
                    // variables bound from those sources are now stale.
                    self.invalidate_after_possible_writes(expr);
                    last_val = result;
                }
                ast::StmtKind::Let { pat, expr } => {
                    // A let rebinding a name drops its source/let tracking.
                    self.invalidate_rebound_pattern(pat);
                    // Track let-bound expressions so SQL pushdown matchers
                    // can fold through them when matching set/replace shapes
                    // later in the do-block.
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        self.let_bindings.insert(var_name.clone(), expr.clone());
                    }
                    // A let-bound relation comprehension (`let xs = do { t <-
                    // *rel; where ...; yield t }`) is typed by inference as
                    // the relation itself (`[T]`) — its IO-ness is only
                    // relation reads, no external effects. Compile it through
                    // the relational loop path (per-row iteration, `where` as
                    // filter) and bind the resulting rows, instead of leaving
                    // an unexecuted IO thunk with guard semantics. Other
                    // relation-only IO expressions are run eagerly with
                    // `knot_io_run` (identity on non-IO values). External-
                    // effect IO (`let action = println "x"`) stays deferred
                    // and runs at its use sites.
                    let relation_only_io =
                        self.expr_is_io(expr) && !self.expr_has_external_io(expr);
                    let val = match &expr.node {
                        ast::ExprKind::Do(stmts)
                            if relation_only_io
                                && Self::do_block_is_comprehension(stmts) =>
                        {
                            self.compile_do(builder, stmts, env, db)
                        }
                        // An `atomic` block compiles *eagerly* — the Atomic arm
                        // of compile_expr emits the savepoint/retry loop inline
                        // rather than producing a deferred IO thunk. Binding it
                        // with the general `compile_expr` path below would run
                        // the whole transaction once, right here at the `let`,
                        // and bind the name to the transaction's *result value*.
                        // Later uses (`let bump = atomic do {…}; bump; bump`)
                        // would then be plain values, and each `knot_io_run` on
                        // them a no-op — so the transaction fires once instead of
                        // per use. Wrap the atomic in an IO thunk instead: the
                        // bound name holds a deferred `Value::IO(fn_ptr, env)`,
                        // and each use re-runs the whole transaction when
                        // `knot_io_run` executes it (the thunk body compiles the
                        // atomic eagerly via compile_io_do_eager → compile_expr,
                        // but only when invoked).
                        ast::ExprKind::Atomic(_) => {
                            let do_stmts = vec![ast::Spanned::new(
                                ast::StmtKind::Expr(expr.clone()),
                                expr.span,
                            )];
                            self.compile_io_do_as_thunk(builder, &do_stmts, env, db)
                        }
                        _ => {
                            let v = self.compile_expr(builder, expr, env, db);
                            if relation_only_io {
                                self.call_rt(builder, "knot_io_run", &[db, v])
                            } else {
                                v
                            }
                        }
                    };
                    // Track names whose let-bound value is statically a
                    // relation (comprehension/groupBy do-blocks, source
                    // reads, list literals, relation-returning stdlib calls,
                    // or another relation-valued name) so a later
                    // `row <- name` bind iterates instead of binding the
                    // whole relation value.
                    let is_relation_value = match &expr.node {
                        ast::ExprKind::Do(do_stmts) => {
                            !self.expr_has_external_io(expr)
                                && Self::do_block_is_relational_shape(do_stmts)
                        }
                        ast::ExprKind::List(_)
                        | ast::ExprKind::SourceRef(_)
                        | ast::ExprKind::DerivedRef(_) => true,
                        _ => {
                            self.expr_is_known_relation(expr)
                                || self.expr_is_relation_var(expr)
                                // A pure comprehension over the relation monad
                                // is desugared to an `App` of `__bind`/`__yield`
                                // — recognize it here so `let xs = do { … }`
                                // (relation-typed) is iterable downstream.
                                || self.desugared_monad_kind(expr)
                                    == Some(MonadKind::Relation)
                        }
                    };
                    self.io_relation_vars.retain(|n| !pat_binds(pat, n));
                    if is_relation_value
                        && let ast::PatKind::Var(var_name) = &pat.node {
                            self.io_relation_vars.insert(var_name.clone());
                        }
                    let mismatch_target =
                        self.io_loop_skip_block.unwrap_or(done_block);
                    self.bind_io_pattern(builder, pat, val, env, Some(mismatch_target));
                    self.invalidate_after_possible_writes(expr);
                    last_val = val;
                }
                ast::StmtKind::Where { cond } => {
                    // In IO do-blocks, where acts as a guard:
                    // if the condition is false, skip remaining statements
                    // and return unit — or, when these statements are the
                    // body of a comprehension-style bind loop, skip the
                    // current ROW (jump to the loop's skip block) so no
                    // unit value is pushed into the loop's result. Inside
                    // an atomic body, also signal skip so the surrounding
                    // `atomic` rolls back.
                    let cond_i32 =
                        self.compile_condition(builder, cond, env, db);
                    self.invalidate_after_possible_writes(cond);
                    let is_true =
                        builder.ins().icmp_imm(IntCC::NotEqual, cond_i32, 0);
                    let pass_block = builder.create_block();
                    let fail_block = builder.create_block();
                    builder
                        .ins()
                        .brif(is_true, pass_block, &[], fail_block, &[]);
                    builder.switch_to_block(fail_block);
                    builder.seal_block(fail_block);
                    // Only signal an atomic-wide skip when this guard is a
                    // top-level statement of the atomic body. Inside a
                    // comprehension bind loop (`io_loop_skip_block.is_some()`)
                    // a false guard just drops the current row — it must not
                    // roll back the whole transaction.
                    if self.atomic_retry_block.is_some() && self.io_loop_skip_block.is_none() {
                        self.call_rt(builder, "knot_stm_skip", &[]);
                    }
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    let guard_target =
                        self.io_loop_skip_block.unwrap_or(done_block);
                    builder.ins().jump(guard_target, &[unit.into()]);
                    builder.switch_to_block(pass_block);
                    builder.seal_block(pass_block);
                }
                ast::StmtKind::Expr(expr) => {
                    last_val = self.compile_io_expr_eager(builder, expr, env, db);
                    // A bare statement that writes (e.g. `*items = ...`)
                    // invalidates source-bound variables read before it.
                    self.invalidate_after_possible_writes(expr);
                }
                ast::StmtKind::GroupBy { .. } => {
                    // groupBy in IO do-blocks is a type error — the type checker
                    // rebinds variables from T to [T], but codegen silently drops
                    // the grouping, producing incorrect results. Emit a clear
                    // error instead of silently miscompiling.
                    panic!("knot codegen: groupBy is not supported inside IO do-blocks");
                }
            }
        }

        // Normal flow joins done_block with the final result
        builder.ins().jump(done_block, &[last_val.into()]);
        builder.switch_to_block(done_block);
        builder.seal_block(done_block);

        self.in_io_eager = prev_io_eager;
        self.source_var_binds = prev_source_var_binds;
        self.let_bindings = prev_let_bindings;
        self.io_relation_vars = prev_io_relation_vars;
        // Canonicalize the hand-off flag to describe exactly THIS block, undoing
        // any clobber from intermediate sub-compilations (see the local's note).
        self.io_do_tail_iterated = ended_in_tail_iteration;
        // Writes/rebinds inside this scope still invalidate outer entries.
        self.replay_source_bind_invalidations_since(invalidation_mark);
        done_param
    }

    /// Compile a comprehension-style bind inside an IO do-block:
    /// `pat <- expr` where `expr` is a plain (non-IO) relation value.
    /// Iterates the relation, binding `pat` to each row and running the
    /// remaining statements per row (IO actions execute per element, `where`
    /// guards and constructor-pattern mismatches skip the current row).
    /// Returns a relation of the per-row results.
    fn compile_io_bind_loop(
        &mut self,
        builder: &mut FunctionBuilder,
        pat: &ast::Pat,
        expr: &ast::Expr,
        rest: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let raw = self.compile_expr(builder, expr, env, db);
        // Relation sources (`*source`, `&derived`) are IO-typed: compile_expr
        // yields an IO value that must be run to produce the relation. Plain
        // relation values (list literals, relation vars) are non-IO and pass
        // through knot_io_run unchanged (it returns non-IO values as-is), so
        // running it unconditionally is safe too — but skip it for non-IO
        // expressions to avoid a redundant call.
        let rel = if self.expr_is_io(expr) {
            self.call_rt(builder, "knot_io_run", &[db, raw])
        } else {
            raw
        };
        let result = self.call_rt(builder, "knot_relation_empty", &[]);
        let len = self.call_rt(builder, "knot_relation_len", &[rel]);

        let header = builder.create_block();
        let body = builder.create_block();
        let continue_blk = builder.create_block();
        // The continue block receives the per-row value and pushes it into
        // the result relation.
        merge_block_param(builder, continue_blk, self.ptr_type);
        // The skip block is the target of guard failures (`where` false)
        // and pattern-bind mismatches: it advances to the next row WITHOUT
        // pushing anything, so skipped rows don't appear as unit values in
        // the result. It takes one (ignored) param so the shared
        // jump-with-unit fail paths in bind_io_pattern/compile_io_do_eager
        // can target it uniformly.
        let skip_blk = builder.create_block();
        merge_block_param(builder, skip_blk, self.ptr_type);
        let exit = builder.create_block();

        let zero = builder.ins().iconst(self.ptr_type, 0);
        builder.ins().jump(header, &[zero.into()]);

        builder.switch_to_block(header);
        let i = builder.append_block_param(header, self.ptr_type);
        let cond = builder.ins().icmp(IntCC::UnsignedLessThan, i, len);
        builder.ins().brif(cond, body, &[], exit, &[]);

        builder.switch_to_block(body);
        builder.seal_block(body);
        let row = self.call_rt(builder, "knot_relation_get", &[rel, i]);
        // Bind into a per-iteration env clone so loop-local SSA values never
        // leak into code emitted after the loop exits.
        let mut body_env = env.clone();
        // Register the loop's skip block as the active row-skip target *before*
        // binding the loop pattern, so a refutable loop-variable bind (e.g.
        // `Circle c <- *shapes`) that mismatches a row skips that row rather
        // than aborting a surrounding `atomic` (the `knot_stm_skip` calls in
        // `bind_io_pattern` are suppressed when `io_loop_skip_block.is_some()`).
        // While compiling the remaining statements, guard failures likewise
        // skip the current row (innermost loop wins; restored on exit).
        let prev_skip = self.io_loop_skip_block.replace(skip_blk);
        self.bind_io_pattern(builder, pat, row, &mut body_env, Some(skip_blk));
        let row_val = if rest.is_empty() {
            row
        } else {
            self.compile_io_do_eager(builder, rest, &mut body_env, db)
        };
        // When the tail is itself a comprehension loop (`t <- *teams; m <-
        // t.members; yield m.who`), its value is that loop's per-row results —
        // rows of THIS comprehension, not a single nested row. Splice them in,
        // or the result comes out one relation deep per nesting level.
        let tail_iterated = !rest.is_empty() && self.io_do_tail_iterated;
        self.io_loop_skip_block = prev_skip;
        builder.ins().jump(continue_blk, &[row_val.into()]);

        // continue_blk pushes the row value, then falls through to skip_blk
        // for the shared "advance to next row" step (this also guarantees
        // skip_blk is reachable even when no guard ever targets it).
        builder.switch_to_block(continue_blk);
        builder.seal_block(continue_blk);
        let cont_val = builder.block_params(continue_blk)[0];
        if tail_iterated {
            self.call_rt_void(builder, "knot_relation_extend", &[result, cont_val]);
        } else {
            self.call_rt_void(builder, "knot_relation_push", &[result, cont_val]);
        }
        builder.ins().jump(skip_blk, &[cont_val.into()]);

        builder.switch_to_block(skip_blk);
        builder.seal_block(skip_blk);
        let one = builder.ins().iconst(self.ptr_type, 1);
        let next = builder.ins().iadd(i, one);
        builder.ins().jump(header, &[next.into()]);
        builder.seal_block(header);

        builder.switch_to_block(exit);
        builder.seal_block(exit);
        result
    }

    /// Compile an expression eagerly in IO do-block context.
    /// For if/else and case expressions, inlines IO do-block branches
    /// directly (using compile_io_do_eager) instead of creating deferred
    /// thunks. This avoids variable capture issues where captured vars
    /// (from enclosing IO binds) resolve to wrong values at runtime.
    /// For other expressions, compiles normally and runs knot_io_run.
    fn compile_io_expr_eager(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        if let Some(inner) = expr.node.as_yield_arg() {
            return self.compile_expr(builder, inner, env, db);
        }
        if let ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } = &expr.node
        {
            let cond_i32 = self.compile_condition(builder, cond, env, db);
            let is_true =
                builder.ins().icmp_imm(IntCC::NotEqual, cond_i32, 0);
            let then_block = builder.create_block();
            let else_block = builder.create_block();
            let merge_block = builder.create_block();
            builder.append_block_param(merge_block, self.ptr_type);
            builder
                .ins()
                .brif(is_true, then_block, &[], else_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            let then_val = self.compile_io_expr_eager(
                builder,
                then_branch,
                &mut env.clone(),
                db,
            );
            builder.ins().jump(merge_block, &[then_val.into()]);

            builder.switch_to_block(else_block);
            builder.seal_block(else_block);
            let else_val = self.compile_io_expr_eager(
                builder,
                else_branch,
                &mut env.clone(),
                db,
            );
            builder.ins().jump(merge_block, &[else_val.into()]);

            builder.switch_to_block(merge_block);
            builder.seal_block(merge_block);
            return builder.block_params(merge_block)[0];
        }
        if let ast::ExprKind::Do(stmts) = &expr.node
            && self.is_io_do_block(stmts) {
                // A fresh nested do-block has its own guard semantics
                // (where false returns unit from THIS block) — its guards
                // must not skip the enclosing bind-loop's row.
                let prev_skip = self.io_loop_skip_block.take();
                let val = self.compile_io_do_eager(builder, stmts, env, db);
                self.io_loop_skip_block = prev_skip;
                return val;
            }
        // General case: compile and run knot_io_run — safe for non-IO
        // values (returns as-is), necessary for higher-order functions
        // whose IO callbacks aren't detectable by expr_is_io.
        let val = self.compile_expr(builder, expr, env, db);
        self.call_rt(builder, "knot_io_run", &[db, val])
    }

    /// Bind a pattern in an IO do-block context.
    /// When `done_block` is Some, constructor mismatches jump there with unit
    /// instead of panicking — the rest of the do-block is skipped.
    fn bind_io_pattern(
        &mut self,
        builder: &mut FunctionBuilder,
        pat: &ast::Pat,
        val: Value,
        env: &mut Env,
        done_block: Option<cranelift_codegen::ir::Block>,
    ) {
        match &pat.node {
            ast::PatKind::Var(name) => {
                env.bindings.insert(name.clone(), val);
            }
            ast::PatKind::Wildcard => {}
            ast::PatKind::Record(fields) => {
                for f in fields {
                    let (field_ptr, field_len) = self.string_ptr(builder, &f.name);
                    let field_val = self.call_rt(
                        builder,
                        "knot_record_field",
                        &[val, field_ptr, field_len],
                    );
                    if let Some(ref inner_pat) = f.pattern {
                        self.bind_io_pattern(builder, inner_pat, field_val, env, done_block);
                    } else {
                        env.bindings.insert(f.name.clone(), field_val);
                    }
                }
            }
            ast::PatKind::Constructor { name, payload } => {
                let is_match = if name == "True" || name == "False" {
                    // Bool is represented as Value::Bool, not Value::Constructor —
                    // knot_constructor_matches would always return 0. Test the
                    // bool value directly, mirroring compile_case.
                    let bool_val =
                        self.call_rt_typed(builder, "knot_value_get_bool", &[val], types::I32);
                    let expected = if name == "True" { 1i64 } else { 0i64 };
                    builder.ins().icmp_imm(IntCC::Equal, bool_val, expected)
                } else {
                    match self.nullable_ctors.get(name).cloned() {
                        Some(NullableRole::None) => {
                            builder.ins().icmp_imm(IntCC::Equal, val, 0)
                        }
                        Some(NullableRole::Some) => {
                            builder.ins().icmp_imm(IntCC::NotEqual, val, 0)
                        }
                        None => {
                            let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                            let matches = self.call_rt_typed(
                                builder,
                                "knot_constructor_matches",
                                &[val, tag_ptr, tag_len],
                                types::I32,
                            );
                            builder.ins().icmp_imm(IntCC::NotEqual, matches, 0)
                        }
                    }
                };

                let then_block = builder.create_block();
                let fail_block = builder.create_block();
                builder.ins().brif(is_match, then_block, &[], fail_block, &[]);

                builder.switch_to_block(fail_block);
                builder.seal_block(fail_block);
                if let Some(done) = done_block {
                    // Inside an atomic IO body, signal "skip" so the
                    // surrounding `atomic` rolls back rather than committing
                    // partial writes. Outside atomic this is a no-op (the
                    // flag will simply be reset on the next atomic entry).
                    // A per-row pattern mismatch inside a comprehension bind
                    // loop (`io_loop_skip_block.is_some()`) only skips the row;
                    // it must not roll back a surrounding `atomic`. Signal an
                    // atomic-wide skip only for a top-level refutable bind.
                    if self.atomic_retry_block.is_some() && self.io_loop_skip_block.is_none() {
                        self.call_rt(builder, "knot_stm_skip", &[]);
                    }
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    builder.ins().jump(done, &[unit.into()]);
                } else {
                    self.call_rt_void(builder, "knot_guard_failed", &[]);
                    builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                }

                builder.switch_to_block(then_block);
                builder.seal_block(then_block);

                // Extract the constructor payload and bind the inner pattern
                let inner = self.case_ctor_payload(builder, name, val);
                self.bind_io_pattern(builder, payload, inner, env, done_block);
            }
            ast::PatKind::Lit(lit) => {
                // A literal pattern binds nothing but is *refutable*: a
                // mismatched value must skip (like the do/case paths), not be
                // silently accepted. Mirror the constructor arm's fail/skip.
                let lit_val = self.compile_lit(builder, lit);
                let eq_i32 = self.call_rt_typed(
                    builder,
                    "knot_value_eq_i32",
                    &[val, lit_val],
                    types::I32,
                );
                let is_match = builder.ins().icmp_imm(IntCC::NotEqual, eq_i32, 0);

                let then_block = builder.create_block();
                let fail_block = builder.create_block();
                builder.ins().brif(is_match, then_block, &[], fail_block, &[]);

                builder.switch_to_block(fail_block);
                builder.seal_block(fail_block);
                if let Some(done) = done_block {
                    // A per-row pattern mismatch inside a comprehension bind
                    // loop (`io_loop_skip_block.is_some()`) only skips the row;
                    // it must not roll back a surrounding `atomic`. Signal an
                    // atomic-wide skip only for a top-level refutable bind.
                    if self.atomic_retry_block.is_some() && self.io_loop_skip_block.is_none() {
                        self.call_rt(builder, "knot_stm_skip", &[]);
                    }
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    builder.ins().jump(done, &[unit.into()]);
                } else {
                    self.call_rt_void(builder, "knot_guard_failed", &[]);
                    builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                }

                builder.switch_to_block(then_block);
                builder.seal_block(then_block);
            }
            ast::PatKind::List(pats) => {
                // A fixed-length list pattern is *refutable*: only relations
                // of exactly `pats.len()` elements may continue. Mirror the
                // Lit arm's fail/skip so wrong-length values are rejected
                // instead of mis-binding out-of-bounds positions to Unit.
                let len = self.call_rt(builder, "knot_relation_len", &[val]);
                let expected = builder.ins().iconst(self.ptr_type, pats.len() as i64);
                let is_match = builder.ins().icmp(IntCC::Equal, len, expected);

                let then_block = builder.create_block();
                let fail_block = builder.create_block();
                builder.ins().brif(is_match, then_block, &[], fail_block, &[]);

                builder.switch_to_block(fail_block);
                builder.seal_block(fail_block);
                if let Some(done) = done_block {
                    // A per-row pattern mismatch inside a comprehension bind
                    // loop (`io_loop_skip_block.is_some()`) only skips the row;
                    // it must not roll back a surrounding `atomic`. Signal an
                    // atomic-wide skip only for a top-level refutable bind.
                    if self.atomic_retry_block.is_some() && self.io_loop_skip_block.is_none() {
                        self.call_rt(builder, "knot_stm_skip", &[]);
                    }
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    builder.ins().jump(done, &[unit.into()]);
                } else {
                    self.call_rt_void(builder, "knot_guard_failed", &[]);
                    builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                }

                builder.switch_to_block(then_block);
                builder.seal_block(then_block);

                for (idx, elem_pat) in pats.iter().enumerate() {
                    let index = builder.ins().iconst(self.ptr_type, idx as i64);
                    let elem =
                        self.call_rt(builder, "knot_relation_get", &[val, index]);
                    self.bind_io_pattern(builder, elem_pat, elem, env, done_block);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                // A cons pattern `(x : xs)` is *refutable*: it only matches a
                // non-empty relation. Without this guard, an empty value would
                // read index 0 (which the runtime returns as Unit) and silently
                // mis-bind `head`. Mirror the List/Lit arms' fail/skip.
                let len = self.call_rt(builder, "knot_relation_len", &[val]);
                let is_match = builder.ins().icmp_imm(IntCC::NotEqual, len, 0);

                let then_block = builder.create_block();
                let fail_block = builder.create_block();
                builder.ins().brif(is_match, then_block, &[], fail_block, &[]);

                builder.switch_to_block(fail_block);
                builder.seal_block(fail_block);
                if let Some(done) = done_block {
                    // A per-row pattern mismatch inside a comprehension bind
                    // loop (`io_loop_skip_block.is_some()`) only skips the row;
                    // it must not roll back a surrounding `atomic`. Signal an
                    // atomic-wide skip only for a top-level refutable bind.
                    if self.atomic_retry_block.is_some() && self.io_loop_skip_block.is_none() {
                        self.call_rt(builder, "knot_stm_skip", &[]);
                    }
                    let unit = self.call_rt(builder, "knot_value_unit", &[]);
                    builder.ins().jump(done, &[unit.into()]);
                } else {
                    self.call_rt_void(builder, "knot_guard_failed", &[]);
                    builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                }

                builder.switch_to_block(then_block);
                builder.seal_block(then_block);

                let zero = builder.ins().iconst(self.ptr_type, 0);
                let head_val =
                    self.call_rt(builder, "knot_relation_get", &[val, zero]);
                let tail_val =
                    self.call_rt(builder, "knot_relation_tail", &[val]);
                self.bind_io_pattern(builder, head, head_val, env, done_block);
                self.bind_io_pattern(builder, tail, tail_val, env, done_block);
            }
        }
    }

    /// Check that statements after a `groupBy` only reference the primary
    /// (grouped) bind variable among the names bound inside the pre-group
    /// loops. Returns a diagnostic for the first offending reference.
    fn validate_group_by_references(
        stmts: &[ast::Stmt],
        group_pos: usize,
    ) -> Option<knot::diagnostic::Diagnostic> {
        // The primary bind is the most recent bind before groupBy — it is
        // the one rebound to each group sub-relation.
        let mut primary: Option<String> = None;
        let mut first_bind_seen = false;
        let mut loop_local: HashSet<String> = HashSet::new();
        for stmt in &stmts[..group_pos] {
            match &stmt.node {
                ast::StmtKind::Bind { pat, .. } => {
                    let mut names = HashSet::new();
                    collect_pat_binds(pat, &mut names);
                    loop_local.extend(names);
                    first_bind_seen = true;
                    if let Some(p) = pat_primary_var(&pat.node) {
                        primary = Some(p);
                    }
                }
                // Lets BEFORE the first bind are emitted outside any loop
                // and stay valid after groupBy; lets after it are emitted
                // inside the loop bodies.
                ast::StmtKind::Let { pat, .. } if first_bind_seen => {
                    let mut names = HashSet::new();
                    collect_pat_binds(pat, &mut names);
                    loop_local.extend(names);
                }
                _ => {}
            }
        }
        if let Some(p) = &primary {
            loop_local.remove(p);
        }
        if loop_local.is_empty() {
            return None;
        }

        let mut live = loop_local;
        for stmt in &stmts[group_pos..] {
            let expr_to_check: Option<&ast::Expr> = match &stmt.node {
                ast::StmtKind::GroupBy { key } => Some(key),
                ast::StmtKind::Where { cond } => Some(cond),
                ast::StmtKind::Expr(e) => Some(e),
                ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => Some(expr),
            };
            if let Some(e) = expr_to_check
                && let Some(name) = live.iter().find(|n| expr_refs_var(e, n)).cloned() {
                    let grouped = primary
                        .as_ref()
                        .map(|p| format!(" '{}'", p))
                        .unwrap_or_default();
                    return Some(
                        knot::diagnostic::Diagnostic::error(format!(
                            "variable '{}' cannot be referenced after groupBy: \
                             only the grouped binding{} is rebound to each group",
                            name, grouped
                        ))
                        .label(stmt.span, format!("'{}' refers to a pre-groupBy row", name))
                        .note(
                            "yield the needed values into an intermediate relation \
                             before grouping, or group directly on the relation that \
                             contains them",
                        ),
                    );
                }
            // A post-group bind/let rebinding a loop-local name shadows it
            // for the remaining statements.
            if let ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } = &stmt.node {
                let mut names = HashSet::new();
                collect_pat_binds(pat, &mut names);
                for n in names {
                    live.remove(&n);
                }
            }
        }
        None
    }

    fn compile_do(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Value {
        // Try to compile as a single SQL query (multi-table joins, filters).
        if let Some(val) = self.try_compile_full_sql(builder, stmts, env, db) {
            return val;
        }

        // ── groupBy post-group reference validation ───────────────────
        // groupBy closes every pre-group loop; afterwards only the PRIMARY
        // bind variable is rebound (to each group). SSA values of other
        // pre-group binds (and lets emitted inside those loops) live in the
        // now-closed loop bodies and do NOT dominate post-group code —
        // compiling a reference to them would produce invalid IR (Cranelift
        // verifier panic). Reject such programs with a clean diagnostic
        // before emitting any IR for the block.
        if let Some(pos) = stmts
            .iter()
            .position(|s| matches!(&s.node, ast::StmtKind::GroupBy { .. }))
            && let Some(diag) = Self::validate_group_by_references(stmts, pos) {
                self.diagnostics.push(diag);
                return self.call_rt(builder, "knot_relation_empty", &[]);
            }

        // Save let_bindings for restoration on exit; new entries inserted
        // below are visible only inside this do-block's scope.
        let prev_let_bindings = self.let_bindings.clone();
        // Snapshot env bindings: every name bound inside this do-block
        // (bind patterns, lets, the groupBy rebind) refers to an SSA value
        // defined inside a loop body, which does NOT dominate code emitted
        // after the do-block. If such a binding shadowed an outer variable
        // that the caller references after the block, the caller would pick
        // up the loop-local SSA value and the Cranelift verifier rejects the
        // function ("uses value vN from non-dominating inst"). Restore the
        // caller's bindings wholesale on exit — do-block bindings are scoped
        // to the block and never legitimately escape it.
        let prev_env_bindings = env.bindings.clone();
        // Save source_var_binds too: binds in this block shadow outer
        // source-read tracking for the duration of the block.
        let prev_source_var_binds = self.source_var_binds.clone();
        let invalidation_mark = self.source_bind_invalidations.len();

        // Wrap the do-block in a dedicated arena frame.  Every yielded value
        // is `knot_arena_promote`d into pinned, which survives the
        // per-iteration `knot_arena_reset_to` but is NOT freed by it.
        // Without this push/pop-promote, pinned entries accumulate in the
        // *caller's* frame for the caller's entire lifetime — a leak in
        // any long-running function with a do-block (e.g. main event loops).
        //
        // With this frame: pinned entries live in the child frame, get
        // deep-cloned into the parent by `pop_frame_promote`, and the
        // child frame (including its pinned set) is dropped.
        self.call_rt_void(builder, "knot_arena_push_frame", &[]);
        // Count the open frame so a direct `retry` jump inside this
        // do-block (when nested in an atomic body) pops it first.
        self.atomic_arena_frames += 1;

        let result = self.call_rt(builder, "knot_relation_empty", &[]);
        let mut loop_stack: Vec<LoopInfo> = Vec::new();
        // Skip blocks for `where` guards that appear before any bind (so there
        // is no enclosing loop to skip to). In list/relation-monad semantics a
        // failed guard yields the empty relation, so these route to the
        // do-block's exit returning `result` as-is rather than trapping.
        let mut pre_bind_where_skips: Vec<cranelift_codegen::ir::Block> = Vec::new();

        // Pre-scan: if there's a groupBy, create a temp relation to collect
        // pre-group rows. This must be allocated before any loops start.
        let group_by_pos = stmts.iter().position(|s| {
            matches!(&s.node, ast::StmtKind::GroupBy { .. })
        });
        let temp = if group_by_pos.is_some() {
            self.call_rt(builder, "knot_relation_empty", &[])
        } else {
            // Placeholder — never used when there's no groupBy
            result
        };
        let mut primary_var: Option<String> = None;
        let mut primary_row_val: Option<Value> = None;
        let mut primary_source: Option<String> = None;
        // Direct schema tracking for groupBy — covers source, derived, and
        // nested-field binds so we don't rely solely on source_schemas lookup.
        let mut primary_schema: Option<String> = None;
        // Track per-variable schemas so FieldAccess binds can derive child schemas.
        let mut var_schemas: HashMap<String, String> = HashMap::new();

        // ── Pre-scan for hash join patterns ──────────────────────────
        // Look for: Bind(a, expr1) ... Bind(b, expr2) ... Where(a.f == b.g)
        // where expr2 does NOT reference a (so it can be hoisted).
        let mut consumed_wheres: HashSet<usize> = HashSet::new();
        // hash_join_info: inner_bind_idx -> (outer_var, outer_field, inner_field, inner_expr)
        struct HashJoinPlan {
            outer_var: String,
            outer_field: String,
            inner_field: String,
            _where_idx: usize,
        }
        let mut hash_join_plans: HashMap<usize, HashJoinPlan> = HashMap::new();

        // Collect bind stmts: (idx, var_name, expr)
        let bind_stmts: Vec<(usize, &str, &ast::Expr)> = stmts
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                if let ast::StmtKind::Bind { pat, expr } = &s.node
                    && let ast::PatKind::Var(name) = &pat.node {
                        return Some((i, name.as_str(), expr));
                    }
                None
            })
            .collect();

        // For each pair of binds, look for equi-join Where clauses.
        // Skipped entirely when a user Eq/Ord/Num impl on a primitive type
        // exists: the hash-index lookup compares keys by built-in equality,
        // bypassing the user's `eq` — the nested-loop fallback dispatches
        // the `==` through the trait correctly.
        let hash_join_allowed = !self.sql_pushdown_disabled_by_user_impls();
        for w in 0..bind_stmts.len() {
            if !hash_join_allowed {
                break;
            }
            // Only ONE hash-join plan is supported per inner bind (the map is
            // keyed by inner bind index). Once we've chosen a plan for this
            // inner bind against some earlier bind, stop: consuming a second
            // matching `where` against a *different* earlier bind would mark it
            // handled while its plan silently overwrites the first — dropping a
            // real join predicate and returning rows that violate it. Leaving
            // the second `where` unconsumed lets the normal nested-loop path
            // enforce it.
            let mut planned_for_inner = false;
            for v in 0..w {
                if planned_for_inner {
                    break;
                }
                let (_outer_idx, outer_var, _outer_expr) = bind_stmts[v];
                let (inner_idx, inner_var, inner_expr) = bind_stmts[w];

                // Inner expr must not reference the outer bind var
                if expr_references_var(inner_expr, outer_var) {
                    continue;
                }
                // Inner expr must be hoistable (source, derived, var, or list).
                let hoistable = match &inner_expr.node {
                    ast::ExprKind::SourceRef(_)
                    | ast::ExprKind::DerivedRef(_)
                    | ast::ExprKind::List(_) => true,
                    // A `Var` is only hoistable when it resolves OUTSIDE this
                    // do-block. A var bound by another Bind in this block is
                    // loop-local: its SSA value is defined inside a loop body
                    // and does not dominate the pre-loop prebuild site, so
                    // compiling it there is invalid IR (undefined-variable
                    // panic) or silently captures a stale outer binding of the
                    // same name. Only hoist vars from the enclosing scope.
                    ast::ExprKind::Var(name) => {
                        !bind_stmts.iter().any(|(_, bv, _)| bv == name)
                    }
                    _ => false,
                };
                if !hoistable {
                    continue;
                }

                // Scan Wheres between inner bind and the next bind/let/groupBy
                let search_end = stmts[inner_idx + 1..]
                    .iter()
                    .position(|s| {
                        matches!(
                            &s.node,
                            ast::StmtKind::Bind { .. }
                                | ast::StmtKind::Let { .. }
                                | ast::StmtKind::GroupBy { .. }
                        )
                    })
                    .map_or(stmts.len(), |p| inner_idx + 1 + p);

                for (offset, stmt) in stmts[inner_idx + 1..search_end].iter().enumerate() {
                    let wi = inner_idx + 1 + offset;
                    if consumed_wheres.contains(&wi) {
                        continue;
                    }
                    if let ast::StmtKind::Where { cond } = &stmt.node
                        && let Some((ov, of, iv, inf)) =
                            Self::match_equi_join(cond, outer_var, inner_var)
                        {
                            // Ensure the matched vars are the correct pair
                            if ov == outer_var && iv == inner_var {
                                consumed_wheres.insert(wi);
                                hash_join_plans.insert(
                                    inner_idx,
                                    HashJoinPlan {
                                        outer_var: ov.to_string(),
                                        outer_field: of.to_string(),
                                        inner_field: inf.to_string(),
                                        _where_idx: wi,
                                    },
                                );
                                planned_for_inner = true;
                                break; // one join per bind pair
                            }
                        }
                }
            }
        }

        // ── Pre-build hash join indices before any loops ──────────────
        let mut prebuilt_indices: HashMap<usize, Value> = HashMap::new();
        // Remember the stack height so this do-block's indices are popped on
        // every exit path (normal, and the groupBy error bails below). They
        // are registered on `pending_index_frees` so a `retry` inside the
        // enclosing `atomic` can free them before re-entering the loop.
        let index_free_mark = self.pending_index_frees.len();
        for (&stmt_idx, plan) in &hash_join_plans {
            if let ast::StmtKind::Bind { expr, .. } = &stmts[stmt_idx].node {
                let inner_rel = self.compile_expr(builder, expr, env, db);
                let (field_ptr, field_len) =
                    self.string_ptr(builder, &plan.inner_field);
                let idx_val = self.call_rt(
                    builder,
                    "knot_relation_build_index",
                    &[inner_rel, field_ptr, field_len],
                );
                prebuilt_indices.insert(stmt_idx, idx_val);
                self.pending_index_frees.push(idx_val);
            }
        }

        for (stmt_idx, stmt) in stmts.iter().enumerate() {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    // A bind shadows any outer source/let tracking for the
                    // names it binds (the bound variable is now a row, not
                    // the source-read it may have referred to outside).
                    self.invalidate_rebound_pattern(pat);
                    // ── Hash join path: use pre-built index for lookup ──
                    if let Some(plan) = hash_join_plans.get(&stmt_idx) {
                        let idx_val = prebuilt_indices[&stmt_idx];

                        // Look up matching rows via the pre-built hash index
                        let outer_val = match env.get(&plan.outer_var) {
                            Some(v) => v,
                            None => {
                                let msg = format!(
                                    "codegen: undefined join variable '{}'",
                                    plan.outer_var
                                );
                                self.push_codegen_error(builder, ast::Span::new(0, 0), msg)
                            }
                        };
                        let (fptr, flen) =
                            self.string_ptr(builder, &plan.outer_field);
                        let key =
                            self.call_rt(builder, "knot_record_field", &[outer_val, fptr, flen]);
                        let rel =
                            self.call_rt(builder, "knot_relation_index_lookup", &[idx_val, key]);

                        let len = self.call_rt(builder, "knot_relation_len", &[rel]);
                        let header = builder.create_block();
                        let body = builder.create_block();
                        let continue_blk = builder.create_block();
                        let exit = builder.create_block();

                        let zero = builder.ins().iconst(self.ptr_type, 0);
                        builder.ins().jump(header, &[zero.into()]);
                        builder.switch_to_block(header);
                        let i = builder.append_block_param(header, self.ptr_type);
                        let cond = builder.ins().icmp(IntCC::UnsignedLessThan, i, len);
                        builder.ins().brif(cond, body, &[], exit, &[]);
                        builder.switch_to_block(body);
                        builder.seal_block(body);

                        // Arena GC: mark at hash join loop body entry
                        let hj_arena_mark = self.call_rt(builder, "knot_arena_mark", &[]);

                        let row = self.call_rt(builder, "knot_relation_get", &[rel, i]);
                        let mut pattern_skips = Vec::new();
                        bind_do_pattern(builder, self, pat, row, env, &mut pattern_skips);

                        if group_by_pos.is_some() {
                            if let Some(name) = pat_primary_var(&pat.node) {
                                primary_var = Some(name.clone());
                            }
                            primary_row_val = Some(row);
                            match &expr.node {
                                ast::ExprKind::SourceRef(name)
                                | ast::ExprKind::DerivedRef(name) => {
                                    primary_source = Some(name.clone());
                                    primary_schema = self.source_schemas.get(name).cloned();
                                }
                                ast::ExprKind::FieldAccess { expr: target, field } => {
                                    primary_schema = None;
                                    if let ast::ExprKind::Var(parent_var) = &target.node
                                        && let Some(parent_schema) = var_schemas.get(parent_var) {
                                            primary_schema = extract_child_schema(parent_schema, field);
                                        }
                                    primary_source = None;
                                }
                                ast::ExprKind::Var(name) => {
                                    // Let-bound or previously-bound variable —
                                    // look up its schema from earlier binds.
                                    primary_source = None;
                                    primary_schema = var_schemas.get(name).cloned();
                                }
                                _ => {
                                    primary_source = None;
                                    primary_schema = None;
                                }
                            }
                            if let Some(ref schema) = primary_schema
                                && let Some(ref var_name) = primary_var {
                                    var_schemas.insert(var_name.clone(), schema.clone());
                                }
                        }

                        loop_stack.push(LoopInfo {
                            header,
                            continue_blk,
                            exit,
                            index_var: i,
                            where_skips: pattern_skips,
                            arena_mark: hj_arena_mark,
                        });
                        continue;
                    }

                    // ── Filter pushdown: try to push Where clauses into SQL ──
                    let use_filter_pushdown = if let ast::PatKind::Var(bind_var) = &pat.node {
                        if let ast::ExprKind::SourceRef(source_name) = &expr.node {
                            if !self.views.contains_key(source_name)
                                && self.source_schemas.contains_key(source_name)
                            {
                                // Look ahead at subsequent Where stmts
                                let mut sql_fragments: Vec<(usize, SqlFragment)> = Vec::new();
                                let search_end = stmts[stmt_idx + 1..]
                                    .iter()
                                    .position(|s| {
                                        matches!(
                                            &s.node,
                                            ast::StmtKind::Bind { .. }
                                                | ast::StmtKind::Let { .. }
                                                | ast::StmtKind::GroupBy { .. }
                                        )
                                    })
                                    .map_or(stmts.len(), |p| stmt_idx + 1 + p);

                                for (offset, stmt) in
                                    stmts[stmt_idx + 1..search_end].iter().enumerate()
                                {
                                    let wi = stmt_idx + 1 + offset;
                                    if consumed_wheres.contains(&wi) {
                                        continue;
                                    }
                                    if let ast::StmtKind::Where { cond } = &stmt.node {
                                        // Check all param sources are in scope
                                        if let Some(frag) =
                                            self.source_schemas.get(source_name).and_then(
                                                |schema| self.try_compile_sql_expr(bind_var, cond, schema))
                                        {
                                            let params_ok = frag.params.iter().all(|p| match p {
                                                SqlParamSource::Literal(_) | SqlParamSource::Expr(_) => true,
                                                SqlParamSource::Var(v) => {
                                                    v != bind_var && env.bindings.contains_key(v)
                                                }
                                                SqlParamSource::FieldAccess(v, _) => {
                                                    v != bind_var && env.bindings.contains_key(v)
                                                }
                                            });
                                            if params_ok {
                                                sql_fragments.push((wi, frag));
                                            }
                                        }
                                    }
                                }

                                if let (false, Some(schema)) = (
                                    sql_fragments.is_empty(),
                                    self.source_schemas.get(source_name).cloned(),
                                ) {
                                    // Mark consumed and emit knot_source_read_where
                                    let mut all_sql = Vec::new();
                                    let mut all_params = Vec::new();
                                    for (wi, frag) in &sql_fragments {
                                        consumed_wheres.insert(*wi);
                                        all_sql.push(format!("({})", frag.sql));
                                        all_params.extend(frag.params.clone());
                                    }
                                    let where_sql = all_sql.join(" AND ");
                                    let (name_ptr, name_len) =
                                        self.string_ptr(builder, source_name);
                                    let (schema_ptr, schema_len) =
                                        self.string_ptr(builder, &schema);
                                    let (where_ptr, where_len) =
                                        self.string_ptr(builder, &where_sql);
                                    let params_rel =
                                        self.compile_sql_params(builder, &all_params, env, db);
                                    let val = self.call_rt(
                                        builder,
                                        "knot_source_read_where",
                                        &[
                                            db, name_ptr, name_len, schema_ptr,
                                            schema_len, where_ptr, where_len,
                                            params_rel,
                                        ],
                                    );
                                    Some(val)
                                } else {
                                    // No pushable fragments, or the source schema is
                                    // unexpectedly missing: skip the pushdown
                                    // optimization and compile the filter normally.
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Arena GC: frame isolation for bind expressions with user calls.
                    // This reduces peak memory by freeing the callee's intermediate
                    // allocations immediately, keeping only the return value.
                    let needs_frame = !loop_stack.is_empty()
                        && use_filter_pushdown.is_none()
                        && expr_has_user_calls(expr, &self.user_fns);

                    if needs_frame {
                        self.call_rt_void(builder, "knot_arena_push_frame", &[]);
                        // Count the open frame for direct `retry` jumps
                        // emitted while compiling the bind expression.
                        self.atomic_arena_frames += 1;
                    }

                    let val = if let Some(pushed_val) = use_filter_pushdown {
                        pushed_val
                    } else {
                        self.compile_expr(builder, expr, env, db)
                    };

                    // Promote return value to parent frame, freeing callee temporaries
                    let val = if needs_frame {
                        self.atomic_arena_frames -= 1;
                        self.call_rt(builder, "knot_arena_pop_frame_promote", &[val])
                    } else {
                        val
                    };

                    // For constructor patterns, the RHS might be a single value
                    // (e.g., `InProgress ip <- t.status`). Wrap in a singleton
                    // relation so the loop logic works uniformly.
                    // Skip the call if the source is statically known to be a relation.
                    let rel = if matches!(&pat.node, ast::PatKind::Constructor { .. }) {
                        let is_known_relation = matches!(
                            &expr.node,
                            ast::ExprKind::SourceRef(_)
                                | ast::ExprKind::DerivedRef(_)
                                | ast::ExprKind::List(_)
                                | ast::ExprKind::Do(_)
                                | ast::ExprKind::Set { .. }
                                | ast::ExprKind::ReplaceSet { .. }
                        ) || self.expr_is_known_relation(expr);
                        if is_known_relation {
                            val
                        } else {
                            self.call_rt(builder, "knot_ensure_relation", &[val])
                        }
                    } else {
                        val
                    };
                    // knot_relation_len returns a raw usize, not a boxed Value
                    let len = self.call_rt(builder, "knot_relation_len", &[rel]);

                    let header = builder.create_block();
                    let body = builder.create_block();
                    let continue_blk = builder.create_block();
                    let exit = builder.create_block();

                    let zero = builder.ins().iconst(self.ptr_type, 0);
                    builder.ins().jump(header, &[zero.into()]);

                    builder.switch_to_block(header);
                    let i = builder.append_block_param(header, self.ptr_type);
                    let cond =
                        builder.ins().icmp(IntCC::UnsignedLessThan, i, len);
                    builder.ins().brif(cond, body, &[], exit, &[]);

                    builder.switch_to_block(body);
                    builder.seal_block(body);

                    // Arena GC: mark at loop body entry for per-iteration reset
                    let arena_mark = self.call_rt(builder, "knot_arena_mark", &[]);

                    let row = self.call_rt(builder, "knot_relation_get", &[rel, i]);

                    // Bind pattern (constructor patterns emit filter branches)
                    let mut pattern_skips = Vec::new();
                    bind_do_pattern(builder, self, pat, row, env, &mut pattern_skips);

                    // Track the primary bind variable (most recent Var or
                    // constructor-payload pattern) and source name for groupBy
                    if group_by_pos.is_some() {
                        if let Some(name) = pat_primary_var(&pat.node) {
                            primary_var = Some(name.clone());
                        }
                        primary_row_val = Some(row);
                        match &expr.node {
                            ast::ExprKind::SourceRef(name)
                            | ast::ExprKind::DerivedRef(name) => {
                                primary_source = Some(name.clone());
                                primary_schema = self.source_schemas.get(name).cloned();
                            }
                            ast::ExprKind::FieldAccess { expr: target, field } => {
                                // Nested relation bind (e.g. `item <- t.children`):
                                // extract the child field schema from the parent's schema.
                                primary_schema = None;
                                if let ast::ExprKind::Var(parent_var) = &target.node
                                    && let Some(parent_schema) = var_schemas.get(parent_var) {
                                        primary_schema = extract_child_schema(parent_schema, field);
                                    }
                                primary_source = None;
                            }
                            ast::ExprKind::Var(name) => {
                                // Let-bound or previously-bound variable —
                                // look up its schema from earlier binds.
                                primary_source = None;
                                primary_schema = var_schemas.get(name).cloned();
                            }
                            _ => {
                                primary_source = None;
                                primary_schema = None;
                            }
                        }
                        // Record the bound variable's schema for downstream FieldAccess lookups.
                        if let Some(ref schema) = primary_schema
                            && let Some(ref var_name) = primary_var {
                                var_schemas.insert(var_name.clone(), schema.clone());
                            }
                    }

                    loop_stack.push(LoopInfo {
                        header,
                        continue_blk,
                        exit,
                        index_var: i,
                        where_skips: pattern_skips,
                        arena_mark,
                    });
                }

                ast::StmtKind::Where { cond } => {
                    // Skip consumed Where stmts (pushed down to SQL or consumed by hash join)
                    if consumed_wheres.contains(&stmt_idx) {
                        continue;
                    }

                    let cond_i32 = self.compile_condition(builder, cond, env, db);
                    let is_true =
                        builder.ins().icmp_imm(IntCC::NotEqual, cond_i32, 0);

                    let then_block = builder.create_block();
                    let skip_block = builder.create_block();
                    builder
                        .ins()
                        .brif(is_true, then_block, &[], skip_block, &[]);

                    builder.switch_to_block(then_block);
                    builder.seal_block(then_block);

                    if let Some(loop_info) = loop_stack.last_mut() {
                        loop_info.where_skips.push(skip_block);
                    } else {
                        // Where outside a loop — there's no loop to skip to. A
                        // failed guard yields the empty relation (list/relation
                        // monad semantics), so defer the skip block and route it
                        // to the do-block's exit at the tail (returning the
                        // empty `result`). We stay in `then_block` to keep
                        // compiling the guarded continuation.
                        pre_bind_where_skips.push(skip_block);
                    }
                }

                ast::StmtKind::Let { pat, expr } => {
                    // A let rebinding a name drops its source/let tracking.
                    self.invalidate_rebound_pattern(pat);
                    // Track let-bound expressions so SQL pushdown matchers
                    // can fold through them when matching set/replace shapes
                    // later in the do-block.
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        self.let_bindings.insert(var_name.clone(), expr.clone());
                    }
                    let val = self.compile_expr(builder, expr, env, db);
                    self.invalidate_after_possible_writes(expr);

                    // Track schema of Let-bound relation variables for groupBy support.
                    // If the expression is a known relation (source, derived, or var),
                    // extract its schema and store it for later use in groupBy.
                    if group_by_pos.is_some()
                        && let ast::PatKind::Var(var_name) = &pat.node {
                            match &expr.node {
                                ast::ExprKind::SourceRef(name)
                                | ast::ExprKind::DerivedRef(name) => {
                                    if let Some(schema) = self.source_schemas.get(name).cloned() {
                                        var_schemas.insert(var_name.clone(), schema);
                                    }
                                }
                                ast::ExprKind::FieldAccess { expr: target, field } => {
                                    if let ast::ExprKind::Var(parent_var) = &target.node
                                        && let Some(parent_schema) = var_schemas.get(parent_var)
                                            && let Some(child_schema) = extract_child_schema(parent_schema, field) {
                                                var_schemas.insert(var_name.clone(), child_schema);
                                            }
                                }
                                ast::ExprKind::Var(source_var) => {
                                    // Let-bound from another variable — inherit its schema
                                    if let Some(schema) = var_schemas.get(source_var).cloned() {
                                        var_schemas.insert(var_name.clone(), schema);
                                    }
                                }
                                _ => {}
                            }
                        }

                    if matches!(
                        &pat.node,
                        ast::PatKind::Constructor { .. }
                            | ast::PatKind::Lit(_)
                            | ast::PatKind::List(_)
                            | ast::PatKind::Cons { .. }
                    ) {
                        // All refutable patterns need filter branches so a
                        // mismatched row is skipped (relational semantics),
                        // not trapped. `bind_do_pattern` emits skip blocks
                        // for Constructor/Lit/List/Cons uniformly.
                        let mut pattern_skips = Vec::new();
                        bind_do_pattern(builder, self, pat, val, env, &mut pattern_skips);
                        if let Some(loop_info) = loop_stack.last_mut() {
                            loop_info.where_skips.extend(pattern_skips);
                        } else {
                            // No loop context — seal skip blocks with guard failure
                            let current_block = builder.current_block().unwrap();
                            for skip in pattern_skips {
                                builder.switch_to_block(skip);
                                builder.seal_block(skip);
                                self.call_rt_void(builder, "knot_guard_failed", &[]);
                                builder.ins().trap(cranelift_codegen::ir::TrapCode::user(1).unwrap());
                            }
                            builder.switch_to_block(current_block);
                        }
                    } else {
                        self.bind_io_pattern(builder, pat, val, env, None);
                    }
                }

                ast::StmtKind::GroupBy { key } => {
                    // ── Phase transition: pre-group → post-group ──
                    //
                    // 1. Push the primary bind variable's value into temp
                    //    (we're inside the pre-group loops)
                    let var_val = match primary_row_val {
                        Some(v) => v,
                        None => {
                            self.diagnostics.push(
                                knot::diagnostic::Diagnostic::error(
                                    "groupBy requires a preceding bind from a relation \
                                     (e.g. `t <- *source` before `groupBy {key: t.field}`)",
                                )
                                .label(key.span, "groupBy without a preceding relation bind"),
                            );
                            // No primary bind means no loops were opened
                            // (loop_stack is empty here), so we can bail — but
                            // must still pop the do-block's arena frame and
                            // restore the shadowed bindings, or a later scope /
                            // `retry` in the enclosing function is corrupted.
                            let empty = self.call_rt(builder, "knot_relation_empty", &[]);
                            let mut promoted =
                                self.call_rt(builder, "knot_arena_pop_frame_promote", &[empty]);
                            self.atomic_arena_frames -= 1;
                            env.bindings = prev_env_bindings;
                            self.let_bindings = prev_let_bindings;
                            self.source_var_binds = prev_source_var_binds;
                            self.pending_index_frees.truncate(index_free_mark);
                            self.replay_source_bind_invalidations_since(invalidation_mark);
                            // Seal any pre-bind `where` skip blocks so they
                            // don't dangle as unsealed block references — the
                            // Cranelift verifier rejects them and aborts
                            // compilation with an internal error instead of the
                            // clean diagnostic we just emitted. Each skip block
                            // produces its own empty relation and passes it as
                            // the merge block param so `promoted` dominates.
                            if !pre_bind_where_skips.is_empty() {
                                let do_exit = builder.create_block();
                                let result_param = builder.append_block_param(do_exit, self.ptr_type);
                                builder.ins().jump(do_exit, &[promoted.into()]);
                                for skip in &pre_bind_where_skips {
                                    builder.switch_to_block(*skip);
                                    builder.seal_block(*skip);
                                    let skip_empty = self.call_rt(builder, "knot_relation_empty", &[]);
                                    let skip_promoted = self.call_rt(
                                        builder,
                                        "knot_arena_pop_frame_promote",
                                        &[skip_empty],
                                    );
                                    builder.ins().jump(do_exit, &[skip_promoted.into()]);
                                }
                                builder.switch_to_block(do_exit);
                                builder.seal_block(do_exit);
                                promoted = result_param;
                            }
                            return promoted;
                        }
                    };
                    // Arena GC: promote the row before pushing it into the
                    // temp relation. When the primary bind was materialized
                    // inside enclosing loops (cross join, non-equi where, or a
                    // compound condition not matched by `match_equi_join`),
                    // closing those loops below runs `knot_arena_reset_to` per
                    // iteration, which would free this row before
                    // `knot_relation_group_by` reads it. Promotion pins it,
                    // exactly as the yield path does. (A row read from a
                    // relation is never a promote-safe singleton, so no
                    // escape-analysis check is needed here.)
                    let var_val = if !loop_stack.is_empty() {
                        self.call_rt(builder, "knot_arena_promote", &[var_val])
                    } else {
                        var_val
                    };
                    self.call_rt_void(
                        builder,
                        "knot_relation_push",
                        &[temp, var_val],
                    );

                    // 2. Close all pre-group loops
                    while let Some(info) = loop_stack.pop() {
                        builder.ins().jump(info.continue_blk, &[]);
                        for skip in &info.where_skips {
                            builder.switch_to_block(*skip);
                            builder.seal_block(*skip);
                            builder.ins().jump(info.continue_blk, &[]);
                        }
                        builder.switch_to_block(info.continue_blk);
                        builder.seal_block(info.continue_blk);
                        self.call_rt_void(builder, "knot_arena_reset_to", &[info.arena_mark]);
                        let one = builder.ins().iconst(self.ptr_type, 1);
                        let next_i = builder.ins().iadd(info.index_var, one);
                        builder.ins().jump(info.header, &[next_i.into()]);
                        builder.seal_block(info.header);
                        builder.switch_to_block(info.exit);
                        builder.seal_block(info.exit);
                    }

                    // 3. Extract schema and key column names for SQLite grouping
                    let schema = match primary_schema.clone().or_else(|| {
                        primary_source.as_ref()
                            .and_then(|name| self.source_schemas.get(name).cloned())
                    }) {
                        Some(s) => s,
                        None => {
                            // No resolvable schema for the grouped relation.
                            // Report a clean compile-time diagnostic and bail
                            // (like the other GroupBy errors below) instead of
                            // panicking and aborting the whole compiler.
                            let hint = if let Some(name) = primary_source.as_ref() {
                                format!(
                                    "groupBy: no schema found for relation '{}' \
                                     (add a type annotation to the declaration)",
                                    name
                                )
                            } else {
                                "groupBy requires a preceding bind from a relation \
                                 with a known schema (*source, &derived with type annotation, \
                                 or nested field of such a relation)"
                                    .to_string()
                            };
                            self.diagnostics.push(
                                knot::diagnostic::Diagnostic::error(hint)
                                    .label(key.span, "groupBy over a relation with no known schema"),
                            );
                            // Pre-group loops are already closed here; still tear
                            // down the do-block scope so the leaked arena frame /
                            // shadowed bindings don't corrupt a later scope.
                            let empty = self.call_rt(builder, "knot_relation_empty", &[]);
                            let mut promoted =
                                self.call_rt(builder, "knot_arena_pop_frame_promote", &[empty]);
                            self.atomic_arena_frames -= 1;
                            env.bindings = prev_env_bindings;
                            self.let_bindings = prev_let_bindings;
                            self.source_var_binds = prev_source_var_binds;
                            self.pending_index_frees.truncate(index_free_mark);
                            self.replay_source_bind_invalidations_since(invalidation_mark);
                            // Seal pre-bind `where` skip blocks (defensive —
                            // normally empty here since a primary bind opened
                            // loops, but guard against a misordered do-block).
                            // Each skip block passes its own empty relation as
                            // the merge block param so `promoted` dominates.
                            if !pre_bind_where_skips.is_empty() {
                                let do_exit = builder.create_block();
                                let result_param = builder.append_block_param(do_exit, self.ptr_type);
                                builder.ins().jump(do_exit, &[promoted.into()]);
                                for skip in &pre_bind_where_skips {
                                    builder.switch_to_block(*skip);
                                    builder.seal_block(*skip);
                                    let skip_empty = self.call_rt(builder, "knot_relation_empty", &[]);
                                    let skip_promoted = self.call_rt(
                                        builder,
                                        "knot_arena_pop_frame_promote",
                                        &[skip_empty],
                                    );
                                    builder.ins().jump(do_exit, &[skip_promoted.into()]);
                                }
                                builder.switch_to_block(do_exit);
                                builder.seal_block(do_exit);
                                promoted = result_param;
                            }
                            return promoted;
                        }
                    };

                    // Extract key column names from the key record expression.
                    // Only plain field accesses (`t.owner`) are supported as
                    // keys: anything else has no corresponding schema column,
                    // so generating code for it would abort at runtime with
                    // "key column not found in schema". Reject it here with a
                    // proper compile-time diagnostic instead.
                    let key_cols: Vec<String> = match &key.node {
                        ast::ExprKind::Record(fields) => fields
                            .iter()
                            .map(|f| match &f.value.node {
                                ast::ExprKind::FieldAccess { expr: key_base, field } => {
                                    // The key column is read from the grouped
                                    // rows (the primary bind), so the field
                                    // access base must BE the primary bind —
                                    // `{g: other.col}` would silently
                                    // attribute the column to the wrong
                                    // relation.
                                    let base_is_primary = matches!(
                                        &key_base.node,
                                        ast::ExprKind::Var(v)
                                            if Some(v) == primary_var.as_ref()
                                    );
                                    if !base_is_primary {
                                        self.diagnostics.push(
                                            knot::diagnostic::Diagnostic::error(format!(
                                                "groupBy key '{}' must access a field of \
                                                 the grouped binding{}",
                                                f.name,
                                                primary_var
                                                    .as_ref()
                                                    .map(|p| format!(" '{}'", p))
                                                    .unwrap_or_default(),
                                            ))
                                            .label(
                                                f.value.span,
                                                "field access on a different variable",
                                            ),
                                        );
                                    }
                                    // Verify the column exists in the schema
                                    // (skip ADT schemas, whose descriptor
                                    // format the lookup doesn't parse).
                                    if base_is_primary
                                        && !schema.starts_with('#')
                                        && lookup_col_type_from_schema(&schema, field).is_none()
                                    {
                                        self.diagnostics.push(
                                            knot::diagnostic::Diagnostic::error(format!(
                                                "groupBy key '{}' refers to field '{}', \
                                                 which is not a column of the grouped relation",
                                                f.name, field
                                            ))
                                            .label(f.value.span, "not a relation column"),
                                        );
                                    }
                                    field.clone()
                                }
                                _ => {
                                    self.diagnostics.push(
                                        knot::diagnostic::Diagnostic::error(format!(
                                            "groupBy key '{}' is a computed expression; \
                                             groupBy keys must be plain field accesses \
                                             like t.owner",
                                            f.name
                                        ))
                                        .label(f.value.span, "computed key expression")
                                        .note(
                                            "bind the computed value with a yield into an \
                                             intermediate relation first, then group on \
                                             that field",
                                        ),
                                    );
                                    f.name.clone()
                                }
                            })
                            .collect(),
                        _ => {
                            self.diagnostics.push(
                                knot::diagnostic::Diagnostic::error(
                                    "groupBy key must be a record expression, \
                                     e.g. groupBy {owner: t.owner}",
                                )
                                .label(key.span, "expected a record of field accesses"),
                            );
                            Vec::new()
                        }
                    };
                    let key_cols_str = key_cols.join(",");

                    let (schema_ptr, schema_len) =
                        self.string_ptr(builder, &schema);
                    let (key_cols_ptr, key_cols_len) =
                        self.string_ptr(builder, &key_cols_str);

                    // 4. Call runtime groupBy (SQLite-based)
                    let groups = self.call_rt(
                        builder,
                        "knot_relation_group_by",
                        &[
                            db,
                            temp,
                            schema_ptr,
                            schema_len,
                            key_cols_ptr,
                            key_cols_len,
                        ],
                    );

                    // 5. Start a new loop over the groups
                    let groups_len = self.call_rt(
                        builder,
                        "knot_relation_len",
                        &[groups],
                    );

                    let g_header = builder.create_block();
                    let g_body = builder.create_block();
                    let g_continue = builder.create_block();
                    let g_exit = builder.create_block();

                    let zero = builder.ins().iconst(self.ptr_type, 0);
                    builder.ins().jump(g_header, &[zero.into()]);

                    builder.switch_to_block(g_header);
                    let g_i = builder.append_block_param(g_header, self.ptr_type);
                    let g_cond = builder
                        .ins()
                        .icmp(IntCC::UnsignedLessThan, g_i, groups_len);
                    builder
                        .ins()
                        .brif(g_cond, g_body, &[], g_exit, &[]);

                    builder.switch_to_block(g_body);
                    builder.seal_block(g_body);

                    // Arena GC: mark at groupBy loop body entry
                    let g_arena_mark = self.call_rt(builder, "knot_arena_mark", &[]);

                    // 6. Rebind the primary variable to the current group
                    let group = self.call_rt(
                        builder,
                        "knot_relation_get",
                        &[groups, g_i],
                    );
                    if let Some(var_name) = primary_var.as_ref() {
                        env.set(var_name, group);
                    }

                    loop_stack.push(LoopInfo {
                        header: g_header,
                        continue_blk: g_continue,
                        exit: g_exit,
                        index_var: g_i,
                        where_skips: Vec::new(),
                        arena_mark: g_arena_mark,
                    });
                }

                ast::StmtKind::Expr(expr) => {
                    let is_last = stmt_idx == stmts.len() - 1;
                    if let Some(inner) = expr.node.as_yield_arg() {
                        let val =
                            self.compile_expr(builder, inner, env, db);
                        // Arena GC: promote yielded value so it survives
                        // per-iteration reset in the continue block.
                        // Escape-analysis hint: if the yielded value is
                        // syntactically a singleton (small int / bool /
                        // `Unit` / Float 0.0 or 1.0), it's already owned
                        // by the thread-local `SINGLETONS` table and
                        // needs no promotion — skip the call entirely.
                        let val = if !loop_stack.is_empty() && !expr_is_promote_safe(inner) {
                            self.call_rt(builder, "knot_arena_promote", &[val])
                        } else {
                            val
                        };
                        self.call_rt_void(
                            builder,
                            "knot_relation_push",
                            &[result, val],
                        );
                    } else if matches!(&expr.node, ast::ExprKind::Set { .. } | ast::ExprKind::ReplaceSet { .. }) {
                        // Compile set inside do block
                        let _ = self.compile_expr(builder, expr, env, db);
                        // Variables bound from the written source are stale now.
                        self.invalidate_after_possible_writes(expr);
                    } else {
                        let val =
                            self.compile_expr(builder, expr, env, db);
                        self.invalidate_after_possible_writes(expr);
                        if is_last && loop_stack.is_empty() {
                            // Last expression in a non-looping do block
                            // — push as result
                            self.call_rt_void(
                                builder,
                                "knot_relation_push",
                                &[result, val],
                            );
                        }
                    }
                }
            }
        }

        // Close loops from innermost to outermost
        while let Some(info) = loop_stack.pop() {
            // From current block, jump to continue_blk
            builder.ins().jump(info.continue_blk, &[]);

            // Handle where skips
            for skip in &info.where_skips {
                builder.switch_to_block(*skip);
                builder.seal_block(*skip);
                builder.ins().jump(info.continue_blk, &[]);
            }

            // Continue block: reset arena and increment
            builder.switch_to_block(info.continue_blk);
            builder.seal_block(info.continue_blk);
            // Arena GC: free per-iteration temporaries (promoted values survive in pinned set)
            self.call_rt_void(builder, "knot_arena_reset_to", &[info.arena_mark]);
            let one = builder.ins().iconst(self.ptr_type, 1);
            let next_i = builder.ins().iadd(info.index_var, one);
            builder.ins().jump(info.header, &[next_i.into()]);

            // Seal header (all predecessors now known)
            builder.seal_block(info.header);

            // Switch to exit block for the next outer loop
            builder.switch_to_block(info.exit);
            builder.seal_block(info.exit);
        }

        // Merge any pre-bind `where` skips (failed guards before the first
        // bind) into the do-block's exit. Both the normal flow and each skip
        // converge here returning the same (empty-on-skip) `result`.
        if !pre_bind_where_skips.is_empty() {
            let do_exit = builder.create_block();
            builder.ins().jump(do_exit, &[]);
            for skip in &pre_bind_where_skips {
                builder.switch_to_block(*skip);
                builder.seal_block(*skip);
                builder.ins().jump(do_exit, &[]);
            }
            builder.switch_to_block(do_exit);
            builder.seal_block(do_exit);
        }

        // Free all pre-built hash join indices
        for idx_val in prebuilt_indices.values() {
            self.call_rt_void(builder, "knot_relation_index_free", &[*idx_val]);
        }
        // Drop this do-block's indices from the live-for-retry stack.
        self.pending_index_frees.truncate(index_free_mark);

        // Pop the do-block's frame, deep-cloning `result` into the parent.
        // This frees every per-iteration pinned yield that would otherwise
        // live until the caller returned.
        let promoted = self.call_rt(builder, "knot_arena_pop_frame_promote", &[result]);
        self.atomic_arena_frames -= 1;
        env.bindings = prev_env_bindings;
        self.let_bindings = prev_let_bindings;
        self.source_var_binds = prev_source_var_binds;
        // Writes/rebinds inside this scope still invalidate outer entries.
        self.replay_source_bind_invalidations_since(invalidation_mark);
        promoted
    }

    // ── Lambda compilation ────────────────────────────────────────

    fn compile_lambda(
        &mut self,
        builder: &mut FunctionBuilder,
        params: &[ast::Pat],
        body: &ast::Expr,
        env: &mut Env,
        _db: Value,
    ) -> Value {
        self.compile_lambda_inner(builder, params, body, env, _db, None)
    }

    fn compile_lambda_inner(
        &mut self,
        builder: &mut FunctionBuilder,
        params: &[ast::Pat],
        body: &ast::Expr,
        env: &mut Env,
        _db: Value,
        source_override: Option<String>,
    ) -> Value {
        // Curry multi-param lambdas: \a b c -> body  =>  \a -> \b -> \c -> body
        if params.len() > 1 {
            let source_text = source_override.unwrap_or_else(|| {
                let ps: Vec<String> = params.iter().map(pretty_pat).collect();
                format!("\\{} -> {}", ps.join(" "), pretty_expr(body))
            });
            let inner_lambda = ast::Spanned::new(
                ast::ExprKind::Lambda {
                    params: params[1..].to_vec(),
                    body: Box::new(body.clone()),
                },
                body.span,
            );
            return self.compile_lambda_inner(
                builder, &params[0..1], &inner_lambda, env, _db,
                Some(source_text),
            );
        }

        let lambda_name = format!("knot_lambda_{}", self.lambda_counter);
        self.lambda_counter += 1;

        // Determine free variables — extract ALL names bound by patterns,
        // not just top-level Var patterns (handles destructuring like \{x, y} -> ...)
        let param_names: Vec<String> = params
            .iter()
            .flat_map(pat_bound_names)
            .collect();
        // Capture decisions respect lexical scope: a name bound in the
        // enclosing local scope (env) shadows any same-named top-level
        // function/constant or builtin and MUST be captured; only
        // unshadowed globals/builtins are resolved inside the lambda body.
        let free_vars: Vec<String> = find_free_vars(body, &param_names)
            .into_iter()
            .filter(|v| {
                // `__derived_self_*` is a synthetic recursive-accumulator key:
                // capture it only when the enclosing body actually provides it,
                // never via the "unshadowed local" fallback (it has no global).
                if v.starts_with("__derived_self_") {
                    return env.bindings.contains_key(v);
                }
                env.bindings.contains_key(v)
                    || (!self.user_fns.contains_key(v) && !is_builtin_name(v))
            })
            .collect();

        // Declare the lambda function: (db, env, arg) -> result
        let mut sig = self.module.make_signature();
        sig.params.push(AbiParam::new(self.ptr_type)); // db
        sig.params.push(AbiParam::new(self.ptr_type)); // env
        sig.params.push(AbiParam::new(self.ptr_type)); // arg
        sig.returns.push(AbiParam::new(self.ptr_type));
        let func_id = self
            .module
            .declare_function(&lambda_name, Linkage::Local, &sig)
            .unwrap();

        // Queue for later compilation
        self.pending_lambdas.push(PendingLambda {
            func_id,
            params: param_names.clone(),
            param_pat: if params.len() == 1 { Some(params[0].clone()) } else { None },
            body: body.clone(),
            free_vars: free_vars.clone(),
        });

        // Build the closure: capture free variables into a record
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);

        let env_val = if free_vars.is_empty() {
            builder.ins().iconst(self.ptr_type, 0) // null env
        } else if free_vars.len() == 1 {
            // Single capture: pass value directly as env (no record allocation)
            match env.get(&free_vars[0]) {
                Some(v) => v,
                None => {
                    let msg =
                        format!("codegen: undefined captured variable '{}'", free_vars[0]);
                    self.push_codegen_error(builder, ast::Span::new(0, 0), msg)
                }
            }
        } else {
            let n = free_vars.len();
            // Sort free vars so index-based extraction matches
            let mut sorted_vars: Vec<&str> = free_vars.iter().map(|s| s.as_str()).collect();
            sorted_vars.sort();

            let ptr_bytes = self.ptr_type.bytes() as i32;
            let slot_size = (3 * n as u32) * ptr_bytes as u32;
            let slot = builder.create_sized_stack_slot(
                StackSlotData::new(StackSlotKind::ExplicitSlot, slot_size, 3),
            );
            for (i, var_name) in sorted_vars.iter().enumerate() {
                let val = match env.get(var_name) {
                    Some(v) => v,
                    None => {
                        let msg =
                            format!("codegen: undefined captured variable '{}'", var_name);
                        self.push_codegen_error(builder, ast::Span::new(0, 0), msg)
                    }
                };
                let (key_ptr, key_len) = self.string_ptr(builder, var_name);
                let base = (i as i32) * (3 * ptr_bytes);
                builder.ins().stack_store(key_ptr, slot, base);
                builder.ins().stack_store(key_len, slot, base + ptr_bytes);
                builder.ins().stack_store(val, slot, base + 2 * ptr_bytes);
            }
            let data_ptr = builder.ins().stack_addr(self.ptr_type, slot, 0);
            let count = builder.ins().iconst(self.ptr_type, n as i64);
            self.call_rt(builder, "knot_record_from_pairs", &[data_ptr, count])
        };

        // Generate source representation for this lambda
        let source_text = source_override.unwrap_or_else(|| {
            let ps: Vec<String> = params.iter().map(pretty_pat).collect();
            format!("\\{} -> {}", ps.join(" "), pretty_expr(body))
        });
        let (src_ptr, src_len) = self.string_ptr(builder, &source_text);
        self.call_rt(builder, "knot_value_function", &[fn_addr, env_val, src_ptr, src_len])
    }

    // ── Literal compilation ───────────────────────────────────────

    fn compile_lit(
        &mut self,
        builder: &mut FunctionBuilder,
        lit: &ast::Literal,
    ) -> Value {
        match lit {
            ast::Literal::Int(n) => {
                if let Ok(small) = n.parse::<i64>() {
                    let n_val = builder.ins().iconst(types::I64, small);
                    self.call_rt(builder, "knot_value_int", &[n_val])
                } else {
                    let (ptr, len) = self.string_ptr(builder, n);
                    self.call_rt(builder, "knot_value_int_from_str", &[ptr, len])
                }
            }
            ast::Literal::Float(n) => {
                let n_val = builder.ins().f64const(*n);
                self.call_rt(builder, "knot_value_float", &[n_val])
            }
            ast::Literal::Text(s) => {
                let (ptr, len) = self.string_ptr(builder, s);
                let slot = self.text_literal_slot(builder, s);
                self.call_rt(builder, "knot_value_text_intern", &[ptr, len, slot])
            }
            ast::Literal::Bytes(b) => {
                let (ptr, len) = self.bytes_ptr(builder, b);
                self.call_rt(builder, "knot_value_bytes", &[ptr, len])
            }
            ast::Literal::Bool(b) => {
                let val = builder.ins().iconst(types::I32, *b as i64);
                self.call_rt(builder, "knot_value_bool", &[val])
            }
        }
    }

    /// Emit a literal value for a compile-time constant override.
    /// Parses `val_str` according to `type_str` and emits the appropriate
    /// Cranelift IR, wrapping in `Just` for Maybe types. A Maybe-typed
    /// override may also be the literal string `Nothing` (emitted as the null
    /// pointer the runtime uses for the none variant). Invalid values push a
    /// clean compile error diagnostic instead of panicking, so a mistyped
    /// `--flag=...` reports like any other compile error rather than dumping a
    /// stack trace.
    fn emit_override_literal(
        &mut self,
        builder: &mut FunctionBuilder,
        val_str: &str,
        type_str: &str,
    ) -> Value {
        let is_maybe = type_str.starts_with("Maybe ");
        // `Nothing` for a Maybe-typed constant compiles to the same
        // `Constructor("Nothing", Unit)` value that a bare `Nothing`
        // reference produces in user code (nullable pointer encoding is
        // disabled), so pattern matching and field access behave identically
        // whether the constant used its default body or this override.
        if is_maybe && val_str == "Nothing" {
            let (tag_ptr, tag_len) = self.string_ptr(builder, "Nothing");
            let unit = self.call_rt(builder, "knot_value_unit", &[]);
            return self.call_rt(builder, "knot_value_constructor", &[tag_ptr, tag_len, unit]);
        }
        // Helper to report a bad override value as a compile error and emit a
        // harmless unit value so codegen can keep going (the error aborts the
        // build before linking, via `compile()`'s diagnostics check).
        let mut bad = |expected: &str| -> Value {
            self.diagnostics.push(knot::diagnostic::Diagnostic::error(
                format!(
                    "override value '{}' is not a valid {} for this constant",
                    val_str, expected
                ),
            ));
            self.call_rt(builder, "knot_value_unit", &[])
        };
        let inner = match type_str {
            "Int" | "Maybe Int" => match val_str.parse::<i64>() {
                Ok(n) => {
                    let n_val = builder.ins().iconst(types::I64, n);
                    self.call_rt(builder, "knot_value_int", &[n_val])
                }
                Err(_) => bad("Int"),
            },
            "Float" | "Maybe Float" => match val_str.parse::<f64>() {
                Ok(n) => {
                    let n_val = builder.ins().f64const(n);
                    self.call_rt(builder, "knot_value_float", &[n_val])
                }
                Err(_) => bad("Float"),
            },
            "Text" | "Maybe Text" => {
                let (ptr, len) = self.string_ptr(builder, val_str);
                let slot = self.text_literal_slot(builder, val_str);
                self.call_rt(builder, "knot_value_text_intern", &[ptr, len, slot])
            }
            "Bool" | "Maybe Bool" => {
                // Reject unparseable Bool overrides instead of silently
                // coercing them to `false`, so a mistyped `--flag=ture` is a
                // clear error, not a wrong value.
                match val_str {
                    "true" | "True" | "1" => {
                        let val = builder.ins().iconst(types::I32, 1);
                        self.call_rt(builder, "knot_value_bool", &[val])
                    }
                    "false" | "False" | "0" => {
                        let val = builder.ins().iconst(types::I32, 0);
                        self.call_rt(builder, "knot_value_bool", &[val])
                    }
                    _ => bad("Bool (expected true/false)"),
                }
            }
            _ => {
                self.diagnostics.push(knot::diagnostic::Diagnostic::error(
                    format!(
                        "constant of type '{}' cannot be supplied as a compile-time override",
                        type_str
                    ),
                ));
                self.call_rt(builder, "knot_value_unit", &[])
            }
        };
        if is_maybe {
            // The runtime's `Maybe` convention is `Just {value: payload}` (see
            // `make_just`), so the payload must be wrapped in a single-field
            // record — not passed as the bare scalar — or `case x of Just {value: n}`
            // and `.value` access read a field from a non-record at runtime.
            let cap = builder.ins().iconst(self.ptr_type, 1);
            let rec = self.call_rt(builder, "knot_record_empty", &[cap]);
            let (key_ptr, key_len) = self.string_ptr(builder, "value");
            self.call_rt_void(builder, "knot_record_set_field", &[rec, key_ptr, key_len, inner]);
            let (tag_ptr, tag_len) = self.string_ptr(builder, "Just");
            self.call_rt(builder, "knot_value_constructor", &[tag_ptr, tag_len, rec])
        } else {
            inner
        }
    }

    // ── Diagnostics ───────────────────────────────────────────────

    /// Record a codegen error and return a null placeholder value so IR
    /// construction can continue without panicking. Compilation aborts with the
    /// accumulated diagnostics before any object file is linked (`compile_inner`
    /// returns `Err` when `self.diagnostics` is non-empty), so the null is never
    /// executed — this only turns internal invariant violations that are still
    /// reachable on malformed input into user-facing diagnostics instead of a
    /// process abort.
    fn push_codegen_error(
        &mut self,
        builder: &mut FunctionBuilder,
        span: ast::Span,
        message: impl Into<String>,
    ) -> Value {
        self.diagnostics
            .push(knot::diagnostic::Diagnostic::error(message).label(span, "here"));
        builder.ins().iconst(self.ptr_type, 0)
    }

    // ── Runtime call helpers ──────────────────────────────────────

    /// Call a runtime function that returns a pointer-typed value.
    fn call_rt(
        &mut self,
        builder: &mut FunctionBuilder,
        name: &str,
        args: &[Value],
    ) -> Value {
        let func_id = self.runtime_fns[name];
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        let call = builder.ins().call(func_ref, args);
        builder.inst_results(call)[0]
    }

    /// Call a runtime function that returns a specific type.
    fn call_rt_typed(
        &mut self,
        builder: &mut FunctionBuilder,
        name: &str,
        args: &[Value],
        _ret_type: types::Type,
    ) -> Value {
        let func_id = self.runtime_fns[name];
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        let call = builder.ins().call(func_ref, args);
        builder.inst_results(call)[0]
    }

    /// Call a runtime function that returns void.
    fn call_rt_void(
        &mut self,
        builder: &mut FunctionBuilder,
        name: &str,
        args: &[Value],
    ) {
        let func_id = self.runtime_fns[name];
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        builder.ins().call(func_ref, args);
    }

    /// Import a runtime function reference into the current function.
    fn import_rt(
        &mut self,
        builder: &mut FunctionBuilder,
        name: &str,
    ) -> cranelift_codegen::ir::FuncRef {
        let func_id = self.runtime_fns[name];
        self.module.declare_func_in_func(func_id, builder.func)
    }

    // ── String constant helpers ───────────────────────────────────

    /// Ensure a string constant exists in the module data section.
    /// Returns the DataId.
    fn ensure_string(&mut self, s: &str) -> DataId {
        if let Some(id) = self.strings.get(s) {
            return *id;
        }
        let name = format!(".str.{}", self.string_counter);
        self.string_counter += 1;
        let data_id = self
            .module
            .declare_data(&name, Linkage::Local, false, false)
            .unwrap();
        let mut desc = DataDescription::new();
        desc.define(s.as_bytes().to_vec().into_boxed_slice());
        self.module.define_data(data_id, &desc).unwrap();
        self.strings.insert(s.to_string(), data_id);
        data_id
    }

    /// Get the pointer and length of a string constant as Cranelift Values.
    fn string_ptr(
        &mut self,
        builder: &mut FunctionBuilder,
        s: &str,
    ) -> (Value, Value) {
        let data_id = self.ensure_string(s);
        let gv = self
            .module
            .declare_data_in_func(data_id, builder.func);
        let ptr = builder.ins().global_value(self.ptr_type, gv);
        let len = builder.ins().iconst(self.ptr_type, s.len() as i64);
        (ptr, len)
    }

    /// Get the pointer and length of a byte string constant as Cranelift Values.
    fn bytes_ptr(
        &mut self,
        builder: &mut FunctionBuilder,
        b: &[u8],
    ) -> (Value, Value) {
        let data_id = self.ensure_bytes(b);
        let gv = self
            .module
            .declare_data_in_func(data_id, builder.func);
        let ptr = builder.ins().global_value(self.ptr_type, gv);
        let len = builder.ins().iconst(self.ptr_type, b.len() as i64);
        (ptr, len)
    }

    /// Get a Cranelift `Value` holding the address of the 8-byte
    /// zero-initialized slot dedicated to caching the interned `Value*`
    /// for `s`.  On first use the slot is null and the runtime's
    /// `knot_value_text_intern` fills it; subsequent uses load directly.
    fn text_literal_slot(
        &mut self,
        builder: &mut FunctionBuilder,
        s: &str,
    ) -> Value {
        let data_id = if let Some(id) = self.text_literal_slots.get(s) {
            *id
        } else {
            let name = format!(".text.slot.{}", self.string_counter);
            self.string_counter += 1;
            // `writable = true` so the first call can write into the
            // slot without the loader marking the page read-only.
            let id = self
                .module
                .declare_data(&name, Linkage::Local, true, false)
                .unwrap();
            let mut desc = DataDescription::new();
            // Zero-initialized 8-byte slot (holds a `*mut Value`). Aligned to
            // pointer size so `knot_value_text_intern`'s atomic load (`ldar`
            // on aarch64) doesn't SIGBUS — `ldar`/`stlr` strictly require
            // natural alignment.
            desc.set_align(std::mem::align_of::<*mut u8>() as u64);
            desc.define_zeroinit(std::mem::size_of::<*mut u8>());
            self.module.define_data(id, &desc).unwrap();
            self.text_literal_slots.insert(s.to_string(), id);
            id
        };
        let gv = self
            .module
            .declare_data_in_func(data_id, builder.func);
        builder.ins().global_value(self.ptr_type, gv)
    }

    fn ensure_bytes(&mut self, b: &[u8]) -> DataId {
        let name = format!(".bytes.{}", self.string_counter);
        self.string_counter += 1;
        let data_id = self
            .module
            .declare_data(&name, Linkage::Local, false, false)
            .unwrap();
        let mut desc = DataDescription::new();
        desc.define(b.to_vec().into_boxed_slice());
        self.module.define_data(data_id, &desc).unwrap();
        data_id
    }

    // ── Set-expression analysis ──────────────────────────────────

    /// Check whether an expression references `*<source_name>` anywhere.
    fn references_source(expr: &ast::Expr, source_name: &str) -> bool {
        match &expr.node {
            ast::ExprKind::SourceRef(name) => name == source_name,
            ast::ExprKind::Lit(_)
            | ast::ExprKind::Var(_)
            | ast::ExprKind::Constructor(_)
            | ast::ExprKind::DerivedRef(_) => false,
            ast::ExprKind::Record(fields) => {
                fields.iter().any(|f| Self::references_source(&f.value, source_name))
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                Self::references_source(base, source_name)
                    || fields.iter().any(|f| Self::references_source(&f.value, source_name))
            }
            ast::ExprKind::FieldAccess { expr, .. } => Self::references_source(expr, source_name),
            ast::ExprKind::List(elems) => {
                elems.iter().any(|e| Self::references_source(e, source_name))
            }
            ast::ExprKind::Lambda { body, .. } => Self::references_source(body, source_name),
            ast::ExprKind::App { func, arg } => {
                Self::references_source(func, source_name)
                    || Self::references_source(arg, source_name)
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                Self::references_source(lhs, source_name)
                    || Self::references_source(rhs, source_name)
            }
            ast::ExprKind::UnaryOp { operand, .. } => {
                Self::references_source(operand, source_name)
            }
            ast::ExprKind::If { cond, then_branch, else_branch } => {
                Self::references_source(cond, source_name)
                    || Self::references_source(then_branch, source_name)
                    || Self::references_source(else_branch, source_name)
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                Self::references_source(scrutinee, source_name)
                    || arms.iter().any(|a| Self::references_source(&a.body, source_name))
            }
            ast::ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
                ast::StmtKind::Bind { expr, .. } => Self::references_source(expr, source_name),
                ast::StmtKind::Let { expr, .. } => Self::references_source(expr, source_name),
                ast::StmtKind::Where { cond } => Self::references_source(cond, source_name),
                ast::StmtKind::GroupBy { key } => Self::references_source(key, source_name),
                ast::StmtKind::Expr(e) => Self::references_source(e, source_name),
            }),
            ast::ExprKind::Set { target, value }
            | ast::ExprKind::ReplaceSet { target, value } => {
                Self::references_source(target, source_name)
                    || Self::references_source(value, source_name)
            }
            ast::ExprKind::Atomic(inner) => Self::references_source(inner, source_name),
            ast::ExprKind::TimeUnitLit { value, .. } => Self::references_source(value, source_name),
            ast::ExprKind::Annot { expr, .. } => Self::references_source(expr, source_name),
            ast::ExprKind::Refine(inner) => Self::references_source(inner, source_name),
            ast::ExprKind::Serve { handlers, .. } => handlers
                .iter()
                .any(|h| Self::references_source(&h.body, source_name)),
        }
    }

    /// Detect `set *rel = union *rel <expr>` (or `union <expr> *rel`) and
    /// return the "new rows" expression so we can emit an append instead of
    /// a full table replacement.
    fn match_union_append<'a>(
        &self,
        source_name: &str,
        value: &'a ast::Expr,
    ) -> Option<&'a ast::Expr> {
        // Match: App(App(Var("union"), arg1), arg2)
        if let ast::ExprKind::App { func, arg: arg2 } = &value.node
            && let ast::ExprKind::App {
                func: inner_func,
                arg: arg1,
            } = &func.node
                && let ast::ExprKind::Var(fn_name) = &inner_func.node
                    && fn_name == "union" {
                        // union *rel <new_rows>
                        if Self::expr_is_source(&arg1.node, source_name, &self.source_var_binds) {
                            return Some(arg2);
                        }
                        // union <new_rows> *rel
                        if Self::expr_is_source(&arg2.node, source_name, &self.source_var_binds) {
                            return Some(arg1);
                        }
                    }
        None
    }

    // ── Full SQL query compilation ─────────────────────────────────

    /// Try to compile a do-block as a single SQL query.
    /// Returns Some(result) if successful, None to fall back to loop codegen.
    fn try_compile_full_sql(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Option<Value> {
        let plan = self.analyze_sql_plan(stmts, env)?;

        let sql = plan.build_sql();
        let result_schema = plan.build_result_schema();

        self.emit_stm_track_reads_for_plan(builder, &plan);
        let params_rel = self.compile_sql_params(builder, &plan.params, env, db);
        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
        Some(self.call_rt(
            builder,
            "knot_source_query",
            &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
        ))
    }

    /// Try to compile application-form `filter/sum/avg lambda *source` to SQL.
    // Mirrors the shape of the SQL pushdown it emits; splitting the params
    // would not improve clarity.
    #[allow(clippy::too_many_arguments)]
    fn try_compile_app_sql(
        &mut self,
        builder: &mut FunctionBuilder,
        fn_name: &str,
        lambda_arg: &ast::Expr,
        source_name: &str,
        schema: &str,
        env: &mut Env,
        db: Value,
    ) -> Option<Value> {
        // User trait impls on primitives change operator semantics that
        // SQL can't replicate — fall back to in-memory evaluation.
        if self.sql_pushdown_disabled_by_user_impls() {
            return None;
        }
        let (bind_var, body) = extract_single_param_lambda(lambda_arg, &self.fun_bodies, &self.let_bindings)?;
        let body: &ast::Expr = &body;
        let table = quote_sql_ident(&format!("_knot_{}", source_name));

        match fn_name {
            "filter" => {
                // Use unqualified column names for knot_source_read_where
                let frag = self.try_compile_sql_expr(&bind_var, body, schema)?;
                let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                let (name_ptr, name_len) = self.string_ptr(builder, source_name);
                let (schema_ptr, schema_len) = self.string_ptr(builder, schema);
                let (where_ptr, where_len) = self.string_ptr(builder, &frag.sql);
                Some(self.call_rt(
                    builder,
                    "knot_source_read_where",
                    &[db, name_ptr, name_len, schema_ptr, schema_len, where_ptr, where_len, params_rel],
                ))
            }
            "sum" | "avg" | "minOn" | "maxOn" => {
                // MIN/MAX over non-numeric columns must stay in memory
                // (see minmax_pushdown_type_ok).
                if matches!(fn_name, "minOn" | "maxOn")
                    && !minmax_pushdown_type_ok(&bind_var, body, schema)
                {
                    return None;
                }
                // Use unqualified column names for direct SQL aggregate
                let col_sql = extract_sql_field_access(&bind_var, body, "", schema)?;
                let (func, rt_fn) = aggregate_sql_func_runtime(fn_name)?;
                let arg_sql = if matches!(fn_name, "minOn" | "maxOn") {
                    col_sql_for_minmax(&col_sql, &bind_var, body, schema)
                } else {
                    col_sql
                };
                let sql = format!("SELECT {}({}) FROM {}", func, arg_sql, table);
                self.emit_stm_track_read(builder, source_name);
                let params_rel = self.compile_sql_params(builder, &[], env, db);
                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                if rt_fn == "knot_source_query_value" {
                    let is_text = builder.ins().iconst(
                        types::I64,
                        minmax_result_is_text(&bind_var, body, schema) as i64,
                    );
                    Some(self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_text]))
                } else if rt_fn == "knot_source_query_sum" {
                    let is_float = builder.ins().iconst(
                        types::I64,
                        sum_result_is_float(&bind_var, body, schema) as i64,
                    );
                    Some(self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel, is_float]))
                } else {
                    Some(self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel]))
                }
            }
            "countWhere" => {
                let frag = self.try_compile_sql_expr(&bind_var, body, schema)?;
                let sql = format!("SELECT COUNT(*) FROM {} WHERE {}", table, frag.sql);
                self.emit_stm_track_read(builder, source_name);
                let params_rel = self.compile_sql_params(builder, &frag.params, env, db);
                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                Some(self.call_rt(
                    builder,
                    "knot_source_query_count",
                    &[db, sql_ptr, sql_len, params_rel],
                ))
            }
            "sortBy" => {
                // sortBy (\m -> m.field) *source → SELECT * FROM source ORDER BY field
                // ORDER BY CASE loses KNOT_INT collation for Int projections and
                // SQL float ordering diverges from total_cmp — keep both in
                // memory (see sortby_projection_pushable).
                if !sortby_projection_pushable(&bind_var, body, schema) {
                    return None;
                }
                let col_sql = extract_sql_field_access(&bind_var, body, "", schema)?;
                let cols = parse_schema_columns(schema).iter()
                    .map(|(name, _)| quote_sql_ident(name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!("SELECT {} FROM {} ORDER BY {}", cols, table, col_sql);
                self.emit_stm_track_read(builder, source_name);
                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                let (schema_ptr, schema_len) = self.string_ptr(builder, schema);
                let params_rel = self.compile_sql_params(builder, &[], env, db);
                Some(self.call_rt(
                    builder,
                    "knot_source_query",
                    &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
                ))
            }
            _ => None,
        }
    }

    /// Try to compile a pipe chain like `*source |> filter f |> map g` to a single SQL query.
    fn try_compile_pipe_sql(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Option<Value> {
        // User trait impls on primitives change operator semantics that
        // SQL can't replicate — fall back to in-memory evaluation.
        if self.sql_pushdown_disabled_by_user_impls() {
            return None;
        }
        let (source, ops) = flatten_pipe_chain(expr, &self.fun_bodies, &self.let_bindings)?;
        if ops.is_empty() {
            return None;
        }

        // Source must be a SourceRef or a variable bound from a source read
        let source_name = match &source.node {
            ast::ExprKind::SourceRef(name) => name.clone(),
            ast::ExprKind::Var(name) => self.source_var_binds.get(name)?.clone(),
            _ => return None,
        };
        if self.views.contains_key(&source_name) {
            return None;
        }
        let schema = self.source_schemas.get(&source_name)?.clone();
        if schema.starts_with('#') || schema.contains('[') {
            return None;
        }

        // ── Operation-order check ─────────────────────────────────
        // The pipeline collapses into ONE SQL query with the fixed clause
        // order WHERE → SELECT → ORDER BY → LIMIT/OFFSET. That only matches
        // the in-memory semantics when the pipe ops appear in the canonical
        // order filter* → sortBy? → map? → drop? → take? → aggregate?.
        // Out-of-order pipelines (e.g. `take 5 |> drop 2`, `take 3 |>
        // filter f`) have different semantics and must fall back to the
        // general in-memory path.
        if !pipe_ops_order_pushable(&ops) {
            return None;
        }

        let alias = "t0".to_string();
        let mut bind_aliases: HashMap<String, String> = HashMap::new();
        let mut bind_schemas: HashMap<String, String> = HashMap::new();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<SqlParamSource> = Vec::new();
        let mut select_override: Option<Vec<SqlSelectColumn>> = None;
        let mut is_count = false;
        let mut limit: Option<SqlParamSource> = None;
        let mut offset: Option<SqlParamSource> = None;
        let mut order_by_cols: Vec<String> = Vec::new();
        let mut aggregate: Option<(&str, String, bool)> = None; // (func, column_sql, result_is_text)

        for op in ops {
            match &op {
                PipeOp::Filter { bind_var, body } => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    bind_schemas.insert(bind_var.clone(), schema.clone());
                    let frag = Self::try_compile_multi_table_sql_expr(
                        &bind_aliases, &bind_schemas, body, env, &HashMap::new(),
                    )?;
                    conditions.push(frag.sql);
                    params.extend(frag.params);
                }
                PipeOp::Map { bind_var, body } => {
                    if is_count || select_override.is_some() || aggregate.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let cols =
                        analyze_map_select(bind_var, body, &alias, &schema)?;
                    select_override = Some(cols);
                }
                PipeOp::Count => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    is_count = true;
                }
                PipeOp::Take { n } | PipeOp::TakeRelation { n } => {
                    if limit.is_some() || is_count || aggregate.is_some() {
                        return None;
                    }
                    limit = Some(expr_to_sql_param(n)?);
                }
                PipeOp::Drop { n } => {
                    if offset.is_some() || is_count || aggregate.is_some() {
                        return None;
                    }
                    offset = Some(expr_to_sql_param(n)?);
                }
                PipeOp::SortBy { bind_var, body } => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    // ORDER BY CASE loses KNOT_INT collation for Int projections
                    // and SQL float ordering diverges from total_cmp — keep
                    // both in memory (see sortby_projection_pushable).
                    if !sortby_projection_pushable(bind_var, body, &schema) {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    order_by_cols.push(col_sql);
                }
                PipeOp::Sum { bind_var, body } => {
                    if is_count || aggregate.is_some() || select_override.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    aggregate = Some(("SUM", col_sql, sum_result_is_float(bind_var, body, &schema)));
                }
                PipeOp::SumDirect => {
                    // Direct `rel |> sum`. For `rel |> map f |> sum` the prior
                    // Map already produced the summable column(s); aggregate
                    // that. Without a map, `sum` over a raw source relation of
                    // records isn't a single column — stay in memory (the
                    // in-memory path handles a relation of numerics, which a
                    // bare source never is).
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    let cols = select_override.as_ref()?;
                    if cols.len() != 1 {
                        return None;
                    }
                    let col = &cols[0];
                    let col_sql = col.sql_expr.clone().unwrap_or_else(|| {
                        format!("{}.{}", col.alias, quote_sql_ident(&col.source_col))
                    });
                    let is_float = col.type_str == "float";
                    aggregate = Some(("SUM", col_sql, is_float));
                }
                PipeOp::Avg { bind_var, body } => {
                    if is_count || aggregate.is_some() || select_override.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    aggregate = Some(("AVG", col_sql, false));
                }
                PipeOp::Min { bind_var, body } => {
                    if is_count || aggregate.is_some() || select_override.is_some() {
                        return None;
                    }
                    // MIN over non-numeric columns must stay in memory
                    // (see minmax_pushdown_type_ok).
                    if !minmax_pushdown_type_ok(bind_var, body, &schema) {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    let arg_sql = col_sql_for_minmax(&col_sql, bind_var, body, &schema);
                    let is_text = minmax_result_is_text(bind_var, body, &schema);
                    aggregate = Some(("MIN", arg_sql, is_text));
                }
                PipeOp::Max { bind_var, body } => {
                    if is_count || aggregate.is_some() || select_override.is_some() {
                        return None;
                    }
                    // MAX over non-numeric columns must stay in memory
                    // (see minmax_pushdown_type_ok).
                    if !minmax_pushdown_type_ok(bind_var, body, &schema) {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    let arg_sql = col_sql_for_minmax(&col_sql, bind_var, body, &schema);
                    let is_text = minmax_result_is_text(bind_var, body, &schema);
                    aggregate = Some(("MAX", arg_sql, is_text));
                }
                PipeOp::CountWhere { bind_var, body } => {
                    if is_count || aggregate.is_some() || select_override.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    bind_schemas.insert(bind_var.clone(), schema.clone());
                    let frag = Self::try_compile_multi_table_sql_expr(
                        &bind_aliases, &bind_schemas, body, env, &HashMap::new(),
                    )?;
                    conditions.push(frag.sql);
                    params.extend(frag.params);
                    is_count = true;
                }
            }
        }

        // The third tuple element is type-dependent on `func`:
        //   MIN/MAX → `is_text` (text vs numeric result column)
        //   SUM     → `is_float` (float vs int result column)
        // Each branch below gives it a meaningful local name.
        if let Some((func, col_sql, result_flag)) = aggregate {
            // An aggregate after take/drop would have to apply AFTER the
            // LIMIT, but aggregate SQL has no LIMIT — fall back. (The order
            // check already rejects this; keep the guard as belt-and-braces.)
            if limit.is_some() || offset.is_some() {
                return None;
            }
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let sql = if conditions.is_empty() {
                format!("SELECT {}({}) FROM {} AS {}", func, col_sql, table, alias)
            } else {
                format!(
                    "SELECT {}({}) FROM {} AS {} WHERE {}",
                    func, col_sql, table, alias, join_sql_conditions(&conditions)
                )
            };
            self.emit_stm_track_read(builder, &source_name);
            let params_rel = self.compile_sql_params(builder, &params, env, db);
            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
            let rt_fn = match func {
                "SUM" => "knot_source_query_sum",
                "AVG" => "knot_source_query_float",
                "MIN" | "MAX" => "knot_source_query_value",
                _ => "knot_source_query_count",
            };
            if rt_fn == "knot_source_query_value" {
                let is_text = builder.ins().iconst(types::I64, result_flag as i64);
                Some(self.call_rt(
                    builder,
                    rt_fn,
                    &[db, sql_ptr, sql_len, params_rel, is_text],
                ))
            } else if rt_fn == "knot_source_query_sum" {
                // For SUM `result_flag` carries `is_float` (see PipeOp::Sum).
                let is_float = builder.ins().iconst(types::I64, result_flag as i64);
                Some(self.call_rt(
                    builder,
                    rt_fn,
                    &[db, sql_ptr, sql_len, params_rel, is_float],
                ))
            } else {
                Some(self.call_rt(
                    builder,
                    rt_fn,
                    &[db, sql_ptr, sql_len, params_rel],
                ))
            }
        } else if is_count {
            // COUNT(*) ignores LIMIT/OFFSET — a count after take/drop must
            // fall back. (Also rejected by the order check.)
            if limit.is_some() || offset.is_some() {
                return None;
            }
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let sql = if conditions.is_empty() {
                format!("SELECT COUNT(*) FROM {}", table)
            } else {
                format!(
                    "SELECT COUNT(*) FROM {} AS {} WHERE {}",
                    table, alias, join_sql_conditions(&conditions)
                )
            };
            self.emit_stm_track_read(builder, &source_name);
            let params_rel = self.compile_sql_params(builder, &params, env, db);
            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
            Some(self.call_rt(
                builder,
                "knot_source_query_count",
                &[db, sql_ptr, sql_len, params_rel],
            ))
        } else {
            let select_columns = if let Some(cols) = select_override {
                cols
            } else {
                // SELECT * — all columns from schema
                parse_schema_columns(&schema)
                    .into_iter()
                    .map(|(col_name, type_str)| SqlSelectColumn {
                        result_field: col_name.clone(),
                        alias: alias.clone(),
                        source_col: col_name,
                        type_str,
                        sql_expr: None,
                    })
                    .collect()
            };

            let plan = SqlQueryPlan {
                tables: vec![SqlTable {
                    source_name,
                    alias,
                }],
                conditions,
                params,
                select_columns,
                order_by: order_by_cols,
                limit,
                offset,
            };

            let sql = plan.build_sql();
            let result_schema = plan.build_result_schema();
            self.emit_stm_track_reads_for_plan(builder, &plan);
            let mut all_params = plan.params;
            if let Some(lim) = &plan.limit {
                all_params.push(lim.clone());
            }
            if let Some(off) = &plan.offset {
                all_params.push(off.clone());
            }
            let params_rel = self.compile_sql_params(builder, &all_params, env, db);
            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
            let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
            Some(self.call_rt(
                builder,
                "knot_source_query",
                &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
            ))
        }
    }

    /// Try to compile a set-op argument (one side of diff/inter/union) to a SQL subquery.
    /// Handles: bare *source, bound variable, filter f *source, and do-block SQL plans.
    fn try_set_op_subquery(
        &self,
        expr: &ast::Expr,
        env: &Env,
    ) -> Option<SetOpSubquery> {
        // Case 1: bare *source or bound variable
        if let Some(source_name) = self.resolve_source(expr) {
            if self.views.contains_key(&source_name) {
                return None;
            }
            let schema = self.source_schemas.get(&source_name)?;
            if schema.starts_with('#') || schema.contains('[') {
                return None;
            }
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let cols = parse_schema_columns(schema).iter()
                .map(|(name, _)| quote_sql_ident(name))
                .collect::<Vec<_>>()
                .join(", ");
            return Some(SetOpSubquery {
                sql: format!("SELECT {} FROM {}", cols, table),
                schema: schema.clone(),
                params: vec![],
                tables: vec![source_name.clone()],
            });
        }

        // Case 2: filter f *source
        if let Some((source_name, filter_bind, filter_body)) =
            extract_filter_on_source(expr, &self.source_var_binds, &self.fun_bodies, &self.let_bindings)
        {
            let source_name: &str = &source_name;
            let filter_body: &ast::Expr = &filter_body;
            if self.views.contains_key(source_name) {
                return None;
            }
            let schema = self.source_schemas.get(source_name)?;
            if schema.starts_with('#') || schema.contains('[') {
                return None;
            }
            let frag = self.try_compile_sql_expr(&filter_bind, filter_body, schema)?;
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let cols = parse_schema_columns(schema).iter()
                .map(|(name, _)| quote_sql_ident(name))
                .collect::<Vec<_>>()
                .join(", ");
            return Some(SetOpSubquery {
                sql: format!("SELECT {} FROM {} WHERE {}", cols, table, frag.sql),
                schema: schema.clone(),
                params: frag.params,
                tables: vec![source_name.to_string()],
            });
        }

        // Case 3: do-block → full SQL plan
        if let ast::ExprKind::Do(stmts) = &expr.node {
            let plan = self.analyze_sql_plan(stmts, env)?;
            let tables = plan.tables.iter().map(|t| t.source_name.clone()).collect();
            return Some(SetOpSubquery {
                sql: plan.build_sql(),
                schema: plan.build_result_schema(),
                params: plan.params,
                tables,
            });
        }

        None
    }

    fn analyze_sql_plan(
        &self,
        stmts: &[ast::Stmt],
        env: &Env,
    ) -> Option<SqlQueryPlan> {
        if stmts.is_empty() {
            return None;
        }
        // User trait impls on primitives change operator semantics that
        // SQL can't replicate — fall back to in-memory evaluation.
        if self.sql_pushdown_disabled_by_user_impls() {
            return None;
        }
        let mut tables: Vec<SqlTable> = Vec::new();
        let mut bind_to_alias: HashMap<String, String> = HashMap::new();
        let mut bind_to_schema: HashMap<String, String> = HashMap::new();
        let mut let_binds: HashMap<String, ast::Expr> = HashMap::new();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<SqlParamSource> = Vec::new();

        for stmt in &stmts[..stmts.len() - 1] {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    let var_name = if let ast::PatKind::Var(name) = &pat.node {
                        name.clone()
                    } else {
                        return None;
                    };
                    let source_name = if let ast::ExprKind::SourceRef(name) = &expr.node {
                        name.clone()
                    } else if let ast::ExprKind::Var(var_name) = &expr.node {
                        // Variable bound from a source read (e.g. `allMessages <- *messages`)
                        self.source_var_binds.get(var_name)?.clone()
                    } else {
                        return None;
                    };

                    if self.views.contains_key(&source_name) {
                        return None;
                    }
                    let schema = self.source_schemas.get(&source_name)?.clone();
                    if schema.starts_with('#') || schema.contains('[') {
                        return None;
                    }

                    let alias = format!("t{}", tables.len());
                    bind_to_alias.insert(var_name.clone(), alias.clone());
                    bind_to_schema.insert(var_name.clone(), schema.clone());
                    tables.push(SqlTable {
                        source_name,
                        alias,
                    });
                }
                ast::StmtKind::Let { pat, expr } => {
                    // Only support simple variable patterns; bail on destructuring.
                    let var_name = if let ast::PatKind::Var(name) = &pat.node {
                        name.clone()
                    } else {
                        return None;
                    };
                    // The let expression must not reference any bind aliases
                    // (it's a computed parameter, not a column alias).
                    if bind_to_alias.keys().any(|k| Self::expr_refs_var(expr, k)) {
                        return None;
                    }
                    // Close the expression over earlier do-local lets: a
                    // chained `let a = 5; let b = a + 1` stores `b` as a
                    // param expression that is later compiled in the
                    // *enclosing* env, where `a` is not bound — substitute
                    // previously-collected let bindings so every stored
                    // expression only references outer-scope names. Entries
                    // in `let_binds` are already closed (each was
                    // substituted when stored), so a single pass suffices.
                    let mut closed = expr.clone();
                    for (k, v) in &let_binds {
                        match substitute(&closed, k, v) {
                            Some(s) => closed = s,
                            // Substitution would capture a free variable —
                            // bail out of the SQL plan entirely rather than
                            // compile a param expr with unbound names.
                            None => return None,
                        }
                    }
                    let_binds.insert(var_name, closed);
                }
                ast::StmtKind::Where { cond } => {
                    let frag = Self::try_compile_multi_table_sql_expr(
                        &bind_to_alias, &bind_to_schema, cond, env, &let_binds,
                    )?;
                    conditions.push(frag.sql);
                    params.extend(frag.params);
                }
                _ => return None,
            }
        }

        if tables.is_empty() {
            return None;
        }

        // Parse the yield statement
        let yield_expr = match &stmts.last()?.node {
            ast::StmtKind::Expr(e) => {
                e.node.as_yield_arg()?
            }
            _ => return None,
        };

        let mut select_columns: Vec<SqlSelectColumn> = Vec::new();

        match &yield_expr.node {
            ast::ExprKind::Record(fields) => {
                for field in fields {
                    // Try simple field access first: var.column
                    if let ast::ExprKind::FieldAccess { expr, field: col_name } = &field.value.node
                        && let ast::ExprKind::Var(var_name) = &expr.node
                            && let Some(alias) = bind_to_alias.get(var_name)
                                && let Some(schema) = bind_to_schema.get(var_name)
                                    && let Some(type_str) = lookup_col_type_from_schema(schema, col_name) {
                                        select_columns.push(SqlSelectColumn {
                                            result_field: field.name.clone(),
                                            alias: alias.clone(),
                                            source_col: col_name.clone(),
                                            type_str,
                                            sql_expr: None,
                                        });
                                        continue;
                                    }
                    // Fallback: try computed expression (arithmetic, CASE WHEN)
                    if let Some(sql_expr) = try_multi_table_arithmetic_expr(
                        &bind_to_alias, &bind_to_schema, &field.value,
                    ) {
                        let type_str = infer_multi_table_sql_expr_type(
                            &bind_to_schema, &field.value,
                        ).unwrap_or_else(|| "float".to_string());
                        select_columns.push(SqlSelectColumn {
                            result_field: field.name.clone(),
                            alias: String::new(),
                            source_col: field.name.clone(),
                            type_str,
                            sql_expr: Some(sql_expr),
                        });
                    } else {
                        return None;
                    }
                }
            }
            ast::ExprKind::Var(var_name) => {
                if tables.len() != 1 {
                    return None;
                }
                let alias = bind_to_alias.get(var_name)?.clone();
                let schema = bind_to_schema.get(var_name)?;
                for (col_name, type_str) in parse_schema_columns(schema) {
                    select_columns.push(SqlSelectColumn {
                        result_field: col_name.clone(),
                        alias: alias.clone(),
                        source_col: col_name,
                        type_str,
                        sql_expr: None,
                    });
                }
            }
            _ => return None,
        }

        Some(SqlQueryPlan {
            tables,
            conditions,
            params,
            select_columns,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }

    /// Compile a multi-table Where condition to a SQL fragment.
    /// Handles both join conditions (field = field) and filter conditions (field op ?).
    /// `let_binds` maps let-bound variable names to their expressions for SQL parameter compilation.
    /// `bind_schemas` maps bind variables to their source schemas (used to
    /// type-check arithmetic comparisons so float arithmetic doesn't get the
    /// integer text-cast treatment).
    fn try_compile_multi_table_sql_expr(
        bind_aliases: &HashMap<String, String>,
        bind_schemas: &HashMap<String, String>,
        expr: &ast::Expr,
        env: &Env,
        let_binds: &HashMap<String, ast::Expr>,
    ) -> Option<SqlFragment> {
        match &expr.node {
            ast::ExprKind::BinOp { op, lhs, rhs } => match op {
                ast::BinOp::And => {
                    let l = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, lhs, env, let_binds)?;
                    let r = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, rhs, env, let_binds)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) AND ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Or => {
                    let l = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, lhs, env, let_binds)?;
                    let r = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, rhs, env, let_binds)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) OR ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Eq | ast::BinOp::Neq | ast::BinOp::Lt
                | ast::BinOp::Gt | ast::BinOp::Le | ast::BinOp::Ge => {
                    let sql_op = match op {
                        ast::BinOp::Eq => "=",
                        ast::BinOp::Neq => "!=",
                        ast::BinOp::Lt => "<",
                        ast::BinOp::Gt => ">",
                        ast::BinOp::Le => "<=",
                        ast::BinOp::Ge => ">=",
                        _ => unreachable!(),
                    };
                    Self::try_compile_multi_table_comparison(bind_aliases, bind_schemas, lhs, rhs, sql_op, env, let_binds)
                        .or_else(|| {
                            let rev = match sql_op {
                                "=" | "!=" => sql_op,
                                "<" => ">",
                                ">" => "<",
                                "<=" => ">=",
                                ">=" => "<=",
                                _ => return None,
                            };
                            Self::try_compile_multi_table_comparison(
                                bind_aliases, bind_schemas, rhs, lhs, rev, env, let_binds,
                            )
                        })
                }
                ast::BinOp::Add | ast::BinOp::Sub | ast::BinOp::Mul | ast::BinOp::Div
                | ast::BinOp::Mod | ast::BinOp::Concat => {
                    // Arithmetic/concat in WHERE: try to compile both sides as SQL atoms.
                    // `/` and `%` only push down with a provably-nonzero literal
                    // divisor (matching every other arithmetic site): SQLite yields
                    // NULL on division by zero where the Knot runtime panics, and
                    // SQLite's `%`/`/` on floats differ from runtime semantics.
                    let sql_op = match op {
                        ast::BinOp::Add => "+",
                        ast::BinOp::Sub => "-",
                        ast::BinOp::Mul => "*",
                        ast::BinOp::Div if divisor_is_nonzero_literal(rhs) => "/",
                        ast::BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => "%",
                        ast::BinOp::Concat => "||",
                        ast::BinOp::Div | ast::BinOp::Mod => return None,
                        _ => unreachable!(),
                    };
                    let l = Self::try_compile_sql_atom(bind_aliases, lhs, env, let_binds)?;
                    let r = Self::try_compile_sql_atom(bind_aliases, rhs, env, let_binds)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({} {} {})", l.sql, sql_op, r.sql),
                        params,
                    })
                }
                _ => None,
            },
            ast::ExprKind::UnaryOp {
                op: ast::UnaryOp::Not,
                operand,
            } => {
                let inner = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, operand, env, let_binds)?;
                Some(SqlFragment {
                    sql: format!("NOT ({})", inner.sql),
                    params: inner.params,
                })
            }
            // `not expr` function application form → NOT (...)
            ast::ExprKind::App { func, arg } => {
                if let ast::ExprKind::Var(name) = &func.node
                    && name == "not" {
                        let inner = Self::try_compile_multi_table_sql_expr(bind_aliases, bind_schemas, arg, env, let_binds)?;
                        return Some(SqlFragment {
                            sql: format!("NOT ({})", inner.sql),
                            params: inner.params,
                        });
                    }
                // Two-arg builtins: App(App(Var(name), arg1), arg2)
                if let ast::ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                    && let ast::ExprKind::Var(name) = &inner_func.node
                        && name == "contains" {
                            // contains needle haystack → INSTR(haystack, needle) > 0
                            let needle = Self::try_compile_sql_atom(bind_aliases, first_arg, env, let_binds)?;
                            let haystack = Self::try_compile_sql_atom(bind_aliases, arg, env, let_binds)?;
                            let mut params = haystack.params;
                            params.extend(needle.params);
                            return Some(SqlFragment {
                                sql: format!("INSTR({}, {}) > 0", haystack.sql, needle.sql),
                                params,
                            });
                        }
                None
            }
            _ => None,
        }
    }

    /// Try to compile an expression as a SQL atom (field access, literal, var, or arithmetic).
    /// Used as operands in comparisons and arithmetic.
    /// `let_binds` maps let-bound variable names to their original expressions.
    fn try_compile_sql_atom(
        bind_aliases: &HashMap<String, String>,
        expr: &ast::Expr,
        env: &Env,
        let_binds: &HashMap<String, ast::Expr>,
    ) -> Option<SqlFragment> {
        match &expr.node {
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                if let ast::ExprKind::Var(name) = &inner.node {
                    if let Some(alias) = bind_aliases.get(name.as_str()) {
                        return Some(SqlFragment {
                            sql: format!("{}.{}", alias, quote_sql_ident(field)),
                            params: vec![],
                        });
                    }
                    // Field access on env variable or global — compute at runtime
                    return Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![if env.bindings.contains_key(name) {
                            SqlParamSource::FieldAccess(name.clone(), field.clone())
                        } else {
                            SqlParamSource::Expr(expr.clone())
                        }],
                    });
                }
                None
            }
            ast::ExprKind::Lit(lit) => Some(SqlFragment {
                sql: "?".to_string(),
                params: vec![SqlParamSource::Literal(lit.clone())],
            }),
            ast::ExprKind::Var(name) => {
                if bind_aliases.contains_key(name.as_str()) {
                    None
                } else if let Some(let_expr) = let_binds.get(name.as_str()) {
                    // Let-bound variable from within the do-block — compile
                    // the original let expression at runtime as a SQL parameter.
                    Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![SqlParamSource::Expr(let_expr.clone())],
                    })
                } else if env.bindings.contains_key(name) {
                    Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![SqlParamSource::Var(name.clone())],
                    })
                } else {
                    // Global constant or user function — compile at runtime
                    Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![SqlParamSource::Expr(expr.clone())],
                    })
                }
            }
            ast::ExprKind::BinOp { op, lhs, rhs } => {
                let sql_op = match op {
                    ast::BinOp::Add => "+",
                    ast::BinOp::Sub => "-",
                    ast::BinOp::Mul => "*",
                    // `/` and `%` may only be pushed down with a provably
                    // nonzero literal divisor (SQLite yields NULL on division
                    // by zero; the runtime panics). `%` additionally requires
                    // integer operands (SQLite `%` truncates to INTEGER while
                    // the runtime does float fmod) — an integer-literal
                    // divisor proves this through type checking.
                    ast::BinOp::Div if divisor_is_nonzero_literal(rhs) => "/",
                    ast::BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => "%",
                    ast::BinOp::Concat => "||",
                    _ => return None,
                };
                let l = Self::try_compile_sql_atom(bind_aliases, lhs, env, let_binds)?;
                let r = Self::try_compile_sql_atom(bind_aliases, rhs, env, let_binds)?;
                let mut params = l.params;
                params.extend(r.params);
                Some(SqlFragment {
                    sql: format!("({} {} {})", l.sql, sql_op, r.sql),
                    params,
                })
            }
            // Built-in functions: toUpper, toLower, trim, length
            ast::ExprKind::App { func, .. } => {
                if let ast::ExprKind::Var(_) = &func.node {
                    // NOTE: toUpper/toLower are deliberately NOT pushed
                    // down — SQLite's UPPER/LOWER are ASCII-only while the
                    // runtime does full Unicode case mapping. Likewise
                    // trim: SQLite TRIM strips ASCII spaces only, while
                    // the runtime trims all Unicode whitespace. length is
                    // also NOT pushed down: SQLite LENGTH() counts chars
                    // before the first NUL byte, while knot_text_length
                    // counts all chars.
                    None
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Compile a multi-table comparison. Both sides can be field accesses,
    /// literals, variables, or arithmetic expressions.
    fn try_compile_multi_table_comparison(
        bind_aliases: &HashMap<String, String>,
        bind_schemas: &HashMap<String, String>,
        lhs: &ast::Expr,
        rhs: &ast::Expr,
        op: &str,
        env: &Env,
        let_binds: &HashMap<String, ast::Expr>,
    ) -> Option<SqlFragment> {
        // Type-witness gate: ints are stored as TEXT (and need the KNOT_INT
        // cast around arithmetic) while floats are stored as REAL (and must
        // NOT get the text cast — it would compare floats byte-wise). Fall
        // back entirely when the comparison can't be typed.
        let col_ty = |v: &str, f: &str| {
            bind_schemas
                .get(v)
                .and_then(|schema| lookup_col_type_from_schema(schema, f))
        };
        let mode = sql_comparison_cast_mode(lhs, rhs, &col_ty)?;
        // Ordered comparisons on tag columns stay in memory (byte-wise
        // constructor-name order ≠ the type's Ord).
        if matches!(op, "<" | ">" | "<=" | ">=")
            && (expr_has_tag_column(lhs, &col_ty) || expr_has_tag_column(rhs, &col_ty))
        {
            return None;
        }
        let l = Self::try_compile_sql_atom(bind_aliases, lhs, env, let_binds)?;
        let r = Self::try_compile_sql_atom(bind_aliases, rhs, env, let_binds)?;
        // Comparisons involving Int arithmetic compare numerically: SQLite
        // arithmetic on TEXT-stored Int columns produces INTEGER, but
        // params/columns are TEXT — INTEGER vs TEXT comparison orders by
        // storage class, not value. Casting both sides to NUMERIC compares
        // by value; on i64 overflow SQLite switches the arithmetic result
        // to REAL, which then compares by its approximate real value
        // (in-memory panics on overflow; the approximation is the closest
        // faithful SQL behavior — unlike the previous CAST-to-TEXT, the
        // overflow text can no longer satisfy arbitrary KNOT_INT filters).
        // Without arithmetic, the plain TEXT comparison stays under the
        // columns' KNOT_INT collation (exact, including legacy
        // BigInt-as-TEXT rows).
        let (l_sql, r_sql) = match mode {
            SqlCastMode::CastInt => {
                if cast_arithmetic_for_where(&l.sql) != l.sql
                    || cast_arithmetic_for_where(&r.sql) != r.sql
                {
                    (
                        format!("CAST({} AS NUMERIC)", l.sql),
                        format!("CAST({} AS NUMERIC)", r.sql),
                    )
                } else {
                    (l.sql.clone(), r.sql.clone())
                }
            }
            SqlCastMode::Plain => (l.sql.clone(), r.sql.clone()),
            SqlCastMode::NoArith => {
                if cast_arithmetic_for_where(&l.sql) != l.sql
                    || cast_arithmetic_for_where(&r.sql) != r.sql
                {
                    return None;
                }
                (l.sql.clone(), r.sql.clone())
            }
        };
        let mut params = l.params;
        params.extend(r.params);
        Some(SqlFragment {
            sql: format!("{} {} {}", l_sql, op, r_sql),
            params,
        })
    }

    // ── SQL expression compilation ──────────────────────────────────

    /// Try to compile a Knot condition to a SQL WHERE fragment.
    /// `bind_var` is the loop variable (e.g., "t" in `t <- *rel`);
    /// `schema` is the bound source's column schema (used to type-check
    /// arithmetic comparisons so float arithmetic doesn't get the
    /// integer text-cast treatment).
    /// Field accesses on bind_var become column references;
    /// literals and free variables become bind parameters (?).
    fn try_compile_sql_expr(
        &self,
        bind_var: &str,
        expr: &ast::Expr,
        schema: &str,
    ) -> Option<SqlFragment> {
        // User trait impls on primitives change operator semantics that
        // SQL can't replicate — fall back to in-memory evaluation.
        if self.sql_pushdown_disabled_by_user_impls() {
            return None;
        }
        match &expr.node {
            ast::ExprKind::BinOp { op, lhs, rhs } => match op {
                ast::BinOp::And => {
                    let l = self.try_compile_sql_expr(bind_var, lhs, schema)?;
                    let r = self.try_compile_sql_expr(bind_var, rhs, schema)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) AND ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Or => {
                    let l = self.try_compile_sql_expr(bind_var, lhs, schema)?;
                    let r = self.try_compile_sql_expr(bind_var, rhs, schema)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) OR ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Eq | ast::BinOp::Neq | ast::BinOp::Lt
                | ast::BinOp::Gt | ast::BinOp::Le | ast::BinOp::Ge => {
                    let sql_op = match op {
                        ast::BinOp::Eq => "=",
                        ast::BinOp::Neq => "!=",
                        ast::BinOp::Lt => "<",
                        ast::BinOp::Gt => ">",
                        ast::BinOp::Le => "<=",
                        ast::BinOp::Ge => ">=",
                        _ => unreachable!(),
                    };
                    // Try field op value, then value op field (reversed),
                    // then atom-based comparison (handles arithmetic like x.a * x.b > 100)
                    Self::try_compile_sql_comparison(bind_var, lhs, rhs, sql_op, schema)
                        .or_else(|| {
                            let rev = match sql_op {
                                "=" | "!=" => sql_op,
                                "<" => ">",
                                ">" => "<",
                                "<=" => ">=",
                                ">=" => "<=",
                                _ => return None,
                            };
                            Self::try_compile_sql_comparison(bind_var, rhs, lhs, rev, schema)
                        })
                        .or_else(|| {
                            // Type-witness gate: decide between the KNOT_INT
                            // text-cast (ints stored as TEXT) and plain numeric
                            // SQL (floats stored as REAL); fall back entirely
                            // when the comparison can't be typed.
                            let col_ty = |v: &str, f: &str| {
                                if v == bind_var {
                                    lookup_col_type_from_schema(schema, f)
                                } else {
                                    None
                                }
                            };
                            let mode = sql_comparison_cast_mode(lhs, rhs, &col_ty)?;
                            // Ordered comparisons on tag columns stay in
                            // memory (byte-wise name order ≠ Ord).
                            if matches!(sql_op, "<" | ">" | "<=" | ">=")
                                && (expr_has_tag_column(lhs, &col_ty)
                                    || expr_has_tag_column(rhs, &col_ty))
                            {
                                return None;
                            }
                            let l = Self::try_compile_single_table_atom(bind_var, lhs)?;
                            let r = Self::try_compile_single_table_atom(bind_var, rhs)?;
                            // See try_compile_multi_table_comparison for
                            // the CastInt/NUMERIC rationale (overflow-safe
                            // numeric comparison around Int arithmetic).
                            let (l_sql, r_sql) = match mode {
                                SqlCastMode::CastInt => {
                                    if cast_arithmetic_for_where(&l.sql) != l.sql
                                        || cast_arithmetic_for_where(&r.sql) != r.sql
                                    {
                                        (
                                            format!("CAST({} AS NUMERIC)", l.sql),
                                            format!("CAST({} AS NUMERIC)", r.sql),
                                        )
                                    } else {
                                        (l.sql.clone(), r.sql.clone())
                                    }
                                }
                                SqlCastMode::Plain => (l.sql.clone(), r.sql.clone()),
                                SqlCastMode::NoArith => {
                                    if cast_arithmetic_for_where(&l.sql) != l.sql
                                        || cast_arithmetic_for_where(&r.sql) != r.sql
                                    {
                                        return None;
                                    }
                                    (l.sql.clone(), r.sql.clone())
                                }
                            };
                            let mut params = l.params;
                            params.extend(r.params);
                            Some(SqlFragment {
                                sql: format!("{} {} {}", l_sql, sql_op, r_sql),
                                params,
                            })
                        })
                }
                _ => None,
            },
            ast::ExprKind::UnaryOp {
                op: ast::UnaryOp::Not,
                operand,
            } => {
                let inner = self.try_compile_sql_expr(bind_var, operand, schema)?;
                Some(SqlFragment {
                    sql: format!("NOT ({})", inner.sql),
                    params: inner.params,
                })
            }
            // `not expr` function application form → NOT (...)
            // `contains needle haystack` → INSTR(haystack, needle) > 0
            ast::ExprKind::App { func, arg } => {
                if let ast::ExprKind::Var(name) = &func.node
                    && name == "not" {
                        let inner = self.try_compile_sql_expr(bind_var, arg, schema)?;
                        return Some(SqlFragment {
                            sql: format!("NOT ({})", inner.sql),
                            params: inner.params,
                        });
                    }
                // Two-arg builtins: App(App(Var(name), arg1), arg2)
                if let ast::ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                    && let ast::ExprKind::Var(name) = &inner_func.node {
                        if name == "contains" {
                            let needle = Self::try_compile_single_table_atom(bind_var, first_arg)?;
                            let haystack = Self::try_compile_single_table_atom(bind_var, arg)?;
                            let mut params = haystack.params;
                            params.extend(needle.params);
                            return Some(SqlFragment {
                                sql: format!("INSTR({}, {}) > 0", haystack.sql, needle.sql),
                                params,
                            });
                        }
                        if name == "elem" {
                            let needle = Self::try_compile_single_table_atom(bind_var, first_arg)?;
                            // (a) Literal list: emit `IN (?, ?, …)` directly so
                            //     SQLite can compare each element by type
                            //     affinity without going through json_each.
                            if let ast::ExprKind::List(elems) = &arg.node {
                                if elems.is_empty() {
                                    return Some(SqlFragment {
                                        sql: "0".to_string(),
                                        params: vec![],
                                    });
                                }
                                // `IN` is equality under the hood, so — like the
                                // dynamic path below — only push down when
                                // inference marked the haystack's element type a
                                // SQL-pushable scalar. This excludes floats
                                // (total_cmp treats -0.0 ≠ +0.0 and NaN as
                                // comparable, SQL doesn't), regardless of whether
                                // the needle is a column, a computed value, or a
                                // literal. Non-pushable → fall back to memory.
                                if !self.elem_pushdown_ok.literal.contains(&arg.span) {
                                    return None;
                                }
                                // An arithmetic needle (e.g. `x.a + x.b`)
                                // evaluates to an INTEGER in SQLite, but Int list
                                // elements bind as TEXT, so a raw `8 IN ('10','20')`
                                // is a storage-class mismatch that never matches.
                                // Cast both the needle and every element to
                                // NUMERIC (mirrors the comparison path's CastInt
                                // handling). A bare-column needle keeps its TEXT
                                // affinity/collation, so only arithmetic needs it.
                                let needle_arith =
                                    cast_arithmetic_for_where(&needle.sql) != needle.sql;
                                let mut parts = Vec::with_capacity(elems.len());
                                let mut params = needle.params;
                                for e in elems {
                                    let frag = Self::try_compile_single_table_atom(bind_var, e)?;
                                    parts.push(if needle_arith {
                                        format!("CAST({} AS NUMERIC)", frag.sql)
                                    } else {
                                        frag.sql
                                    });
                                    params.extend(frag.params);
                                }
                                let needle_sql = if needle_arith {
                                    format!("CAST({} AS NUMERIC)", needle.sql)
                                } else {
                                    needle.sql
                                };
                                return Some(SqlFragment {
                                    sql: format!("{} IN ({})", needle_sql, parts.join(", ")),
                                    params,
                                });
                            }
                            // (b) Dynamic haystack: bind the whole list as a
                            //     single JSON-encoded param (value_to_sql_param
                            //     auto-encodes Relations) and expand via
                            //     json_each. Gated by inference's `dynamic` set
                            //     (Text/Bool/Uuid only — Int and Float excluded,
                            //     since json_each yields JSON storage classes that
                            //     don't match the TEXT-stored Int column).
                            //     Param can't reference the bind var since the
                            //     haystack is evaluated outside the SQL row scope.
                            if self.elem_pushdown_ok.dynamic.contains(&arg.span)
                                && !Self::expr_refs_var(arg, bind_var)
                            {
                                let mut params = needle.params;
                                params.push(SqlParamSource::Expr((**arg).clone()));
                                return Some(SqlFragment {
                                    sql: format!(
                                        "{} IN (SELECT value FROM json_each(?))",
                                        needle.sql,
                                    ),
                                    params,
                                });
                            }
                            return None;
                        }
                    }
                None
            }
            _ => None,
        }
    }

    /// Try to compile `field_expr op value_expr` to SQL.
    fn try_compile_sql_comparison(
        bind_var: &str,
        field_side: &ast::Expr,
        value_side: &ast::Expr,
        op: &str,
        schema: &str,
    ) -> Option<SqlFragment> {
        let col_name = if let ast::ExprKind::FieldAccess { expr, field } = &field_side.node {
            if let ast::ExprKind::Var(name) = &expr.node {
                if name == bind_var {
                    field.clone()
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        };

        let col_ty = lookup_col_type_from_schema(schema, &col_name);
        // Payload-bearing ADT fields and nested records are stored as JSON
        // documents, but the runtime encodes the compared Knot value
        // differently when binding it as a SQL parameter (constructor
        // params bind as bare tag text). A pushed-down `col = ?` would
        // silently drop matching rows — fall back to in-memory evaluation.
        if col_ty.as_deref() == Some("json") {
            return None;
        }
        // Float comparisons must stay in memory: Knot compares floats with
        // total_cmp (-0.0 < +0.0, NaN orderable) while SQL says -0.0 = 0.0
        // and stores NaN as NULL — `col != ?` would silently drop NaN rows.
        if col_ty.as_deref() == Some("float") {
            return None;
        }
        // Equality on all-nullary ("tag") ADT columns stays pushable (tag
        // equality is name equality), but ordered comparisons would use
        // byte-wise name order, ignoring the type's Ord (declaration order
        // or a user impl) — keep those in memory.
        if col_ty.as_deref() == Some("tag") && matches!(op, "<" | ">" | "<=" | ">=") {
            return None;
        }

        let param = match &value_side.node {
            ast::ExprKind::Lit(lit) => SqlParamSource::Literal(lit.clone()),
            ast::ExprKind::Var(name) => SqlParamSource::Var(name.clone()),
            ast::ExprKind::FieldAccess { expr, field } => {
                if let ast::ExprKind::Var(var_name) = &expr.node {
                    if var_name != bind_var {
                        SqlParamSource::FieldAccess(var_name.clone(), field.clone())
                    } else {
                        return None; // both sides are bind_var fields
                    }
                } else {
                    return None;
                }
            }
            // Computed expression (e.g. `t - messageMaxAge`): compile at runtime
            // and pass as a SQL parameter.  Only accept if the expression doesn't
            // reference the bind variable (would need column-level SQL instead).
            _ => {
                if Self::expr_refs_var(value_side, bind_var) {
                    return None;
                }
                SqlParamSource::Expr(value_side.clone())
            }
        };

        Some(SqlFragment {
            sql: format!("{} {} ?", quote_sql_ident(&col_name), op),
            params: vec![param],
        })
    }

    /// Try to compile a single-table expression as a SQL atom.
    /// Handles: `bind_var.field` → column ref, literals/vars → `?`, arithmetic combos.
    fn try_compile_single_table_atom(
        bind_var: &str,
        expr: &ast::Expr,
    ) -> Option<SqlFragment> {
        match &expr.node {
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                if let ast::ExprKind::Var(name) = &inner.node {
                    if name == bind_var {
                        return Some(SqlFragment {
                            sql: quote_sql_ident(field),
                            params: vec![],
                        });
                    }
                    // Field access on other variable → parameter
                    return Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![SqlParamSource::FieldAccess(name.clone(), field.clone())],
                    });
                }
                None
            }
            ast::ExprKind::Lit(lit) => Some(SqlFragment {
                sql: "?".to_string(),
                params: vec![SqlParamSource::Literal(lit.clone())],
            }),
            ast::ExprKind::Var(name) => {
                if name == bind_var {
                    return None;
                }
                Some(SqlFragment {
                    sql: "?".to_string(),
                    params: vec![SqlParamSource::Var(name.clone())],
                })
            }
            ast::ExprKind::BinOp { op, lhs, rhs } => {
                let sql_op = match op {
                    ast::BinOp::Add => "+",
                    ast::BinOp::Sub => "-",
                    ast::BinOp::Mul => "*",
                    // See try_compile_sql_atom: `/`/`%` need a provably
                    // nonzero literal divisor; `%` must be integer-typed.
                    ast::BinOp::Div if divisor_is_nonzero_literal(rhs) => "/",
                    ast::BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => "%",
                    ast::BinOp::Concat => "||",
                    _ => return None,
                };
                let l = Self::try_compile_single_table_atom(bind_var, lhs)?;
                let r = Self::try_compile_single_table_atom(bind_var, rhs)?;
                let mut params = l.params;
                params.extend(r.params);
                Some(SqlFragment {
                    sql: format!("({} {} {})", l.sql, sql_op, r.sql),
                    params,
                })
            }
            // Built-in functions: toUpper, toLower, trim, length
            ast::ExprKind::App { func, .. } => {
                if let ast::ExprKind::Var(_) = &func.node {
                    // NOTE: toUpper/toLower are deliberately NOT pushed
                    // down — SQLite's UPPER/LOWER are ASCII-only while the
                    // runtime does full Unicode case mapping. Likewise
                    // trim: SQLite TRIM strips ASCII spaces only, while
                    // the runtime trims all Unicode whitespace. length is
                    // also NOT pushed down: SQLite LENGTH() counts chars
                    // before the first NUL byte, while knot_text_length
                    // counts all chars.
                    None
                } else {
                    None
                }
            }
            _ => {
                if Self::expr_refs_var(expr, bind_var) {
                    return None;
                }
                Some(SqlFragment {
                    sql: "?".to_string(),
                    params: vec![SqlParamSource::Expr(expr.clone())],
                })
            }
        }
    }

    /// Check if an expression refers to a specific source (directly or via a bound variable).
    fn expr_is_source(node: &ast::ExprKind, source_name: &str, var_binds: &HashMap<String, String>) -> bool {
        match node {
            ast::ExprKind::SourceRef(name) => name == source_name,
            ast::ExprKind::Var(name) => var_binds.get(name).is_some_and(|s| s == source_name),
            _ => false,
        }
    }

    /// Check if an expression references a specific variable.
    fn expr_refs_var(expr: &ast::Expr, var: &str) -> bool {
        expr_refs_var(expr, var)
    }

    /// Match an equi-join pattern: `a.f == b.g` where a and b are two different
    /// bind variables. Returns (var1, field1, var2, field2) if matched.
    fn match_equi_join<'a>(
        cond: &'a ast::Expr,
        var_a: &str,
        var_b: &str,
    ) -> Option<(&'a str, &'a str, &'a str, &'a str)> {
        if let ast::ExprKind::BinOp {
            op: ast::BinOp::Eq,
            lhs,
            rhs,
        } = &cond.node
        {
            let extract_field_access =
                |e: &'a ast::Expr| -> Option<(&'a str, &'a str)> {
                    if let ast::ExprKind::FieldAccess { expr, field } = &e.node
                        && let ast::ExprKind::Var(name) = &expr.node {
                            return Some((name.as_str(), field.as_str()));
                        }
                    None
                };

            let (lv, lf) = extract_field_access(lhs)?;
            let (rv, rf) = extract_field_access(rhs)?;

            // Check that we have one from each side
            if lv == var_a && rv == var_b {
                return Some((lv, lf, rv, rf));
            }
            if lv == var_b && rv == var_a {
                return Some((rv, rf, lv, lf));
            }
        }
        None
    }

    // ── Additional set-expression pattern matchers ───────────────────

    /// Detect `do { t <- *rel; yield expr }` with no `where` clauses.
    /// A simple map: every input row produces one output row, so full write
    /// is safe and avoids diff overhead.
    fn match_map_no_filter(source_name: &str, value: &ast::Expr) -> bool {
        if let ast::ExprKind::Do(stmts) = &value.node
            && stmts.len() == 2
                && let ast::StmtKind::Bind { expr, .. } = &stmts[0].node
                    && let ast::ExprKind::SourceRef(name) = &expr.node
                        && name == source_name
                            && let ast::StmtKind::Expr(e) = &stmts[1].node {
                                return e.node.as_yield_arg().is_some();
                            }
        false
    }

    /// Detect `do { t <- *rel; where cond1; ...; yield t }`.
    /// Returns (bind_var_name, conditions) for SQL DELETE WHERE compilation.
    fn match_filter_only<'a>(
        source_name: &str,
        value: &'a ast::Expr,
    ) -> Option<(String, Vec<&'a ast::Expr>)> {
        let stmts = if let ast::ExprKind::Do(stmts) = &value.node {
            stmts
        } else {
            return None;
        };
        if stmts.len() < 3 {
            return None;
        }

        // First: t <- *rel
        let bind_var = if let ast::StmtKind::Bind { pat, expr } = &stmts[0].node {
            if let ast::ExprKind::SourceRef(name) = &expr.node {
                if name == source_name {
                    if let ast::PatKind::Var(v) = &pat.node {
                        v.clone()
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        };

        // Last: yield t
        if let ast::StmtKind::Expr(e) = &stmts.last()?.node {
            if let Some(inner) = e.node.as_yield_arg() {
                if let ast::ExprKind::Var(v) = &inner.node {
                    if v != &bind_var {
                        return None;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        }

        // Middle: all must be where clauses
        let mut conditions = Vec::new();
        for stmt in &stmts[1..stmts.len() - 1] {
            if let ast::StmtKind::Where { cond } = &stmt.node {
                conditions.push(cond);
            } else {
                return None;
            }
        }
        if conditions.is_empty() {
            return None;
        }

        Some((bind_var, conditions))
    }

    /// Detect `do { t <- *rel; yield (if cond then {t | fields} else t) }`.
    /// Returns (bind_var, condition, update_fields) for SQL UPDATE WHERE.
    fn match_conditional_update<'a>(
        source_name: &str,
        value: &'a ast::Expr,
    ) -> Option<ConditionalUpdateMatch<'a>> {
        let stmts = if let ast::ExprKind::Do(stmts) = &value.node {
            stmts
        } else {
            return None;
        };
        if stmts.len() != 2 {
            return None;
        }

        // First: t <- *rel
        let bind_var = if let ast::StmtKind::Bind { pat, expr } = &stmts[0].node {
            if let ast::ExprKind::SourceRef(name) = &expr.node {
                if name == source_name {
                    if let ast::PatKind::Var(v) = &pat.node {
                        v.clone()
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            return None;
        };

        // Second: yield (if cond then {t | ...} else t)
        if let ast::StmtKind::Expr(e) = &stmts[1].node
            && let Some(yield_inner) = e.node.as_yield_arg()
                && let ast::ExprKind::If {
                    cond,
                    then_branch,
                    else_branch,
                } = &yield_inner.node
                {
                    // else must be just the bind var
                    if let ast::ExprKind::Var(v) = &else_branch.node {
                        if v != &bind_var {
                            return None;
                        }
                    } else {
                        return None;
                    }
                    // then must be {t | field: val, ...}
                    if let ast::ExprKind::RecordUpdate { base, fields } = &then_branch.node {
                        if let ast::ExprKind::Var(v) = &base.node {
                            if v != &bind_var {
                                return None;
                            }
                        } else {
                            return None;
                        }
                        let update_fields: Vec<(&str, &ast::Expr)> =
                            fields.iter().map(|f| (f.name.as_str(), &f.value)).collect();
                        return Some((bind_var, cond, update_fields));
                    }
                }
        None
    }

    /// Compile SQL bind parameters into a runtime Relation value.
    fn compile_sql_params(
        &mut self,
        builder: &mut FunctionBuilder,
        params: &[SqlParamSource],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let rel = if params.is_empty() {
            self.call_rt(builder, "knot_relation_empty", &[])
        } else {
            let cap = builder.ins().iconst(self.ptr_type, params.len() as i64);
            self.call_rt(builder, "knot_relation_with_capacity", &[cap])
        };
        for param in params {
            let val = match param {
                SqlParamSource::Literal(lit) => self.compile_lit(builder, lit),
                // Var/FieldAccess names that aren't local bindings are
                // top-level constants — resolve them through compile_expr
                // (env → user_fns), matching the Expr-style fallback that
                // try_compile_sql_atom uses, instead of panicking in
                // `Env::get`.
                SqlParamSource::Var(name) => {
                    if let Some(&v) = env.bindings.get(name.as_str()) {
                        v
                    } else {
                        // Check let_bindings first — a do-local let variable
                        // would panic if resolved through compile_expr as a
                        // top-level function.
                        let let_expr = self.let_bindings.get(name).cloned();
                        if let Some(let_expr) = let_expr {
                            self.compile_expr(builder, &let_expr, env, db)
                        } else {
                            let var_expr = ast::Spanned::new(
                                ast::ExprKind::Var(name.clone()),
                                ast::Span::new(0, 0),
                            );
                            self.compile_expr(builder, &var_expr, env, db)
                        }
                    }
                }
                SqlParamSource::FieldAccess(var, field) => {
                    let let_expr = self.let_bindings.get(var).cloned();
                    let record = if let Some(&v) = env.bindings.get(var.as_str()) {
                        v
                    } else if let Some(let_expr) = let_expr {
                        self.compile_expr(builder, &let_expr, env, db)
                    } else {
                        let var_expr = ast::Spanned::new(
                            ast::ExprKind::Var(var.clone()),
                            ast::Span::new(0, 0),
                        );
                        self.compile_expr(builder, &var_expr, env, db)
                    };
                    let (fptr, flen) = self.string_ptr(builder, field);
                    self.call_rt(builder, "knot_record_field", &[record, fptr, flen])
                }
                SqlParamSource::Expr(expr) => self.compile_expr(builder, expr, env, db),
            };
            self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
        }
        rel
    }

    /// Emit a runtime `knot_stm_track_read_pred` call refining the most-recent
    /// `All` filter for this table into a richer `Cols` filter built from
    /// `preds`. No-op when `preds` is `None` (the broad `knot_stm_track_read`
    /// the caller already emitted stays the only entry).
    /// Emit a `knot_stm_track_read` call for a pushed-down SQL read of
    /// `source_name`. The `knot_source_query*` runtime functions do NOT
    /// track reads internally (unlike `knot_source_read`/`read_where`), so
    /// every codegen path that emits one of them must call this for every
    /// table the query touches — otherwise an STM `retry` watching the
    /// table is never woken by writes to it.
    fn emit_stm_track_read(&mut self, builder: &mut FunctionBuilder, source_name: &str) {
        let (tn_ptr, tn_len) = self.string_ptr(builder, source_name);
        self.call_rt_void(builder, "knot_stm_track_read", &[tn_ptr, tn_len]);
    }

    /// Emit `knot_stm_track_read` for every table of a SQL plan.
    fn emit_stm_track_reads_for_plan(
        &mut self,
        builder: &mut FunctionBuilder,
        plan: &SqlQueryPlan,
    ) {
        let names: Vec<String> = plan.tables.iter().map(|t| t.source_name.clone()).collect();
        for name in names {
            self.emit_stm_track_read(builder, &name);
        }
    }

    fn emit_stm_track_pred(
        &mut self,
        builder: &mut FunctionBuilder,
        tn_ptr: Value,
        tn_len: Value,
        preds: &Option<Vec<StmFieldPred>>,
        env: &mut Env,
        db: Value,
    ) {
        let Some(preds) = preds else { return };
        if preds.is_empty() {
            return;
        }
        let value_sources: Vec<SqlParamSource> =
            preds.iter().flat_map(|p| p.values.clone()).collect();
        // Every value source must be resolvable here: locals via `env`,
        // top-level constants via `user_fns`. Do-local `let` names from a
        // pushed-down SQL plan are neither — the plan substituted their
        // defining expressions, but the pred extractor still emits the raw
        // `Var`. Skip the precision upgrade and keep the broad `All` filter
        // (safe fallback, same policy as runtime spec parse errors) instead
        // of panicking in codegen.
        let resolvable = value_sources.iter().all(|p| match p {
            SqlParamSource::Literal(_) | SqlParamSource::Expr(_) => true,
            SqlParamSource::Var(v) | SqlParamSource::FieldAccess(v, _) => {
                env.bindings.contains_key(v) || self.user_fns.contains_key(v)
            }
        });
        if !resolvable {
            return;
        }
        let spec = serialize_stm_preds(preds);
        let pred_params_rel = self.compile_sql_params(builder, &value_sources, env, db);
        let (spec_ptr, spec_len) = self.string_ptr(builder, &spec);
        self.call_rt_void(
            builder,
            "knot_stm_track_read_pred",
            &[tn_ptr, tn_len, spec_ptr, spec_len, pred_params_rel],
        );
    }

    /// Check if an expression is (possibly through annotation wrappers) a
    /// variable bound earlier in the current IO do-block to a value that is
    /// statically known to be a relation (see `io_relation_vars`).
    fn expr_is_relation_var(&self, expr: &ast::Expr) -> bool {
        match &strip_expr_wrappers(expr).node {
            ast::ExprKind::Var(name) => self.io_relation_vars.contains(name),
            _ => false,
        }
    }

    /// A pure-comprehension do-block is desugared (before codegen) into nested
    /// `__bind`/`__yield`/`__empty` applications, so by the time codegen sees a
    /// `let xs = do { r <- rel; ...; yield e }` the RHS is an `App` spine, not
    /// an `ExprKind::Do`. Inference records the resolved monad kind keyed by the
    /// head combinator's span (the same key `compile_app` dispatches on). Return
    /// that kind when `expr` is such a desugared comprehension, so a relation-
    /// monad comprehension is recognized as relation-valued and a later
    /// `row <- xs` bind iterates per-row instead of binding the whole relation.
    fn desugared_monad_kind(&self, expr: &ast::Expr) -> Option<MonadKind> {
        let mut head = strip_expr_wrappers(expr);
        while let ast::ExprKind::App { func, .. } = &head.node {
            head = func.as_ref();
        }
        if let ast::ExprKind::Var(name) = &head.node
            && matches!(name.as_str(), "__bind" | "__yield" | "__empty")
        {
            return self.monad_info.get(&head.span).cloned();
        }
        None
    }

    /// Check if an expression is statically known to produce a relation,
    /// beyond the simple pattern match in compile_do.
    fn expr_is_known_relation(&self, expr: &ast::Expr) -> bool {
        match &expr.node {
            // Pipe into filter/map/take/drop/diff/inter/union always yields a relation
            ast::ExprKind::BinOp { op: ast::BinOp::Pipe, .. } => true,
            // A nested-relation field (`t.members` where `members : [{who: Text}]`).
            // Inference recorded which field accesses are relation-typed; a
            // scalar field (`t.status`) is not in the set, so `InProgress ip <-
            // t.status` keeps its bind-the-value semantics.
            ast::ExprKind::FieldAccess { .. } => self.relation_fields.contains(&expr.span),
            // Application of known relation-returning stdlib functions
            ast::ExprKind::App { func, .. } => {
                if let ast::ExprKind::Var(name) = &func.node {
                    matches!(name.as_str(),
                        "filter" | "map" | "take" | "drop" | "diff" | "inter"
                        | "union" | "reverse" | "chars" | "sort" | "sortBy"
                    )
                } else if let ast::ExprKind::App { func: inner, .. } = &func.node {
                    // Curried: (filter pred) applied to relation
                    if let ast::ExprKind::Var(name) = &inner.node {
                        matches!(name.as_str(),
                            "filter" | "map" | "take" | "drop" | "diff" | "inter"
                            | "union" | "sort" | "sortBy"
                        )
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    // ── Trait dispatch helpers ─────────────────────────────────────

    /// Resolve a trait method occurrence to the mangled name of the impl its
    /// static type selects (`area` at a `Blob` site → `Area_Blob_area`).
    ///
    /// The runtime dispatcher keys on the value's constructor tag, which does
    /// not identify a type: two ADTs may share a constructor name, and the tag
    /// chain then picks whichever impl was registered first. Inference resolves
    /// the trait's parameter to a concrete type at every monomorphic site, so
    /// prefer that impl and never consult the tag.
    ///
    /// Returns `None` — leaving the occurrence on the runtime dispatcher — when
    /// the site is still polymorphic (inside an `Area a => a -> Float` body the
    /// type is genuinely unknown until run time), when the resolved type has no
    /// impl of this trait, or when a user top-level fn shadows the method name
    /// (no dispatcher is built in that case, and that fn must keep winning).
    fn static_impl_name(&self, method: &str, span: Span) -> Option<String> {
        if !self.trait_dispatcher_fns.contains_key(method) {
            return None;
        }
        let trait_name = self.trait_method_traits.get(method)?;
        let type_name =
            self.trait_call_targets.get(&(span, trait_name.clone()))?;
        let info = self.trait_methods.get(method)?;
        if !info.impls.iter().any(|e| &e.type_name == type_name) {
            return None;
        }
        let mangled = format!("{}_{}_{}", trait_name, type_name, method);
        // Every registered impl is also a `user_fns` entry under its mangled
        // name. Requiring the arity to match the dispatcher's keeps the
        // substitution transparent to call sites.
        let (_, n_params) = self.user_fns.get(&mangled).copied()?;
        (n_params == info.param_count).then_some(mangled)
    }

    /// `static_impl_name`, additionally noting occurrences that fall back to
    /// the runtime dispatcher so `check_ambiguous_dynamic_dispatch` can verify
    /// the tag chain is actually able to tell the impls apart.
    fn resolve_trait_call(
        &mut self,
        method: &str,
        span: Span,
    ) -> Option<String> {
        let resolved = self.static_impl_name(method, span);
        if resolved.is_none()
            && self.trait_dispatcher_fns.contains_key(method)
            && self.trait_method_traits.contains_key(method)
        {
            self.dynamic_dispatch_sites
                .push((method.to_string(), span));
        }
        resolved
    }

    /// Reject programs whose runtime trait dispatch cannot pick an impl.
    ///
    /// The dispatcher matches a value's constructor tag against each impl's
    /// constructor set, so two ADTs declaring the same constructor name are
    /// indistinguishable to it. Monomorphic call sites never reach it — they
    /// resolve statically — but a polymorphic one (inside an `Area a => a ->
    /// Float` body, say) has no static type, and the tag chain would silently
    /// run whichever impl was registered first. Report that instead of
    /// miscompiling it; the call needs a concrete type to dispatch on.
    fn check_ambiguous_dynamic_dispatch(&mut self) {
        let sites = std::mem::take(&mut self.dynamic_dispatch_sites);
        let mut reported: HashSet<String> = HashSet::new();
        let mut diags = Vec::new();
        for (method, span) in sites {
            if !reported.insert(method.clone()) {
                continue;
            }
            let Some(info) = self.trait_methods.get(&method) else {
                continue;
            };
            // Constructor name → the impl types that declare it.
            let mut owners: HashMap<&str, Vec<&str>> = HashMap::new();
            for e in &info.impls {
                let Some(ctors) = self.data_constructors.get(&e.type_name)
                else {
                    continue;
                };
                for c in ctors {
                    owners
                        .entry(c.as_str())
                        .or_default()
                        .push(e.type_name.as_str());
                }
            }
            let mut clashes: Vec<(&str, Vec<&str>)> = owners
                .into_iter()
                .filter(|(_, types)| types.len() > 1)
                .collect();
            clashes.sort();

            // Also check for multiple nullable ADT impls: their null (none)
            // variants are bare null pointers with no tag to distinguish them,
            // so dispatching a null value to the correct impl is impossible.
            // The constructor-name overlap check above does NOT catch this
            // because different nullable ADTs have different constructor names
            // (e.g. `NothingA` vs `NothingB`).
            let nullable_impl_types: Vec<&str> = info
                .impls
                .iter()
                .filter_map(|e| {
                    let ctors = self.data_constructors.get(&e.type_name)?;
                    let is_nullable = ctors
                        .iter()
                        .any(|c| self.nullable_ctors.contains_key(c));
                    if is_nullable { Some(e.type_name.as_str()) } else { None }
                })
                .collect();
            if nullable_impl_types.len() > 1 {
                let trait_name = self
                    .trait_method_traits
                    .get(&method)
                    .cloned()
                    .unwrap_or_default();
                diags.push(
                    knot::diagnostic::Diagnostic::error(format!(
                        "cannot dispatch '{}' at run time: multiple nullable ADT types \
                         ({}) implement '{}', and their null values are indistinguishable",
                        method,
                        nullable_impl_types
                            .iter()
                            .map(|t| format!("'{}'", t))
                            .collect::<Vec<_>>()
                            .join(" and "),
                        trait_name,
                    ))
                    .label(
                        span,
                        format!(
                            "this call is polymorphic, so '{}' has no static type here",
                            method
                        ),
                    ),
                );
                continue;
            }

            let Some((ctor, types)) = clashes.first() else {
                continue;
            };
            let trait_name = self
                .trait_method_traits
                .get(&method)
                .cloned()
                .unwrap_or_default();
            diags.push(
                knot::diagnostic::Diagnostic::error(format!(
                    "cannot dispatch '{}' at run time: constructor '{}' is \
                     declared by {}, which all implement '{}', so the value's \
                     tag does not identify which impl to run",
                    method,
                    ctor,
                    types
                        .iter()
                        .map(|t| format!("'{}'", t))
                        .collect::<Vec<_>>()
                        .join(" and "),
                    trait_name,
                ))
                .label(
                    span,
                    format!(
                        "this call is polymorphic, so '{}' has no static type here",
                        method
                    ),
                ),
            );
        }
        self.diagnostics.extend(diags);
    }

    // ── Operator trait dispatch helpers ────────────────────────────

    /// Check if a trait method has any non-builtin implementation (a user or
    /// prelude `impl`, on any type — ADTs *or* primitives like Int/Text).
    /// Operators must dispatch through the trait method whenever one exists;
    /// the intrinsic registrations delegate to the same runtime functions as
    /// the operator fast paths, so they alone never require dispatch.
    fn has_user_impls(&self, method: &str) -> bool {
        self.trait_methods.get(method).is_some_and(|info| {
            info.impls.iter().any(|e| !e.is_builtin)
        })
    }

    /// Check if a trait method has a non-builtin implementation on a
    /// primitive type (Int/Float/Text/Bool/…). When true, tag-based operator
    /// fast paths must be skipped entirely — a primitive value would
    /// otherwise bypass the user's impl.
    fn has_user_primitive_impl(&self, method: &str) -> bool {
        self.trait_methods.get(method).is_some_and(|info| {
            info.impls
                .iter()
                .any(|e| !e.is_builtin && type_name_to_tag(&e.type_name).is_some())
        })
    }

    /// SQL pushdown executes comparisons and arithmetic as native SQLite
    /// operations, bypassing operator trait dispatch entirely. When the
    /// user overrides `eq`/`compare` or a `Num` method on a PRIMITIVE type
    /// (the only types stored in SQL columns), the in-memory paths dispatch
    /// through that impl — a pushed-down query would silently use the
    /// built-in semantics instead. Disable pushdown wholesale in that case;
    /// the in-memory fallback is always correct.
    fn sql_pushdown_disabled_by_user_impls(&self) -> bool {
        ["eq", "compare", "add", "sub", "mul", "div", "mod", "negate"]
            .iter()
            .any(|m| self.has_user_primitive_impl(m))
    }

    /// Compile a binary operator via trait dispatch (e.g., `+` → `add` dispatcher).
    /// Falls back to `fallback_rt` if no dispatcher exists (e.g., user redefined the trait).
    /// Skips the dispatcher entirely when only primitive impls exist.
    fn compile_trait_binop(
        &mut self,
        builder: &mut FunctionBuilder,
        method: &str,
        l: Value,
        r: Value,
        db: Value,
        fallback_rt: &str,
    ) -> Value {
        if self.has_user_impls(method)
            && let Some(&func_id) = self.trait_dispatcher_fns.get(method) {
                let func_ref = self.module.declare_func_in_func(func_id, builder.func);
                let call = builder.ins().call(func_ref, &[db, l, r]);
                return builder.inst_results(call)[0];
            }
        self.call_rt(builder, fallback_rt, &[l, r])
    }

    /// Compile a unary operator via trait dispatch (e.g., `-x` → `negate` dispatcher).
    /// Skips the dispatcher entirely when only primitive impls exist.
    fn compile_trait_unop(
        &mut self,
        builder: &mut FunctionBuilder,
        method: &str,
        val: Value,
        db: Value,
        fallback_rt: &str,
    ) -> Value {
        if self.has_user_impls(method)
            && let Some(&func_id) = self.trait_dispatcher_fns.get(method) {
                let func_ref = self.module.declare_func_in_func(func_id, builder.func);
                let call = builder.ins().call(func_ref, &[db, val]);
                return builder.inst_results(call)[0];
            }
        self.call_rt(builder, fallback_rt, &[val])
    }

    /// Compile a comparison operator.
    /// When no custom Ord trait impls exist for ADTs, uses direct runtime comparison
    /// functions (`knot_value_lt` etc.) which return Bool in a single call.
    /// When a `compare` dispatcher exists (ADT Ord impls), calls the dispatcher
    /// to get an Ordering value and checks its constructor tag.
    /// `match_tag` is the Ordering constructor to check ("LT" or "GT").
    /// `negate` inverts the result (for `<=` and `>=`).
    fn compile_comparison(
        &mut self,
        builder: &mut FunctionBuilder,
        l: Value,
        r: Value,
        db: Value,
        match_tag: &str,
        negate: bool,
    ) -> Value {
        // Check if any non-primitive Ord impls exist (ADT types)
        let has_adt_ord_impls = self.has_user_impls("compare");

        if has_adt_ord_impls {
            // ADT path: compute i32 result via compile_comparison_i32, then box
            let result_i32 = self.compile_comparison_i32(builder, l, r, db, match_tag, negate);
            self.call_rt(builder, "knot_value_bool", &[result_i32])
        } else {
            // No ADT Ord impls — use direct runtime comparison (1 call, 1 alloc)
            let rt_fn = match (match_tag, negate) {
                ("LT", false) => "knot_value_lt",
                ("GT", false) => "knot_value_gt",
                ("GT", true) => "knot_value_le",
                ("LT", true) => "knot_value_ge",
                _ => unreachable!(),
            };
            self.call_rt(builder, rt_fn, &[l, r])
        }
    }

    /// Compile a comparison to unboxed i32 (0/1) — avoids Bool allocation.
    /// Used by `compile_condition` when the result feeds directly into a branch.
    fn compile_comparison_i32(
        &mut self,
        builder: &mut FunctionBuilder,
        l: Value,
        r: Value,
        db: Value,
        match_tag: &str,
        negate: bool,
    ) -> Value {
        let has_adt_ord_impls = self.has_user_impls("compare");

        if has_adt_ord_impls {
            if let Some(&func_id) = self.trait_dispatcher_fns.get("compare") {
                let func_ref = self.module.declare_func_in_func(func_id, builder.func);
                let call = builder.ins().call(func_ref, &[db, l, r]);
                let ordering = builder.inst_results(call)[0];

                // Use integer tag (0=LT, 1=EQ, 2=GT) instead of string comparison
                let ord_i32 = self.call_rt_typed(
                    builder,
                    "knot_ordering_tag_i32",
                    &[ordering],
                    types::I32,
                );
                let expected = match match_tag {
                    "LT" => 0i64,
                    "EQ" => 1,
                    "GT" => 2,
                    _ => unreachable!(),
                };
                let matches = builder.ins().icmp_imm(IntCC::Equal, ord_i32, expected);
                let result_i32 = builder.ins().uextend(types::I32, matches);

                if negate {
                    let one = builder.ins().iconst(types::I32, 1);
                    builder.ins().isub(one, result_i32)
                } else {
                    result_i32
                }
            } else {
                let rt_fn = match (match_tag, negate) {
                    ("LT", false) => "knot_value_lt_i32",
                    ("GT", false) => "knot_value_gt_i32",
                    ("GT", true) => "knot_value_le_i32",
                    ("LT", true) => "knot_value_ge_i32",
                    _ => unreachable!(),
                };
                self.call_rt_typed(builder, rt_fn, &[l, r], types::I32)
            }
        } else {
            let rt_fn = match (match_tag, negate) {
                ("LT", false) => "knot_value_lt_i32",
                ("GT", false) => "knot_value_gt_i32",
                ("GT", true) => "knot_value_le_i32",
                ("LT", true) => "knot_value_ge_i32",
                _ => unreachable!(),
            };
            self.call_rt_typed(builder, rt_fn, &[l, r], types::I32)
        }
    }

    /// Compile an expression used as a boolean condition directly to i32 (0/1),
    /// avoiding the Bool box/unbox round-trip when possible.
    /// Falls back to compile_expr + knot_value_get_bool for non-optimizable cases.
    fn compile_condition(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        match &expr.node {
            ast::ExprKind::BinOp { op, lhs, rhs } => {
                match op {
                    // Equality — unboxed; fast path for non-constructor types
                    ast::BinOp::Eq => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        if self.has_user_primitive_impl("eq") {
                            // A user impl of Eq on a primitive type exists —
                            // the tag-based fast path below would bypass it,
                            // so every == goes through trait dispatch.
                            let boxed = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            self.call_rt_typed(builder, "knot_value_get_bool", &[boxed], types::I32)
                        } else if self.trait_dispatcher_fns.contains_key("eq") {
                            // Fast path: non-constructor types use direct unboxed comparison
                            let tag = self.call_rt_typed(builder, "knot_value_get_tag", &[l], types::I32);
                            let is_non_ctor = builder.ins().icmp_imm(IntCC::NotEqual, tag, 7);

                            let fast_block = builder.create_block();
                            let dispatch_block = builder.create_block();
                            let merge_block = builder.create_block();
                            merge_block_param(builder, merge_block, types::I32);

                            builder.ins().brif(is_non_ctor, fast_block, &[], dispatch_block, &[]);

                            builder.switch_to_block(fast_block);
                            builder.seal_block(fast_block);
                            let eq_i32 = self.call_rt_typed(builder, "knot_value_eq_i32", &[l, r], types::I32);
                            builder.ins().jump(merge_block, &[eq_i32.into()]);

                            builder.switch_to_block(dispatch_block);
                            builder.seal_block(dispatch_block);
                            let boxed = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            let unboxed = self.call_rt_typed(builder, "knot_value_get_bool", &[boxed], types::I32);
                            builder.ins().jump(merge_block, &[unboxed.into()]);

                            builder.switch_to_block(merge_block);
                            builder.seal_block(merge_block);
                            builder.block_params(merge_block)[0]
                        } else {
                            self.call_rt_typed(builder, "knot_value_eq_i32", &[l, r], types::I32)
                        }
                    }
                    ast::BinOp::Neq => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        if self.has_user_primitive_impl("eq") {
                            // See == above: user Eq impl on a primitive type
                            // disables the tag-based fast path.
                            let boxed = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            let eq_i32 = self.call_rt_typed(builder, "knot_value_get_bool", &[boxed], types::I32);
                            let one = builder.ins().iconst(types::I32, 1);
                            builder.ins().isub(one, eq_i32)
                        } else if self.trait_dispatcher_fns.contains_key("eq") {
                            // Fast path: non-constructor types use direct unboxed comparison
                            let tag = self.call_rt_typed(builder, "knot_value_get_tag", &[l], types::I32);
                            let is_non_ctor = builder.ins().icmp_imm(IntCC::NotEqual, tag, 7);

                            let fast_block = builder.create_block();
                            let dispatch_block = builder.create_block();
                            let merge_block = builder.create_block();
                            merge_block_param(builder, merge_block, types::I32);

                            builder.ins().brif(is_non_ctor, fast_block, &[], dispatch_block, &[]);

                            builder.switch_to_block(fast_block);
                            builder.seal_block(fast_block);
                            let neq_i32 = self.call_rt_typed(builder, "knot_value_neq_i32", &[l, r], types::I32);
                            builder.ins().jump(merge_block, &[neq_i32.into()]);

                            builder.switch_to_block(dispatch_block);
                            builder.seal_block(dispatch_block);
                            let boxed = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            let eq_i32 = self.call_rt_typed(builder, "knot_value_get_bool", &[boxed], types::I32);
                            // Negate: eq result → neq result
                            let one = builder.ins().iconst(types::I32, 1);
                            let neq_result = builder.ins().isub(one, eq_i32);
                            builder.ins().jump(merge_block, &[neq_result.into()]);

                            builder.switch_to_block(merge_block);
                            builder.seal_block(merge_block);
                            builder.block_params(merge_block)[0]
                        } else {
                            self.call_rt_typed(builder, "knot_value_neq_i32", &[l, r], types::I32)
                        }
                    }
                    // Comparisons — unboxed
                    ast::BinOp::Lt => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        self.compile_comparison_i32(builder, l, r, db, "LT", false)
                    }
                    ast::BinOp::Gt => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        self.compile_comparison_i32(builder, l, r, db, "GT", false)
                    }
                    ast::BinOp::Le => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        self.compile_comparison_i32(builder, l, r, db, "GT", true)
                    }
                    ast::BinOp::Ge => {
                        let l = self.compile_expr(builder, lhs, env, db);
                        let r = self.compile_expr(builder, rhs, env, db);
                        self.compile_comparison_i32(builder, l, r, db, "LT", true)
                    }
                    // Short-circuit &&
                    ast::BinOp::And => {
                        let l_i32 = self.compile_condition(builder, lhs, env, db);
                        let l_true = builder.ins().icmp_imm(IntCC::NotEqual, l_i32, 0);

                        let rhs_block = builder.create_block();
                        let merge_block = builder.create_block();
                        merge_block_param(builder, merge_block, types::I32);

                        let zero = builder.ins().iconst(types::I32, 0);
                        builder.ins().brif(l_true, rhs_block, &[], merge_block, &[zero.into()]);

                        builder.switch_to_block(rhs_block);
                        builder.seal_block(rhs_block);
                        let r_i32 = self.compile_condition(builder, rhs, env, db);
                        builder.ins().jump(merge_block, &[r_i32.into()]);

                        builder.switch_to_block(merge_block);
                        builder.seal_block(merge_block);
                        builder.block_params(merge_block)[0]
                    }
                    // Short-circuit ||
                    ast::BinOp::Or => {
                        let l_i32 = self.compile_condition(builder, lhs, env, db);
                        let l_true = builder.ins().icmp_imm(IntCC::NotEqual, l_i32, 0);

                        let rhs_block = builder.create_block();
                        let merge_block = builder.create_block();
                        merge_block_param(builder, merge_block, types::I32);

                        let one = builder.ins().iconst(types::I32, 1);
                        builder.ins().brif(l_true, merge_block, &[one.into()], rhs_block, &[]);

                        builder.switch_to_block(rhs_block);
                        builder.seal_block(rhs_block);
                        let r_i32 = self.compile_condition(builder, rhs, env, db);
                        builder.ins().jump(merge_block, &[r_i32.into()]);

                        builder.switch_to_block(merge_block);
                        builder.seal_block(merge_block);
                        builder.block_params(merge_block)[0]
                    }
                    // Other binary ops — fall back
                    _ => {
                        let val = self.compile_expr(builder, expr, env, db);
                        self.call_rt_typed(builder, "knot_value_get_bool", &[val], types::I32)
                    }
                }
            }
            ast::ExprKind::UnaryOp { op: ast::UnaryOp::Not, operand } => {
                let inner = self.compile_condition(builder, operand, env, db);
                let one = builder.ins().iconst(types::I32, 1);
                builder.ins().isub(one, inner)
            }
            // Bool literal: return constant i32 directly, no allocation
            ast::ExprKind::Lit(ast::Literal::Bool(b)) => {
                builder.ins().iconst(types::I32, *b as i64)
            }
            // Fall back: compile as boxed Value, then unbox
            _ => {
                let val = self.compile_expr(builder, expr, env, db);
                self.call_rt_typed(builder, "knot_value_get_bool", &[val], types::I32)
            }
        }
    }
}

// ── View analysis ─────────────────────────────────────────────────

/// Analyze a view body expression to extract column provenance.
/// Returns `None` if the view body cannot be analyzed (unsupported pattern).
/// Analyze a view body. Returns `Ok(Some(info))` when the body is a
/// recognizable view, `Ok(None)` when it isn't a view shape at all, and
/// `Err((span, msg))` when it *is* a view but contains a `where` filter we
/// cannot represent — the caller turns that into a diagnostic. We must not
/// silently drop such a filter (that would return unfiltered rows on read and
/// write rows that violate the filter), and we must not panic on valid input.
fn analyze_view(body: &ast::Expr) -> Result<Option<ViewInfo>, (ast::Span, String)> {
    // Case 1: simple alias — *view = *source
    if let ast::ExprKind::SourceRef(source_name) = &body.node {
        return Ok(Some(ViewInfo {
            source_name: source_name.clone(),
            source_columns: vec![],
            constant_columns: vec![],
            body: body.clone(),
        }));
    }

    // Case 2: do-block with bind + yield
    if let ast::ExprKind::Do(stmts) = &body.node {
        // Find the bind statement: t <- *source
        let bind_info = stmts.iter().find_map(|s| {
            if let ast::StmtKind::Bind { pat, expr } = &s.node
                && let ast::ExprKind::SourceRef(source_name) = &expr.node
                    && let ast::PatKind::Var(var_name) = &pat.node {
                        return Some((var_name.clone(), source_name.clone()));
                    }
            None
        });
        let bind_info = match bind_info {
            Some(bi) => bi,
            None => return Ok(None),
        };

        let (bind_var, source_name) = bind_info;

        // Find the yield expression with a record
        let yield_record = stmts.iter().rev().find_map(|s| {
            if let ast::StmtKind::Expr(expr) = &s.node
                && let Some(inner) = expr.node.as_yield_arg()
                    && let ast::ExprKind::Record(fields) = &inner.node {
                        return Some(fields.clone());
                    }
            None
        });
        let yield_record = match yield_record {
            Some(yr) => yr,
            None => return Ok(None),
        };

        let mut source_columns = Vec::new();
        let mut constant_columns = Vec::new();

        // Extract `where <bindvar>.<col> == <const>` filter statements. Such a
        // filter restricts the view (read side) and implies a constant column
        // to auto-fill on write, exactly like a constant yield field — so we
        // record it as a constant column keyed by the *source* column name.
        // Forms we cannot reduce to `col == const` (e.g. inequalities, computed
        // predicates) make the view unanalyzable for writes; bail out so we
        // never silently drop a filter.
        for s in stmts {
            if let ast::StmtKind::Where { cond } = &s.node {
                if let ast::ExprKind::BinOp {
                    op: ast::BinOp::Eq,
                    lhs,
                    rhs,
                } = &cond.node
                {
                    // Identify which side is `bindvar.col` and which is the
                    // constant (must not reference the bind var).
                    let field_of = |e: &ast::Expr| -> Option<String> {
                        if let ast::ExprKind::FieldAccess { expr, field } = &e.node
                            && let ast::ExprKind::Var(v) = &expr.node
                                && v == &bind_var {
                                    return Some(field.clone());
                                }
                        None
                    };
                    let lhs_col = field_of(lhs);
                    let rhs_col = field_of(rhs);
                    match (lhs_col, rhs_col) {
                        (Some(col), None) if !expr_references_var(rhs, &bind_var) => {
                            constant_columns.push((col, (**rhs).clone()));
                            continue;
                        }
                        (None, Some(col)) if !expr_references_var(lhs, &bind_var) => {
                            constant_columns.push((col, (**lhs).clone()));
                            continue;
                        }
                        _ => {}
                    }
                }
                // Any other `where` form can't be represented in ViewInfo's
                // constant-column model. Report a diagnostic rather than
                // silently dropping the filter (which would return unfiltered
                // rows on read and write rows that violate the filter) or
                // panicking the whole compile on otherwise-valid input.
                return Err((
                    cond.span,
                    "unsupported `where` filter in view body — view `where` clauses \
                     must have the form `<bindvar>.<field> == <constant>`"
                        .to_string(),
                ));
            }
        }

        for field in &yield_record {
            // Check if it's a field access on the bind var: t.field
            if let ast::ExprKind::FieldAccess {
                expr,
                field: accessed_field,
            } = &field.value.node
                && let ast::ExprKind::Var(var_name) = &expr.node
                    && var_name == &bind_var {
                        source_columns.push((field.name.clone(), accessed_field.clone()));
                        continue;
                    }
            // Check it doesn't reference the bind var (constant column)
            if !expr_references_var(&field.value, &bind_var) {
                constant_columns.push((field.name.clone(), field.value.clone()));
            }
            // If it references bind_var but isn't a simple field access,
            // it's a computed column — view reads work, writes are not supported.
        }

        return Ok(Some(ViewInfo {
            source_name,
            source_columns,
            constant_columns,
            body: body.clone(),
        }));
    }

    Ok(None)
}

/// Check if an expression references a specific variable name.
/// Check if an expression contains function applications to user-defined
/// Escape-analysis hint: returns true if `expr` trivially evaluates to a
/// value that does not need `knot_arena_promote` to survive an
/// enclosing `reset_to`.
///
/// The runtime pre-allocates singletons for small ints (`-128..=127`),
/// `Bool(true)` / `Bool(false)`, `Unit`, and `Float(0.0)` / `Float(1.0)`.
/// Pointers returned for these values are owned by the thread-local
/// `SINGLETONS` table, not by the current frame's chunks — so they're
/// already safe.  Emitting a `knot_arena_promote` call for them just
/// burns cycles inside the runtime's `owns_in_chunks` check and cache
/// lookup; skipping the call when the expression is syntactically
/// guaranteed to produce a singleton avoids that work.
///
/// This is a deliberately conservative analysis: false negatives are
/// fine (we just emit a redundant promote call), false positives are
/// a memory-safety bug.
fn expr_is_promote_safe(expr: &ast::Expr) -> bool {
    match &expr.node {
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(s) => {
                s.parse::<i64>().is_ok_and(|n| (-128..=127).contains(&n))
            }
            ast::Literal::Bool(_) => true,
            ast::Literal::Float(f) => {
                f.to_bits() == 0.0_f64.to_bits() || *f == 1.0
            }
            // Text/Bytes are freshly allocated on each evaluation and
            // need promotion.  (Text literals can be cached, but the
            // compiler doesn't currently emit the cached path for
            // yield-position literals.)
            ast::Literal::Text(_) | ast::Literal::Bytes(_) => false,
        },
        // Bare `True` and `False` constructors compile to the Bool
        // singletons (see codegen's special-case for Constructor("True"
        // | "False", ...)).  Same for `Unit`-producing nullary
        // constructors via the runtime's shared `Unit` singleton.
        //
        // NOTE: we can't easily distinguish "plain Unit constructor"
        // from a user-defined nullary constructor with the same name
        // without resolving through the type environment.  Stick to
        // the bare primitive names which codegen special-cases.
        ast::ExprKind::Constructor(name) => {
            matches!(name.as_str(), "True" | "False")
        }
        // `if cond then t else e` produces whichever branch ran.  If
        // both branches are promote-safe, the result is too.  Note we
        // don't inspect `cond` — it's evaluated but its result doesn't
        // reach the yield position; only the selected branch does.
        ast::ExprKind::If { then_branch, else_branch, .. } => {
            expr_is_promote_safe(then_branch) && expr_is_promote_safe(else_branch)
        }
        // Case dispatch: if every arm's body is promote-safe, so is
        // the result.  An empty arm list shouldn't occur (ill-typed),
        // but we conservatively return false to avoid false positives.
        ast::ExprKind::Case { arms, .. } => {
            !arms.is_empty() && arms.iter().all(|a| expr_is_promote_safe(&a.body))
        }
        _ => false,
    }
}

/// functions (not builtins/runtime). Such calls may produce significant
/// intermediate arena allocations that benefit from frame isolation.
fn expr_has_user_calls(expr: &ast::Expr, user_fns: &HashMap<String, (FuncId, usize)>) -> bool {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            // Check if the function head is a user-defined function
            let head_is_user_fn = match &func.node {
                ast::ExprKind::Var(name) => user_fns.contains_key(name.as_str()),
                // Curried application: f x y → App(App(Var("f"), x), y)
                ast::ExprKind::App { .. } => expr_has_user_calls(func, user_fns),
                _ => false,
            };
            head_is_user_fn
                || expr_has_user_calls(func, user_fns)
                || expr_has_user_calls(arg, user_fns)
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_has_user_calls(lhs, user_fns) || expr_has_user_calls(rhs, user_fns)
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_has_user_calls(operand, user_fns),
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            expr_has_user_calls(cond, user_fns)
                || expr_has_user_calls(then_branch, user_fns)
                || expr_has_user_calls(else_branch, user_fns)
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            expr_has_user_calls(scrutinee, user_fns)
                || arms.iter().any(|a| expr_has_user_calls(&a.body, user_fns))
        }
        ast::ExprKind::Record(fields) => {
            fields.iter().any(|f| expr_has_user_calls(&f.value, user_fns))
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            expr_has_user_calls(base, user_fns)
                || fields.iter().any(|f| expr_has_user_calls(&f.value, user_fns))
        }
        ast::ExprKind::FieldAccess { expr, .. } => expr_has_user_calls(expr, user_fns),
        ast::ExprKind::Lambda { body, .. } => expr_has_user_calls(body, user_fns),
        ast::ExprKind::Annot { expr, .. } => expr_has_user_calls(expr, user_fns),
        ast::ExprKind::TimeUnitLit { value, .. } => expr_has_user_calls(value, user_fns),
        ast::ExprKind::Refine(inner) => expr_has_user_calls(inner, user_fns),
        ast::ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
            ast::StmtKind::Bind { expr, .. } => expr_has_user_calls(expr, user_fns),
            ast::StmtKind::Let { expr, .. } => expr_has_user_calls(expr, user_fns),
            ast::StmtKind::Where { cond } => expr_has_user_calls(cond, user_fns),
            ast::StmtKind::GroupBy { key } => expr_has_user_calls(key, user_fns),
            ast::StmtKind::Expr(e) => expr_has_user_calls(e, user_fns),
        }),
        // Leaves: no function calls
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Var(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_)
        | ast::ExprKind::List(_) => false,
        // Conservative: treat complex nodes as potentially having user calls
        _ => true,
    }
}

fn expr_references_var(expr: &ast::Expr, var_name: &str) -> bool {
    match &expr.node {
        ast::ExprKind::Var(name) => name == var_name,
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_) => false,
        ast::ExprKind::Record(fields) => fields
            .iter()
            .any(|f| expr_references_var(&f.value, var_name)),
        ast::ExprKind::App { func, arg } => {
            expr_references_var(func, var_name) || expr_references_var(arg, var_name)
        }
        ast::ExprKind::FieldAccess { expr, .. } => expr_references_var(expr, var_name),
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_references_var(lhs, var_name) || expr_references_var(rhs, var_name)
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_references_var(operand, var_name),
        ast::ExprKind::Lambda { body, params, .. } => {
            // If the lambda rebinds the var, don't look inside
            let rebinds = params
                .iter()
                .any(|p| matches!(&p.node, ast::PatKind::Var(n) if n == var_name));
            !rebinds && expr_references_var(body, var_name)
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            expr_references_var(base, var_name)
                || fields
                    .iter()
                    .any(|f| expr_references_var(&f.value, var_name))
        }
        ast::ExprKind::TimeUnitLit { value, .. } => expr_references_var(value, var_name),
        ast::ExprKind::Annot { expr, .. } => expr_references_var(expr, var_name),
        ast::ExprKind::Refine(inner) => expr_references_var(inner, var_name),
        // Conservatively return true for complex expressions
        _ => true,
    }
}

// ── SQL compilation types ─────────────────────────────────────────

/// Map a 2-arg projection-based aggregate name (`sum`/`avg`/`minimum`/`maximum`)
/// to its SQL aggregate function and the runtime function used to read the result.
fn aggregate_sql_func_runtime(name: &str) -> Option<(&'static str, &'static str)> {
    match name {
        "sum" => Some(("SUM", "knot_source_query_sum")),
        "avg" => Some(("AVG", "knot_source_query_float")),
        "minOn" => Some(("MIN", "knot_source_query_value")),
        "maxOn" => Some(("MAX", "knot_source_query_value")),
        _ => None,
    }
}

/// minOn/maxOn pushdown is sound for Int and Text projections only.
/// `knot_source_query_value` re-parses TEXT results as Int when its
/// `is_text` flag is unset (Knot Ints are stored as TEXT in SQLite); for
/// genuine Text columns the compiler sets `is_text` (see
/// `minmax_result_is_text`) so the value is returned verbatim, and SQLite's
/// default BINARY collation orders Text byte-wise, matching Knot's
/// `str`-based Text ordering. A `tag` (all-nullary ADT) column must NOT push
/// down: SQLite would compare its constructor names alphabetically and return
/// a bare Text, whereas Knot's derived `Ord` compares by declaration order and
/// expects a reconstructed `Constructor` — both diverge, so tags fall back to
/// in-memory evaluation. Float ordering is handled by the per-path filters.
pub(crate) fn minmax_pushdown_type_ok(
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> bool {
    matches!(
        infer_sql_expr_type(bind_var, body, schema).as_deref(),
        Some("int") | Some("text")
    ) && int_case_projection_pushable(bind_var, body, schema)
}

/// Whether the result of a `minOn`/`maxOn` pushdown over `body` is a genuine
/// Knot `Text` column (stored as SQLite TEXT). When true the compiler passes
/// `is_text = 1` to `knot_source_query_value` so the runtime returns the value
/// as `Text` instead of parsing numeric-looking strings back to `Int`/`Float`.
pub(crate) fn minmax_result_is_text(
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> bool {
    matches!(
        infer_sql_expr_type(bind_var, body, schema).as_deref(),
        Some("text")
    )
}

/// Whether a `sum` pushdown projects a Float column. Passed to
/// `knot_source_query_sum` as `is_float` so that an empty/all-NULL Float
/// column yields `Float 0.0` instead of `Int 0`, preserving the statically
/// expected numeric type for downstream float arithmetic / `show` / JSON.
pub(crate) fn sum_result_is_float(
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> bool {
    matches!(
        infer_sql_expr_type(bind_var, body, schema).as_deref(),
        Some("float")
    )
}

/// Whether a `sortBy` projection is safe to push to SQL `ORDER BY`: not an
/// Int-typed CASE (collation loss, see `int_case_projection_pushable`) and not
/// Float/tag/bool. Float sort keys fall back to the faithful in-memory sort
/// because SQLite orders floats differently from Knot's `total_cmp` (NaN sorts
/// as NULL and -0.0/+0.0 are conflated). `tag` and `bool` keys likewise fall
/// back: SQLite would order them by their TEXT/`1`/`0` storage, which does not
/// match the declared `Ord` for those types — the same reason
/// `minmax_pushdown_type_ok` and `try_compile_sql_comparison` treat tag/bool
/// ordering as unsound and keep it in memory.
pub(crate) fn sortby_projection_pushable(
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> bool {
    int_case_projection_pushable(bind_var, body, schema)
        && !matches!(
            infer_sql_expr_type(bind_var, body, schema).as_deref(),
            Some("float") | Some("tag") | Some("bool")
        )
}

/// True when an expression tree contains an if/then/else node (which
/// compiles to a SQL CASE WHEN when pushed down).
fn expr_contains_if(expr: &ast::Expr) -> bool {
    match &expr.node {
        ast::ExprKind::If { .. } => true,
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_contains_if(lhs) || expr_contains_if(rhs)
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_contains_if(operand),
        ast::ExprKind::App { func, arg } => {
            expr_contains_if(func) || expr_contains_if(arg)
        }
        ast::ExprKind::FieldAccess { expr: inner, .. } => expr_contains_if(inner),
        ast::ExprKind::Annot { expr: inner, .. } => expr_contains_if(inner),
        ast::ExprKind::TimeUnitLit { value, .. } => expr_contains_if(value),
        _ => false,
    }
}

/// MIN/MAX and ORDER BY over a CASE expression must not push down for
/// Int-typed projections: SQLite does not propagate the KNOT_INT column
/// collation through CASE (so values compare byte-wise as TEXT), and CASE
/// branches mixing TEXT-stored Int columns with INTEGER literals compare
/// by storage class rather than value. In-memory evaluation is the
/// faithful fallback. Float (REAL storage) and Text projections are typed
/// out elsewhere or compare consistently.
pub(crate) fn int_case_projection_pushable(
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> bool {
    !(expr_contains_if(body)
        && infer_sql_expr_type(bind_var, body, schema).as_deref() == Some("int"))
}

/// Wrap a column SQL expression for use inside MIN/MAX so integer-typed
/// columns sort numerically rather than lexicographically. Knot stores
/// `Int` columns as `TEXT COLLATE KNOT_INT`, but SQLite does not propagate
/// column collation through MIN/MAX, so we add `COLLATE KNOT_INT`
/// explicitly when the projection is a simple Int field access. For
/// Float/Text columns and arithmetic expressions, the expression is
/// returned unchanged.
fn col_sql_for_minmax(
    col_sql: &str,
    bind_var: &str,
    body: &ast::Expr,
    schema: &str,
) -> String {
    if let ast::ExprKind::FieldAccess { expr, field } = &body.node
        && let ast::ExprKind::Var(name) = &expr.node
            && name == bind_var
                && let Some(ty) = lookup_col_type_from_schema(schema, field)
                    && ty == "int" {
                        return format!("{} COLLATE KNOT_INT", col_sql);
                    }
    col_sql.to_string()
}

/// Escape a SQL identifier by wrapping in double quotes, doubling internal `"`.
fn quote_sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// AND-join independently-generated boolean SQL fragments, parenthesizing
/// each one first. Without the parens a top-level `OR` inside one fragment
/// would bind across the `AND` (SQLite precedence: AND binds tighter than
/// OR), silently matching the wrong rows.
fn join_sql_conditions(conditions: &[String]) -> String {
    conditions
        .iter()
        .map(|c| format!("({})", c))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// A field-level predicate extracted from a filter body for STM row tracking.
/// Used by `try_extract_field_preds` to produce inputs for the runtime
/// `knot_stm_track_read_pred` ABI.
#[derive(Clone)]
struct StmFieldPred {
    col: String,
    op: StmCmpOp,
    /// Re-compilable value sources. Restricted to Literal/Var/FieldAccess in
    /// the extractor so re-evaluation alongside the SQL query is cheap.
    values: Vec<SqlParamSource>,
}

#[derive(Clone, Copy)]
enum StmCmpOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
}

impl StmCmpOp {
    fn as_spec(&self) -> &'static str {
        match self {
            StmCmpOp::Eq => "=",
            StmCmpOp::Neq => "!=",
            StmCmpOp::Lt => "<",
            StmCmpOp::Le => "<=",
            StmCmpOp::Gt => ">",
            StmCmpOp::Ge => ">=",
            StmCmpOp::In => "in",
        }
    }
}

/// Walk a filter body and produce per-column predicates suitable for the
/// runtime `Cols` filter. The body must be a conjunction of supported forms:
/// `bind_var.col {==|!=|<|<=|>|>=} simple_value`, `simple_value op bind_var.col`,
/// or `elem bind_var.col [simple_value, ...]`. `simple_value` is Literal / Var /
/// FieldAccess-on-another-var (cheap to re-evaluate as a tracking param).
/// Returns `None` if the body contains any OR/NOT/arithmetic/function call —
/// the caller falls back to the broad `All` filter in that case.
fn try_extract_field_preds(
    bind_var: &str,
    expr: &ast::Expr,
) -> Option<Vec<StmFieldPred>> {
    let mut out: Vec<StmFieldPred> = Vec::new();
    extract_preds_walk(bind_var, expr, &mut out)?;
    if out.is_empty() { None } else { Some(out) }
}

/// Same as `try_extract_field_preds` but for the single-table do-block path:
/// walks all `Where` conditions for a do-block whose single `Bind` names
/// `bind_var`. Other statement kinds (extra binds, lets, groupBy, …) cause
/// `None`.
fn try_extract_field_preds_from_where_stmts(
    bind_var: &str,
    stmts: &[ast::Stmt],
) -> Option<Vec<StmFieldPred>> {
    let mut out: Vec<StmFieldPred> = Vec::new();
    let last = stmts.len().checked_sub(1)?;
    for stmt in &stmts[..last] {
        match &stmt.node {
            ast::StmtKind::Bind { .. } => {
                // Single-table only: the caller already verified one bind.
                // Re-encountering one means the plan has joins.
                // Allow only the first bind (the caller's bind_var); reject any further bind.
                // We rely on the caller to invoke us only for single-bind plans.
            }
            ast::StmtKind::Where { cond } => {
                extract_preds_walk(bind_var, cond, &mut out)?;
            }
            ast::StmtKind::Let { .. } => {
                // Let-bound names appear in Where conditions as Var nodes.
                // `simple_value_param` accepts them as `Var` params, but
                // they are NOT in the Cranelift env at the call site (the
                // SQL plan substitutes their defining expressions instead of
                // compiling the lets). `emit_stm_track_pred` detects such
                // unresolvable params and skips the precision upgrade,
                // leaving the broad `All` filter in place.
            }
            _ => return None,
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

fn extract_preds_walk(
    bv: &str,
    e: &ast::Expr,
    out: &mut Vec<StmFieldPred>,
) -> Option<()> {
    match &e.node {
        ast::ExprKind::BinOp { op: ast::BinOp::And, lhs, rhs } => {
            extract_preds_walk(bv, lhs, out)?;
            extract_preds_walk(bv, rhs, out)?;
            Some(())
        }
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let stm_op = match op {
                ast::BinOp::Eq => StmCmpOp::Eq,
                ast::BinOp::Neq => StmCmpOp::Neq,
                ast::BinOp::Lt => StmCmpOp::Lt,
                ast::BinOp::Le => StmCmpOp::Le,
                ast::BinOp::Gt => StmCmpOp::Gt,
                ast::BinOp::Ge => StmCmpOp::Ge,
                _ => return None,
            };
            if let Some(col) = field_access_of(bv, lhs) {
                let v = simple_value_param(bv, rhs)?;
                out.push(StmFieldPred { col, op: stm_op, values: vec![v] });
                return Some(());
            }
            if let Some(col) = field_access_of(bv, rhs) {
                let v = simple_value_param(bv, lhs)?;
                out.push(StmFieldPred { col, op: reverse_stm_op(stm_op), values: vec![v] });
                return Some(());
            }
            None
        }
        // `elem needle haystack` ↦ IN
        ast::ExprKind::App { func: outer, arg: haystack } => {
            if let ast::ExprKind::App { func: inner, arg: needle } = &outer.node
                && let ast::ExprKind::Var(name) = &inner.node
                    && name == "elem" {
                        let col = field_access_of(bv, needle)?;
                        if let ast::ExprKind::List(elems) = &haystack.node {
                            if elems.is_empty() {
                                return None;
                            }
                            let mut values = Vec::with_capacity(elems.len());
                            for el in elems {
                                values.push(simple_value_param(bv, el)?);
                            }
                            out.push(StmFieldPred { col, op: StmCmpOp::In, values });
                            return Some(());
                        }
                    }
            None
        }
        _ => None,
    }
}

fn field_access_of(bv: &str, e: &ast::Expr) -> Option<String> {
    if let ast::ExprKind::FieldAccess { expr, field } = &e.node
        && let ast::ExprKind::Var(name) = &expr.node
            && name == bv {
                return Some(field.clone());
            }
    None
}

/// Accept the cheap-to-re-evaluate value forms: literal, env var, or a field
/// access on a non-bind variable. Rejects expressions involving arithmetic,
/// function calls, or the bind variable itself (would mean the comparison
/// references the same row on both sides — not a constant filter).
fn simple_value_param(bv: &str, e: &ast::Expr) -> Option<SqlParamSource> {
    match &e.node {
        ast::ExprKind::Lit(lit) => Some(SqlParamSource::Literal(lit.clone())),
        ast::ExprKind::Var(name) if name != bv => Some(SqlParamSource::Var(name.clone())),
        ast::ExprKind::FieldAccess { expr, field } => {
            if let ast::ExprKind::Var(name) = &expr.node
                && name != bv {
                    return Some(SqlParamSource::FieldAccess(name.clone(), field.clone()));
                }
            None
        }
        _ => None,
    }
}

fn reverse_stm_op(op: StmCmpOp) -> StmCmpOp {
    match op {
        StmCmpOp::Eq | StmCmpOp::Neq | StmCmpOp::In => op,
        StmCmpOp::Lt => StmCmpOp::Gt,
        StmCmpOp::Le => StmCmpOp::Ge,
        StmCmpOp::Gt => StmCmpOp::Lt,
        StmCmpOp::Ge => StmCmpOp::Le,
    }
}

/// Serialize a list of `StmFieldPred` into the spec string consumed by
/// `knot_stm_track_read_pred`. Indices are assigned as a flat 0..N enumeration
/// over each pred's `values` in order, matching the params relation built from
/// the same flat sequence.
/// Try to extract STM tracking predicates for a single-table do-block plan.
/// Returns `None` for multi-table plans (joins) or when any condition uses a
/// form the extractor can't analyze. Pairs with `analyze_sql_plan` at the
/// call sites that already emit bare `knot_stm_track_read` per table.
fn try_extract_preds_for_single_table_plan(
    stmts: &[ast::Stmt],
    plan: &SqlQueryPlan,
) -> Option<Vec<StmFieldPred>> {
    if plan.tables.len() != 1 {
        return None;
    }
    let last = stmts.len().checked_sub(1)?;
    let mut bind_var: Option<String> = None;
    for stmt in &stmts[..last] {
        if let ast::StmtKind::Bind { pat, .. } = &stmt.node
            && let ast::PatKind::Var(name) = &pat.node {
                if bind_var.is_some() {
                    return None;
                }
                bind_var = Some(name.clone());
            }
    }
    let bv = bind_var?;
    try_extract_field_preds_from_where_stmts(&bv, stmts)
}

fn serialize_stm_preds(preds: &[StmFieldPred]) -> String {
    let mut next_idx: usize = 0;
    let mut parts: Vec<String> = Vec::with_capacity(preds.len());
    for p in preds {
        let n = p.values.len();
        let idxs: Vec<String> = (next_idx..next_idx + n).map(|i| i.to_string()).collect();
        next_idx += n;
        parts.push(format!("{}:{}:{}", p.col, p.op.as_spec(), idxs.join(",")));
    }
    parts.join(";")
}

struct SqlFragment {
    sql: String,
    params: Vec<SqlParamSource>,
}

/// A SQL subquery for one side of a set operation (diff/inter/union).
struct SetOpSubquery {
    sql: String,
    schema: String,
    params: Vec<SqlParamSource>,
    /// Source tables read by this subquery (for STM read tracking).
    tables: Vec<String>,
}

#[derive(Clone)]
enum SqlParamSource {
    Literal(ast::Literal),
    Var(String),
    FieldAccess(String, String), // (var_name, field_name)
    /// Arbitrary expression compiled at runtime.  Used for computed
    /// values like `t - messageMaxAge` in WHERE clauses.
    Expr(ast::Expr),
}

// ── SQL query plan types ────────────────────────────────────────

struct SqlQueryPlan {
    tables: Vec<SqlTable>,
    conditions: Vec<String>,
    params: Vec<SqlParamSource>,
    select_columns: Vec<SqlSelectColumn>,
    order_by: Vec<String>,
    limit: Option<SqlParamSource>,
    offset: Option<SqlParamSource>,
}

struct SqlTable {
    source_name: String,
    alias: String,
}

struct SqlSelectColumn {
    result_field: String,
    alias: String,
    source_col: String,
    type_str: String,
    /// Optional raw SQL expression (e.g. "t0.\"price\" * t0.\"qty\"").
    /// When set, used instead of alias.source_col in SELECT.
    sql_expr: Option<String>,
}

impl SqlQueryPlan {
    fn build_sql(&self) -> String {
        let select = self
            .select_columns
            .iter()
            .map(|c| {
                // Alias every output column to its result field name. Reads are
                // positional (`knot_source_query`), so this is cosmetic for the
                // normal path — but it lets set-op subqueries be reordered by
                // name to align two differently-ordered SELECT lists.
                if let Some(ref sql_expr) = c.sql_expr {
                    format!("{} AS {}", sql_expr, quote_sql_ident(&c.result_field))
                } else {
                    format!(
                        "{}.{} AS {}",
                        c.alias,
                        quote_sql_ident(&c.source_col),
                        quote_sql_ident(&c.result_field)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let from = self
            .tables
            .iter()
            .map(|t| {
                format!(
                    "{} AS {}",
                    quote_sql_ident(&format!("_knot_{}", t.source_name)),
                    t.alias
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = if self.conditions.is_empty() {
            format!("SELECT {} FROM {}", select, from)
        } else {
            let where_clause = join_sql_conditions(&self.conditions);
            format!("SELECT {} FROM {} WHERE {}", select, from, where_clause)
        };

        if !self.order_by.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", self.order_by.join(", ")));
        }

        if self.limit.is_some() || self.offset.is_some() {
            // Clamp a negative limit to 0 (empty) so the SQL `take` matches the
            // in-memory `knot_relation_take`, which clamps negatives to 0.
            // SQLite otherwise reads a negative LIMIT as "no limit" (all rows).
            // The bound param is TEXT (Ints store as TEXT), so CAST first.
            sql.push_str(&format!(
                " LIMIT {}",
                if self.limit.is_some() { "MAX(CAST(? AS INTEGER), 0)" } else { "-1" }
            ));
            if self.offset.is_some() {
                sql.push_str(" OFFSET ?");
            }
        }

        sql
    }

    fn build_result_schema(&self) -> String {
        self.select_columns
            .iter()
            .map(|c| format!("{}:{}", c.result_field, c.type_str))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// True when the plan's yield projection is the identity: every select
/// column is a plain (non-computed) base-table column whose result field
/// name equals the underlying source column.
fn plan_projection_is_identity(plan: &SqlQueryPlan) -> bool {
    plan.select_columns
        .iter()
        .all(|c| c.sql_expr.is_none() && c.result_field == c.source_col)
}

/// Look up `field` in the plan's yield projection. Returns the qualified
/// column SQL (`alias."source_col"`) and the column's schema type when the
/// result field maps to a plain (non-computed) base-table column; `None`
/// for computed projections — callers must fall back to in-memory
/// evaluation in that case.
fn plan_projection_column(plan: &SqlQueryPlan, field: &str) -> Option<(String, String)> {
    let c = plan
        .select_columns
        .iter()
        .find(|c| c.result_field == field)?;
    if c.sql_expr.is_some() {
        return None;
    }
    Some((
        format!("{}.{}", c.alias, quote_sql_ident(&c.source_col)),
        c.type_str.clone(),
    ))
}

/// Rewrite `body`'s field accesses on `bind_var` through the plan's yield
/// projection (result field name → underlying source column name), so that
/// a lambda written against the *projected* rows can be compiled against
/// the *base* table. Returns `None` when a referenced field doesn't map to
/// a plain base-table column (computed projections must fall back).
/// Identity projections return the body unchanged.
fn rewrite_body_through_projection(
    plan: &SqlQueryPlan,
    bind_var: &str,
    body: &ast::Expr,
) -> Option<ast::Expr> {
    if plan_projection_is_identity(plan) {
        return Some(body.clone());
    }

    fn rewrite(plan: &SqlQueryPlan, bind_var: &str, e: &ast::Expr) -> Option<ast::Expr> {
        let mk = |node: ast::ExprKind| ast::Spanned::new(node, e.span);
        match &e.node {
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                if let ast::ExprKind::Var(v) = &inner.node
                    && v == bind_var {
                        let c = plan
                            .select_columns
                            .iter()
                            .find(|c| c.result_field == *field)?;
                        if c.sql_expr.is_some() {
                            return None; // computed column — fall back
                        }
                        return Some(mk(ast::ExprKind::FieldAccess {
                            expr: inner.clone(),
                            field: c.source_col.clone(),
                        }));
                    }
                let new_inner = rewrite(plan, bind_var, inner)?;
                Some(mk(ast::ExprKind::FieldAccess {
                    expr: Box::new(new_inner),
                    field: field.clone(),
                }))
            }
            ast::ExprKind::Lit(_) | ast::ExprKind::Var(_) | ast::ExprKind::Constructor(_) => {
                Some(e.clone())
            }
            ast::ExprKind::BinOp { op, lhs, rhs } => Some(mk(ast::ExprKind::BinOp {
                op: *op,
                lhs: Box::new(rewrite(plan, bind_var, lhs)?),
                rhs: Box::new(rewrite(plan, bind_var, rhs)?),
            })),
            ast::ExprKind::UnaryOp { op, operand } => Some(mk(ast::ExprKind::UnaryOp {
                op: *op,
                operand: Box::new(rewrite(plan, bind_var, operand)?),
            })),
            ast::ExprKind::App { func, arg } => Some(mk(ast::ExprKind::App {
                func: Box::new(rewrite(plan, bind_var, func)?),
                arg: Box::new(rewrite(plan, bind_var, arg)?),
            })),
            ast::ExprKind::If { cond, then_branch, else_branch } => {
                Some(mk(ast::ExprKind::If {
                    cond: Box::new(rewrite(plan, bind_var, cond)?),
                    then_branch: Box::new(rewrite(plan, bind_var, then_branch)?),
                    else_branch: Box::new(rewrite(plan, bind_var, else_branch)?),
                }))
            }
            ast::ExprKind::List(elems) => {
                let new_elems = elems
                    .iter()
                    .map(|el| rewrite(plan, bind_var, el))
                    .collect::<Option<Vec<_>>>()?;
                Some(mk(ast::ExprKind::List(new_elems)))
            }
            // Anything else (case, do, records, ...) isn't SQL-pushable
            // anyway — refuse so callers fall back.
            _ => None,
        }
    }

    rewrite(plan, bind_var, body)
}

pub(crate) fn lookup_col_type_from_schema(schema: &str, col_name: &str) -> Option<String> {
    // ADT-relation schemas (`#Ctor:field=type;field=type|Ctor2:...|Nullary`)
    // describe a wide table where each constructor's fields are columns. The
    // record-schema parser below can't read this shape (it splits on top-level
    // commas and `:`), so it would return `None` for every ADT field — which
    // silently bypasses the float/json/tag pushdown guards in callers, pushing
    // e.g. a float `<` comparison into SQL where -0.0/NaN semantics diverge.
    if let Some(adt) = schema.strip_prefix('#') {
        for ctor in adt.split('|') {
            let fields = match ctor.split_once(':') {
                Some((_name, fields)) => fields,
                None => continue, // nullary constructor — no fields
            };
            for field in fields.split(';') {
                if let Some((name, ty)) = field.split_once('=')
                    && name == col_name {
                        return Some(ty.to_string());
                    }
            }
        }
        return None;
    }
    for part in split_schema_fields(schema) {
        // A field part lacking a `:` (malformed/unexpected) must be skipped,
        // not abort the whole lookup — `?` here would poison every later column.
        let Some(colon) = part.find(':') else { continue };
        let name = &part[..colon];
        let ty = &part[colon + 1..];
        if name == col_name {
            return Some(ty.to_string());
        }
    }
    None
}

fn parse_schema_columns(schema: &str) -> Vec<(String, String)> {
    split_schema_fields(schema)
        .into_iter()
        .filter_map(|part| {
            let colon = part.find(':')?;
            let name = part[..colon].to_string();
            let ty = part[colon + 1..].to_string();
            Some((name, ty))
        })
        .collect()
}

/// Column names of a schema descriptor (bracket-aware).
fn schema_col_names(schema: &str) -> HashSet<String> {
    split_schema_fields(schema)
        .into_iter()
        .map(|part| part.split(':').next().unwrap_or("").to_string())
        .filter(|n| !n.is_empty())
        .collect()
}

/// Split a schema descriptor by commas while respecting `[...]` bracket nesting
/// for nested relation fields (e.g. `name:text,items:[price:int,qty:int]`).
pub(crate) fn split_schema_fields(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

// ── Pipe chain analysis ───────────────────────────────────────────


/// Check that a pipe chain's operations appear in an order that is
/// equivalent to a single SQL query's fixed clause order
/// (WHERE -> SELECT -> ORDER BY -> LIMIT/OFFSET).
///
/// Accepted (non-decreasing) stage order:
///   filter* -> sortBy? -> map? -> drop? -> take? -> terminal aggregate?
///
/// Notes:
/// - `drop M |> take N` maps to `LIMIT N OFFSET M` exactly; the reverse
///   (`take N |> drop M`) does NOT (it means elements [M..N)), so `drop`
///   gets a lower stage than `take`.
/// - filter/sortBy after take/drop apply AFTER the truncation in-memory
///   but would apply BEFORE the LIMIT in SQL — rejected.
/// - filter/sortBy after map would resolve fields against the base schema
///   instead of the mapped projection — rejected.
/// - at most one sortBy: `sortBy f |> sortBy g` re-sorts by g in-memory,
///   while `ORDER BY f, g` sorts primarily by f — rejected.
/// - aggregates/count are only valid as the final op (enforced by the
///   per-op guards in try_compile_pipe_sql) and never after take/drop
///   (their stage is highest, but the LIMIT would be silently dropped —
///   also re-checked at SQL-build time).
fn pipe_ops_order_pushable(ops: &[PipeOp]) -> bool {
    fn stage(op: &PipeOp) -> u8 {
        match op {
            PipeOp::Filter { .. } => 1,
            PipeOp::SortBy { .. } => 2,
            PipeOp::Map { .. } => 3,
            PipeOp::Drop { .. } => 4,
            PipeOp::Take { .. } | PipeOp::TakeRelation { .. } => 5,
            PipeOp::Count
            | PipeOp::CountWhere { .. }
            | PipeOp::Sum { .. }
            | PipeOp::SumDirect
            | PipeOp::Avg { .. }
            | PipeOp::Min { .. }
            | PipeOp::Max { .. } => 6,
        }
    }
    let mut last_stage = 0u8;
    let mut sort_seen = false;
    for op in ops {
        let st = stage(op);
        if st < last_stage {
            return false;
        }
        if matches!(op, PipeOp::SortBy { .. }) {
            if sort_seen {
                return false;
            }
            sort_seen = true;
        }
        // Aggregates/count must not follow a take/drop: the single-query
        // form has no way to apply the LIMIT before aggregating.
        if st == 6 && last_stage >= 4 {
            return false;
        }
        last_stage = st;
    }
    true
}

enum PipeOp {
    Filter { bind_var: String, body: ast::Expr },
    Map { bind_var: String, body: ast::Expr },
    Count,
    CountWhere { bind_var: String, body: ast::Expr },
    Take { n: ast::Expr },
    Drop { n: ast::Expr },
    Sum { bind_var: String, body: ast::Expr },
    /// Direct `sum rel` (no projection): the relation's own elements are the
    /// summands. Distinguished from `Sum` so the SQL lowering aggregates the
    /// (already-mapped) column directly.
    SumDirect,
    Avg { bind_var: String, body: ast::Expr },
    Min { bind_var: String, body: ast::Expr },
    Max { bind_var: String, body: ast::Expr },
    SortBy { bind_var: String, body: ast::Expr },
    TakeRelation { n: ast::Expr },
}

/// Flatten a nested pipe chain `a |> f |> g |> h` into `(a, [f, g, h])`.
/// Each operation must be a recognized stdlib function (filter, map, count).
fn flatten_pipe_chain<'a>(
    expr: &'a ast::Expr,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> Option<(&'a ast::Expr, Vec<PipeOp>)> {
    let mut ops = Vec::new();
    let mut current = expr;

    while let ast::ExprKind::BinOp {
        op: ast::BinOp::Pipe,
        lhs,
        rhs,
    } = &current.node
    {
        let pipe_op = analyze_pipe_op(rhs, fun_bodies, let_bindings)?;
        ops.push(pipe_op);
        current = lhs;
    }

    ops.reverse();
    Some((current, ops))
}

/// Recognize a pipe RHS as a SQL-compilable operation.
fn analyze_pipe_op(
    expr: &ast::Expr,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> Option<PipeOp> {
    match &expr.node {
        ast::ExprKind::Var(name) if name == "count" => Some(PipeOp::Count),
        // `rel |> sum` — direct aggregation over a numeric relation, no
        // projection. The PipeOp::Sum fields are unused for this form (the
        // relation's own element is the summand); reuse the identity shape.
        ast::ExprKind::Var(name) if name == "sum" => Some(PipeOp::SumDirect),
        ast::ExprKind::App { func, arg } => {
            if let ast::ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "filter" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::Filter { bind_var, body }
                    }),
                    "map" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::Map { bind_var, body }
                    }),
                    "take" => Some(PipeOp::Take { n: (**arg).clone() }),
                    "takeRelation" => Some(PipeOp::TakeRelation { n: (**arg).clone() }),
                    "drop" => Some(PipeOp::Drop { n: (**arg).clone() }),
                    "sortBy" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::SortBy { bind_var, body }
                    }),
                    "avg" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::Avg { bind_var, body }
                    }),
                    "minOn" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::Min { bind_var, body }
                    }),
                    "maxOn" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::Max { bind_var, body }
                    }),
                    "countWhere" => extract_single_param_lambda(arg, fun_bodies, let_bindings).map(|(bind_var, body)| {
                        PipeOp::CountWhere { bind_var, body }
                    }),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract bind variable name and body from a single-parameter lambda,
/// fully inlining function calls (beta-reduction + named-function expansion)
/// before checking the shape. The body is owned because inlining may
/// synthesize new sub-expressions.
fn extract_single_param_lambda(
    expr: &ast::Expr,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> Option<(String, ast::Expr)> {
    let reduced = beta_reduce(expr, fun_bodies, let_bindings);
    if let ast::ExprKind::Lambda { params, body } = reduced.node
        && params.len() == 1
            && let ast::PatKind::Var(name) = &params[0].node {
                return Some((name.clone(), *body));
            }
    None
}

/// Extract `filter (\x -> cond) *source` or `filter (\x -> cond) bound_var`
/// after full inlining. Returns (source_name, bind_var, filter_body).
fn extract_filter_on_source(
    expr: &ast::Expr,
    source_var_binds: &HashMap<String, String>,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> Option<(String, String, ast::Expr)> {
    let reduced = beta_reduce(expr, fun_bodies, let_bindings);
    if let ast::ExprKind::App { func, arg: source_expr } = &reduced.node {
        let resolve_source = |se: &ast::Expr| -> Option<String> {
            match &se.node {
                ast::ExprKind::SourceRef(name) => Some(name.clone()),
                ast::ExprKind::Var(name) => source_var_binds.get(name).cloned(),
                _ => None,
            }
        };

        if let ast::ExprKind::App { func: inner_func, arg: filter_lambda } = &func.node
            && let ast::ExprKind::Var(fn_name) = &inner_func.node
                && fn_name == "filter"
                    && let Some(source_name) = resolve_source(source_expr)
                        && let Some((bind_var, body)) =
                            extract_single_param_lambda(filter_lambda, fun_bodies, let_bindings)
                        {
                            return Some((source_name, bind_var, body));
                        }
    }
    None
}

// ── Beta-reduction / inlining for SQL pushdown ────────────────────────
//
// Rewrites an expression by repeatedly:
//   1. Replacing `Var(f)` with the body of `f` (when `f` is a known top-level
//      function), and
//   2. Beta-reducing `App(Lambda, arg)` into the lambda body with the
//      parameter substituted by `arg`.
// Continues until no more reductions apply. Recursive functions are left
// unexpanded — see `recursive_names`, which seeds the `visited` set that the
// `Var` case consults; pushdown falls back to runtime for calls to them.
//
// Substitution is capture-avoiding: when entering a `Lambda` (or `Case`
// arm pattern, etc.) whose bound names overlap with the free vars of the
// substituted value, we abandon that branch of substitution and leave the
// expression unchanged, which is sound (just less optimized).

pub(crate) fn beta_reduce(
    expr: &ast::Expr,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> ast::Expr {
    // Seeded with the names that can reach themselves through the call graph,
    // so the `Var` case below leaves every recursive reference alone. The set
    // is never popped for those names (only names inserted by the `Var` case
    // are), so the block holds for the whole reduction.
    let mut visited = recursive_names(fun_bodies, let_bindings);
    // Fuel bounds work to prevent exponential expansion when substituted
    // bodies contain repeated occurrences of their parameter — re-reducing
    // the substituted result can multiply work at each level. On budget
    // exhaustion we return the partially-reduced expression, which is still
    // sound (callers fall back to None when the shape isn't recognized).
    let mut fuel: usize = 50_000;
    beta_reduce_inner(expr, fun_bodies, let_bindings, &mut visited, &mut fuel)
}

/// Names whose definition can reach itself through the call graph — directly
/// (`f` mentions `f`) or mutually (`f` mentions `g`, `g` mentions `f`).
/// `beta_reduce` must not expand these.
///
/// Its `visited` set alone does not stop them: it blocks re-entering a name
/// only *while* that name's body is being reduced, and pops it as soon as the
/// expansion returns. The body it just substituted still holds a self-
/// reference, so the next reduction step expands it again, and the next, with
/// nothing bounding the unrolling. `fuel` caps the number of steps but not the
/// size of what each step builds: `substitute` copies the argument into every
/// occurrence of the parameter, so each unroll multiplies the term rather than
/// growing it by a constant, and the reduction never finishes in practice.
///
/// Leaving them unexpanded costs no pushdown. Beta reduction does not evaluate
/// the base-case condition, so unrolling a recursive function never bottoms
/// out — a self-reference always survives in the recursive branch, and no SQL
/// fragment can compile that. Pushdown falls back to evaluating the call at
/// runtime, which is where it ended up anyway once the fuel ran out.
fn recursive_names(
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
) -> HashSet<String> {
    let names: HashSet<&str> = fun_bodies
        .keys()
        .chain(let_bindings.keys())
        .map(String::as_str)
        .collect();

    let callees: HashMap<&str, Vec<&str>> = names
        .iter()
        .map(|&name| {
            // A `let` shadows a top-level function of the same name, mirroring
            // the lookup order in `beta_reduce_inner`'s `Var` case.
            let body = let_bindings.get(name).or_else(|| fun_bodies.get(name));
            let calls = body
                .map(|body| {
                    compute_free_vars(body)
                        .iter()
                        .filter_map(|v| names.get(v.as_str()).copied())
                        .collect()
                })
                .unwrap_or_default();
            (name, calls)
        })
        .collect();

    names
        .iter()
        .filter(|&&name| {
            let mut stack: Vec<&str> = callees[name].clone();
            let mut seen: HashSet<&str> = stack.iter().copied().collect();
            while let Some(callee) = stack.pop() {
                if callee == name {
                    return true;
                }
                for &next in &callees[callee] {
                    if seen.insert(next) {
                        stack.push(next);
                    }
                }
            }
            false
        })
        .map(|&name| name.to_string())
        .collect()
}

fn beta_reduce_inner(
    expr: &ast::Expr,
    fun_bodies: &HashMap<String, ast::Expr>,
    let_bindings: &HashMap<String, ast::Expr>,
    visited: &mut HashSet<String>,
    fuel: &mut usize,
) -> ast::Expr {
    use ast::ExprKind::*;
    if *fuel == 0 {
        return expr.clone();
    }
    *fuel -= 1;
    let span = expr.span;
    let new_node = match &expr.node {
        Var(name) => {
            if !visited.contains(name) {
                // Local let bindings shadow top-level functions: a let
                // inside a do-block introduces a fresh name in scope,
                // and the matchers see the do-block AST so the local
                // definition is the relevant one.
                let body = let_bindings.get(name).or_else(|| fun_bodies.get(name));
                if let Some(body) = body {
                    visited.insert(name.clone());
                    let result =
                        beta_reduce_inner(body, fun_bodies, let_bindings, visited, fuel);
                    visited.remove(name);
                    return result;
                }
            }
            Var(name.clone())
        }
        App { func, arg } => {
            let f = beta_reduce_inner(func, fun_bodies, let_bindings, visited, fuel);
            let a = beta_reduce_inner(arg, fun_bodies, let_bindings, visited, fuel);
            if let Lambda { params, body } = &f.node
                && !params.is_empty()
                    && let ast::PatKind::Var(name) = &params[0].node
                        // For a multi-param lambda the remaining params
                        // (`params[1..]`) become binders wrapped *around* the
                        // substituted body. `substitute` is capture-avoiding
                        // only w.r.t. the head param it replaces — it has no
                        // knowledge of these outer binders. So if a free
                        // variable of the argument collides with one of them,
                        // wrapping would capture it (e.g. `(\l p -> ...) p.l`
                        // turning the outer `p` into the inner row `p`). Bail
                        // out in that case; SQL pushdown then falls back to
                        // in-memory evaluation rather than emitting wrong SQL.
                        && !multi_param_would_capture(&params[1..], &a)
                        && let Some(substituted) = substitute(body, name, &a) {
                            if params.len() == 1 {
                                return beta_reduce_inner(
                                    &substituted, fun_bodies, let_bindings, visited, fuel,
                                );
                            }
                            let new_lambda = ast::Spanned {
                                node: Lambda {
                                    params: params[1..].to_vec(),
                                    body: Box::new(substituted),
                                },
                                span: f.span,
                            };
                            return beta_reduce_inner(
                                &new_lambda, fun_bodies, let_bindings, visited, fuel,
                            );
                        }
            App { func: Box::new(f), arg: Box::new(a) }
        }
        Lambda { params, body } => {
            // A lambda parameter shadows any same-named do-local let or
            // top-level binding for the body: mask those names out of the
            // expansion maps so `\q -> q.value` is NOT rewritten to the
            // outer `q` definition. (The App `substitute` path is already
            // capture-avoiding; only this named-map path needs masking.)
            let shadows = |k: &String| params.iter().any(|p| pat_binds(p, k));
            if fun_bodies.keys().any(shadows) || let_bindings.keys().any(shadows) {
                let mut masked_funs = fun_bodies.clone();
                masked_funs.retain(|k, _| !shadows(k));
                let mut masked_lets = let_bindings.clone();
                masked_lets.retain(|k, _| !shadows(k));
                Lambda {
                    params: params.clone(),
                    body: Box::new(beta_reduce_inner(
                        body, &masked_funs, &masked_lets, visited, fuel,
                    )),
                }
            } else {
                Lambda {
                    params: params.clone(),
                    body: Box::new(beta_reduce_inner(
                        body, fun_bodies, let_bindings, visited, fuel,
                    )),
                }
            }
        }
        BinOp { op, lhs, rhs } => BinOp {
            op: *op,
            lhs: Box::new(beta_reduce_inner(lhs, fun_bodies, let_bindings, visited, fuel)),
            rhs: Box::new(beta_reduce_inner(rhs, fun_bodies, let_bindings, visited, fuel)),
        },
        UnaryOp { op, operand } => UnaryOp {
            op: *op,
            operand: Box::new(beta_reduce_inner(operand, fun_bodies, let_bindings, visited, fuel)),
        },
        FieldAccess { expr: e, field } => FieldAccess {
            expr: Box::new(beta_reduce_inner(e, fun_bodies, let_bindings, visited, fuel)),
            field: field.clone(),
        },
        Record(fields) => Record(
            fields
                .iter()
                .map(|f| ast::Field {
                    name: f.name.clone(),
                    value: beta_reduce_inner(&f.value, fun_bodies, let_bindings, visited, fuel),
                })
                .collect(),
        ),
        RecordUpdate { base, fields } => RecordUpdate {
            base: Box::new(beta_reduce_inner(base, fun_bodies, let_bindings, visited, fuel)),
            fields: fields
                .iter()
                .map(|f| ast::Field {
                    name: f.name.clone(),
                    value: beta_reduce_inner(&f.value, fun_bodies, let_bindings, visited, fuel),
                })
                .collect(),
        },
        List(items) => List(
            items
                .iter()
                .map(|e| beta_reduce_inner(e, fun_bodies, let_bindings, visited, fuel))
                .collect(),
        ),
        If { cond, then_branch, else_branch } => If {
            cond: Box::new(beta_reduce_inner(cond, fun_bodies, let_bindings, visited, fuel)),
            then_branch: Box::new(beta_reduce_inner(
                then_branch,
                fun_bodies,
                let_bindings,
                visited,
                fuel,
            )),
            else_branch: Box::new(beta_reduce_inner(
                else_branch,
                fun_bodies,
                let_bindings,
                visited,
                fuel,
            )),
        },
        // For constructs that bind names (Case arms, Do statements, Set, etc.)
        // we keep them unchanged: SQL pushdown never sees these inside the
        // expressions it analyzes (lambda bodies of filter/map/aggregate).
        Lit(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) | Case { .. } | Do(_)
        | Set { .. } | ReplaceSet { .. } | Atomic(_) | TimeUnitLit { .. }
        | Annot { .. } | Refine(_) | Serve { .. } => return expr.clone(),
    };
    ast::Spanned { node: new_node, span }
}

/// Capture-avoiding substitution `expr[var := value]`. Returns `None` if
/// performing the substitution would capture a free variable of `value`
/// (in which case the caller leaves the expression unchanged).
fn substitute(expr: &ast::Expr, var: &str, value: &ast::Expr) -> Option<ast::Expr> {
    let value_fv = compute_free_vars(value);
    substitute_inner(expr, var, value, &value_fv)
}

fn substitute_inner(
    expr: &ast::Expr,
    var: &str,
    value: &ast::Expr,
    value_fv: &HashSet<String>,
) -> Option<ast::Expr> {
    use ast::ExprKind::*;
    let span = expr.span;
    let new_node = match &expr.node {
        Var(name) if name == var => return Some(value.clone()),
        Var(_) | Lit(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) => {
            return Some(expr.clone())
        }
        Lambda { params, body } => {
            if params.iter().any(|p| pat_binds(p, var)) {
                return Some(expr.clone());
            }
            if params.iter().any(|p| pat_captures(p, value_fv)) {
                return None;
            }
            Lambda {
                params: params.clone(),
                body: Box::new(substitute_inner(body, var, value, value_fv)?),
            }
        }
        App { func, arg } => App {
            func: Box::new(substitute_inner(func, var, value, value_fv)?),
            arg: Box::new(substitute_inner(arg, var, value, value_fv)?),
        },
        BinOp { op, lhs, rhs } => BinOp {
            op: *op,
            lhs: Box::new(substitute_inner(lhs, var, value, value_fv)?),
            rhs: Box::new(substitute_inner(rhs, var, value, value_fv)?),
        },
        UnaryOp { op, operand } => UnaryOp {
            op: *op,
            operand: Box::new(substitute_inner(operand, var, value, value_fv)?),
        },
        FieldAccess { expr: e, field } => FieldAccess {
            expr: Box::new(substitute_inner(e, var, value, value_fv)?),
            field: field.clone(),
        },
        Record(fields) => Record(
            fields
                .iter()
                .map(|f| {
                    substitute_inner(&f.value, var, value, value_fv).map(|v| ast::Field {
                        name: f.name.clone(),
                        value: v,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        ),
        RecordUpdate { base, fields } => RecordUpdate {
            base: Box::new(substitute_inner(base, var, value, value_fv)?),
            fields: fields
                .iter()
                .map(|f| {
                    substitute_inner(&f.value, var, value, value_fv).map(|v| ast::Field {
                        name: f.name.clone(),
                        value: v,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        },
        List(items) => List(
            items
                .iter()
                .map(|e| substitute_inner(e, var, value, value_fv))
                .collect::<Option<Vec<_>>>()?,
        ),
        If { cond, then_branch, else_branch } => If {
            cond: Box::new(substitute_inner(cond, var, value, value_fv)?),
            then_branch: Box::new(substitute_inner(then_branch, var, value, value_fv)?),
            else_branch: Box::new(substitute_inner(else_branch, var, value, value_fv)?),
        },
        TimeUnitLit { value: v, unit_name } => TimeUnitLit {
            value: Box::new(substitute_inner(v, var, value, value_fv)?),
            unit_name: unit_name.clone(),
        },
        Annot { expr: e, ty } => Annot {
            expr: Box::new(substitute_inner(e, var, value, value_fv)?),
            ty: ty.clone(),
        },
        Refine(e) => Refine(Box::new(substitute_inner(e, var, value, value_fv)?)),
        // Constructs that introduce binders the substituter doesn't rewrite.
        // If the substituted variable never occurs inside, the expression is
        // unaffected and can be kept as-is. Otherwise the substitution FAILS
        // (caller falls back to the non-beta-reduced expression). Returning
        // the expression unchanged here used to leave the lambda parameter
        // unsubstituted inside e.g. `case` bodies — downstream shape
        // matchers then compiled the broken AST, panicking with
        // "codegen: undefined variable" or silently capturing a same-named
        // in-scope variable.
        Case { .. } | Do(_) | Set { .. } | ReplaceSet { .. } | Atomic(_)
        | Serve { .. } => {
            if expr_mentions_var(expr, var) {
                return None;
            }
            return Some(expr.clone());
        }
    };
    Some(ast::Spanned { node: new_node, span })
}

/// Conservative occurs check: does `var` appear as a `Var` node anywhere
/// inside `expr`? Shadowing is deliberately ignored (a shadowed occurrence
/// still counts), so `true` over-approximates "occurs free" — safe for
/// deciding whether a substitution can skip a subtree.
fn expr_mentions_var(expr: &ast::Expr, var: &str) -> bool {
    use ast::ExprKind::*;
    let in_stmts = |stmts: &[ast::Stmt]| -> bool {
        stmts.iter().any(|s| match &s.node {
            ast::StmtKind::Bind { expr, .. }
            | ast::StmtKind::Let { expr, .. }
            | ast::StmtKind::Expr(expr) => expr_mentions_var(expr, var),
            ast::StmtKind::Where { cond } => expr_mentions_var(cond, var),
            ast::StmtKind::GroupBy { key } => expr_mentions_var(key, var),
        })
    };
    match &expr.node {
        Var(name) => name == var,
        Lit(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) => false,
        Record(fields) => fields.iter().any(|f| expr_mentions_var(&f.value, var)),
        RecordUpdate { base, fields } => {
            expr_mentions_var(base, var)
                || fields.iter().any(|f| expr_mentions_var(&f.value, var))
        }
        FieldAccess { expr: e, .. } => expr_mentions_var(e, var),
        List(items) => items.iter().any(|e| expr_mentions_var(e, var)),
        Lambda { body, .. } => expr_mentions_var(body, var),
        App { func, arg } => {
            expr_mentions_var(func, var) || expr_mentions_var(arg, var)
        }
        BinOp { lhs, rhs, .. } => {
            expr_mentions_var(lhs, var) || expr_mentions_var(rhs, var)
        }
        UnaryOp { operand, .. } => expr_mentions_var(operand, var),
        If { cond, then_branch, else_branch } => {
            expr_mentions_var(cond, var)
                || expr_mentions_var(then_branch, var)
                || expr_mentions_var(else_branch, var)
        }
        Case { scrutinee, arms } => {
            expr_mentions_var(scrutinee, var)
                || arms.iter().any(|a| expr_mentions_var(&a.body, var))
        }
        Do(stmts) => in_stmts(stmts),
        Set { target, value } | ReplaceSet { target, value } => {
            expr_mentions_var(target, var) || expr_mentions_var(value, var)
        }
        Atomic(inner) | Refine(inner) => expr_mentions_var(inner, var),
        TimeUnitLit { value, .. } => expr_mentions_var(value, var),
        Annot { expr: e, .. } => expr_mentions_var(e, var),
        Serve { handlers, .. } => {
            handlers.iter().any(|h| expr_mentions_var(&h.body, var))
        }
    }
}

fn compute_free_vars(expr: &ast::Expr) -> HashSet<String> {
    let mut free = HashSet::new();
    let bound = HashSet::new();
    collect_free_vars_set(expr, &bound, &mut free);
    free
}

fn collect_free_vars_set(expr: &ast::Expr, bound: &HashSet<String>, free: &mut HashSet<String>) {
    use ast::ExprKind::*;
    match &expr.node {
        Var(name) => {
            if !bound.contains(name) {
                free.insert(name.clone());
            }
        }
        Lit(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) => {}
        Lambda { params, body } => {
            let mut new_bound = bound.clone();
            for p in params {
                collect_pat_binds(p, &mut new_bound);
            }
            collect_free_vars_set(body, &new_bound, free);
        }
        App { func, arg } => {
            collect_free_vars_set(func, bound, free);
            collect_free_vars_set(arg, bound, free);
        }
        BinOp { lhs, rhs, .. } => {
            collect_free_vars_set(lhs, bound, free);
            collect_free_vars_set(rhs, bound, free);
        }
        UnaryOp { operand, .. } => collect_free_vars_set(operand, bound, free),
        FieldAccess { expr: e, .. } => collect_free_vars_set(e, bound, free),
        Record(fields) => {
            for f in fields {
                collect_free_vars_set(&f.value, bound, free);
            }
        }
        RecordUpdate { base, fields } => {
            collect_free_vars_set(base, bound, free);
            for f in fields {
                collect_free_vars_set(&f.value, bound, free);
            }
        }
        List(items) => {
            for e in items {
                collect_free_vars_set(e, bound, free);
            }
        }
        If { cond, then_branch, else_branch } => {
            collect_free_vars_set(cond, bound, free);
            collect_free_vars_set(then_branch, bound, free);
            collect_free_vars_set(else_branch, bound, free);
        }
        TimeUnitLit { value: v, .. } => collect_free_vars_set(v, bound, free),
        Annot { expr: e, .. } => collect_free_vars_set(e, bound, free),
        Refine(e) => collect_free_vars_set(e, bound, free),
        Serve { handlers, .. } => {
            for h in handlers {
                collect_free_vars_set(&h.body, bound, free);
            }
        }
        Case { scrutinee, arms } => {
            collect_free_vars_set(scrutinee, bound, free);
            for arm in arms {
                // Each arm's pattern binds names local to that arm's body.
                let mut arm_bound = bound.clone();
                collect_pat_binds(&arm.pat, &mut arm_bound);
                collect_free_vars_set(&arm.body, &arm_bound, free);
            }
        }
        Do(stmts) => {
            // A `do` block sequences statements; `Bind`/`Let` introduce names
            // visible to *subsequent* statements, so thread the bound set.
            let mut do_bound = bound.clone();
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } => {
                        collect_free_vars_set(expr, &do_bound, free);
                        collect_pat_binds(pat, &mut do_bound);
                    }
                    ast::StmtKind::Let { pat, expr } => {
                        collect_free_vars_set(expr, &do_bound, free);
                        collect_pat_binds(pat, &mut do_bound);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_free_vars_set(cond, &do_bound, free);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_free_vars_set(key, &do_bound, free);
                    }
                    ast::StmtKind::Expr(e) => {
                        collect_free_vars_set(e, &do_bound, free);
                    }
                }
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            collect_free_vars_set(target, bound, free);
            collect_free_vars_set(value, bound, free);
        }
        Atomic(inner) => collect_free_vars_set(inner, bound, free),
    }
}

/// True if wrapping `params` as binders around a substituted body would
/// capture a free variable of `arg`. Used by `beta_reduce_inner` to refuse a
/// partial application whose argument mentions a name that one of the
/// remaining lambda parameters would shadow.
fn multi_param_would_capture(params: &[ast::Pat], arg: &ast::Expr) -> bool {
    let mut bound: HashSet<String> = HashSet::new();
    for p in params {
        collect_pat_binds(p, &mut bound);
    }
    if bound.is_empty() {
        return false;
    }
    let mut free: HashSet<String> = HashSet::new();
    collect_free_vars_set(arg, &HashSet::new(), &mut free);
    free.iter().any(|f| bound.contains(f))
}

fn collect_pat_binds(pat: &ast::Pat, bound: &mut HashSet<String>) {
    match &pat.node {
        ast::PatKind::Var(n) => {
            bound.insert(n.clone());
        }
        ast::PatKind::Constructor { payload, .. } => collect_pat_binds(payload, bound),
        ast::PatKind::Record(fields) => {
            for f in fields {
                if let Some(p) = &f.pattern {
                    collect_pat_binds(p, bound);
                } else {
                    bound.insert(f.name.clone());
                }
            }
        }
        ast::PatKind::List(items) => {
            for p in items {
                collect_pat_binds(p, bound);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_binds(head, bound);
            collect_pat_binds(tail, bound);
        }
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => {}
    }
}

fn pat_binds(pat: &ast::Pat, name: &str) -> bool {
    match &pat.node {
        ast::PatKind::Var(n) => n == name,
        ast::PatKind::Constructor { payload, .. } => pat_binds(payload, name),
        ast::PatKind::Record(fields) => fields.iter().any(|f| match &f.pattern {
            Some(p) => pat_binds(p, name),
            None => f.name == name,
        }),
        ast::PatKind::List(items) => items.iter().any(|p| pat_binds(p, name)),
        ast::PatKind::Cons { head, tail } => pat_binds(head, name) || pat_binds(tail, name),
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => false,
    }
}

fn pat_captures(pat: &ast::Pat, free_vars: &HashSet<String>) -> bool {
    match &pat.node {
        ast::PatKind::Var(n) => free_vars.contains(n),
        ast::PatKind::Constructor { payload, .. } => pat_captures(payload, free_vars),
        ast::PatKind::Record(fields) => fields.iter().any(|f| match &f.pattern {
            Some(p) => pat_captures(p, free_vars),
            None => free_vars.contains(&f.name),
        }),
        ast::PatKind::List(items) => items.iter().any(|p| pat_captures(p, free_vars)),
        ast::PatKind::Cons { head, tail } => {
            pat_captures(head, free_vars) || pat_captures(tail, free_vars)
        }
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => false,
    }
}

/// Convert an expression to a SQL parameter source (literal int or variable).
fn expr_to_sql_param(expr: &ast::Expr) -> Option<SqlParamSource> {
    match &expr.node {
        ast::ExprKind::Lit(lit) => Some(SqlParamSource::Literal(lit.clone())),
        ast::ExprKind::Var(name) => Some(SqlParamSource::Var(name.clone())),
        _ => None,
    }
}

/// Wrap arithmetic/function SQL expressions in CAST for correct WHERE comparison.
/// SQLite arithmetic on TEXT columns (INT stored as TEXT COLLATE KNOT_INT)
/// produces INTEGER results, but parameters are TEXT. Without CAST,
/// SQLite's type affinity puts INTEGER before TEXT, breaking `>` / `<`.
/// Also wraps built-in functions like LENGTH() that return INTEGER.
fn cast_arithmetic_for_where(sql: &str) -> String {
    // Arithmetic atoms are wrapped in parentheses by try_compile_sql_atom;
    // both those and built-in functions like LENGTH() yield INTEGER results
    // that need the same CAST-to-TEXT treatment for correct WHERE comparison.
    if (sql.starts_with('(') && !sql.starts_with("(CAST")) || sql.starts_with("LENGTH(") {
        format!("CAST({} AS TEXT) COLLATE KNOT_INT", sql)
    } else {
        sql.to_string()
    }
}

/// Extract a SQL column reference from a lambda body like `\x -> x.price`.
/// Returns the SQL fragment e.g. `t0."price"` (or just `"price"` if alias is empty).
fn extract_sql_field_access(
    bind_var: &str,
    body: &ast::Expr,
    alias: &str,
    schema: &str,
) -> Option<String> {
    if let ast::ExprKind::FieldAccess { expr, field: col_name } = &body.node
        && let ast::ExprKind::Var(name) = &expr.node
            && name == bind_var {
                // Verify column exists in schema
                lookup_col_type_from_schema(schema, col_name)?;
                return Some(sql_col_ref(alias, col_name));
            }
    // Also handle arithmetic expressions like `\x -> x.price * x.qty`
    try_sql_arithmetic_expr(bind_var, body, alias, schema)
}

/// Format a column reference, with or without table alias.
fn sql_col_ref(alias: &str, col_name: &str) -> String {
    if alias.is_empty() {
        quote_sql_ident(col_name)
    } else {
        format!("{}.{}", alias, quote_sql_ident(col_name))
    }
}

/// Try to compile a condition expression to an inline SQL condition (no params).
/// Used inside CASE WHEN expressions where everything must be inlined.
/// Handles: comparisons (==, !=, <, >, <=, >=), AND, OR, NOT.
fn try_sql_inline_condition(
    bind_var: &str,
    expr: &ast::Expr,
    alias: &str,
    schema: &str,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::BinOp { op, lhs, rhs } => match op {
            ast::BinOp::And => {
                let l = try_sql_inline_condition(bind_var, lhs, alias, schema)?;
                let r = try_sql_inline_condition(bind_var, rhs, alias, schema)?;
                Some(format!("({}) AND ({})", l, r))
            }
            ast::BinOp::Or => {
                let l = try_sql_inline_condition(bind_var, lhs, alias, schema)?;
                let r = try_sql_inline_condition(bind_var, rhs, alias, schema)?;
                Some(format!("({}) OR ({})", l, r))
            }
            ast::BinOp::Eq | ast::BinOp::Neq | ast::BinOp::Lt
            | ast::BinOp::Gt | ast::BinOp::Le | ast::BinOp::Ge => {
                let sql_op = match op {
                    ast::BinOp::Eq => "=",
                    ast::BinOp::Neq => "!=",
                    ast::BinOp::Lt => "<",
                    ast::BinOp::Gt => ">",
                    ast::BinOp::Le => "<=",
                    ast::BinOp::Ge => ">=",
                    _ => unreachable!(),
                };
                // Mirror the WHERE-pushdown gates: float comparisons stay
                // in memory (total_cmp vs SQL -0.0/NaN-as-NULL semantics);
                // ordered comparisons on tag columns ignore the type's Ord.
                let lt = infer_sql_expr_type(bind_var, lhs, schema);
                let rt = infer_sql_expr_type(bind_var, rhs, schema);
                if lt.as_deref() == Some("float") || rt.as_deref() == Some("float") {
                    return None;
                }
                // json-stored columns (ADT payloads / nested records) compare
                // as raw JSON text in SQL, which can diverge from Knot's
                // structural equality. The WHERE-pushdown path rejects these
                // (sql_scalar_kind returns Err for "json"); mirror that here.
                if lt.as_deref() == Some("json") || rt.as_deref() == Some("json") {
                    return None;
                }
                if matches!(sql_op, "<" | ">" | "<=" | ">=")
                    && (lt.as_deref() == Some("tag") || rt.as_deref() == Some("tag"))
                {
                    return None;
                }
                let l = try_sql_arithmetic_expr(bind_var, lhs, alias, schema)?;
                let r = try_sql_arithmetic_expr(bind_var, rhs, alias, schema)?;
                Some(format!("{} {} {}", l, sql_op, r))
            }
            _ => None,
        },
        ast::ExprKind::UnaryOp {
            op: ast::UnaryOp::Not,
            operand,
        } => {
            let inner = try_sql_inline_condition(bind_var, operand, alias, schema)?;
            Some(format!("NOT ({})", inner))
        }
        // `not expr` function application form → NOT (...)
        // `contains needle haystack` → INSTR(haystack, needle) > 0
        ast::ExprKind::App { func, arg } => {
            if let ast::ExprKind::Var(name) = &func.node
                && name == "not" {
                    let inner = try_sql_inline_condition(bind_var, arg, alias, schema)?;
                    return Some(format!("NOT ({})", inner));
                }
            if let ast::ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                && let ast::ExprKind::Var(name) = &inner_func.node {
                    if name == "contains" {
                        let needle = try_sql_arithmetic_expr(bind_var, first_arg, alias, schema)?;
                        let haystack = try_sql_arithmetic_expr(bind_var, arg, alias, schema)?;
                        return Some(format!("INSTR({}, {}) > 0", haystack, needle));
                    }
                    if name == "elem" {
                        // elem (bind_var.field) [lit, lit, ...] → field IN (lit, lit, ...)
                        // Empty list → always-false (`0`).
                        // Float `IN` equality stays in memory (see the
                        // float comparison gates above).
                        if infer_sql_expr_type(bind_var, first_arg, schema).as_deref()
                            == Some("float")
                        {
                            return None;
                        }
                        let col = try_sql_arithmetic_expr(bind_var, first_arg, alias, schema)?;
                        let elems = match &arg.node {
                            ast::ExprKind::List(es) => es,
                            _ => return None,
                        };
                        if elems.is_empty() {
                            return Some("0".to_string());
                        }
                        let mut parts = Vec::with_capacity(elems.len());
                        for e in elems {
                            match &e.node {
                                ast::ExprKind::Lit(ast::Literal::Int(n)) => parts.push(n.to_string()),
                                ast::ExprKind::Lit(ast::Literal::Float(f)) => parts.push(f.to_string()),
                                ast::ExprKind::Lit(ast::Literal::Text(s)) => {
                                    parts.push(format!("'{}'", s.replace('\'', "''")))
                                }
                                ast::ExprKind::Lit(ast::Literal::Bool(b)) => {
                                    parts.push(if *b { "1" } else { "0" }.to_string())
                                }
                                _ => return None,
                            }
                        }
                        return Some(format!("{} IN ({})", col, parts.join(", ")));
                    }
                }
            None
        }
        _ => None,
    }
}

/// Try to compile an arithmetic expression to a SQL fragment.
/// Handles: field access, literals, +, -, *, / binary ops, and if/then/else → CASE WHEN.
fn try_sql_arithmetic_expr(
    bind_var: &str,
    expr: &ast::Expr,
    alias: &str,
    schema: &str,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node
                && name == bind_var {
                    lookup_col_type_from_schema(schema, col_name)?;
                    return Some(sql_col_ref(alias, col_name));
                }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(n) => Some(n.to_string()),
            ast::Literal::Float(f) => Some(f.to_string()),
            ast::Literal::Text(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            ast::Literal::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let sql_op = match op {
                ast::BinOp::Add => "+",
                ast::BinOp::Sub => "-",
                ast::BinOp::Mul => "*",
                // `/`/`%` only push down with a provably nonzero literal
                // divisor (SQLite NULLs on /0, the runtime panics); `%` must
                // be integer-typed (int-literal divisor proves it).
                ast::BinOp::Div if divisor_is_nonzero_literal(rhs) => "/",
                ast::BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => "%",
                ast::BinOp::Concat => "||",
                _ => return None,
            };
            let l = try_sql_arithmetic_expr(bind_var, lhs, alias, schema)?;
            let r = try_sql_arithmetic_expr(bind_var, rhs, alias, schema)?;
            Some(format!("({} {} {})", l, sql_op, r))
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            let cond_sql = try_sql_inline_condition(bind_var, cond, alias, schema)?;
            let then_sql = try_sql_arithmetic_expr(bind_var, then_branch, alias, schema)?;
            let else_sql = try_sql_arithmetic_expr(bind_var, else_branch, alias, schema)?;
            Some(format!("CASE WHEN {} THEN {} ELSE {} END", cond_sql, then_sql, else_sql))
        }
        // Built-in functions: toUpper, toLower, trim, length
        ast::ExprKind::App { func, .. } => {
            if let ast::ExprKind::Var(_) = &func.node {
                // toUpper/toLower deliberately not pushed down (SQLite
                // UPPER/LOWER are ASCII-only; the runtime is Unicode-aware).
                // trim likewise: SQLite TRIM strips ASCII spaces only, the
                // runtime trims all Unicode whitespace. length likewise:
                // SQLite LENGTH() counts chars before the first NUL byte,
                // while knot_text_length counts all chars.
                None
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Analyze a map lambda body to extract SQL SELECT columns.
/// Each field can be a simple `bind_var.column` or an arithmetic expression.
fn analyze_map_select(
    bind_var: &str,
    body: &ast::Expr,
    alias: &str,
    schema: &str,
) -> Option<Vec<SqlSelectColumn>> {
    if let ast::ExprKind::Record(fields) = &body.node {
        let mut cols = Vec::new();
        for field in fields {
            // Try simple field access first
            if let ast::ExprKind::FieldAccess {
                expr,
                field: col_name,
            } = &field.value.node
                && let ast::ExprKind::Var(name) = &expr.node
                    && name == bind_var {
                        let type_str = lookup_col_type_from_schema(schema, col_name)?;
                        cols.push(SqlSelectColumn {
                            result_field: field.name.clone(),
                            alias: alias.to_string(),
                            source_col: col_name.clone(),
                            type_str,
                            sql_expr: None,
                        });
                        continue;
                    }
            // Try arithmetic expression
            if let Some(sql_expr) = try_sql_arithmetic_expr(bind_var, &field.value, alias, schema) {
                // Infer result type from the expression (default to float for arithmetic)
                let type_str = infer_sql_expr_type(bind_var, &field.value, schema)
                    .unwrap_or_else(|| "float".to_string());
                cols.push(SqlSelectColumn {
                    result_field: field.name.clone(),
                    alias: alias.to_string(),
                    source_col: field.name.clone(),
                    type_str,
                    sql_expr: Some(sql_expr),
                });
            } else {
                return None;
            }
        }
        Some(cols)
    } else {
        None
    }
}

/// Infer the SQL type of an arithmetic expression by examining its leaf types.
pub(crate) fn infer_sql_expr_type(bind_var: &str, expr: &ast::Expr, schema: &str) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node
                && name == bind_var {
                    return lookup_col_type_from_schema(schema, col_name);
                }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(_) => Some("int".to_string()),
            ast::Literal::Float(_) => Some("float".to_string()),
            ast::Literal::Text(_) => Some("text".to_string()),
            // A bool literal is emitted as SQL `1`/`0`; it must be typed
            // `bool` (not `int`) so the column reads back through
            // `ColType::Bool` -> `Value::Bool`, matching the bool-column path.
            ast::Literal::Bool(_) => Some("bool".to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            match op {
                ast::BinOp::Concat => Some("text".to_string()),
                _ => {
                    // Division joins operand witnesses like the other
                    // arithmetic operators: Knot Int/Int division is
                    // integer division (and SQLite `/` on TEXT-stored ints
                    // also integer-divides), so the result column must be
                    // typed int — typing it float would box `4 / 2` as 2.0.
                    let l = infer_sql_expr_type(bind_var, lhs, schema);
                    let r = infer_sql_expr_type(bind_var, rhs, schema);
                    match (l.as_deref(), r.as_deref()) {
                        // Float on either side → float
                        (Some("float"), _) | (_, Some("float")) => Some("float".to_string()),
                        (Some(t), _) => Some(t.to_string()),
                        (_, Some(t)) => Some(t.to_string()),
                        _ => None,
                    }
                }
            }
        }
        ast::ExprKind::If { then_branch, else_branch, .. } => {
            infer_sql_expr_type(bind_var, then_branch, schema)
                .or_else(|| infer_sql_expr_type(bind_var, else_branch, schema))
        }
        // Built-in functions
        ast::ExprKind::App { func, .. } => {
            if let ast::ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "length" => Some("int".to_string()),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

// ── Multi-table SQL expression helpers ───────────────────────────

/// Try to compile a condition expression to inline SQL for multi-table contexts.
/// Used inside CASE WHEN in multi-table yield expressions.
fn try_multi_table_inline_condition(
    bind_to_alias: &HashMap<String, String>,
    bind_to_schema: &HashMap<String, String>,
    expr: &ast::Expr,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::BinOp { op, lhs, rhs } => match op {
            ast::BinOp::And => {
                let l = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, lhs)?;
                let r = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, rhs)?;
                Some(format!("({}) AND ({})", l, r))
            }
            ast::BinOp::Or => {
                let l = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, lhs)?;
                let r = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, rhs)?;
                Some(format!("({}) OR ({})", l, r))
            }
            ast::BinOp::Eq | ast::BinOp::Neq | ast::BinOp::Lt
            | ast::BinOp::Gt | ast::BinOp::Le | ast::BinOp::Ge => {
                let sql_op = match op {
                    ast::BinOp::Eq => "=",
                    ast::BinOp::Neq => "!=",
                    ast::BinOp::Lt => "<",
                    ast::BinOp::Gt => ">",
                    ast::BinOp::Le => "<=",
                    ast::BinOp::Ge => ">=",
                    _ => unreachable!(),
                };
                // Mirror the WHERE-pushdown gates: float comparisons stay
                // in memory (total_cmp vs SQL -0.0/NaN-as-NULL semantics);
                // ordered comparisons on tag columns ignore the type's Ord.
                let lt = infer_multi_table_sql_expr_type(bind_to_schema, lhs);
                let rt = infer_multi_table_sql_expr_type(bind_to_schema, rhs);
                if lt.as_deref() == Some("float") || rt.as_deref() == Some("float") {
                    return None;
                }
                // json-stored columns (ADT payloads / nested records) compare
                // as raw JSON text in SQL, which can diverge from Knot's
                // structural equality. The WHERE-pushdown path rejects these
                // (sql_scalar_kind returns Err for "json"); mirror that here.
                if lt.as_deref() == Some("json") || rt.as_deref() == Some("json") {
                    return None;
                }
                if matches!(sql_op, "<" | ">" | "<=" | ">=")
                    && (lt.as_deref() == Some("tag") || rt.as_deref() == Some("tag"))
                {
                    return None;
                }
                let l = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, lhs)?;
                let r = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, rhs)?;
                Some(format!("{} {} {}", l, sql_op, r))
            }
            _ => None,
        },
        ast::ExprKind::UnaryOp {
            op: ast::UnaryOp::Not,
            operand,
        } => {
            let inner = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, operand)?;
            Some(format!("NOT ({})", inner))
        }
        // `not expr` function application form → NOT (...)
        // `contains needle haystack` → INSTR(haystack, needle) > 0
        ast::ExprKind::App { func, arg } => {
            if let ast::ExprKind::Var(name) = &func.node
                && name == "not" {
                    let inner = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, arg)?;
                    return Some(format!("NOT ({})", inner));
                }
            if let ast::ExprKind::App { func: inner_func, arg: first_arg } = &func.node
                && let ast::ExprKind::Var(name) = &inner_func.node
                    && name == "contains" {
                        let needle = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, first_arg)?;
                        let haystack = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, arg)?;
                        return Some(format!("INSTR({}, {}) > 0", haystack, needle));
                    }
            None
        }
        _ => None,
    }
}

/// Try to compile an expression to an inline SQL string for multi-table yield contexts.
/// Handles: field access on any bound table, literals, arithmetic, CASE WHEN.
fn try_multi_table_arithmetic_expr(
    bind_to_alias: &HashMap<String, String>,
    bind_to_schema: &HashMap<String, String>,
    expr: &ast::Expr,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node
                && let Some(alias) = bind_to_alias.get(name.as_str()) {
                    let schema = bind_to_schema.get(name.as_str())?;
                    lookup_col_type_from_schema(schema, col_name)?;
                    return Some(format!("{}.{}", alias, quote_sql_ident(col_name)));
                }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(n) => Some(n.to_string()),
            ast::Literal::Float(f) => Some(f.to_string()),
            ast::Literal::Text(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            ast::Literal::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let sql_op = match op {
                ast::BinOp::Add => "+",
                ast::BinOp::Sub => "-",
                ast::BinOp::Mul => "*",
                // See try_sql_arithmetic_expr for the `/`/`%` pushdown rules.
                ast::BinOp::Div if divisor_is_nonzero_literal(rhs) => "/",
                ast::BinOp::Mod if divisor_is_nonzero_int_literal(rhs) => "%",
                ast::BinOp::Concat => "||",
                _ => return None,
            };
            let l = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, lhs)?;
            let r = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, rhs)?;
            Some(format!("({} {} {})", l, sql_op, r))
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            let cond_sql = try_multi_table_inline_condition(bind_to_alias, bind_to_schema, cond)?;
            let then_sql = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, then_branch)?;
            let else_sql = try_multi_table_arithmetic_expr(bind_to_alias, bind_to_schema, else_branch)?;
            Some(format!("CASE WHEN {} THEN {} ELSE {} END", cond_sql, then_sql, else_sql))
        }
        // Built-in functions: toUpper, toLower, trim, length
        ast::ExprKind::App { func, .. } => {
            if let ast::ExprKind::Var(_) = &func.node {
                // toUpper/toLower deliberately not pushed down (SQLite
                // UPPER/LOWER are ASCII-only; the runtime is Unicode-aware).
                // trim likewise: SQLite TRIM strips ASCII spaces only, the
                // runtime trims all Unicode whitespace. length likewise:
                // SQLite LENGTH() counts chars before the first NUL byte,
                // while knot_text_length counts all chars.
                None
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Infer the SQL type of an expression in a multi-table context.
fn infer_multi_table_sql_expr_type(
    bind_to_schema: &HashMap<String, String>,
    expr: &ast::Expr,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node
                && let Some(schema) = bind_to_schema.get(name.as_str()) {
                    return lookup_col_type_from_schema(schema, col_name);
                }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(_) => Some("int".to_string()),
            ast::Literal::Float(_) => Some("float".to_string()),
            ast::Literal::Text(_) => Some("text".to_string()),
            // A bool literal is emitted as SQL `1`/`0`; it must be typed
            // `bool` (not `int`) so the column reads back through
            // `ColType::Bool` -> `Value::Bool`, matching the bool-column path.
            ast::Literal::Bool(_) => Some("bool".to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            match op {
                ast::BinOp::Concat => Some("text".to_string()),
                _ => {
                    // Division joins witnesses like other arithmetic —
                    // see infer_sql_expr_type (Int/Int stays int).
                    let l = infer_multi_table_sql_expr_type(bind_to_schema, lhs);
                    let r = infer_multi_table_sql_expr_type(bind_to_schema, rhs);
                    match (l.as_deref(), r.as_deref()) {
                        (Some("float"), _) | (_, Some("float")) => Some("float".to_string()),
                        (Some(t), _) => Some(t.to_string()),
                        (_, Some(t)) => Some(t.to_string()),
                        _ => None,
                    }
                }
            }
        }
        ast::ExprKind::If { then_branch, else_branch, .. } => {
            infer_multi_table_sql_expr_type(bind_to_schema, then_branch)
                .or_else(|| infer_multi_table_sql_expr_type(bind_to_schema, else_branch))
        }
        // Built-in functions
        ast::ExprKind::App { func, .. } => {
            if let ast::ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "length" => Some("int".to_string()),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

// ── Free functions ────────────────────────────────────────────────

fn merge_block_param(
    builder: &mut FunctionBuilder,
    block: cranelift_codegen::ir::Block,
    ty: types::Type,
) {
    builder.append_block_param(block, ty);
}

/// Extract the primary variable name from a pattern for groupBy tracking.
/// For `Var(name)` returns the name; for `Constructor { payload, .. }` recurses
/// into the payload; for other patterns returns None.
/// Extract a nested-field schema from a parent schema descriptor.
/// Given `"name:text,items:[qty:int,price:float]"` and field `"items"`,
/// returns `Some("qty:int,price:float")`.
fn extract_child_schema(parent_schema: &str, field: &str) -> Option<String> {
    // Split schema by commas while respecting brackets (nested schemas).
    let mut depth = 0usize;
    let mut start = 0;
    let bytes = parent_schema.as_bytes();
    let mut parts = Vec::new();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'[' => depth += 1,
            b']' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&parent_schema[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < parent_schema.len() {
        parts.push(&parent_schema[start..]);
    }

    for part in parts {
        // Each part is "field_name:type" or "field_name:[child_schema]"
        if let Some(colon) = part.find(':') {
            let name = &part[..colon];
            if name == field {
                let type_part = &part[colon + 1..];
                // Nested relation: "[child_schema]" — strip brackets
                if type_part.starts_with('[') && type_part.ends_with(']') {
                    return Some(type_part[1..type_part.len() - 1].to_string());
                }
                // Not a nested relation field
                return None;
            }
        }
    }
    None
}

fn pat_primary_var(pat: &ast::PatKind) -> Option<String> {
    match pat {
        ast::PatKind::Var(name) => Some(name.clone()),
        ast::PatKind::Constructor { payload, .. } => pat_primary_var(&payload.node),
        _ => None,
    }
}

/// A case pattern is irrefutable when it can never fail at runtime:
/// wildcards, variables, and records whose sub-patterns are all
/// irrefutable. Literals, constructors, and list shapes (and records
/// containing them) require runtime tests.
fn case_pattern_is_irrefutable(pat: &ast::Pat) -> bool {
    match &pat.node {
        ast::PatKind::Wildcard | ast::PatKind::Var(_) => true,
        ast::PatKind::Record(fields) => fields.iter().all(|fp| {
            fp.pattern
                .as_ref()
                .is_none_or(case_pattern_is_irrefutable)
        }),
        ast::PatKind::Lit(_)
        | ast::PatKind::Constructor { .. }
        | ast::PatKind::List(_)
        | ast::PatKind::Cons { .. } => false,
    }
}

fn bind_do_pattern(
    builder: &mut FunctionBuilder,
    cg: &mut Codegen,
    pat: &ast::Pat,
    val: Value,
    env: &mut Env,
    skips: &mut Vec<cranelift_codegen::ir::Block>,
) {
    match &pat.node {
        ast::PatKind::Var(name) => env.set(name, val),
        ast::PatKind::Wildcard => {}
        ast::PatKind::Record(fields) => {
            for fp in fields {
                let (key_ptr, key_len) = cg.string_ptr(builder, &fp.name);
                let field_val =
                    cg.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len]);
                if let Some(inner_pat) = &fp.pattern {
                    bind_do_pattern(builder, cg, inner_pat, field_val, env, skips);
                } else {
                    env.set(&fp.name, field_val);
                }
            }
        }
        ast::PatKind::Constructor { name, payload } => {
            // Pattern match bind: `Circle c <- *shapes`
            // Filter: only rows matching the constructor tag continue
            let is_match = if name == "True" || name == "False" {
                // Bool is represented as Value::Bool, not Value::Constructor —
                // knot_constructor_matches would always return 0. Test the bool
                // value directly, mirroring compile_case.
                let bool_val =
                    cg.call_rt_typed(builder, "knot_value_get_bool", &[val], types::I32);
                let expected = if name == "True" { 1i64 } else { 0i64 };
                builder.ins().icmp_imm(IntCC::Equal, bool_val, expected)
            } else {
                match cg.nullable_ctors.get(name).cloned() {
                    Some(NullableRole::None) => {
                        builder.ins().icmp_imm(IntCC::Equal, val, 0)
                    }
                    Some(NullableRole::Some) => {
                        builder.ins().icmp_imm(IntCC::NotEqual, val, 0)
                    }
                    None => {
                        let (tag_ptr, tag_len) = cg.string_ptr(builder, name);
                        let matches = cg.call_rt_typed(
                            builder,
                            "knot_constructor_matches",
                            &[val, tag_ptr, tag_len],
                            types::I32,
                        );
                        builder.ins().icmp_imm(IntCC::NotEqual, matches, 0)
                    }
                }
            };

            let then_block = builder.create_block();
            let skip_block = builder.create_block();
            builder.ins().brif(is_match, then_block, &[], skip_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            skips.push(skip_block);

            // Extract payload and bind inner pattern
            let inner = cg.case_ctor_payload(builder, name, val);
            bind_do_pattern(builder, cg, payload, inner, env, skips);
        }
        ast::PatKind::Lit(lit) => {
            // Filter: only rows matching the literal value continue
            let lit_val = cg.compile_lit(builder, lit);
            let eq_i32 = cg.call_rt_typed(
                builder,
                "knot_value_eq_i32",
                &[val, lit_val],
                types::I32,
            );
            let is_match = builder.ins().icmp_imm(IntCC::NotEqual, eq_i32, 0);

            let then_block = builder.create_block();
            let skip_block = builder.create_block();
            builder.ins().brif(is_match, then_block, &[], skip_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            skips.push(skip_block);
        }
        ast::PatKind::List(pats) => {
            // Filter: a fixed-length list pattern is *refutable* — only
            // relations of exactly `pats.len()` elements continue. Without
            // this guard, shorter rows bind missing positions to `Unit`
            // (knot_relation_get returns Unit out-of-bounds) and longer rows
            // are silently kept, both of which are wrong (see the
            // compile_case List arm, which does the same length check).
            let len = cg.call_rt(builder, "knot_relation_len", &[val]);
            let expected = builder.ins().iconst(cg.ptr_type, pats.len() as i64);
            let is_match = builder.ins().icmp(IntCC::Equal, len, expected);

            let then_block = builder.create_block();
            let skip_block = builder.create_block();
            builder.ins().brif(is_match, then_block, &[], skip_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            skips.push(skip_block);

            for (idx, elem_pat) in pats.iter().enumerate() {
                let index = builder.ins().iconst(cg.ptr_type, idx as i64);
                let elem = cg.call_rt(builder, "knot_relation_get", &[val, index]);
                bind_do_pattern(builder, cg, elem_pat, elem, env, skips);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            // Filter: only non-empty relations continue.
            let len = cg.call_rt(builder, "knot_relation_len", &[val]);
            let is_match = builder.ins().icmp_imm(IntCC::NotEqual, len, 0);

            let then_block = builder.create_block();
            let skip_block = builder.create_block();
            builder.ins().brif(is_match, then_block, &[], skip_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            skips.push(skip_block);

            let zero = builder.ins().iconst(cg.ptr_type, 0);
            let head_val = cg.call_rt(builder, "knot_relation_get", &[val, zero]);
            let tail_val = cg.call_rt(builder, "knot_relation_tail", &[val]);
            bind_do_pattern(builder, cg, head, head_val, env, skips);
            bind_do_pattern(builder, cg, tail, tail_val, env, skips);
        }
    }
}

/// Uncurry nested applications: `f x y` → `(f, [x, y])`.
fn uncurry_app(expr: &ast::Expr) -> (&ast::Expr, Vec<&ast::Expr>) {
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            let (f, mut args) = uncurry_app(func);
            args.push(arg);
            (f, args)
        }
        _ => (expr, Vec::new()),
    }
}

/// Collect all `Var` binder names introduced by a pattern (recursing into
/// constructor/record/list/cons sub-patterns).
fn collect_pat_var_names(pat: &ast::Pat, out: &mut HashSet<String>) {
    match &pat.node {
        ast::PatKind::Var(name) => {
            out.insert(name.clone());
        }
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => {}
        ast::PatKind::Constructor { payload, .. } => collect_pat_var_names(payload, out),
        ast::PatKind::Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => collect_pat_var_names(p, out),
                    None => {
                        out.insert(f.name.clone());
                    }
                }
            }
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_var_names(p, out);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_var_names(head, out);
            collect_pat_var_names(tail, out);
        }
    }
}

/// Check if an expression contains a DerivedRef to the given name (self-reference detection).
fn expr_contains_derived_ref(expr: &ast::Expr, name: &str) -> bool {
    match &expr.node {
        ast::ExprKind::DerivedRef(n) => n == name,
        ast::ExprKind::Lit(_) | ast::ExprKind::Var(_) | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_) => false,
        ast::ExprKind::Record(fields) => {
            fields.iter().any(|f| expr_contains_derived_ref(&f.value, name))
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            expr_contains_derived_ref(base, name)
                || fields.iter().any(|f| expr_contains_derived_ref(&f.value, name))
        }
        ast::ExprKind::FieldAccess { expr, .. } => expr_contains_derived_ref(expr, name),
        ast::ExprKind::List(elems) => elems.iter().any(|e| expr_contains_derived_ref(e, name)),
        ast::ExprKind::Lambda { body, .. } => expr_contains_derived_ref(body, name),
        ast::ExprKind::App { func, arg } => {
            expr_contains_derived_ref(func, name) || expr_contains_derived_ref(arg, name)
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_contains_derived_ref(lhs, name) || expr_contains_derived_ref(rhs, name)
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_contains_derived_ref(operand, name),
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            expr_contains_derived_ref(cond, name)
                || expr_contains_derived_ref(then_branch, name)
                || expr_contains_derived_ref(else_branch, name)
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            expr_contains_derived_ref(scrutinee, name)
                || arms.iter().any(|a| expr_contains_derived_ref(&a.body, name))
        }
        ast::ExprKind::Do(stmts) => stmts.iter().any(|s| match &s.node {
            ast::StmtKind::Bind { expr, .. } => expr_contains_derived_ref(expr, name),
            ast::StmtKind::Let { expr, .. } => expr_contains_derived_ref(expr, name),
            ast::StmtKind::Where { cond } => expr_contains_derived_ref(cond, name),
            ast::StmtKind::GroupBy { key } => expr_contains_derived_ref(key, name),
            ast::StmtKind::Expr(e) => expr_contains_derived_ref(e, name),
        }),
        ast::ExprKind::Atomic(inner) => {
            expr_contains_derived_ref(inner, name)
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            expr_contains_derived_ref(target, name) || expr_contains_derived_ref(value, name)
        }
        ast::ExprKind::TimeUnitLit { value, .. } => expr_contains_derived_ref(value, name),
        ast::ExprKind::Annot { expr, .. } => expr_contains_derived_ref(expr, name),
        ast::ExprKind::Refine(inner) => expr_contains_derived_ref(inner, name),
        ast::ExprKind::Serve { handlers, .. } => handlers
            .iter()
            .any(|h| expr_contains_derived_ref(&h.body, name)),
    }
}

/// Extract all variable names bound by a pattern (handles destructuring).
fn pat_bound_names(pat: &ast::Pat) -> Vec<String> {
    match &pat.node {
        ast::PatKind::Var(name) => vec![name.clone()],
        ast::PatKind::Record(fields) => fields
            .iter()
            .flat_map(|f| {
                if let Some(ref inner) = f.pattern {
                    pat_bound_names(inner)
                } else {
                    vec![f.name.clone()]
                }
            })
            .collect(),
        ast::PatKind::Constructor { payload, .. } => pat_bound_names(payload),
        ast::PatKind::List(pats) => pats.iter().flat_map(pat_bound_names).collect(),
        ast::PatKind::Cons { head, tail } => {
            let mut names = pat_bound_names(head);
            names.extend(pat_bound_names(tail));
            names
        }
        _ => vec![],
    }
}

/// Find free variables in an expression (variables not bound by params).
fn find_free_vars(expr: &ast::Expr, bound: &[String]) -> Vec<String> {
    let mut free = Vec::new();
    let bound_set: HashSet<&str> = bound.iter().map(|s| s.as_str()).collect();
    collect_free_vars(expr, &bound_set, &mut free);
    free.sort();
    free.dedup();
    free
}

fn collect_free_vars(expr: &ast::Expr, bound: &HashSet<&str>, free: &mut Vec<String>) {
    match &expr.node {
        ast::ExprKind::Var(name) => {
            // Builtin/top-level names are included here too: the call sites
            // (compile_lambda_inner / compile_io_do_as_thunk) keep them only
            // when a local binding of the same name is in scope — i.e. the
            // global is shadowed and the LOCAL value must be captured.
            if !bound.contains(name.as_str()) {
                free.push(name.clone());
            }
        }
        ast::ExprKind::Lit(_) | ast::ExprKind::Constructor(_) => {}
        ast::ExprKind::SourceRef(_) => {}
        ast::ExprKind::DerivedRef(name) => {
            // A recursive derived relation passes its in-progress accumulator
            // through the env under `__derived_self_<name>` (see the DerivedRef
            // codegen arm). When the self-reference appears inside a lambda, the
            // lambda must capture that key — otherwise it falls through to the
            // public wrapper and restarts the fixpoint from the empty relation.
            // Outside a recursive body the key is absent from the env and the
            // capture filter drops it, so this is harmless for ordinary derived
            // references.
            free.push(format!("__derived_self_{}", name));
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_free_vars(&f.value, bound, free);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_free_vars(base, bound, free);
            for f in fields {
                collect_free_vars(&f.value, bound, free);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_free_vars(expr, bound, free);
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_free_vars(e, bound, free);
            }
        }
        ast::ExprKind::Lambda { params, body } => {
            let mut new_bound = bound.clone();
            for p in params {
                collect_pat_bindings_set(p, &mut new_bound);
            }
            collect_free_vars(body, &new_bound, free);
        }
        ast::ExprKind::App { func, arg } => {
            collect_free_vars(func, bound, free);
            collect_free_vars(arg, bound, free);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_free_vars(lhs, bound, free);
            collect_free_vars(rhs, bound, free);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_free_vars(operand, bound, free);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_free_vars(cond, bound, free);
            collect_free_vars(then_branch, bound, free);
            collect_free_vars(else_branch, bound, free);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_free_vars(scrutinee, bound, free);
            for arm in arms {
                let mut arm_bound = bound.clone();
                collect_pat_bindings_set(&arm.pat, &mut arm_bound);
                collect_free_vars(&arm.body, &arm_bound, free);
            }
        }
        ast::ExprKind::Do(stmts) => {
            let mut do_bound = bound.clone();
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } => {
                        collect_free_vars(expr, &do_bound, free);
                        collect_pat_bindings_set(pat, &mut do_bound);
                    }
                    ast::StmtKind::Let { pat, expr } => {
                        collect_free_vars(expr, &do_bound, free);
                        collect_pat_bindings_set(pat, &mut do_bound);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_free_vars(cond, &do_bound, free);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_free_vars(key, &do_bound, free);
                    }
                    ast::StmtKind::Expr(e) => {
                        collect_free_vars(e, &do_bound, free);
                    }
                }
            }
        }
        ast::ExprKind::Set { target, value }
        | ast::ExprKind::ReplaceSet { target, value } => {
            collect_free_vars(target, bound, free);
            collect_free_vars(value, bound, free);
        }
        ast::ExprKind::Atomic(inner) => {
            collect_free_vars(inner, bound, free);
        }
        ast::ExprKind::TimeUnitLit { value, .. } => {
            collect_free_vars(value, bound, free);
        }
        ast::ExprKind::Annot { expr, .. } => {
            collect_free_vars(expr, bound, free);
        }
        ast::ExprKind::Refine(inner) => {
            collect_free_vars(inner, bound, free);
        }
        ast::ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                collect_free_vars(&h.body, bound, free);
            }
        }
    }
}

fn collect_pat_bindings_set<'a>(pat: &'a ast::Pat, bound: &mut HashSet<&'a str>) {
    match &pat.node {
        ast::PatKind::Var(name) => { bound.insert(name.as_str()); }
        ast::PatKind::Wildcard => {}
        ast::PatKind::Constructor { payload, .. } => {
            collect_pat_bindings_set(payload, bound);
        }
        ast::PatKind::Record(fields) => {
            for f in fields {
                if let Some(p) = &f.pattern {
                    collect_pat_bindings_set(p, bound);
                } else {
                    bound.insert(f.name.as_str());
                }
            }
        }
        ast::PatKind::Lit(_) => {}
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_bindings_set(p, bound);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_bindings_set(head, bound);
            collect_pat_bindings_set(tail, bound);
        }
    }
}

fn is_builtin_name(name: &str) -> bool {
    crate::builtins::is_builtin(name)
}

/// Check whether an expression references a specific variable.
///
/// Used by the SQL pushdown machinery to decide whether a value-side
/// expression can be evaluated *outside* row scope (as a bind parameter).
/// The match is deliberately exhaustive (no catch-all) so that new AST
/// variants are forced through here at compile time — a missed variant
/// previously let bind-var-referencing `case` expressions be hoisted out
/// of row scope, causing a codegen ICE ("undefined variable").
///
/// Scope-aware: bindings introduced by lambdas, case arms, and do
/// statements shadow `var` for the sub-expressions they cover.
pub(crate) fn expr_refs_var(expr: &ast::Expr, var: &str) -> bool {
    let pat_binds_var = |pat: &ast::Pat| pat_bound_names(pat).iter().any(|n| n == var);
    match &expr.node {
        ast::ExprKind::Var(name) => name == var,
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_) => false,
        ast::ExprKind::FieldAccess { expr: e, .. } => expr_refs_var(e, var),
        ast::ExprKind::App { func, arg } => expr_refs_var(func, var) || expr_refs_var(arg, var),
        ast::ExprKind::BinOp { lhs, rhs, .. } => expr_refs_var(lhs, var) || expr_refs_var(rhs, var),
        ast::ExprKind::UnaryOp { operand, .. } => expr_refs_var(operand, var),
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            expr_refs_var(cond, var)
                || expr_refs_var(then_branch, var)
                || expr_refs_var(else_branch, var)
        }
        ast::ExprKind::Lambda { params, body } => {
            if params.iter().any(pat_binds_var) {
                false // shadowed
            } else {
                expr_refs_var(body, var)
            }
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            expr_refs_var(scrutinee, var)
                || arms.iter().any(|arm| {
                    !pat_binds_var(&arm.pat) && expr_refs_var(&arm.body, var)
                })
        }
        ast::ExprKind::Record(fields) => fields.iter().any(|f| expr_refs_var(&f.value, var)),
        ast::ExprKind::RecordUpdate { base, fields } => {
            expr_refs_var(base, var) || fields.iter().any(|f| expr_refs_var(&f.value, var))
        }
        ast::ExprKind::List(elems) => elems.iter().any(|e| expr_refs_var(e, var)),
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr: e } | ast::StmtKind::Let { pat, expr: e } => {
                        if expr_refs_var(e, var) {
                            return true;
                        }
                        if pat_binds_var(pat) {
                            return false; // shadowed for remaining stmts
                        }
                    }
                    ast::StmtKind::Where { cond } => {
                        if expr_refs_var(cond, var) {
                            return true;
                        }
                    }
                    ast::StmtKind::GroupBy { key } => {
                        if expr_refs_var(key, var) {
                            return true;
                        }
                    }
                    ast::StmtKind::Expr(e) => {
                        if expr_refs_var(e, var) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            expr_refs_var(target, var) || expr_refs_var(value, var)
        }
        ast::ExprKind::Atomic(inner) | ast::ExprKind::Refine(inner) => expr_refs_var(inner, var),
        ast::ExprKind::TimeUnitLit { value, .. } => expr_refs_var(value, var),
        ast::ExprKind::Annot { expr: e, .. } => expr_refs_var(e, var),
        ast::ExprKind::Serve { handlers, .. } => {
            handlers.iter().any(|h| expr_refs_var(&h.body, var))
        }
    }
}

/// Like `expr_refs_var`, but a bare field access on `var` (`p.age`) does NOT
/// count as a reference: it reports only uses of `var` as a *whole value* —
/// passed to a function (`filter pred people`), yielded, compared, and so on.
///
/// A do-block bind from a relation source is ambiguous on its own: DESIGN.md's
/// `&seniors = do { people <- *people; yield (filter … people) }` binds the
/// WHOLE relation, while `do { p <- *people; where p.age > 27; yield p.name }`
/// binds each ROW. The bound name's use sites are what tell them apart, so
/// `compile_io_do_eager` pairs this with `expr_refs_var`: referenced, but never
/// as a value, means every use is a field access — a row.
///
/// Mirrors `expr_refs_var`'s shadowing rules (and its exhaustive match, so a
/// new `ExprKind` cannot be silently forgotten here).
fn expr_uses_var_as_value(expr: &ast::Expr, var: &str) -> bool {
    let pat_binds_var = |pat: &ast::Pat| pat_bound_names(pat).iter().any(|n| n == var);
    match &expr.node {
        ast::ExprKind::Var(name) => name == var,
        ast::ExprKind::Lit(_)
        | ast::ExprKind::Constructor(_)
        | ast::ExprKind::SourceRef(_)
        | ast::ExprKind::DerivedRef(_) => false,
        // `var.field` is a ROW use, not a value use — the whole point of this
        // walker. A field access on anything else still recurses (`f x . name`
        // may well pass `var` to `f`), as does a nested base (`var.a.b` has
        // `var.a` as its base, which is itself a row use of `var`).
        ast::ExprKind::FieldAccess { expr: e, .. } => {
            if matches!(&e.node, ast::ExprKind::Var(name) if name == var) {
                false
            } else {
                expr_uses_var_as_value(e, var)
            }
        }
        ast::ExprKind::App { func, arg } => {
            expr_uses_var_as_value(func, var) || expr_uses_var_as_value(arg, var)
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_uses_var_as_value(lhs, var) || expr_uses_var_as_value(rhs, var)
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_uses_var_as_value(operand, var),
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            expr_uses_var_as_value(cond, var)
                || expr_uses_var_as_value(then_branch, var)
                || expr_uses_var_as_value(else_branch, var)
        }
        ast::ExprKind::Lambda { params, body } => {
            if params.iter().any(pat_binds_var) {
                false // shadowed
            } else {
                expr_uses_var_as_value(body, var)
            }
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            expr_uses_var_as_value(scrutinee, var)
                || arms.iter().any(|arm| {
                    !pat_binds_var(&arm.pat) && expr_uses_var_as_value(&arm.body, var)
                })
        }
        ast::ExprKind::Record(fields) => {
            fields.iter().any(|f| expr_uses_var_as_value(&f.value, var))
        }
        // `{p | age: 1}` rebuilds the whole record `p`, so the base is a value
        // use even though field names appear next to it.
        ast::ExprKind::RecordUpdate { base, fields } => {
            expr_uses_var_as_value(base, var)
                || fields.iter().any(|f| expr_uses_var_as_value(&f.value, var))
        }
        ast::ExprKind::List(elems) => {
            elems.iter().any(|e| expr_uses_var_as_value(e, var))
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr: e } | ast::StmtKind::Let { pat, expr: e } => {
                        if expr_uses_var_as_value(e, var) {
                            return true;
                        }
                        if pat_binds_var(pat) {
                            return false; // shadowed for remaining stmts
                        }
                    }
                    ast::StmtKind::Where { cond } => {
                        if expr_uses_var_as_value(cond, var) {
                            return true;
                        }
                    }
                    ast::StmtKind::GroupBy { key } => {
                        if expr_uses_var_as_value(key, var) {
                            return true;
                        }
                    }
                    ast::StmtKind::Expr(e) => {
                        if expr_uses_var_as_value(e, var) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            expr_uses_var_as_value(target, var) || expr_uses_var_as_value(value, var)
        }
        ast::ExprKind::Atomic(inner) | ast::ExprKind::Refine(inner) => {
            expr_uses_var_as_value(inner, var)
        }
        ast::ExprKind::TimeUnitLit { value, .. } => {
            expr_uses_var_as_value(value, var)
        }
        ast::ExprKind::Annot { expr: e, .. } => expr_uses_var_as_value(e, var),
        ast::ExprKind::Serve { handlers, .. } => {
            handlers.iter().any(|h| expr_uses_var_as_value(&h.body, var))
        }
    }
}

// ── SQL pushdown type witnesses (shared with sql_lint) ───────────

/// Scalar type witness for SQL-pushable expressions, derived from column
/// schema types and literal shapes. Knot's type checker guarantees that
/// both operands of an arithmetic/comparison operator have the same type,
/// so a single witness anywhere in the expression types the whole tree.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SqlScalarKind {
    Int,
    Float,
    Text,
    Other,
}

/// How a pushed-down comparison over (possibly arithmetic) atoms must be
/// emitted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SqlCastMode {
    /// Integer (or text) comparison: arithmetic atoms get the
    /// `CAST(... AS TEXT) COLLATE KNOT_INT` wrapper (Knot Ints are stored
    /// as TEXT in SQLite).
    CastInt,
    /// Float comparison: emit plain numeric SQL — floats are stored as
    /// REAL and params bind as REAL, so native numeric comparison is
    /// correct. The KNOT_INT text-cast would compare floats byte-wise.
    Plain,
    /// No type witness found: only safe to push down when no atom would
    /// have received the cast wrapper (i.e. no arithmetic involved).
    NoArith,
}

/// Strip wrappers that don't affect the runtime value.
fn strip_expr_wrappers(expr: &ast::Expr) -> &ast::Expr {
    match &expr.node {
        ast::ExprKind::Annot { expr: inner, .. } => strip_expr_wrappers(inner),
        ast::ExprKind::TimeUnitLit { value, .. } => strip_expr_wrappers(value),
        _ => expr,
    }
}

/// True if `expr` is a numeric literal (possibly negated/annotated) that is
/// provably nonzero. `/` and `%` may only be pushed down to SQLite with a
/// provably nonzero divisor: SQLite yields NULL on division by zero
/// (silently dropping rows) while the Knot runtime panics.
/// Resolve an (optionally negated) integer-literal divisor to its value.
fn int_literal_divisor_value(expr: &ast::Expr) -> Option<i64> {
    let e = strip_expr_wrappers(expr);
    match &e.node {
        ast::ExprKind::UnaryOp { op: ast::UnaryOp::Neg, operand } => {
            int_literal_divisor_value(operand).and_then(i64::checked_neg)
        }
        ast::ExprKind::Lit(ast::Literal::Int(n)) => n.parse::<i64>().ok(),
        _ => None,
    }
}

/// True if `expr` is an (optionally negated) nonzero float literal.
fn float_literal_divisor_nonzero(expr: &ast::Expr) -> bool {
    let e = strip_expr_wrappers(expr);
    match &e.node {
        ast::ExprKind::UnaryOp { op: ast::UnaryOp::Neg, operand } => {
            float_literal_divisor_nonzero(operand)
        }
        ast::ExprKind::Lit(ast::Literal::Float(f)) => *f != 0.0,
        _ => false,
    }
}

pub(crate) fn divisor_is_nonzero_literal(expr: &ast::Expr) -> bool {
    if let Some(v) = int_literal_divisor_value(expr) {
        // An integer-literal divisor implies Int division. `x / -1`
        // overflows at i64::MIN (the runtime panics, SQLite silently goes
        // REAL) — keep that edge in memory.
        return v != 0 && v != -1;
    }
    float_literal_divisor_nonzero(expr)
}

/// True if `expr` is a provably nonzero *integer* literal. `%` may only be
/// pushed down for integer operands (SQLite `%` truncates to INTEGER while
/// the runtime does float fmod); an integer-literal divisor forces both
/// operands to Int through type checking. `-1` is also rejected:
/// `i64::MIN % -1` panics in memory while SQLite returns 0.
pub(crate) fn divisor_is_nonzero_int_literal(expr: &ast::Expr) -> bool {
    int_literal_divisor_value(expr).is_some_and(|v| v != 0 && v != -1)
}

fn join_sql_kinds(
    a: Option<SqlScalarKind>,
    b: Option<SqlScalarKind>,
) -> Result<Option<SqlScalarKind>, ()> {
    match (a, b) {
        (None, x) | (x, None) => Ok(x),
        (Some(x), Some(y)) if x == y => Ok(Some(x)),
        _ => Err(()), // conflicting witnesses — don't push down
    }
}

/// Compute a scalar type witness for a SQL-pushable expression.
/// `col_ty` maps `(var, field)` to the schema column type when the field
/// access resolves to a column of a bound row.
///
/// Returns `Err(())` when the expression must not be pushed down at all
/// (conflicting witnesses, or `/`/`%` without a provably nonzero literal
/// divisor). `Ok(None)` means no witness was found (params only).
pub(crate) fn sql_scalar_kind(
    expr: &ast::Expr,
    col_ty: &dyn Fn(&str, &str) -> Option<String>,
) -> Result<Option<SqlScalarKind>, ()> {
    let e = strip_expr_wrappers(expr);
    match &e.node {
        ast::ExprKind::FieldAccess { expr: inner, field } => {
            if let ast::ExprKind::Var(v) = &inner.node {
                Ok(match col_ty(v, field).as_deref() {
                    Some("int") => Some(SqlScalarKind::Int),
                    Some("float") => Some(SqlScalarKind::Float),
                    Some("text") => Some(SqlScalarKind::Text),
                    // Payload-bearing ADT fields and nested records are
                    // stored as JSON documents, but the runtime binds the
                    // corresponding Knot values differently (constructor
                    // params bind as bare tag text) — SQL comparison would
                    // silently mismatch. Never push down json columns.
                    Some("json") => return Err(()),
                    Some(_) => Some(SqlScalarKind::Other),
                    None => None, // not a column — runtime param
                })
            } else {
                Ok(None)
            }
        }
        ast::ExprKind::Lit(lit) => Ok(Some(match lit {
            ast::Literal::Int(_) => SqlScalarKind::Int,
            ast::Literal::Float(_) => SqlScalarKind::Float,
            ast::Literal::Text(_) => SqlScalarKind::Text,
            _ => SqlScalarKind::Other,
        })),
        ast::ExprKind::Var(_) => Ok(None),
        ast::ExprKind::UnaryOp { op: ast::UnaryOp::Neg, operand } => {
            sql_scalar_kind(operand, col_ty)
        }
        ast::ExprKind::BinOp { op, lhs, rhs } => match op {
            ast::BinOp::Concat => Ok(Some(SqlScalarKind::Text)),
            ast::BinOp::Add | ast::BinOp::Sub | ast::BinOp::Mul => join_sql_kinds(
                sql_scalar_kind(lhs, col_ty)?,
                sql_scalar_kind(rhs, col_ty)?,
            ),
            ast::BinOp::Div => {
                if !divisor_is_nonzero_literal(rhs) {
                    return Err(());
                }
                join_sql_kinds(sql_scalar_kind(lhs, col_ty)?, sql_scalar_kind(rhs, col_ty)?)
            }
            ast::BinOp::Mod => {
                if !divisor_is_nonzero_int_literal(rhs) {
                    return Err(());
                }
                join_sql_kinds(
                    join_sql_kinds(
                        sql_scalar_kind(lhs, col_ty)?,
                        sql_scalar_kind(rhs, col_ty)?,
                    )?,
                    Some(SqlScalarKind::Int),
                )
            }
            _ => Ok(None),
        },
        ast::ExprKind::App { func, .. } => {
            if let ast::ExprKind::Var(name) = &func.node {
                Ok(match name.as_str() {
                    "length" => Some(SqlScalarKind::Int),
                    _ => None,
                })
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

/// Decide how a pushed-down comparison between `lhs` and `rhs` must be
/// emitted, or `None` when it must fall back to in-memory evaluation.
pub(crate) fn sql_comparison_cast_mode(
    lhs: &ast::Expr,
    rhs: &ast::Expr,
    col_ty: &dyn Fn(&str, &str) -> Option<String>,
) -> Option<SqlCastMode> {
    let l = sql_scalar_kind(lhs, col_ty).ok()?;
    let r = sql_scalar_kind(rhs, col_ty).ok()?;
    let joined = join_sql_kinds(l, r).ok()?;
    Some(match joined {
        // Float comparisons must stay in memory: Knot compares floats with
        // total_cmp (-0.0 < +0.0, NaN orderable) while SQL says
        // -0.0 = 0.0 and stores NaN as NULL, so pushed comparisons would
        // silently drop NaN rows and conflate signed zeros.
        Some(SqlScalarKind::Float) => return None,
        // Text comparisons must use SQLite's default BINARY (byte-wise)
        // collation, matching Knot's Text semantics. The KNOT_INT collation
        // compares numerically, so e.g. `"0" ++ "7" == "7"` would match.
        Some(SqlScalarKind::Text) => SqlCastMode::Plain,
        Some(_) => SqlCastMode::CastInt,
        None => SqlCastMode::NoArith,
    })
}

/// True when a comparison side contains a field access on a "tag"
/// (enum-ADT) column. Ordered comparisons on tag columns must not be
/// pushed down: SQL would compare constructor names byte-wise, ignoring
/// the type's Ord (declaration order, or a user impl). Equality stays
/// pushable — tag equality IS name equality.
pub(crate) fn expr_has_tag_column(
    expr: &ast::Expr,
    col_ty: &dyn Fn(&str, &str) -> Option<String>,
) -> bool {
    let e = strip_expr_wrappers(expr);
    match &e.node {
        ast::ExprKind::FieldAccess { expr: inner, field } => {
            matches!(&inner.node, ast::ExprKind::Var(v)
                if col_ty(v, field).as_deref() == Some("tag"))
        }
        ast::ExprKind::UnaryOp { operand, .. } => expr_has_tag_column(operand, col_ty),
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            expr_has_tag_column(lhs, col_ty) || expr_has_tag_column(rhs, col_ty)
        }
        ast::ExprKind::If { then_branch, else_branch, .. } => {
            expr_has_tag_column(then_branch, col_ty)
                || expr_has_tag_column(else_branch, col_ty)
        }
        _ => false,
    }
}

// ── AST pretty-printer (for function source display) ─────────────

fn pretty_expr(expr: &ast::Expr) -> String {
    match &expr.node {
        ast::ExprKind::Lit(lit) => pretty_lit(lit),
        ast::ExprKind::Var(name) => name.clone(),
        ast::ExprKind::Constructor(name) => name.clone(),
        ast::ExprKind::SourceRef(name) => format!("*{}", name),
        ast::ExprKind::DerivedRef(name) => format!("&{}", name),
        ast::ExprKind::Record(fields) => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, pretty_expr(&f.value)))
                .collect();
            format!("{{{}}}", fs.join(", "))
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, pretty_expr(&f.value)))
                .collect();
            format!("{{{} | {}}}", pretty_expr(base), fs.join(", "))
        }
        ast::ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", pretty_expr(expr), field)
        }
        ast::ExprKind::List(elems) => {
            let es: Vec<String> = elems.iter().map(pretty_expr).collect();
            format!("[{}]", es.join(", "))
        }
        ast::ExprKind::Lambda { params, body } => {
            let ps: Vec<String> = params.iter().map(pretty_pat).collect();
            format!("\\{} -> {}", ps.join(" "), pretty_expr(body))
        }
        ast::ExprKind::App { func, arg } => {
            let f = pretty_expr(func);
            let a = pretty_expr(arg);
            let needs_parens = matches!(
                arg.node,
                ast::ExprKind::App { .. }
                    | ast::ExprKind::BinOp { .. }
                    | ast::ExprKind::Lambda { .. }
            );
            if needs_parens {
                format!("{} ({})", f, a)
            } else {
                format!("{} {}", f, a)
            }
        }
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            format!(
                "{} {} {}",
                pretty_expr(lhs),
                pretty_binop(op),
                pretty_expr(rhs)
            )
        }
        ast::ExprKind::UnaryOp { op, operand } => match op {
            ast::UnaryOp::Neg => format!("-{}", pretty_expr(operand)),
            ast::UnaryOp::Not => format!("not {}", pretty_expr(operand)),
        },
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => format!(
            "if {} then {} else {}",
            pretty_expr(cond),
            pretty_expr(then_branch),
            pretty_expr(else_branch)
        ),
        ast::ExprKind::Case { scrutinee, arms } => {
            let arm_strs: Vec<String> = arms
                .iter()
                .map(|a| format!("{} -> {}", pretty_pat(&a.pat), pretty_expr(&a.body)))
                .collect();
            format!(
                "case {} of {{ {} }}",
                pretty_expr(scrutinee),
                arm_strs.join("; ")
            )
        }
        ast::ExprKind::Do(stmts) => {
            let ss: Vec<String> = stmts.iter().map(pretty_stmt).collect();
            format!("do {{ {} }}", ss.join("; "))
        }
        ast::ExprKind::Set { target, value } => {
            format!("{} = {}", pretty_expr(target), pretty_expr(value))
        }
        ast::ExprKind::ReplaceSet { target, value } => {
            format!(
                "replace {} = {}",
                pretty_expr(target),
                pretty_expr(value)
            )
        }
        ast::ExprKind::Atomic(e) => format!("atomic ({})", pretty_expr(e)),
        ast::ExprKind::TimeUnitLit { value, .. } => pretty_expr(value),
        ast::ExprKind::Annot { expr, .. } => pretty_expr(expr),
        ast::ExprKind::Refine(inner) => format!("refine {}", pretty_expr(inner)),
        ast::ExprKind::Serve { api, handlers, .. } => {
            let hs: Vec<String> = handlers
                .iter()
                .map(|h| format!("{} = {}", h.endpoint, pretty_expr(&h.body)))
                .collect();
            format!("serve {} where {{ {} }}", api, hs.join("; "))
        }
    }
}

fn pretty_pat(pat: &ast::Pat) -> String {
    match &pat.node {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".to_string(),
        ast::PatKind::Constructor { name, payload } => {
            format!("{} {}", name, pretty_pat(payload))
        }
        ast::PatKind::Record(fields) => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| {
                    if let Some(p) = &f.pattern {
                        format!("{}: {}", f.name, pretty_pat(p))
                    } else {
                        f.name.clone()
                    }
                })
                .collect();
            format!("{{{}}}", fs.join(", "))
        }
        ast::PatKind::Lit(lit) => pretty_lit(lit),
        ast::PatKind::List(pats) => {
            let ps: Vec<String> = pats.iter().map(pretty_pat).collect();
            format!("[{}]", ps.join(", "))
        }
        ast::PatKind::Cons { head, tail } => {
            format!("Cons {} {}", pretty_pat(head), pretty_pat(tail))
        }
    }
}

fn pretty_lit(lit: &ast::Literal) -> String {
    match lit {
        ast::Literal::Int(n) => n.to_string(),
        ast::Literal::Float(n) => {
            if *n == (*n as i64) as f64 {
                format!("{:.1}", n)
            } else {
                n.to_string()
            }
        }
        ast::Literal::Text(s) => format!("\"{}\"", s),
        ast::Literal::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("b\"{}\"", hex)
        }
        ast::Literal::Bool(b) => if *b { "true" } else { "false" }.to_string(),
    }
}

fn pretty_binop(op: &ast::BinOp) -> &'static str {
    match op {
        ast::BinOp::Add => "+",
        ast::BinOp::Sub => "-",
        ast::BinOp::Mul => "*",
        ast::BinOp::Div => "/",
        ast::BinOp::Mod => "%",
        ast::BinOp::Eq => "==",
        ast::BinOp::Neq => "!=",
        ast::BinOp::Lt => "<",
        ast::BinOp::Gt => ">",
        ast::BinOp::Le => "<=",
        ast::BinOp::Ge => ">=",
        ast::BinOp::And => "&&",
        ast::BinOp::Or => "||",
        ast::BinOp::Concat => "++",
        ast::BinOp::Pipe => "|>",
    }
}

fn pretty_stmt(stmt: &ast::Stmt) -> String {
    match &stmt.node {
        ast::StmtKind::Bind { pat, expr } => {
            format!("{} <- {}", pretty_pat(pat), pretty_expr(expr))
        }
        ast::StmtKind::Let { pat, expr } => {
            format!("let {} = {}", pretty_pat(pat), pretty_expr(expr))
        }
        ast::StmtKind::Where { cond } => format!("where {}", pretty_expr(cond)),
        ast::StmtKind::GroupBy { key } => format!("groupBy {}", pretty_expr(key)),
        ast::StmtKind::Expr(e) => pretty_expr(e),
    }
}

// ── Trait support helpers ─────────────────────────────────────────

/// Find the dispatch parameter index for an HKT trait method.
/// Returns `Some(index)` if the method has a parameter whose outermost type
/// constructor is the trait's HKT variable (e.g., `f a` where `f` is the trait param).
/// Returns `None` if no parameter uses the HKT variable (e.g., `yield : a -> f a`).
fn find_dispatch_index(hkt_param: Option<&str>, type_param: Option<&str>, ty: &ast::Type) -> Option<usize> {
    // First try HKT param (e.g., `f` in `Functor (f : Type -> Type)`)
    if let Some(param_name) = hkt_param {
        let mut current = ty;
        let mut index = 0;
        while let ast::TypeKind::Function { param, result } = &current.node {
            if type_uses_hkt_var(param, param_name) {
                return Some(index);
            }
            index += 1;
            current = result;
        }
    }
    // Then try regular type param (e.g., `a` in `Eq a` or `ToJSON a`)
    // Find the first function parameter that IS the type variable
    if let Some(param_name) = type_param {
        let mut current = ty;
        let mut index = 0;
        while let ast::TypeKind::Function { param, result } = &current.node {
            if type_is_plain_var(param, param_name) {
                return Some(index);
            }
            index += 1;
            current = result;
        }
    }
    None
}

/// Check if a type is exactly a named type variable (e.g., `a` matches param_name `a`).
fn type_is_plain_var(ty: &ast::Type, param_name: &str) -> bool {
    matches!(&ty.node, ast::TypeKind::Var(name) if name == param_name)
}

/// Runtime fallback function for a trait method, if any.
/// Methods with fallbacks use the generic runtime function for types without explicit impls.
fn trait_method_fallback(method_name: &str) -> Option<&'static str> {
    match method_name {
        "eq" => Some("knot_value_eq"),
        "compare" => Some("knot_value_compare"),
        "add" => Some("knot_value_add"),
        "sub" => Some("knot_value_sub"),
        "mul" => Some("knot_value_mul"),
        "div" => Some("knot_value_div"),
        "mod" => Some("knot_value_mod"),
        "negate" => Some("knot_value_negate"),
        "append" => Some("knot_value_concat"),
        "toJson" => Some("knot_json_encode"),
        "parseJson" => Some("knot_json_decode_maybe"),
        _ => None,
    }
}

/// Whether a trait method has a runtime fallback (used for dispatcher creation).
fn has_trait_fallback(method_name: &str) -> bool {
    trait_method_fallback(method_name).is_some()
}

/// Check if a type's outermost constructor is the given HKT variable.
/// e.g., `f a` matches param_name `f`, `[a]` does not match unless param_name is `[]`.
fn type_uses_hkt_var(ty: &ast::Type, param_name: &str) -> bool {
    match &ty.node {
        ast::TypeKind::App { func, .. } => match &func.node {
            ast::TypeKind::Var(name) => name == param_name,
            _ => false,
        },
        _ => false,
    }
}

/// Count the number of function parameters from a type annotation.
/// `a -> b -> c` has 2 parameters.
fn count_fn_params(ty: &ast::Type) -> usize {
    match &ty.node {
        ast::TypeKind::Function { result, .. } => 1 + count_fn_params(result),
        _ => 0,
    }
}

/// Extract the type name from an impl's type arguments.
/// `impl Display Int` → Some("Int"), `impl Functor []` → Some("Relation").
fn impl_type_name(args: &[ast::Type]) -> Option<String> {
    if args.is_empty() {
        return None;
    }
    match &args[0].node {
        ast::TypeKind::Named(name) => {
            // Normalize `[]` (bare type constructor) to "Relation"
            if name == "[]" {
                Some("Relation".to_string())
            } else {
                Some(name.clone())
            }
        }
        ast::TypeKind::Relation(_) => Some("Relation".to_string()),
        // Partially applied type constructor, e.g. (Result e) in `impl Monad (Result e)`
        ast::TypeKind::App { func, .. } => match &func.node {
            ast::TypeKind::Named(name) => Some(name.clone()),
            _ => None,
        },
        ast::TypeKind::UnitAnnotated { base, .. } => {
            // Units are erased; resolve the base type
            impl_type_name(&[*base.clone()])
        }
        _ => None,
    }
}

/// Key for argument `i` in a trampoline curry-chain environment record.
/// Zero-padded to a fixed width so the runtime's lexicographic field order
/// matches numeric argument order — `knot_record_from_pairs` requires sorted
/// keys and `knot_record_field_by_index(env, i)` assumes index `i` ↔ arg `i`,
/// both of which break with plain decimal keys once "10" sorts before "2".
fn tramp_arg_key(i: usize) -> String {
    format!("{:04}", i)
}

/// Effective (params, body) for a trait/impl method definition. A method may
/// split its parameters arbitrarily between the explicit form and one or more
/// trailing lambdas: `eq a b = true`, `eq = \a b -> true`, and `eq a = \b -> true`
/// are all equivalent. Flatten the explicit params together with every leading
/// lambda so the impl's declared arity equals the trait signature's arrow count
/// (`count_fn_params`), which is the arity the runtime dispatcher calls with.
/// Unwrapping only the empty-params case (the previous behavior) left
/// `eq a = \b -> ...` declared as a 1-param function while the dispatcher called
/// it with 2 args — a signature mismatch / miscompile.
fn method_params_body<'a>(
    params: &'a [ast::Pat],
    body: &'a ast::Expr,
) -> (Vec<ast::Pat>, &'a ast::Expr) {
    let mut all: Vec<ast::Pat> = params.to_vec();
    let mut cur = body;
    while let ast::ExprKind::Lambda { params: lambda_params, body: lambda_body } = &cur.node {
        all.extend(lambda_params.iter().cloned());
        cur = lambda_body;
    }
    (all, cur)
}

/// Map a type name to its runtime Value tag (as used by knot_value_get_tag).
/// Every refinement predicate reachable from a route body field's declared
/// type, as `(path, type_name, predicate)`.
///
/// Only the field's own top-level type used to be checked, so a refinement
/// nested inside a list or a record — `events: [GossipEvent]` where
/// `GossipEvent` has `pubkey: Maybe PubkeyHex` — was decoded straight into the
/// handler with its predicate never run. The path tells the runtime how to
/// walk in: `events[].pubkey?` descends the list, then the field, then unwraps
/// the `Maybe` (a `Nothing` is vacuously valid). See `parse_ref_path` in the
/// runtime for the grammar.
fn collect_type_refinements(
    ty: &ast::Type,
    path: &str,
    alias_ast: &HashMap<String, ast::Type>,
    visiting: &mut Vec<String>,
    out: &mut Vec<(String, String, ast::Expr)>,
) {
    match &ty.node {
        ast::TypeKind::Refined { base, predicate } => {
            // An inline refinement has no name of its own; the path reads well
            // enough in the diagnostic ("field 'events[].score' does not
            // satisfy …"). A named one is handled by the `Named` arm below,
            // which passes the alias name through as the type name.
            out.push((path.to_string(), path.to_string(), (**predicate).clone()));
            collect_type_refinements(base, path, alias_ast, visiting, out);
        }
        ast::TypeKind::Named(name) => {
            // Guard against `type A = B; type B = A`, which `check_alias_cycles`
            // reports separately — don't recurse forever before it does.
            if visiting.iter().any(|n| n == name) {
                return;
            }
            let Some(aliased) = alias_ast.get(name) else {
                return; // primitive, data type, or unknown — nothing nested
            };
            visiting.push(name.clone());
            if let ast::TypeKind::Refined { base, predicate } = &aliased.node {
                out.push((path.to_string(), name.clone(), (**predicate).clone()));
                collect_type_refinements(base, path, alias_ast, visiting, out);
            } else {
                collect_type_refinements(aliased, path, alias_ast, visiting, out);
            }
            visiting.pop();
        }
        ast::TypeKind::Record { fields, .. } => {
            for f in fields {
                let sub = format!("{}.{}", path, f.name);
                collect_type_refinements(&f.value, &sub, alias_ast, visiting, out);
            }
        }
        ast::TypeKind::Relation(inner) => {
            let sub = format!("{}[]", path);
            collect_type_refinements(inner, &sub, alias_ast, visiting, out);
        }
        // `Maybe T` — the only type application whose payload the JSON decoder
        // unwraps positionally. Everything else (`Result e a`, user ADTs) is
        // tagged and has no single "the value" to refine.
        ast::TypeKind::App { func, arg }
            if matches!(&func.node, ast::TypeKind::Named(n) if n == "Maybe") =>
        {
            let sub = format!("{}?", path);
            collect_type_refinements(arg, &sub, alias_ast, visiting, out);
        }
        _ => {}
    }
}

pub fn type_name_to_tag(name: &str) -> Option<i64> {
    match name {
        "Int" => Some(0),
        "Float" => Some(1),
        "Text" => Some(2),
        "Bool" => Some(3),
        "Unit" => Some(4),
        "Relation" => Some(6),
        "Bytes" => Some(10),
        "IO" => Some(11),
        // `Uuid` has no distinct runtime tag — it is stored as `Value::Text`
        // (tag 2). Mapping it lets an `impl T Uuid` participate in dispatch
        // instead of being silently dropped (which would panic at runtime via
        // `knot_trait_no_impl`). A program with both `impl T Text` and
        // `impl T Uuid` is inherently ambiguous at the tag level, but the type
        // checker keeps the two from being reached by the wrong value.
        "Uuid" => Some(2),
        _ => None,
    }
}

/// Convert route path segments to a pattern string like "/todos/{owner:text}".
fn path_segments_to_pattern(
    segments: &[ast::PathSegment],
    aliases: &std::collections::HashMap<String, ResolvedType>,
) -> String {
    let mut parts = Vec::new();
    for seg in segments {
        match seg {
            ast::PathSegment::Literal(s) => parts.push(s.clone()),
            ast::PathSegment::Param { name, ty } => {
                let ty_str = ast_type_to_descriptor_type(ty, aliases);
                parts.push(format!("{{{name}:{ty_str}}}"));
            }
        }
    }
    format!("/{}", parts.join("/"))
}

/// Convert typed fields to a descriptor string like "name:text,age:int".
fn fields_to_descriptor(
    fields: &[ast::Field<ast::Type>],
    aliases: &std::collections::HashMap<String, ResolvedType>,
) -> String {
    fields
        .iter()
        .map(|f| {
            let ty_str = ast_type_to_descriptor_type(&f.value, aliases);
            format!("{}:{}", f.name, ty_str)
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Wire descriptor for an enum-like (all-nullary) ADT route field:
/// `tag(Low|Medium|High|Critical)`. The constructor list travels with the
/// descriptor so the runtime can reject an undeclared tag on the wire with HTTP
/// 400. A bare `tag` would leave it no way to tell `Fake` from `Low`, and the
/// forged constructor would reach the handler and panic its exhaustive `case`.
fn tag_descriptor(ctors: &[(String, Vec<(String, ResolvedType)>)]) -> String {
    let names: Vec<&str> = ctors.iter().map(|(n, _)| n.as_str()).collect();
    format!("tag({})", names.join("|"))
}

fn ast_type_to_descriptor_type(
    ty: &ast::Type,
    aliases: &std::collections::HashMap<String, ResolvedType>,
) -> String {
    match &ty.node {
        ast::TypeKind::Named(n) => match n.as_str() {
            "Int" => "int".to_string(),
            "Float" => "float".to_string(),
            "Bool" => "bool".to_string(),
            "Text" => "text".to_string(),
            _ => match aliases.get(n) {
                // A type alias to a primitive must carry the primitive's wire
                // type — otherwise a route field typed `type Cents = Int` gets
                // a `text` descriptor and the request side (de)serializes it as
                // Text, inconsistent with the response side which resolves the
                // alias via `resolve_type_for_descriptor`.
                Some(ResolvedType::Int) => "int".to_string(),
                Some(ResolvedType::Float) => "float".to_string(),
                Some(ResolvedType::Bool) => "bool".to_string(),
                Some(ResolvedType::Text) => "text".to_string(),
                Some(ResolvedType::Adt(ctors))
                    if ctors.iter().all(|(_, fields)| fields.is_empty()) =>
                {
                    tag_descriptor(ctors)
                }
                // An alias to a compound shape (record, relation, Maybe, or a
                // non-nullary ADT) must carry its structural descriptor —
                // otherwise the request side serializes it as `text` and the
                // runtime rejects valid JSON-object/array bodies with HTTP 400,
                // and aliased `Maybe` positions lose their `?` marker (so `null`
                // fails to decode to `Nothing`). The response side already
                // resolves these via `resolve_type_for_descriptor`; mirror it.
                Some(resolved @ (ResolvedType::Record(_)
                | ResolvedType::Relation(_)
                | ResolvedType::Adt(_))) => resolved_type_to_descriptor(resolved),
                _ => "text".to_string(),
            },
        },
        ast::TypeKind::App { func, arg } => {
            if matches!(&func.node, ast::TypeKind::Named(n) if n == "Maybe") {
                format!("?{}", ast_type_to_descriptor_type(arg, aliases))
            } else {
                "text".to_string()
            }
        }
        // Structural descriptors for nested records/relations so the runtime
        // can normalize Maybe positions inside request bodies (`null`/absent
        // → Nothing, present → Just) via `apply_wire_type`.
        ast::TypeKind::Record { fields, .. } => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name, ast_type_to_descriptor_type(&f.value, aliases)))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        ast::TypeKind::Relation(inner) => {
            format!("[{}]", ast_type_to_descriptor_type(inner, aliases))
        }
        ast::TypeKind::UnitAnnotated { base, .. } => ast_type_to_descriptor_type(base, aliases),
        // A refined field (`amount: Int 1 where \x -> x > 0`) is transparent for
        // the wire: it must carry the base type's descriptor, not the `text`
        // fallback — otherwise valid JSON numbers/objects are (de)serialized as
        // Text, inconsistent with the refinement validators codegen already
        // registers for the same fields, yielding spurious HTTP 400s.
        ast::TypeKind::Refined { base, .. } => ast_type_to_descriptor_type(base, aliases),
        _ => "text".to_string(),
    }
}

/// Convert an optional response type to a descriptor string for OpenAPI generation.
///
/// Format: `int`, `float`, `text`, `bool`, `unit`,
///         `[<inner>]` for relations/arrays,
///         `{name:type,name:type}` for records.
fn response_type_descriptor(
    ty: &Option<ast::Type>,
    aliases: &std::collections::HashMap<String, ResolvedType>,
) -> String {
    match ty {
        None => String::new(),
        Some(t) => {
            let resolved = resolve_type_for_descriptor(t, aliases);
            resolved_type_to_descriptor(&resolved)
        }
    }
}

fn resolve_type_for_descriptor(
    ty: &ast::Type,
    aliases: &std::collections::HashMap<String, ResolvedType>,
) -> ResolvedType {
    match &ty.node {
        ast::TypeKind::Named(n) => match n.as_str() {
            "Int" => ResolvedType::Int,
            "Float" => ResolvedType::Float,
            "Bool" => ResolvedType::Bool,
            "Text" => ResolvedType::Text,
            _ => aliases
                .get(n)
                .cloned()
                .unwrap_or(ResolvedType::Named(n.clone())),
        },
        ast::TypeKind::Record { fields, .. } => {
            let resolved: Vec<(String, ResolvedType)> = fields
                .iter()
                .map(|f| {
                    (f.name.clone(), resolve_type_for_descriptor(&f.value, aliases))
                })
                .collect();
            ResolvedType::Record(resolved)
        }
        ast::TypeKind::Relation(inner) => {
            ResolvedType::Relation(Box::new(
                resolve_type_for_descriptor(inner, aliases),
            ))
        }
        ast::TypeKind::App { func, arg } => {
            // Inline `Maybe T` — resolve to the built-in Maybe ADT shape so
            // the descriptor marks the position as `?<inner>` (wire `null`).
            if matches!(&func.node, ast::TypeKind::Named(n) if n == "Maybe") {
                ResolvedType::Adt(vec![
                    ("Nothing".into(), vec![]),
                    (
                        "Just".into(),
                        vec![("value".into(), resolve_type_for_descriptor(arg, aliases))],
                    ),
                ])
            } else {
                ResolvedType::Text
            }
        }
        ast::TypeKind::UnitAnnotated { base, .. } => resolve_type_for_descriptor(base, aliases),
        // Refined types are transparent for the wire descriptor; recurse into
        // the base so the response side agrees with the request side.
        ast::TypeKind::Refined { base, .. } => resolve_type_for_descriptor(base, aliases),
        _ => ResolvedType::Text,
    }
}

fn resolved_type_to_descriptor(ty: &ResolvedType) -> String {
    match ty {
        ResolvedType::Int => "int".to_string(),
        ResolvedType::Float => "float".to_string(),
        ResolvedType::Bool => "bool".to_string(),
        ResolvedType::Text => "text".to_string(),
        ResolvedType::Unit => "unit".to_string(),
        ResolvedType::Record(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|(name, ty)| format!("{}:{}", name, resolved_type_to_descriptor(ty)))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        ResolvedType::Relation(inner) => {
            format!("[{}]", resolved_type_to_descriptor(inner))
        }
        ResolvedType::Adt(ctors) => {
            // The built-in Maybe shape encodes as `null`/bare value on the
            // wire — descriptor `?<inner>` so the client can Just-wrap.
            if let [(a, af), (b, bf)] = ctors.as_slice() {
                let maybe_inner = match (a.as_str(), b.as_str()) {
                    ("Nothing", "Just") if af.is_empty() => Some(bf),
                    ("Just", "Nothing") if bf.is_empty() => Some(af),
                    _ => None,
                };
                if let Some(just_fields) = maybe_inner
                    && let [(fname, fty)] = just_fields.as_slice()
                        && fname == "value" {
                            return format!("?{}", resolved_type_to_descriptor(fty));
                        }
            }
            // Represent ADT as object with _tag + all constructor fields.
            // Seed `seen` with the synthetic `_tag` so a constructor field
            // literally named `_tag` can't emit a duplicate descriptor entry
            // (the synthetic tag column wins).
            let mut fields: Vec<String> = vec!["_tag:text".to_string()];
            let mut seen = std::collections::HashSet::<String>::new();
            seen.insert("_tag".to_string());
            for (_ctor_name, ctor_fields) in ctors {
                for (fname, fty) in ctor_fields {
                    if seen.insert(fname.clone()) {
                        fields.push(format!("{}:{}", fname, resolved_type_to_descriptor(fty)));
                    }
                }
            }
            format!("{{{}}}", fields.join(","))
        }
        ResolvedType::Named(n) => n.to_lowercase(),
        _ => "text".to_string(),
    }
}

/// A literal value extracted from an AST expression, for compile-time
/// refinement checking.
enum CompileLit {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl CompileLit {
    fn display(&self) -> String {
        match self {
            CompileLit::Int(n) => n.to_string(),
            CompileLit::Float(f) => f.to_string(),
            CompileLit::Text(s) => format!("{:?}", s),
            CompileLit::Bool(b) => b.to_string(),
        }
    }
}

/// Extract a literal from an AST expression (peeling annotations).
fn extract_literal(expr: &ast::Expr) -> Option<CompileLit> {
    match &expr.node {
        ast::ExprKind::Lit(ast::Literal::Int(s)) => s.parse::<i64>().ok().map(CompileLit::Int),
        ast::ExprKind::Lit(ast::Literal::Float(f)) => Some(CompileLit::Float(*f)),
        ast::ExprKind::Lit(ast::Literal::Text(s)) => Some(CompileLit::Text(s.clone())),
        ast::ExprKind::Lit(ast::Literal::Bool(b)) => Some(CompileLit::Bool(*b)),
        ast::ExprKind::Annot { expr, .. } => extract_literal(expr),
        _ => None,
    }
}

/// Evaluate a refinement predicate (a lambda `\x -> pred`) against a
/// compile-time literal. Returns `Some(true)` if the predicate is
/// satisfied, `Some(false)` if it fails, or `None` if it can't be
/// evaluated at compile time.
fn eval_refine_predicate(pred: &ast::Expr, lit: &CompileLit) -> Option<bool> {
    // The predicate is a lambda `\\x -> body`. Extract the parameter name
    // and body so we only substitute for the actual parameter, not any
    // other variable the predicate may reference (e.g. a top-level constant).
    let (param_name, body) = match &pred.node {
        ast::ExprKind::Lambda { params, body } => {
            if params.len() != 1 { return None; }
            let name = match &params[0].node {
                ast::PatKind::Var(n) => n.clone(),
                _ => return None,
            };
            (name, body)
        }
        _ => return None,
    };
    // Evaluate the body with the parameter bound to the literal.
    eval_expr_bool(body, lit, &param_name)
}

/// Evaluate an expression to a boolean, with the refinement variable
/// `param_name` bound to `lit`. Returns `None` if the expression can't be
/// evaluated (e.g. references a variable other than `param_name`).
fn eval_expr_bool(expr: &ast::Expr, lit: &CompileLit, param_name: &str) -> Option<bool> {
    match &expr.node {
        ast::ExprKind::Lit(ast::Literal::Bool(b)) => Some(*b),
        ast::ExprKind::BinOp { op, lhs, rhs, .. } => {
            let lv = eval_expr_num(lhs, lit, param_name)?;
            let rv = eval_expr_num(rhs, lit, param_name)?;
            match op {
                ast::BinOp::Lt => Some(lv < rv),
                ast::BinOp::Gt => Some(lv > rv),
                ast::BinOp::Le => Some(lv <= rv),
                ast::BinOp::Ge => Some(lv >= rv),
                ast::BinOp::Eq => Some(lv == rv),
                ast::BinOp::Neq => Some(lv != rv),
                ast::BinOp::And => {
                    Some(eval_expr_bool(lhs, lit, param_name)? && eval_expr_bool(rhs, lit, param_name)?)
                }
                ast::BinOp::Or => {
                    Some(eval_expr_bool(lhs, lit, param_name)? || eval_expr_bool(rhs, lit, param_name)?)
                }
                _ => None,
            }
        }
        ast::ExprKind::UnaryOp { op: ast::UnaryOp::Not, operand, .. } => {
            Some(!eval_expr_bool(operand, lit, param_name)?)
        }
        _ => None,
    }
}

/// Evaluate an expression to an f64 (for numeric comparisons), with the
/// refinement variable `param_name` bound to `lit`. Returns `None` for any
/// variable that isn't the refinement parameter.
fn eval_expr_num(expr: &ast::Expr, lit: &CompileLit, param_name: &str) -> Option<f64> {
    match &expr.node {
        ast::ExprKind::Lit(ast::Literal::Int(s)) => s.parse::<f64>().ok(),
        ast::ExprKind::Lit(ast::Literal::Float(f)) => Some(*f),
        ast::ExprKind::Var(name) if name == param_name => {
            // The lambda parameter — return the literal value
            match lit {
                CompileLit::Int(n) => Some(*n as f64),
                CompileLit::Float(f) => Some(*f),
                _ => None,
            }
        }
        ast::ExprKind::Var(_) => {
            // A different variable (e.g. a top-level constant) — can't
            // evaluate at compile time, fall back to runtime check.
            None
        }
        ast::ExprKind::Annot { expr, .. } => eval_expr_num(expr, lit, param_name),
        ast::ExprKind::BinOp { op, lhs, rhs, .. } => {
            let lv = eval_expr_num(lhs, lit, param_name)?;
            let rv = eval_expr_num(rhs, lit, param_name)?;
            match op {
                ast::BinOp::Add => Some(lv + rv),
                ast::BinOp::Sub => Some(lv - rv),
                ast::BinOp::Mul => Some(lv * rv),
                ast::BinOp::Div => Some(lv / rv),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn preds_for(source: &str, bv: &str) -> Option<Vec<StmFieldPred>> {
        // Parse a Knot expression and run the STM predicate extractor on it.
        let expr = parse_expr(source);
        try_extract_field_preds(bv, &expr)
    }

    #[test]
    fn stm_preds_extract_single_eq() {
        let preds = preds_for("r.id == 5", "r").unwrap();
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].col, "id");
        assert!(matches!(preds[0].op, StmCmpOp::Eq));
        assert_eq!(serialize_stm_preds(&preds), "id:=:0");
    }

    #[test]
    fn stm_preds_extract_cmp_ops() {
        for (src, op_str) in &[
            ("r.qty > 100", ">"),
            ("r.qty >= 100", ">="),
            ("r.qty < 100", "<"),
            ("r.qty <= 100", "<="),
            ("r.qty != 100", "!="),
        ] {
            let preds = preds_for(src, "r").unwrap_or_else(|| panic!("{src}"));
            assert_eq!(serialize_stm_preds(&preds), format!("qty:{}:0", op_str));
        }
    }

    #[test]
    fn stm_preds_reverse_value_then_field() {
        // 100 < r.qty  ↦  qty > 100
        let preds = preds_for("100 < r.qty", "r").unwrap();
        assert_eq!(serialize_stm_preds(&preds), "qty:>:0");
    }

    #[test]
    fn stm_preds_and_chain() {
        let preds = preds_for(r#"r.status == "open" && r.qty > 100"#, "r").unwrap();
        assert_eq!(preds.len(), 2);
        assert_eq!(serialize_stm_preds(&preds), "status:=:0;qty:>:1");
    }

    #[test]
    fn stm_preds_or_rejected() {
        // OR breaks the conjunction model — fall back to All.
        assert!(preds_for(r#"r.status == "open" || r.qty > 100"#, "r").is_none());
    }

    #[test]
    fn stm_preds_arithmetic_value_rejected() {
        // r.qty > a + b — value side is arithmetic; reject to avoid double-eval.
        assert!(preds_for("r.qty > a + b", "r").is_none());
    }

    #[test]
    fn stm_preds_function_call_rejected() {
        // length(r.name) > 5 — function call on field side; reject.
        assert!(preds_for("length r.name > 5", "r").is_none());
    }

    #[test]
    fn stm_preds_in_literal_list() {
        let preds = preds_for("elem r.id [1, 2, 3]", "r").unwrap();
        assert_eq!(preds.len(), 1);
        assert!(matches!(preds[0].op, StmCmpOp::In));
        assert_eq!(preds[0].values.len(), 3);
        assert_eq!(serialize_stm_preds(&preds), "id:in:0,1,2");
    }

    #[test]
    fn stm_preds_in_empty_list_rejected() {
        assert!(preds_for("elem r.id []", "r").is_none());
    }

    #[test]
    fn stm_preds_mixed_indices_match_sql_param_order() {
        // The walker should produce predicates with indices that align with
        // try_compile_sql_expr's param ordering — left-to-right, one per
        // simple comparison.
        let preds =
            preds_for(r#"r.a == 1 && r.b == 2 && r.c == 3"#, "r").unwrap();
        assert_eq!(serialize_stm_preds(&preds), "a:=:0;b:=:1;c:=:2");
    }

    fn parse_expr(source: &str) -> ast::Expr {
        // Wrap the source as a top-level binding so it parses as a module
        // declaration; pull the body back out.
        let module_src = format!("__test = {}\n", source);
        let lexer = knot::lexer::Lexer::new(&module_src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(module_src.clone(), tokens);
        let (module, _) = parser.parse_module();
        for decl in module.decls {
            if let ast::DeclKind::Fun { body: Some(body), .. } = decl.node {
                return body;
            }
        }
        panic!("parse_expr: expected a single function declaration");
    }

    /// `-1` divisors are rejected (i64::MIN / -1 and i64::MIN % -1 overflow
    /// in memory while SQLite silently produces REAL / 0); other nonzero
    /// literals stay pushable.
    #[test]
    fn divisor_negative_one_not_pushable() {
        assert!(!divisor_is_nonzero_int_literal(&parse_expr("-1")));
        assert!(!divisor_is_nonzero_literal(&parse_expr("-1")));
        assert!(divisor_is_nonzero_int_literal(&parse_expr("-2")));
        assert!(divisor_is_nonzero_int_literal(&parse_expr("3")));
        assert!(!divisor_is_nonzero_int_literal(&parse_expr("0")));
        assert!(divisor_is_nonzero_literal(&parse_expr("-1.0")));
        assert!(!divisor_is_nonzero_literal(&parse_expr("0.0")));
    }

    /// `Var(x)` resolves through `let_bindings` first, falling back to
    /// `fun_bodies` when no local binding exists.  Local entries shadow
    /// top-level functions of the same name.
    #[test]
    fn beta_reduce_folds_through_let_bindings() {
        let value = parse_expr("merged");
        let union_call = parse_expr("union 1 2");
        let mut let_bindings = HashMap::new();
        let_bindings.insert("merged".to_string(), union_call);
        let fun_bodies = HashMap::new();

        let reduced = beta_reduce(&value, &fun_bodies, &let_bindings);
        match &reduced.node {
            ast::ExprKind::App { func, arg: _ } => match &func.node {
                ast::ExprKind::App { func: inner, .. } => match &inner.node {
                    ast::ExprKind::Var(name) => assert_eq!(name, "union"),
                    other => panic!("expected Var(union), got {:?}", other),
                },
                other => panic!("expected nested App, got {:?}", other),
            },
            other => panic!("expected App after inlining, got {:?}", other),
        }
    }

    #[test]
    fn beta_reduce_local_shadows_fun_bodies() {
        let value = parse_expr("foo");
        let mut fun_bodies = HashMap::new();
        fun_bodies.insert("foo".to_string(), parse_expr("1"));
        let mut let_bindings = HashMap::new();
        let_bindings.insert("foo".to_string(), parse_expr("2"));

        let reduced = beta_reduce(&value, &fun_bodies, &let_bindings);
        match &reduced.node {
            ast::ExprKind::Lit(ast::Literal::Int(n)) => assert_eq!(n, "2"),
            other => panic!("expected literal 2, got {:?}", other),
        }
    }

    /// A non-recursive top-level function is still inlined, so the SQL
    /// matchers see through a named predicate to its definition.
    #[test]
    fn beta_reduce_inlines_non_recursive_functions() {
        let mut fun_bodies = HashMap::new();
        fun_bodies.insert("isAdult".to_string(), parse_expr("\\p -> p.age > 18"));

        let reduced = beta_reduce(&parse_expr("isAdult r"), &fun_bodies, &HashMap::new());
        match &reduced.node {
            ast::ExprKind::BinOp { op: ast::BinOp::Gt, lhs, .. } => match &lhs.node {
                ast::ExprKind::FieldAccess { field, .. } => assert_eq!(field, "age"),
                other => panic!("expected r.age on the left, got {:?}", other),
            },
            other => panic!("expected the predicate body, got {:?}", other),
        }
    }

    /// Issue #71: a self-recursive function must be left alone. Substituting
    /// its body reintroduces the call, and every unroll copies the argument
    /// into each occurrence of the parameter, so the term grows multiplicatively
    /// and the reduction never finishes — the compiler used to hang here.
    #[test]
    fn beta_reduce_leaves_self_recursive_calls_unexpanded() {
        let mut fun_bodies = HashMap::new();
        fun_bodies.insert(
            "afterChar".to_string(),
            parse_expr(
                "\\sep s -> if s == \"\" then \"\" \
                 else if take 1 s == sep then drop 1 s \
                 else afterChar sep (drop 1 s)",
            ),
        );

        // Partially applied (`afterChar ","`), which is the shape that made the
        // reduction blow up: the remaining binder is reduced under, unrolling
        // the recursive call before the outer argument is substituted in.
        let reduced = beta_reduce(&parse_expr("afterChar \",\" s"), &fun_bodies, &HashMap::new());
        match &reduced.node {
            ast::ExprKind::App { func, .. } => match &func.node {
                ast::ExprKind::App { func: inner, .. } => match &inner.node {
                    ast::ExprKind::Var(name) => assert_eq!(name, "afterChar"),
                    other => panic!("expected Var(afterChar), got {:?}", other),
                },
                other => panic!("expected nested App, got {:?}", other),
            },
            other => panic!("expected the call left in place, got {:?}", other),
        }
    }

    /// Mutual recursion is a cycle in the call graph just the same, and unrolls
    /// just as endlessly.
    #[test]
    fn beta_reduce_leaves_mutually_recursive_calls_unexpanded() {
        let mut fun_bodies = HashMap::new();
        fun_bodies.insert(
            "isEven".to_string(),
            parse_expr("\\n -> if n == 0 then true else isOdd (n - 1)"),
        );
        fun_bodies.insert(
            "isOdd".to_string(),
            parse_expr("\\n -> if n == 0 then false else isEven (n - 1)"),
        );

        let reduced = beta_reduce(&parse_expr("isEven k"), &fun_bodies, &HashMap::new());
        match &reduced.node {
            ast::ExprKind::App { func, .. } => match &func.node {
                ast::ExprKind::Var(name) => assert_eq!(name, "isEven"),
                other => panic!("expected Var(isEven), got {:?}", other),
            },
            other => panic!("expected the call left in place, got {:?}", other),
        }
    }

    /// A let body whose value also references another let entry is
    /// resolved transitively; the matchers ultimately see the fully
    /// expanded shape.
    #[test]
    fn beta_reduce_chains_through_nested_lets() {
        let value = parse_expr("outer");
        let mut let_bindings = HashMap::new();
        let_bindings.insert("inner".to_string(), parse_expr("union 1 2"));
        let_bindings.insert("outer".to_string(), parse_expr("inner"));
        let fun_bodies = HashMap::new();

        let reduced = beta_reduce(&value, &fun_bodies, &let_bindings);
        match &reduced.node {
            ast::ExprKind::App { func, .. } => match &func.node {
                ast::ExprKind::App { func: inner, .. } => match &inner.node {
                    ast::ExprKind::Var(n) => assert_eq!(n, "union"),
                    other => panic!("expected Var(union), got {:?}", other),
                },
                other => panic!("expected nested App, got {:?}", other),
            },
            other => panic!("expected union App, got {:?}", other),
        }
    }
}
