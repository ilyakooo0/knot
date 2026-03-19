//! Cranelift-based code generator for the Knot language.
//!
//! Compiles a Knot AST into a native object file. All Knot values are
//! represented at the machine level as pointers to heap-allocated tagged
//! values (managed by the runtime). The generated code calls into runtime
//! functions for value construction, operations, and SQLite persistence.

use crate::infer::{MonadInfo, MonadKind};
use crate::types::{ResolvedType, TypeEnv};
use cranelift_codegen::ir::condcodes::IntCC;
use cranelift_codegen::ir::types;
use cranelift_codegen::ir::{AbiParam, InstBuilder, Value};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_codegen::Context;
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{DataDescription, DataId, FuncId, Linkage, Module};
use cranelift_object::{ObjectBuilder, ObjectModule};
use knot::ast;
use std::collections::{HashMap, HashSet};


// ── Codegen state ─────────────────────────────────────────────────

pub struct Codegen {
    module: ObjectModule,
    ctx: Context,
    builder_ctx: FunctionBuilderContext,
    ptr_type: types::Type,

    // Interned string constants
    strings: HashMap<String, DataId>,
    string_counter: usize,

    // Runtime function declarations (imported)
    runtime_fns: HashMap<String, FuncId>,

    // User function declarations: name -> (func_id, param_count)
    user_fns: HashMap<String, (FuncId, usize)>,

    // Source relation schemas: name -> schema descriptor
    source_schemas: HashMap<String, String>,

    // Constructor info: ctor_name -> [(field_name, field_type_str)]
    constructors: HashMap<String, Vec<(String, String)>>,

    // Counter for generating unique lambda names
    lambda_counter: usize,

    // Pending lambda definitions: (func_id, params, body, free_vars)
    pending_lambdas: Vec<PendingLambda>,

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

    // Sources with `with history` enabled
    history_sources: HashSet<String>,

    // Subset constraints: (sub, sup) relation paths
    subset_constraints: Vec<(knot::ast::RelationPath, knot::ast::RelationPath)>,

    // Names of derived relations that are self-referencing (recursive)
    recursive_derived: HashSet<String>,

    // Body function IDs for recursive derived relations: name -> func_id
    recursive_body_fns: HashMap<String, FuncId>,

    // Route entries: route_name -> entries (for HTTP codegen)
    route_entries: HashMap<String, Vec<ast::RouteEntry>>,

    // Type aliases for resolving response types in OpenAPI descriptors
    type_aliases: HashMap<String, ResolvedType>,

    // Trampolines for user functions used as values: fn_name -> trampoline_func_id
    user_fn_trampolines: HashMap<String, FuncId>,

    // Resolved monad types for desugared do-blocks (from type inference)
    monad_info: MonadInfo,

    // Builtin relation impls that were actually registered (not already provided by user/prelude)
    registered_builtin_impls: HashSet<String>,

    // Nullable-encoded ADTs: ctor_name -> NullableInfo
    // Types isomorphic to Maybe (one nullary ctor, one non-nullary ctor)
    // are encoded as nullable pointers: null = none variant, bare payload = some variant.
    nullable_ctors: HashMap<String, NullableRole>,
}

/// Role of a constructor in a nullable-encoded ADT.
#[derive(Clone, Debug)]
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
    body: ast::Expr,
    free_vars: Vec<String>,
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

    fn get(&self, name: &str) -> Value {
        *self.bindings.get(name).unwrap_or_else(|| {
            panic!("codegen: undefined variable '{}'", name);
        })
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
}

// ── Public API ────────────────────────────────────────────────────

pub fn compile(
    module: &ast::Module,
    type_env: &TypeEnv,
    source_file: &str,
    monad_info: &MonadInfo,
) -> Result<Vec<u8>, Vec<knot::diagnostic::Diagnostic>> {
    let mut cg = Codegen::new();
    // Derive database path from source filename: "foo.knot" → "foo.db"
    let stem = std::path::Path::new(source_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("knot");
    cg.db_path = format!("{}.db", stem);
    cg.source_schemas = type_env.source_schemas.clone();
    cg.migrate_schemas = type_env.migrate_schemas.clone();
    cg.type_aliases = type_env.aliases.clone();
    cg.history_sources = type_env.history_sources.clone();
    cg.subset_constraints = type_env.subset_constraints.clone();
    cg.monad_info = monad_info.clone();
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
            if let Some(info) = analyze_view(body) {
                cg.views.insert(name.clone(), info);
            }
        }
    }
    cg.collect_declarations(module);
    cg.define_functions(module, type_env);
    cg.generate_main(module);
    // Drain lambdas created by generate_main (e.g., migration functions)
    while !cg.pending_lambdas.is_empty() {
        let lambdas: Vec<PendingLambda> = std::mem::take(&mut cg.pending_lambdas);
        for lambda in lambdas {
            cg.define_lambda_function(&lambda);
        }
    }
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
        let isa_builder =
            cranelift_native::builder().expect("failed to detect host CPU");
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
        let module = ObjectModule::new(builder);

        Self {
            ctx: module.make_context(),
            module,
            builder_ctx: FunctionBuilderContext::new(),
            ptr_type,
            strings: HashMap::new(),
            string_counter: 0,
            runtime_fns: HashMap::new(),
            user_fns: HashMap::new(),
            source_schemas: HashMap::new(),
            constructors: HashMap::new(),
            lambda_counter: 0,
            pending_lambdas: Vec::new(),
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
            history_sources: HashSet::new(),
            subset_constraints: Vec::new(),
            recursive_derived: HashSet::new(),
            recursive_body_fns: HashMap::new(),
            route_entries: HashMap::new(),
            type_aliases: HashMap::new(),
            user_fn_trampolines: HashMap::new(),
            monad_info: HashMap::new(),
            registered_builtin_impls: HashSet::new(),
            nullable_ctors: HashMap::new(),
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
        self.declare_rt("knot_record_update", &[p], &[p]);

        // Relation operations
        self.declare_rt("knot_relation_empty", &[], &[p]);
        self.declare_rt("knot_relation_singleton", &[p], &[p]);
        self.declare_rt("knot_relation_push", &[p, p], &[]);
        self.declare_rt("knot_relation_len", &[p], &[p]);
        self.declare_rt("knot_relation_get", &[p, p], &[p]);
        self.declare_rt("knot_relation_union", &[p, p, p], &[p]);

        // Binary operations
        self.declare_rt("knot_value_add", &[p, p], &[p]);
        self.declare_rt("knot_value_sub", &[p, p], &[p]);
        self.declare_rt("knot_value_mul", &[p, p], &[p]);
        self.declare_rt("knot_value_div", &[p, p], &[p]);
        self.declare_rt("knot_value_eq", &[p, p], &[p]);
        self.declare_rt("knot_value_neq", &[p, p], &[p]);
        self.declare_rt("knot_value_lt", &[p, p], &[p]);
        self.declare_rt("knot_value_gt", &[p, p], &[p]);
        self.declare_rt("knot_value_le", &[p, p], &[p]);
        self.declare_rt("knot_value_ge", &[p, p], &[p]);
        self.declare_rt("knot_value_and", &[p, p], &[p]);
        self.declare_rt("knot_value_or", &[p, p], &[p]);
        self.declare_rt("knot_value_concat", &[p, p], &[p]);

        // Comparison (returns Ordering ADT)
        self.declare_rt("knot_value_compare", &[p, p], &[p]);

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
        self.declare_rt("knot_source_write", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_append", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_diff_write", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_delete_where", &[p, p, p, p, p, p], &[]);
        self.declare_rt("knot_source_update_where", &[p, p, p, p, p, p, p, p], &[]);

        // Schema tracking
        self.declare_rt("knot_schema_init", &[p], &[]);
        self.declare_rt("knot_source_migrate", &[p, p, p, p, p, p, p, p], &[]);

        // Debug
        self.declare_rt("knot_debug_init", &[], &[]);

        // Transactions
        self.declare_rt("knot_atomic_begin", &[p], &[]);
        self.declare_rt("knot_atomic_commit", &[p], &[]);

        // View operations
        self.declare_rt("knot_view_read", &[p, p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_relation_add_fields", &[p, p], &[p]);
        self.declare_rt("knot_view_write", &[p, p, p, p, p, p, p, p, p], &[]);

        // Constructor matching
        self.declare_rt("knot_constructor_matches", &[p, p, p], &[types::I32]);
        self.declare_rt("knot_constructor_payload", &[p], &[p]);
        self.declare_rt("knot_ensure_relation", &[p], &[p]);

        // Trait dispatch error
        self.declare_rt("knot_trait_no_impl", &[p, p, p], &[p]);

        // Type tag inspection (for trait dispatch)
        self.declare_rt("knot_value_get_tag", &[p], &[types::I32]);

        // Random number generation
        self.declare_rt("knot_random_int", &[p], &[p]);
        self.declare_rt("knot_random_float", &[], &[p]);

        // Elliptic curve cryptography
        self.declare_rt("knot_crypto_generate_key_pair", &[], &[p]);
        self.declare_rt("knot_crypto_generate_signing_key_pair", &[], &[p]);
        self.declare_rt("knot_crypto_encrypt", &[p, p], &[p]);
        self.declare_rt("knot_crypto_decrypt", &[p, p], &[p]);
        self.declare_rt("knot_crypto_sign", &[p, p], &[p]);
        self.declare_rt("knot_crypto_verify", &[p, p, p, p], &[p]);

        // Temporal queries (history)
        self.declare_rt("knot_now", &[], &[p]);
        self.declare_rt("knot_history_init", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_history_snapshot", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_source_read_at", &[p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_view_read_at", &[p, p, p, p, p, p, p, p, p], &[p]);

        // Subset constraints
        self.declare_rt("knot_constraint_register", &[p, p, p, p, p, p, p, p, p], &[]);

        // Monadic bind for relations (do-desugaring)
        self.declare_rt("knot_relation_bind", &[p, p, p], &[p]);

        // GroupBy: group relation by key columns using SQLite ORDER BY
        self.declare_rt("knot_relation_group_by", &[p, p, p, p, p, p], &[p]);

        // Standard library: relation operations
        self.declare_rt("knot_relation_filter", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_match", &[p, p], &[p]);
        self.declare_rt("knot_source_match", &[p, p, p, p, p, p, p], &[p]);
        self.declare_rt("knot_relation_map", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_ap", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_fold", &[p, p, p, p], &[p]);
        self.declare_rt("knot_relation_single", &[p], &[p]);
        self.declare_rt("knot_relation_diff", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_inter", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_sum", &[p, p, p], &[p]);
        self.declare_rt("knot_relation_avg", &[p, p, p], &[p]);

        // Standard library: text operations
        self.declare_rt("knot_text_to_upper", &[p], &[p]);
        self.declare_rt("knot_text_to_lower", &[p], &[p]);
        self.declare_rt("knot_text_take", &[p, p], &[p]);
        self.declare_rt("knot_text_drop", &[p, p], &[p]);
        self.declare_rt("knot_text_length", &[p], &[p]);
        self.declare_rt("knot_text_trim", &[p], &[p]);
        self.declare_rt("knot_text_contains", &[p, p], &[p]);
        self.declare_rt("knot_text_reverse", &[p], &[p]);
        self.declare_rt("knot_text_chars", &[p], &[p]);

        // Standard library: utility
        self.declare_rt("knot_value_id", &[p], &[p]);
        self.declare_rt("knot_value_not_fn", &[p], &[p]);

        // Standard library: JSON
        self.declare_rt("knot_json_encode", &[p], &[p]);
        self.declare_rt("knot_json_decode", &[p], &[p]);

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

        // HTTP server (routes)
        self.declare_rt("knot_route_table_new", &[], &[p]);
        self.declare_rt(
            "knot_route_table_add",
            &[p, p, p, p, p, p, p, p, p, p, p, p, p],
            &[],
        );
        self.declare_rt("knot_http_listen", &[p, p, p, p], &[p]);

        // OpenAPI / api command
        self.declare_rt("knot_api_register", &[p, p, p], &[]);
        self.declare_rt("knot_api_handle", &[types::I32, p], &[types::I32]);
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

    /// Define a 2-param stdlib function using currying:
    /// outer(db, arg1) -> Function(inner, env={arg1}, name)
    /// inner(db, env, arg2) -> rt_fn(db, arg1, arg2)
    fn define_stdlib_fn_2(
        &mut self,
        name: &str,
        rt_name: &str,
        rt_needs_db: bool,
    ) {
        let inner_id = self.declare_closure_fn(&format!("__stdlib_{}_apply", name));

        // Define the outer function: captures arg1, returns closure
        let (func_id, _) = self.user_fns[name];
        let mut outer_sig = self.module.make_signature();
        outer_sig.params.push(AbiParam::new(self.ptr_type)); // db
        outer_sig.params.push(AbiParam::new(self.ptr_type)); // arg1
        outer_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(func_id, outer_sig, |cg, builder, entry| {
            let arg1 = builder.block_params(entry)[1];

            // Build env record with arg1
            let n = builder.ins().iconst(cg.ptr_type, 1);
            let env = cg.call_rt(builder, "knot_record_empty", &[n]);
            let (k_ptr, k_len) = cg.string_ptr(builder, "0");
            cg.call_rt_void(builder, "knot_record_set_field", &[env, k_ptr, k_len, arg1]);

            // Create Function value pointing to inner closure
            let inner_ref = cg.module.declare_func_in_func(inner_id, builder.func);
            let fn_addr = builder.ins().func_addr(cg.ptr_type, inner_ref);
            let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
            let result =
                cg.call_rt(builder, "knot_value_function", &[fn_addr, env, src_ptr, src_len]);
            builder.ins().return_(&[result]);
        });

        // Define the inner closure: extracts arg1 from env, calls runtime
        let mut inner_sig = self.module.make_signature();
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // db
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // env
        inner_sig.params.push(AbiParam::new(self.ptr_type)); // arg2
        inner_sig.returns.push(AbiParam::new(self.ptr_type));

        let rt_name = rt_name.to_string();
        self.build_function(inner_id, inner_sig, |cg, builder, entry| {
            let db = builder.block_params(entry)[0];
            let env = builder.block_params(entry)[1];
            let arg2 = builder.block_params(entry)[2];

            // Extract arg1 from env
            let (k_ptr, k_len) = cg.string_ptr(builder, "0");
            let arg1 = cg.call_rt(builder, "knot_record_field", &[env, k_ptr, k_len]);

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
        let middle_id = self.declare_closure_fn(&format!("__stdlib_{}_mid", name));
        let inner_id = self.declare_closure_fn(&format!("__stdlib_{}_apply", name));

        // Outer: captures arg1, returns Function(middle)
        let (func_id, _) = self.user_fns[name];
        let mut outer_sig = self.module.make_signature();
        outer_sig.params.push(AbiParam::new(self.ptr_type));
        outer_sig.params.push(AbiParam::new(self.ptr_type));
        outer_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(func_id, outer_sig, |cg, builder, entry| {
            let arg1 = builder.block_params(entry)[1];

            let n = builder.ins().iconst(cg.ptr_type, 1);
            let env = cg.call_rt(builder, "knot_record_empty", &[n]);
            let (k_ptr, k_len) = cg.string_ptr(builder, "0");
            cg.call_rt_void(builder, "knot_record_set_field", &[env, k_ptr, k_len, arg1]);

            let mid_ref = cg.module.declare_func_in_func(middle_id, builder.func);
            let fn_addr = builder.ins().func_addr(cg.ptr_type, mid_ref);
            let (src_ptr, src_len) = cg.string_ptr(builder, &fn_name);
            let result =
                cg.call_rt(builder, "knot_value_function", &[fn_addr, env, src_ptr, src_len]);
            builder.ins().return_(&[result]);
        });

        // Middle: captures arg1 + arg2, returns Function(inner)
        let mut mid_sig = self.module.make_signature();
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.params.push(AbiParam::new(self.ptr_type));
        mid_sig.returns.push(AbiParam::new(self.ptr_type));

        let fn_name = name.to_string();
        self.build_function(middle_id, mid_sig, |cg, builder, entry| {
            let prev_env = builder.block_params(entry)[1];
            let arg2 = builder.block_params(entry)[2];

            // Extract arg1 from previous env
            let (k0_ptr, k0_len) = cg.string_ptr(builder, "0");
            let arg1 = cg.call_rt(builder, "knot_record_field", &[prev_env, k0_ptr, k0_len]);

            // Build new env with both args
            let n = builder.ins().iconst(cg.ptr_type, 2);
            let env = cg.call_rt(builder, "knot_record_empty", &[n]);
            cg.call_rt_void(builder, "knot_record_set_field", &[env, k0_ptr, k0_len, arg1]);
            let (k1_ptr, k1_len) = cg.string_ptr(builder, "1");
            cg.call_rt_void(builder, "knot_record_set_field", &[env, k1_ptr, k1_len, arg2]);

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

            let (k0_ptr, k0_len) = cg.string_ptr(builder, "0");
            let arg1 = cg.call_rt(builder, "knot_record_field", &[env, k0_ptr, k0_len]);
            let (k1_ptr, k1_len) = cg.string_ptr(builder, "1");
            let arg2 = cg.call_rt(builder, "knot_record_field", &[env, k1_ptr, k1_len]);

            let result = cg.call_rt(builder, &rt_name, &[db, arg1, arg2, arg3]);
            builder.ins().return_(&[result]);
        });
    }

    // ── Declaration collection ────────────────────────────────────

    fn collect_declarations(&mut self, module: &ast::Module) {
        // __bind/__yield/__empty are desugared do-block operations that dispatch
        // through Monad/Applicative/Alternative trait impls (see compile_app,
        // compile_monadic_yield, compile_monadic_empty). No standalone user
        // function is registered — dispatch is compile-time via monad_info.

        // Register standard library functions (all as 1-param for proper currying)
        // map and fold are now trait methods (Functor.map, Foldable.fold)
        // with [] impls registered directly in register_builtin_relation_impls.
        let stdlib_names = [
            "filter", "match", "single", "diff", "inter", "sum", "avg",
            "toUpper", "toLower", "take", "drop",
            "length", "trim", "contains", "reverse",
            "chars", "id", "not",
            "toJson", "parseJson",
            "bytesLength", "bytesSlice", "bytesConcat",
            "textToBytes", "bytesToText", "bytesToHex", "bytesFromHex",
            "bytesGet",
            "readFile", "writeFile", "appendFile",
            "fileExists", "removeFile", "listDir",
            "randomInt",
            "encrypt", "decrypt", "sign", "verify",
        ];
        for name in &stdlib_names {
            self.register_stdlib_fn(name);
        }

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body, .. } => {
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

                    // Detect Maybe-isomorphic types: exactly 2 constructors,
                    // one nullary (0 fields) and one with fields.
                    if ctors.len() == 2 {
                        let (nullary, non_nullary): (Vec<_>, Vec<_>) =
                            ctors.iter().partition(|c| c.fields.is_empty());
                        if nullary.len() == 1 && non_nullary.len() == 1 {
                            self.nullable_ctors.insert(
                                nullary[0].name.clone(),
                                NullableRole::None,
                            );
                            self.nullable_ctors.insert(
                                non_nullary[0].name.clone(),
                                NullableRole::Some,
                            );
                        }
                    }
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
                            } => {
                                let param_count = if default_body.is_some() {
                                    default_params.len()
                                } else {
                                    count_fn_params(&ty.ty)
                                };
                                let dispatch_index = find_dispatch_index(
                                    hkt_param_name.as_deref(),
                                    &ty.ty,
                                );
                                self.trait_methods
                                    .entry(method_name.clone())
                                    .or_insert(TraitMethodInfo {
                                        param_count,
                                        dispatch_index,
                                        impls: Vec::new(),
                                    });
                                if let Some(body) = default_body {
                                    defaults.insert(
                                        method_name.clone(),
                                        DefaultMethod {
                                            params: default_params.clone(),
                                            body: body.clone(),
                                        },
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
                                ..
                            } = item
                            {
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
                }
                ast::DeclKind::RouteComposite { name, components } => {
                    let mut all = Vec::new();
                    for comp in components {
                        if let Some(entries) = self.route_entries.get(comp) {
                            all.extend_from_slice(entries);
                        }
                    }
                    self.route_entries.insert(name.clone(), all);
                }
                _ => {}
            }
        }

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
                !info.impls.is_empty() && !self.user_fns.contains_key(name.as_str())
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
            ("Semigroup_Relation_append", "append", 2),
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
                });

            // Track for supertrait validation
            self.trait_impl_types
                .entry(match *method_name {
                    "map" => "Functor".to_string(),
                    "yield" | "ap" => "Applicative".to_string(),
                    "bind" => "Monad".to_string(),
                    "empty" | "alt" => "Alternative".to_string(),
                    "fold" => "Foldable".to_string(),
                    "append" => "Semigroup".to_string(),
                    _ => continue,
                })
                .or_default()
                .push(("Relation".to_string(), ast::Span { start: 0, end: 0 }));
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
            ("Num_Int_negate", "negate", "Int", 1, "Num"),
            ("Num_Float_add", "add", "Float", 2, "Num"),
            ("Num_Float_sub", "sub", "Float", 2, "Num"),
            ("Num_Float_mul", "mul", "Float", 2, "Num"),
            ("Num_Float_div", "div", "Float", 2, "Num"),
            ("Num_Float_negate", "negate", "Float", 1, "Num"),
            // Semigroup impls
            ("Semigroup_Text_append", "append", "Text", 2, "Semigroup"),
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

        // Semigroup_Relation_append(db, a, b) → knot_value_concat(a, b)
        define_if_registered!("Semigroup_Relation_append", |cg: &mut Self, func_id: FuncId| {
            let mut sig = cg.module.make_signature();
            sig.params.push(AbiParam::new(cg.ptr_type)); // db
            sig.params.push(AbiParam::new(cg.ptr_type)); // a
            sig.params.push(AbiParam::new(cg.ptr_type)); // b
            sig.returns.push(AbiParam::new(cg.ptr_type));
            cg.build_function(func_id, sig, |cg, builder, entry| {
                let a = builder.block_params(entry)[1];
                let b = builder.block_params(entry)[2];
                let result = cg.call_rt(builder, "knot_value_concat", &[a, b]);
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
        define_unop_impl!("Num_Int_negate", "knot_value_negate");

        define_binop_impl!("Num_Float_add", "knot_value_add");
        define_binop_impl!("Num_Float_sub", "knot_value_sub");
        define_binop_impl!("Num_Float_mul", "knot_value_mul");
        define_binop_impl!("Num_Float_div", "knot_value_div");
        define_unop_impl!("Num_Float_negate", "knot_value_negate");

        // Semigroup impls: append(a, b) → knot_value_concat(a, b)
        define_binop_impl!("Semigroup_Text_append", "knot_value_concat");
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

        // 2-param: curried (outer captures arg1, inner calls runtime)
        self.define_stdlib_fn_2("filter", "knot_relation_filter", true);
        self.define_stdlib_fn_2("match", "knot_relation_match", false);
        self.define_stdlib_fn_2("take", "knot_text_take", false);
        self.define_stdlib_fn_2("drop", "knot_text_drop", false);
        self.define_stdlib_fn_2("contains", "knot_text_contains", false);
        self.define_stdlib_fn_2("diff", "knot_relation_diff", true);
        self.define_stdlib_fn_2("inter", "knot_relation_inter", true);
        self.define_stdlib_fn_2("sum", "knot_relation_sum", true);
        self.define_stdlib_fn_2("avg", "knot_relation_avg", true);

        // JSON: 1-param
        self.define_stdlib_fn_1("toJson", "knot_json_encode");
        self.define_stdlib_fn_1("parseJson", "knot_json_decode");

        // Bytes: 1-param
        self.define_stdlib_fn_1("bytesLength", "knot_bytes_length");
        self.define_stdlib_fn_1("textToBytes", "knot_text_to_bytes");
        self.define_stdlib_fn_1("bytesToText", "knot_bytes_to_text");
        self.define_stdlib_fn_1("bytesToHex", "knot_bytes_to_hex");
        self.define_stdlib_fn_1("bytesFromHex", "knot_bytes_from_hex");

        // Bytes: 2-param (curried)
        self.define_stdlib_fn_2("bytesConcat", "knot_bytes_concat", false);
        self.define_stdlib_fn_2("bytesGet", "knot_bytes_get", false);

        // Bytes: 3-param (double-curried)
        self.define_stdlib_fn_3("bytesSlice", "knot_bytes_slice");

        // Random: 1-param
        self.define_stdlib_fn_1("randomInt", "knot_random_int");

        // Crypto: 2-param (curried)
        self.define_stdlib_fn_2("encrypt", "knot_crypto_encrypt", false);
        self.define_stdlib_fn_2("decrypt", "knot_crypto_decrypt", false);
        self.define_stdlib_fn_2("sign", "knot_crypto_sign", false);

        // Crypto: 3-param (double-curried)
        self.define_stdlib_fn_3("verify", "knot_crypto_verify");

        // File system: 1-param
        self.define_stdlib_fn_1("readFile", "knot_fs_read_file");
        self.define_stdlib_fn_1("fileExists", "knot_fs_file_exists");
        self.define_stdlib_fn_1("removeFile", "knot_fs_remove_file");
        self.define_stdlib_fn_1("listDir", "knot_fs_list_dir");

        // File system: 2-param (curried)
        self.define_stdlib_fn_2("writeFile", "knot_fs_write_file", false);
        self.define_stdlib_fn_2("appendFile", "knot_fs_append_file", false);

        // Define built-in [] impls for HKT traits
        self.define_builtin_relation_impls();

        // Define built-in primitive impls for Eq, Ord, Num traits
        self.define_builtin_primitive_impls();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body, .. } => {
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
                            if let ast::ImplItem::Method { name, params, body } =
                                item
                            {
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, name
                                );
                                self.define_user_function(&mangled, params, body);
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

        // Compile any pending lambdas (may generate more lambdas)
        while !self.pending_lambdas.is_empty() {
            let lambdas: Vec<PendingLambda> =
                std::mem::take(&mut self.pending_lambdas);
            for lambda in lambdas {
                self.define_lambda_function(&lambda);
            }
        }

        // Define trait dispatcher function bodies
        self.define_trait_dispatchers();

        // Compile any pending lambdas from dispatchers
        while !self.pending_lambdas.is_empty() {
            let lambdas: Vec<PendingLambda> =
                std::mem::take(&mut self.pending_lambdas);
            for lambda in lambdas {
                self.define_lambda_function(&lambda);
            }
        }
    }

    // ── Trait dispatcher code generation ─────────────────────────

    /// Generate runtime dispatch function bodies for trait methods.
    /// Each dispatcher checks the runtime type tag of the first argument
    /// and calls the appropriate impl method.
    fn define_trait_dispatchers(&mut self) {
        // (method_name, dispatcher_id, param_count, dispatch_index, impls)
        let dispatcher_info: Vec<(String, FuncId, usize, Option<usize>, Vec<(String, FuncId)>)> =
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
                if param_count == 0 {
                    if let Some((_, impl_func_id)) = impls.first() {
                        let impl_ref = cg
                            .module
                            .declare_func_in_func(*impl_func_id, builder.func);
                        let call = builder.ins().call(impl_ref, &[db]);
                        let result = builder.inst_results(call)[0];
                        builder.ins().return_(&[result]);
                        return;
                    }
                }

                let dispatch_arg = all_params[dispatch_index.unwrap_or(0)];

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
                    builder.ins().jump(merge_block, &[result]);
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
                    builder.ins().jump(merge_block, &[result]);

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

                    // Check each constructor name
                    builder.switch_to_block(ctor_check_block);
                    builder.seal_block(ctor_check_block);
                    for (j, ctor_name) in ctors.iter().enumerate() {
                        let (tag_ptr, tag_len) =
                            cg.string_ptr(builder, ctor_name);
                        let matches = cg.call_rt_typed(
                            builder,
                            "knot_constructor_matches",
                            &[dispatch_arg, tag_ptr, tag_len],
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
                    builder.ins().jump(merge_block, &[result]);

                    builder.switch_to_block(next_adt_block);
                    builder.seal_block(next_adt_block);
                }

                // Nullable ADT "Some" dispatch: non-null bare payload
                // (value didn't match any Constructor-based ADT)
                for impl_func_id in &nullable_adt_impls {
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
                    builder.ins().jump(merge_block, &[result]);

                    builder.switch_to_block(next_block);
                    builder.seal_block(next_block);
                }

                // Fallback: for operator-mapped trait methods, delegate to the
                // runtime function (handles types without explicit impls like
                // Record == Record). For other traits, panic with no-impl error.
                let fallback_rt = match method_name.as_str() {
                    "eq" => Some("knot_value_eq"),
                    "compare" => Some("knot_value_compare"),
                    "add" => Some("knot_value_add"),
                    "sub" => Some("knot_value_sub"),
                    "mul" => Some("knot_value_mul"),
                    "div" => Some("knot_value_div"),
                    "negate" => Some("knot_value_negate"),
                    _ => None,
                };
                if let Some(rt_fn) = fallback_rt {
                    let result = cg.call_rt(builder, rt_fn, &all_params);
                    builder.ins().jump(merge_block, &[result]);
                } else {
                    let (name_ptr, name_len) =
                        cg.string_ptr(builder, &method_name);
                    let err = cg.call_rt(
                        builder,
                        "knot_trait_no_impl",
                        &[name_ptr, name_len, dispatch_arg],
                    );
                    builder.ins().jump(merge_block, &[err]);
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
        self.builder_ctx = fb_ctx;
        self.ctx = ctx;
        self.module.clear_context(&mut self.ctx);
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

        self.build_function(func_id, sig, |cg, builder, entry| {
            let mut env = Env::new();
            let db = builder.block_params(entry)[0];
            for (i, pat) in params_owned.iter().enumerate() {
                let val = builder.block_params(entry)[i + 1];
                bind_pattern_env(pat, val, &mut env);
            }

            let result = cg.compile_expr(builder, &body_owned, &mut env, db);
            builder.ins().return_(&[result]);
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
        let body = lambda.body.clone();
        let free_vars = lambda.free_vars.clone();

        self.build_function(func_id, sig, |cg, builder, entry| {
            let mut env = Env::new();
            let db = builder.block_params(entry)[0];
            let closure_env = builder.block_params(entry)[1];
            let arg = builder.block_params(entry)[2];

            // Unpack free variables from closure env
            for var_name in &free_vars {
                let (key_ptr, key_len) = cg.string_ptr(builder, var_name);
                let field_val =
                    cg.call_rt(builder, "knot_record_field", &[closure_env, key_ptr, key_len]);
                env.set(var_name, field_val);
            }

            // Bind parameter
            if params.len() == 1 {
                env.set(&params[0], arg);
            }

            let result = cg.compile_expr(builder, &body, &mut env, db);
            builder.ins().return_(&[result]);
        });
    }

    /// Get or create a trampoline function that wraps a user function with the
    /// standard lambda calling convention (db, env, arg) -> result.
    /// For 1-param user functions: trampoline(db, env, arg) calls user_fn(db, arg).
    /// For n-param: trampoline(db, env, arg) partially applies (curries remaining args).
    fn get_or_create_trampoline(&mut self, name: &str, _n_params: usize) -> FuncId {
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

        // For 1-param functions: body is App(Var(name), Var(param))
        // For multi-param: body is App(Var(name), Var(param)) which creates a partial application
        // (compile_app handles partial application by wrapping in dynamic calls)
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
            body,
            free_vars: vec![],
        });

        self.user_fn_trampolines.insert(name.to_string(), trampoline_id);
        trampoline_id
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

        self.build_function(main_id, sig, |cg, builder, entry| {
            let argc = builder.block_params(entry)[0];
            let argv = builder.block_params(entry)[1];

            // Register route tables for the api command
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
                    let path_pattern = path_segments_to_pattern(&route_entry.path);
                    let (path_ptr, path_len) = cg.string_ptr(builder, &path_pattern);
                    let (ctor_ptr, ctor_len) = cg.string_ptr(builder, &route_entry.constructor);
                    let body_desc = fields_to_descriptor(&route_entry.body_fields);
                    let (body_ptr, body_len) = cg.string_ptr(builder, &body_desc);
                    let query_desc = fields_to_descriptor(&route_entry.query_params);
                    let (query_ptr, query_len) = cg.string_ptr(builder, &query_desc);
                    let resp_desc = response_type_descriptor(&route_entry.response_ty, &cg.type_aliases);
                    let (resp_ptr, resp_len) = cg.string_ptr(builder, &resp_desc);
                    cg.call_rt_void(
                        builder,
                        "knot_route_table_add",
                        &[
                            table, method_ptr, method_len, path_ptr, path_len,
                            ctor_ptr, ctor_len, body_ptr, body_len, query_ptr,
                            query_len, resp_ptr, resp_len,
                        ],
                    );
                }
                let (name_ptr, name_len) = cg.string_ptr(builder, route_name);
                cg.call_rt_void(builder, "knot_api_register", &[name_ptr, name_len, table]);
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

            // Check --debug flag
            let debug_init_ref = cg.import_rt(builder, "knot_debug_init");
            builder.ins().call(debug_init_ref, &[]);

            // Open database
            let db_path = cg.db_path.clone();
            let (db_path_ptr, db_path_len) = cg.string_ptr(builder, &db_path);
            let db_open_ref = cg.import_rt(builder, "knot_db_open");
            let db_open_call =
                builder.ins().call(db_open_ref, &[db_path_ptr, db_path_len]);
            let db = builder.inst_results(db_open_call)[0];

            // Initialize schema tracking
            cg.call_rt_void(builder, "knot_schema_init", &[db]);

            // Apply pending migrations (before source init)
            let migrate_schemas = cg.migrate_schemas.clone();
            let mut migrate_counters: HashMap<String, usize> = HashMap::new();
            for decl in &decls {
                if let ast::DeclKind::Migrate {
                    relation,
                    using_fn,
                    ..
                } = &decl.node
                {
                    if let Some(migrations) = migrate_schemas.get(relation) {
                        let idx = migrate_counters.entry(relation.clone()).or_insert(0);
                        if let Some((old_schema, new_schema)) = migrations.get(*idx) {
                            let (name_ptr, name_len) = cg.string_ptr(builder, relation);
                            let (old_ptr, old_len) = cg.string_ptr(builder, old_schema);
                            let (new_ptr, new_len) = cg.string_ptr(builder, new_schema);

                            // Compile the using expression (typically a lambda)
                            let mut env = Env::new();
                            let migrate_fn_val =
                                cg.compile_expr(builder, using_fn, &mut env, db);

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
            }

            // Initialize source tables
            let history_sources = cg.history_sources.clone();
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

                    // Initialize history table for sources with `with history`
                    if history_sources.contains(name) {
                        let (hn_ptr, hn_len) = cg.string_ptr(builder, name);
                        let (hs_ptr, hs_len) = cg.string_ptr(builder, &schema);
                        cg.call_rt_void(
                            builder,
                            "knot_history_init",
                            &[db, hn_ptr, hn_len, hs_ptr, hs_len],
                        );
                    }
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

            // Call user's main function if it exists
            if let Some((main_fn_id, n_params)) = user_main {
                if n_params == 0 {
                    let user_main_ref =
                        cg.module.declare_func_in_func(main_fn_id, builder.func);
                    let call = builder.ins().call(user_main_ref, &[db]);
                    let result = builder.inst_results(call)[0];

                    // Print the result
                    let println_ref = cg.import_rt(builder, "knot_println");
                    builder.ins().call(println_ref, &[result]);
                }
            }

            // Close database
            let db_close_ref = cg.import_rt(builder, "knot_db_close");
            builder.ins().call(db_close_ref, &[db]);

            let zero = builder.ins().iconst(types::I32, 0);
            builder.ins().return_(&[zero]);
        });
    }

    // ── Finish ────────────────────────────────────────────────────

    fn finish(self) -> Vec<u8> {
        let product = self.module.finish();
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
                return self.compile_monadic_empty(builder, expr.span, db);
            }

            ast::ExprKind::Var(name) => {
                if name == "now" {
                    return self.call_rt(builder, "knot_now", &[]);
                }
                if name == "randomFloat" {
                    return self.call_rt(builder, "knot_random_float", &[]);
                }
                if name == "generateKeyPair" {
                    return self.call_rt(builder, "knot_crypto_generate_key_pair", &[]);
                }
                if name == "generateSigningKeyPair" {
                    return self.call_rt(builder, "knot_crypto_generate_signing_key_pair", &[]);
                }
                if name == "readLine" {
                    return self.call_rt(builder, "knot_read_line", &[]);
                }
                if let Some(&val) = env.bindings.get(name) {
                    val
                } else if let Some((func_id, n_params)) =
                    self.user_fns.get(name).copied()
                {
                    if n_params == 0 {
                        // 0-param function is a constant — call it directly
                        let func_ref =
                            self.module.declare_func_in_func(func_id, builder.func);
                        let call = builder.ins().call(func_ref, &[db]);
                        builder.inst_results(call)[0]
                    } else {
                        // Create a trampoline that bridges (db, env, arg) calling
                        // convention to the user function's (db, arg1, ...) convention.
                        let trampoline_id = self.get_or_create_trampoline(name, n_params);
                        let func_ref =
                            self.module.declare_func_in_func(trampoline_id, builder.func);
                        let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);
                        let null = builder.ins().iconst(self.ptr_type, 0);
                        let (src_ptr, src_len) = self.string_ptr(builder, name);
                        self.call_rt(builder, "knot_value_function", &[fn_addr, null, src_ptr, src_len])
                    }
                } else {
                    panic!("codegen: undefined variable '{}'", name)
                }
            }

            ast::ExprKind::Constructor(name) => {
                if matches!(self.nullable_ctors.get(name), Some(NullableRole::None)) {
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
                    if view.constant_columns.is_empty() {
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
                        // Filtered view: SELECT source columns WHERE constants match
                        let view_schema = self.compute_view_schema(&view);
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

                        self.call_rt(
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
                        )
                    }
                } else {
                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();
                    let (name_ptr, name_len) = self.string_ptr(builder, name);
                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                    self.call_rt(
                        builder,
                        "knot_source_read",
                        &[db, name_ptr, name_len, schema_ptr, schema_len],
                    )
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
                    panic!("codegen: undefined derived relation '&{}'", name)
                }
            }

            ast::ExprKind::Record(fields) => {
                let n = fields.len();
                let n_val = builder.ins().iconst(self.ptr_type, n as i64);
                let record = self.call_rt(builder, "knot_record_empty", &[n_val]);
                for field in fields {
                    let val = self.compile_expr(builder, &field.value, env, db);
                    let (key_ptr, key_len) = self.string_ptr(builder, &field.name);
                    self.call_rt_void(
                        builder,
                        "knot_record_set_field",
                        &[record, key_ptr, key_len, val],
                    );
                }
                record
            }

            ast::ExprKind::RecordUpdate { base, fields } => {
                let base_val = self.compile_expr(builder, base, env, db);
                let updated = self.call_rt(builder, "knot_record_update", &[base_val]);
                for field in fields {
                    let val = self.compile_expr(builder, &field.value, env, db);
                    let (key_ptr, key_len) = self.string_ptr(builder, &field.name);
                    self.call_rt_void(
                        builder,
                        "knot_record_set_field",
                        &[updated, key_ptr, key_len, val],
                    );
                }
                updated
            }

            ast::ExprKind::FieldAccess { expr, field } => {
                let val = self.compile_expr(builder, expr, env, db);
                let (key_ptr, key_len) = self.string_ptr(builder, field);
                self.call_rt(builder, "knot_record_field", &[val, key_ptr, key_len])
            }

            ast::ExprKind::List(elems) => {
                let rel = self.call_rt(builder, "knot_relation_empty", &[]);
                for elem in elems {
                    let val = self.compile_expr(builder, elem, env, db);
                    self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
                }
                rel
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                if matches!(op, ast::BinOp::Pipe) {
                    // Check for: source |> match Constructor → SQL-level match
                    if let ast::ExprKind::App { func: match_fn, arg: match_arg } = &rhs.node {
                        if let (ast::ExprKind::Var(fn_name), ast::ExprKind::Constructor(ctor_name)) = (&match_fn.node, &match_arg.node) {
                            if fn_name == "match" {
                                if let ast::ExprKind::SourceRef(source_name) = &lhs.node {
                                    if let Some(schema) = self.source_schemas.get(source_name).cloned() {
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
                        }
                    }
                    // Try to compile the entire pipe chain to a single SQL query
                    if let Some(val) = self.try_compile_pipe_sql(builder, expr, env, db) {
                        return val;
                    }
                    // lhs |> rhs  =>  rhs(lhs)
                    let arg = self.compile_expr(builder, lhs, env, db);
                    let func = self.compile_expr(builder, rhs, env, db);
                    self.call_rt(builder, "knot_value_call", &[db, func, arg])
                } else {
                    let l = self.compile_expr(builder, lhs, env, db);
                    let r = self.compile_expr(builder, rhs, env, db);
                    match op {
                        // Arithmetic: dispatch through Num trait
                        ast::BinOp::Add => self.compile_trait_binop(builder, "add", l, r, db, "knot_value_add"),
                        ast::BinOp::Sub => self.compile_trait_binop(builder, "sub", l, r, db, "knot_value_sub"),
                        ast::BinOp::Mul => self.compile_trait_binop(builder, "mul", l, r, db, "knot_value_mul"),
                        ast::BinOp::Div => self.compile_trait_binop(builder, "div", l, r, db, "knot_value_div"),
                        // Equality: dispatch through Eq trait
                        ast::BinOp::Eq => self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq"),
                        ast::BinOp::Neq => {
                            let eq_result = self.compile_trait_binop(builder, "eq", l, r, db, "knot_value_eq");
                            self.call_rt(builder, "knot_value_not", &[eq_result])
                        }
                        // Comparison: dispatch through Ord trait (compare → Ordering)
                        ast::BinOp::Lt => self.compile_comparison(builder, l, r, db, "LT", false),
                        ast::BinOp::Gt => self.compile_comparison(builder, l, r, db, "GT", false),
                        ast::BinOp::Le => self.compile_comparison(builder, l, r, db, "GT", true),
                        ast::BinOp::Ge => self.compile_comparison(builder, l, r, db, "LT", true),
                        // Boolean ops: no trait dispatch
                        ast::BinOp::And => self.call_rt(builder, "knot_value_and", &[l, r]),
                        ast::BinOp::Or => self.call_rt(builder, "knot_value_or", &[l, r]),
                        // Semigroup: dispatch through Semigroup trait
                        ast::BinOp::Concat => self.compile_trait_binop(builder, "append", l, r, db, "knot_value_concat"),
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
                let cond_val = self.compile_expr(builder, cond, env, db);
                let bool_val =
                    self.call_rt_typed(builder, "knot_value_get_bool", &[cond_val], types::I32);
                let is_true =
                    builder.ins().icmp_imm(IntCC::NotEqual, bool_val, 0);

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
                builder.ins().jump(merge_block, &[then_val]);

                builder.switch_to_block(else_block);
                builder.seal_block(else_block);
                let else_val =
                    self.compile_expr(builder, else_branch, &mut env.clone(), db);
                builder.ins().jump(merge_block, &[else_val]);

                builder.switch_to_block(merge_block);
                builder.seal_block(merge_block);
                builder.block_params(merge_block)[0]
            }

            ast::ExprKind::Lambda { params, body } => {
                self.compile_lambda(builder, params, body, env, db)
            }

            ast::ExprKind::App { func, arg } => {
                // Check for monadic yield: __yield(e)
                if let ast::ExprKind::Var(name) = &func.node {
                    if name == "__yield" {
                        let val = self.compile_expr(builder, arg, env, db);
                        return self.compile_monadic_yield(builder, val, func.span, db);
                    }
                }
                self.compile_app(builder, expr, env, db)
            }

            ast::ExprKind::Do(stmts) => self.compile_do(builder, stmts, env, db),

            ast::ExprKind::Yield(inner) => {
                let val = self.compile_expr(builder, inner, env, db);
                self.call_rt(builder, "knot_relation_singleton", &[val])
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

                    // Snapshot history before writing (if history-enabled)
                    self.emit_history_snapshot(builder, db, name, &schema);

                    if let Some(new_rows_expr) = self.match_union_append(name, value) {
                        // 1. Append: union *rel <new> → INSERT only
                        let new_rows = self.compile_expr(builder, new_rows_expr, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_append",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, new_rows],
                        );
                    } else if !Self::references_source(value, name) {
                        // 2. Full replace: value doesn't read the source
                        let val = self.compile_expr(builder, value, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt_void(
                            builder,
                            "knot_source_write",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                        );
                    } else if let Some((bind_var, cond, update_fields)) =
                        Self::match_conditional_update(name, value)
                    {
                        // 3. Conditional update: do { t <- *rel; yield (if cond then {t | ...} else t) }
                        //    Try SQL UPDATE WHERE
                        let where_frag = Self::try_compile_sql_expr(&bind_var, cond);
                        let set_frag = where_frag.as_ref().and_then(|_| {
                            let mut parts = Vec::new();
                            let mut params = Vec::new();
                            for (field_name, field_val) in &update_fields {
                                let param = match &field_val.node {
                                    ast::ExprKind::Lit(lit) => {
                                        SqlParamSource::Literal(lit.clone())
                                    }
                                    ast::ExprKind::Var(name) => {
                                        SqlParamSource::Var(name.clone())
                                    }
                                    _ => return None,
                                };
                                parts.push(format!("{} = ?", quote_sql_ident(field_name)));
                                params.push(param);
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
                                self.compile_sql_params(builder, &all_params, env);
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
                            let val = self.compile_expr(builder, value, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            self.call_rt_void(
                                builder,
                                "knot_source_write",
                                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                            );
                        }
                    } else if let Some((bind_var, conditions)) =
                        Self::match_filter_only(name, value)
                    {
                        // 4. Filter only: do { t <- *rel; where cond; yield t }
                        //    Try SQL DELETE WHERE
                        let combined_sql: Option<SqlFragment> = {
                            let mut frags = Vec::new();
                            let mut all_ok = true;
                            for cond in &conditions {
                                if let Some(f) =
                                    Self::try_compile_sql_expr(&bind_var, cond)
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
                                self.compile_sql_params(builder, &frag.params, env);
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
                            let val = self.compile_expr(builder, value, env, db);
                            let (name_ptr, name_len) = self.string_ptr(builder, name);
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            self.call_rt_void(
                                builder,
                                "knot_source_diff_write",
                                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
                            );
                        }
                    } else if Self::match_map_no_filter(name, value) {
                        // 5. Map without filter: every row transformed, no filtering
                        //    Full write is safe and avoids diff overhead.
                        let val = self.compile_expr(builder, value, env, db);
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
                        let val = self.compile_expr(builder, value, env, db);
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
                    panic!("codegen: set target must be a source reference")
                }
            }

            ast::ExprKind::FullSet { target, value } => {
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

                    // Snapshot history before writing (if history-enabled)
                    self.emit_history_snapshot(builder, db, name, &schema);

                    let val = self.compile_expr(builder, value, env, db);
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
                    panic!("codegen: full set target must be a source reference")
                }
            }

            ast::ExprKind::Atomic(inner) => {
                self.call_rt_void(builder, "knot_atomic_begin", &[db]);
                let val = self.compile_expr(builder, inner, env, db);
                self.call_rt_void(builder, "knot_atomic_commit", &[db]);
                val
            }

            ast::ExprKind::Case {
                scrutinee,
                arms,
            } => self.compile_case(builder, scrutinee, arms, env, db),

            ast::ExprKind::At { relation, time } => {
                // Temporal query: *source @(timestamp) or *view @(timestamp)
                if let ast::ExprKind::SourceRef(name) = &relation.node {
                    let view_info = self.views.get(name).cloned();
                    if let Some(view) = view_info {
                        // View temporal query — read from underlying source's history
                        let timestamp = self.compile_expr(builder, time, env, db);
                        let source_name = &view.source_name;
                        let (name_ptr, name_len) =
                            self.string_ptr(builder, source_name);

                        if view.constant_columns.is_empty() {
                            // Simple alias view: read all columns from source history
                            let schema = self
                                .source_schemas
                                .get(source_name)
                                .cloned()
                                .unwrap_or_default();
                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &schema);
                            self.call_rt(
                                builder,
                                "knot_source_read_at",
                                &[db, name_ptr, name_len, schema_ptr, schema_len, timestamp],
                            )
                        } else {
                            // Filtered view: read view columns with constant filter
                            let view_schema = self.compute_view_schema(&view);
                            let (filter_where, constant_cols) =
                                self.compute_view_filter(&view);
                            let filter_params = self.compile_view_filter_params(
                                builder,
                                &constant_cols,
                                env,
                                db,
                            );

                            let (schema_ptr, schema_len) =
                                self.string_ptr(builder, &view_schema);
                            let (filter_ptr, filter_len) =
                                self.string_ptr(builder, &filter_where);

                            self.call_rt(
                                builder,
                                "knot_view_read_at",
                                &[
                                    db,
                                    name_ptr,
                                    name_len,
                                    schema_ptr,
                                    schema_len,
                                    filter_ptr,
                                    filter_len,
                                    filter_params,
                                    timestamp,
                                ],
                            )
                        }
                    } else {
                        // Direct source temporal query
                        let schema = self
                            .source_schemas
                            .get(name)
                            .cloned()
                            .unwrap_or_default();
                        let timestamp = self.compile_expr(builder, time, env, db);
                        let (name_ptr, name_len) = self.string_ptr(builder, name);
                        let (schema_ptr, schema_len) =
                            self.string_ptr(builder, &schema);
                        self.call_rt(
                            builder,
                            "knot_source_read_at",
                            &[db, name_ptr, name_len, schema_ptr, schema_len, timestamp],
                        )
                    }
                } else {
                    // For non-source At expressions, compile the relation normally
                    self.compile_expr(builder, relation, env, db)
                }
            }
        }
    }

    // ── View compilation ─────────────────────────────────────────

    /// Compute the view schema: subset of source schema for source columns only.
    fn compute_view_schema(&self, view: &ViewInfo) -> String {
        let source_schema = self
            .source_schemas
            .get(&view.source_name)
            .cloned()
            .unwrap_or_default();
        let view_col_names: Vec<&str> = view
            .source_columns
            .iter()
            .map(|(_, src_col)| src_col.as_str())
            .collect();
        source_schema
            .split(',')
            .filter(|part| {
                let name = part.split(':').next().unwrap_or("");
                view_col_names.contains(&name)
            })
            .collect::<Vec<_>>()
            .join(",")
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

        // Snapshot history before writing (if underlying source has history)
        self.emit_history_snapshot(builder, db, &source_name, &source_schema);

        // Check for append optimization: set *view = union *view newRows
        if let Some(new_rows_expr) = self.match_union_append(view_name, value) {
            let new_rows_expr = new_rows_expr.clone();
            let new_rows = self.compile_expr(builder, &new_rows_expr, env, db);
            let augmented =
                self.compile_view_augment(builder, new_rows, &view.constant_columns, env, db);
            let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
            let (schema_ptr, schema_len) = self.string_ptr(builder, &source_schema);
            self.call_rt_void(
                builder,
                "knot_source_append",
                &[db, name_ptr, name_len, schema_ptr, schema_len, augmented],
            );
        } else if view.constant_columns.is_empty() {
            // No constant columns — simple alias, use diff-write on underlying source
            let val = self.compile_expr(builder, value, env, db);
            let (name_ptr, name_len) = self.string_ptr(builder, &source_name);
            let (schema_ptr, schema_len) = self.string_ptr(builder, &source_schema);
            self.call_rt_void(
                builder,
                "knot_source_diff_write",
                &[db, name_ptr, name_len, schema_ptr, schema_len, val],
            );
        } else {
            // General case: delete matching rows, insert new rows with constants
            let new_val = self.compile_expr(builder, value, env, db);
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
            let (schema_ptr, schema_len) = self.string_ptr(builder, &source_schema);
            let (filter_ptr, filter_len) = self.string_ptr(builder, &filter_where);

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

    fn compile_app(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &ast::Expr,
        env: &mut Env,
        db: Value,
    ) -> Value {
        // Uncurry nested applications
        let (func_expr, args) = uncurry_app(expr);

        // Special case: count *rel → SQL COUNT(*)
        if let ast::ExprKind::Var(name) = &func_expr.node {
            if name == "count" && args.len() == 1 {
                if let ast::ExprKind::SourceRef(source_name) = &args[0].node {
                    // Only for actual sources, not views
                    if !self.views.contains_key(source_name)
                        && self.source_schemas.contains_key(source_name)
                    {
                        let (name_ptr, name_len) =
                            self.string_ptr(builder, source_name);
                        return self.call_rt(
                            builder,
                            "knot_source_count",
                            &[db, name_ptr, name_len],
                        );
                    }
                }
            }
        }

        // Special case: match Constructor SourceRef → SQL-level filtered read
        if let ast::ExprKind::Var(name) = &func_expr.node {
            if name == "match" && args.len() == 2 {
                if let ast::ExprKind::Constructor(ctor_name) = &args[0].node {
                    if let ast::ExprKind::SourceRef(source_name) = &args[1].node {
                        if let Some(schema) = self.source_schemas.get(source_name).cloned() {
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
                    }
                    // Non-source relation: compile and use value-level match
                    let rel = self.compile_expr(builder, &args[1], env, db);
                    let ctor = self.compile_expr(builder, &args[0], env, db);
                    return self.call_rt(
                        builder,
                        "knot_relation_match",
                        &[ctor, rel],
                    );
                }
            }
        }

        // Special case: filter/sum/avg with lambda on source → SQL
        if let ast::ExprKind::Var(name) = &func_expr.node {
            if args.len() == 2 {
                if let ast::ExprKind::SourceRef(source_name) = &args[1].node {
                    if !self.views.contains_key(source_name) {
                        if let Some(schema) = self.source_schemas.get(source_name).cloned() {
                            if !schema.starts_with('#') && !schema.contains('[') {
                                if let Some(result) = self.try_compile_app_sql(
                                    builder, name, &args[0], source_name, &schema, env, db,
                                ) {
                                    return result;
                                }
                            }
                        }
                    }
                }
            }
        }

        // SQL set operations: diff/inter/union on two source relations
        if let ast::ExprKind::Var(name) = &func_expr.node {
            let sql_op = match name.as_str() {
                "diff" => Some("EXCEPT"),
                "inter" => Some("INTERSECT"),
                "union" => Some("UNION"),
                _ => None,
            };
            if let Some(sql_op) = sql_op {
                if args.len() == 2 {
                    if let (ast::ExprKind::SourceRef(a), ast::ExprKind::SourceRef(b)) =
                        (&args[0].node, &args[1].node)
                    {
                        if let Some(result) =
                            self.try_compile_set_op_sql(builder, sql_op, a, b, env, db)
                        {
                            return result;
                        }
                    }
                }
            }
        }

        let compiled_args: Vec<Value> = args
            .iter()
            .map(|a| self.compile_expr(builder, a, env, db))
            .collect();

        match &func_expr.node {
            // Monadic bind: __bind(f, m) — dispatch based on monad type
            ast::ExprKind::Var(name) if name == "__bind" => {
                // Dispatch through Monad trait impls based on resolved monad type
                if compiled_args.len() == 2 {
                    let type_name = match self.monad_info.get(&func_expr.span) {
                        Some(MonadKind::Adt(name)) => name.clone(),
                        _ => "Relation".to_string(),
                    };
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

            // Direct call to a known user function
            ast::ExprKind::Var(name)
                if self.user_fns.contains_key(name) =>
            {
                let (func_id, expected_params) =
                    self.user_fns[name];
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

            // Built-in functions
            ast::ExprKind::Var(name) if name == "println" || name == "putLine" => {
                let rt_name = "knot_println";
                if compiled_args.len() == 1 {
                    self.call_rt(builder, rt_name, &[compiled_args[0]])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "print" => {
                if compiled_args.len() == 1 {
                    self.call_rt(builder, "knot_print", &[compiled_args[0]])
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }
            ast::ExprKind::Var(name) if name == "show" => {
                if compiled_args.len() == 1 {
                    self.call_rt(builder, "knot_value_show", &[compiled_args[0]])
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
            ast::ExprKind::Var(name) if name == "listen" => {
                if compiled_args.len() == 2 {
                    // listen port handler
                    // Build route table from known route declarations
                    let table = self.call_rt(builder, "knot_route_table_new", &[]);

                    // Find route entries — use the first available route
                    let entries: Vec<ast::RouteEntry> = self
                        .route_entries
                        .values()
                        .next()
                        .cloned()
                        .unwrap_or_default();

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

                        let path_pattern = path_segments_to_pattern(&entry.path);
                        let (path_ptr, path_len) =
                            self.string_ptr(builder, &path_pattern);

                        let (ctor_ptr, ctor_len) =
                            self.string_ptr(builder, &entry.constructor);

                        let body_desc = fields_to_descriptor(&entry.body_fields);
                        let (body_ptr, body_len) =
                            self.string_ptr(builder, &body_desc);

                        let query_desc = fields_to_descriptor(&entry.query_params);
                        let (query_ptr, query_len) =
                            self.string_ptr(builder, &query_desc);

                        let resp_desc = response_type_descriptor(&entry.response_ty, &self.type_aliases);
                        let (resp_ptr, resp_len) =
                            self.string_ptr(builder, &resp_desc);

                        self.call_rt_void(
                            builder,
                            "knot_route_table_add",
                            &[
                                table, method_ptr, method_len, path_ptr, path_len,
                                ctor_ptr, ctor_len, body_ptr, body_len, query_ptr,
                                query_len, resp_ptr, resp_len,
                            ],
                        );
                    }

                    self.call_rt(
                        builder,
                        "knot_http_listen",
                        &[db, compiled_args[0], table, compiled_args[1]],
                    )
                } else {
                    self.call_rt(builder, "knot_value_unit", &[])
                }
            }

            // Constructor application: `Circle {radius: 3.14}`
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

        for (i, arm) in arms.iter().enumerate() {
            let is_last = i == arms.len() - 1;
            let arm_block = builder.create_block();

            // For unconditional patterns on the last arm, use merge_block
            // as next_block. For conditional patterns, always create a
            // separate block (merge_block has a parameter that brif can't
            // provide).
            let is_unconditional = matches!(
                &arm.pat.node,
                ast::PatKind::Wildcard | ast::PatKind::Var(_)
            );
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
                ast::PatKind::Lit(lit) => {
                    let lit_val = self.compile_lit(builder, lit);
                    let eq =
                        self.call_rt(builder, "knot_value_eq", &[scrut, lit_val]);
                    let eq_bool = self.call_rt_typed(
                        builder,
                        "knot_value_get_bool",
                        &[eq],
                        types::I32,
                    );
                    let is_eq =
                        builder.ins().icmp_imm(IntCC::NotEqual, eq_bool, 0);
                    builder
                        .ins()
                        .brif(is_eq, arm_block, &[], next_block, &[]);
                }
                ast::PatKind::List(pats) => {
                    // Check if relation length matches the number of patterns
                    let len = self.call_rt(builder, "knot_relation_len", &[scrut]);
                    let expected =
                        builder.ins().iconst(types::I64, pats.len() as i64);
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
                ast::PatKind::Record(_) => {
                    // Record patterns always match (no top-level guard)
                    builder.ins().jump(arm_block, &[]);
                }
            }

            builder.switch_to_block(arm_block);
            builder.seal_block(arm_block);

            // Bind pattern variables
            let mut arm_env = env.clone();
            self.bind_case_pattern(builder, &arm.pat, scrut, &mut arm_env);

            let arm_val =
                self.compile_expr(builder, &arm.body, &mut arm_env, db);
            builder.ins().jump(merge_block, &[arm_val]);

            if is_last && !is_unconditional {
                // Last arm was conditional — fallback block for non-exhaustive match
                builder.switch_to_block(next_block);
                builder.seal_block(next_block);
                let unit = self.call_rt(builder, "knot_value_unit", &[]);
                builder.ins().jump(merge_block, &[unit]);
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
                if self.nullable_ctors.contains_key(name) {
                    // Nullable: val is the bare payload (or null for none)
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
                    let index = builder.ins().iconst(types::I64, idx as i64);
                    let elem =
                        self.call_rt(builder, "knot_relation_get", &[val, index]);
                    self.bind_case_pattern(builder, elem_pat, elem, env);
                }
            }
        }
    }

    // ── Monadic operation compilation ─────────────────────────────

    /// Compile `__yield(val)` — dispatches through Applicative trait impl.
    fn compile_monadic_yield(
        &mut self,
        builder: &mut FunctionBuilder,
        val: Value,
        span: ast::Span,
        db: Value,
    ) -> Value {
        let type_name = match self.monad_info.get(&span) {
            Some(MonadKind::Adt(name)) => name.clone(),
            _ => "Relation".to_string(),
        };
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
            _ => "Relation".to_string(),
        };
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

        let result = self.call_rt(builder, "knot_relation_empty", &[]);
        let mut loop_stack: Vec<LoopInfo> = Vec::new();

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
        let mut primary_source: Option<String> = None;

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
                if let ast::StmtKind::Bind { pat, expr } = &s.node {
                    if let ast::PatKind::Var(name) = &pat.node {
                        return Some((i, name.as_str(), expr));
                    }
                }
                None
            })
            .collect();

        // For each pair of binds, look for equi-join Where clauses
        for w in 0..bind_stmts.len() {
            for v in 0..w {
                let (_outer_idx, outer_var, _outer_expr) = bind_stmts[v];
                let (inner_idx, inner_var, inner_expr) = bind_stmts[w];

                // Inner expr must not reference the outer bind var
                if expr_references_var(inner_expr, outer_var) {
                    continue;
                }
                // Inner expr must be hoistable (source, derived, var, or list)
                let hoistable = matches!(
                    &inner_expr.node,
                    ast::ExprKind::SourceRef(_)
                        | ast::ExprKind::DerivedRef(_)
                        | ast::ExprKind::Var(_)
                        | ast::ExprKind::List(_)
                );
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

                for wi in (inner_idx + 1)..search_end {
                    if consumed_wheres.contains(&wi) {
                        continue;
                    }
                    if let ast::StmtKind::Where { cond } = &stmts[wi].node {
                        if let Some((ov, of, iv, inf)) =
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
                                break; // one join per bind pair
                            }
                        }
                    }
                }
            }
        }

        for (stmt_idx, stmt) in stmts.iter().enumerate() {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    // ── Hash join path: build index inline and lookup ──
                    if let Some(plan) = hash_join_plans.get(&stmt_idx) {
                        // Build the hash index from the inner relation right here
                        let inner_rel = self.compile_expr(builder, expr, env, db);
                        let (field_ptr, field_len) =
                            self.string_ptr(builder, &plan.inner_field);
                        let idx_val = self.call_rt(
                            builder,
                            "knot_relation_build_index",
                            &[inner_rel, field_ptr, field_len],
                        );

                        // Look up matching rows via the hash index
                        let outer_val = env.get(&plan.outer_var);
                        let (fptr, flen) =
                            self.string_ptr(builder, &plan.outer_field);
                        let key =
                            self.call_rt(builder, "knot_record_field", &[outer_val, fptr, flen]);
                        let rel =
                            self.call_rt(builder, "knot_relation_index_lookup", &[idx_val, key]);

                        // Free the index immediately — we have the result relation
                        self.call_rt_void(builder, "knot_relation_index_free", &[idx_val]);

                        let len = self.call_rt(builder, "knot_relation_len", &[rel]);
                        let header = builder.create_block();
                        let body = builder.create_block();
                        let continue_blk = builder.create_block();
                        let exit = builder.create_block();

                        let zero = builder.ins().iconst(types::I64, 0);
                        builder.ins().jump(header, &[zero]);
                        builder.switch_to_block(header);
                        let i = builder.append_block_param(header, types::I64);
                        let cond = builder.ins().icmp(IntCC::SignedLessThan, i, len);
                        builder.ins().brif(cond, body, &[], exit, &[]);
                        builder.switch_to_block(body);
                        builder.seal_block(body);

                        let row = self.call_rt(builder, "knot_relation_get", &[rel, i]);
                        let mut pattern_skips = Vec::new();
                        bind_do_pattern(builder, self, pat, row, env, &mut pattern_skips);

                        if group_by_pos.is_some() {
                            if let ast::PatKind::Var(name) = &pat.node {
                                primary_var = Some(name.clone());
                            }
                        }

                        loop_stack.push(LoopInfo {
                            header,
                            continue_blk,
                            exit,
                            index_var: i,
                            where_skips: pattern_skips,
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

                                for wi in (stmt_idx + 1)..search_end {
                                    if consumed_wheres.contains(&wi) {
                                        continue;
                                    }
                                    if let ast::StmtKind::Where { cond } = &stmts[wi].node {
                                        // Check all param sources are in scope
                                        if let Some(frag) =
                                            Self::try_compile_sql_expr(bind_var, cond)
                                        {
                                            let params_ok = frag.params.iter().all(|p| match p {
                                                SqlParamSource::Literal(_) => true,
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

                                if !sql_fragments.is_empty() {
                                    // Mark consumed and emit knot_source_read_where
                                    let mut all_sql = Vec::new();
                                    let mut all_params = Vec::new();
                                    for (wi, frag) in &sql_fragments {
                                        consumed_wheres.insert(*wi);
                                        all_sql.push(format!("({})", frag.sql));
                                        all_params.extend(frag.params.clone());
                                    }
                                    let where_sql = all_sql.join(" AND ");
                                    let schema = self.source_schemas.get(source_name).cloned().unwrap();
                                    let (name_ptr, name_len) =
                                        self.string_ptr(builder, source_name);
                                    let (schema_ptr, schema_len) =
                                        self.string_ptr(builder, &schema);
                                    let (where_ptr, where_len) =
                                        self.string_ptr(builder, &where_sql);
                                    let params_rel =
                                        self.compile_sql_params(builder, &all_params, env);
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

                    let val = if let Some(pushed_val) = use_filter_pushdown {
                        pushed_val
                    } else {
                        self.compile_expr(builder, expr, env, db)
                    };

                    // For constructor patterns, the RHS might be a single value
                    // (e.g., `InProgress ip <- t.status`). Wrap in a singleton
                    // relation so the loop logic works uniformly.
                    let rel = if matches!(&pat.node, ast::PatKind::Constructor { .. }) {
                        self.call_rt(builder, "knot_ensure_relation", &[val])
                    } else {
                        val
                    };
                    // knot_relation_len returns a raw usize, not a boxed Value
                    let len = self.call_rt(builder, "knot_relation_len", &[rel]);

                    let header = builder.create_block();
                    let body = builder.create_block();
                    let continue_blk = builder.create_block();
                    let exit = builder.create_block();

                    let zero = builder.ins().iconst(types::I64, 0);
                    builder.ins().jump(header, &[zero]);

                    builder.switch_to_block(header);
                    let i = builder.append_block_param(header, types::I64);
                    let cond =
                        builder.ins().icmp(IntCC::SignedLessThan, i, len);
                    builder.ins().brif(cond, body, &[], exit, &[]);

                    builder.switch_to_block(body);
                    builder.seal_block(body);

                    let row = self.call_rt(builder, "knot_relation_get", &[rel, i]);

                    // Bind pattern (constructor patterns emit filter branches)
                    let mut pattern_skips = Vec::new();
                    bind_do_pattern(builder, self, pat, row, env, &mut pattern_skips);

                    // Track the primary bind variable (most recent Var pattern)
                    // and source name for groupBy
                    if group_by_pos.is_some() {
                        if let ast::PatKind::Var(name) = &pat.node {
                            primary_var = Some(name.clone());
                        }
                        if let ast::ExprKind::SourceRef(name) = &expr.node {
                            primary_source = Some(name.clone());
                        }
                    }

                    loop_stack.push(LoopInfo {
                        header,
                        continue_blk,
                        exit,
                        index_var: i,
                        where_skips: pattern_skips,
                    });
                }

                ast::StmtKind::Where { cond } => {
                    // Skip consumed Where stmts (pushed down to SQL or consumed by hash join)
                    if consumed_wheres.contains(&stmt_idx) {
                        continue;
                    }

                    let cond_val = self.compile_expr(builder, cond, env, db);
                    let bool_val = self.call_rt_typed(
                        builder,
                        "knot_value_get_bool",
                        &[cond_val],
                        types::I32,
                    );
                    let is_true =
                        builder.ins().icmp_imm(IntCC::NotEqual, bool_val, 0);

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
                        // Where outside a loop — just seal the skip block
                        builder.switch_to_block(skip_block);
                        builder.seal_block(skip_block);
                        // Switch back — this is a degenerate case
                    }
                }

                ast::StmtKind::Let { pat, expr } => {
                    let val = self.compile_expr(builder, expr, env, db);
                    bind_pattern_env(pat, val, env);
                }

                ast::StmtKind::GroupBy { key } => {
                    // ── Phase transition: pre-group → post-group ──
                    //
                    // 1. Push the primary bind variable's value into temp
                    //    (we're inside the pre-group loops)
                    let var_name = primary_var.as_ref().expect(
                        "groupBy requires a preceding bind statement with a variable pattern"
                    );
                    let var_val = env.get(var_name);
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
                        let one = builder.ins().iconst(types::I64, 1);
                        let next_i = builder.ins().iadd(info.index_var, one);
                        builder.ins().jump(info.header, &[next_i]);
                        builder.seal_block(info.header);
                        builder.switch_to_block(info.exit);
                        builder.seal_block(info.exit);
                    }

                    // 3. Extract schema and key column names for SQLite grouping
                    let source_name = primary_source.as_ref().expect(
                        "groupBy requires a preceding bind from a source relation (*name)"
                    );
                    let schema = self
                        .source_schemas
                        .get(source_name)
                        .cloned()
                        .unwrap_or_default();

                    // Extract key column names from the key record expression
                    let key_cols: Vec<String> = match &key.node {
                        ast::ExprKind::Record(fields) => fields
                            .iter()
                            .map(|f| match &f.value.node {
                                ast::ExprKind::FieldAccess { field, .. } => {
                                    field.clone()
                                }
                                _ => f.name.clone(),
                            })
                            .collect(),
                        _ => panic!(
                            "groupBy key must be a record expression"
                        ),
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

                    let zero = builder.ins().iconst(types::I64, 0);
                    builder.ins().jump(g_header, &[zero]);

                    builder.switch_to_block(g_header);
                    let g_i = builder.append_block_param(g_header, types::I64);
                    let g_cond = builder
                        .ins()
                        .icmp(IntCC::SignedLessThan, g_i, groups_len);
                    builder
                        .ins()
                        .brif(g_cond, g_body, &[], g_exit, &[]);

                    builder.switch_to_block(g_body);
                    builder.seal_block(g_body);

                    // 6. Rebind the primary variable to the current group
                    let group = self.call_rt(
                        builder,
                        "knot_relation_get",
                        &[groups, g_i],
                    );
                    env.set(var_name, group);

                    loop_stack.push(LoopInfo {
                        header: g_header,
                        continue_blk: g_continue,
                        exit: g_exit,
                        index_var: g_i,
                        where_skips: Vec::new(),
                    });
                }

                ast::StmtKind::Expr(expr) => {
                    let is_last = stmt_idx == stmts.len() - 1;
                    match &expr.node {
                        ast::ExprKind::Yield(inner) => {
                            let val =
                                self.compile_expr(builder, inner, env, db);
                            self.call_rt_void(
                                builder,
                                "knot_relation_push",
                                &[result, val],
                            );
                        }
                        ast::ExprKind::Set { .. } | ast::ExprKind::FullSet { .. } => {
                            // Compile set inside do block
                            let _ = self.compile_expr(builder, expr, env, db);
                        }
                        _ => {
                            let val =
                                self.compile_expr(builder, expr, env, db);
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

            // Continue block: increment and loop back
            builder.switch_to_block(info.continue_blk);
            builder.seal_block(info.continue_blk);
            let one = builder.ins().iconst(types::I64, 1);
            let next_i = builder.ins().iadd(info.index_var, one);
            builder.ins().jump(info.header, &[next_i]);

            // Seal header (all predecessors now known)
            builder.seal_block(info.header);

            // Switch to exit block for the next outer loop
            builder.switch_to_block(info.exit);
            builder.seal_block(info.exit);
        }

        result
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

        // Determine free variables
        let param_names: Vec<String> = params
            .iter()
            .filter_map(|p| match &p.node {
                ast::PatKind::Var(name) => Some(name.clone()),
                _ => None,
            })
            .collect();
        let free_vars: Vec<String> = find_free_vars(body, &param_names)
            .into_iter()
            .filter(|v| !self.user_fns.contains_key(v))
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
            body: body.clone(),
            free_vars: free_vars.clone(),
        });

        // Build the closure: capture free variables into a record
        let func_ref = self.module.declare_func_in_func(func_id, builder.func);
        let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);

        let env_val = if free_vars.is_empty() {
            builder.ins().iconst(self.ptr_type, 0) // null env
        } else {
            let n = free_vars.len();
            let n_val = builder.ins().iconst(self.ptr_type, n as i64);
            let env_record = self.call_rt(builder, "knot_record_empty", &[n_val]);
            for var_name in &free_vars {
                let val = env.get(var_name);
                let (key_ptr, key_len) = self.string_ptr(builder, var_name);
                self.call_rt_void(
                    builder,
                    "knot_record_set_field",
                    &[env_record, key_ptr, key_len, val],
                );
            }
            env_record
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
                let (ptr, len) = self.string_ptr(builder, n);
                self.call_rt(builder, "knot_value_int_from_str", &[ptr, len])
            }
            ast::Literal::Float(n) => {
                let n_val = builder.ins().f64const(*n);
                self.call_rt(builder, "knot_value_float", &[n_val])
            }
            ast::Literal::Text(s) => {
                let (ptr, len) = self.string_ptr(builder, s);
                self.call_rt(builder, "knot_value_text", &[ptr, len])
            }
            ast::Literal::Bytes(b) => {
                let (ptr, len) = self.bytes_ptr(builder, b);
                self.call_rt(builder, "knot_value_bytes", &[ptr, len])
            }
        }
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

    /// Emit a history snapshot call if the source has `with history`.
    fn emit_history_snapshot(
        &mut self,
        builder: &mut FunctionBuilder,
        db: Value,
        name: &str,
        schema: &str,
    ) {
        if self.history_sources.contains(name) {
            let (name_ptr, name_len) = self.string_ptr(builder, name);
            let (schema_ptr, schema_len) = self.string_ptr(builder, schema);
            self.call_rt_void(
                builder,
                "knot_history_snapshot",
                &[db, name_ptr, name_len, schema_ptr, schema_len],
            );
        }
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
            ast::ExprKind::Yield(inner) => Self::references_source(inner, source_name),
            ast::ExprKind::Set { target, value }
            | ast::ExprKind::FullSet { target, value } => {
                Self::references_source(target, source_name)
                    || Self::references_source(value, source_name)
            }
            ast::ExprKind::Atomic(inner) => Self::references_source(inner, source_name),
            ast::ExprKind::At { relation, time } => {
                Self::references_source(relation, source_name)
                    || Self::references_source(time, source_name)
            }
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
        if let ast::ExprKind::App { func, arg: arg2 } = &value.node {
            if let ast::ExprKind::App {
                func: inner_func,
                arg: arg1,
            } = &func.node
            {
                if let ast::ExprKind::Var(fn_name) = &inner_func.node {
                    if fn_name == "union" {
                        // union *rel <new_rows>
                        if let ast::ExprKind::SourceRef(name) = &arg1.node {
                            if name == source_name {
                                return Some(arg2);
                            }
                        }
                        // union <new_rows> *rel
                        if let ast::ExprKind::SourceRef(name) = &arg2.node {
                            if name == source_name {
                                return Some(arg1);
                            }
                        }
                    }
                }
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

        let params_rel = self.compile_sql_params(builder, &plan.params, env);
        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
        Some(self.call_rt(
            builder,
            "knot_source_query",
            &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
        ))
    }

    /// Try to compile application-form `filter/sum/avg lambda *source` to SQL.
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
        let (bind_var, body) = extract_single_param_lambda(lambda_arg)?;
        let table = quote_sql_ident(&format!("_knot_{}", source_name));

        match fn_name {
            "filter" => {
                // Use unqualified column names for knot_source_read_where
                let frag = Self::try_compile_sql_expr(&bind_var, body)?;
                let params_rel = self.compile_sql_params(builder, &frag.params, env);
                let (name_ptr, name_len) = self.string_ptr(builder, source_name);
                let (schema_ptr, schema_len) = self.string_ptr(builder, schema);
                let (where_ptr, where_len) = self.string_ptr(builder, &frag.sql);
                Some(self.call_rt(
                    builder,
                    "knot_source_read_where",
                    &[db, name_ptr, name_len, schema_ptr, schema_len, where_ptr, where_len, params_rel],
                ))
            }
            "sum" | "avg" => {
                // Use unqualified column names for direct SQL aggregate
                let col_sql = extract_sql_field_access(&bind_var, body, "", schema)?;
                let func = if fn_name == "sum" { "SUM" } else { "AVG" };
                let sql = format!("SELECT {}({}) FROM {}", func, col_sql, table);
                let params_rel = self.compile_sql_params(builder, &[], env);
                let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
                let rt_fn = if fn_name == "avg" { "knot_source_query_float" } else { "knot_source_query_count" };
                Some(self.call_rt(builder, rt_fn, &[db, sql_ptr, sql_len, params_rel]))
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
        let (source, ops) = flatten_pipe_chain(expr)?;
        if ops.is_empty() {
            return None;
        }

        // Source must be a SourceRef to a plain source relation
        let source_name = match &source.node {
            ast::ExprKind::SourceRef(name) => name.clone(),
            _ => return None,
        };
        if self.views.contains_key(&source_name) {
            return None;
        }
        let schema = self.source_schemas.get(&source_name)?.clone();
        if schema.starts_with('#') || schema.contains('[') {
            return None;
        }

        let alias = "t0".to_string();
        let mut bind_aliases: HashMap<String, String> = HashMap::new();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<SqlParamSource> = Vec::new();
        let mut select_override: Option<Vec<SqlSelectColumn>> = None;
        let mut is_count = false;
        let mut limit: Option<SqlParamSource> = None;
        let mut offset: Option<SqlParamSource> = None;
        let mut aggregate: Option<(&str, String)> = None; // (func, column_sql)

        for op in &ops {
            match op {
                PipeOp::Filter { bind_var, body } => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let frag = Self::try_compile_multi_table_sql_expr(
                        &bind_aliases, body, env,
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
                PipeOp::Take { n } => {
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
                PipeOp::Sum { bind_var, body } => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    aggregate = Some(("SUM", col_sql));
                }
                PipeOp::Avg { bind_var, body } => {
                    if is_count || aggregate.is_some() {
                        return None;
                    }
                    bind_aliases.insert(bind_var.clone(), alias.clone());
                    let col_sql = extract_sql_field_access(bind_var, body, &alias, &schema)?;
                    aggregate = Some(("AVG", col_sql));
                }
            }
        }

        if let Some((func, col_sql)) = aggregate {
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let sql = if conditions.is_empty() {
                format!("SELECT {}({}) FROM {}", func, col_sql, table)
            } else {
                format!(
                    "SELECT {}({}) FROM {} AS {} WHERE {}",
                    func, col_sql, table, alias, conditions.join(" AND ")
                )
            };
            let params_rel = self.compile_sql_params(builder, &params, env);
            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
            let rt_fn = if func == "AVG" { "knot_source_query_float" } else { "knot_source_query_count" };
            Some(self.call_rt(
                builder,
                rt_fn,
                &[db, sql_ptr, sql_len, params_rel],
            ))
        } else if is_count {
            let table = quote_sql_ident(&format!("_knot_{}", source_name));
            let sql = if conditions.is_empty() {
                format!("SELECT COUNT(*) FROM {}", table)
            } else {
                format!(
                    "SELECT COUNT(*) FROM {} AS {} WHERE {}",
                    table, alias, conditions.join(" AND ")
                )
            };
            let params_rel = self.compile_sql_params(builder, &params, env);
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
                limit,
                offset,
            };

            let sql = plan.build_sql();
            let result_schema = plan.build_result_schema();
            let mut all_params = plan.params;
            if let Some(lim) = &plan.limit {
                all_params.push(lim.clone());
            }
            if let Some(off) = &plan.offset {
                all_params.push(off.clone());
            }
            let params_rel = self.compile_sql_params(builder, &all_params, env);
            let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
            let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
            Some(self.call_rt(
                builder,
                "knot_source_query",
                &[db, sql_ptr, sql_len, schema_ptr, schema_len, params_rel],
            ))
        }
    }

    /// Analyze do-block statements and produce a SQL query plan.
    /// Returns None if the block can't be compiled to a single SQL query.
    /// Compile `diff *a *b` / `inter *a *b` / `union *a *b` to SQL EXCEPT/INTERSECT/UNION.
    fn try_compile_set_op_sql(
        &mut self,
        builder: &mut FunctionBuilder,
        sql_op: &str,
        source_a: &str,
        source_b: &str,
        _env: &mut Env,
        db: Value,
    ) -> Option<Value> {
        // Both must be plain source relations (not views, ADTs, or nested)
        if self.views.contains_key(source_a) || self.views.contains_key(source_b) {
            return None;
        }
        let schema_a = self.source_schemas.get(source_a)?.clone();
        let schema_b = self.source_schemas.get(source_b)?.clone();
        if schema_a.starts_with('#') || schema_a.contains('[')
            || schema_b.starts_with('#') || schema_b.contains('[')
        {
            return None;
        }

        let table_a = quote_sql_ident(&format!("_knot_{}", source_a));
        let table_b = quote_sql_ident(&format!("_knot_{}", source_b));
        let sql = format!("SELECT * FROM {} {} SELECT * FROM {}", table_a, sql_op, table_b);
        let result_schema = schema_a.clone();
        let empty_params = self.call_rt(builder, "knot_relation_empty", &[]);
        let (sql_ptr, sql_len) = self.string_ptr(builder, &sql);
        let (schema_ptr, schema_len) = self.string_ptr(builder, &result_schema);
        Some(self.call_rt(
            builder,
            "knot_source_query",
            &[db, sql_ptr, sql_len, schema_ptr, schema_len, empty_params],
        ))
    }

    fn analyze_sql_plan(
        &self,
        stmts: &[ast::Stmt],
        env: &Env,
    ) -> Option<SqlQueryPlan> {
        let mut tables: Vec<SqlTable> = Vec::new();
        let mut bind_to_alias: HashMap<String, String> = HashMap::new();
        let mut bind_to_schema: HashMap<String, String> = HashMap::new();
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
                ast::StmtKind::Where { cond } => {
                    let frag = Self::try_compile_multi_table_sql_expr(
                        &bind_to_alias, cond, env,
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
                if let ast::ExprKind::Yield(inner) = &e.node {
                    inner
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        let mut select_columns: Vec<SqlSelectColumn> = Vec::new();

        match &yield_expr.node {
            ast::ExprKind::Record(fields) => {
                for field in fields {
                    if let ast::ExprKind::FieldAccess { expr, field: col_name } = &field.value.node
                    {
                        if let ast::ExprKind::Var(var_name) = &expr.node {
                            let alias = bind_to_alias.get(var_name)?.clone();
                            let schema = bind_to_schema.get(var_name)?;
                            let type_str = lookup_col_type_from_schema(schema, col_name)?;
                            select_columns.push(SqlSelectColumn {
                                result_field: field.name.clone(),
                                alias,
                                source_col: col_name.clone(),
                                type_str,
                                sql_expr: None,
                            });
                        } else {
                            return None;
                        }
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
            limit: None,
            offset: None,
        })
    }

    /// Compile a multi-table Where condition to a SQL fragment.
    /// Handles both join conditions (field = field) and filter conditions (field op ?).
    fn try_compile_multi_table_sql_expr(
        bind_aliases: &HashMap<String, String>,
        expr: &ast::Expr,
        env: &Env,
    ) -> Option<SqlFragment> {
        match &expr.node {
            ast::ExprKind::BinOp { op, lhs, rhs } => match op {
                ast::BinOp::And => {
                    let l = Self::try_compile_multi_table_sql_expr(bind_aliases, lhs, env)?;
                    let r = Self::try_compile_multi_table_sql_expr(bind_aliases, rhs, env)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) AND ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Or => {
                    let l = Self::try_compile_multi_table_sql_expr(bind_aliases, lhs, env)?;
                    let r = Self::try_compile_multi_table_sql_expr(bind_aliases, rhs, env)?;
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
                    Self::try_compile_multi_table_comparison(bind_aliases, lhs, rhs, sql_op, env)
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
                                bind_aliases, rhs, lhs, rev, env,
                            )
                        })
                }
                ast::BinOp::Add | ast::BinOp::Sub | ast::BinOp::Mul | ast::BinOp::Div => {
                    // Arithmetic in WHERE: try to compile both sides as SQL atoms
                    let sql_op = match op {
                        ast::BinOp::Add => "+",
                        ast::BinOp::Sub => "-",
                        ast::BinOp::Mul => "*",
                        ast::BinOp::Div => "/",
                        _ => unreachable!(),
                    };
                    let l = Self::try_compile_sql_atom(bind_aliases, lhs, env)?;
                    let r = Self::try_compile_sql_atom(bind_aliases, rhs, env)?;
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
                let inner = Self::try_compile_multi_table_sql_expr(bind_aliases, operand, env)?;
                Some(SqlFragment {
                    sql: format!("NOT ({})", inner.sql),
                    params: inner.params,
                })
            }
            _ => None,
        }
    }

    /// Try to compile an expression as a SQL atom (field access, literal, var, or arithmetic).
    /// Used as operands in comparisons and arithmetic.
    fn try_compile_sql_atom(
        bind_aliases: &HashMap<String, String>,
        expr: &ast::Expr,
        env: &Env,
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
                    if env.bindings.contains_key(name) {
                        return Some(SqlFragment {
                            sql: "?".to_string(),
                            params: vec![SqlParamSource::FieldAccess(name.clone(), field.clone())],
                        });
                    }
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
                } else if env.bindings.contains_key(name) {
                    Some(SqlFragment {
                        sql: "?".to_string(),
                        params: vec![SqlParamSource::Var(name.clone())],
                    })
                } else {
                    None
                }
            }
            ast::ExprKind::BinOp { op, lhs, rhs } => {
                let sql_op = match op {
                    ast::BinOp::Add => "+",
                    ast::BinOp::Sub => "-",
                    ast::BinOp::Mul => "*",
                    ast::BinOp::Div => "/",
                    _ => return None,
                };
                let l = Self::try_compile_sql_atom(bind_aliases, lhs, env)?;
                let r = Self::try_compile_sql_atom(bind_aliases, rhs, env)?;
                let mut params = l.params;
                params.extend(r.params);
                Some(SqlFragment {
                    sql: format!("({} {} {})", l.sql, sql_op, r.sql),
                    params,
                })
            }
            _ => None,
        }
    }

    /// Compile a multi-table comparison. Both sides can be field accesses,
    /// literals, variables, or arithmetic expressions.
    fn try_compile_multi_table_comparison(
        bind_aliases: &HashMap<String, String>,
        lhs: &ast::Expr,
        rhs: &ast::Expr,
        op: &str,
        env: &Env,
    ) -> Option<SqlFragment> {
        let l = Self::try_compile_sql_atom(bind_aliases, lhs, env)?;
        let r = Self::try_compile_sql_atom(bind_aliases, rhs, env)?;
        let mut params = l.params;
        params.extend(r.params);
        Some(SqlFragment {
            sql: format!("{} {} {}", l.sql, op, r.sql),
            params,
        })
    }

    // ── SQL expression compilation ──────────────────────────────────

    /// Try to compile a Knot condition to a SQL WHERE fragment.
    /// `bind_var` is the loop variable (e.g., "t" in `t <- *rel`).
    /// Field accesses on bind_var become column references;
    /// literals and free variables become bind parameters (?).
    fn try_compile_sql_expr(
        bind_var: &str,
        expr: &ast::Expr,
    ) -> Option<SqlFragment> {
        match &expr.node {
            ast::ExprKind::BinOp { op, lhs, rhs } => match op {
                ast::BinOp::And => {
                    let l = Self::try_compile_sql_expr(bind_var, lhs)?;
                    let r = Self::try_compile_sql_expr(bind_var, rhs)?;
                    let mut params = l.params;
                    params.extend(r.params);
                    Some(SqlFragment {
                        sql: format!("({}) AND ({})", l.sql, r.sql),
                        params,
                    })
                }
                ast::BinOp::Or => {
                    let l = Self::try_compile_sql_expr(bind_var, lhs)?;
                    let r = Self::try_compile_sql_expr(bind_var, rhs)?;
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
                    // Try field op value, then value op field (reversed)
                    Self::try_compile_sql_comparison(bind_var, lhs, rhs, sql_op)
                        .or_else(|| {
                            let rev = match sql_op {
                                "=" | "!=" => sql_op,
                                "<" => ">",
                                ">" => "<",
                                "<=" => ">=",
                                ">=" => "<=",
                                _ => return None,
                            };
                            Self::try_compile_sql_comparison(bind_var, rhs, lhs, rev)
                        })
                }
                _ => None,
            },
            ast::ExprKind::UnaryOp {
                op: ast::UnaryOp::Not,
                operand,
            } => {
                let inner = Self::try_compile_sql_expr(bind_var, operand)?;
                Some(SqlFragment {
                    sql: format!("NOT ({})", inner.sql),
                    params: inner.params,
                })
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
            _ => return None,
        };

        Some(SqlFragment {
            sql: format!("{} {} ?", quote_sql_ident(&col_name), op),
            params: vec![param],
        })
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
                    if let ast::ExprKind::FieldAccess { expr, field } = &e.node {
                        if let ast::ExprKind::Var(name) = &expr.node {
                            return Some((name.as_str(), field.as_str()));
                        }
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
        if let ast::ExprKind::Do(stmts) = &value.node {
            if stmts.len() == 2 {
                if let ast::StmtKind::Bind { expr, .. } = &stmts[0].node {
                    if let ast::ExprKind::SourceRef(name) = &expr.node {
                        if name == source_name {
                            if let ast::StmtKind::Expr(e) = &stmts[1].node {
                                return matches!(&e.node, ast::ExprKind::Yield(_));
                            }
                        }
                    }
                }
            }
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
            if let ast::ExprKind::Yield(inner) = &e.node {
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
    ) -> Option<(String, &'a ast::Expr, Vec<(&'a str, &'a ast::Expr)>)> {
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
        if let ast::StmtKind::Expr(e) = &stmts[1].node {
            if let ast::ExprKind::Yield(yield_inner) = &e.node {
                if let ast::ExprKind::If {
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
    ) -> Value {
        let rel = self.call_rt(builder, "knot_relation_empty", &[]);
        for param in params {
            let val = match param {
                SqlParamSource::Literal(lit) => self.compile_lit(builder, lit),
                SqlParamSource::Var(name) => env.get(name),
                SqlParamSource::FieldAccess(var, field) => {
                    let record = env.get(var);
                    let (fptr, flen) = self.string_ptr(builder, field);
                    self.call_rt(builder, "knot_record_field", &[record, fptr, flen])
                }
            };
            self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
        }
        rel
    }

    // ── Operator trait dispatch helpers ────────────────────────────

    /// Compile a binary operator via trait dispatch (e.g., `+` → `add` dispatcher).
    /// Falls back to `fallback_rt` if no dispatcher exists (e.g., user redefined the trait).
    fn compile_trait_binop(
        &mut self,
        builder: &mut FunctionBuilder,
        method: &str,
        l: Value,
        r: Value,
        db: Value,
        fallback_rt: &str,
    ) -> Value {
        if let Some(&func_id) = self.trait_dispatcher_fns.get(method) {
            let func_ref = self.module.declare_func_in_func(func_id, builder.func);
            let call = builder.ins().call(func_ref, &[db, l, r]);
            builder.inst_results(call)[0]
        } else {
            self.call_rt(builder, fallback_rt, &[l, r])
        }
    }

    /// Compile a unary operator via trait dispatch (e.g., `-x` → `negate` dispatcher).
    fn compile_trait_unop(
        &mut self,
        builder: &mut FunctionBuilder,
        method: &str,
        val: Value,
        db: Value,
        fallback_rt: &str,
    ) -> Value {
        if let Some(&func_id) = self.trait_dispatcher_fns.get(method) {
            let func_ref = self.module.declare_func_in_func(func_id, builder.func);
            let call = builder.ins().call(func_ref, &[db, val]);
            builder.inst_results(call)[0]
        } else {
            self.call_rt(builder, fallback_rt, &[val])
        }
    }

    /// Compile a comparison operator via the `compare` trait dispatcher.
    /// Calls `compare(l, r)` to get an Ordering value, then checks the constructor tag.
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
        if let Some(&func_id) = self.trait_dispatcher_fns.get("compare") {
            let func_ref = self.module.declare_func_in_func(func_id, builder.func);
            let call = builder.ins().call(func_ref, &[db, l, r]);
            let ordering = builder.inst_results(call)[0];

            // Check if ordering matches the tag
            let (tag_ptr, tag_len) = self.string_ptr(builder, match_tag);
            let matches = self.call_rt_typed(
                builder,
                "knot_constructor_matches",
                &[ordering, tag_ptr, tag_len],
                types::I32,
            );

            let result_i32 = if negate {
                // Negate: 1 → 0, 0 → 1  (e.g., <= means NOT GT)
                let one = builder.ins().iconst(types::I32, 1);
                builder.ins().isub(one, matches)
            } else {
                matches
            };

            self.call_rt(builder, "knot_value_bool", &[result_i32])
        } else {
            // Fallback to direct runtime comparison
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
}

// ── View analysis ─────────────────────────────────────────────────

/// Analyze a view body expression to extract column provenance.
/// Returns `None` if the view body cannot be analyzed (unsupported pattern).
fn analyze_view(body: &ast::Expr) -> Option<ViewInfo> {
    // Case 1: simple alias — *view = *source
    if let ast::ExprKind::SourceRef(source_name) = &body.node {
        return Some(ViewInfo {
            source_name: source_name.clone(),
            source_columns: vec![],
            constant_columns: vec![],
            body: body.clone(),
        });
    }

    // Case 2: do-block with bind + yield
    if let ast::ExprKind::Do(stmts) = &body.node {
        // Find the bind statement: t <- *source
        let bind_info = stmts.iter().find_map(|s| {
            if let ast::StmtKind::Bind { pat, expr } = &s.node {
                if let ast::ExprKind::SourceRef(source_name) = &expr.node {
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        return Some((var_name.clone(), source_name.clone()));
                    }
                }
            }
            None
        })?;

        let (bind_var, source_name) = bind_info;

        // Find the yield expression with a record
        let yield_record = stmts.iter().rev().find_map(|s| {
            if let ast::StmtKind::Expr(expr) = &s.node {
                if let ast::ExprKind::Yield(inner) = &expr.node {
                    if let ast::ExprKind::Record(fields) = &inner.node {
                        return Some(fields.clone());
                    }
                }
            }
            None
        })?;

        let mut source_columns = Vec::new();
        let mut constant_columns = Vec::new();

        for field in &yield_record {
            // Check if it's a field access on the bind var: t.field
            if let ast::ExprKind::FieldAccess {
                expr,
                field: accessed_field,
            } = &field.value.node
            {
                if let ast::ExprKind::Var(var_name) = &expr.node {
                    if var_name == &bind_var {
                        source_columns.push((field.name.clone(), accessed_field.clone()));
                        continue;
                    }
                }
            }
            // Check it doesn't reference the bind var (constant column)
            if !expr_references_var(&field.value, &bind_var) {
                constant_columns.push((field.name.clone(), field.value.clone()));
            }
            // If it references bind_var but isn't a simple field access,
            // it's a computed column — view reads work, writes are not supported.
        }

        return Some(ViewInfo {
            source_name,
            source_columns,
            constant_columns,
            body: body.clone(),
        });
    }

    None
}

/// Check if an expression references a specific variable name.
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
        // Conservatively return true for complex expressions
        _ => true,
    }
}

// ── SQL compilation types ─────────────────────────────────────────

/// Escape a SQL identifier by wrapping in double quotes, doubling internal `"`.
fn quote_sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct SqlFragment {
    sql: String,
    params: Vec<SqlParamSource>,
}

#[derive(Clone)]
enum SqlParamSource {
    Literal(ast::Literal),
    Var(String),
    FieldAccess(String, String), // (var_name, field_name)
}

// ── SQL query plan types ────────────────────────────────────────

struct SqlQueryPlan {
    tables: Vec<SqlTable>,
    conditions: Vec<String>,
    params: Vec<SqlParamSource>,
    select_columns: Vec<SqlSelectColumn>,
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
                if let Some(ref sql_expr) = c.sql_expr {
                    format!("{} AS {}", sql_expr, quote_sql_ident(&c.result_field))
                } else {
                    format!("{}.{}", c.alias, quote_sql_ident(&c.source_col))
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
            let where_clause = self.conditions.join(" AND ");
            format!("SELECT {} FROM {} WHERE {}", select, from, where_clause)
        };

        if self.limit.is_some() || self.offset.is_some() {
            sql.push_str(&format!(" LIMIT {}", if self.limit.is_some() { "?" } else { "-1" }));
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

fn lookup_col_type_from_schema(schema: &str, col_name: &str) -> Option<String> {
    for part in schema.split(',') {
        let colon = part.find(':')?;
        let name = &part[..colon];
        let ty = &part[colon + 1..];
        if name == col_name {
            return Some(ty.to_string());
        }
    }
    None
}

fn parse_schema_columns(schema: &str) -> Vec<(String, String)> {
    schema
        .split(',')
        .filter_map(|part| {
            let colon = part.find(':')?;
            let name = part[..colon].to_string();
            let ty = part[colon + 1..].to_string();
            Some((name, ty))
        })
        .collect()
}

// ── Pipe chain analysis ───────────────────────────────────────────

enum PipeOp<'a> {
    Filter { bind_var: String, body: &'a ast::Expr },
    Map { bind_var: String, body: &'a ast::Expr },
    Count,
    Take { n: &'a ast::Expr },
    Drop { n: &'a ast::Expr },
    Sum { bind_var: String, body: &'a ast::Expr },
    Avg { bind_var: String, body: &'a ast::Expr },
}

/// Flatten a nested pipe chain `a |> f |> g |> h` into `(a, [f, g, h])`.
/// Each operation must be a recognized stdlib function (filter, map, count).
fn flatten_pipe_chain(expr: &ast::Expr) -> Option<(&ast::Expr, Vec<PipeOp<'_>>)> {
    let mut ops = Vec::new();
    let mut current = expr;

    loop {
        match &current.node {
            ast::ExprKind::BinOp {
                op: ast::BinOp::Pipe,
                lhs,
                rhs,
            } => {
                let pipe_op = analyze_pipe_op(rhs)?;
                ops.push(pipe_op);
                current = lhs;
            }
            _ => break,
        }
    }

    ops.reverse();
    Some((current, ops))
}

/// Recognize a pipe RHS as a SQL-compilable operation.
fn analyze_pipe_op(expr: &ast::Expr) -> Option<PipeOp<'_>> {
    match &expr.node {
        ast::ExprKind::Var(name) if name == "count" => Some(PipeOp::Count),
        ast::ExprKind::App { func, arg } => {
            if let ast::ExprKind::Var(name) = &func.node {
                match name.as_str() {
                    "filter" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        PipeOp::Filter { bind_var, body }
                    }),
                    "map" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        PipeOp::Map { bind_var, body }
                    }),
                    "take" => Some(PipeOp::Take { n: arg }),
                    "drop" => Some(PipeOp::Drop { n: arg }),
                    "sum" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        PipeOp::Sum { bind_var, body }
                    }),
                    "avg" => extract_single_param_lambda(arg).map(|(bind_var, body)| {
                        PipeOp::Avg { bind_var, body }
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

/// Extract bind variable name and body from a single-parameter lambda.
fn extract_single_param_lambda(expr: &ast::Expr) -> Option<(String, &ast::Expr)> {
    if let ast::ExprKind::Lambda { params, body } = &expr.node {
        if params.len() == 1 {
            if let ast::PatKind::Var(name) = &params[0].node {
                return Some((name.clone(), body));
            }
        }
    }
    None
}

/// Convert an expression to a SQL parameter source (literal int or variable).
fn expr_to_sql_param(expr: &ast::Expr) -> Option<SqlParamSource> {
    match &expr.node {
        ast::ExprKind::Lit(lit) => Some(SqlParamSource::Literal(lit.clone())),
        ast::ExprKind::Var(name) => Some(SqlParamSource::Var(name.clone())),
        _ => None,
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
    if let ast::ExprKind::FieldAccess { expr, field: col_name } = &body.node {
        if let ast::ExprKind::Var(name) = &expr.node {
            if name == bind_var {
                // Verify column exists in schema
                lookup_col_type_from_schema(schema, col_name)?;
                return Some(sql_col_ref(alias, col_name));
            }
        }
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

/// Try to compile an arithmetic expression to a SQL fragment.
/// Handles: field access, literals, and +, -, *, / binary ops.
fn try_sql_arithmetic_expr(
    bind_var: &str,
    expr: &ast::Expr,
    alias: &str,
    schema: &str,
) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node {
                if name == bind_var {
                    lookup_col_type_from_schema(schema, col_name)?;
                    return Some(sql_col_ref(alias, col_name));
                }
            }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(n) => Some(n.to_string()),
            ast::Literal::Float(f) => Some(f.to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let sql_op = match op {
                ast::BinOp::Add => "+",
                ast::BinOp::Sub => "-",
                ast::BinOp::Mul => "*",
                ast::BinOp::Div => "/",
                _ => return None,
            };
            let l = try_sql_arithmetic_expr(bind_var, lhs, alias, schema)?;
            let r = try_sql_arithmetic_expr(bind_var, rhs, alias, schema)?;
            Some(format!("({} {} {})", l, sql_op, r))
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
            {
                if let ast::ExprKind::Var(name) = &expr.node {
                    if name == bind_var {
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
                }
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
fn infer_sql_expr_type(bind_var: &str, expr: &ast::Expr, schema: &str) -> Option<String> {
    match &expr.node {
        ast::ExprKind::FieldAccess { expr: inner, field: col_name } => {
            if let ast::ExprKind::Var(name) = &inner.node {
                if name == bind_var {
                    return lookup_col_type_from_schema(schema, col_name);
                }
            }
            None
        }
        ast::ExprKind::Lit(lit) => match lit {
            ast::Literal::Int(_) => Some("int".to_string()),
            ast::Literal::Float(_) => Some("float".to_string()),
            _ => None,
        },
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let l = infer_sql_expr_type(bind_var, lhs, schema);
            let r = infer_sql_expr_type(bind_var, rhs, schema);
            match (l.as_deref(), r.as_deref(), op) {
                // Division always produces float
                (_, _, ast::BinOp::Div) => Some("float".to_string()),
                // Float on either side → float
                (Some("float"), _, _) | (_, Some("float"), _) => Some("float".to_string()),
                (Some(t), _, _) => Some(t.to_string()),
                (_, Some(t), _) => Some(t.to_string()),
                _ => None,
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

fn bind_pattern_env(pat: &ast::Pat, val: Value, env: &mut Env) {
    match &pat.node {
        ast::PatKind::Var(name) => env.set(name, val),
        ast::PatKind::Wildcard => {}
        _ => {} // Simplified: only var and wildcard bindings for now
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
            let is_match = match cg.nullable_ctors.get(name).cloned() {
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
            };

            let then_block = builder.create_block();
            let skip_block = builder.create_block();
            builder.ins().brif(is_match, then_block, &[], skip_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            skips.push(skip_block);

            // Extract payload and bind inner pattern
            if cg.nullable_ctors.contains_key(name) {
                // Nullable: val is the bare payload (or null for none)
                bind_do_pattern(builder, cg, payload, val, env, skips);
            } else {
                let inner = cg.call_rt(builder, "knot_constructor_payload", &[val]);
                bind_do_pattern(builder, cg, payload, inner, env, skips);
            }
        }
        _ => {}
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
        ast::ExprKind::Yield(inner) | ast::ExprKind::Atomic(inner) => {
            expr_contains_derived_ref(inner, name)
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            expr_contains_derived_ref(target, name) || expr_contains_derived_ref(value, name)
        }
        ast::ExprKind::At { relation, time } => {
            expr_contains_derived_ref(relation, name) || expr_contains_derived_ref(time, name)
        }
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
            if !bound.contains(name.as_str()) && !is_builtin_name(name) {
                free.push(name.clone());
            }
        }
        ast::ExprKind::Lit(_) | ast::ExprKind::Constructor(_) => {}
        ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) => {}
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
                if let ast::PatKind::Var(name) = &p.node {
                    new_bound.insert(name.as_str());
                }
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
        ast::ExprKind::Yield(inner) => {
            collect_free_vars(inner, bound, free);
        }
        ast::ExprKind::Set { target, value }
        | ast::ExprKind::FullSet { target, value } => {
            collect_free_vars(target, bound, free);
            collect_free_vars(value, bound, free);
        }
        ast::ExprKind::Atomic(inner) => {
            collect_free_vars(inner, bound, free);
        }
        ast::ExprKind::At { relation, time } => {
            collect_free_vars(relation, bound, free);
            collect_free_vars(time, bound, free);
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
    }
}

fn is_builtin_name(name: &str) -> bool {
    matches!(
        name,
        "println"
            | "putLine"
            | "print"
            | "show"
            | "union"
            | "count"
            | "filter"
            | "map"
            | "fold"
            | "now"
            | "__bind"
            | "__yield"
            | "__empty"
            | "listen"
            | "single"
            | "toUpper"
            | "toLower"
            | "take"
            | "drop"
            | "length"
            | "trim"
            | "contains"
            | "reverse"
            | "chars"
            | "id"
            | "not"
            // Built-in trait methods
            | "eq"
            | "compare"
            | "ap"
            | "bind"
            | "alt"
            | "empty"
            | "readFile"
            | "writeFile"
            | "appendFile"
            | "fileExists"
            | "removeFile"
            | "listDir"
            | "generateKeyPair"
            | "generateSigningKeyPair"
            | "encrypt"
            | "decrypt"
            | "sign"
            | "verify"
    )
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
        ast::ExprKind::Yield(e) => format!("yield {}", pretty_expr(e)),
        ast::ExprKind::Set { target, value } => {
            format!("set {} = {}", pretty_expr(target), pretty_expr(value))
        }
        ast::ExprKind::FullSet { target, value } => {
            format!(
                "full set {} = {}",
                pretty_expr(target),
                pretty_expr(value)
            )
        }
        ast::ExprKind::Atomic(e) => format!("atomic ({})", pretty_expr(e)),
        ast::ExprKind::At { relation, time } => {
            format!("{} @({})", pretty_expr(relation), pretty_expr(time))
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
    }
}

fn pretty_binop(op: &ast::BinOp) -> &'static str {
    match op {
        ast::BinOp::Add => "+",
        ast::BinOp::Sub => "-",
        ast::BinOp::Mul => "*",
        ast::BinOp::Div => "/",
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
fn find_dispatch_index(hkt_param: Option<&str>, ty: &ast::Type) -> Option<usize> {
    let param_name = hkt_param?;
    let mut current = ty;
    let mut index = 0;
    loop {
        match &current.node {
            ast::TypeKind::Function { param, result } => {
                if type_uses_hkt_var(param, param_name) {
                    return Some(index);
                }
                index += 1;
                current = result;
            }
            _ => return None,
        }
    }
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
        _ => None,
    }
}

/// Map a type name to its runtime Value tag (as used by knot_value_get_tag).
fn type_name_to_tag(name: &str) -> Option<i64> {
    match name {
        "Int" => Some(0),
        "Float" => Some(1),
        "Text" => Some(2),
        "Bool" => Some(3),
        "Unit" => Some(4),
        "Relation" => Some(6),
        _ => None,
    }
}

/// Convert route path segments to a pattern string like "/todos/{owner:text}".
fn path_segments_to_pattern(segments: &[ast::PathSegment]) -> String {
    let mut parts = Vec::new();
    for seg in segments {
        match seg {
            ast::PathSegment::Literal(s) => parts.push(s.clone()),
            ast::PathSegment::Param { name, ty } => {
                let ty_str = ast_type_to_descriptor_type(ty);
                parts.push(format!("{{{name}:{ty_str}}}"));
            }
        }
    }
    format!("/{}", parts.join("/"))
}

/// Convert typed fields to a descriptor string like "name:text,age:int".
fn fields_to_descriptor(fields: &[ast::Field<ast::Type>]) -> String {
    fields
        .iter()
        .map(|f| {
            let ty_str = ast_type_to_descriptor_type(&f.value);
            format!("{}:{}", f.name, ty_str)
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn ast_type_to_descriptor_type(ty: &ast::Type) -> String {
    match &ty.node {
        ast::TypeKind::Named(n) => match n.as_str() {
            "Int" => "int".to_string(),
            "Float" => "float".to_string(),
            "Bool" => "bool".to_string(),
            "Text" => "text".to_string(),
            _ => "text".to_string(),
        },
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
            // Represent ADT as object with _tag + all constructor fields
            let mut fields: Vec<String> = vec!["_tag:text".to_string()];
            let mut seen = std::collections::HashSet::<String>::new();
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
