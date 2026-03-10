//! Cranelift-based code generator for the Knot language.
//!
//! Compiles a Knot AST into a native object file. All Knot values are
//! represented at the machine level as pointers to heap-allocated tagged
//! values (managed by the runtime). The generated code calls into runtime
//! functions for value construction, operations, and SQLite persistence.

use crate::types::TypeEnv;
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

    // Migration schemas: relation_name -> (old_schema, new_schema)
    migrate_schemas: HashMap<String, (String, String)>,

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
    cg.history_sources = type_env.history_sources.clone();
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
        }
    }

    // ── Runtime function declarations ─────────────────────────────

    fn declare_runtime_fns(&mut self) {
        let p = self.ptr_type;

        // Value constructors
        self.declare_rt("knot_value_int", &[types::I64], &[p]);
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
        self.declare_rt("knot_relation_union", &[p, p], &[p]);

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

        // Unary operations
        self.declare_rt("knot_value_negate", &[p], &[p]);
        self.declare_rt("knot_value_not", &[p], &[p]);

        // Function calls
        self.declare_rt("knot_value_call", &[p, p, p], &[p]);

        // Printing / show
        self.declare_rt("knot_print", &[p], &[p]);
        self.declare_rt("knot_println", &[p], &[p]);
        self.declare_rt("knot_value_show", &[p], &[p]);

        // Database
        self.declare_rt("knot_db_open", &[p, p], &[p]);
        self.declare_rt("knot_db_close", &[p], &[]);
        self.declare_rt("knot_db_exec", &[p, p, p], &[]);
        self.declare_rt("knot_source_init", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_source_read", &[p, p, p, p, p], &[p]);
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

        // Trait dispatch error
        self.declare_rt("knot_trait_no_impl", &[p, p, p], &[p]);

        // Type tag inspection (for trait dispatch)
        self.declare_rt("knot_value_get_tag", &[p], &[types::I32]);

        // Temporal queries (history)
        self.declare_rt("knot_now", &[], &[p]);
        self.declare_rt("knot_history_init", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_history_snapshot", &[p, p, p, p, p], &[]);
        self.declare_rt("knot_source_read_at", &[p, p, p, p, p, p], &[p]);
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

    // ── Declaration collection ────────────────────────────────────

    fn collect_declarations(&mut self, module: &ast::Module) {
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
                ast::DeclKind::Derived { name, .. } => {
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
                }
                ast::DeclKind::Data {
                    name,
                    constructors: ctors,
                    ..
                } => {
                    let ctor_names: Vec<String> =
                        ctors.iter().map(|c| c.name.clone()).collect();
                    self.data_constructors.insert(name.clone(), ctor_names);
                }
                ast::DeclKind::Trait {
                    name: trait_name,
                    supertraits,
                    items,
                    ..
                } => {
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
                                self.trait_methods
                                    .entry(method_name.clone())
                                    .or_insert(TraitMethodInfo {
                                        param_count,
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
                        let defaults_to_derive: Vec<(String, DefaultMethod)> = trait_def
                            .defaults
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
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

        // Clone the data we need to iterate so we can push diagnostics
        let impl_types: Vec<(String, Vec<(String, ast::Span)>)> = self
            .trait_impl_types
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let supertraits: HashMap<String, Vec<String>> = self.trait_supertraits.clone();

        for (trait_name, types) in &impl_types {
            if let Some(required) = supertraits.get(trait_name) {
                for supertrait in required {
                    for (type_name, span) in types {
                        if !impl_set.contains(&(supertrait.as_str(), type_name.as_str())) {
                            self.diagnostics.push(
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
    }

    // ── Function definitions ──────────────────────────────────────

    fn define_functions(&mut self, module: &ast::Module, _type_env: &TypeEnv) {
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
                    self.define_user_function(name, &[], body);
                }
                ast::DeclKind::Impl {
                    trait_name,
                    args,
                    items,
                    ..
                } => {
                    if let Some(type_name) = impl_type_name(args) {
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
                            if let ast::ImplItem::Method { name, params, body } =
                                item
                            {
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, name
                                );
                                self.define_user_function(&mangled, params, body);
                            }
                        }

                        // Define default method bodies for methods not in this impl
                        if let Some(trait_def) = self.trait_defs.get(trait_name) {
                            let defaults_to_define: Vec<(String, DefaultMethod)> =
                                trait_def
                                    .defaults
                                    .iter()
                                    .filter(|(method_name, _)| {
                                        !provided_methods.contains(method_name)
                                    })
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                            for (method_name, default) in defaults_to_define {
                                let mangled = format!(
                                    "{}_{}_{}", trait_name, type_name, method_name
                                );
                                self.define_user_function(
                                    &mangled,
                                    &default.params,
                                    &default.body,
                                );
                            }
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
        let dispatcher_info: Vec<(String, FuncId, usize, Vec<(String, FuncId)>)> = self
            .trait_dispatcher_fns
            .iter()
            .filter_map(|(method_name, &dispatcher_id)| {
                let info = self.trait_methods.get(method_name)?;
                let impls: Vec<(String, FuncId)> = info
                    .impls
                    .iter()
                    .map(|e| (e.type_name.clone(), e.func_id))
                    .collect();
                Some((method_name.clone(), dispatcher_id, info.param_count, impls))
            })
            .collect();

        let data_ctors = self.data_constructors.clone();

        for (method_name, dispatcher_id, param_count, impls) in dispatcher_info {
            let mut sig = self.module.make_signature();
            sig.params.push(AbiParam::new(self.ptr_type)); // db
            for _ in 0..param_count {
                sig.params.push(AbiParam::new(self.ptr_type));
            }
            sig.returns.push(AbiParam::new(self.ptr_type));

            let data_ctors_ref = data_ctors.clone();

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

                let dispatch_arg = all_params[0];

                let merge_block = builder.create_block();
                merge_block_param(builder, merge_block, cg.ptr_type);

                // Get value tag for dispatch
                let tag = cg.call_rt_typed(
                    builder,
                    "knot_value_get_tag",
                    &[dispatch_arg],
                    types::I32,
                );

                // Separate primitive and ADT impls
                let mut primitive_impls: Vec<(i64, FuncId)> = Vec::new();
                let mut adt_impls: Vec<(Vec<String>, FuncId)> = Vec::new();
                for (type_name, impl_func_id) in &impls {
                    if let Some(runtime_tag) = type_name_to_tag(type_name) {
                        primitive_impls.push((runtime_tag, *impl_func_id));
                    } else if let Some(ctors) = data_ctors_ref.get(type_name) {
                        adt_impls.push((ctors.clone(), *impl_func_id));
                    }
                }

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

                // Generate ADT type checks (Constructor tag + constructor name)
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

                // Fallback: runtime error (no matching impl found)
                let (name_ptr, name_len) =
                    cg.string_ptr(builder, &method_name);
                let err = cg.call_rt(
                    builder,
                    "knot_trait_no_impl",
                    &[name_ptr, name_len, dispatch_arg],
                );
                builder.ins().jump(merge_block, &[err]);

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

    // ── Main function generation ──────────────────────────────────

    fn generate_main(&mut self, module: &ast::Module) {
        let mut sig = self.module.make_signature();
        sig.returns.push(AbiParam::new(types::I32));
        let main_id = self
            .module
            .declare_function("main", Linkage::Export, &sig)
            .unwrap();

        let decls: Vec<ast::Decl> = module.decls.clone();
        let user_main = self.user_fns.get("main").copied();

        self.build_function(main_id, sig, |cg, builder, _entry| {
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
            for decl in &decls {
                if let ast::DeclKind::Migrate {
                    relation,
                    using_fn,
                    ..
                } = &decl.node
                {
                    if let Some((old_schema, new_schema)) = migrate_schemas.get(relation) {
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

            ast::ExprKind::Var(name) => {
                if name == "now" {
                    return self.call_rt(builder, "knot_now", &[]);
                }
                if env.bindings.contains_key(name) {
                    env.get(name)
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
                        // Create a function value wrapping the user function
                        let func_ref =
                            self.module.declare_func_in_func(func_id, builder.func);
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
                // Bare constructor reference — return as a unit constructor
                let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                let unit = self.call_rt(builder, "knot_value_unit", &[]);
                let ctor =
                    self.call_rt(builder, "knot_value_constructor", &[tag_ptr, tag_len, unit]);
                ctor
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
                // For now, treat derived refs like function calls
                if let Some((func_id, 0)) = self.user_fns.get(name).copied() {
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
                    // lhs |> rhs  =>  rhs(lhs)
                    let arg = self.compile_expr(builder, lhs, env, db);
                    let func = self.compile_expr(builder, rhs, env, db);
                    self.call_rt(builder, "knot_value_call", &[db, func, arg])
                } else {
                    let l = self.compile_expr(builder, lhs, env, db);
                    let r = self.compile_expr(builder, rhs, env, db);
                    let fn_name = match op {
                        ast::BinOp::Add => "knot_value_add",
                        ast::BinOp::Sub => "knot_value_sub",
                        ast::BinOp::Mul => "knot_value_mul",
                        ast::BinOp::Div => "knot_value_div",
                        ast::BinOp::Eq => "knot_value_eq",
                        ast::BinOp::Neq => "knot_value_neq",
                        ast::BinOp::Lt => "knot_value_lt",
                        ast::BinOp::Gt => "knot_value_gt",
                        ast::BinOp::Le => "knot_value_le",
                        ast::BinOp::Ge => "knot_value_ge",
                        ast::BinOp::And => "knot_value_and",
                        ast::BinOp::Or => "knot_value_or",
                        ast::BinOp::Concat => "knot_value_concat",
                        ast::BinOp::Pipe => unreachable!(),
                    };
                    self.call_rt(builder, fn_name, &[l, r])
                }
            }

            ast::ExprKind::UnaryOp { op, operand } => {
                let val = self.compile_expr(builder, operand, env, db);
                let fn_name = match op {
                    ast::UnaryOp::Neg => "knot_value_negate",
                    ast::UnaryOp::Not => "knot_value_not",
                };
                self.call_rt(builder, fn_name, &[val])
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

            ast::ExprKind::App { func: _, arg: _ } => {
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
                // Temporal query: *source @(timestamp)
                if let ast::ExprKind::SourceRef(name) = &relation.node {
                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();
                    let timestamp = self.compile_expr(builder, time, env, db);
                    let (name_ptr, name_len) = self.string_ptr(builder, name);
                    let (schema_ptr, schema_len) = self.string_ptr(builder, &schema);
                    self.call_rt(
                        builder,
                        "knot_source_read_at",
                        &[db, name_ptr, name_len, schema_ptr, schema_len, timestamp],
                    )
                } else {
                    // For non-source At expressions, compile the relation normally
                    // (future: could support views with history)
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
        let compiled_args: Vec<Value> = args
            .iter()
            .map(|a| self.compile_expr(builder, a, env, db))
            .collect();

        match &func_expr.node {
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
                        &[compiled_args[0], compiled_args[1]],
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

            // Constructor application: `Circle {radius: 3.14}`
            ast::ExprKind::Constructor(name) => {
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
                    let (tag_ptr, tag_len) = self.string_ptr(builder, name);
                    let matches = self.call_rt_typed(
                        builder,
                        "knot_constructor_matches",
                        &[scrut, tag_ptr, tag_len],
                        types::I32,
                    );
                    let is_match =
                        builder.ins().icmp_imm(IntCC::NotEqual, matches, 0);
                    builder
                        .ins()
                        .brif(is_match, arm_block, &[], next_block, &[]);
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
                _ => {
                    // Default: try matching
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
            ast::PatKind::Constructor { name: _, payload } => {
                let inner = self.call_rt(builder, "knot_constructor_payload", &[val]);
                self.bind_case_pattern(builder, payload, inner, env);
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
            ast::PatKind::List(_) => {
                // List pattern not yet implemented in codegen
            }
        }
    }

    // ── Do-block compilation ──────────────────────────────────────

    fn compile_do(
        &mut self,
        builder: &mut FunctionBuilder,
        stmts: &[ast::Stmt],
        env: &mut Env,
        db: Value,
    ) -> Value {
        let result = self.call_rt(builder, "knot_relation_empty", &[]);
        let mut loop_stack: Vec<LoopInfo> = Vec::new();

        for (stmt_idx, stmt) in stmts.iter().enumerate() {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    let rel = self.compile_expr(builder, expr, env, db);
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

                    // Bind pattern
                    bind_do_pattern(builder, self, pat, row, env);

                    loop_stack.push(LoopInfo {
                        header,
                        continue_blk,
                        exit,
                        index_var: i,
                        where_skips: Vec::new(),
                    });
                }

                ast::StmtKind::Where { cond } => {
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
        let free_vars = find_free_vars(body, &param_names);

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
                let n_val = builder.ins().iconst(types::I64, *n);
                self.call_rt(builder, "knot_value_int", &[n_val])
            }
            ast::Literal::Float(n) => {
                let n_val = builder.ins().f64const(*n);
                self.call_rt(builder, "knot_value_float", &[n_val])
            }
            ast::Literal::Text(s) => {
                let (ptr, len) = self.string_ptr(builder, s);
                self.call_rt(builder, "knot_value_text", &[ptr, len])
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
            _ => return None,
        };

        Some(SqlFragment {
            sql: format!("{} {} ?", quote_sql_ident(&col_name), op),
            params: vec![param],
        })
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
            };
            self.call_rt_void(builder, "knot_relation_push", &[rel, val]);
        }
        rel
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
                    bind_do_pattern(builder, cg, inner_pat, field_val, env);
                } else {
                    env.set(&fp.name, field_val);
                }
            }
        }
        ast::PatKind::Constructor { name: _, payload } => {
            // Pattern match bind: `Circle c <- *shapes`
            // This filters — only rows matching the constructor are bound
            // For now, just extract the payload
            let inner = cg.call_rt(builder, "knot_constructor_payload", &[val]);
            bind_do_pattern(builder, cg, payload, inner, env);
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

/// Find free variables in an expression (variables not bound by params).
fn find_free_vars(expr: &ast::Expr, bound: &[String]) -> Vec<String> {
    let mut free = Vec::new();
    collect_free_vars(expr, bound, &mut free);
    free.sort();
    free.dedup();
    free
}

fn collect_free_vars(expr: &ast::Expr, bound: &[String], free: &mut Vec<String>) {
    match &expr.node {
        ast::ExprKind::Var(name) => {
            if !bound.contains(name) && !is_builtin_name(name) {
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
            let mut new_bound: Vec<String> = bound.to_vec();
            for p in params {
                if let ast::PatKind::Var(name) = &p.node {
                    new_bound.push(name.clone());
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
                let mut arm_bound = bound.to_vec();
                collect_pat_bindings(&arm.pat, &mut arm_bound);
                collect_free_vars(&arm.body, &arm_bound, free);
            }
        }
        ast::ExprKind::Do(stmts) => {
            let mut do_bound = bound.to_vec();
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } => {
                        collect_free_vars(expr, &do_bound, free);
                        collect_pat_bindings(pat, &mut do_bound);
                    }
                    ast::StmtKind::Let { pat, expr } => {
                        collect_free_vars(expr, &do_bound, free);
                        collect_pat_bindings(pat, &mut do_bound);
                    }
                    ast::StmtKind::Where { cond } => {
                        collect_free_vars(cond, &do_bound, free);
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

fn collect_pat_bindings(pat: &ast::Pat, bound: &mut Vec<String>) {
    match &pat.node {
        ast::PatKind::Var(name) => bound.push(name.clone()),
        ast::PatKind::Wildcard => {}
        ast::PatKind::Constructor { payload, .. } => {
            collect_pat_bindings(payload, bound);
        }
        ast::PatKind::Record(fields) => {
            for f in fields {
                if let Some(p) = &f.pattern {
                    collect_pat_bindings(p, bound);
                } else {
                    bound.push(f.name.clone());
                }
            }
        }
        ast::PatKind::Lit(_) => {}
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_bindings(p, bound);
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
        ast::StmtKind::Expr(e) => pretty_expr(e),
    }
}

// ── Trait support helpers ─────────────────────────────────────────

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
        ast::TypeKind::Named(name) => Some(name.clone()),
        ast::TypeKind::Relation(_) => Some("Relation".to_string()),
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
