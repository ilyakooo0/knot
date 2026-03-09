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
use std::collections::HashMap;

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

    // Collected diagnostics
    diagnostics: Vec<knot::diagnostic::Diagnostic>,
}

struct PendingLambda {
    func_id: FuncId,
    params: Vec<String>,
    body: ast::Expr,
    free_vars: Vec<String>,
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
    for (name, fields) in &type_env.constructors {
        let field_strs: Vec<(String, String)> = fields
            .iter()
            .map(|(n, _)| (n.clone(), "unknown".into()))
            .collect();
        cg.constructors.insert(name.clone(), field_strs);
    }
    cg.declare_runtime_fns();
    cg.collect_declarations(module);
    cg.define_functions(module, type_env);
    cg.generate_main(module);
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
            diagnostics: Vec::new(),
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
        self.declare_rt("knot_value_function", &[p, p], &[p]);
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

        // Printing
        self.declare_rt("knot_print", &[p], &[p]);
        self.declare_rt("knot_println", &[p], &[p]);

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

        // Debug
        self.declare_rt("knot_debug_init", &[], &[]);

        // Transactions
        self.declare_rt("knot_atomic_begin", &[p], &[]);
        self.declare_rt("knot_atomic_commit", &[p], &[]);

        // Constructor matching
        self.declare_rt("knot_constructor_matches", &[p, p, p], &[types::I32]);
        self.declare_rt("knot_constructor_payload", &[p], &[p]);
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
                ast::DeclKind::Fun { name, params, .. } => {
                    let n_params = params.len();
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
                _ => {}
            }
        }
    }

    // ── Function definitions ──────────────────────────────────────

    fn define_functions(&mut self, module: &ast::Module, _type_env: &TypeEnv) {
        for decl in &module.decls {
            if let ast::DeclKind::Fun {
                name,
                params,
                body,
                ..
            } = &decl.node
            {
                self.define_user_function(name, params, body);
            }
        }

        // Compile any pending lambdas (may generate more lambdas)
        while !self.pending_lambdas.is_empty() {
            let lambdas: Vec<PendingLambda> =
                std::mem::take(&mut self.pending_lambdas);
            for lambda in lambdas {
                self.define_lambda_function(&lambda);
            }
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
                if env.bindings.contains_key(name) {
                    env.get(name)
                } else if let Some((func_id, _n_params)) =
                    self.user_fns.get(name).copied()
                {
                    // Create a function value wrapping the user function
                    let func_ref =
                        self.module.declare_func_in_func(func_id, builder.func);
                    let fn_addr = builder.ins().func_addr(self.ptr_type, func_ref);
                    let null = builder.ins().iconst(self.ptr_type, 0);
                    let mk_fn = self.import_rt(builder, "knot_value_function");
                    let call = builder.ins().call(mk_fn, &[fn_addr, null]);
                    builder.inst_results(call)[0]
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
                // target should be a SourceRef
                if let ast::ExprKind::SourceRef(name) = &target.node {
                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();

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
                    let schema = self
                        .source_schemas
                        .get(name)
                        .cloned()
                        .unwrap_or_default();
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

            ast::ExprKind::At { .. } => {
                // Temporal queries not yet supported
                self.call_rt(builder, "knot_value_unit", &[])
            }
        }
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
                // show is identity for now (values are already printable)
                if compiled_args.len() == 1 {
                    compiled_args[0]
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
            let next_block = if is_last {
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

            if !is_last {
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

        self.call_rt(builder, "knot_value_function", &[fn_addr, env_val])
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
    )
}
