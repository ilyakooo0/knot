//! In-process JIT driver for knot's `compile : Text -> Maybe a` builtin.
//!
//! Compiles a knot source *string* to machine code in-process via Cranelift's
//! `JITModule` (no disk, no subprocess), resolving the generated code's
//! `knot_*` runtime imports against the host process's own runtime symbols
//! (see `codegen::Codegen::new_jit`). The program's body is compiled as
//! `knot_user_main(db) -> Value*`, which we call directly to force evaluation
//! and obtain the resulting `Value`.

use knot_compiler::codegen::{self, Codegen};
use knot_compiler::{desugar, infer, types};

/// The result of JIT-compiling a knot source string: the resolved entry
/// address plus the `Value*` produced by running it. The `Value` is owned by
/// the runtime arena and lives as long as the host process's runtime does.
pub struct CompiledValue {
    /// Raw pointer to the produced `knot_runtime::Value`.
    pub value: *mut knot_runtime::Value,
    /// The inferred type of the compiled program's body (for the caller's
    /// `Maybe a` unification check). `None` if it couldn't be determined.
    pub ty: Option<String>,
}

/// Error from JIT compilation: a human-readable diagnostic message.
#[derive(Debug)]
pub struct CompileError(pub String);

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for CompileError {}

/// JIT-compile `source` (a full knot program text) and run it in-process,
/// returning the forced `Value*`. Returns `Err` on any lex/parse/type/codegen
/// error. The caller is responsible for unifying the program's type against
/// the expected `a` and wrapping in `Maybe`.
///
/// `db` is the host program's open database handle (from `knot_db_open`) so a
/// compiled snippet that reads the host's persisted relations sees real data.
pub fn compile_and_run(
    source: &str,
    db: *mut std::ffi::c_void,
) -> Result<CompiledValue, CompileError> {
    // ── Lex ──
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    if lex_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return Err(CompileError(render_diags(&lex_diags, source)));
    }

    // ── Parse ──
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (mut program, parse_diags) = parser.parse_file_expr();
    if parse_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return Err(CompileError(render_diags(&parse_diags, source)));
    }

    // ── Desugar + type env + infer ──
    desugar::desugar(&mut program);
    let type_env = types::TypeEnv::from_program(&program);
    let (
        infer_diags,
        monad_info,
        type_info,
        _local_types,
        refine_targets,
        refined_types,
        from_json_targets,
        elem_pushdown_ok,
        show_unit_strings,
        sum_float_spans,
        relation_fields,
        with_fields,
        type_arg_spans,
        implicit_refs,
        implicit_dict_args,
        resolved_calls,
        todo_types,
        todo_bindings,
        trace_types,
        trace_bindings,
    ) = infer::check(&mut program);
    if infer_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return Err(CompileError(render_diags(&infer_diags, source)));
    }

    // The program's body type, for the caller's `Maybe a` check. `type_info`
    // maps top-level names to type strings; the file body is bound as `main`.
    let body_ty = type_info.get("main").cloned();

    // ── Codegen into a JITModule, on a grown stack ──
    // JITModule is not Send, so it can't cross stack::grow's thread boundary.
    // Compile + finalize inside grow and return only the Send-safe entry
    // address; the JITModule itself is intentionally leaked (compiled code may
    // be re-entered and holds references into it).
    let overrides = std::collections::HashMap::new();
    // Return the entry address as usize (Send) — *const u8 isn't Send and
    // can't cross stack::grow's thread boundary.
    let entry = knot_compiler::stack::grow(|| -> Result<usize, CompileError> {
        let cg = codegen::compile_with(
            Codegen::new_jit(),
            &program,
            &type_env,
            "<compile>",
            &monad_info,
            &refine_targets,
            &refined_types,
            &from_json_targets,
            &type_info,
            &elem_pushdown_ok,
            &show_unit_strings,
            &sum_float_spans,
            &relation_fields,
            &with_fields,
            &implicit_refs,
            &type_arg_spans,
            &implicit_dict_args,
            &resolved_calls,
            &todo_types,
            &todo_bindings,
            &trace_types,
            &trace_bindings,
            source,
            &overrides,
            false,
        )
        .map_err(|diags| CompileError(render_diags(&diags, source)))?;
        let (entry, module) = cg.finish_jit();
        std::mem::forget(module);
        Ok(entry as usize)
    })?;

    // ── Call knot_user_main(db) -> Value* ──
    let entry_fn: extern "C" fn(*mut std::ffi::c_void) -> *mut knot_runtime::Value =
        unsafe { std::mem::transmute(entry) };
    let value = entry_fn(db);

    Ok(CompiledValue { value, ty: body_ty })
}

/// Render a list of diagnostics into a single message string.
fn render_diags(diags: &[knot::diagnostic::Diagnostic], source: &str) -> String {
    diags
        .iter()
        .map(|d| d.render(source, "<compile>"))
        .collect::<Vec<_>>()
        .join("\n")
}
