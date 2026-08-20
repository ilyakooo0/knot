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
/// address plus the value produced by running it. The produced value is an
/// opaque pointer into the host runtime's arena (a `Value*` in knot-runtime's
/// terms); knot-jit deliberately treats it as `*mut c_void` so it has no
/// dependency on knot-runtime (which would create a Cargo cycle — knot-runtime
/// depends on knot-jit for the `compile` builtin).
pub struct CompiledValue {
    /// Opaque pointer to the produced value (`knot_runtime::Value*`).
    pub value: *mut std::ffi::c_void,
    /// The inferred type of the compiled program's body (for the caller's
    /// `Maybe a` unification check). `None` if it couldn't be determined.
    pub ty: Option<String>,
    /// Relations the snippet declares (`*name : T` in a `with` block), for the
    /// caller's host-relation check: a snippet may only use relations the host
    /// program defined. The JIT never runs the generated `main`'s source-init
    /// (its entry is `knot_user_main`), so these are surfaced statically from
    /// the snippet's type env rather than observed at runtime.
    pub relations: Vec<String>,
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
/// `expected_src`, when `Some`, is the host's expected type for the snippet as
/// a knot source type-annotation string (e.g. `Int 1 -> Maybe (Int 1)`,
/// `Priority`). It is threaded into inference so the snippet's body type is
/// checked against it on REAL types (`infer::ty_subsumes`); a `false` verdict
/// makes this return `Err` (the runtime then yields `Nothing`).
pub fn compile_and_run(
    source: &str,
    db: *mut std::ffi::c_void,
    expected_src: Option<&str>,
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
    // Run inference, subsuming the body type against the host's expected type
    // (if any) on real types inside `check` — the verdict is read after.
    let infer::CheckOutput {
        diagnostics: infer_diags,
        monad_info,
        type_info,
        local_type_info: _local_types,
        refine_targets,
        refined_type_info: refined_types,
        from_json_targets,
        string_lit_bytes,
        list_lit_lists,
        list_lit_vecs,
        elem_pushdown_ok,
        show_unit_strings,
        sum_float_spans,
        relation_field_spans: relation_fields,
        with_fields,
        type_arg_spans,
        implicit_refs,
        implicit_dict_args,
        fold_dict_args,
        collect_refs,
        resolved_calls,
        todo_types,
        todo_bindings,
        trace_types,
        trace_bindings,
        compile_expected_types: _compile_expected_types,
        file_body_type,
        refined_field_preds,
    } = match expected_src {
        Some(exp) => infer::check_with_expected(&mut program, exp),
        None => infer::check(&mut program),
    };
    if infer_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return Err(CompileError(render_diags(&infer_diags, source)));
    }

    // Option A: the real-type subsumption verdict from `check`. A `false` means
    // the snippet's body type isn't usable where the host's expected type is
    // wanted — reject (the runtime yields `Nothing`). `None` (no expected type
    // / unparseable / no body type) conservatively accepts, matching the prior
    // "don't break working programs over the checker" contract.
    if infer::take_subsumption_verdict() == Some(false) {
        return Err(CompileError(format!(
            "compiled snippet's type is not usable where the expected type `{expected_src:?}` is wanted"
        )));
    }

    // The program's body type, surfaced for the caller's `Maybe a` record (the
    // file body is inferred as the root `main`, not a named decl, so
    // `type_info` never holds it). Passed through verbatim — no descriptor
    // enrichment: the expected type is a knot source annotation now, and the
    // subsumption already ran on real `Ty`s inside `check`.
    let body_ty = file_body_type;

    // Relations the snippet declares (for the host-relation check).
    let relations: Vec<String> = type_env.source_schemas.keys().cloned().collect();

    // ── Codegen into a JITModule, on a grown stack ──
    // JITModule is not Send, so it can't cross stack::grow's thread boundary.
    // Compile + finalize inside grow and return only the Send-safe entry
    // address; the JITModule itself is intentionally leaked (compiled code may
    // be re-entered and holds references into it).
    let overrides = std::collections::HashMap::new();
    // A JIT'd snippet has no `base.compile` expected-type context of its own.
    let compile_expected_types = knot_compiler::infer::CompileExpectedTypes::new();
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
            &refined_field_preds,
            &from_json_targets,
            &string_lit_bytes,
            &list_lit_lists,
            &list_lit_vecs,
            &type_info,
            &elem_pushdown_ok,
            &show_unit_strings,
            &sum_float_spans,
            &compile_expected_types,
            &relation_fields,
            &with_fields,
            &implicit_refs,
            &type_arg_spans,
            &implicit_dict_args,
            &fold_dict_args,
            &collect_refs,
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
    let entry_fn: extern "C" fn(*mut std::ffi::c_void) -> *mut std::ffi::c_void =
        unsafe { std::mem::transmute(entry) };
    let value = entry_fn(db);

    Ok(CompiledValue {
        value,
        ty: body_ty,
        relations,
    })
}

/// Render a list of diagnostics into a single message string.
fn render_diags(diags: &[knot::diagnostic::Diagnostic], source: &str) -> String {
    diags
        .iter()
        .map(|d| d.render(source, "<compile>"))
        .collect::<Vec<_>>()
        .join("\n")
}
