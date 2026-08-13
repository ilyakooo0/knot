//! Knot compiler CLI.
//!
//! Usage: knot build <file.knot>

use knot_compiler::{codegen, desugar, infer, linker, lockfile, nonterm, stratify, types};

use std::collections::HashMap;
use std::path::PathBuf;
use std::process;

/// Prepend the host's `data` declarations for any ADTs a `base.compile`
/// expected-type descriptor references, so the JIT can compare constructor sets
/// structurally (nominal-by-structure) rather than by bare name. The output is
/// `data A = ... \n data B = ... \n <type>` — leading `data` decls, then the
/// type. Only ADTs the host actually declares (`ResolvedType::Adt`) are
/// emitted; scalar/record/alias names pass through untouched.
/// Render a single-variant ADT's `data` decl. Single-variant `data Name =
/// Ctor {..}` is stored in `aliases` as `ResolvedType::Record` (the ctor name
/// is dropped); `ctor_name` recovers it from `TypeEnv.constructors`.
fn single_variant_to_data_decl(
    name: &str,
    ctor_name: &str,
    fields: &[(String, types::ResolvedType)],
) -> String {
    let inner: Vec<String> = fields
        .iter()
        .map(|(fname, fty)| format!("{fname}: {}", types::resolved_to_source(fty)))
        .collect();
    format!("data {name} = {ctor_name} {{{}}}", inner.join(", "))
}

fn prepend_host_data_decls(
    descriptor: &str,
    aliases: &HashMap<String, types::ResolvedType>,
    constructors: &HashMap<String, Vec<(String, types::ResolvedType)>>,
) -> String {
    // Collect host ADT names appearing in the descriptor, TRANSITIVELY: an
    // included `data Task = Todo {pri: Priority}` decl references `Priority`,
    // so `Priority`'s decl must come along too, recursively — otherwise the
    // JIT sees a payload type it can't resolve as a host ADT and the ctor-set
    // check treats it as unconstrained (unsound).
    //
    // Multi-variant ADTs live in `aliases` as `ResolvedType::Adt`; SINGLE-
    // variant `data Name = Ctor {..}` live there as `ResolvedType::Record`
    // (the record bridge). Both must be emitted — a single-variant ADT's ctor
    // set is trivially equal on both sides, but its PAYLOAD fields (e.g.
    // `pri: Priority`) are exactly where a nested ctor-set difference hides.
    // Build a reverse map from a single-variant's record fields to its ctor
    // name so we can reconstruct `data Name = Ctor {..}`.
    let ctor_of_record: HashMap<String, String> = aliases
        .iter()
        .filter_map(|(name, rt)| {
            if let types::ResolvedType::Record(fields) = rt {
                // Find the constructor whose field names match this record's.
                constructors.iter().find_map(|(ctor, cfields)| {
                    let same = cfields.len() == fields.len()
                        && cfields
                            .iter()
                            .zip(fields.iter())
                            .all(|((cn, _), (fn_, _))| cn == fn_);
                    same.then(|| (name.clone(), ctor.clone()))
                })
            } else {
                None
            }
        })
        .collect();

    // Resolve a name to its `data` decl text, for either storage form.
    let decl_of = |name: &str| -> Option<String> {
        match aliases.get(name) {
            Some(types::ResolvedType::Adt(ctors)) => {
                Some(types::adt_to_data_decl(name, ctors))
            }
            Some(types::ResolvedType::Record(fields)) => {
                let ctor = ctor_of_record.get(name)?;
                Some(single_variant_to_data_decl(name, ctor, fields))
            }
            _ => None,
        }
    };

    let mut decls: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Worklist seeded with names directly in the descriptor; grow with names
    // referenced by each included decl.
    let mut worklist: Vec<String> = aliases
        .keys()
        .filter(|name| decl_of(name).is_some() && references_name(descriptor, name))
        .cloned()
        .collect();
    while let Some(name) = worklist.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let Some(decl) = decl_of(&name) else {
            continue;
        };
        // Enqueue any ADT this decl's payload fields reference.
        for other in aliases.keys() {
            if !seen.contains(other)
                && decl_of(other).is_some()
                && references_name(&decl, other)
            {
                worklist.push(other.clone());
            }
        }
        decls.push(decl);
    }
    if decls.is_empty() {
        descriptor.to_string()
    } else {
        let out = format!("{}\n{}", decls.join("\n"), descriptor);
        out
    }
}

/// Does `descriptor` mention `name` as a whole identifier (not as a substring
/// of a longer identifier)?
fn references_name(descriptor: &str, name: &str) -> bool {
    descriptor
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|tok| tok == name)
}

/// The compiler binary's own `knot_compile_rt_init`. Generated `main` calls
/// this at startup to register the JIT compile implementation; when the JIT
/// runs in-process (compile-time const-eval) it resolves the symbol via
/// `dlsym` against this binary, so the symbol must exist here. It lives in the
/// BINARY (not the lib) so test binaries that link knot-compiler alongside
/// knot-compile-rt don't get a duplicate `knot_compile_impl` from the rlib.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn knot_compile_rt_init() {
    knot_runtime::knot_register_compile_impl(knot_compile_impl as *mut std::ffi::c_void);
}

/// The compile implementation registered above. Mirrors knot-compile-rt's
/// `knot_compile_impl`: compile+run the snippet in-process, returning the
/// forced `Value*` (null on any error). Fills the inferred-type / relations
/// out-params so `base.compile`'s typed wrapper and the host-relation check
/// work; the error out-param is left null (the compile path surfaces failures
/// as a null value rather than a rendered message).
///
/// # Safety
///
/// Called from knot-runtime via the registered function pointer. `src_ptr`
/// must point to `src_len` valid bytes of knot source (need not be
/// NUL-terminated); `db` must be a live knot-runtime db handle. Any non-null
/// out-param pointer must be valid for writes of its documented payload.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn knot_compile_impl(
    src_ptr: *const u8,
    src_len: usize,
    db: *mut std::ffi::c_void,
    expected_ptr: *const u8,
    expected_len: usize,
    out_ty_ptr: *mut *mut u8,
    out_ty_len: *mut usize,
    out_rels_ptr: *mut *mut u8,
    out_rels_len: *mut usize,
    out_err_ptr: *mut *mut u8,
    out_err_len: *mut usize,
) -> *mut knot_runtime::Value {
    // Null out-params up front so a compile error leaves them well-defined.
    unsafe {
        for (p, l) in [
            (out_ty_ptr, out_ty_len),
            (out_rels_ptr, out_rels_len),
            (out_err_ptr, out_err_len),
        ] {
            if !p.is_null() {
                *p = std::ptr::null_mut();
            }
            if !l.is_null() {
                *l = 0;
            }
        }
    }

    let src_bytes = unsafe { std::slice::from_raw_parts(src_ptr, src_len) };
    let Ok(source) = std::str::from_utf8(src_bytes) else {
        return std::ptr::null_mut();
    };
    // The host's expected type as a source-annotation string; empty = unpinned
    // (no subsumption check).
    let expected: Option<&str> = if expected_ptr.is_null() || expected_len == 0 {
        None
    } else {
        let b = unsafe { std::slice::from_raw_parts(expected_ptr, expected_len) };
        std::str::from_utf8(b).ok()
    };

    // Hand a string back as a malloc'd buffer (C ABI — the runtime frees it).
    unsafe fn write_out(s: &str, ptr: *mut *mut u8, len: *mut usize) {
        if ptr.is_null() || len.is_null() {
            return;
        }
        let buf = unsafe { libc::malloc(s.len().max(1)) as *mut u8 };
        if buf.is_null() {
            return;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(s.as_ptr(), buf, s.len());
            *ptr = buf;
            *len = s.len();
        }
    }

    match codegen::jit_compile_typed(source, expected) {
        Some(compiled) => {
            if let Some(ty) = &compiled.body_ty {
                unsafe { write_out(ty, out_ty_ptr, out_ty_len) };
            }
            let csv = compiled.relations.join(",");
            unsafe { write_out(&csv, out_rels_ptr, out_rels_len) };
            let entry_fn: extern "C" fn(*mut std::ffi::c_void) -> *mut knot_runtime::Value =
                unsafe { std::mem::transmute(compiled.entry) };
            entry_fn(db)
        }
        None => std::ptr::null_mut(),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    match args[1].as_str() {
        // Hidden dev hook: evaluate a pure snippet to Bool via the in-process
        // JIT (exercises the compile-time const-eval path directly).
        "__const-eval" => {
            let src = args.get(2).map(|s| s.as_str()).unwrap_or("true");
            match codegen::eval_pure_to_bool(src) {
                Some(b) => println!("{b}"),
                None => println!("<none>"),
            }
            process::exit(0);
        }
        "build" => {
            if args.len() < 3 {
                eprintln!("Error: missing source file");
                eprintln!("Usage: knot build <file.knot> [-o <path>] [--debug] [--name=value ...]");
                process::exit(1);
            }
            // Parse -o/--output and compile-time overrides from remaining args
            let mut overrides = HashMap::new();
            let mut output: Option<PathBuf> = None;
            let mut debug = false;
            let mut i = 3;
            while i < args.len() {
                if args[i] == "-o" {
                    if i + 1 >= args.len() {
                        eprintln!("Error: missing value for -o");
                        process::exit(1);
                    }
                    // Don't swallow a flag-like token as the output path —
                    // mirrors the `--name value` guard below. Use `-o=<value>`
                    // for paths that start with `-`.
                    if !args[i + 1].is_empty() && args[i + 1].starts_with('-') {
                        eprintln!(
                            "Error: missing value for -o (for values starting with '-', use -o=<value>)"
                        );
                        process::exit(1);
                    }
                    output = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else if let Some(val) = args[i].strip_prefix("-o=") {
                    output = Some(PathBuf::from(val));
                    i += 1;
                } else if let Some(rest) = args[i].strip_prefix("--") {
                    if rest == "debug" {
                        debug = true;
                        i += 1;
                        continue;
                    }
                    if rest == "output" {
                        if i + 1 >= args.len() {
                            eprintln!("Error: missing value for --output");
                            process::exit(1);
                        }
                        if !args[i + 1].is_empty() && args[i + 1].starts_with('-') {
                            eprintln!(
                                "Error: missing value for --output (for values starting with '-', use --output=<value>)"
                            );
                            process::exit(1);
                        }
                        output = Some(PathBuf::from(&args[i + 1]));
                        i += 2;
                        continue;
                    }
                    if let Some(val) = rest.strip_prefix("output=") {
                        output = Some(PathBuf::from(val));
                        i += 1;
                        continue;
                    }
                    if let Some((name, val)) = rest.split_once('=') {
                        overrides.insert(name.to_string(), val.to_string());
                    } else if i + 1 < args.len() && !args[i + 1].starts_with('-') {
                        // Space-separated form: the next token is the value.
                        // Any token starting with '-' (e.g. `-o`, another
                        // `--flag`, or a negative number) is NOT consumed as
                        // the value — use the `--name=value` form for those.
                        overrides.insert(rest.to_string(), args[i + 1].clone());
                        i += 1;
                    } else {
                        eprintln!(
                            "Error: missing value for --{} (for values starting with '-', use --{}=<value>)",
                            rest, rest
                        );
                        process::exit(1);
                    }
                    i += 1;
                } else {
                    eprintln!("Error: unexpected argument '{}'", args[i]);
                    process::exit(1);
                }
            }

            // Warn when a compile-time constant name collides with a reserved
            // CLI flag name.  `--output` (and `--output=value`) is consumed by
            // the build subcommand as the output path, so a constant named
            // `output` can never be overridden via `--output=…` at build time.
            // However `./app --output=x` *does* override the constant at run
            // time, which is surprising — emit a warning so users notice.
            const RESERVED_FLAGS: &[&str] = &["output"];
            for name in overrides.keys() {
                if RESERVED_FLAGS.contains(&name.as_str()) {
                    eprintln!(
                        "Warning: compile-time constant '{}' has the same name as a reserved CLI flag; \
                         it cannot be overridden at build time via --{}=… (the flag is used for the output path). \
                         At run time the flag will override the constant instead.",
                        name, name
                    );
                }
            }

            cmd_build(&args[2], output.as_deref(), &overrides, debug);
        }
        "fmt" => {
            cmd_fmt(&args[2..]);
        }
        "--help" | "-h" | "help" => print_usage(),
        other => {
            eprintln!("Unknown command: {}", other);
            print_usage();
            process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Knot compiler");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  knot build <file.knot> [-o <path>] [--debug] [--name=value ...]  Compile with optional output path and constant overrides");
    eprintln!("      --debug   Print each expression that generates SQL with the SQL it pushed down");
    eprintln!("  knot fmt [--check] [--stdout] <file.knot>              Format a source file in place ('-' reads stdin, writes stdout)");
    eprintln!("  knot help                                              Show this help message");
}

fn cmd_fmt(args: &[String]) {
    let mut check = false;
    let mut to_stdout = false;
    let mut paths: Vec<&str> = Vec::new();
    for a in args {
        match a.as_str() {
            "--check" => check = true,
            "--stdout" => to_stdout = true,
            // Conventional stdin marker: read source from stdin and write
            // the formatted output to stdout (or just diff with --check).
            "-" => paths.push("-"),
            other if other.starts_with("--") => {
                eprintln!("Error: unknown fmt flag '{}'", other);
                eprintln!("Usage: knot fmt [--check] [--stdout] <file.knot>... (use '-' for stdin)");
                process::exit(2);
            }
            other => paths.push(other),
        }
    }
    if paths.is_empty() {
        eprintln!("Error: missing source file");
        eprintln!("Usage: knot fmt [--check] [--stdout] <file.knot>... (use '-' for stdin)");
        process::exit(2);
    }
    // Writing multiple files to stdout would concatenate them without any
    // delimiter, producing an unparseable blob — reject it instead.
    if to_stdout && paths.len() > 1 {
        eprintln!("error: --stdout cannot be used with multiple files");
        process::exit(1);
    }

    let mut any_diff = false;
    for path_str in &paths {
        let from_stdin = *path_str == "-";
        let source_path = PathBuf::from(path_str);
        let source = if from_stdin {
            use std::io::Read;
            let mut buf = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut buf) {
                eprintln!("Error reading stdin: {}", e);
                process::exit(1);
            }
            buf
        } else {
            match std::fs::read_to_string(&source_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error reading {}: {}", source_path.display(), e);
                    process::exit(1);
                }
            }
        };

        let lexer = knot::lexer::Lexer::new(&source);
        let (tokens, lex_diags) = lexer.tokenize();
        let filename = if from_stdin {
            "<stdin>".to_string()
        } else {
            source_path.display().to_string()
        };
        let lex_errs: Vec<_> = lex_diags
            .iter()
            .filter(|d| d.severity == knot::diagnostic::Severity::Error)
            .collect();
        if !lex_errs.is_empty() {
            for d in &lex_errs {
                eprintln!("{}", d.render(&source, &filename));
            }
            process::exit(1);
        }

        let parser = knot::parser::Parser::new(source.clone(), tokens);
        let (expr, parse_diags) = parser.parse_file_expr();
        let parse_errs: Vec<_> = parse_diags
            .iter()
            .filter(|d| d.severity == knot::diagnostic::Severity::Error)
            .collect();
        if !parse_errs.is_empty() {
            eprintln!("Cannot format {}: parse errors", filename);
            for d in &parse_errs {
                eprintln!("{}", d.render(&source, &filename));
            }
            process::exit(1);
        }

        let formatted = knot::format::format_expr(&source, &expr);

        if check {
            if formatted != source {
                eprintln!("{}: not formatted", filename);
                any_diff = true;
            }
        } else if to_stdout || from_stdin {
            // stdin input has no file to rewrite — always format to stdout.
            print!("{}", formatted);
        } else if formatted != source {
            if let Err(e) = std::fs::write(&source_path, &formatted) {
                eprintln!("Error writing {}: {}", source_path.display(), e);
                process::exit(1);
            }
            eprintln!("Formatted: {}", source_path.display());
        }
    }

    if check && any_diff {
        process::exit(1);
    }
}

/// Compare two paths for filesystem identity. Nonexistent paths are
/// normalized against their (canonicalized) parent directory so that
/// e.g. `./prog` and `prog` compare equal even before `prog`'s output
/// twin exists.
fn same_file_path(a: &std::path::Path, b: &std::path::Path) -> bool {
    fn normalize(p: &std::path::Path) -> PathBuf {
        if let Ok(c) = p.canonicalize() {
            return c;
        }
        let parent = p
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .map(|d| d.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let parent = parent.canonicalize().unwrap_or(parent);
        match p.file_name() {
            Some(name) => parent.join(name),
            None => parent,
        }
    }
    normalize(a) == normalize(b)
}

fn cmd_build(source_file: &str, output_override: Option<&std::path::Path>, overrides: &HashMap<String, String>, debug: bool) {
    let source_path = PathBuf::from(source_file);

    // Read source
    let source = std::fs::read_to_string(&source_path).unwrap_or_else(|e| {
        eprintln!("Error reading {}: {}", source_path.display(), e);
        process::exit(1);
    });

    // Determine the output path up front so we can refuse to overwrite the
    // source file (e.g. `knot build prog` on an extensionless source would
    // otherwise silently replace `prog` with the linked binary).
    let output_path: PathBuf = match output_override {
        Some(p) => {
            if same_file_path(p, &source_path) {
                eprintln!(
                    "Error: output path '{}' is the same as the source file; pass a different path to -o",
                    p.display()
                );
                process::exit(1);
            }
            p.to_path_buf()
        }
        None => {
            let default = source_path.with_extension("");
            if same_file_path(&default, &source_path) {
                // Extensionless source: emit `<name>.out` instead of clobbering it.
                source_path.with_extension("out")
            } else {
                default
            }
        }
    };

    // Pick an intermediate object path that collides with neither the source
    // (e.g. a source named `foo.o`) nor the output executable.
    let obj_path: PathBuf = {
        let mut candidate = source_path.with_extension("o");
        let mut n = 0u32;
        while same_file_path(&candidate, &source_path) || same_file_path(&candidate, &output_path) {
            n += 1;
            candidate = source_path.with_extension(format!("knot{}.o", n));
        }
        candidate
    };

    // Lex
    let lexer = knot::lexer::Lexer::new(&source);
    let (tokens, lex_diags) = lexer.tokenize();
    let filename = source_path.display().to_string();
    if !lex_diags.is_empty() {
        for diag in &lex_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if lex_diags.iter().any(|d| d.severity == knot::diagnostic::Severity::Error) {
            process::exit(1);
        }
    }

    // Parse — a `.knot` file is a single expression.
    let parser = knot::parser::Parser::new(source.clone(), tokens);
    let (mut program, parse_diags) = parser.parse_file_expr();
    let has_errors = parse_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error);
    if has_errors {
        for diag in &parse_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // The `base` record is bound globally by infer (`bind_base_record`) and
    // codegen (`define_base_record`) — no `with` wrapper is injected.

    // Desugar monadic do blocks into trait method calls
    desugar::desugar(&mut program);

    // Detect recursive type aliases before resolution — a cyclic alias
    // (`type A = {x: A}`, mutual cycles) can never be resolved, so report
    // a diagnostic instead of letting resolution chase the cycle.
    let cycle_diags = types::check_alias_cycles(&program);
    if !cycle_diags.is_empty() {
        for diag in &cycle_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Reject persisted fields whose names collide with the runtime's internal
    // SQLite columns (`_id`, `_tag`, ...) — they used to compile clean and
    // abort at table init with "duplicate column name".
    let reserved_diags = types::check_reserved_field_names(&program);
    if !reserved_diags.is_empty() {
        for diag in &reserved_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Reject `with`-chain shadowing: a field name bound by two layers of the
    // top-level `with {…} (with {…} …)` chain silently shadows the outer one.
    let with_shadow_diags = types::check_with_chain_shadowing(&program);
    if !with_shadow_diags.is_empty() {
        for diag in &with_shadow_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Resolve types
    let type_env = types::TypeEnv::from_program(&program);

    // Type inference
    let infer::CheckOutput { diagnostics: infer_diags, monad_info, type_info, local_type_info: _local_types, refine_targets, refined_type_info: refined_types, from_json_targets, elem_pushdown_ok, show_unit_strings, sum_float_spans, relation_field_spans: relation_fields, with_fields, type_arg_spans, implicit_refs, implicit_dict_args, fold_dict_args, collect_refs, resolved_calls, todo_types, todo_bindings, trace_types, trace_bindings, compile_expected_types, file_body_type: _file_body_type, refined_field_preds } = infer::check(&mut program);
    // The expected-type descriptor of each `compile` call is knot source-type
    // syntax (from `display_ty_clean`). For ADTs the bare NAME alone is
    // insufficient — the JIT must compare constructor sets, and `Priority` says
    // nothing about `{Low|High}` vs `{Low|High|Medium}`. So when the expected
    // type references a host ADT, prepend the host's `data` declaration(s) for
    // those ADTs (existing knot syntax, not an invented grammar). The JIT
    // splits the leading `data` decls from the trailing type, registers them,
    // and compares ctor sets structurally.
    let compile_expected_types: infer::CompileExpectedTypes = compile_expected_types
        .into_iter()
        .map(|(span, desc)| (span, prepend_host_data_decls(&desc, &type_env.aliases, &type_env.constructors)))
        .collect();
    if !infer_diags.is_empty() {
        for diag in &infer_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if infer_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error)
        {
            process::exit(1);
        }
    }

    // Warn about every `todo` hole left in the codebase: it compiles and runs
    // (aborting with a report only if reached), but is a debug placeholder, not
    // a real implementation. One warning per hole, with a caret at the site.
    // Unlike errors this does not stop the build.
    if !todo_types.is_empty() {
        let mut spans: Vec<_> = todo_types.keys().copied().collect();
        spans.sort_by_key(|s| s.start);
        for span in spans {
            let diag = knot::diagnostic::Diagnostic::warning(
                "`todo` hole used — this code path is a debug placeholder, not implemented",
            )
            .label(span, "debug placeholder")
            .note("replace `base.todo` with an implementation before shipping");
            eprintln!("{}", diag.render(&source, &filename));
        }
    }

    // Warn about every `trace` probe left in the codebase: it compiles, runs,
    // and returns its value unchanged, but prints a report every time it fires —
    // a debugging aid, not shipping behaviour. One warning per probe, with a
    // caret at the site. Unlike errors this does not stop the build.
    if !trace_types.is_empty() {
        let mut spans: Vec<_> = trace_types.keys().copied().collect();
        spans.sort_by_key(|s| s.start);
        for span in spans {
            let diag = knot::diagnostic::Diagnostic::warning(
                "`trace` probe left in — prints a report at runtime every time it fires",
            )
            .label(span, "debug probe")
            .note("remove `base.trace` before shipping");
            eprintln!("{}", diag.render(&source, &filename));
        }
    }

    // Stratification check for recursive derived relations
    let strat_diags = stratify::check(&program);
    if !strat_diags.is_empty() {
        for diag in &strat_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if strat_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error)
        {
            process::exit(1);
        }
    }

    // Non-termination check: reject definitions that provably recurse forever
    // (unguarded self-calls with no base case).
    let nonterm_diags = nonterm::check(&program);
    if !nonterm_diags.is_empty() {
        for diag in &nonterm_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if nonterm_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error)
        {
            process::exit(1);
        }
    }

    // Check schema lockfile
    let lock_diags = lockfile::check(&source_path, &program, &type_env);
    if !lock_diags.is_empty() {
        for diag in &lock_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if lock_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error)
        {
            process::exit(1);
        }
    }

    // Code generation
    let obj_bytes = match codegen::compile(&program, &type_env, source_file, &monad_info, &refine_targets, &refined_types, &refined_field_preds, &from_json_targets, &type_info, &elem_pushdown_ok, &show_unit_strings, &sum_float_spans, &compile_expected_types, &relation_fields, &with_fields, &implicit_refs, &type_arg_spans, &implicit_dict_args, &fold_dict_args, &collect_refs, &resolved_calls, &todo_types, &todo_bindings, &trace_types, &trace_bindings, &source, overrides, debug) {
        Ok(bytes) => bytes,
        Err(diags) => {
            for diag in &diags {
                eprintln!("{}", diag.render(&source, &filename));
            }
            process::exit(1);
        }
    };

    // Write object file (path chosen above so it never clobbers the source)
    std::fs::write(&obj_path, &obj_bytes).unwrap_or_else(|e| {
        eprintln!("Error writing object file: {}", e);
        process::exit(1);
    });

    // Find runtime
    let runtime_path = find_runtime();
    // The JIT compile-runtime archive, always linked so `base.compile` works.
    let compile_rt_path = find_compile_rt();

    // Link (output path computed and collision-checked above)
    if let Err(e) = linker::link(&obj_path, &runtime_path, &compile_rt_path, &output_path) {
        eprintln!("Link error: {}", e);
        let _ = std::fs::remove_file(&obj_path);
        if is_extracted_temp_runtime(&runtime_path) {
            let _ = std::fs::remove_file(&runtime_path);
        }
        if is_extracted_temp_compile_rt(&compile_rt_path) {
            let _ = std::fs::remove_file(&compile_rt_path);
        }
        process::exit(1);
    }

    // Clean up
    let _ = std::fs::remove_file(&obj_path);
    // Remove temp runtime if it was extracted from embedded bytes
    if is_extracted_temp_runtime(&runtime_path) {
        let _ = std::fs::remove_file(&runtime_path);
    }
    if is_extracted_temp_compile_rt(&compile_rt_path) {
        let _ = std::fs::remove_file(&compile_rt_path);
    }

    // Update schema lockfile from the (prelude-wrapped, desugared) program.
    if let Err(e) = lockfile::update(&source_path, &source, &program) {
        eprintln!("Warning: {}", e);
    }

    eprintln!("Compiled: {}", output_path.display());
}

/// Runtime library embedded at build time. The build.rs copies
/// libknot_runtime.a into OUT_DIR; we include those bytes so the
/// compiler binary is fully self-contained after `cargo install`.
#[cfg(has_embedded_runtime)]
const EMBEDDED_RUNTIME: Option<&[u8]> =
    Some(include_bytes!(concat!(env!("OUT_DIR"), "/libknot_runtime.a")));
#[cfg(not(has_embedded_runtime))]
const EMBEDDED_RUNTIME: Option<&[u8]> = None;

/// JIT compile-runtime archive (knot-compile-rt), embedded at build time and
/// always linked into compiled programs so `base.compile` works in-process.
#[cfg(has_embedded_compile_rt)]
const EMBEDDED_COMPILE_RT: Option<&[u8]> =
    Some(include_bytes!(concat!(env!("OUT_DIR"), "/libknot_compile_rt.a")));
#[cfg(not(has_embedded_compile_rt))]
const EMBEDDED_COMPILE_RT: Option<&[u8]> = None;

/// True if `p` is a runtime archive that `find_runtime` extracted into the
/// temp directory for this process (and which is therefore ours to delete).
fn is_extracted_temp_runtime(p: &std::path::Path) -> bool {
    let tmp_dir = std::env::temp_dir();
    p.parent() == Some(tmp_dir.as_path())
        && p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| {
                n.starts_with(&format!("libknot_runtime_{}_", std::process::id()))
                    && n.ends_with(".a")
            })
}

/// The blake3 content hash of the runtime archive this compiler embeds (set by
/// build.rs when an archive was embedded). Comparing a candidate archive's hash
/// against this is the freshness check: a match means byte-identical bits, so
/// the archive is exactly what this compiler would link — independent of file
/// mtimes, clock skew, or which build produced it.
const EMBEDDED_RUNTIME_HASH: Option<&str> = option_env!("KNOT_EMBEDDED_RUNTIME_HASH");

/// Hash an archive's bytes. `None` on read error.
fn hash_archive(p: &std::path::Path) -> Option<String> {
    std::fs::read(p)
        .ok()
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
}

/// Rebuild the runtime staticlib in place when the compiler detects its
/// exe-dir archive is stale. Locates the workspace by walking up from the
/// executable (`<ws>/target/<profile>/knot`), then runs
/// `cargo build -p knot-runtime` so the rebuilt archive lands where this
/// lookup found it. Returns the refreshed archive's path on success, `None`
/// if the workspace can't be located or the build fails (caller falls back
/// to the embedded runtime).
fn rebuild_runtime_at_exe(stale: &std::path::Path) -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    // exe = <ws>/target/<profile>/knot  →  ws = exe ../../..
    let ws = exe.parent()?.parent()?.parent()?;
    if !ws.join("crates/knot-runtime/Cargo.toml").exists() {
        return None;
    }
    let profile = exe
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("debug")
        .to_string();
    let mut cmd = std::process::Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    cmd.arg("build")
        .arg("-p")
        .arg("knot-runtime")
        .current_dir(ws)
        .stdout(std::process::Stdio::null());
    if profile == "release" {
        cmd.arg("--release");
    }
    eprintln!("knot: refreshing stale runtime archive (cargo build -p knot-runtime)...");
    if cmd.status().ok()?.success() && !is_runtime_stale(stale) {
        Some(stale.to_path_buf())
    } else {
        None
    }
}

/// True if `lib` is stale — its content differs from the runtime archive this
/// compiler embeds. Falls back to `false` (assume fresh) when there's no
/// embedded runtime to compare against or the file can't be read, so we never
/// block builds spuriously. Replaces the old mtime walk: content comparison is
/// immune to clock skew, `touch`, and VCS checkouts that reset mtimes.
fn is_runtime_stale(lib: &std::path::Path) -> bool {
    match EMBEDDED_RUNTIME_HASH {
        Some(expected) => hash_archive(lib).map(|h| h != expected).unwrap_or(false),
        None => false,
    }
}

fn find_runtime() -> PathBuf {
    // 1. Environment variable override
    if let Ok(path) = std::env::var("KNOT_RUNTIME_LIB") {
        let p = PathBuf::from(&path);
        if p.exists() {
            // Warn if the archive's content differs from the embedded runtime,
            // but still use it — the user set the override explicitly.
            if is_runtime_stale(&p) {
                eprintln!(
                    "Warning: KNOT_RUNTIME_LIB archive '{}' content differs from \
                     the runtime this compiler embeds — rebuild knot-runtime to \
                     pick up source changes",
                    path
                );
            }
            return p;
        }
        // The user explicitly set the override; a typo (or a stale path)
        // should not silently fall through to the embedded runtime, which
        // would produce binaries with subtly different behavior.
        eprintln!(
            "Error: KNOT_RUNTIME_LIB is set to '{}' but the file does not exist",
            path
        );
        process::exit(1);
    }

    // 2. Same directory as the compiler executable
    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_dir) = exe.parent() {
            let candidate = exe_dir.join("libknot_runtime.a");
            if candidate.exists() {
                // Freshness check: if the archive's content differs from the
                // runtime this compiler embeds (e.g. only knot-compiler was
                // rebuilt, or knot-runtime sources changed), the archive is
                // stale. Rather than just warn, try to rebuild it in place —
                // the compiler knows its own workspace (exe at
                // <ws>/target/<profile>/knot), so it can refresh the archive
                // the moment it detects staleness. This removes the recurring
                // "run cargo build -p knot-runtime" manual step: the first
                // compile after a runtime change rebuilds, and every compile
                // after that is fresh.
                if is_runtime_stale(&candidate) {
                    if let Some(fresh) = rebuild_runtime_at_exe(&candidate) {
                        return fresh;
                    }
                    // Couldn't rebuild (no workspace found, or build failed) —
                    // fall back to the embedded runtime with the warning.
                    eprintln!(
                        "Warning: {} content differs from the embedded runtime \
                         — skipping stale archive, falling back to embedded \
                         runtime. Run `cargo build -p knot-runtime` to refresh.",
                        candidate.display()
                    );
                } else {
                    return candidate;
                }
            }
        }

    // 3. Extract embedded runtime to a temp file. The name includes the
    //    pid plus a nanosecond nonce and attempt counter, and the file is
    //    opened with `create_new` (O_CREAT|O_EXCL — fails instead of
    //    following an attacker-planted symlink or reusing an existing
    //    file) and owner-only permissions on unix; collisions retry with
    //    a fresh name.
    if let Some(bytes) = EMBEDDED_RUNTIME {
        use std::io::Write;
        for attempt in 0..32u32 {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let tmp = std::env::temp_dir().join(format!(
                "libknot_runtime_{}_{}_{:08x}.a",
                std::process::id(),
                attempt,
                nonce
            ));
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            match opts.open(&tmp) {
                Ok(mut f) => {
                    if f.write_all(bytes).is_ok() {
                        return tmp;
                    }
                    let _ = std::fs::remove_file(&tmp);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    continue; // name collision — retry with a fresh nonce
                }
                Err(_) => break,
            }
        }
    }

    eprintln!("Error: cannot find libknot_runtime.a");
    eprintln!("Ensure knot-runtime is built (cargo build -p knot-runtime)");
    eprintln!("Or set KNOT_RUNTIME_LIB=/path/to/libknot_runtime.a");
    process::exit(1);
}

/// True if `p` is a compile-rt archive `find_compile_rt` extracted into the
/// temp dir (and which is therefore ours to delete).
fn is_extracted_temp_compile_rt(p: &std::path::Path) -> bool {
    let tmp_dir = std::env::temp_dir();
    p.parent() == Some(tmp_dir.as_path())
        && p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| {
                n.starts_with(&format!("libknot_compile_rt_{}_", std::process::id()))
                    && n.ends_with(".a")
            })
}

/// Locate the knot-compile-rt archive (JIT compile-runtime), always linked
/// into compiled programs so `base.compile` works. Mirrors `find_runtime`:
/// KNOT_COMPILE_RT_LIB override → beside the compiler exe → extract embedded.
fn find_compile_rt() -> PathBuf {
    if let Ok(path) = std::env::var("KNOT_COMPILE_RT_LIB") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return p;
        }
        eprintln!(
            "Error: KNOT_COMPILE_RT_LIB is set to '{}' but the file does not exist",
            path
        );
        process::exit(1);
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_dir) = exe.parent()
    {
        let candidate = exe_dir.join("libknot_compile_rt.a");
        if candidate.exists() {
            return candidate;
        }
    }
    if let Some(bytes) = EMBEDDED_COMPILE_RT {
        use std::io::Write;
        for attempt in 0..32u32 {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let tmp = std::env::temp_dir().join(format!(
                "libknot_compile_rt_{}_{}_{:08x}.a",
                std::process::id(),
                attempt,
                nonce
            ));
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            match opts.open(&tmp) {
                Ok(mut f) => {
                    if f.write_all(bytes).is_ok() {
                        return tmp;
                    }
                    let _ = std::fs::remove_file(&tmp);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => break,
            }
        }
    }
    eprintln!("Error: cannot find libknot_compile_rt.a");
    eprintln!("Ensure knot-compile-rt is built (cargo build -p knot-compile-rt)");
    eprintln!("Or set KNOT_COMPILE_RT_LIB=/path/to/libknot_compile_rt.a");
    process::exit(1);
}
