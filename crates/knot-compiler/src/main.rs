//! Knot compiler CLI.
//!
//! Usage: knotc build <file.knot>

use knot_compiler::{base, codegen, desugar, effects, infer, linker, lockfile, modules, stratify, types};

use std::path::PathBuf;
use std::process;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    match args[1].as_str() {
        "build" => {
            if args.len() < 3 {
                eprintln!("Error: missing source file");
                eprintln!("Usage: knotc build <file.knot>");
                process::exit(1);
            }
            cmd_build(&args[2]);
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
    eprintln!("  knotc build <file.knot>   Compile a Knot source file to an executable");
    eprintln!("  knotc help                Show this help message");
}

fn cmd_build(source_file: &str) {
    let source_path = PathBuf::from(source_file);

    // Read source
    let source = std::fs::read_to_string(&source_path).unwrap_or_else(|e| {
        eprintln!("Error reading {}: {}", source_path.display(), e);
        process::exit(1);
    });

    // Lex
    let lexer = knot::lexer::Lexer::new(&source);
    let (tokens, lex_diags) = lexer.tokenize();
    let filename = source_path.display().to_string();
    if !lex_diags.is_empty() {
        for diag in &lex_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
    }

    // Parse
    let parser = knot::parser::Parser::new(source.clone(), tokens);
    let (mut module, parse_diags) = parser.parse_module();
    let has_errors = parse_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error);
    if has_errors {
        for diag in &parse_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Save original decls before mutations (imports/prelude/desugar add decls
    // with spans referencing other source texts — lockfile needs original spans).
    let original_decls = module.decls.clone();

    // Resolve imports — load, parse, and merge imported modules
    if let Err(diags) = modules::resolve_imports(&mut module, &source_path) {
        for diag in &diags {
            eprintln!("{}", diag);
        }
        process::exit(1);
    }

    // Inject built-in trait declarations and primitive impls
    base::inject_prelude(&mut module);

    // Desugar monadic do blocks into trait method calls
    desugar::desugar(&mut module);

    // Resolve types
    let type_env = types::TypeEnv::from_module(&module);

    // Type inference
    let (infer_diags, monad_info, _type_info, _local_types, refine_targets, refined_types) = infer::check(&module);
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

    // Effect inference
    let effect_diags = effects::check(&module);
    if !effect_diags.is_empty() {
        for diag in &effect_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        if effect_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error)
        {
            process::exit(1);
        }
    }

    // Stratification check for recursive derived relations
    let strat_diags = stratify::check(&module);
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

    // Check schema lockfile
    let lock_diags = lockfile::check(&source_path, &module, &type_env);
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
    let obj_bytes = match codegen::compile(&module, &type_env, source_file, &monad_info, &refine_targets, &refined_types) {
        Ok(bytes) => bytes,
        Err(diags) => {
            for diag in &diags {
                eprintln!("{}", diag.render(&source, &filename));
            }
            process::exit(1);
        }
    };

    // Write object file
    let obj_path = source_path.with_extension("o");
    std::fs::write(&obj_path, &obj_bytes).unwrap_or_else(|e| {
        eprintln!("Error writing object file: {}", e);
        process::exit(1);
    });

    // Find runtime
    let runtime_path = find_runtime();

    // Link
    let output_path = source_path.with_extension("");
    if let Err(e) = linker::link(&obj_path, &runtime_path, &output_path) {
        eprintln!("Link error: {}", e);
        let _ = std::fs::remove_file(&obj_path);
        let temp_runtime = std::env::temp_dir().join(format!("libknot_runtime_{}.a", std::process::id()));
        if runtime_path == temp_runtime {
            let _ = std::fs::remove_file(&runtime_path);
        }
        process::exit(1);
    }

    // Clean up
    let _ = std::fs::remove_file(&obj_path);
    // Remove temp runtime if it was extracted from embedded bytes
    let temp_runtime = std::env::temp_dir().join(format!("libknot_runtime_{}.a", std::process::id()));
    if runtime_path == temp_runtime {
        let _ = std::fs::remove_file(&runtime_path);
    }

    // Update schema lockfile (use original decls — the mutated module contains
    // prelude/import decls whose spans don't correspond to this source text).
    let lockfile_module = knot::ast::Module {
        imports: vec![],
        decls: original_decls,
    };
    if let Err(e) = lockfile::update(&source_path, &source, &lockfile_module) {
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

fn find_runtime() -> PathBuf {
    // 1. Environment variable override
    if let Ok(path) = std::env::var("KNOT_RUNTIME_LIB") {
        let p = PathBuf::from(path);
        if p.exists() {
            return p;
        }
    }

    // 2. Same directory as the compiler executable
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            let candidate = exe_dir.join("libknot_runtime.a");
            if candidate.exists() {
                return candidate;
            }
        }
    }

    // 3. Extract embedded runtime to a temp file
    if let Some(bytes) = EMBEDDED_RUNTIME {
        let tmp = std::env::temp_dir().join(format!("libknot_runtime_{}.a", std::process::id()));
        if std::fs::write(&tmp, bytes).is_ok() {
            return tmp;
        }
    }

    eprintln!("Error: cannot find libknot_runtime.a");
    eprintln!("Ensure knot-runtime is built (cargo build -p knot-runtime)");
    eprintln!("Or set KNOT_RUNTIME_LIB=/path/to/libknot_runtime.a");
    process::exit(1);
}
