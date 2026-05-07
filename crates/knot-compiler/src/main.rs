//! Knot compiler CLI.
//!
//! Usage: knotc build <file.knot>

use knot_compiler::{base, codegen, desugar, effects, infer, linker, lockfile, modules, stratify, types, unused};

use std::collections::HashMap;
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
                eprintln!("Usage: knotc build <file.knot> [--name=value ...]");
                process::exit(1);
            }
            // Parse compile-time overrides from remaining args (--name=value or --name value)
            let mut overrides = HashMap::new();
            let mut i = 3;
            while i < args.len() {
                if let Some(rest) = args[i].strip_prefix("--") {
                    if let Some((name, val)) = rest.split_once('=') {
                        overrides.insert(name.to_string(), val.to_string());
                    } else if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                        overrides.insert(rest.to_string(), args[i + 1].clone());
                        i += 1;
                    } else {
                        eprintln!("Error: missing value for --{}", rest);
                        process::exit(1);
                    }
                } else {
                    eprintln!("Error: unexpected argument '{}'", args[i]);
                    process::exit(1);
                }
                i += 1;
            }
            cmd_build(&args[2], &overrides);
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
    eprintln!("  knotc build <file.knot> [--name=value ...]   Compile with optional constant overrides");
    eprintln!("  knotc fmt [--check] [--stdout] <file.knot>   Format a source file in place");
    eprintln!("  knotc help                                   Show this help message");
}

fn cmd_fmt(args: &[String]) {
    let mut check = false;
    let mut to_stdout = false;
    let mut paths: Vec<&str> = Vec::new();
    for a in args {
        match a.as_str() {
            "--check" => check = true,
            "--stdout" | "-" => to_stdout = true,
            other if other.starts_with("--") => {
                eprintln!("Error: unknown fmt flag '{}'", other);
                eprintln!("Usage: knotc fmt [--check] [--stdout] <file.knot>...");
                process::exit(2);
            }
            other => paths.push(other),
        }
    }
    if paths.is_empty() {
        eprintln!("Error: missing source file");
        eprintln!("Usage: knotc fmt [--check] [--stdout] <file.knot>...");
        process::exit(2);
    }

    let mut any_diff = false;
    for path_str in &paths {
        let source_path = PathBuf::from(path_str);
        let source = match std::fs::read_to_string(&source_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error reading {}: {}", source_path.display(), e);
                process::exit(1);
            }
        };

        let lexer = knot::lexer::Lexer::new(&source);
        let (tokens, lex_diags) = lexer.tokenize();
        let filename = source_path.display().to_string();
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
        let (module, parse_diags) = parser.parse_module();
        let parse_errs: Vec<_> = parse_diags
            .iter()
            .filter(|d| d.severity == knot::diagnostic::Severity::Error)
            .collect();
        if !parse_errs.is_empty() {
            eprintln!("Cannot format {}: parse errors", source_path.display());
            for d in &parse_errs {
                eprintln!("{}", d.render(&source, &filename));
            }
            process::exit(1);
        }

        let formatted = knot::format::format_module(&source, &module);

        if check {
            if formatted != source {
                eprintln!("{}: not formatted", source_path.display());
                any_diff = true;
            }
        } else if to_stdout {
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

fn cmd_build(source_file: &str, overrides: &HashMap<String, String>) {
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
        if lex_diags.iter().any(|d| d.severity == knot::diagnostic::Severity::Error) {
            process::exit(1);
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
    let (infer_diags, monad_info, type_info, _local_types, refine_targets, refined_types, from_json_targets, elem_pushdown_ok) = infer::check(&module);
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

    // Unused-definition warnings (use original_decls to avoid flagging
    // prelude/imports, and to anchor spans to the user's source text).
    let unused_diags = unused::check(&original_decls);
    for diag in &unused_diags {
        eprintln!("{}", diag.render(&source, &filename));
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
    let obj_bytes = match codegen::compile(&module, &type_env, source_file, &monad_info, &refine_targets, &refined_types, &from_json_targets, &type_info, &elem_pushdown_ok, overrides) {
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
