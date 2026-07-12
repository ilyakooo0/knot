//! Knot compiler CLI.
//!
//! Usage: knot build <file.knot>

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
                eprintln!("Usage: knot build <file.knot> [-o <path>] [--name=value ...]");
                process::exit(1);
            }
            // Parse -o/--output and compile-time overrides from remaining args
            let mut overrides = HashMap::new();
            let mut output: Option<PathBuf> = None;
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

            cmd_build(&args[2], output.as_deref(), &overrides);
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
    eprintln!("  knot build <file.knot> [-o <path>] [--name=value ...]  Compile with optional output path and constant overrides");
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
        let (module, parse_diags) = parser.parse_module();
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

        let formatted = knot::format::format_module(&source, &module);

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

fn cmd_build(source_file: &str, output_override: Option<&std::path::Path>, overrides: &HashMap<String, String>) {
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
    let imported_snippets = match modules::resolve_imports(&mut module, &source_path) {
        Ok(snippets) => snippets,
        Err(diags) => {
            for diag in &diags {
                eprintln!("{}", diag);
            }
            process::exit(1);
        }
    };

    // Inject built-in trait declarations and primitive impls
    base::inject_prelude(&mut module);

    // Desugar monadic do blocks into trait method calls
    desugar::desugar(&mut module);

    // Detect recursive type aliases before resolution — a cyclic alias
    // (`type A = {x: A}`, mutual cycles) can never be resolved, so report
    // a diagnostic instead of letting resolution chase the cycle.
    let cycle_diags = types::check_alias_cycles(&module);
    if !cycle_diags.is_empty() {
        for diag in &cycle_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Reject persisted fields whose names collide with the runtime's internal
    // SQLite columns (`_id`, `_tag`, ...) — they used to compile clean and
    // abort at table init with "duplicate column name".
    let reserved_diags = types::check_reserved_field_names(&module);
    if !reserved_diags.is_empty() {
        for diag in &reserved_diags {
            eprintln!("{}", diag.render(&source, &filename));
        }
        process::exit(1);
    }

    // Resolve types
    let type_env = types::TypeEnv::from_module(&module);

    // Type inference
    let (infer_diags, monad_info, type_info, _local_types, refine_targets, refined_types, from_json_targets, elem_pushdown_ok, trait_call_targets, show_unit_strings, sum_float_spans) = infer::check(&mut module);
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
    let obj_bytes = match codegen::compile(&module, &type_env, source_file, &monad_info, &refine_targets, &refined_types, &from_json_targets, &type_info, &elem_pushdown_ok, &trait_call_targets, &show_unit_strings, &sum_float_spans, overrides) {
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

    // Link (output path computed and collision-checked above)
    if let Err(e) = linker::link(&obj_path, &runtime_path, &output_path) {
        eprintln!("Link error: {}", e);
        let _ = std::fs::remove_file(&obj_path);
        if is_extracted_temp_runtime(&runtime_path) {
            let _ = std::fs::remove_file(&runtime_path);
        }
        process::exit(1);
    }

    // Clean up
    let _ = std::fs::remove_file(&obj_path);
    // Remove temp runtime if it was extracted from embedded bytes
    if is_extracted_temp_runtime(&runtime_path) {
        let _ = std::fs::remove_file(&runtime_path);
    }

    // Update schema lockfile (use original decls — the mutated module contains
    // prelude/import decls whose spans don't correspond to this source text).
    let lockfile_module = knot::ast::Module {
        imports: vec![],
        decls: original_decls,
    };
    if let Err(e) = lockfile::update(&source_path, &source, &lockfile_module, &imported_snippets.types, &imported_snippets.sources) {
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

/// Returns true if the runtime archive `lib` is stale — i.e. the newest
/// source file under `crates/knot-runtime/src/` (recursively) or the
/// runtime crate's `Cargo.toml` has an mtime newer than the archive itself.
/// Falls back to `false` (assume fresh) when the workspace root can't be
/// located or mtimes can't be compared, so we never block builds spuriously.
fn is_runtime_stale(lib: &std::path::Path, exe_dir: &std::path::Path) -> bool {
    // Walk up from the compiler's directory to find the workspace root
    // (the directory containing `crates/knot-runtime/`). In a cargo
    // workspace the compiler binary lives in `target/<profile>/`, so the
    // workspace root is two levels above `exe_dir`.
    let workspace_root = exe_dir
        .ancestors()
        .skip(1) // skip `target/<profile>` itself
        .find(|d| d.join("crates/knot-runtime").is_dir());

    let workspace_root = match workspace_root {
        Some(r) => r,
        None => return false, // can't locate workspace — assume fresh
    };

    let lib_mtime = match std::fs::metadata(lib).and_then(|m| m.modified()) {
        Ok(m) => m,
        Err(_) => return false,
    };

    // Recursively find the newest mtime among all `*.rs` files under
    // `crates/knot-runtime/src/`, plus the runtime crate's `Cargo.toml`.
    let mut newest_src: Option<std::time::SystemTime> = None;

    fn consider(
        p: &std::path::Path,
        newest: &mut Option<std::time::SystemTime>,
    ) {
        if let Ok(m) = std::fs::metadata(p).and_then(|m| m.modified())
            && newest.map(|n| m > n).unwrap_or(true) {
            *newest = Some(m);
        }
    }

    fn walk_dir(dir: &std::path::Path, newest: &mut Option<std::time::SystemTime>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk_dir(&p, newest);
                } else if p.extension().map(|e| e == "rs").unwrap_or(false) {
                    consider(&p, newest);
                }
            }
        }
    }

    walk_dir(&workspace_root.join("crates/knot-runtime/src"), &mut newest_src);
    consider(
        &workspace_root.join("crates/knot-runtime/Cargo.toml"),
        &mut newest_src,
    );

    match newest_src {
        Some(src_mtime) => src_mtime > lib_mtime,
        None => false, // no source files found — assume fresh
    }
}

fn find_runtime() -> PathBuf {
    // 1. Environment variable override
    if let Ok(path) = std::env::var("KNOT_RUNTIME_LIB") {
        let p = PathBuf::from(&path);
        if p.exists() {
            // Warn if the archive is stale relative to the runtime source,
            // but still use it — the user set the override explicitly.
            if let Some(exe_dir) = std::env::current_exe()
                .ok()
                .and_then(|e| e.parent().map(|p| p.to_path_buf()))
                && is_runtime_stale(&p, &exe_dir)
            {
                    eprintln!(
                        "Warning: KNOT_RUNTIME_LIB archive '{}' is older than \
                         crates/knot-runtime/src/ — rebuild knot-runtime to \
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
                // Freshness check: if the runtime source is newer than the
                // archive, the archive is stale (e.g. only knot-compiler was
                // rebuilt). Skip it and fall through to the embedded runtime
                // rather than silently linking stale code.
                if is_runtime_stale(&candidate, exe_dir) {
                    eprintln!(
                        "Warning: {} is older than crates/knot-runtime/src/ \
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
