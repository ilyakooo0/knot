//! Regression tests:
//! 1. `knot build` must never overwrite the source file with the object file
//!    or the linked binary (extensionless sources, `foo.o` sources, `-o` equal
//!    to the source path).
//! 2. Nested route composites (`route All = AB | BApi` where `AB` is itself a
//!    composite) must resolve all transitive endpoints regardless of
//!    declaration order in desugaring.

use knot::ast::{DeclKind, Module};
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse(src: &str) -> Module {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        !lex_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "lex errors in test source"
    );
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(
        !parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "parse errors in test source: {:?}",
        parse_diags
    );
    module
}

/// Constructor names of the synthetic `data` decl that desugaring generates
/// for the route named `route_name`.
fn data_ctor_names(module: &Module, route_name: &str) -> Vec<String> {
    for decl in &module.decls {
        if let DeclKind::Data { name, constructors, .. } = &decl.node
            && name == route_name {
                return constructors.iter().map(|c| c.name.clone()).collect();
            }
    }
    panic!("no generated data decl named {}", route_name);
}

fn scratch_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_{}_{}",
        label,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn knot_build(args: &[&str], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_knot"))
        .arg("build")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to spawn knot binary")
}

const HELLO_SRC: &str = "main = println \"hi\"\n";

// ---------------------------------------------------------------------------
// Fix 1: build output must never clobber the source file
// ---------------------------------------------------------------------------

#[test]
fn build_extensionless_source_is_not_overwritten() {
    let dir = scratch_dir("noext");
    let src_path = dir.join("prog");
    std::fs::write(&src_path, HELLO_SRC).unwrap();

    let out = knot_build(&["prog"], &dir);
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The source must survive untouched.
    let after = std::fs::read_to_string(&src_path).expect("source no longer readable as text");
    assert_eq!(after, HELLO_SRC, "source file was overwritten by the build");

    // The binary lands at a non-colliding sibling path.
    let bin = dir.join("prog.out");
    assert!(bin.exists(), "expected fallback binary at prog.out");
    let meta = std::fs::metadata(&bin).unwrap();
    assert!(meta.len() > 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn build_rejects_output_equal_to_source() {
    let dir = scratch_dir("oclash");
    let src_path = dir.join("prog");
    std::fs::write(&src_path, HELLO_SRC).unwrap();

    let out = knot_build(&["prog", "-o", "prog"], &dir);
    assert!(
        !out.status.success(),
        "build should fail when -o equals the source path"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("same as the source"),
        "expected a clear error message, got: {}",
        stderr
    );

    let after = std::fs::read_to_string(&src_path).unwrap();
    assert_eq!(after, HELLO_SRC, "source file was modified");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn build_dot_o_source_is_not_clobbered_by_object_file() {
    let dir = scratch_dir("doto");
    let src_path = dir.join("prog.o");
    std::fs::write(&src_path, HELLO_SRC).unwrap();

    let out = knot_build(&["prog.o"], &dir);
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let after = std::fs::read_to_string(&src_path).expect("source no longer readable as text");
    assert_eq!(
        after, HELLO_SRC,
        "source file was overwritten by the intermediate object file"
    );
    assert!(dir.join("prog").exists(), "expected binary at prog");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn build_normal_knot_source_produces_sibling_binary() {
    let dir = scratch_dir("normal");
    let src_path = dir.join("prog.knot");
    std::fs::write(&src_path, HELLO_SRC).unwrap();

    let out = knot_build(&["prog.knot"], &dir);
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir.join("prog").exists(), "expected binary at prog");
    assert_eq!(std::fs::read_to_string(&src_path).unwrap(), HELLO_SRC);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn build_explicit_output_path_still_works() {
    let dir = scratch_dir("explicit");
    let src_path = dir.join("prog.knot");
    std::fs::write(&src_path, HELLO_SRC).unwrap();

    let out = knot_build(&["prog.knot", "-o", "myapp"], &dir);
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir.join("myapp").exists(), "expected binary at myapp");
    assert_eq!(std::fs::read_to_string(&src_path).unwrap(), HELLO_SRC);

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Fix 2: nested route composites resolve transitively, order-independently
// ---------------------------------------------------------------------------

const ROUTES_A: &str = r#"
route AApi where
  /a
    GET /one -> Int = GetOne

route BApi where
  /b
    GET /two -> Int = GetTwo
"#;

fn ctors_for(src: &str, route: &str) -> Vec<String> {
    let mut module = parse(src);
    knot_compiler::desugar::desugar(&mut module);
    let mut ctors = data_ctor_names(&module, route);
    ctors.sort();
    ctors
}

#[test]
fn nested_composite_resolves_transitive_endpoints() {
    // Composite-of-composite declared *after* its component.
    let src = format!("{}\nroute AB = AApi\nroute All = AB | BApi\n", ROUTES_A);
    assert_eq!(ctors_for(&src, "All"), vec!["GetOne", "GetTwo"]);
    assert_eq!(ctors_for(&src, "AB"), vec!["GetOne"]);
}

#[test]
fn nested_composite_is_order_independent() {
    // Composite-of-composite declared *before* its component.
    let forward = format!("{}\nroute AB = AApi\nroute All = AB | BApi\n", ROUTES_A);
    let reverse = format!("{}\nroute All = AB | BApi\nroute AB = AApi\n", ROUTES_A);
    let fwd = ctors_for(&forward, "All");
    let rev = ctors_for(&reverse, "All");
    assert_eq!(fwd, rev, "composite endpoints depend on declaration order");
    assert_eq!(fwd, vec!["GetOne", "GetTwo"]);
}

#[test]
fn deeply_nested_composites_resolve() {
    let src = format!(
        "{}\nroute L3 = L2\nroute L2 = L1\nroute L1 = AApi | BApi\n",
        ROUTES_A
    );
    assert_eq!(ctors_for(&src, "L3"), vec!["GetOne", "GetTwo"]);
}

#[test]
fn unknown_composite_component_does_not_drop_known_ones() {
    // `Bogus` resolves to nothing (inference reports it); the known
    // component must still contribute its endpoints, and desugar must
    // not loop or panic.
    let src = format!("{}\nroute All = Bogus | BApi\n", ROUTES_A);
    assert_eq!(ctors_for(&src, "All"), vec!["GetTwo"]);
}

#[test]
fn cyclic_composites_do_not_hang() {
    // Mutually recursive composites are a user error, but desugar must
    // terminate and still emit a data decl for each.
    let src = format!("{}\nroute X = Y | AApi\nroute Y = X | BApi\n", ROUTES_A);
    let mut module = parse(&src);
    knot_compiler::desugar::desugar(&mut module);
    // Both decls exist; entry sets may be partial, but the run terminates.
    let _ = data_ctor_names(&module, "X");
    let _ = data_ctor_names(&module, "Y");
}

// ---------------------------------------------------------------------------
// Fix 3: schema lockfile must embed type declarations from imported modules
// so `*rel : [ImportedType]` round-trips through the lockfile check
// ---------------------------------------------------------------------------

#[test]
fn lockfile_resolves_types_imported_from_other_modules() {
    let dir = scratch_dir("lockfile_imports");
    std::fs::write(
        dir.join("types.knot"),
        "type Person = {name: Text, age: Int}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("main.knot"),
        "import ./types\n\n*people : [Person]\n\nmain = do\n  rows <- *people\n  yield (count rows)\n",
    )
    .unwrap();

    // First build writes the lockfile.
    let out = knot_build(&["main.knot"], &dir);
    assert!(
        out.status.success(),
        "first build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lock = std::fs::read_to_string(dir.join("main.schema.lock")).unwrap();
    assert!(
        lock.contains("type Person"),
        "lockfile must embed the imported Person alias, got:\n{}",
        lock
    );

    // Second build must pass the lockfile check (previously it reported a
    // phantom "breaking schema change" because Person resolved to _value:text).
    let out = knot_build(&["main.knot"], &dir);
    assert!(
        out.status.success(),
        "second build failed the lockfile check: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Fix 4: importing the same module twice with disjoint selective lists must
// make *all* selected names visible. The diamond-dedup used to skip the second
// import wholesale, dropping its selection.
// ---------------------------------------------------------------------------

#[test]
fn repeated_selective_imports_of_same_module_union() {
    let dir = scratch_dir("repeated_selective_imports");
    std::fs::write(dir.join("util.knot"), "valX = 10\n\nvalY = 20\n").unwrap();
    std::fs::write(
        dir.join("main.knot"),
        "import ./util (valX)\nimport ./util (valY)\n\nmain = do\n  println (show valX)\n  println (show valY)\n",
    )
    .unwrap();

    let out = knot_build(&["main.knot"], &dir);
    assert!(
        out.status.success(),
        "build failed — second selective import was dropped: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let run = Command::new(dir.join("main"))
        .current_dir(&dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        stdout.contains("10") && stdout.contains("20"),
        "both valX and valY should be visible, got stdout:\n{}",
        stdout
    );

    let _ = std::fs::remove_dir_all(&dir);
}
