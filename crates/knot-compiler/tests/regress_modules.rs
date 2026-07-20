//! Regression tests for the module system (`modules.rs`):
//!
//! 1. A top-level name defined by two modules reached codegen as two
//!    definitions of one symbol and aborted the compiler with an unhandled
//!    Cranelift `DuplicateDefinition` panic. It must be a diagnostic.
//! 2. `unit` declarations were dropped by a module's export filter, leaving the
//!    units its exported signatures are written in (`Float N`) undefined at the
//!    import site — reported as bogus unit mismatches.
//! 3. An `impl` of a trait declared in *another* module was dropped by the
//!    export filter, which only kept impls of traits the module exported itself.
//! 4. A selective import (`import ./a (foo)`) claimed the module for the whole
//!    program: a sibling's full `import ./a` then found it already imported and
//!    merged nothing, so `a`'s other declarations — and everything `a` itself
//!    imports — silently vanished.
//!
//! Each test drives the real `knot` binary over a scratch directory.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct Scratch {
    dir: PathBuf,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

impl Scratch {
    fn new(test_name: &str) -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "knot_regress_modules_{}_{}",
            test_name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch { dir }
    }

    fn write(&self, name: &str, source: &str) {
        fs::write(self.dir.join(name), source).unwrap();
    }

    /// Run `knot build <entry>`; returns (ok, stdout, stderr).
    fn build(&self, entry: &str) -> (bool, String, String) {
        let out = Command::new(env!("CARGO_BIN_EXE_knot"))
            .arg("build")
            .arg(self.dir.join(entry))
            .current_dir(&self.dir)
            .output()
            .expect("failed to spawn knot compiler");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// Build `entry` and run the resulting executable; returns its stdout.
    fn build_and_run(&self, entry: &str, exe: &str) -> String {
        let (ok, stdout, stderr) = self.build(entry);
        assert!(ok, "build failed\nstdout: {stdout}\nstderr: {stderr}");
        let out = Command::new(self.dir.join(exe))
            .current_dir(&self.dir)
            .output()
            .expect("failed to run compiled program");
        assert!(
            out.status.success(),
            "program exited non-zero:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

// ── 1. Duplicate top-level names are a diagnostic, not a panic ───────────

#[test]
fn cross_module_duplicate_name_is_a_diagnostic() {
    let s = Scratch::new("dup_cross_module");
    s.write("lib.knot", "foo = 1\n");
    s.write(
        "main.knot",
        "import ./lib\n\nfoo = 2\n\nmain = do\n  println (show foo)\n  yield {}\n",
    );

    let (ok, _stdout, stderr) = s.build("main.knot");
    assert!(!ok, "a name defined by two modules must not compile");
    assert!(
        stderr.contains("duplicate definition of 'foo'"),
        "expected a duplicate-definition diagnostic, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "the compiler must not panic on a duplicate definition, got:\n{stderr}"
    );
}

#[test]
fn duplicate_name_within_one_module_is_a_diagnostic() {
    let s = Scratch::new("dup_same_module");
    s.write(
        "main.knot",
        "foo = 1\nfoo = 2\n\nmain = do\n  println (show foo)\n  yield {}\n",
    );

    let (ok, _stdout, stderr) = s.build("main.knot");
    assert!(!ok, "a name defined twice in one module must not compile");
    assert!(
        stderr.contains("duplicate definition of 'foo'"),
        "expected a duplicate-definition diagnostic, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "the compiler must not panic on a duplicate definition, got:\n{stderr}"
    );
}

#[test]
fn a_type_and_a_function_may_share_a_name_across_modules() {
    // Control: the duplicate check is per namespace. A relation `*items`, a
    // function `items`, and a type `Items` are three different things.
    let s = Scratch::new("dup_namespaces");
    s.write(
        "lib.knot",
        "export type Items = [{v: Int 1}]\n\nexport items = 42\n",
    );
    s.write(
        "main.knot",
        "import ./lib\n\n*items : Items\n\nmain = do\n  replace *items = [{v 1}]\n  rows <- *items\n  println (show (count rows + items))\n  yield {}\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("43"),
        "a source, a function, and a type named alike must coexist"
    );
}

// ── 2. Units match across a module boundary ──────────────────────────────

#[test]
fn units_match_across_a_module_boundary() {
    let s = Scratch::new("export_units");
    // Units need no declaration: the same unit expression written in an
    // exported signature and at the call site denote the same unit, so an
    // imported function's unit-typed argument unifies with the caller's value.
    s.write(
        "phys.knot",
        "\n\
         export baseForce : Float (Kg*M/S^2)\nbaseForce = (5.0 : Float (Kg*M/S^2))\n\n\
         export addForce : Float (Kg*M/S^2) -> Float (Kg*M/S^2) -> Float (Kg*M/S^2)\naddForce = \\a b -> a + b\n",
    );
    s.write(
        "main.knot",
        "import ./phys\n\nmain = with {total (addForce baseForce (3.0 : Float (Kg*M/S^2)))} (do\n  \
         println (show (stripFloatUnit total))\n  yield {})\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("8.0"),
        "the same unit expression must unify across the import boundary"
    );
}

// ── 4. A selective import must not claim the module for the program ──────

#[test]
fn selective_import_does_not_poison_a_sibling_full_import() {
    let s = Scratch::new("selective_vs_full");
    // `b` imports only `foo` from `a`; `c` imports all of `a`. Whichever is
    // resolved first, both must get what they asked for — including `deep`,
    // which `a` itself imports and `bar` depends on.
    s.write("deep.knot", "export deep = 7\n");
    s.write("a.knot", "import ./deep\n\nexport foo = 1\nexport bar = deep + 2\n");
    s.write("b.knot", "import ./a (foo)\n\nexport useFoo = foo + 10\n");
    s.write("c.knot", "import ./a\n\nexport useBar = bar + 100\n");
    s.write(
        "main.knot",
        "import ./b\nimport ./c\n\nmain = do\n  println (show (useFoo + useBar))\n  yield {}\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("120"),
        "the full import of `a` must still contribute `bar` (and `deep`)"
    );
}

#[test]
fn full_import_before_a_sibling_selective_import_of_the_same_module() {
    // The mirror image of the test above: the full import is resolved first,
    // and the selective one must not narrow what it already contributed.
    let s = Scratch::new("full_vs_selective");
    s.write("deep.knot", "export deep = 7\n");
    s.write("a.knot", "import ./deep\n\nexport foo = 1\nexport bar = deep + 2\n");
    s.write("b.knot", "import ./a (foo)\n\nexport useFoo = foo + 10\n");
    s.write("c.knot", "import ./a\n\nexport useBar = bar + 100\n");
    s.write(
        "main.knot",
        "import ./c\nimport ./b\n\nmain = do\n  println (show (useFoo + useBar))\n  yield {}\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("120"),
        "the selective import of `a` must not drop what the full import merged"
    );
}

#[test]
fn selective_import_keeps_the_modules_transitive_dependencies() {
    // Selecting `foo` out of `a` must still bring in what `a` imports —
    // `foo`'s own definition depends on it.
    let s = Scratch::new("selective_transitive");
    s.write("deep.knot", "export deep = 7\n");
    s.write("a.knot", "import ./deep\n\nexport foo = deep + 1\nexport bar = 2\n");
    s.write(
        "main.knot",
        "import ./a (foo)\n\nmain = do\n  println (show foo)\n  yield {}\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("8"),
        "`deep` must be merged even though only `foo` was selected"
    );
}

#[test]
fn repeated_imports_of_one_module_merge_its_decls_once() {
    // Control for the merge bookkeeping: a module reached by several paths
    // (diamond) contributes its declarations exactly once — merging them twice
    // would be a duplicate definition.
    let s = Scratch::new("diamond");
    s.write("base.knot", "export base = 5\n");
    s.write("left.knot", "import ./base\n\nexport left = base + 1\n");
    s.write("right.knot", "import ./base\n\nexport right = base + 2\n");
    s.write(
        "main.knot",
        "import ./left\nimport ./right\nimport ./base\n\nmain = do\n  \
         println (show (left + right + base))\n  yield {}\n",
    );

    assert!(
        s.build_and_run("main.knot", "main").contains("18"),
        "a module reached by several paths must be merged exactly once"
    );
}

// ── Controls: import errors still reported ───────────────────────────────

#[test]
fn selective_import_of_an_unknown_name_still_errors() {
    let s = Scratch::new("unknown_name");
    s.write("a.knot", "export foo = 1\n");
    s.write(
        "main.knot",
        "import ./a (nope)\n\nmain = do\n  println (show foo)\n  yield {}\n",
    );

    let (ok, _stdout, stderr) = s.build("main.knot");
    assert!(!ok, "selecting a name the module doesn't have must fail");
    assert!(
        stderr.contains("'nope' not found in module"),
        "expected a not-found diagnostic, got:\n{stderr}"
    );
}

#[test]
fn import_cycles_are_still_detected() {
    let s = Scratch::new("cycle");
    s.write("x.knot", "import ./y\n\nexport ax = 1\n");
    s.write("y.knot", "import ./x\n\nexport ay = 2\n");
    s.write(
        "main.knot",
        "import ./x\n\nmain = do\n  println (show ax)\n  yield {}\n",
    );

    let (ok, _stdout, stderr) = s.build("main.knot");
    assert!(!ok, "an import cycle must fail the build");
    assert!(
        stderr.contains("import cycle detected"),
        "expected a cycle diagnostic, got:\n{stderr}"
    );
}
