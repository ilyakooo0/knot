//! Regression tests for trait dispatch when two ADTs share a constructor name.
//!
//! The codegen dispatcher (`define_trait_dispatchers`) selects an impl by
//! matching the value's runtime constructor tag against each impl's set of
//! constructor names. A tag does not identify a type: given
//!
//!     data Shape = Circle {r: Float 1} | Square {s: Float 1}
//!     data Blob  = Circle {r: Float 1} | Blob2 {x: Int 1}
//!
//! a `Circle` value matches the `Shape` arm and the `Blob` arm alike, so the
//! chain ran whichever impl was registered first — `area (… : Blob)` silently
//! returned the `Area Shape` result.
//!
//! Inference already resolves the trait's parameter to a concrete type at every
//! monomorphic site (`TraitCallTargets`), so those sites now bypass the
//! dispatcher and call the selected impl directly. A polymorphic site (inside
//! an `Area a => a -> Float` body) has no static type to dispatch on; when the
//! impls there are tag-ambiguous the program is rejected rather than
//! miscompiled.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn scratch_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_traitdisp_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Compile `source` in a fresh scratch dir; returns Ok(exe) or Err(stderr).
fn try_compile(test_name: &str, source: &str) -> Result<Compiled, String> {
    let dir = scratch_dir(test_name);
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_knot"))
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");

    if out.status.success() {
        let exe = dir.join("prog");
        Ok(Compiled { dir, exe })
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let _ = fs::remove_dir_all(&dir);
        Err(stderr)
    }
}

/// Compile and run; returns stdout. Panics if compilation or the run fails.
fn compile_and_run(test_name: &str, source: &str) -> String {
    let c = try_compile(test_name, source)
        .unwrap_or_else(|e| panic!("knot build failed for {test_name}:\n{e}"));
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    assert!(
        out.status.success(),
        "program {test_name} exited with failure:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Two ADTs sharing the `Circle` constructor. `Area Shape` is declared first,
/// so a tag-keyed dispatcher answers 1.0 for *both* types.
const SHARED_CTOR_PRELUDE: &str = r#"data Shape = Circle {r: Float 1} | Square {s: Float 1}
data Blob = Circle {r: Float 1} | Blob2 {x: Int 1}

trait Area a where
  area : a -> Float 1
  scaled : a -> Float 1 -> Float 1

impl Area Shape where
  area sh = 1.0
  scaled sh k = 1.0 * k

impl Area Blob where
  area b = 2.0
  scaled b k = 2.0 * k
"#;

#[test]
fn shared_constructor_dispatches_on_static_type() {
    // The reported bug: `area (Circle {…} : Blob)` ran the `Area Shape` impl
    // because both types spell the constructor `Circle`.
    let src = format!(
        "{SHARED_CTOR_PRELUDE}
main = with {{s (Circle {{r 2.0}} : Shape) b (Circle {{r 2.0}} : Blob)}} (do
  println (show (area s))
  println (show (area b)))
"
    );
    let stdout = compile_and_run("static_type", &src);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "\"1.0\"", "Shape must run the Area Shape impl");
    assert_eq!(
        lines[1], "\"2.0\"",
        "Blob must run the Area Blob impl, not Shape's (stdout: {stdout})"
    );
}

#[test]
fn shared_constructor_dispatches_multi_arg_method() {
    // Dispatch is on the first param; the extra arg must still be forwarded.
    let src = format!(
        "{SHARED_CTOR_PRELUDE}
main = with {{s (Circle {{r 2.0}} : Shape) b (Circle {{r 2.0}} : Blob)}} (do
  println (show (scaled s 10.0))
  println (show (scaled b 10.0)))
"
    );
    let stdout = compile_and_run("multi_arg", &src);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "\"10.0\"");
    assert_eq!(
        lines[1], "\"20.0\"",
        "Blob must run the Area Blob impl (stdout: {stdout})"
    );
}

#[test]
fn shared_constructor_dispatches_when_method_is_a_bare_value() {
    // `map area xs` boxes the method as a function value. That path used to
    // box the tag dispatcher; it must box the statically selected impl.
    let src = format!(
        "{SHARED_CTOR_PRELUDE}
main = with {{shapes [Circle {{r 1.0}} : Shape] blobs [Circle {{r 1.0}} : Blob]}} (do
  println (show (map area shapes))
  println (show (map area blobs)))
"
    );
    let stdout = compile_and_run("bare_value", &src);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "\"[1.0]\"");
    assert_eq!(
        lines[1], "\"[2.0]\"",
        "Blob must run the Area Blob impl (stdout: {stdout})"
    );
}

#[test]
fn ambiguous_polymorphic_dispatch_is_rejected() {
    // Inside `Area a => a -> Float` the type is unknown until run time, and the
    // shared `Circle` tag cannot select between the two impls. Reject the
    // program — previously it compiled and silently ran the first impl.
    let src = format!(
        "{SHARED_CTOR_PRELUDE}
describe : Area a => a -> Float 1
describe = \\x -> area x

main = do
  println (show (describe (Circle {{r 2.0}} : Blob)))
"
    );
    let err = try_compile("ambiguous_poly", &src)
        .err()
        .expect("a tag-ambiguous polymorphic dispatch must not compile");
    assert!(
        err.contains("cannot dispatch 'area' at run time"),
        "expected an ambiguous-dispatch error, got:\n{err}"
    );
    assert!(
        err.contains("'Shape'") && err.contains("'Blob'"),
        "error should name both clashing types, got:\n{err}"
    );
}

#[test]
fn polymorphic_dispatch_still_works_without_a_tag_clash() {
    // The ambiguity guard must not overfire: with distinct constructor names
    // the runtime dispatcher is still sound and polymorphic calls must work.
    let src = r#"data Shape = Circle {r: Float 1} | Square {s: Float 1}
data Blob = Blob1 {r: Float 1} | Blob2 {x: Int 1}

trait Area a where
  area : a -> Float 1

impl Area Shape where
  area sh = 1.0

impl Area Blob where
  area b = 2.0

describe : Area a => a -> Float 1
describe = \x -> area x

main = do
  println (show (describe (Circle {r 2.0} : Shape)))
  println (show (describe (Blob1 {r 2.0} : Blob)))
"#;
    let stdout = compile_and_run("poly_no_clash", src);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "\"1.0\"");
    assert_eq!(
        lines[1], "\"2.0\"",
        "runtime tag dispatch is sound here (stdout: {stdout})"
    );
}
