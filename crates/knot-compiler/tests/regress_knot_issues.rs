//! Regression tests for the issues reported in `knot-issues.txt`:
//!
//!  1. CRITICAL — every IO do-block failed with "expected {}, found IO _ {}",
//!     including the prelude's own `forEach`, so essentially no program
//!     compiled. The desugarer wrapped a do-block's final *bare* expression in
//!     `__yield` unconditionally, typing an already-monadic final statement as
//!     `m (m a)`.
//!  3. `filter` with a base-typed predicate downgraded the list's refined
//!     element type.
//!  4. `elem` did not work across a refined/base subtype boundary.
//!  7. A `do` block of only `let`s plus a final expression got a fresh monad
//!     variable instead of the final expression's type.
//!  8. No explicit `main` type annotation worked for an IO do-block.
//!  9. `[PubkeyHex]` could not be used where `[Text]` was expected.
//! 10. `{}` as an IO do-block's final expression was rejected.
//! 11. Refinement predicates reading a CLI-overridable constant were said to
//!     bake in the compile-time value (not reproducible — locked in here).
//! 12. Refined route body fields nested inside a list were never validated.

use knot::diagnostic::Diagnostic;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn parse(src: &str) -> knot::ast::Module {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(parse_diags.is_empty(), "unexpected parse diagnostics: {parse_diags:?}");
    module
}

/// Mirror the compiler pipeline: prelude → desugar → infer.
fn check_src(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _refine, _refined, _json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn errors(diags: &[Diagnostic]) -> Vec<&str> {
    diags
        .iter()
        .filter(|d| matches!(d.severity, knot::diagnostic::Severity::Error))
        .map(|d| d.message.as_str())
        .collect()
}

fn assert_clean(src: &str, what: &str) {
    let diags = check_src(src);
    let errs = errors(&diags);
    assert!(errs.is_empty(), "{what} must type-check, got: {errs:?}");
}

fn assert_rejects(src: &str, needle: &str, what: &str) {
    let diags = check_src(src);
    let errs = errors(&diags);
    assert!(
        errs.iter().any(|e| e.contains(needle)),
        "{what} must be rejected with a message containing {needle:?}, got: {errs:?}",
    );
}

// ── End-to-end harness ────────────────────────────────────────────

struct Compiled {
    dir: PathBuf,
    exe: PathBuf,
}

impl Drop for Compiled {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn compile(test_name: &str, source: &str) -> Compiled {
    let dir = std::env::temp_dir().join(format!(
        "knot_regress_issues_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src_path = dir.join("prog.knot");
    fs::write(&src_path, source).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_knot"))
        .arg("build")
        .arg(&src_path)
        .current_dir(&dir)
        .output()
        .expect("failed to spawn knot compiler");
    assert!(
        out.status.success(),
        "knot build failed for {test_name}:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let exe = dir.join("prog");
    Compiled { dir, exe }
}

/// Compile, run with `args`, and return (stdout, stderr, success).
fn compile_and_run(test_name: &str, source: &str, args: &[&str]) -> (String, String, bool) {
    let c = compile(test_name, source);
    let out = Command::new(&c.exe)
        .args(args)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

/// A port nothing is listening on right now. Racy in principle; in practice
/// the kernel does not hand the same ephemeral port out twice in the window
/// between this returning and the server binding it.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A compiled program running as a child process, killed on drop.
struct Server {
    child: Child,
    _compiled: Compiled,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(test_name: &str, source: &str, port: u16) -> Server {
    let compiled = compile(test_name, source);
    let child = Command::new(&compiled.exe)
        .current_dir(&compiled.dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn server");
    // Wrap before the wait loop so the child is reaped even if it never
    // reaches the listening state.
    let server = Server { child, _compiled: compiled };

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start listening on port {port}");
}

/// POST `body` as JSON and return (status code, response body).
fn post_json(port: u16, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {response:?}"));
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

// ── 1. IO do-blocks ───────────────────────────────────────────────

/// The narrowest possible guard on the regression: the prelude's `forEach`
/// ends its do-block with a bare recursive call, which is *already* an
/// `IO {| e} {}`. Wrapping it in `__yield` typed it as `IO (IO {})`, and since
/// the prelude is injected into every module, *every* program failed to
/// compile — `main = println "hi"` included.
#[test]
fn prelude_typechecks_so_a_trivial_program_compiles() {
    assert_clean("main = println \"hi\"\n", "the smallest possible program");
}

#[test]
fn io_do_block_ending_in_yield_unit_compiles_and_runs() {
    let src = "main = do\n\
               \x20 forEach [1, 2, 3] (\\x -> println (show x))\n\
               \x20 println \"done\"\n\
               \x20 yield {}\n";
    let (stdout, stderr, ok) = compile_and_run("io_do_yield", src, &[]);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains('1') && stdout.contains('3') && stdout.contains("done"),
        "forEach did not run every element: {stdout:?}",
    );
}

/// A do-block whose final bare statement is already an action in the block's
/// monad evaluates to that action — it must not be `pure`-wrapped. Applies to
/// every monad, not just IO: this is `m >>= f`, and `f a : Maybe Int` here.
#[test]
fn do_block_final_monadic_action_is_not_double_wrapped() {
    let src = "safeDiv : Int -> Int -> Maybe Int\n\
               safeDiv = \\a b -> if b == 0 then Nothing {} else Just {value: a / b}\n\
               chain : Int -> Maybe Int\n\
               chain = \\x -> do\n\
               \x20 a <- safeDiv 100 x\n\
               \x20 safeDiv a 2\n\
               main = println (show (chain 5))\n";
    let (stdout, stderr, ok) = compile_and_run("maybe_chain", src, &[]);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("Just") && stdout.contains("10"),
        "chain 5 must be Just 10 (not Just (Just 10)): {stdout:?}",
    );
}

/// The other half of the same decision, and the case that motivated the
/// (over-broad) `__yield` wrap in the first place: a final bare expression
/// that is a plain *value* still becomes the monadic result via `pure`.
#[test]
fn do_block_final_plain_value_is_still_pure_wrapped() {
    let src = "describe : Int -> IO {random} Text\n\
               describe = \\n -> do\n\
               \x20 x <- randomInt n\n\
               \x20 show x\n\
               main = do\n\
               \x20 s <- describe 5\n\
               \x20 println (\"got \" ++ s)\n";
    assert_clean(src, "a do-block ending in a plain value");
}

/// The final statement is an IO action reached through an opaque callback
/// parameter — nothing about it is syntactically IO, so only its *type* says
/// it must not be `pure`-wrapped.
#[test]
fn io_action_from_a_callback_param_is_the_block_result() {
    let src = "runTwice : (Int -> IO {console} {}) -> IO {console} {}\n\
               runTwice = \\cb -> do\n\
               \x20 cb 1\n\
               \x20 cb 2\n\
               main = runTwice (\\x -> println (show x))\n";
    let (stdout, stderr, ok) = compile_and_run("callback_seq", src, &[]);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains('1') && stdout.contains('2'),
        "both callback invocations must run: {stdout:?}",
    );
}

// ── 7. Pure `let` do-blocks ───────────────────────────────────────

/// `do { let n = …; <expr> }` binds nothing monadic: no `<-`, no `where`, no
/// `yield`. It is plain `let … in <expr>` and its type is the final
/// expression's — not `m Bool` for some invented monad `m`.
#[test]
fn pure_let_do_block_has_the_final_expressions_type() {
    let src = "isValidHex : Text -> Bool\n\
               isValidHex = \\s -> do\n\
               \x20 let n = length s\n\
               \x20 n > 0 && n % 2 == 0\n\
               main = do\n\
               \x20 println (show (isValidHex \"abc\"))\n\
               \x20 println (show (isValidHex \"abcd\"))\n";
    let (stdout, stderr, ok) = compile_and_run("pure_let_do", src, &[]);
    assert!(ok, "program failed: {stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        lines[0].contains("False") && lines[1].contains("True"),
        "expected False then True, got: {stdout:?}",
    );
}

/// An explicit `yield` in an otherwise-pure `let` block still asks for the
/// monadic reading — the block is a one-element comprehension, not a `let`.
#[test]
fn let_block_with_explicit_yield_stays_monadic() {
    let src = "ones : [Int]\n\
               ones = do\n\
               \x20 let n = 1\n\
               \x20 yield n\n\
               main = println (show (count ones))\n";
    assert_clean(src, "a let block with an explicit yield");
}

// ── 8 & 10. `main` annotations and `{}` as a result ───────────────

#[test]
fn main_accepts_an_explicit_io_annotation() {
    assert_clean(
        "main : IO {console} {}\nmain = do\n  println \"a\"\n  yield {}\n",
        "main with a concrete effect row",
    );
    assert_clean(
        "main : IO _ {}\nmain = do\n  println \"a\"\n  yield {}\n",
        "main with an inferred effect row",
    );
}

/// Not a bug: a *rigid* row variable cannot absorb the concrete `console`
/// effect the body performs. Pinned so the fix above does not quietly loosen
/// effect checking.
#[test]
fn main_with_a_rigid_effect_row_is_still_rejected() {
    assert_rejects(
        "main : IO {| r} {}\nmain = do\n  println \"a\"\n  yield {}\n",
        "rigid type variable would escape",
        "a polymorphic effect row over a console-performing body",
    );
}

/// `{}` and `yield {}` are interchangeable as an IO do-block's last statement:
/// both make the block `IO … {}`. (Issue 10 read this as an ambiguity; it was
/// a symptom of issue 1 — neither form compiled.)
#[test]
fn bare_unit_and_yield_unit_agree_as_an_io_result() {
    assert_clean(
        "main = do\n  println \"a\"\n  yield {}\n",
        "an IO do-block ending in `yield {}`",
    );
    assert_clean(
        "main = do\n  println \"a\"\n  {}\n",
        "an IO do-block ending in a bare `{}`",
    );
    assert_clean(
        "main = do\n  println \"a\"\n  let done : {} = {}\n  done\n",
        "an IO do-block ending in a let-bound unit",
    );
}

// ── 3, 4, 9. Refined types across the subtype boundary ────────────

const SERVER_NAME: &str = "type ServerName = Text where \\s -> length s > 0\n\
                           isLocal : Text -> Bool\n\
                           isLocal = \\s -> s == \"localhost\"\n";

/// The predicate takes the *base* type, so inferring it first pinned
/// `filter`'s element variable to `Text` and the declared `[ServerName]`
/// result was then rejected. The data argument is what knows the type.
#[test]
fn filter_with_a_base_typed_predicate_keeps_the_refined_element_type() {
    let src = format!(
        "{SERVER_NAME}\
         collectPeers : [ServerName] -> [ServerName]\n\
         collectPeers = \\names -> filter (\\s -> not (isLocal s)) names\n\
         main = println (show (count (collectPeers [])))\n"
    );
    assert_clean(&src, "filter over a refined list with a base-typed predicate");
}

/// The same, through `map` — the reported shape.
#[test]
fn filter_over_a_mapped_refined_list_keeps_the_refinement() {
    let src = format!(
        "{SERVER_NAME}\
         collectPeers : [a] -> (a -> ServerName) -> [ServerName]\n\
         collectPeers = \\items getServer -> \
             filter (\\s -> not (isLocal s)) (map getServer items)\n\
         main = println (show (count (collectPeers ([] : [{{host: ServerName}}]) (\\p -> p.host))))\n"
    );
    assert_clean(&src, "filter over a mapped refined list");
}

/// `elem : a -> [a] -> Bool` pinned `a` to the refined needle and then refused
/// the base-typed haystack. Every `ServerName` *is* a `Text`, so `a` widens.
/// (`seen` is reached through `refine` — calling it on a bare literal would be
/// the base-into-refined introduction that `refine` exists to mediate.)
#[test]
fn elem_works_across_the_refined_subtype_boundary() {
    let src = format!(
        "{SERVER_NAME}\
         touched : [Text]\n\
         touched = [\"a\", \"b\"]\n\
         seen : ServerName -> Bool\n\
         seen = \\n -> elem n touched\n\
         main = case refine \"a\" of\n\
         \x20 Ok {{value: n}} -> println (show (seen n))\n\
         \x20 Err {{error: e}} -> println \"bad\"\n"
    );
    let (stdout, stderr, ok) = compile_and_run("elem_subtype", &src, &[]);
    assert!(ok, "program failed: {stderr}");
    assert!(
        stdout.contains("True"),
        "a refined needle must be findable in a base-typed list: {stdout:?}",
    );
}

#[test]
fn a_refined_list_flows_into_base_list_positions() {
    let src = "type PubkeyHex = Text where \\s -> length s == 64\n\
               joinAll : [Text] -> Text\n\
               joinAll = \\xs -> fold (\\acc t -> acc ++ t) \"\" xs\n\
               report : [PubkeyHex] -> Text\n\
               report = \\keys -> joinAll keys\n\
               merge : [PubkeyHex] -> [Text] -> [Text]\n\
               merge = \\keys extra -> union keys extra\n\
               anyKnown : [PubkeyHex] -> [Text] -> Bool\n\
               anyKnown = \\keys known -> any (\\k -> elem k known) keys\n\
               main = println (report [])\n";
    assert_clean(src, "a refined list used where a base list is expected");
}

// Soundness guards: widening a refinement away is sound, *introducing* one
// without `refine` is not. These must stay rejected.

#[test]
fn identity_cannot_launder_a_base_value_into_a_refinement() {
    assert_rejects(
        "type Nat = Int where \\x -> x >= 0\n\
         asNat : Int -> Nat\n\
         asNat = \\x -> x\n\
         main = println (show (asNat (0 - 5)))\n",
        "cannot implicitly use",
        "an identity function from a base type to its refinement",
    );
}

#[test]
fn filter_cannot_launder_a_base_value_into_a_refined_list() {
    assert_rejects(
        "type ServerName = Text where \\s -> length s > 0\n\
         launder : Text -> [ServerName]\n\
         launder = \\t -> filter (\\_ -> True {}) [t]\n\
         main = println (show (count (launder \"\")))\n",
        "cannot implicitly use",
        "a base-typed element smuggled through filter into a refined list",
    );
}

#[test]
fn a_base_element_read_back_out_of_a_base_list_stays_base() {
    assert_rejects(
        "type ServerName = Text where \\s -> length s > 0\n\
         takesServer : ServerName -> Bool\n\
         takesServer = \\s -> length s > 0\n\
         sneak : [Text] -> Bool\n\
         sneak = \\xs -> case head xs of\n\
         \x20 Just {value: t} -> takesServer t\n\
         \x20 Nothing {} -> False {}\n\
         main = println (show (sneak []))\n",
        "cannot implicitly use",
        "a Text pulled out of a [Text] and passed where a ServerName is required",
    );
}

// ── 11. Refinement predicates and CLI-overridable constants ───────

/// Reported as "the predicate uses the compile-time value, not the runtime
/// override". It does not: an overridable constant compiles to a function that
/// consults the CLI on every call, and a refinement predicate calls it like
/// any other reference. Locked in so it stays that way.
#[test]
fn a_refinement_predicate_reads_the_runtime_override_of_a_constant() {
    let src = "maxLen : Int\n\
               maxLen = 10\n\
               isValidName : Text -> Bool\n\
               isValidName = \\s -> length s > 0 && length s <= maxLen\n\
               type ShortName = Text where isValidName\n\
               describe : ShortName -> Text\n\
               describe = \\n -> \"accepted\"\n\
               check : Text -> Text\n\
               check = \\s -> case refine s of\n\
               \x20 Ok {value: n} -> describe n\n\
               \x20 Err {error: e} -> \"rejected\"\n\
               main = println (check \"abcdefg\")\n";
    let c = compile("override_refinement", src);

    let run = |args: &[&str]| -> String {
        let out = Command::new(&c.exe)
            .args(args)
            .current_dir(&c.dir)
            .output()
            .expect("failed to run compiled program");
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    // 7 characters: within the compiled-in default of 10 …
    assert!(
        run(&[]).contains("accepted"),
        "the predicate must accept a name within the default limit",
    );
    // … but not within a runtime override of 5.
    assert!(
        run(&["--maxLen=5"]).contains("rejected"),
        "the predicate must see --maxLen=5, not the compile-time 10",
    );
}

/// A constant annotated with an *alias* of a scalar (`type Host = Text`) was
/// not overridable: the check that decides which constants get a
/// `knot_override_lookup` call matched the rendered type string, which for an
/// alias is the alias name (`"Host"`), not `"Text"`. The constant compiled to
/// its default with no argv check at all, so `--host=…` was accepted (the
/// runtime ignores unknown flags) and silently discarded, and the constant was
/// missing from `--help`. The type string is now resolved through the alias
/// table, matching what body-less constants already did.
#[test]
fn a_constant_typed_by_an_alias_of_a_scalar_is_overridable() {
    let src = "type Host = Text\n\
               type Port = Int\n\
               host : Host\n\
               host = \"localhost\"\n\
               port : Port\n\
               port = 8080\n\
               main = println (host ++ \":\" ++ show port)\n";
    let c = compile("override_alias", src);

    let run = |args: &[&str]| -> String {
        let out = Command::new(&c.exe)
            .args(args)
            .current_dir(&c.dir)
            .output()
            .expect("failed to run compiled program");
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    assert!(
        run(&[]).contains("localhost:8080"),
        "the compiled-in defaults must still apply with no flags",
    );
    assert!(
        run(&["--host=example.com", "--port=9999"]).contains("example.com:9999"),
        "an alias-typed constant must take its value from the CLI override",
    );

    // `--help` (written to stderr) lists them under the base type the flag parses as.
    let help = Command::new(&c.exe)
        .arg("--help")
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    let help = String::from_utf8_lossy(&help.stderr).into_owned();
    assert!(
        help.contains("--host") && help.contains("Text"),
        "an alias-typed constant must be listed in --help under its base type, got: {help}",
    );
}

/// The counterpart to the above: resolving a constant's type through the alias
/// table must NOT sweep in *refined* aliases. Nothing on that path runs the
/// predicate, so honouring `--limit=-5` for a `Nat` would smuggle a value in
/// through the check the type promises. `limit` keeps its checked value.
/// (`base` is body-less, so it takes the refined path, which does validate.)
#[test]
fn a_constant_typed_by_a_refined_alias_is_not_overridable() {
    let src = "type Nat = Int where \\x -> x >= 0\n\
               base : Nat\n\
               limit : Nat\n\
               limit = base\n\
               main = println (\"limit=\" ++ show limit)\n";
    let c = compile("override_refined_alias", src);

    let out = Command::new(&c.exe)
        .args(["--base=7", "--limit=-5"])
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("limit=7"),
        "--limit=-5 must not bypass Nat's `>= 0` predicate, got: {stdout}",
    );
}

// ── 12. Refinements nested inside route body fields ───────────────

/// Only a body field's *own* top-level type was validated, so a refined type
/// reached through a list or a record — `events: [Ev]` where `Ev` carries a
/// `PubkeyHex` — was decoded straight into the handler unchecked. The runtime
/// now walks a path (`events[].pubkey?`) into the decoded body.
#[test]
fn refinements_nested_in_route_body_fields_are_validated() {
    let port = free_port();
    let src = format!(
        "type PubkeyHex = Text where \\s -> length s == 4\n\
         type Ev = {{pubkey: Maybe PubkeyHex, tags: [PubkeyHex], note: Text}}\n\
         route API where\n\
         \x20 /gossip\n\
         \x20   POST {{events: [Ev], top: PubkeyHex}} / -> {{ok: Bool}} = Gossip\n\
         srv = serve API where\n\
         \x20 Gossip = \\r -> yield (Ok {{value: {{ok: True {{}}}}}})\n\
         main = listen {port} srv\n"
    );
    let _server = start_server("nested_refinements", &src, port);

    let ok = r#"{"events":[{"pubkey":"abcd","tags":["wxyz"],"note":"n"}],"top":"abcd"}"#;
    assert_eq!(post_json(port, "/gossip", ok).0, 200, "a fully valid body must be accepted");

    let bad_nested = r#"{"events":[{"pubkey":"TOOLONG","tags":[],"note":"n"}],"top":"abcd"}"#;
    let (status, body) = post_json(port, "/gossip", bad_nested);
    assert_eq!(status, 400, "a refined field nested in a list must be validated");
    assert!(
        body.contains("events[].pubkey"),
        "the error must name the nested path, got: {body:?}",
    );

    let bad_in_nested_list = r#"{"events":[{"pubkey":"abcd","tags":["abcd","BAD"],"note":"n"}],"top":"abcd"}"#;
    assert_eq!(
        post_json(port, "/gossip", bad_in_nested_list).0,
        400,
        "every element of a nested refined list must be validated",
    );

    // A `Nothing` has no value to check, so it is vacuously valid.
    let absent = r#"{"events":[{"pubkey":null,"tags":[],"note":"n"}],"top":"abcd"}"#;
    assert_eq!(post_json(port, "/gossip", absent).0, 200, "a Nothing must not be rejected");

    // The top-level check that already worked must keep working.
    let bad_top = r#"{"events":[],"top":"BAD"}"#;
    assert_eq!(post_json(port, "/gossip", bad_top).0, 400, "a top-level refined field");
}

// ── 13. Trait default-body placeholder overwrites real signature ────

/// A trait method with both a signature and a default body caused the
/// placeholder entry (TypeKind::Hole) to overwrite the real signature,
/// pinning the method to a single unquantified type variable. The first
/// call site bound it; the second failed with "type mismatch".
#[test]
fn trait_default_body_does_not_pin_method_type() {
    let src = r#"trait Greet a where
  greet : a -> Text
  greet x = "hi"
data Dog = Dog {}
data Cat = Cat {}
impl Greet Dog where
impl Greet Cat where
main = do
  println (greet (Dog {}))
  println (greet (Cat {}))
  yield {}
"#;
    let c = compile("trait_default_monomorphic", src);
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("\"hi\"") && stdout.matches("\"hi\"").count() == 2,
        "both impls should use the default body, got: {stdout}",
    );
}

/// `deriving` for a trait with a default body must work at multiple types.
#[test]
fn deriving_works_at_multiple_types() {
    let src = r#"trait Describe a where
  describe : a -> Text
  describe x = "value: " ++ show x
data Priority = Low {} | High {} deriving (Describe)
data Color = Red {} | Blue {} deriving (Describe)
main = do
  println (describe (Low {}))
  println (describe (Red {}))
  yield {}
"#;
    let c = compile("deriving_multi_type", src);
    let out = Command::new(&c.exe)
        .current_dir(&c.dir)
        .output()
        .expect("failed to run compiled program");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Low") && stdout.contains("Red"),
        "both derived impls should work, got: {stdout}",
    );
}
