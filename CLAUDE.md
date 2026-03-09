# Knot

Knot is a functional relational programming language. Relations are the primary data structure, computation is pure and functional, and state is automatically persisted to SQLite.

See `DESIGN.md` for the full language specification.

## Project Structure

Cargo workspace with three crates:

```
crates/
  knot/              Frontend library (lexer, parser, AST, diagnostics)
  knot-runtime/      Rust staticlib linked into compiled programs (value representation, SQLite persistence)
  knot-compiler/     Cranelift-based compiler producing native executables (CLI binary: knotc)
examples/            Example .knot programs
```

## Build & Test

```sh
cargo build                  # Build all crates
cargo test                   # Run all tests (230 parser/lexer tests)
cargo test -p knot           # Run only frontend tests
```

## Compiling Knot Programs

```sh
cargo run -p knot-compiler -- build examples/hello.knot
./examples/hello
```

The compiler (`knotc`) looks for `libknot_runtime.a` next to its own executable. In a cargo workspace, both end up in `target/<profile>/`, so this works automatically. Override with `KNOT_RUNTIME_LIB=/path/to/libknot_runtime.a`.

Compiled binaries create a `knot.db` SQLite database in the current directory for persistence.

## Architecture

### Compilation Pipeline

```
source.knot → Lexer → Tokens → Parser → AST → Type Resolution → Cranelift IR → .o → cc link → executable
```

### Frontend (`crates/knot/`)

- **Lexer** (`lexer.rs`): Tokenizer with layout-sensitive newline handling
- **Parser** (`parser.rs`): Recursive-descent with Pratt expression parsing for operator precedence
- **AST** (`ast.rs`): Spanned AST nodes for all language constructs
- **Diagnostics** (`diagnostic.rs`): Error reporting with source locations

Public API:
```rust
let lexer = knot::lexer::Lexer::new(&source);
let (tokens, lex_diags) = lexer.tokenize();
let parser = knot::parser::Parser::new(source, tokens);
let (module, parse_diags) = parser.parse_module();
```

### Runtime (`crates/knot-runtime/`)

Compiled as a `staticlib` (with `rlib` for workspace dependency resolution). All functions use `extern "C"` ABI, called by Cranelift-generated code via symbol references.

- All Knot values are `*mut Value` (heap-allocated tagged enum)
- `rusqlite` with `bundled` feature statically links SQLite into every compiled binary
- Source relations map to SQLite tables with schema derived from Knot types
- Schema descriptor format: `"name:text,age:int"` (passed as string constants from generated code)

### Compiler (`crates/knot-compiler/`)

- **Type resolution** (`types.rs`): Resolves aliases, computes SQLite schemas from Knot type annotations
- **Codegen** (`codegen.rs`): Cranelift IR generation — the `build_function` pattern moves `ctx`/`builder_ctx` out of `self` to avoid borrow conflicts while allowing `self.method()` calls during IR building
- **Linker** (`linker.rs`): Invokes `cc` with platform-appropriate flags
- **CLI** (`main.rs`): `knotc build <file.knot>` entry point

Key codegen patterns:
- All values are pointer-typed (`ptr_type`) in Cranelift IR — uniform representation
- `do` blocks compile to nested loops with SSA block params for iteration counters
- `where` clauses become conditional branches; skip blocks jump to the loop's continue block
- Lambdas compile as separate functions; free variables captured in a record-valued closure environment
- Runtime functions are pre-declared as imports; `call_rt`/`call_rt_void` helpers emit calls
- `knot_relation_len` returns raw `usize`, not a boxed `Value` — use directly as loop bound

## Supported Language Features

Currently compiled: source declarations, type aliases, data declarations, functions, literals, records, field access, record updates, relation literals, binary/unary operations, if/else, do blocks (bind/where/yield/let), set expressions, lambdas, closures, function application, case expressions, constructors, atomic transactions, migrations (schema tracking + `migrate` blocks), schema lockfile (`<name>.schema.lock`), built-ins (println, print, show, union, count).

Not yet implemented: traits, impls, views, derived relations, routes, temporal queries, pattern matching in do-bind (constructor patterns filter but don't branch), partial application.

## Version Control

This repository uses **jujutsu (jj)**, not git. Use `jj` commands for all VCS operations.
