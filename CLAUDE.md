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
source.knot → Lexer → Tokens → Parser → AST → Desugar → Type Inference → Effect Inference → Type Resolution → Cranelift IR → .o → cc link → executable
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
- Schema descriptor format for records: `"name:text,age:int"` (passed as string constants from generated code)
- Schema descriptor format for direct ADT relations: `"#Circle:radius=float|Rect:width=float;height=float"` — `#` prefix signals ADT schema, `|` separates constructors, `:` separates name from fields, `;` separates fields, `=` separates field name from type; runtime creates wide table with `_tag TEXT` + all constructor fields as nullable columns
- Column type `tag` for enum-like ADT fields (all nullary constructors): stored as TEXT in SQLite, reconstructed as `Constructor(tag, Unit)` on read
- Automatic indexing: the runtime observes query patterns and creates indexes lazily — ADT tables get a `_tag` index at init; columns in `DELETE WHERE` and `UPDATE WHERE` clauses are auto-indexed on first use; `KnotDb` tracks created indexes in a per-session `HashSet` to avoid redundant DDL; uses `CREATE INDEX IF NOT EXISTS` for cross-session idempotency
- Nested relation fields (`[T]` inside records): stored in **child tables** (`_knot_{source}__{field}`) with `_parent_id` FK linking to parent's `_id`; parent tables with nested children get `_id INTEGER PRIMARY KEY AUTOINCREMENT`; schema descriptor format `field:[child_schema]` (bracket-aware parsing via `split_respecting_brackets`); deeply nested relations recurse arbitrarily

### Compiler (`crates/knot-compiler/`)

- **Desugaring** (`desugar.rs`): Transforms "pure comprehension" do blocks into nested calls to `__bind`, `__yield`, and `__empty`. A pure comprehension is a do block with only Bind/Where/Let/Yield statements (no bare side-effecting expressions or `groupBy`). Do blocks that are direct values of `set`/`full set` expressions are NOT desugared (preserves SQL optimization patterns). Do blocks containing `groupBy` are NOT desugared (require loop-based codegen for the collect/group/re-iterate pattern). Mixed/sequential do blocks (e.g. `main = do { println ...; yield {} }`) remain as `Do(stmts)` nodes for imperative codegen. The desugared `__bind`/`__yield`/`__empty` dispatch through Monad/Applicative/Alternative trait impls — the monad type is resolved at compile time via HKT unification (`monad_info` maps spans to `MonadKind::Relation` or `MonadKind::Adt(name)`), and codegen calls the appropriate `Monad_{type}_bind`, `Applicative_{type}_yield`, or `Alternative_{type}_empty` mangled function. This allows `do` blocks to work with any type implementing the Monad/Applicative/Alternative traits, not just `[]`.
- **Type inference** (`infer.rs`): Hindley-Milner type inference with row-polymorphic records, let-generalization, unification with occurs check, ADT constructor typing, trait method registration, associated type erasure, higher-kinded type polymorphism (`TyCon`/`App` type nodes with normalization-based unification), and trait bound checking (explicit `TraitName a =>` constraints on function signatures are validated at call sites against known `impl` declarations; trait method calls automatically carry their trait's constraint)
- **Effect inference** (`effects.rs`): Infers per-declaration effects (`{reads *rel}`, `{writes *rel}`, `{console}`, `{clock}`, etc.), checks IO-in-atomic constraints, validates explicit effect annotations against inferred effects
- **Type resolution** (`types.rs`): Resolves aliases (including multi-variant ADTs to `ResolvedType::Adt`), computes SQLite schemas from Knot type annotations, collects subset constraints
- **Codegen** (`codegen.rs`): Cranelift IR generation — the `build_function` pattern moves `ctx`/`builder_ctx` out of `self` to avoid borrow conflicts while allowing `self.method()` calls during IR building
- **Linker** (`linker.rs`): Invokes `cc` with platform-appropriate flags
- **CLI** (`main.rs`): `knotc build <file.knot>` entry point

Key codegen patterns:
- All values are pointer-typed (`ptr_type`) in Cranelift IR — uniform representation
- `do` blocks compile to nested loops with SSA block params for iteration counters
- `where` clauses become conditional branches; skip blocks jump to the loop's continue block
- Functions are constants bound to lambdas (`add = \x y -> x + y`); when a Fun's body is a Lambda, the compiler extracts its params for direct Cranelift function compilation (no closure overhead)
- Standalone lambdas compile as separate functions; free variables captured in a record-valued closure environment; multi-param lambdas (`\a b c -> body`) are curried into nested single-param lambdas at compile time
- Runtime functions are pre-declared as imports; `call_rt`/`call_rt_void` helpers emit calls
- `knot_relation_len` returns raw `usize`, not a boxed `Value` — use directly as loop bound
- Trait impl methods compile as mangled functions (`TraitName_TypeName_methodName`); a dispatcher function checks `knot_value_get_tag` at runtime and calls the matching impl; missing impls panic with a clear error message (except operator-mapped methods like `eq`/`compare`/`add`/`sub`/`mul`/`div`/`negate` which fall back to runtime functions for types without explicit impls)
- Operator trait dispatch: arithmetic operators (`+`, `-`, `*`, `/`) dispatch through `Num` trait methods (`add`, `sub`, `mul`, `div`); unary negation dispatches through `Num.negate`; `==` dispatches through `Eq.eq`; `!=` dispatches through `Eq.eq` then negates; comparison operators (`<`, `>`, `<=`, `>=`) dispatch through `Ord.compare` and check the resulting `Ordering` constructor tag; `&&`/`||`/`++` remain direct runtime calls (no trait). Primitive impls (Int, Float, Text, Bool) are registered as intrinsic codegen impls that delegate to runtime functions, avoiding circular dependencies (the prelude source does NOT contain these impls)
- Default trait methods: if an impl omits a method with a default body, the default is auto-compiled for that type
- `deriving (TraitName)` on data types auto-generates impls using the trait's default method bodies
- `show` calls `knot_value_show` (converts any value to Text representation)
- Standard library functions are registered as 1-param user_fns with currying: 1-param functions delegate directly to runtime; 2-param functions (filter, match, map, take, drop, contains) curry via `define_stdlib_fn_2` — outer function captures first arg in a closure env, inner closure extracts it and calls the runtime; 3-param functions (fold) use double currying via `define_stdlib_fn_3`
- Recursive derived relations: detected via `expr_contains_derived_ref` AST walk; compile to a body function `knot_user_<name>_body(db, self_val)` where self-references read from `self_val` param (via `__derived_self_<name>` env key), plus a wrapper that calls `knot_relation_fixpoint(db, body_fn_ptr, empty)` for fixed-point iteration
- `groupBy` in do-blocks: compiles as a two-phase loop — phase 1 collects qualifying rows into a temp relation (inside existing loops), closes those loops, then calls `knot_relation_group_by(db, temp, schema, key_cols)` which uses SQLite for grouping: inserts row indices + key column values into a temp table, executes `ORDER BY` on key columns, and groups consecutive rows with matching keys in O(n); phase 2 opens a new loop over the groups and rebinds the primary bind variable to each group (a sub-relation); field access on groups delegates to the first element via `knot_record_field`'s `Value::Relation` handling; codegen extracts key column names from the record key expression and schema from `source_schemas`

## Supported Language Features

Currently compiled: source declarations, type aliases, data declarations, functions, literals, records, field access, record updates, relation literals, nested relations (record fields holding `[T]` stored as JSON in SQLite, queryable via `m <- t.field` bind), binary/unary operations, if/else, do blocks (bind/where/yield/let/groupBy — pure comprehension do blocks are desugared to `__bind`/`__yield`/`__empty` calls that dispatch through Monad/Applicative/Alternative trait impls — works with any monad, not just `[]` (e.g., `Maybe`); monad type resolved at compile time via HKT unification; mixed/sequential do blocks use direct loop codegen; set-value do blocks preserved for SQL optimization; `groupBy {key}` collects rows, groups by key, and rebinds the variable to each group sub-relation — field access on a group returns the first element's field, aggregates like `count` operate on the group), set expressions, lambdas, closures, function application, case expressions (with exhaustiveness checking for ADTs), constructors, atomic transactions, migrations (schema tracking + `migrate` blocks), schema lockfile (`<name>.schema.lock`), views (`*view = do { ... yield {...} }` with bidirectional read/write, constant column filtering, auto-fill on write), derived relations (`&name = expr` — read-only computed relations, compiled as 0-param functions recomputed on each access; recursive derived relations with self-references use Datalog-style fixpoint iteration via `knot_relation_fixpoint` runtime function, converging when the relation stabilizes), traits and impls (single-dispatch on runtime type tags for primitives and ADTs, default methods, deriving from defaults, supertrait enforcement, associated types with type-level pattern matching), trait bounds (`Display a => [a] -> [Text]` syntax — explicit constraints on function type signatures validated at call sites; trait method calls automatically carry constraints; multiple bounds via `Trait1 a => Trait2 a => Type`; compile-time error when a concrete type lacks a required impl), type inference (Hindley-Milner with row polymorphism, let-generalization, ADT/trait/view-aware, higher-kinded type polymorphism), higher-kinded types (trait params with kind annotations like `(f : Type -> Type)`, type-level application `f a` in method signatures, `[]` and ADT names as bare type constructors in impl args, `TyCon`/`App` internal representation with normalization-based unification), temporal queries (`*rel : [T] with history` enables history tracking, `*rel @(timestamp)` queries past state, `now` returns current time in ms), pattern matching in do-bind (`Circle c <- *shapes` filters relation to matching constructors and destructures; `InProgress ip <- t.status` matches a single value and skips if no match), built-ins (println, print, show, union, count, now, filter, match, map, fold, single, diff, inter, sum, avg, toUpper, toLower, take, drop, length, trim, contains, reverse, chars, id, not, toJson, parseJson, readFile, writeFile, appendFile, fileExists, removeFile, listDir), effect inference (per-declaration effect tracking for reads/writes/console/clock/network/fs/random, IO-in-atomic validation, explicit annotation checking), subset constraints (`*orders.customer <= *people.name` for referential integrity, `*users <= *users.email` for uniqueness — validated at runtime before each write commits), routes (`route Name where` with HTTP method/path/body/query/response declarations, path prefix nesting, `route Name = A | B` composition, desugared to ADTs with `respond` callback for per-branch response type safety, `listen port handler` starts `tiny_http` server with route table matching, JSON request/response serialization).

## Version Control

This repository uses **jujutsu (jj)**, not git. Use `jj` commands for all VCS operations.
