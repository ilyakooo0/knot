# Knot

<p align="center">
  <img src="logo.png" width="300px" align="center">
</p>

Knot is a functional relational programming language. Relations are the
primary data structure, computation is pure and functional, and state is
automatically persisted to SQLite. Effects are inferred and tracked in the
type system, so the compiler always knows what each function reads, writes,
or talks to over the network.

## Quick Start

```sh
# Build the compiler and runtime
cargo build

# Compile and run a Knot program
cargo run -p knot-compiler -- build examples/hello.knot
./examples/hello
```

Compiled binaries create a `<name>.db` SQLite file in the working directory
for persistence and a `<name>.schema.lock` file that records the schema for
migration tracking.

## Documentation

- [Language Design](DESIGN.md) — full language specification
- [Standard Library](stdlib.md) — built-in functions, traits, and types

## A Taste

```knot
type Person = {name: Text, age: Int}

*people : [Person]

main = do
  replace *people = [
    {name: "Alice", age: 30},
    {name: "Bob",   age: 25},
    {name: "Carol", age: 35}
  ]
  people <- *people
  with {seniors: do
    p <- people
    where p.age > 27
    yield p}
  (do
    println ("Senior count: " ++ show (count seniors))
    forEach seniors (\p -> println ("  " ++ p.name))
    yield {})
```

`*people` is a *source relation* — declared with a type but no body, so
it's persisted to SQLite on first run. The `with {seniors: do ...}` block is
a query expression that compiles to a single `SELECT ... WHERE age > 27`
against the auto-indexed `age` column.

## What's in the box

The compiler is a Cranelift backend producing native executables linked
against a Rust runtime. Most of the language is implemented and demonstrated
under [examples/](examples/):

**Relations and queries.** Source relations (`*name : [T]`), pure
expression-bindings, and derived relations (`&name = ...`) compose through
`do`-notation. Comprehension queries push down to SQL when they can —
`filter`, `map`, `count`, `countWhere`, `sum`, `avg`, `minOn`, `maxOn`,
multi-table joins, and `sortBy` all become SELECT statements with
auto-indexed WHERE/ORDER BY columns. See `examples/query_opt.knot`,
`examples/inline_pushdown.knot`, `examples/let_pushdown.knot`.

**ADTs and pattern matching.** Sum types are first-class — `[Shape]` holds
circles and rects in one table. Constructor patterns work in `case` and in
`do`-bind (`Circle c <- *shapes` filters and destructures). Built-ins
include `Bool`, `Maybe`, and `Result`. See `examples/maybe.knot`,
`examples/result.knot`, `examples/cons_pattern.knot`.

**Traits and HKT.** Single-dispatch traits with default methods, deriving,
and supertraits. Higher-kinded type parameters let you write `Functor`,
`Applicative`, `Monad`, `Alternative` once and instantiate per type.
Associated types are supported. See `examples/traits.knot`,
`examples/associated_types.knot`.

**Type inference.** Hindley-Milner with row-polymorphic records and
variants, let-generalization, trait-bound checking, and unit polymorphism.

**IO effects.** Every function carries an effect row in its type:
`IO {console, fs} Text`, `IO {network | r} {}`, etc. Atomic blocks are
typed `IO {} a -> IO {} a` so the compiler rejects `println` inside a
transaction. See `examples/log_test.knot`.

**Concurrency.** `fork`, `race`, and STM-style `atomic` blocks with
`retry`. Row-level read-filter wakeups mean a watcher on `WHERE id = 1`
isn't woken by writes to `id = 2`. See `examples/race.knot`,
`examples/stm_row_filter.knot`.

**HTTP routes and serving.** `route Api where ... = Endpoint` declarations
define endpoints by ADT constructor. `serve Api where E = handler`
type-checks every handler against the declared method/path/body/query/
headers/response, and `listen 8080 api` runs a `tiny_http` server.
`fetch url (Endpoint {...})` is a type-safe client that reuses route
declarations. Per-route rate limiting is built in. See
`examples/routes.knot`.

**Refined types.** `type Port = Int where \p -> p > 0 && p < 65536` is a
nominal type whose predicate is checked at boundaries — relation writes,
HTTP body decoding, and explicit `refine expr`. Route handlers auto-return
HTTP 400 on validation failure. See `examples/required_args.knot`.

**Units of measure.** `unit Ms`, `unit Usd`, `unit N = Kg * M / S^2`.
Numeric literals carry units via `42.0 M` and `(expr : Int Ms)`. The
compiler checks unit consistency through arithmetic. Most stdlib numeric
functions are unit-polymorphic — `sleep` takes `Int Ms`, `now` returns
`Int Ms`, `randomInt 100 Usd` returns `Int Usd`.

**Schema evolution.** A `<name>.schema.lock` file records the persisted
schema. Adding nullable fields or ADT variants auto-updates; breaking
changes require a `migrate` block.

**Constraints.** Subset constraints (`*orders.customer <= *people.name`)
enforce referential integrity and uniqueness at write time. See
`examples/constraints.knot`.

**Modules.** `import ./types` brings in another file's `export`ed
declarations. See `examples/modules/`.

**Other goodies.** Bytes and hex encoding, BLAKE3 hashing, UUIDv7,
JSON encode/decode, file I/O, leveled logging (`logInfo`/`logWarn`/...),
crypto (`generateKeyPair`/`encrypt`/`sign`/`verify`).

**Runtime CLI.** Every compiled program accepts a common set of flags and
subcommands for free:

```sh
./my_program                     # run main
./my_program --debug             # turn on logDebug output
./my_program --help              # print usage + any compile-time overrides
./my_program --http-max-body-bytes=32M
./my_program db                  # browse the .db file in a TUI
./my_program api MyRouteName     # print OpenAPI 3.0 spec for a `route`
./my_program --my-flag=value     # override a compile-time constant
```

Constant overrides can also be supplied at build time
(`knot build foo.knot --my-flag=value`). The compiler also ships with
`knot fmt [--check] [--stdout] <file.knot>` for in-place formatting.

## Project Structure

```
crates/
  knot/              Frontend library: lexer, parser, AST, diagnostics
  knot-runtime/      Rust staticlib linked into compiled programs (values,
                     SQLite persistence, concurrency, HTTP, crypto)
  knot-compiler/     Cranelift compiler producing native executables;
                     CLI binary is `knot`
  knot-lsp/          Language server for editor integration (binary `knot-lsp`)
examples/            Example .knot programs
DESIGN.md            Language specification
stdlib.md            Standard library reference
```

## Tests

```sh
cargo test                   # All tests
cargo test -p knot           # Frontend (parser/lexer) only
cargo test -p knot-compiler  # Inference, codegen, etc.
```
