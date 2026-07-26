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

A Knot file is a single expression. Declarations go in a `with { ... }`
block; the trailing expression is the program body.

```knot
with {
type Person = {name: Text, age: Int 1}

*people : [Person]
}
(do
  replace *people = [{name "Alice" age 30}, {name "Bob" age 25}, {name "Carol" age 35}]
  seniors <- (do
    p <- *people
    where p.age > 27
    yield p)
  base.println ("Senior count: " ++ base.show (base.count seniors))
  base.forEach seniors (\p -> base.println ("  " ++ p.name))
  yield {})
```

`*people` is a *source relation* — declared with a type but no body, so
it's persisted to SQLite on first run. The inner `do ... where ... yield`
query compiles to a single `SELECT ... WHERE age > 27`. `Int 1` is an `Int`
with a (dimensionless) unit of measure — every numeric type carries a unit.

## Learn Knot in Y Minutes

A whirlwind tour. Everything below is real, runnable Knot — paste any block
into a `.knot` file and `knot build` it. (Single-page cheat-sheet style,
after [learnxinyminutes](https://learnxinyminutes.com/).)

### The shape of a program

```knot
-- A comment starts with `--`.
-- A file is ONE expression. Declarations live in `with { ... }`;
-- the expression after the closing `}` is the body (usually a `do` block).
with {
answer 42
}
(base.println (base.show answer))   -- prints "42"
```

There are no statements, only expressions. `with { ... } body` brings the
declarations into scope for `body`. Indentation is significant (layout), and
application is by juxtaposition: `f x y`, with parens to group.

### Values and primitive types

```knot
with {
anInt    42               -- Int 1  (unit is mandatory; `1` = dimensionless)
aFloat   3.14             -- Float 1
aText    "hello"          -- Text
aBool    true             -- Bool (also `false`)
aRecord  {name "Ada" age 36}          -- records: space-separated fields
aUnit    {}               -- the empty record / unit value
}
(base.println aText)
```

Numbers carry **units of measure**. `Int 1` is dimensionless; you can declare
`unit Ms` and write `250 Ms`. Field access uses a dot: `aRecord.name`.

### Relations — the core data structure

A *relation* is an unordered set of values, written with `[ ... ]`. It plays
the role other languages give to lists/arrays.

```knot
with {
nums   [1, 2, 3, 4]
empty  []                       -- the empty relation
people [{name "Ada" age 36}, {name "Grace" age 17}]
}
(do
  base.println (base.show (base.count nums))        -- "4"
  base.println (base.show (base.map (\n -> n * 2) nums))  -- "[2, 4, 6, 8]"
  base.println (base.show (base.filter (\n -> n % 2 == 0) nums))
  base.println (base.show (base.fold (\a b -> a + b) 0 nums))  -- "10"
  yield {})
```

### Functions

```knot
with {
-- name (\\arg1 arg2 -> body)
add (\a b -> a + b)

-- optional type signature on the line above
safeDiv : Int 1 -> Int 1 -> Maybe (Int 1)
safeDiv (\x y -> case y == 0 of
  true  -> Maybe.Nothing {}
  false -> Maybe.Just {value (x / y)})
}
(do
  base.println (base.show (add 2 3))         -- "5"
  base.println (base.show (safeDiv 10 2))    -- "Just {value: 5}"
  base.println (base.show (safeDiv 10 0))    -- "Nothing {}"
  yield {})
```

Lambdas are `\x y -> body`. Functions are values; `add 2 3` is just
application. There are no named `def`s with bodies — a "named function" is a
plain binding whose value happens to be a lambda.

### Refined types

A refined type is a base type plus a predicate — `T where \x -> ...`. The
predicate is an ordinary pure function. `Refined(T) <: T`, so a refined value
flows anywhere its base type is expected (no check). Going the other way
(base → refined) is where the check happens:

```knot
with {
type Nat = Int 1 where \x -> x >= 0
-- stacking: Age is Nat further restricted — predicates conjoin (>= 0 && <= 150)
type Age = Nat where \x -> x <= 150

greet : Age -> Text
greet (\a -> "age " ++ base.show a)

double (\x -> x + x)
}
(do
  base.println (greet 30)                 -- OK: 30 is a compile-time constant,
                                          -- the compiler checks it satisfies
                                          -- 0 <= 30 <= 150 and lets it through.
  case refine (double 21) of              -- a runtime value needs `refine`
    Result.Ok {value a} -> base.println (greet a)   -- "age 42"
    Result.Err {error e} -> base.println "not a valid age"
  yield {})
```

- A **compile-time constant** (`greet 30`, or a `with`-bound literal like
  `drinkingAge 21`) is checked against the predicate *at compile time* — no
  `refine` needed, and a constant that fails the predicate is a **build
  error** naming the value.
- A **runtime value** must go through `refine`, which returns
  `Result RefinementError T` — handle the `Err` case.
- Refinements on **source-relation fields** are checked row-by-row on every
  write, and route bodies are validated on decode (HTTP 400 on violation).

### Conditionals are `case` on a Bool

There is **no `if`**. Pattern matching with `case ... of` is the only
branch, and a Bool scrutinee gives you if/else:

```knot
with {
sign (\n -> case n < 0 of
  true  -> (0 - 1)
  false -> case n > 0 of
    true  -> 1
    false -> 0)
}
(base.println (base.show (sign (0 - 5))))   -- "-1"
```

Arms are separated by newlines (or `;` on one line). Matching is exhaustive —
the compiler enforces that you cover every constructor.

### ADTs and pattern matching

```knot
with {
data Shape
  = Circle {radius: Float 1}
  | Rect {width: Float 1, height: Float 1}

area (\s -> case s of
  Shape.Circle {radius r}      -> 3.14159 * r * r
  Shape.Rect {width w height h} -> w * h)
}
(base.println (base.show (area (Shape.Circle {radius 2.0}))))  -- "12.56636"
```

Constructors carry a record of fields. You destructure them in a `case` arm.
Built-in ADTs include `Bool` (`True {}`/`False {}`), `Maybe a`
(`Nothing {}`/`Just {value}`), and `Result e a` (`Err {error}`/`Ok {value}`).

### Recursion over a relation

```knot
with {
sumList (\xs -> case xs of
  []        -> 0
  Cons h t  -> h + sumList t)
}
(base.println (base.show (sumList [1, 2, 3, 4])))   -- "10"
```

`[]` matches the empty relation and `Cons h t` splits off one element `h`
and the rest `t` — this is how you iterate structurally.

### `do` notation and `Maybe`

`do` sequences monadic steps. Over `Maybe` it short-circuits on `Nothing`;
`where` filters (it desugars to `empty` on `false`):

```knot
with {
safeDiv (\x y -> case y == 0 of
  true  -> Maybe.Nothing {}
  false -> Maybe.Just {value (x / y)})

compute (do
  a <- safeDiv 20 4     -- a = 5
  b <- safeDiv a 5      -- b = 1
  where b > 0           -- guard; Nothing if false
  yield (a + b))        -- Just {value: 6}
}
(base.println (base.show compute))
```

`<-` binds, `where` guards, `yield` produces the result. The same `do`
notation is used for relations (a comprehension) and for IO (sequencing
effects).

### Persisted relations and queries

Prefix a name with `*` to declare a **source relation** — state that
auto-persists to SQLite. Reads/writes are IO, so they happen inside the main
`do` block:

```knot
with {
type Todo = {title: Text, done: Int 1}

*todos : [Todo]
}
(do
  replace *todos = [{title "write guide" done 0}, {title "ship it" done 0}]

  -- a query comprehension: binds each row, filters, yields results
  open <- (do
    t <- *todos
    where t.done == 0
    yield t)
  base.println ("open: " ++ base.show (base.count open))

  -- update: bind the rows to a local, then map, marking one done
  -- ({t | done 1} is a record update)
  todos <- *todos
  *todos = (do
    t <- todos
    yield (case t.title == "write guide" of
      true  -> {t | done 1}
      false -> t))
  yield {})
```

`replace *todos = ...` overwrites; `*todos = ...` assigns a computed value.
Comprehensions over a source push down to SQL (`WHERE`, joins, aggregates
like `count`/`sum`/`avg`, `sortBy`). A `<name>.schema.lock` file tracks the
schema across runs for automatic migration.

### Effects are tracked

Every function's type records what it can do. `base.println` returns
`IO {console} {}`; reading a file is `IO {fs} Text`; the DB operations are
`IO {} ...`. An `atomic` block (a transaction) is typed `IO {} a -> IO {} a`,
so the compiler *rejects* a `println` inside it. You don't write these
annotations — they're inferred.

### Handy builtins (the `base.` namespace)

```knot
(do
  base.println (base.toUpper "knot")            -- "KNOT"
  base.println (base.show (base.length "hi"))   -- "2"
  base.println (base.show (base.take 2 [9, 8, 7]))  -- "[9, 8]"
  base.println (base.show (base.sum [1, 2, 3]))     -- "6"
  base.println (base.show (base.min 3 7))           -- "3"
  base.println (base.show (base.max 3 7))           -- "7"
  yield {})
```

Also: `map`, `filter`, `fold`, `count`, `countWhere`, `avg`, `minOn`,
`maxOn`, `sortBy`, `head`, `findFirst`, `union`, `inter`, `diff`, `reverse`,
`contains`, `trim`, `chars`, `forEach`, `when`, `unless`, `not`, plus JSON,
hashing, UUIDs, file I/O, crypto, and leveled logging. See
[stdlib.md](stdlib.md) for the full list.

### Where to go next

- [DESIGN.md](DESIGN.md) — the full language specification (routes/HTTP,
  refined types, units, concurrency with `fork`/`race`/`atomic`, modules,
  schema migration, subset constraints).
- [stdlib.md](stdlib.md) — every builtin with its type.
- [examples/](examples/) — runnable programs for each feature.

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
