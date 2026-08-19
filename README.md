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
- [The `base` Namespace](base.md) — every standard-library function with its
  full path, type, and description
- [Standard Library](stdlib.md) — operators, built-in types, and units of
  measure

## A Taste

A Knot file is a single expression. Declarations go in a `with { ... }`
block; the trailing expression is the program body.

```knot
with {
type Person = {name: Text, age: Int 1}

*people : [Person]
}
(do
  full *people = [{name "Alice" age 30} {name "Bob" age 25} {name "Carol" age 35}]
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

There are no statements, only expressions. **Everything is an expression** — a
`case` arm, a `do` block, a function body, a whole program — and every
expression evaluates to a value. A Knot *file* is one big expression whose
value is what the program produces. `with { ... } body` is itself an
expression: it brings the declarations into scope for `body` and its value is
`body`'s value. Indentation is significant (layout), and application is by
juxtaposition: `f x y`, with parens to group.

Because everything is an expression, you can nest any of these wherever a value
is expected — a `do` block inside a `with` field, a `case` inside an argument,
a `with` inside another `with`.

### Values and primitive types

```knot
with {
anInt    42               -- Int 1  (unit is mandatory; `1` = dimensionless)
aFloat   3.14             -- Float 1
aText    "hello"          -- Text
aBool    (Bool.True {})     -- Bool (also `Bool.False {}`)
}
(base.println aText)
```

Numbers carry **units of measure**. `Int 1` is dimensionless; you can declare
`unit Ms` and write `250 Ms`.

### Records

A **record** groups named values into one value. Fields are space-separated —
no `:` and no `,` — and each field pairs a name with a value:

```knot
with {
point   {x 3 y 4}                  -- two Int fields
person  {name "Ada" age 36}        -- mixed field types
empty   {}                          -- the empty record (the unit value)
}
(base.println (base.show point))    -- "{x: 3, y: 4}"
```

You always *write* a record with spaces — `{x 3 y 4}`. The `{x: 3, y: 4}` above
is just how `show` *prints* a record (with `:` and `,`); that printed form is
not valid input syntax.

Read a field with a dot. Records nest, and access chains left to right:

```knot
with {
pt     {x 3 y 4}
person {name "Ada" addr {city "London" zip "E1"}}
}
(do
  base.println (base.show pt.x)              -- "3"
  base.println (base.show person.addr.city)  -- "London"
  yield {})
```

Records are immutable, but `base.unify` builds a new record by merging an old
one with some fields replaced or added — right-biased, so the second record's
fields win on a name conflict:

```knot
with {
pt    {x 3 y 4}
moved (base.unify pt {x 10})       -- like pt, but x is 10
}
(base.println (base.show moved))  -- "{x: 10, y: 4}"
```

`unify` can also add fields the base didn't have (`base.unify pt {z 5}` is
`{x: 3, y: 4, z: 5}`); see `unify` in the stdlib reference.

The empty record `{}` is the **unit** value — the "nothing interesting here"
result. A `do` block that only performs effects ends in `yield {}` to produce
it. Records are how `with` declarations, constructor payloads, and function
dictionaries are all shaped, so you'll see them everywhere.

### Relations — the core data structure

A *relation* is an unordered set of values, written with `[ ... ]`. It plays
the role other languages give to lists/arrays.

```knot
with {
nums   [1 2 3 4]
empty  []                       -- the empty relation
people [{name "Ada" age 36} {name "Grace" age 17}]
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
-- name (\arg1 arg2 -> body)
add (\a b -> a + b)

-- optional type signature on the line above
safeDiv : Int 1 -> Int 1 -> Maybe (Int 1)
safeDiv (\x y -> case y == 0 of
  Bool.True {} -> Maybe.Nothing {}
  Bool.False {} -> Maybe.Just {value (x / y)})
}
(do
  base.println (base.show (add 2 3))         -- "5"
  base.println (base.show (safeDiv 10 2))    -- "Just {value 5}"
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
  Bool.True {} -> (0 - 1)
  Bool.False {} -> case n > 0 of
    Bool.True {} -> 1
    Bool.False {} -> 0)
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
(base.println (base.show (sumList [1 2 3 4])))   -- "10"
```

`[]` matches the empty relation and `Cons h t` splits off one element `h`
and the rest `t` — this is how you iterate structurally.

### `do` notation and `Maybe`

`do` sequences monadic steps. Over `Maybe` it short-circuits on `Nothing`;
`where` filters (it desugars to `empty` on `false`):

```knot
with {
safeDiv (\x y -> case y == 0 of
  Bool.True {} -> Maybe.Nothing {}
  Bool.False {} -> Maybe.Just {value (x / y)})

compute (do
  a <- safeDiv 20 4     -- a = 5
  b <- safeDiv a 5      -- b = 1
  where b > 0           -- guard; Nothing if false
  yield (a + b))        -- Just {value 6}
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
  full *todos = [{title "write guide" done 0} {title "ship it" done 0}]

  -- a query comprehension: binds each row, filters, yields results
  open <- (do
    t <- *todos
    where t.done == 0
    yield t)
  base.println ("open: " ++ base.show (base.count open))

  -- update: bind the rows to a local, then map, marking one done
  -- (base.unify t {done 1} merges the row with the new field value)
  todos <- *todos
  *todos = (do
    t <- todos
    yield (case t.title == "write guide" of
      Bool.True {} -> (base.unify t {done 1})
      Bool.False {} -> t))
  yield {})
```

`full *todos = ...` overwrites; `*todos = ...` assigns a computed value.
Comprehensions over a source push down to SQL (`WHERE`, joins, aggregates
like `count`/`sum`/`avg`, `sortBy`). A `<name>.schema.lock` file tracks the
schema across runs for automatic migration.

### Query fields

A `with`-level binding whose value is a relation query (a comprehension, a
source, or a relation literal) is a **query field**: a read-only, computed view
over one or more source relations. It is typed as a plain relation `[T]` and is
never persisted — the query is re-evaluated on each access, so it always
reflects the current state of its inputs. Recursive and mutually-recursive
query fields are supported (transitive closures, etc.).

```knot
with {
*manages : [{manager: Text, report: Text}]

-- recomputed on every read; rows are projected from *manages
directReports (do
  m <- *manages
  yield {manager m.manager report m.report})
}
(do
  full *manages = [
    {manager "Alice" report "Bob"}
    {manager "Alice" report "Carol"}
    {manager "Bob" report "Dave"}
  ]
  rows <- base.run directReports
  base.println (base.show rows)
  base.println ("Total: " ++ base.show (base.count directReports))   -- "Total: 3"
  yield {})
```

See `examples/recursive.knot`.

### Required CLI arguments — signature-only constants

A `with`-level binding with a type signature but **no value** is a *required
command-line argument*. The compiled program reads `--name=value` at startup;
the value is parsed and checked against the declared type (including any
refinement predicate), and a missing or invalid argument aborts with a clear
error and `--help` output listing every required flag.

```knot
with {
type Port = Int 1 where \p -> p > 0 && p < 65536

-- no value on the right — these are required CLI args
port     : Port
host     : Text
greeting : Text
}
(do
  base.println (greeting ++ " from " ++ host ++ " on port " ++ base.show port))
```

```sh
$ ./program
Error: missing required argument --port
  pass --port=<value> on the command line, or run --help for usage

$ ./program --port=8080 --host=localhost --greeting=Hello
"Hello from localhost on port 8080"

$ ./program --port=99999 --host=x --greeting=y
Error: value supplied for --port does not satisfy 'Port' predicate
```

Only scalar-ish types (integers, floats, text, bools, and refinements of
those) can be CLI-overridden. See `examples/required_args.knot`.

### Implicit field projection — `^field` and dictionary constraints

`^field` is an *implicit reference*: it resolves to some field named `field`
found in an in-scope record, with the **expected type** disambiguating which
one. You don't name the record — the compiler searches lexical scope and picks
the field whose type unifies with what the context needs.

The canonical use is lightweight ad-hoc polymorphism: bundle functions in a
record (a "dictionary"), bring it into scope with `with`, and project
operations out with `(^field)`:

```knot
with {
intRender  {render (\n -> "int: " ++ base.show n)}
textRender {render (\s -> "text: " ++ s)}
}
(do
  with intRender  (base.println ((^render) 41))        -- "int: 41"
  with textRender (base.println ((^render) "hello"))   -- "text: hello"
  yield {})
```

`(^render)` resolves to whichever `render` field is lexically in scope and
type-compatible — `Int -> Text` at the first site, `Text -> Text` at the
second. If no in-scope field matches the expected type, you get a compile
error listing what was searched. See `examples/trait_replacement.knot` and
`examples/ord.knot`.

#### Dictionary constraints — `(^field : T) =>`

A function can *declare* that it needs a dictionary without naming it, using a
signature constraint `(^field : T) =>`. The compiler then resolves the
dictionary from lexical scope at each full-arity callsite and splices it in as
a hidden leading argument:

```knot
with {
intOrd     {compare (\a b -> case a > b of Bool.True {} -> 1; Bool.False {} -> case a < b of Bool.True {} -> (0 - 1); Bool.False {} -> 0)}
textOrd    {compare (\a b -> case a > b of Bool.True {} -> 1; Bool.False {} -> case a < b of Bool.True {} -> (0 - 1); Bool.False {} -> 0)}
intOrdDesc {compare (\a b -> case a < b of Bool.True {} -> 1; Bool.False {} -> case a > b of Bool.True {} -> (0 - 1); Bool.False {} -> 0)}

-- clamp needs a `compare` dictionary but doesn't name it
clamp : (^compare : a -> a -> Int 1) => a -> a -> a -> a
clamp (\lo hi x -> case ((^compare) x lo) < 0 of
  Bool.True {} -> lo
  Bool.False {} -> case ((^compare) x hi) > 0 of
    Bool.True {} -> hi
    Bool.False {} -> x)
}
(do
  base.println (base.show (with intOrd     (clamp 0 10 42)))   -- "10"
  base.println (base.show (with textOrd    (clamp "a" "m" "z"))) -- "m"
  base.println (base.show (with intOrdDesc (clamp 0 10 42)))   -- "0"
  yield {})
```

At each `clamp ...` callsite, the enclosing `with <dict>` supplies the record
whose `compare` field satisfies the constraint. This is the post-trait idiom
for overloading: an "instance" is an ordinary record passed implicitly. See
`examples/implicit_dictionaries.knot`.

### Effects are tracked

Every function's type records what it can do. `base.println` returns
`IO {}`; reading a file is `IO Text`; the DB operations are
`IO {} ...`. An `atomic` block (a transaction) is typed `IO {} a -> IO {} a`,
so the compiler *rejects* a `println` inside it. You don't write these
annotations — they're inferred.

### Nested relations

A record field can itself hold a relation — `[T]` nested inside a row. Nested
relations persist as JSON in SQLite and are queryable by binding into the
field (`m <- team.members`), so a hierarchy (a team and its members) lives in
one row. See `examples/nested.knot`.

### Grouping — `groupBy`

Inside a comprehension, `groupBy {key row.field}` partitions a relation by the
key fields; after it, the bound variable becomes the sub-relation for that
group, so aggregates apply per-group:

```knot
(do
  t <- *todos
  where t.done == 0
  groupBy {owner t.owner}
  yield {owner t.owner count (base.count t)})
```

Multiple keys (`groupBy {region o.region status o.status}`) nest the grouping.
Pushed down to `GROUP BY` when the query is translatable. See
`examples/groupby.knot`.

### Pipe-forward — `|>`

`x |> f |> g` is `g (f x)` — function composition read left to right, the
idiomatic way to chain relation combinators:

```knot
yield (employees
  |> base.filter (\e -> e.salary > 150000)
  |> base.map (\e -> {name e.name salary e.salary}))
```

### Higher-rank types — `forall`

A `forall a. T` in a function-*argument* signature makes the argument itself
polymorphic (rank-2): the caller passes a function that works for *any* type,
and the callee picks the type at each use:

```knot
with {
applyTwice (\(f : (forall a. a -> a)) -> {asText (f "text") asInt (f 42)})
}
(do
  with { r (applyTwice (\y -> y)) }
    (base.println (r.asText ++ " and " ++ base.show r.asInt)))
-- "text and 42"
```

An ordinary inferred function is monomorphic once inferred; only a
`forall`-typed parameter lets the callee instantiate it at several types. See
`examples/forall.knot`.

### Type-directed conversions — `base.morph`

`base.morph` is a nested record of conversions keyed by source then target
type — `base.morph.textToInt.into : Text -> Maybe (Int 1)`. It's consumed by
the `^into` implicit-field projection, which resolves the conversion by *both*
the argument's type and the expected result type:

```knot
asInt : Text -> Maybe (Int 1)
asInt (\s -> (^into) s)     -- resolves base.morph.textToInt.into
```

See `base.md` §Morphs and `examples/morph.knot`.

### Filtering a relation by variant — `base.match`

`base.match (Ctor {}) rows` keeps only the rows of an ADT relation built with
`Ctor`, returning their payloads — the cross-variant query over a mixed
relation. See `base.md`.

### Collecting folds — `(<>field) =>`

The sibling of the `(^field)` dictionary constraint: a `(<>field)` constraint
collects *every* in-scope `field` at the call site and merges them
(innermost-wins), passing the merged record as a hidden argument. This is how
structured-logging context threads through: `base.info` declares
`(<>logCtx)`, so `with {logCtx {...}}` blocks accumulate context that the
logger attaches automatically. See `examples/fold_dictionaries.knot`.

### Handy builtins (the `base.` namespace)

```knot
(do
  base.println (base.toUpper "knot")            -- "KNOT"
  base.println (base.show (base.length "hi"))   -- "2"
  base.println (base.show (base.take 2 [9 8 7]))  -- "[9, 8]"
  base.println (base.show (base.sum [1 2 3]))     -- "6"
  base.println (base.show (base.min 3 7))           -- "3"
  base.println (base.show (base.max 3 7))           -- "7"
  yield {})
```

Also: `map`, `filter`, `fold`, `count`, `countWhere`, `avg`, `minOn`,
`maxOn`, `sortBy`, `head`, `findFirst`, `union`, `inter`, `diff`, `reverse`,
`contains`, `trim`, `chars`, `forEach`, `when`, `unless`, `not`, plus JSON,
hashing, UUIDs, file I/O, crypto, and leveled logging. See
[stdlib.md](stdlib.md) for the full list.

### Debugging with `base.todo` and `base.trace`

Two primitives for development-time debugging. Both are gated special forms
reached through the `base` record, and both print a rich report showing the
relevant type, the source location (file:line:col with a caret), and every
in-scope local binding with its type and runtime value.

```knot
with {
greet (\name -> "hello " ++ name)

-- `base.todo : ∀a. a` is an unimplemented hole. It type-checks as ANY type,
-- so you can stub out a definition and fill it in later. The program
-- COMPILES, but when evaluation reaches the hole it aborts (exit 1) with the
-- report:
--   Error: reached a `todo` hole — this code path is not implemented yet
--     expected to produce a value of type: Text
--     local bindings in scope: ...
--     values in scope: ...
tagline base.todo          -- stub: "I'll write the tagline later"
}
(do
  base.println (greet "world")
  base.println tagline           -- a top-level hole is forced when the program
  yield {})                      -- starts, so this aborts with the report above
```

Every surviving `base.todo` also emits a **compile-time warning** (one per
site), so holes can't slip into a build unnoticed:

```
Warning: `todo` hole used — this code path is a debug placeholder, not implemented
```

`base.trace : ∀a. a -> a` is the non-crashing sibling — a transparent probe.
It prints the same report *plus* the traced value, then returns the value
unchanged, so you can drop it into the middle of an expression without
changing its meaning:

```knot
(do
  base.println (base.toUpper (base.trace "hello"))   -- prints the report +
                                                     -- traced value, then "HELLO"
  base.println (base.show (base.trace 21))           -- report + 21, then "21"
  yield {})
```

Like `todo`, a surviving `base.trace` emits a compile-time warning so you
don't ship a stray probe. The difference: `todo` is a placeholder to *replace*
before shipping (it aborts); `trace` is a *temporary observation point* (it
returns its argument, so the program keeps running).

### Compile-time termination checking

The compiler rejects definitions it can prove never terminate — a function
that unconditionally calls itself with no base case that could ever bottom
out, and certain non-productive recursive bindings. This is a *conservative*
check: it only fires on definite non-termination, so ordinary recursion
(`sumList`, folds, etc.) is unaffected.

### Refined-type constant checking and SMT subtyping

Two refinements to the refined-type machinery beyond the basics above:

- **Compile-time constants are checked against the predicate at build time.**
  `greet 30` needs no `refine` — the compiler evaluates the constant against
  `0 <= 30 <= 150` and either accepts it or fails the build naming the value.
  `&&`/`||` in predicates are const-folded so compound predicates check
  statically.
- **SMT-based subtyping.** The compiler can *prove* one refined type is a
  subtype of another when the predicates allow it — e.g. `Int 1 where \x ->
  x >= 10` is accepted where `Int 1 where \x -> x >= 0` is expected, because
  `x >= 10` implies `x >= 0`. This uses an SMT solver (Z3) to discharge the
  implication, so you can pass a *more* refined value where a *less* refined
  one is required without an explicit `refine`.

### Where to go next

- [DESIGN.md](DESIGN.md) — the full language specification (routes/HTTP,
  refined types, units, concurrency with `fork`/`race`/`atomic`, traits,
  schema migration, subset constraints).
- [stdlib.md](stdlib.md) — every builtin with its type.
- [examples/](examples/) — runnable programs for each feature.

## What's in the box

The compiler is a Cranelift backend producing native executables linked
against a Rust runtime. Most of the language is implemented and demonstrated
under [examples/](examples/):

**Relations and queries.** Source relations (`*name : [T]`), pure
expression-bindings, and query fields (`name <query>`) compose through
`do`-notation. Comprehension queries push down to SQL when they can —
`filter`, `map`, `count`, `countWhere`, `sum`, `avg`, `minOn`, `maxOn`,
multi-table joins, `groupBy`, and `sortBy` all become SELECT statements with
auto-indexed WHERE/ORDER BY columns. Pushdown goes through a unified Query IR,
matches stdlib functions by *resolution* (not bare name, so a same-named user
function can't be mistaken for a builtin), and *inlines pure helper functions*
in predicates so `where (salaryOf e) > 75` becomes `WHERE salary > 75`.
See `examples/query_opt.knot`, `examples/inline_pushdown.knot`,
`examples/let_pushdown.knot`, `examples/groupby.knot`, `examples/recursive.knot`.

**ADTs and pattern matching.** Sum types are first-class — `[Shape]` holds
circles and rects in one table. Constructor patterns work in `case` and in
`do`-bind (`Circle c <- *shapes` filters and destructures). Built-ins
include `Bool`, `Maybe`, and `Result`. See `examples/maybe.knot`,
`examples/result.knot`, `examples/cons_pattern.knot`.

**Ad-hoc polymorphism via record dictionaries.** There is no user-facing
`trait`/`impl` mechanism — the comparison and arithmetic operators are
intrinsic, and equality/ordering/`show` work structurally on any value. For
overloading you control, the **record-dictionary idiom** — bundling operations
in a record and passing it implicitly via `^field` projection or a
`(^field : T) =>` constraint — gives lightweight, first-class overloading
without a separate instance mechanism. See `examples/ord.knot`,
`examples/trait_replacement.knot`, `examples/implicit_dictionaries.knot`.

**Type inference.** Hindley-Milner with row-polymorphic records and
variants, let-generalization, implicit-dictionary constraints, and unit
polymorphism.

**IO.** Effectful functions return `IO a` — a description of an effectful
computation producing an `a`. `atomic do ...` runs a block in a transaction.
See `examples/log_test.knot`.

**Concurrency.** `fork`, `race`, and STM-style `atomic` blocks with
`retry`. Row-level read-filter wakeups mean a watcher on `WHERE id = 1`
isn't woken by writes to `id = 2`. See `examples/race.knot`,
`examples/stm_row_filter.knot`.

**HTTP routes and serving.** `api Name where ...` declares endpoints with a
URL-template syntax: each line is a constructor, an HTTP method, a path with
typed `{param: Type}` segments, optional `?{query: Type}` params, `@{request
headers: Type}`, a `={body: Type}`, and a `-> Response` type. A trailing
`@{...}` after the response declares response headers (the handler then
returns `{body, headers}`). `serve Name where Ctor = handler` type-checks
every handler against the declared method/path/body/query/headers/response
with full exhaustiveness, and `base.listen 8080 srv` runs the server.
Per-route `rateLimit {key, limit: {requests, window}}` is built in. Routes
compose: shared path prefixes factor out, and several `api` blocks merge into
one server. See `examples/routes.knot` and `examples/apiserver.knot`.

**Refined types.** `type Port = Int 1 where \p -> p > 0 && p < 65536` is a
nominal type whose predicate is checked at boundaries — relation writes,
HTTP body decoding, explicit `refine expr`, and CLI-argument parsing. Route
handlers auto-return HTTP 400 on validation failure. Compile-time constants
are checked against the predicate at build time (no `refine` needed), and an
SMT solver (Z3) proves subtyping between refined types, so a *more* refined
value is accepted where a *less* refined one is required. See
`examples/required_args.knot`.

**Required CLI arguments.** A `with`-level binding with a signature but no
value becomes a required `--name=value` flag on the compiled binary, parsed
and checked against its declared (possibly refined) type at startup. Missing
or invalid arguments abort with a clear error and a `--help` listing. See
`examples/required_args.knot`.

**Debugging primitives.** `base.todo` (an `∀a. a` unimplemented hole that
aborts with a full context report) and `base.trace` (an `∀a. a -> a`
transparent probe that prints a value and returns it) both show the relevant
type, source location, and in-scope bindings with values — and both emit a
compile-time warning so they don't get shipped accidentally.

**Lexical scoping.** `with { ... }` bindings are ordinary record fields with
purely lexical shadowing: an inner `with` shadows an outer one, siblings don't
collide, and a direct binding shadows an outer `with` field. Shadowing across
the top-level `with` chain (or of the `base` record itself) is rejected with a
clean error rather than silently changing meaning.

**Units of measure.** `unit Ms`, `unit Usd`, `unit N = Kg * M / S^2`.
Numeric literals carry units via `42.0 M` and `(expr : Int Ms)`. The
compiler checks unit consistency through arithmetic. Most stdlib numeric
functions are unit-polymorphic — `sleep` takes `Int Ms`, `now` returns
`Int Ms`, `randomInt 100 Usd` returns `Int Usd`.

**Schema evolution.** A `<name>.schema.lock` file records the persisted
schema. Adding nullable fields or ADT variants auto-updates; breaking
changes require a `migrate` block (see [DESIGN.md](DESIGN.md)).

**Constraints.** Subset constraints (`*orders.customer <= *people.name`)
enforce referential integrity and uniqueness at write time. See
`examples/constraints.knot`.

**Other goodies.** Bytes and hex encoding, BLAKE3 hashing, UUIDv7,
JSON encode/decode, file I/O, structured leveled logging
(`base.info`/`base.warn`/… with `logCtx` context records), crypto
(`generateKeyPair`/`encrypt`/`sign`/`verify`). See
`examples/bytes.knot`, `examples/hash.knot`, `examples/uuid.knot`,
`examples/json.knot`, `examples/crypto.knot`, `examples/log_test.knot`.

**Runtime CLI.** Every compiled program accepts a common set of flags, plus
any required-argument constants declared in the source:

```sh
./my_program                     # run main
./my_program --debug             # turn on base.debug output
./my_program --help              # print usage + any required CLI arguments
./my_program --http-max-body-bytes=32M   # cap HTTP body size (K/M/G suffixes)
./my_program --port=8080         # supply a required CLI argument
```

Required-argument constants can also be supplied at build time
(`knot build foo.knot --port=8080`). The compiler ships with
`knot fmt [--check] [--stdout] <file.knot>` for in-place formatting and
`knot build <file.knot> [-o <path>]` for compilation.

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
knot.md              Language reference (syntax, types, semantics)
stdlib.md            Standard library reference (operators, types, units)
base.md              `base` namespace reference (every builtin function)
AGENTS.md            Contributor/agent guide (build, test, architecture)
```

## Tests

```sh
cargo test                       # All tests
cargo test -p knot-runtime       # Runtime: SQLite, indexing, value repr
```

Unit tests currently live in `knot-runtime`; the
frontend/compiler/LSP are exercised end-to-end by building and running the
programs under [examples/](examples/).