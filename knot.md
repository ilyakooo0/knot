# Knot Language Reference

Knot is a functional relational programming language. Relations (typed sets) are the primary data structure, computation is pure and functional, and state is automatically persisted to SQLite.

## Quick Start

```knot
type Person = {name: Text, age: Int 1}

*people : [Person]

do
  full *people = [{name "Alice" age 30} {name "Bob" age 25}]
  people <- *people
  with {result do
    p <- people
    where p.age > 27
    yield p.name}
  (do
    base.println (base.show result)
    yield {})
```

Build and run:

```sh
cargo run -p knot-compiler -- build file.knot
./file
```

---

## Types

### Primitives

| Type | Description | Literals |
|------|-------------|----------|
| `Int 1` | 64-bit signed integer (overflow panics) | `42`, `-7`, `1_000_000` |
| `Float 1` | 64-bit float | `3.14`, `-0.5` |
| `Text` | Unicode string | `"hello"`, `"line\n"` |
| `Bool` | Boolean | `True {}`, `False {}` |
| `Bytes` | Byte string | `b"hello"` |
| `Uuid` | UUIDv7 identifier | (constructed via `randomUuid`) |
| `{}` | Unit / empty record | `{}` |

### Units of Measure

Compile-time units on `Int` and `Float`. Units are fully erased at runtime — no performance cost. Units are not declared: any name used in a unit position is a unit, and compound units are written inline.

```knot
distance : Float M
distance 42.0
speed : Float (M / S)
speed 10.0
force : Float (Kg * M / S^2)
force 9.8
cents : Int Usd
cents 100
```

Arithmetic rules: `+`/`-` require matching units, `*`/`/` compose units, negation preserves units.

```knot
(10.0 : Float M) + (5.0 : Float M)   -- Float M
(10.0 : Float M) * (5.0 : Float M)   -- Float (M^2)
(100.0 : Float M) / (10.0 : Float S) -- Float (M/S)
2.0 * (5.0 : Float M)                -- Float M (scalar mul)
-((5.0 : Float M))                   -- Float M
```

Unit polymorphism — concrete units are uppercase, lowercase names are unit variables:

```knot
double : Float u -> Float u
double \x -> x + x
```

Unit-preserving stdlib: `sum` (over a numeric relation) and `avg` (via its projection) propagate units.

### Records

```knot
-- Anonymous record VALUE: space-separated `name value` pairs, no colons/commas
alice {name "Alice" age 30}

-- Record TYPE: colon after each field name, comma-separated
type Person = {name: Text, age: Int 1}
```

Field access: `person.name`

Record merge: `base.unify person {age (person.age + 1)}` — right-biased; see `unify`.

Note the two syntaxes differ: **values** are `{name "Alice" age 30}` (space-separated, no `:` or `,`); **types** are `{name: Text, age: Int 1}` (`:` after each field name, `,` between fields). There is no field-name punning — a record value always pairs an explicit field name with its value (`{name t.name age t.age}`).

### Relations

A relation `[T]` is a typed **set** of `T` values. No duplicates. No ordering guarantees.

```knot
names (["Alice" "Bob" "Carol"]) -- [Text]
empty ([]) -- [a]
people ([{name "Alice" age 30}]) -- [{name: Text, age: Int 1}]
```

### ADTs (Algebraic Data Types)

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

data Maybe a = Nothing {} | Just {value: a}
```

**Every constructor requires `{}`** — even those with no fields. `Open {}`, not `Open`.

Constructing values: `Circle {radius: 5.0}`, `Nothing {}`, `Just {value: 42}`

### Type Aliases

```knot
type Person = {name: Text, age: Int 1}
type TodoList = [{title: Text, done: Bool}]
```

---

## Declarations

There are five kinds of top-level declarations:

| Declaration | Kind | Description |
|---|---|---|---|
| `*foo : [T]` | Source relation | Persisted in SQLite, mutable via `*foo = expr` |
| `foo <query>` | Query field | Read-only, recomputed on access |
| `foo = expr` | Constant/function | Pure value, no DB effects |
| `type Foo = T` | Type alias | Name for a type |

```knot
data Status = Open {} | Closed {}

-- Source: stored in DB
*people : [{name: Text, age: Int 1}]
*todos : [{title: Text, owner: Text}]

-- Query field: read-only computed relation
seniors (base.filter (\p -> p.age > 65) *people)

-- Constant
maxRetries (3)

-- Function (constants bound to lambdas)
double \x -> x * 2
```

---

## Functions and Lambdas

Functions are constants bound to lambdas:

```knot
-- Single parameter
greet \name -> "Hello, " ++ name

-- Multiple parameters
add \x y -> x + y

-- With type signature (optional — types are inferred)
formatName : Text -> Text
formatName \n -> base.toUpper (base.take 1 n) ++ base.drop 1 n
```

Function application is juxtaposition:

```knot
with {
greet \name -> "Hello, " ++ name
add \x y -> x + y
people [{name "Al" age 30} {name "Bo" age 20}]
}
(do
  base.println (greet "Alice")                          -- "Hello, Alice"
  base.println (base.show (add 2 3))                    -- 5
  base.println (base.show (base.filter (\p -> p.age > 30) people))
  yield {})
```

Pipe-forward operator:

```knot
with {
people [{name "Al" age 30} {name "Bo" age 20}]
}
(do
  base.println (base.show (people
    |> base.filter (\p -> p.age > 30)
    |> base.map (\p -> p.name)))
  yield {})
```

### Inline Type Annotations

Any expression can carry a postfix type annotation, both with and without
surrounding parens:

```knot
cents (0 : Int Usd)             -- bare postfix annotation
m ((2.0 + 3.0) : Float M)      -- parenthesized
distance (42.0 : Float (M / S)) -- units on a literal
```

Annotations are common with units of measure where the literal alone is
unit-agnostic.

### Type Holes

In any type position you can write `_` instead of a concrete type, and the
compiler infers it. Each `_` is an independent placeholder — it unifies with
whatever the surrounding code requires and is generalized like a type variable:

```knot
with {
id : _ -> _
id \x -> x
first : [_] -> _
first \xs -> base.head xs
const : _ -> _ -> _
const \x y -> x
}
(do
  base.println (base.show (id 42))        -- 42 : Int 1
  base.println (id "hello")               -- "hello" : Text
  base.println (base.show (first [10 20]))  -- element type inferred
  base.println (base.show (const 1 "two"))  -- three independent holes
  yield {})
```

Holes work anywhere a type can appear: function arrows, records, relation
element types, `Maybe _`, and inline annotations. In a unit-annotated numeric
type, `_` is a unit hole — it binds the unit by unification, like a lowercase
unit variable:

```knot
with {
double : Float _ -> Float _
double \x -> x + x
}
(do
  base.println (base.show (double (5.0 : Float M)))   -- 10.0 M — the unit flows through the hole
  yield {})
```

Each occurrence is fresh: `f : _ -> _` does not force the result to equal the
argument. To share a type between positions, use an explicit type variable
(`a`, `u`) instead of `_`.

---

## Do Blocks

`do` blocks are the primary syntax for comprehensions and sequencing. They work on four types — relations `[a]`, `IO`, `Maybe`, and `Result` — compiled structurally, not via a `Monad` trait.

### Relation Comprehensions

A comprehension over one source filters and projects its rows:

```knot
with {
*employees : [{name: Text, salary: Int 1}]
}
(with {result (do
  e <- *employees
  where e.salary > 75
  yield {name e.name salary e.salary})}
yield result)
```

**Joins.** Bind two (or more) relations in the same comprehension and relate
them with a `where` clause — an equi-join predicate (`a.f == b.g`) plus any
single-table predicates:

```knot
with {
*employees : [{name: Text, dept: Text, salary: Int 1}]
*departments : [{name: Text, budget: Int 1}]
}
(with {result (do
  e <- *employees
  d <- *departments
  where e.dept == d.name        -- the join condition
  where e.salary > 75           -- extra single-table filter
  yield {name e.name dept d.name budget d.budget})}
yield result)
```

A read-only comprehension like this compiles to a **single multi-table SQL
query**:

```sql
SELECT t0."name", t1."name", t1."budget"
FROM "_knot_employees" AS t0, "_knot_departments" AS t1
WHERE (t0."dept" = t1."name") AND (t0."salary" > ?)
```

The join columns (`employees.dept`, `departments.name`) and filter columns
(`employees.salary`) are auto-indexed on first use. A comprehension the
planner cannot translate falls back to reading the sources and joining in
memory — results are identical either way, only the strategy differs.

### Whole-relation reads

A relation read that is **not** pushed down to SQL loads the whole relation
into memory. This happens at a `base.run` boundary or a whole-relation bind
(`rows <- *rel` in an IO do-block), both of which materialize the query:

```knot
*people : [{name: Text, age: Int 1}]
(do
  people <- *people            -- whole relation loaded into memory
  base.println (base.show (base.count people))
  yield {})
```

A read that *is* pushed down — a comprehension the planner translates, or a
recognized aggregate such as `base.count *people` — runs in SQL and loads
nothing. The strategy is chosen at the use site, after the query is inlined.

Statements in a `do` block:

| Statement | Meaning |
|-----------|---------|
| `x <- expr` | Bind: iterate a relation / sequence IO / unwrap `Maybe`/`Result` |
| `where cond` | Filter: skip when condition is false |
| `yield expr` | Emit a value into the result |
| `groupBy {fields}` | Group by key fields (see Grouping) |
| `expr` | Bare expression (for IO side effects) |

### IO Do Blocks

When bound expressions are `IO` values, the do block sequences IO actions:

```knot
-- IO do block with console effects
do
  content <- base.readFile "input.txt"
  base.println content
  yield {}
```

```knot
-- IO do block with DB operations
*people : [{name: Text, age: Int 1}]
addPerson \name age -> do
  people <- *people
  *people = base.union people [{name name age age}]
```

The compiler detects whether a do block is relational or IO from the types. Relation operations (`*rel`, `&rel`, and writes `*rel = expr`) all return `IO value`.

### Pattern Matching in Bind

Filter and destructure in one step:

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
data Status = InProgress {assignee: Text} | Done {}
*shapes : [Shape]
*tickets : [{title: Text, status: Status}]
&circles = do
  shapes <- *shapes
  with {result do
    Shape.Circle c <- shapes
    yield c}
  (do
    yield result)

&inProgress = do
  tickets <- *tickets
  with {result do
    t <- tickets
    Status.InProgress ip <- t.status
    yield {title t.title assignee ip.assignee}}
  (do
    yield result)
```

---

## Mutation

Mutation is written `*rel = expr`, which makes the source relation equal to `expr`. There is no `set` keyword — the assignment is the bare `*rel = ...` form. The compiler recognizes common shapes (`*rel = union *rel [...]` → INSERT, conditional `map` → UPDATE, `filter` → DELETE) and emits minimal SQL; otherwise it rewrites the whole relation. Use `full *rel = expr` to force a full overwrite.

```knot
*people : [{name: Text, age: Int 1}]

-- Insert (union with singleton)
addPerson do
  people <- *people
  *people = base.union people [{name "Alice" age 30}]

-- Update (map with conditional)
birthday \name -> do
  people <- *people
  *people = do
    p <- people
    yield (case p.name == name of
      Bool.True {}  -> (base.unify p {age (p.age + 1)})
      Bool.False {} -> p)

-- Delete (filter to keep)
removePerson \name -> do
  people <- *people
  *people = do
    p <- people
    where p.name != name
    yield p
```

Relations are sets — inserting a duplicate row is a no-op.

---

## Control Flow

### Conditionals — `case` on a Bool

There is **no `if`/`then`/`else`**. The only branch is pattern matching with
`case ... of`; a Bool scrutinee gives you if/else:

```knot
with {result (case 5 > 0 of
  Bool.True {}  -> "positive"
  Bool.False {} -> "non-positive")}
result
```

Both arms must have the same type, and matching must be exhaustive (the
compiler enforces covering every constructor).

### `with` Expressions

`with {name value, ...} body` evaluates `body` with each record field bound as a variable in scope. The whole expression's value is `body`'s value:

```knot
with {x 2 y 3} (x + y)            -- 5
```

The bound value can be any expression, including a do block:

```knot
with {result do
  p <- [{name "Al" age 30} {name "Bo" age 20}]
  where p.age > 27
  yield p.name}
(do
  base.println (base.show result)
  yield {})
```

Use `with` wherever you would otherwise introduce a local name — inside do blocks, in function bodies, or nested in other expressions.

#### Importing a type's constructors

Naming a **data type** (uppercase) as a `with` field brings that type's constructors into scope **unqualified** for the body — you can write `Just {value v}` instead of `Maybe.Just {value v}`:

```knot
with {Maybe} (case (Just {value 5}) of
  Just {value v} -> v
  Nothing {} -> 0)              -- 5
```

Types and value bindings mix freely:

```knot
with {Maybe five 5} (case (Just {value five}) of
  Just {value v} -> v
  Nothing {} -> 0)              -- 5
```

Import several types at once:

```knot
with {Maybe Result} (case (Ok {value 3}) of
  Ok {value v} -> v
  Err {error e} -> 0)           -- 3
```

The unqualified constructors are confined to the `with` body — the qualified form (`Maybe.Just {…}`) still works everywhere, including inside the body. A name that isn't a known data type is an error, and if two imported types share a constructor name you must use that one qualified. This also works for your own ADTs once their `data` declaration is in scope.

### Case Expressions

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
data Priority = Critical {} | High {} | Low {}

describe \s -> case s of
  Shape.Circle {radius r} -> "circle r=" ++ base.show r
  Shape.Rect {width w height h} -> base.show w ++ "x" ++ base.show h

-- With wildcard
priority \p -> case p of
  Priority.Critical {} -> 1
  Priority.High {} -> 2
  _ -> 3
```

### Pattern Matching on Relations

```knot
describe \rel -> case rel of
  [] -> "empty"
  [{name n}] -> "just " ++ n
  Cons h t -> "first of many: " ++ base.show h.name
```

| Pattern | Matches |
|---------|---------|
| `[]` | empty relation |
| `[p1 p2 ...]` | relation with exactly that many rows |
| `Cons head tail` | non-empty relation, `head` is one row, `tail` is the rest |

`Cons` is also useful in recursive functions:

```knot
sumList \xs -> case xs of
  [] -> 0
  Cons h t -> h + sumList t
```

---

## Grouping

`groupBy` partitions a relation by key fields. After `groupBy`, the bound variable becomes a sub-relation:

```knot
*todos : [{title: Text, owner: Text, done: Int 1}]
&workload = do
  with {result do
    t <- *todos
    where t.done == 0
    groupBy {owner t.owner}
    yield {owner t.owner count (base.count t)}}
  (do
    yield result)
```

Multiple keys: `groupBy {region o.region status o.status}`

After `groupBy {owner t.owner}`:
- `t.owner` returns the shared key value
- `count t` counts rows in the group
- `sum (map (\x -> x.points) t)` aggregates over the group

---

## Nested Relations

Fields can hold `[T]` — sets nested inside rows:

```knot
*teams : [{name: Text, members: [{name: Text, age: Int 1}]}]

-- Query into nested relations
&allMembers = do
  teams <- *teams
  with {result do
    t <- teams
    m <- t.members
    yield {team t.name member m.name}}
  (do
    yield result)

-- Update nested relations
updateTeams do
  teams <- *teams
  *teams = do
    t <- teams
    yield (base.unify t {members do
      m <- t.members
      where m.name != "Eve"
      yield m})
```

---

## Query Fields

A field whose value is a query is a **query field**: a read-only, computed
relation that is recomputed each time it is read. Query fields are pure and
lazy — declaring one reads nothing; the query runs when the field is used.

```knot
data Status = Open {} | Closed {}
*todos : [{title: Text, owner: Text, done: Int 1}]

openTodos (do
  t <- *todos
  where t.done == 0
  yield {title t.title owner t.owner})
```

Query fields compose: one query field can reference another, and SQL pushdown
folds through the chain — the whole composed query is pushed down to a single
SQL statement when the planner can translate it, or evaluated in memory
otherwise. `base.run` materializes a query field into a concrete `Vec`.

### Recursive query fields

A query field that references its own name computes a fixpoint (transitive
closure):

```knot
*manages : [{manager: Text, report: Text}]

reportsTo (base.union *manages (do
  r <- reportsTo
  m <- *manages
  where r.report == m.manager
  yield {manager r.manager report m.report}))
```

## No User-Facing Traits

Knot has **no trait system you can use**. The parser rejects `trait`
declarations, `impl` blocks, `deriving`, and `Num a =>`-style bounds — writing
any of them is a syntax error.

The operators and higher-order functions are still there, but they are
**intrinsic / ordinary polymorphic functions**, not trait methods:

- `==` `!=` `<` `>` `<=` `>=` work on `Int 1`, `Float 1`, `Text` (equality also
  on `Bool`); `+` `-` `*` `/` `%` and unary `-` on numerics; `++` on `Text`
  and `[a]`. No type class is involved — the compiler evaluates them directly.
- `base.map`, `base.fold`, `base.traverse`, `base.bind`, etc. are plain
  polymorphic functions over the concrete types `[a]`, `Maybe a`,
  `Result e a`, and `IO`, with no bounds. Do-notation with `<-` works on
  `[a]`, `IO`, `Maybe`, and `Result` because the compiler knows how to sequence
  each of them — not because of a `Monad` instance you could write.

Because there are no instances to define, there is also nothing to derive and
no way to make your own type work with an operator beyond the built-in cases.

---

## IO and Effects

### IO Type

Effectful functions return `IO a` — a description of an effectful computation
producing an `a`, not immediate execution. `IO` takes a single type argument
(the result type); there is no effect-row parameter:

```knot
println : a -> IO {}
readFile : Text -> IO Text
now : IO (Int Ms)
```

A `do` block sequences `IO` actions; the whole block is itself an `IO`.

### IO Do Blocks

```knot
do
  content <- base.readFile "data.txt"
  base.println ("Read " ++ base.show (base.length content) ++ " chars")
  yield {}
```

### DB Effects

Relation operations are also `IO`-wrapped:

```knot
type Person = {name: Text, age: Int 1}
*people : [Person]

-- All relation operations are IO:
birthday \name -> do
  people <- *people              -- IO [Person]
  *people = do               -- IO {}
    p <- people
    yield (case p.name == name of
      Bool.True {}  -> (base.unify p {age (p.age + 1)})
      Bool.False {} -> p)
-- Inferred effects: {rw *people}
-- Type: Text -> IO {} {}
```

### Transactions

`atomic` takes an IO expression containing only DB operations and runs it in a transaction:

```knot
*orders : [{item: Text, qty: Int 1}]
handleOrder \item -> do
  orderId <- atomic do
    orders <- *orders
    *orders = base.union orders [{item item qty 1}]
    newOrders <- *orders
    yield (base.count newOrders)
  base.println ("Order #" ++ base.show orderId)
  yield {orderId orderId}
```

The body of `atomic` must be an IO expression containing only DB operations. External effects (console, fs, etc.) are not allowed inside `atomic`.

### Concurrency

#### `fork`

Fire-and-forget: runs an IO action on a new OS thread. Each thread gets its own SQLite connection (WAL mode).

```knot
-- base.fork : IO a -> IO {}
(do
  base.fork do
    base.println "hello from thread 1"
  base.fork do
    base.println "hello from thread 2"
  base.println "hello from main"
  yield {})
  -- main waits for all spawned threads before exiting
```

`fork` spawns an `IO` action concurrently and returns `IO {}`. Do blocks can be
passed directly as arguments: `fork do ...` (no parentheses needed).

#### `retry`

Used inside `atomic` blocks for STM-style waiting. Causes the transaction to rollback and wait until some relation changes, then re-executes the atomic block.

```knot
waitFor \condition -> atomic do
  cond <- condition
  base.when (not cond) base.retry
  yield cond
```

The compiler enforces that `retry` is only used inside `atomic`.

The runtime tracks which rows the atomic block actually read (via row-level
read filters extracted from `WHERE`/`single (filter ...)` patterns) and only
wakes a parked `retry` when a write affects a matching row. So a worker
retrying on `WHERE id = 1` is not woken by writes to `id = 2`, and a worker
retrying on `status IN ("queued", "running")` is not woken by writes that
leave the status outside that set. Bulk replacements wake all watchers
conservatively.

#### `race`

```knot
race : IO a -> IO b -> IO (Result a b)
```

Run two IO actions concurrently and return the winner. The winner is reported
using the built-in `Result` ADT — `Err {error: a}` when the left action wins,
`Ok {value: b}` when the right action wins.

```knot
slow do
  base.sleep (1000 : Int Ms)
  yield "slow"

fast do
  base.sleep (50 : Int Ms)
  yield "fast"

do
  r <- base.race slow fast
  case r of
    Result.Err {error a} -> base.println ("left won: " ++ a)
    Result.Ok {value b}  -> base.println ("right won: " ++ b)
  yield {}
```

Cancellation is cooperative but aggressive: `knot_io_run` checks the loser's
cancel token between every IO thunk, and `sleep` parks on a condvar that's
signalled on cancel — so a loser stuck in a long `sleep` wakes immediately.
The parent does not join the loser; it returns as soon as a winner is observed
and the loser unwinds at its next safe point.

`race` cannot be used inside `atomic` (its effects are not rollback-safe).

---

## Operators

| Operator | Meaning | Works on |
|----------|---------|----------|
| `+` `-` `*` `/` | Arithmetic | `Int 1`, `Float 1`, unit-annotated |
| `%` | Modulo (remainder) | `Int 1`, `Float 1` |
| unary `-` | Negation | numerics |
| `==` `!=` | Equality | `Int 1`, `Float 1`, `Text`, `Bool` |
| `<` `>` `<=` `>=` | Comparison | `Int 1`, `Float 1`, `Text` |
| `++` | Concatenation | `Text`, `[a]` |
| `&&` `\|\|` | Boolean logic | `Bool` (direct) |
| `\|>` | Pipe forward | `x \|> f` = `f x` |

All intrinsic — no trait mechanism.

---

## Standard Library Functions

### Relations

| Function | Type | Description |
|----------|------|-------------|
| `filter` | `(a -> Bool) -> [a] -> [a]` | Keep matching rows |
| `map` | `(a -> b) -> [a] -> [b]` | Transform each row |
| `match` | `Constructor -> [ADT] -> [Payload]` | Filter by variant |
| `fold` | `(b -> a -> b) -> b -> [a] -> b` | Left fold |
| `count` | `[a] -> Int u` | Number of rows |
| `countWhere` | `(a -> Bool) -> [a] -> Int u` | Filtered count (SQL-pushed when possible) |
| `sum` | `(a -> b) -> [a] -> b` | Sum projected field (preserves units) |
| `avg` | `(a -> Float u) -> [a] -> Float u` | Average projected field (preserves units) |
| `minOn` | `(a -> b) -> [a] -> b` | Min of projected field (panics on empty) |
| `maxOn` | `(a -> b) -> [a] -> b` | Max of projected field (panics on empty) |
| `min` | `a -> a -> a` | Binary min of two orderable values |
| `max` | `a -> a -> a` | Binary max of two orderable values |
| `head` | `[a] -> Maybe a` | First row, or `Nothing` if empty |
| `findFirst` | `[a] -> (a -> Bool) -> Maybe a` | First row matching predicate |
| `single` | `[a] -> Maybe a` | `Just x` for a singleton, `Nothing` otherwise |
| `any` | `(a -> Bool) -> [a] -> Bool` | True if any row matches |
| `all` | `(a -> Bool) -> [a] -> Bool` | True if every row matches |
| `elem` | `a -> [a] -> Bool` | Membership by structural equality |
| `union` | `[a] -> [a] -> [a]` | Set union |
| `diff` | `[a] -> [a] -> [a]` | Set difference |
| `inter` | `[a] -> [a] -> [a]` | Set intersection |
| `sortBy` | `(a -> b) -> [a] -> [a]` | Reorder rows by projected key (`Ord b`) |
| `take` | `Int 1 -> [a] -> [a]` | First *n* rows (`Sequence.take`) |
| `drop` | `Int 1 -> [a] -> [a]` | Drop first *n* rows (`Sequence.drop`) |

### Text

| Function | Type | Description |
|----------|------|-------------|
| `toUpper` | `Text -> Text` | Uppercase |
| `toLower` | `Text -> Text` | Lowercase |
| `length` | `Text -> Int u` | Character count |
| `trim` | `Text -> Text` | Strip whitespace |
| `reverse` | `Text -> Text` | Reverse |
| `chars` | `Text -> [Text]` | Split to characters |
| `take` | `Int 1 -> Text -> Text` | First *n* characters (`Sequence.take`) |
| `drop` | `Int 1 -> Text -> Text` | Drop first *n* characters (`Sequence.drop`) |
| `contains` | `Text -> Text -> Bool` | Substring check |

`take` and `drop` are `Sequence` trait methods with built-in impls for both
`Text` and relations.

### Conversion

| Function | Type | Description |
|----------|------|-------------|
| `show` | `a -> Text` | Any value to text |
| `toJson` | `a -> Text` | Encode as JSON (`ToJSON.toJson`) |
| `parseJson` | `Text -> Maybe a` | Decode JSON (`FromJSON.parseJson`) |
| `display` | `a -> Text` | Render a value as human-readable text |
| `stripUnit` | `Int u -> Int 1` | Drop unit tag from `Int 1` |
| `withUnit` | `Int 1 -> Int u` | Attach unit tag to `Int 1` |
| `stripFloatUnit` | `Float u -> Float 1` | Drop unit tag from `Float 1` |
| `withFloatUnit` | `Float 1 -> Float u` | Attach unit tag to `Float 1` |
| `strip` | `a u -> a 1` | Drop unit tag (any unit-carrying numeric) |
| `dress` | `a 1 -> a u` | Attach unit tag (any unit-carrying numeric) |
| `floor` | `Float u -> Int 1` | Round toward negative infinity |
| `intToFloat` | `Int u -> Float 1` | Widen `Int` to `Float` (lossy past 2⁵³) |
| `textToInt` | `Text -> Maybe (Int 1)` | Parse integer (`Nothing` on bad input) |
| `textToFloat` | `Text -> Maybe (Float 1)` | Parse float (`Nothing` on bad input) |

Type-directed conversions also live in the nested **`base.morph`** record,
consumed by the `^into` implicit-field projection: `(^into) x` resolves the
matching `base.morph.<from>To<to>.into` by both the argument and expected
result type (see [base.md](base.md#morphs-basemorph)).

### IO

| Function | Type | Description |
|----------|------|-------------|
| `println` | `a -> IO {}` | Print with newline |
| `print` | `a -> IO {}` | Print without newline |
| `readLine` | `IO Text` | Read stdin line |
| `debug` | `(<>logCtx) => Text -> IO {}` | Leveled log to stderr (DEBUG); only emits when run with `--debug` |
| `info` | `(<>logCtx) => Text -> IO {}` | Leveled log to stderr (INFO) |
| `warn` | `(<>logCtx) => Text -> IO {}` | Leveled log to stderr (WARN) |
| `error` | `(<>logCtx) => Text -> IO {}` | Leveled log to stderr (ERROR) |
| `readFile` | `Text -> IO Text` | Read file |
| `writeFile` | `Text -> Text -> IO {}` | Write file (path, content) |
| `appendFile` | `Text -> Text -> IO {}` | Append to file |
| `fileExists` | `Text -> IO Bool` | Check file exists |
| `removeFile` | `Text -> IO {}` | Delete file |
| `listDir` | `Text -> IO [Text]` | List directory |
| `now` | `IO Int Ms` | Unix timestamp (milliseconds) |
| `sleep` | `Int Ms -> IO {}` | Pause the current thread |
| `randomInt` | `Int u -> IO Int u` | Random int `[0, bound)`, preserves unit |
| `randomFloat` | `IO Float u` | Random float `[0.0, 1.0)`, unit-polymorphic |
| `randomUuid` | `IO Uuid` | Generate a RFC 9562 UUIDv7 |
| `atomic` | `IO {} a -> IO {} a` | Run DB operations in a transaction |
| `fork` | `IO a -> IO {}` | Fire-and-forget on new OS thread |
| `race` | `IO a -> IO b -> IO (Result a b)` | Run two IO actions, return the winner |
| `retry` | `a` | Rollback and wait (inside `atomic` only) |
| `when` | `Bool -> IO {} -> IO {}` | Run action when condition is true |
| `unless` | `Bool -> IO {} -> IO {}` | Run action when condition is false |
| `forEach` | `[a] -> (a -> IO {}) -> IO {}` | Sequence an action over each row |
| `listen` | `Int u -> Server a -> IO {}` | Start an HTTP server |
| `fetch` | `Text -> Endpoint -> IO (Result HttpError T)` | Type-safe HTTP client |
| `fetchWith` | `Text -> {headers: [..]} -> Endpoint -> IO (Result HttpError T)` | `fetch` with ad-hoc headers |

`listen` takes a `Server` built by `serve API where` and binds the HTTP port.

### Bytes

| Function | Type | Description |
|----------|------|-------------|
| `textToBytes` | `Text -> Bytes` | UTF-8 encode |
| `bytesToText` | `Bytes -> Maybe Text` | UTF-8 decode (`Nothing` on invalid UTF-8) |
| `bytesLength` | `Bytes -> Int u` | Byte length |
| `bytesSlice` | `Int u1 -> Int u2 -> Bytes -> Bytes` | Sub-range (start, length, bytes) |
| `bytesConcat` | `Bytes -> Bytes -> Bytes` | Concatenate |
| `bytesGet` | `Int u1 -> Bytes -> Int u2` | Byte value (0–255) at index |
| `bytesToHex` | `Bytes -> Text` | Hex encode |
| `bytesFromHex` | `Text -> Maybe Bytes` | Hex decode (`Nothing` on bad input) |
| `hash` | `a -> Bytes` | BLAKE3 hash (32 bytes) of any value |

### Cryptography

| Function | Type | Description |
|----------|------|-------------|
| `generateKeyPair` | `IO ({privateKey: Bytes, publicKey: Bytes})` | X25519 keypair |
| `generateSigningKeyPair` | `IO ({privateKey: Bytes, publicKey: Bytes})` | Ed25519 keypair |
| `encrypt` | `Bytes -> Bytes -> IO Bytes` | Sealed-box (public key, plaintext) |
| `decrypt` | `Bytes -> Bytes -> Bytes` | Open sealed-box (private key, ciphertext) |
| `sign` | `Bytes -> Bytes -> Bytes` | Ed25519 sign (private key, message) |
| `verify` | `Bytes -> Bytes -> Bytes -> Bool` | Verify (public key, message, signature) |

### Utility

| Function | Type | Description |
|----------|------|-------------|
| `id` | `a -> a` | Identity |
| `not` | `Bool -> Bool` | Boolean negation |

---

## Comments

```knot
-- Single-line comment
```

---

## Routes

HTTP routing with typed paths, query params, bodies, and headers:

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}
type Todo = {title: Text, owner: Text, priority: Priority}
*todos : [Todo]

route TodoApi where
  GET /todos/{owner: Text} -> [Todo] = GetTodos
  POST {title: Text, owner: Text} /todos -> Todo = CreateTodo

route AdminApi where
  GET /admin/count -> Int 1 = GetCount

-- Compose routes
route Api = TodoApi | AdminApi

addTodo (\title owner -> do
  todos <- *todos
  *todos = base.union todos [{title title owner owner priority (Priority.Low {})}])

getTodos (\owner -> do
  todos <- *todos
  with {result (do t <- todos; where t.owner == owner; yield t)} yield result)

-- Handler
api (serve Api where
  GetTodos = \{owner owner} -> do
    todos <- getTodos owner
    yield (Result.Ok {value todos})
  CreateTodo = \{title title owner owner} -> do
    addTodo title owner
    yield (Result.Ok {value {title title owner owner priority (Priority.Low {})}})
  GetCount = \{} -> do
    todos <- *todos
    yield (Result.Ok {value (base.count todos)}))
```

`serve API where` produces a value of type `Server API`. Each handler takes the request record and returns `Result HttpError T`, where `T` is the response type declared on the endpoint and `HttpError = {status: Int 1, message: Text}`. `listen : Int u -> Server a -> IO {}` binds the server to a port.

### HTTP Status Codes

`Ok {value: v}` responds 200 with `v` as JSON. `Err {error: {status, message}}` responds with the given status code and a JSON error body:

```knot
*people : [{id: Int 1, name: Text}]
route Api where
  GET /users/{id: Int 1} -> {id: Int 1, name: Text} = GetUser
api (serve Api where
  GetUser = \{id id} -> do
    users <- *people
    case base.filter (\u -> u.id == id) users of
      [] -> yield (Result.Err {error {status 404 message "user not found"}})
      Cons u rest -> yield (Result.Ok {value u}))
```

Status is clamped to `100..=599`. The runtime emits `400` for path/query/body parsing failures and refinement violations, and `404` for unmatched routes — only return `Err` for application-level errors.

### Typed Headers

Request and response headers use the `headers` keyword:

```knot
route Api where
  GET /todos headers {authorization: Text}
    -> [{title: Text}] headers {xTotalCount: Int 1}
    = GetTodos
  POST {title: Text} /todos headers {authorization: Text}
    -> {id: Int 1}
    = CreateTodo
```

Field names use camelCase, auto-converted to HTTP-Header-Case: `authorization` → `Authorization`, `contentType` → `Content-Type`, `xRequestId` → `X-Request-Id`.

Request headers become constructor fields. When response headers are declared, the handler returns a `{body: ..., headers: ...}` record:

```knot
type Todo = {title: Text}
*todos : [Todo]
route Api where
  GET /todos headers {authorization: Text} -> [Todo] headers {xTotalCount: Int 1} = GetTodos

api (serve Api where
  GetTodos = \{authorization authorization} -> do
    allTodos <- *todos
    yield (Result.Ok {value {body allTodos headers {xTotalCount (base.count allTodos)}}}))
```

Optional headers use `Maybe`:

```knot
route Api where
  GET /todos headers {authorization: Maybe Text} -> [Todo] = GetTodos
```

Server gets `Nothing {}` if absent, `Just {value: "..."}` if present. In `fetch`, `Nothing` headers are skipped.

On the `fetch` side, header fields are sent automatically. When response headers are declared, the result wraps as `{body: T, headers: H}`:

<!-- doccheck: skip — the `fetch`+route-constructor form is documented here but
     the endpoint constructor is not yet value-accessible in expressions (doc-vs-impl gap). -->
```knot-skip
result <- base.fetch "https://api.example.com" (GetTodos {authorization "Bearer tok"})
-- result : IO (Result ... {body: [Todo], headers: {xTotalCount: Int 1}})
```

### Rate Limiting

Add a per-endpoint token-bucket rate limit with `rateLimit <expr>` (placed after the response type/headers, before `=`). The expression has type `RateLimit input a`:

```knot
type RequestCtx = {
  clientIp: Text,
  receivedAt: Int Ms,
  header: Text -> Maybe Text
}

-- `key` is Ord a; returning Nothing exempts the request. `header` is a case-insensitive lookup.
type RateLimit input a = {key: input -> RequestCtx -> Maybe a, limit: {requests: Int 1, window: Int Ms}}
```

`key` receives the same input record the handler does (path/query/body/header fields, combined) plus the runtime-supplied `RequestCtx`, so you can key on any field of either:

```knot
byClientIp \input ctx -> Maybe.Just {value ctx.clientIp}

byOwner \{owner owner} ctx -> Maybe.Just {value owner}   -- key on a path/body field

route Api where
  GET /hello -> {message: Text}
    rateLimit {key byClientIp limit {requests 100 window (60000 : Int Ms)}}
    = Hello

  GET /user/{owner: Text} -> {message: Text}
    rateLimit {key byOwner limit {requests 10 window (60000 : Int Ms)}}
    = User

  GET /open -> {message: Text} = Open                  -- no clause = unlimited
```

The `key` value can be any `Ord` type — the runtime serializes it via `show` for the SQLite bucket key. Returning `Nothing` from `key` skips rate limiting for that request (e.g. exempt admin requests by reading `ctx.header "Authorization"`).

On rejection the runtime responds `429 Too Many Requests` with body `{"error":"Rate limit exceeded"}` and a `Retry-After: <seconds>` header — the handler is not invoked. Buckets persist in a hidden `_knot_rate_limits` SQLite table; concurrent requests for the same key serialize via `BEGIN IMMEDIATE`.

Common keying strategies are regular expressions, so extract them once and reuse:

```knot
data Event = Gossip {payload: Text}
serverLimit ({key (\input ctx -> Maybe.Just {value ctx.clientIp}) limit {requests 1000 window (60000 : Int Ms)}})

route Api where
  POST {events: [Event]} /federation/gossip -> {} rateLimit serverLimit = RecvGossip
```

---

## Schema Evolution

The compiler maintains a lockfile (`<name>.schema.lock`) tracking persisted schemas.

### Automatic Changes

- Adding a `Maybe` field, a new variant, or a new relation: auto-updated
- Removing fields/variants or changing types: requires `migrate`

### Migrations

```knot
*people : [{name: Text, age: Int 1}]
  migrate from {name: Text, age: Int 1}
  to {name: Text, age: Int 1, email: Text}
  using (\old -> (base.unify old {email (old.name ++ "@unknown.com")}))
```

---

## Subset Constraints

```knot
*people : [{name: Text, age: Int 1}]
*orders : [{customer: Text, amount: Int 1}]
*users : [{name: Text, email: Text}]

-- Referential integrity
*orders.customer <= *people.name

-- Uniqueness
*users <= *users.email
```

---

## Refined Types

Types restricted by predicate functions, checked at runtime boundaries.

### Declaration

```knot
-- Simple refined type alias
type Nat = Int 1 where \x -> x >= 0

-- Per-field refinements
type ValidPerson = {name: Text, age: Int 1 where \x -> x >= 0 && x <= 150}

-- Cross-field refinements
type Range = {lo: Int 1, hi: Int 1} where \r -> r.lo <= r.hi

-- ADT constructor refinements
data Shape
  = Circle {radius: Float 1 where \r -> r > 0.0}
  | Rect {width: Float 1 where \w -> w > 0.0, height: Float 1 where \h -> h > 0.0}
```

### Checking with `refine`

`refine expr` validates a value against a refined type inferred from context. Returns `Result RefinementError T`:

```knot
type Nat = Int 1 where \x -> x >= 0
```
```knot
with {
type Nat = Int 1 where \x -> x >= 0
}
(do
  -- Use with case
  case refine (5 : Int 1) of
    Result.Ok {value n} -> base.println ("Valid: " ++ base.show n)
    Result.Err {error e} -> base.println ("Invalid: " ++ base.show e)
  -- Use in Result do-block
  with {validated do
    n <- refine (3 : Int 1)     -- binds n : Nat on success, short-circuits on failure
    m <- refine (4 : Int 1)
    yield (n + m)}
  (do
    base.println (base.show validated)   -- validated : Result RefinementError (Int 1)
    yield {})
  yield {})
```

`RefinementError = {typeName: Text, violations: [{field: Maybe Text, message: Text}]}`

### Automatic Validation

**Write validation**: refined fields on source relations are checked before each write (`*rel = ...`). Panics on violation.

```knot
type Nat = Int 1 where \x -> x >= 0
*people : [{name: Text, age: Nat}]

-- This panics if any age is negative:
do
  full *people = [{name "Alice" age (0 - 1)}]
  yield {}
```

**Route handlers**: refined body fields are auto-validated after JSON decoding. Returns HTTP 400 on failure.

```knot
route Api where
  POST {age: Nat} /users -> User = CreateUser

-- POST with {"age": -1} returns 400 automatically
```

### Subtyping

Refined types are subtypes of their base type. `Nat` is compatible with `Int 1` in both directions — passing a `Nat` where `Int 1` is expected works, and vice versa (but the latter is unchecked unless you use `refine`).

---

## `do` on `Maybe` and `Result`

Besides relations and `IO`, `do`/`<-`/`yield` works on `Maybe` and `Result` via
the compiler's built-in structural support — short-circuiting on `Nothing` /
`Err`:

```knot
-- Maybe — short-circuits on Nothing
result do
  a <- Maybe.Just {value 10}
  b <- Maybe.Just {value 2}
  where b != 0
  yield (a / b)

-- Result — short-circuits on Err
safeDivide \x y -> case y == 0 of
  Bool.True {}  -> Result.Err {error "div by zero"}
  Bool.False {} -> Result.Ok {value (x / y)}

compute do
  a <- Result.Ok {value 10}
  b <- safeDivide a 2
  yield (a + b)
```

`do` support is **fixed to these four types** (`[a]`, `IO`, `Maybe`, `Result`)
— there is no way to make a user-defined type work with `do`, because that
would require a `Monad` instance and there is no trait system to write one in.

---

## Complete Example: Todo App

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

type Todo = {title: Text, owner: Text, priority: Priority, status: Status}

*todos : [Todo]

add \title owner priority -> do
  todos <- *todos
  *todos = base.union todos [{title title owner owner priority priority status (Status.Open {})}]

complete \title -> do
  todos <- *todos
  *todos = do
    t <- todos
    yield (case t.title == title of
      Bool.True {} -> (base.unify t {status (Status.Resolved {resolution "done"})})
      Bool.False {} -> t)

assign \title person -> do
  todos <- *todos
  *todos = do
    t <- todos
    yield (case t.title == title of
      Bool.True {} -> (base.unify t {status (Status.InProgress {assignee person})})
      Bool.False {} -> t)

pending \owner -> do
  todos <- *todos
  with {result do
    t <- todos
    where t.owner == owner
    Status.Open {} <- t.status
    yield {title t.title priority t.priority}}
  (do
    yield result)

&workload = do
  with {result do
    t <- *todos
    where t.status == Status.Open {}
    groupBy {owner t.owner}
    yield {owner t.owner count (base.count t)}}
  (do
    yield result)

do
  add "Write parser" "Alice" (Priority.High {})
  add "Write codegen" "Alice" (Priority.Critical {})
  add "Write runtime" "Bob" (Priority.Medium {})
  assign "Write parser" "Carol"
  complete "Write runtime"
  p <- pending "Alice"
  base.println "Alice's pending:"
  base.println (base.show p)
  w <- &workload
  base.println "Workload:"
  base.println (base.show w)
  yield {}
```

---

## Common Patterns

### Insert

```knot
*rel : [{value: Int 1}]
addRow \newRow -> do
  rel <- *rel
  *rel = base.union rel [newRow]
```

### Delete by condition

```knot
*rel : [{id: Int 1, field: Int 1}]
deleteWhere \valueToDelete -> do
  rel <- *rel
  *rel = do
    r <- rel
    where r.field != valueToDelete
    yield r
```

### Update by condition

```knot
*rel : [{id: Int 1, field: Int 1}]
updateWhere \target newValue -> do
  rel <- *rel
  *rel = do
    r <- rel
    yield (case r.id == target of
      Bool.True {}  -> (base.unify r {field newValue})
      Bool.False {} -> r)
```

### Join two relations

```knot
*employees : [{name: Text, dept: Text}]
*departments : [{name: Text, budget: Int 1}]
&joined = do
  employees <- *employees
  departments <- *departments
  with {result do
    e <- employees
    d <- departments
    where e.dept == d.name
    yield {name e.name budget d.budget}}
  (do
    yield result)
```

### Aggregate

```knot
*orders : [{amount: Int 1}]
*people : [{name: Text, age: Int 1}]
getTotal do
  orders <- *orders
  yield (base.fold (\acc x -> acc + x.amount) 0 orders)

getCount do
  people <- *people
  yield (base.count people)
```

### Filter by variant

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
*shapes : [Shape]

-- Using match
&circles = do
  shapes <- *shapes
  yield (base.match Shape.Circle shapes)

-- Using pattern bind in do
&circles2 = do
  shapes <- *shapes
  with {result do
    Shape.Circle c <- shapes
    yield c}
  (do
    yield result)
```