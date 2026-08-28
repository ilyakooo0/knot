# Knot Language Design

Knot is a functional relational programming language. Relations are the primary data structure, computation is pure and functional, and state is automatically persisted.

## Core Principles

1. **Relations are the data structure** — not lists, not arrays. `[T]` is a typed set of `T` values.
2. **Effects are inferred** — the compiler tracks reads and writes. No annotations needed.
3. **ADTs are native to relations** — a `[Shape]` holds circles and rects in one relation. The tag is an implementation detail.
4. **No keys** — relations are sets. Identity is structural. The runtime handles indexing.
5. **State is visible** — source relations (mutable, persisted) are prefixed with `*`, derived relations (read-only) with `&`. Every reference site shows whether you're touching state. No ORM, no SQL.

## Data Model

### Relations

A relation is a typed set of values. Duplicate values cannot exist — it's a set.

```knot
-- Literal relation (constant — pure, no DB references)
names (["Alice" "Bob" "Carol"])

-- Empty relation
none ([ ])
```

### Declarations

There are five kinds of top-level declarations:

```knot
-- Type alias: just a name for a type
type Person = {name: Text, age: Int 1}
data Priority = Low {} | High {}
data Status = Open {} | Closed {}

-- Source: stored in DB, mutable via `*people = ...`
*people : [Person]
*orders : [{customer: Text, amount: Int 1}]
*todos : [{title: Text, owner: Text, priority: Priority}]

-- View: defined by a query over source relations, settable (writes propagate back)
*openTodos = do
  t <- *todos
  yield {title t.title owner t.owner priority t.priority status (Status.Open {})}

-- Constant: a pure expression with no DB references (zero-argument function)
maxRetries (3)
defaultPriority (Priority.Low {})
httpCodes ([{code 200 name "OK"} {code 404 name "Not Found"}])

-- Derived: references source relations, recomputed on access (read-only)
&seniors = do
  people <- full *people
  yield (base.filter (\p -> p.age > 65) people)
```

The prefix determines mutability, the presence of a body determines whether it's stored or computed:

| Declaration | Category | How the compiler knows |
|---|---|---|
| `*foo : [T]` | Source (persisted) | `*` prefix, no body |
| `*foo = expr` | View (read/write) | `*` prefix, has body |
| `&foo = expr` | Derived (read-only) | `&` prefix, has body |
| `foo = expr` (pure) | Constant | No prefix, no effects inferred |
| `type Foo = T` | Type alias | `type` keyword |

### ADTs as Relation Schemas

Every ADT defines a relation schema. Each constructor is a record variant. A `[T]` holds values of any variant of `T` in the same relation.

```knot
data Shape
  = Circle {radius: Float 1}
  | Rect {width: Float 1, height: Float 1}

*shapes : [Shape]  -- source (no body)
```

Single-variant types are equivalent to bare records:

```knot
-- These are the same:
*people : [{name: Text, age: Int 1}]
*people : [Person]
```

Constructors are the interface for building values, inserting, and querying. The tag/discriminator is an internal storage detail that never appears in the language.

Every constructor requires `{}` — even those with no fields. This keeps the syntax uniform: a constructor is always `Name {fields}`, whether it has fields or not. There is no distinction between "a constructor" and "a constructor applied to a record."

`Bool`, `Maybe`, and `Result` are built-in — their constructors (`True`/`False`, `Nothing`/`Just`, `Ok`/`Err`) are always available without a `data` declaration. Booleans are written `Bool.True {}` and `Bool.False {}` and can be used in `case` patterns. `do`-notation works on `Maybe` and `Result` out of the box via the compiler's structural support (short-circuiting on `Nothing`/`Err`), with no instances to write.

```knot
data Maybe a = Nothing {} | Just {value: a}
data List a = Nil {} | Cons {head: a, tail: List a}
```

### ADTs, Records, and Relations Compose Freely

Any type can be a column type — including sum types, nested records, and nested relations.

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}
  | Blocked {reason: Text, dependencies: [{title: Text}]}

*tickets : [{title: Text, priority: Priority, status: Status}]
```

### Nested Relations

A field can hold a `[]` — a set nested inside a row. This departs from SQL's first normal form restriction.

```knot
type Person = {name: Text, age: Int 1}

*teams : [{name: Text, members: [Person]}]
```

#### Querying into Nested Relations

Bind through multiple levels with `<-`:

```knot
type Person = {name: Text, role: Text}
*teams : [{name: Text, members: [Person]}]

-- All people across all teams
&allMembers = do
  teams <- full *teams
  with {result do
    t <- teams
    m <- t.members
    yield {team t.name member m.name}}
  (do
    yield result)

-- Engineers on large teams
&engineers = do
  teams <- full *teams
  with {result do
    t <- teams
    where (base.count t.members) > 10
    m <- t.members
    where m.role == "engineer"
    yield {team t.name name m.name}}
  (do
    yield result)
```

#### Updating Nested Relations

Write `*rel = ...` with a `map` over the outer relation that transforms the nested relation:

```knot
type Person = {name: Text, role: Text}
*teams : [{name: Text, members: [Person]}]

-- Add a member to a team
addMember \teamName person -> do
  teams <- full *teams
  *teams = do
    t <- teams
    yield ((? (t.name == teamName)
      Bool.True {}  (base.unify t {members (base.union t.members [person])})
      Bool.False {}  t))

-- Remove a member from all teams
removePerson \personName -> do
  teams <- full *teams
  *teams = do
    t <- teams
    yield (base.unify t {members do
      m <- t.members
      where m.name != personName
      yield m})
```

#### Flattening and Nesting

Convert between flat and nested representations:

```knot
type Person = {name: Text, role: Text, age: Int 1}
*teams : [{name: Text, members: [Person]}]

-- Flat relation
type FlatMembership = {team: Text, member: Text, age: Int 1}
*memberships : [FlatMembership]

-- Nest: group a flat relation into nested structure
&nested = do
  memberships <- full *memberships
  with {result do
    t <- do m <- memberships; yield m.team
    yield {name t members (do
      m <- memberships
      where m.team == t
      yield {name m.member age m.age})}}
  (do
    yield result)

-- Flatten: expand nested relation into flat rows
&flat = do
  teams <- full *teams
  with {result do
    t <- teams
    m <- t.members
    yield {team t.name member m.name age m.age}}
  (do
    yield result)
```

#### Deeply Nested Relations

Nesting is arbitrarily deep:

```knot
type Course = {name: Text, students: [{name: Text, grades: [{subject: Text, score: Int 1}]}]}

*departments : [{name: Text, courses: [Course]}]

-- Find all failing grades across all departments
&failing = do
  departments <- full *departments
  with {result do
    d <- departments
    c <- d.courses
    s <- c.students
    g <- s.grades
    where g.score < 50
    yield {dept d.name course c.name student s.name subject g.subject score g.score}}
  (do
    yield result)
```

## Primitives

### `do` Works on Four Types, Structurally

`do` syntax is not trait-polymorphic — there is no `Monad` trait a user type can
implement. Instead the compiler recognizes `do` blocks over exactly four types
and compiles each by a dedicated path:

- **`[a]`** (relation comprehension): `<-` iterates rows, `where` filters,
  `yield` emits a row. Compiled to SQL when over source relations (see
  [Querying](#querying)), otherwise to an in-memory loop.
- **`IO a`**: `<-` sequences effects, `yield` returns a pure value. IO do
  blocks (those containing IO-returning expressions like `*rel`, `println`,
  `readFile`, `now`) use a dedicated IO compilation path that sequences actions
  directly.
- **`Maybe a`**: `<-` unwraps `Just`, short-circuits on `Nothing`.
- **`Result e a`**: `<-` unwraps `Ok`, short-circuits on `Err`.

Higher-order operations (`map`, `fold`, `traverse`, `bind`, …) are ordinary
polymorphic functions over these concrete types — there are no instances to
write, no dictionaries, and no higher-kinded trait bounds. `take`/`drop` work on
both `Text` (characters) and `[a]` (rows) as built-in polymorphic functions, so
the same `take 5 x` works on a string or a relation.

### `do` Desugaring

Do blocks can appear anywhere an expression is expected, including as function arguments: `f do ...` or `f (do ...)`.

- `x <- expr` sequences/binds, according to which of the four types `expr` has
- `yield x` produces a result in that type
- `where cond` filters (relation comprehension) or guards (`Maybe`/`Result`)

IO do blocks (those containing IO-returning expressions like `*rel`, `println`, `readFile`, `now`) are not desugared to a generic bind — they use a dedicated compilation path that sequences IO actions directly.

```knot
*employees : [{name: Text, dept: Text, salary: Int 1}]
*departments : [{name: Text, budget: Int 1}]

-- do with [] (pure relation comprehension over plain values)
richOnes \employees departments -> do
  e <- employees
  d <- departments
  where e.dept == d.name
  yield {name e.name salary e.salary budget d.budget}

-- IO do block (binds from *rel, which returns IO)
&richEmployees = do
  employees <- full *employees
  departments <- full *departments
  yield (richOnes employees departments)

-- do with Maybe
safeDivide \a b -> (? (b == 0)
  Bool.True {}  Maybe.Nothing {}
  Bool.False {}  Maybe.Just {value (a / b)})

tryCompute do
  x <- safeDivide 10 2
  y <- safeDivide x 5
  yield (x + y)

-- do with Result
safeDivideR \a b -> (? (b == 0)
  Bool.True {}  Result.Err {error "div by zero"}
  Bool.False {}  Result.Ok {value (a / b)})

computeR do
  x <- safeDivideR 10 2
  yield (x + 1)
```

### The `[]` Primitives

The only `[]`-specific primitive is the relation write; everything else is a
built-in function over relations:

| Primitive | Form | Description |
|-----------|------|-------------|
| relation write | `*rel = expr  :  IO {} {}` | Make a persistent relation equal to `expr` (use `full *rel = expr` to force a full overwrite) |

The relation operations are built-in functions, not trait methods:

| Operation | Built-in |
|-----------|----------|
| empty relation | `[]` literal |
| `yield` (singleton) | comprehension `yield` |
| `<-` (iterate) | comprehension `<-` |
| `union` | `base.union` |
| `fold` | `base.fold` |
| `map` | `base.map` |

### Derived Operations

These are built-in functions over relations plus the `*rel = expr` write. The compiler recognizes these patterns and executes them as efficient set operations (hash joins, indexed lookups, SQL pushdown, etc.) — the operations define semantics, the runtime chooses the strategy.

**`where`** — keep matching rows (in a comprehension, or via `base.filter`):

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
where \cond -> (? (cond)
  Bool.True {}  yield {}
  Bool.False {}  empty)
```

**`filter`** — filter rows:

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
base.filter \p rel -> do
  x <- rel
  where (p x)
  yield x
```

**`join`** — combine relations on a condition:

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
base.join \a b -> do
  x <- a
  y <- b
  where (x.id == y.id)
  yield {x, y}
```

**`diff`** — rows in one relation but not another:

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
elem = \x rel -> base.fold (\acc r -> acc || r == x) False {} rel

base.diff \a b -> do
  x <- a
  where (not (base.elem x b))
  yield x
```

**`inter`** — rows in both relations:

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
base.inter \a b -> do
  x <- a
  where (base.contains x b)
  yield x
```

**insert** — add a value (union with a singleton). Recognized as an INSERT:

```knot
*rel : [{x: Int 1}]
insertRow \x -> do
  rows <- full *rel
  *rel = base.union rows [{x x}]
```

**delete** — remove matching rows (keep the rest). Recognized as a DELETE:

```knot
*rel : [{x: Int 1}]
deleteWhere \p -> do
  rows <- full *rel
  *rel = base.filter (\x -> not (p x)) rows
```

**update** — transform matching rows. Recognized as an UPDATE:

```knot
*rel : [{x: Int 1}]
updateWhere \p f -> do
  rows <- full *rel
  *rel = base.map (\x -> (? (p x)
    Bool.True {}  f x
    Bool.False {}  x) rows)
```

**`count`**, **`sum`**, **`avg`** — folds:

<!-- doccheck: skip — definitional illustration of a builtin's semantics (redefines a builtin/keyword; not a runnable program). -->
```knot-skip
base.count \rel -> base.fold (\n _ -> n + 1) 0 rel
base.sum \f rel -> base.fold (\acc x -> acc + f x) 0 rel
```

**`match`** — filter a relation of ADT values to one variant, extracting the payload (built-in `base.match`):

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
shapes [(Shape.Circle {radius 1.0}) (Shape.Rect {width 2.0 height 3.0})]
circles (base.match Shape.Circle shapes) -- [Shape] -> [{radius: Float 1}]
```

## Querying

### Comprehensions

Relation comprehensions use `do` syntax with `yield` to produce rows. A read-only comprehension binds each row of a source with `<-` (like `FROM`), filters with `where` (like `WHERE`), and emits rows with `yield`:

```knot
with {
*employees : [{ename: Text, dept: Text, salary: Int 1}]
*departments : [{name: Text, budget: Int 1}]
}
(with {result (do
  e <- *employees
  d <- *departments
  where e.dept == d.name          -- the join condition
  where d.budget > 1000000        -- extra filter
  yield {name e.ename salary e.salary budget d.budget})}
yield result)
```

Binding two sources and relating them with an equi-join predicate (`e.dept == d.name`) is a **join**. A read-only join/filter comprehension like this compiles to a single multi-table SQL `SELECT ... FROM "_knot_employees" AS t0, "_knot_departments" AS t1 WHERE t0."dept" = t1."name" AND t1."budget" > ?`, with the join and filter columns auto-indexed; anything the planner can't translate falls back to an in-memory join with identical results.

Record fields in the `yield` need explicit names — `{name e.ename}`, not `{e.ename}` (there is no field-name shorthand). Relation references (`*rel`, `&rel`) return `IO {} value`; binding them inside a read-only comprehension is handled directly by the compiler, which reads the sources as part of the query.

**`full` reads.** When a relation read is not pushed down to SQL, the whole relation is loaded into memory, and the read must be marked with `full` before the relation name (`rows <- full *rel`). Pushed-down reads — a translatable comprehension, or a recognized aggregate such as `base.count *rel` — need no marker; the compiler reports an error at any read it cannot push down, so a full table load is always explicit in the source. The requirement is exact in both directions: a `full` marker on a read that *is* pushed down is reported as an error (`unnecessary `full``), so the marker appears precisely where a whole-relation load happens.

### Pipe-Forward Composition

Derived combinators like `filter` compose with `|>`:

```knot
*employees : [{name: Text, salary: Int 1}]
&highEarners = do
  employees <- full *employees
  yield (employees
    |> base.filter (\e -> e.salary > 150000)
    |> base.map (\e -> {name e.name salary e.salary}))
```

### Querying by Variant: `match`

`match` filters to one variant and exposes its fields:

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
*shapes : [Shape]

&circles = do                              -- : IO [{radius: Float 1}]
  shapes <- full *shapes
  yield (base.match Shape.Circle shapes)

&rects = do                                -- : IO [{width: Float 1, height: Float 1}]
  shapes <- full *shapes
  yield (base.match Shape.Rect shapes)

&bigCircles = do
  circles <- &circles
  yield (base.filter (\c -> c.radius > 10.0) circles)
```

### Pattern Matching in Comprehensions

Pattern matching on `<-` filters and binds in one step:

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
*shapes : [Shape]
data Status = Blocked {dependencies: [Text]} | Open {}
*tickets : [{title: Text, status: Status}]

&bigCircleAreas = do
  shapes <- full *shapes
  with {result do
    Shape.Circle c <- shapes
    where c.radius > 10.0
    yield {area (3.14159 * c.radius * c.radius)}}
  (do
    yield result)

&blockedDetails = do
  tickets <- full *tickets
  with {result do
    t <- tickets
    Status.Blocked {dependencies deps} <- t.status
    dep <- deps
    yield {title t.title dep dep}}
  (do
    yield result)
```

### Cross-Variant Operations

Operate on the whole relation with `case`:

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}
*shapes : [Shape]
scale \factor -> do
  shapes <- full *shapes
  *shapes = do
    s <- shapes
    yield ((? (s)
      Shape.Circle {radius r}  Shape.Circle {radius (r * factor)}
      Shape.Rect {width w height h}  Shape.Rect {width (w * factor) height (h * factor)}))
```

### Pattern Matching on Relations

```knot
describe \rel -> (? (rel)
  [ ]          "empty"
  [{name n}]   "just " ++ n
  Cons h t     "first of many: " ++ base.show h.name)
```

`[ ]` matches an empty relation. `[p1, p2, ...]` matches a relation with exactly that many rows in any iteration order. `Cons head tail` matches a non-empty relation, binding `head` to the first row and `tail` to the rest (the relation has no inherent order; `Cons` chooses a deterministic iteration order for the match).

### Grouping

`groupBy` partitions a relation by key fields, like SQL's `GROUP BY`. After `groupBy`, the bound variable becomes a sub-relation (the group), enabling aggregation:

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

The key expression is a record literal whose fields select the grouping columns. After `groupBy {owner t.owner}`, `t` is rebound from a single row to a sub-relation of all rows sharing that `owner` value. Field access on a group (e.g. `t.owner`) returns the shared key value. Aggregate functions like `count` operate on the whole group.

Multiple key fields group by their combination:

```knot
*orders : [{region: Text, status: Text, amount: Int 1}]
&summary = do
  with {result do
    o <- full *orders
    groupBy {region o.region status o.status}
    yield {region o.region status o.status total (base.count o)}}
  (do
    yield result)
```

Grouping is executed via SQLite — key columns are inserted into a temp table and sorted with `ORDER BY`, then consecutive rows with matching keys are collected into groups.

## Effects and IO

### Unified IO Model

All state operations in Knot return IO values. `IO a` is a single type
constructor taking one argument — the result type — with no effect-row
parameter:

- **DB operations** return `IO value`. Source refs (`*rel`), derived refs
  (`&rel`), and relation writes (`*rel = expr`, `full *rel = expr`) all
  return `IO value`.
- **External effects** are also plain `IO`: `IO {}`, `IO Text`, `IO Result`,
  `IO (Int Ms)`, `IO Float 1`.

This unified model means all stateful code lives in IO do-blocks, while pure comprehensions over plain values remain non-IO.

### The IO Type

Effectful functions return descriptions of effects (`IO a`) rather than performing them. IO values are thunks that execute when run.

```knot
-- DB operations return IO — a read of *people yields IO [Person].
-- External effects are also plain IO:
base.println : a -> IO {}

base.readFile : Text -> IO Text

base.now : IO (Int Ms)
```

### IO Do-Blocks

IO do-blocks sequence effects. The `<-` operator runs an IO action and binds its result. Since relation references return IO, you bind to get the plain value, then use pure comprehensions:

```knot
with {
type Person = {name: Text, age: Int 1}
*people : [Person]
}
(do
  people <- full *people                       -- IO [Person] → binds [Person]
  content <- base.readFile "input.txt"    -- IO Text → binds Text
  base.println content                     -- IO {}
  t <- base.now                            -- IO (Int Ms) → binds Int Ms
  base.println ("time: " ++ base.show t)
  yield {})
```

The pattern for querying relations is: IO-bind to get the value, then pure comprehension on the plain value:

```knot
*employees : [{name: Text, salary: Int 1}]
&richEmployees = do
  employees <- full *employees       -- IO bind: [Employee] from IO [Employee]
  with {result do              -- pure comprehension on the value
    e <- employees
    where e.salary > 100000
    yield e}
  (do
    yield result)
```

The compiler detects whether a do-block is IO or relational based on the types of bound expressions. IO do-blocks work correctly in all positions, including as arms of a `case`.

### DB Effect Inference

All relation access returns `IO` values. The compiler tracks which functions touch the DB through the `IO` type — no separate effect annotations to write:

```knot
*people : [{name: Text, age: Int 1}]

-- Pure (no DB access)
formatName \n -> base.toUpper (base.take 1 n) ++ base.drop 1 n

-- DB read
&seniors = do
  people <- full *people
  yield (base.filter (\p -> p.age > 65) people)

-- DB write
birthday \name -> do
  people <- full *people
  *people = do
    p <- people
    yield ((? (p.name == name)
      Bool.True {}  (base.unify p {age (p.age + 1)})
      Bool.False {}  p))
```

### Effect Annotations

Effect signatures are inferred; you do not write them. A function that touches the DB returns an `IO` value:

```knot
*people : [{name: Text, age: Int 1}]
birthday \name -> do
  people <- full *people
  *people = do
    p <- people
    yield ((? (p.name == name)
      Bool.True {}  (base.unify p {age (p.age + 1)})
      Bool.False {}  p))
```

### IO and Transactions

`atomic` takes an IO body and runs it in a transaction. `atomic do ...` is a
keyword form (not a `base.` function); the block is an `IO a`.

```knot
*orders : [{item: Text, qty: Int 1}]
-- DB writes go in `atomic`, IO happens after commit
handleOrder \req -> do
  orderId <- atomic do
    orders <- full *orders
    *orders = base.union orders [{item req.body.item qty 1}]
    newOrders <- full *orders
    yield (base.count newOrders)
  base.println ("New order #" ++ base.show orderId)
  yield {orderId orderId}
```

#### `retry`

`retry` is used inside `atomic` blocks to implement STM (Software Transactional Memory) style concurrency. When executed, `retry` causes the transaction to rollback and wait until some relation changes, then re-executes the entire `atomic` block.

```knot
retry : forall a. a  -- bottom type, never returns
```

The compiler enforces that `retry` is only used inside `atomic`. This enables blocking waits on relation state without busy-polling:

```knot
*status : [{ready: Int 1}]
-- Wait until a condition is met
waitForReady (atomic do
  status <- full *status
  base.when (base.count (base.filter (\s -> s.ready == 1) status) == 0) base.retry
  yield status)
```

##### Row-Level Invalidation

A naive STM implementation wakes every parked watcher on every commit. Knot
narrows wakeups to rows the atomic block actually read:

- Codegen inspects each `WHERE`/`single (filter (\r -> r.col OP expr) rows)`
  pattern in the atomic body and, for the SQL-pushed-down query path,
  registers a row-level read filter alongside the broad table read. Supported
  column predicates are equality (`==`/`!=`), ordered comparison
  (`<`/`<=`/`>`/`>=`), and membership (`r.col == a || r.col == b`, treated as
  `IN (a, b)`).
- Each write — INSERT, UPDATE, or DELETE — emits a `WriteEvent` carrying the
  affected rows' column values. The runtime evaluates each watcher's filter
  against the event; only matching watchers wake.
- A bulk replacement (`*rel = ...`) emits `WriteEvent::Bulk` which wakes
  every watcher on that table conservatively, since the row deltas are not
  enumerated.

This means a worker retrying on `WHERE id = 1` is unaffected by writes to
`id = 2`, and a worker retrying on `status IN ("queued", "running")` is
unaffected by writes that leave the status outside that set. The end result is
the contention pattern of a fine-grained lock manager but expressed as
ordinary functional code.

### File System

Built-in functions for file I/O. All return `IO ` values.

| Function | Type | Description |
|----------|------|-------------|
| `readFile` | `Text -> IO Text` | Read entire file contents as text |
| `writeFile` | `Text -> Text -> IO {}` | Write text to a file (creates or overwrites) |
| `appendFile` | `Text -> Text -> IO {}` | Append text to a file |
| `fileExists` | `Text -> IO Bool` | Check whether a path exists |
| `removeFile` | `Text -> IO {}` | Delete a file |
| `listDir` | `Text -> IO [Text]` | List directory entries as a relation of filenames |

```knot
-- Copy a file (IO do-block)
copyFile \src dst -> do
  content <- base.readFile src
  base.writeFile dst content

-- Append a log line
log \msg -> base.appendFile "app.log" (msg ++ "\n")

-- List .knot files
knotFiles do
  files <- base.listDir "."
  yield (base.filter (\f -> base.contains ".knot" f) files)

-- Conditional read
loadConfig \path -> do
  exists <- base.fileExists path
  (? (exists)
    Bool.True {}  base.readFile path
    Bool.False {}  yield "{}")
```

### Concurrency

#### `fork`

`fork` runs an IO action on a new OS thread. It is fire-and-forget — the forked action runs independently and its result is discarded. Each spawned thread gets its own SQLite connection (WAL mode enables concurrent access). The main thread waits for all spawned threads before exiting.

```knot
fork : IO a -> IO {}
```

`fork` spawns an IO action on a new OS thread and returns `IO {}`. Do blocks can be passed as arguments without parentheses: `fork do ...`.

```knot
*counter : [{n: Int 1}]

increment do
  c <- full *counter
  *counter = [{n ((base.fold (\_ x -> x.n) 0 c) + 1)}]

do
  full *counter = [{n 0}]
  base.fork do
    increment
    increment
  base.fork do
    increment
    increment
  -- main waits for all threads before exiting
```

#### `fork` + `atomic` + `retry`

The combination of `fork`, `atomic`, and `retry` enables STM-style concurrent coordination:

```knot
*tasks : [{id: Int 1, status: Text}]

waitForCompletion \id -> atomic do
  tasks <- full *tasks
  with {task do
    t <- tasks
    where t.id == id
    where t.status == "done"
    yield t}
  (do
    base.when ((base.count task) == 0) base.retry
    yield task)

do
  full *tasks = [{id 1 status "pending"}]
  base.fork do
    -- simulate work
    atomic do
      full *tasks = [{id 1 status "done"}]
  result <- waitForCompletion 1
  base.println (base.show result)
  yield {}
```

SQLite WAL mode ensures that concurrent readers and writers do not block each other. Each thread operates on its own connection, and `atomic` provides transaction isolation within a thread.

#### `race`

`race` runs two IO actions concurrently and returns as soon as one wins. The
winner is reported via the built-in `Result a b` ADT —
`Err {error: a}` when the left action wins, `Ok {value: b}` when the right
action wins.

```knot
race : IO a -> IO b -> IO (Result a b)
```

```knot
slow do
  base.sleep (1000 : Int Ms)
  yield "slow"

fast do
  base.sleep (50 : Int Ms)
  yield "fast"

do
  r <- base.race slow fast
  (? (r)
    Result.Err {error a}  base.println ("left won: " ++ a)
    Result.Ok {value b}  base.println ("right won: " ++ b))
  yield {}
```

Cancellation is **cooperative but aggressive**:

- The parent never joins the loser. It returns as soon as it observes a winner, so the loser does not block program progress.
- Each worker carries a thread-local `CancelToken`. `knot_io_run` checks the token between every IO thunk, so the loser unwinds at its next bind/then boundary instead of running to completion.
- Blocking primitives like `sleep` park on the token's condvar instead of `std::thread::sleep`, so a loser stuck in a long sleep wakes immediately when the peer wins.
- The loser is still tracked for the final program-exit join (the runtime waits for every spawned thread before closing the database), so cancellation is best-effort progress rather than thread termination.

`race` is not permitted inside `atomic` blocks — its effects are not part of the savepoint and cannot be rolled back.

### Routes

Routes are first-class. A `route` declaration defines an ADT and its HTTP mapping in one place. Each line maps a method + typed path to a constructor. The constructor's fields are the union of path params, query params, body fields, and request headers.

- `/{name: Type}` in the path — path parameter
- `?{name: Type, ...}` after the path — query parameters
- `{name: Type, ...}` after the verb — request body
- `headers {name: Type, ...}` after query params — request headers
- `headers {name: Type, ...}` after response type — response headers

Header field names use camelCase and auto-convert to HTTP-Header-Case (`authorization` → `Authorization`, `contentType` → `Content-Type`, `xRequestId` → `X-Request-Id`). Optional headers use `Maybe` type.

Constructors are bare names — their fields are automatically the union of path, query, body, and header params.

```knot
route Api where
  GET /todos/{user: Text}?{page: Int 1, limit: Int 1} -> [Todo] = GetTodos
  POST {title: Text, owner: Text, priority: Priority} /todos -> {ok: Bool} = AddTodo
  PUT {owner: Text, person: Text} /todos/{title: Text}/assign -> {ok: Bool} = AssignTodo
```

Handlers are bound per-endpoint with `< API` — the compiler ensures every endpoint has exactly one handler:

```knot
data Priority = Low {} | High {}
data Status = Open {} | Done {}
type Todo = {title: Text, owner: Text, priority: Priority, status: Status}
*todos : [Todo]

route Api where
  GET /todos/{user: Text}?{page: Int 1, limit: Int 1} -> [Todo] = GetTodos
  POST {title: Text, owner: Text, priority: Priority} /todos -> {ok: Bool} = AddTodo
  PUT {owner: Text, person: Text} /todos/{title: Text}/assign -> {ok: Bool} = AssignTodo

add \title owner priority -> do
  todos <- full *todos
  *todos = base.union todos [{title title owner owner priority priority status (Status.Open {})}]

assign \title owner person -> do
  todos <- full *todos
  *todos = do
    t <- todos
    yield ((? (t.title == title)
      Bool.True {}  (base.unify t {owner person})
      Bool.False {}  t))

pendingFor \user page limit -> do
  todos <- full *todos
  with {result (do t <- todos; where t.owner == user; yield t)} yield result

api (< Api
  GetTodos = \{user user page page limit limit} -> do
    todos <- pendingFor user page limit
    yield (Result.Ok {value todos})
  AddTodo = \{title title owner owner priority priority} -> do
    atomic (add title owner priority)
    yield (Result.Ok {value {ok (Bool.True {})}})
  AssignTodo = \{title title owner owner person person} -> do
    atomic (assign title owner person)
    yield (Result.Ok {value {ok (Bool.True {})}}))
```

`< API` produces a value of type `Server API`. Each handler receives the request record (path/query/body/header fields) and returns `Result HttpError T`, where `T` is the response type declared on the endpoint and `HttpError = {status: Int 1, message: Text}`. `listen : Int u -> Server a -> IO {}` binds the server to a port. No string routes, no untyped params, no missing handlers.

#### HTTP Status Codes

Handlers return `Result HttpError T`. `Ok {value: v}` responds with HTTP 200 and serializes `v` as JSON. `Err {error: {status, message}}` responds with the given status code and a JSON error body:

```knot
type Person = {id: Int 1, name: Text, email: Text}
*people : [Person]
route Api where
  GET /users/{id: Int 1} -> Person = GetUser
  POST {name: Text, email: Text} /users -> {name: Text, email: Text} = CreateUser

api (< Api
  GetUser = \{id id} -> do
    users <- full *people
    (? (base.filter (\u -> u.id == id) users)
      [ ]  yield (Result.Err {error {status 404 message "user not found"}})
      Cons u rest  yield (Result.Ok {value u}))
  CreateUser = \{name name email email} -> do
    (? (base.length name == 0)
      Bool.True {}  yield (Result.Err {error {status 400 message "name required"}})
      Bool.False {}  (|
        atomic |
          users <- full *people
          *people = base.union users [{id 0 name name email email}]
        yield (Result.Ok {value {name name email email}})))
```

Status codes are clamped to the range `100..=599`. Common codes: `400` (bad request), `401` (unauthorized), `403` (forbidden), `404` (not found), `409` (conflict), `500` (internal error). The runtime emits `400` automatically for path/query/body/header parsing failures and refinement violations, and `404` for unmatched routes — handlers only need to return `Err` for application-level errors.

#### Typed Responses

Return types can be declared per-endpoint:

```knot
data Priority = Low {} | High {}
route Api where
  GET /todos/{user: Text} -> [{title: Text, priority: Priority}] = GetTodos
  POST {title: Text, owner: Text} /todos -> {ok: Bool} = AddTodo
```

The compiler checks that each handler returns the declared type.

#### Typed Headers

Request and response headers are declared with the `headers` keyword:

```knot
route Api where
  GET /todos headers {authorization: Text} -> [{title: Text}] headers {xTotalCount: Int 1, xPage: Int 1} = GetTodos
  POST {title: Text} /todos headers {authorization: Text, xIdempotencyKey: Text} -> {id: Int 1} = CreateTodo
  GET /health -> {status: Text} = HealthCheck
```

Request headers become constructor fields, just like body/query/path params. The handler destructures them:

```knot
*todos : [{title: Text}]
route Api where
  GET /todos headers {authorization: Text} -> [{title: Text}] headers {xTotalCount: Int 1, xPage: Int 1} = GetTodos
  POST {title: Text} /todos headers {authorization: Text, xIdempotencyKey: Text} -> {id: Int 1} = CreateTodo
  GET /health -> {status: Text} = HealthCheck

addTodo \title -> do
  todos <- full *todos
  *todos = base.union todos [{title title}]
  yield {id 1}

api (< Api
  GetTodos = \{authorization authorization} -> do
    todos <- full *todos
    yield (Result.Ok {value {body todos headers {xTotalCount (base.count todos) xPage 1}}})
  CreateTodo = \{title title authorization authorization xIdempotencyKey xIdempotencyKey} -> do
    r <- addTodo title
    yield (Result.Ok {value r})
  HealthCheck = \{} -> yield (Result.Ok {value {status "ok"}}))
```

When response headers are declared, the success branch wraps a `{body: ..., headers: ...}` record inside `Ok {value: ...}`. Without response headers, `Ok` carries the body directly. Error responses (`Err {error: {status, message}}`) never include custom headers — only the status code and JSON error body.

Optional headers use `Maybe`:

```knot
route Api where
  GET /todos headers {authorization: Maybe Text} -> [Todo] = GetTodos
```

The server gets `Nothing {}` if the header is absent, `Just {value: "..."}` if present. In `fetch`, `Nothing` headers are skipped.

On the fetch side, request headers are sent automatically from constructor fields. When response headers are declared, the result wraps as `{body: ResponseType, headers: {h: T}}`:

<!-- doccheck: skip — fetch+route-constructor form documented but endpoint constructor not yet value-accessible (doc-vs-impl gap). -->
```knot-skip
result <- base.fetch "https://api.example.com" (GetTodos {authorization "Bearer tok"})
-- result : IO (Result ... {body: [Todo], headers: {xTotalCount: Int 1, xPage: Int 1}})
```

#### Rate Limiting

Endpoints may declare a per-route token-bucket rate limit with the `rateLimit` clause, placed after the response type (and after response `headers`, if any) and before `=`. The clause takes a single expression of type `RateLimit input a`:

```knot
type RequestCtx = {
  clientIp: Text,
  receivedAt: Int Ms,
  header: Text -> Maybe Text
}

-- `key` returns Nothing to exempt the request; `header` is a case-insensitive lookup.
type RateLimit input a = {key: input -> RequestCtx -> Maybe a, limit: {requests: Int 1, window: Int Ms}}
```

The `key` function receives the same input record the handler does (path params, query params, body fields, request headers — combined into one record), plus the runtime-supplied `RequestCtx`. Returning `Nothing` exempts the request from rate limiting; returning `Just k` puts the request into the bucket named by `k`. The key type `a` only has to satisfy `Ord` — the runtime serializes it (via `show`) for the SQLite bucket key, so any `Ord` value works (text, int, tuples, records, ADTs).

```knot
byClientIp \input ctx -> Maybe.Just {value ctx.clientIp}

byOwner \{owner owner} ctx -> Maybe.Just {value owner}      -- key by path/query/body field

byApiKey \input ctx -> (? (ctx.header "Authorization")
  Maybe.Just {value k}  Maybe.Just {value k}
  Maybe.Nothing {}  Maybe.Just {value ctx.clientIp})          -- fall back to IP

route Api where
  GET /hello -> {message: Text}
    rateLimit {key byClientIp limit {requests 100 window (60000 : Int Ms)}}
    = Hello

  GET /user/{owner: Text} -> {message: Text}
    rateLimit {key byOwner limit {requests 10 window (60000 : Int Ms)}}
    = User

  POST {body: Text} /upload -> {ok: Bool}
    rateLimit {key byApiKey limit {requests 10 window (60000 : Int Ms)}}
    = Upload

  GET /open -> {message: Text} = Open       -- no clause = unlimited
```

The clause accepts any expression of type `RateLimit input a`, so common keying strategies and limits can be extracted into top-level bindings and reused:

```knot
data Event = Gossip {payload: Text}
serverLimit ({key (\input ctx -> Maybe.Just {value ctx.clientIp}) limit {requests 1000 window (60000 : Int Ms)}})

route Api where
  POST {events: [Event]} /federation/gossip -> {} rateLimit serverLimit = RecvGossip
```

**Algorithm.** A token bucket per `(route, key)` pair, refilled lazily on access at rate `limit.requests / limit.window`. A request that finds at least one token consumes one and is dispatched normally; otherwise the runtime responds `429 Too Many Requests` with body `{"error":"Rate limit exceeded"}` and a `Retry-After: <seconds>` header. The handler is not invoked.

**Storage.** Buckets persist in a SQLite table `_knot_rate_limits` created lazily on first use:

```sql
CREATE TABLE _knot_rate_limits (
  route       TEXT NOT NULL,    -- endpoint constructor name
  key         TEXT NOT NULL,    -- show(keyFn(ctx))
  tokens      REAL NOT NULL,
  last_refill INTEGER NOT NULL,
  PRIMARY KEY (route, key)
) WITHOUT ROWID;
```

The check runs in a `BEGIN IMMEDIATE` transaction so concurrent requests for the same client serialize correctly; different keys do not contend.

**Effects.** Rate limiting reads and writes a hidden internal table.

#### Path Prefixes

Factor out common path prefixes with nesting:

```knot
route Api where
  /todos
    GET                                  /{user: Text}         = GetTodos
    POST {title: Text, owner: Text}      /                     = AddTodo
    PUT  {owner: Text, person: Text}     /{title: Text}/assign = AssignTodo
  /admin
    GET  /stats                    = Stats
    POST /reset                    = Reset
```

Prefixes nest arbitrarily:

```knot
route Api where
  /api/v1
    /users
      GET  /                       = ListUsers
      GET  /{id: Int 1}              = GetUser
      POST {name: Text, email: Text}  /  = CreateUser
    /teams
      GET  /                       = ListTeams
      GET  /{id: Int 1}/members      = GetMembers
```

#### Route Composition

Routes compose — combine multiple route types:

```knot
route TodoApi where
  /todos
    GET                              /{user: Text}  = ListTodos
    POST {title: Text, owner: Text}  /              = CreateTodo

route AdminApi where
  /admin
    GET  /stats         = Stats
    POST /reset         = Reset

route Api = TodoApi | AdminApi
```

### Transaction Boundaries

DB writes within handlers must use `atomic`. IO happens outside `atomic`:

```knot
*orders : [{item: Text, qty: Int 1}]
route Api where
  POST {item: Text, qty: Int 1} /orders -> {orderId: Int 1} = CreateOrder

api (< Api
  CreateOrder = \{item item qty qty} -> do
    orderId <- atomic do
      orders <- full *orders
      *orders = base.union orders [{item item qty qty}]
      newOrders <- full *orders
      yield (base.count newOrders)
    base.println ("New order #" ++ base.show orderId)
    yield (Result.Ok {value {orderId orderId}}))
```

For sub-transaction boundaries:

```knot
*accounts : [{name: Text, balance: Int 1}]
transfer \from to amount -> do
  accounts <- full *accounts
  *accounts = do
    a <- accounts
    yield ((? (a.name == from)
      Bool.True {}  (base.unify a {balance (a.balance - amount)})
      Bool.False {}  ((? (a.name == to)
        Bool.True {}  (base.unify a {balance (a.balance + amount)})
        Bool.False {}  a))))

batchTransfer \transfers ->
  base.map (\t -> atomic (transfer t.from t.to t.amount)) transfers
```

## Persistence

### Mutation

All mutation is done through the `*rel = expr` write, which makes a persistent relation equal to `expr` (there is no `set` keyword — the bare assignment is the write). The compiler recognizes common shapes (`union *rel [...]` → INSERT, conditional `map` → UPDATE, `filter` → DELETE) and emits minimal SQL; otherwise it rewrites the whole relation. `full *rel = expr` forces a full overwrite. Since relation references return IO, you bind to get the current value first:

```knot
*people : [{name: Text, age: Int 1}]

-- Insert: union with a singleton
addPerson do
  people <- full *people
  *people = base.union people [{name "Alice" age 30}]

-- Update: map with a conditional
birthday \name -> do
  people <- full *people
  *people = do
    p <- people
    yield ((? (p.name == "Alice")
      Bool.True {}  (base.unify p {age (p.age + 1)})
      Bool.False {}  p))

-- Delete: filter to keep the rest
removePerson \name -> do
  people <- full *people
  *people = do
    p <- people
    where p.name != name
    yield p
```

### Identity is Structural

Relations are sets. Two rows are the same row iff all their fields are equal. Setting a relation to a value that includes a duplicate is a no-op for that row.

```knot
*people : [{name: Text, age: Int 1}]

-- Adding an already-existing row changes nothing
addAliceTwice do
  people <- full *people
  *people = base.union people [{name "Alice" age 30}]
  people2 <- full *people
  *people = base.union people2 [{name "Alice" age 30}]  -- no change
```

No surrogate IDs, no key declarations. Data identifies itself.

### Indexing

Automatic. The runtime observes query patterns and indexes accordingly. No `CREATE INDEX`, no key declarations.

ADT tables get an index on the discriminator (`_tag`) at table creation time. Columns referenced in `DELETE WHERE`, `UPDATE WHERE`, and `READ WHERE` clauses are auto-indexed on first use. Columns inside the `WHERE` and `ORDER BY` clauses of pushed-down SELECT and aggregate queries — including filtered counts, `sortBy`, and multi-table join keys (`where e.dept == d.name` indexes both join columns) — are auto-indexed as well. The compiler emits `CREATE INDEX IF NOT EXISTS` and per-session bookkeeping deduplicates redundant DDL.

For UUIDv7 primary keys, time-ordered values mean inserts append to the right edge of the index — no random hot-page churn.

## Views

A `*`-prefixed relation with a body is a **view** — a bidirectional query over source relations. Reads compute the query; writes propagate back to the underlying sources.

```knot
data Priority = Low {} | High {}
data Status = Open {} | Closed {}
*employees : [{name: Text, salary: Int 1}]
*todos : [{title: Text, owner: Text, priority: Priority}]

&seniorStaff = do                                            -- read-only (& prefix)
  employees <- full *employees
  yield (base.filter (\e -> e.salary > 100000) employees)

*openTodos = do                                              -- settable (* prefix)
  t <- *todos
  yield {title t.title owner t.owner priority t.priority status (Status.Open {})}
```

### Column Provenance

The compiler tracks each column in a view's `yield`:

| Kind | Syntax in `yield` | On read | On write | In view type |
|------|-------------------|---------|----------|--------------|
| **source** | `t.column` | passthrough | passthrough | yes |
| **constant** | literal or constructor | filter | auto-fill | no |
| **computed** | `expr` | computed | error | yes (read-only) |

For `*openTodos` above:

| Column | Kind | Read | Write |
|--------|------|------|-------|
| `title` | source (`t.title`) | passthrough | passthrough |
| `owner` | source (`t.owner`) | passthrough | passthrough |
| `priority` | source (`t.priority`) | passthrough | passthrough |
| `status` | constant (`Open {}`) | filter | auto-fill |

The constant column is hidden from the type — its value is fixed by definition:

```knot
data Priority = Low {} | High {}
data Status = Open {} | Closed {}
*todos : [{title: Text, owner: Text, priority: Priority}]
*openTodos = do
  t <- *todos
  yield {title t.title owner t.owner priority t.priority status (Status.Open {})}
```

Writing through a view auto-fills constants and propagates source columns:

```knot
data Priority = Low {} | High {}
data Status = Open {} | Closed {}
*todos : [{title: Text, owner: Text, priority: Priority}]
*openTodos = do
  t <- *todos
  yield {title t.title owner t.owner priority t.priority status (Status.Open {})}

-- Insert through view — the compiler constrains status to Open {}
addOpenTodo do
  openTodos <- full *openTodos
  *openTodos = base.union openTodos [{title "New task" owner "Alice" priority (Priority.High {}) status (Status.Open {})}]

-- Delete through view — only affects rows matching the constant
removeAliceTodos do
  openTodos <- full *openTodos
  *openTodos = do
    t <- openTodos
    where t.owner != "Alice"
    yield t
-- Only removes Alice's Open todos; resolved/in-progress ones are untouched
```

Multiple constants create narrow slices:

```knot
data Priority = Low {} | Critical {}
data Status = Open {} | Closed {}
*todos : [{title: Text, owner: Text, priority: Priority, status: Status}]
*criticalOpen = do
  t <- *todos
  yield {title t.title owner t.owner status (Status.Open {}) priority (Priority.Critical {})}

-- Reads: only critical open todos. Writes: the compiler constrains status=Open, priority=Critical.
```

### Recursive Derived Relations

Datalog-style transitive closure:

```knot
*manages : [{manager: Text, report: Text}]

&reportsTo : [{ancestor: Text, descendant: Text}] = do
  manages <- full *manages
  reportsTo <- &reportsTo
  yield (base.union
    (do m <- manages
        yield {ancestor m.manager descendant m.report})
    (do r <- reportsTo
        m <- manages
        where r.descendant == m.manager
        yield {ancestor r.ancestor descendant m.report}))
```

The compiler checks stratification.

## Schema Evolution

The compiler maintains a **schema lock** (`<name>.schema.lock`) that owns the migration history. The lock is **append-only** and is **never written during `knot build`** — it is updated only by `knot lock`. It is valid Knot: a `with { … } (main)` expression holding the current type/data/relation declarations, plus a `migrate_history` section recording each committed migration as raw schema descriptors.

### The Lock

```knot
-- schema.lock (append-only — written only by `knot lock`)
-- Commit to source control. Do not edit by hand.
with {
Priority  Low {}  Medium {}  High {}  Critical {}

Person  {name Text  age (Int 1)  email Text}

Rel Person  *people

migrate_history [
  {source "people"  from "name:text,age:int"  to "name:text,age:int,email:text"  using "\old -> {name old.name  age old.age  email (old.name ++ \"@unknown.com\")}"}
]
}
(main)
```

The source file holds **only the current schema** — never migration history. The lock holds every committed schema and migration, so the full chain can be baked into the binary at build time.

### How It Works

A source's schema lives only in source. To change it:

1. **Edit the schema** and add a pending migration clause — a bare lambda `\old -> <new row>` directly under the `Rel` declaration. There is **no `from`** — it is derived from the lock's last recorded schema. The lambda is a hand-written function applied to each stored row (no auto-derived transforms).
2. **`knot build`** reads the lock as a build input. A pending migration produces an **uncommitted-migration warning**; the binary bakes the committed chain plus the pending step. Running it applies the migration on a **fork** of the database (a content-hashed copy), so the **main DB is never touched** by uncommitted work. Re-running the same pending migration reuses the fork; editing the lambda forks fresh.
3. **`knot lock`** commits: it appends the migration to the lock's `migrate_history`, snapshots the current schema, and strips the migration clause from source. The next run of the committed binary fast-forwards the **main** DB through the locked chain.

On each compile, the compiler diffs the source schema against the lock's last schema:

| Change | Compiler action |
|--------|-----------------|
| Schema unchanged | OK (a staged migration clause here is an error) |
| Schema changed + pending migration clause | Warning: uncommitted migration (run `knot lock`) |
| Schema changed, no migration clause | Error: schema change requires a migration clause |
| Remove relation | Warning (data will be orphaned) |

There are no "safe" auto-applied changes — **every** schema change is an explicit migration with a hand-written lambda.

### Migrations

A change requires a migration clause on the source — a bare lambda, no keywords:

```knot
Person  {name Text  age (Int 1)  email Text}

Rel Person  *people
  \old -> {name old.name  age old.age  email (old.name ++ "@unknown.com")}
```

The lambda is type-checked against the lock's last schema (its input) and the new source schema (its output).

### Runtime

The runtime stores each source's schema in the database. On startup the binary applies its baked migration chain (committed + pending) in order, skipping already-applied steps. With a pending migration present, the binary opens a content-hashed **fork** of the database instead of the main file and logs a warning; the main DB is only migrated once the migration is committed via `knot lock` and the binary rebuilt.

Committed history is append-only: `knot lock` only ever adds to `migrate_history`. A source holds at most one pending migration clause at a time — rewriting it before locking replaces the single pending jump (last-locked → current). History is never edited or removed by the tooling.

## Type System

### Primitive Types

| Type | Description |
|------|-------------|
| `Int 1` | 64-bit signed integer (`i64`); arithmetic is checked and panics on overflow |
| `Float 1` | 64-bit float |
| `Int u` | Integer tagged with a compile-time unit (`Int Usd`) |
| `Float u` | Float tagged with a compile-time unit (`Float M`, `Float (M/S^2)`) |
| `Text` | Unicode string |
| `Bool` | `Bool.True {}` / `Bool.False {}` |
| `Bytes` | Opaque byte string |
| `Uuid` | RFC 9562 UUIDv7 identifier — generated by `randomUuid`, stored as TEXT in SQLite |
| `Maybe a` | `Nothing {}` / `Just {value: a}` |
| `Result e a` | `Err {error: e}` / `Ok {value: a}` |

`Uuid` is a primitive (not an ADT) so it can be the column type of a source relation without any wrapper constructor. UUIDv7 values are time-ordered, which makes them well-suited as primary keys — inserts append to the right edge of any index built on the column.

**SQLite storage.** Source-relation columns map to native SQLite storage classes: `Int` → `INTEGER`, `Float` → `REAL`, `Bool` → `INTEGER` (0/1), `Text`/`Uuid`/enum tags → `TEXT`, `Bytes` → `BLOB`, and nested records / payload-bearing ADTs / non-record nested relations → JSON in a `TEXT` column. (`Int` was historically stored as `TEXT` with a numeric collation — a leftover from when `Int` was a bignum. It is `i64` now, so it uses SQLite's native `INTEGER` directly.)

### Row Polymorphism

Functions can be generic over records and relations with specific fields:

```knot
getName : {name: Text | r} -> Text
getName \r -> r.name
```

### Row-Polymorphic Variants

Functions can be generic over any ADT that has a particular variant:

```knot
data Status = Open {} | Closed {}
countOpen \rel ->
  rel |> base.filter (\r -> (? (r.status)
    Status.Open {}  Bool.True {}
    other  Bool.False {})) |> base.count

-- Inferred: works on tickets, issues, orders — anything with an Open status variant
```

### Explicit Type Arguments

A lambda can bind a type as an explicit, erased parameter with `\(T : Type)`. At the call site the type is passed as a bare uppercase name, before the value arguments:

```knot
apply (\(T : Type) (x : T) -> x)

apply Int 42      -- 42
apply Text "hi"   -- "hi"
```

The witness `T` is a scoped type variable: it constrains exactly where you name it. An unannotated parameter (`\(T : Type) x -> x`) is *not* linked to `T` — write `(x : T)` to connect them. The witness is fully erased at runtime (it has no value); `apply` compiles to the identity function.

```knot
apply Int "hi"    -- type error: expected Int, found Text
```

### Higher-Rank Polymorphism

A parameter's type can itself be polymorphic via an inline `forall`, so the caller must pass a function that works for *every* type, not one fixed type:

```knot
twice (\(f : (forall a. a -> a)) -> (f (f 7)))

twice (\y -> y)            -- 7: the identity is polymorphic
twice (\(x : Int 1) -> x)  -- type error: a monomorphic function cannot be used at every type
```

This is predicative rank-N polymorphism: the `forall` is only allowed in function argument/result positions, never inside a record, variant, or type application.

### Units of Measure

Compile-time units on `Int` and `Float`. Units are fully erased at runtime — no performance cost, no runtime representation. **Every `Int` and `Float` type must carry a unit** — there is no bare `Int`/`Float`. A dimensionless numeric is written explicitly as `Int 1` / `Float 1`.

#### No Declaration Needed

Units are not declared. Any name used in a unit position is a unit — the compiler figures out that something is a unit from how it's used, and since a unit has no body (only a name), there is nothing to declare. Compound units are written inline as expressions:

```knot
height : Float M
height 5.0
force : Float (Kg * M / S^2)
force 9.8
frequency : Float (1 / S)
frequency 60.0
```

#### Type Syntax

Postfix unit argument on numeric types only. Concrete units are uppercase; lowercase names are unit variables (see [Unit Polymorphism](#unit-polymorphism)).

```knot
height : Float M
height 5.0
mass : Float Kg
mass 70.0
speed : Float (M / S)
speed 10.0
force : Float (Kg * M / S^2)
force 9.8
acceleration : Float (M / S^2)
acceleration 3.0
cents : Int Usd
cents 100
```

#### Literal Syntax

Literals are unit-polymorphic and pick up their unit from an annotation:

```knot
distance ((42.0 : Float M))
duration ((3.5 : Float S))
price ((999 : Int Usd))
piVal (3.14159) -- dimensionless (Float 1)
```

#### Arithmetic

`+`/`-` require matching units. `*`/`/` compose units. The compiler normalizes unit expressions algebraically (`M * S / S` → `M`, `M / M` → `1`).

```knot
-- Same-unit addition/subtraction (mismatched units are a type error)
addSame ((10.0 : Float M) + (5.0 : Float M))      -- Float M

-- Unit composition
mulSq ((10.0 : Float M) * (5.0 : Float M))        -- Float (M^2)
divSpeed ((100.0 : Float M) / (10.0 : Float S))   -- Float (M/S)
force ((2.0 : Float Kg) * (9.8 : Float (M / S^2)))  -- Float (Kg * M / S^2)

-- Dimensionless scalars
scaledL (2.0 * (5.0 : Float M))                   -- Float M
scaledR ((5.0 : Float M) / 2.0)                   -- Float M

-- Negation preserves units
negated (-((5.0 : Float M)))                      -- Float M
```

Arbitrary integer powers arise naturally from multiplication: `M * M` = `M^2`, `S * S * S` = `S^3`. Powers can also be written directly in type annotations: `Float (M^2)`, `Float (S^-1)`.

#### Unit Polymorphism

Concrete units are uppercase; lowercase names inside `<...>` are unit variables — no extra syntax needed:

```knot
double : Float u -> Float u
double \x -> x + x

computeSpeed : Float d -> Float t -> Float (d / t)
computeSpeed \distance time -> distance / time
```

Unit variables are inferred like type variables:

```knot
double \x -> x + x
-- inferred: Float u -> Float u  (or Int u -> Int u via Num)
```

#### Conversion

`stripUnit` / `withUnit` (Int 1) and `stripFloatUnit` / `withFloatUnit` (Float 1) are identity functions that exist only for the type checker. Use them to drop a unit tag and re-attach a different one. The result of `withUnit`/`withFloatUnit` carries a free unit variable, so the caller pins the target unit via the surrounding type context (e.g. the function's return signature) or an explicit annotation:

```knot
base.stripUnit       : Int u -> Int 1           -- drop unit from Int 1
base.withUnit        : Int 1 -> Int u           -- attach unit to Int 1
base.stripFloatUnit  : Float u -> Float 1
base.withFloatUnit   : Float 1 -> Float u

toS : Int Ms -> Int S
toS \ms -> base.withUnit (base.stripUnit ms / 1000)

toMiles : Float Km -> Float Mi
toMiles \d -> base.withFloatUnit (base.stripFloatUnit d * 0.621371)
```

The generalized top-level pair `strip : a u -> a 1` and `dress : a 1 -> a u` performs the same rebranding across both numeric types with one call. The `u` is a unit variable of kind `Unit`, so in practice `a` is a unit-carrying numeric (`Int` or `Float`); these are registered directly in the compiler because the surface syntax cannot write `a 1` (`1` is not a type). Both are identity at runtime:

```knot
toS : Int Ms -> Int S
toS \ms -> base.dress (base.strip ms / 1000)
```

Every numeric type carries a unit — a bare `Int` or `Float` is a **compile error**; you must write a unit. Use `Int 1` / `Float 1` for the dimensionless case (e.g. counts, indices). A value of a concrete unit does **not** implicitly convert to the dimensionless form — `x : Float 1; x = (1.5 : Float M)` is a type error (`expected Float 1, found Float M`). Numeric **literals** are unit-polymorphic: `1.5` has type `Float u` for a fresh unit variable, so it flows into whatever unit the context demands (`(1.5 : Float M)`, `sum` over `[Float M]`, or a `Float 1` field) and defaults to dimensionless when unconstrained. These helpers are only needed when you must rebrand a value with a *different* concrete unit.

For explicit unit ascription you can put a type annotation on any expression, either inside parens or as a bare postfix:

```knot
cents ((0 : Int Usd))          -- bare postfix annotation
total ((2.0 + 3.0) : Float M)  -- parenthesized form
```

#### Unit-Preserving Stdlib

`sum`, `avg`, `minOn`, `maxOn`, and binary `min`/`max` preserve units:

```knot
sum   : [a] -> a                        -- direct; use `map` to project first
avg   : (a -> Float u) -> [a] -> Float u
minOn : (a -> b) -> [a] -> b           -- units flow through via b
maxOn : (a -> b) -> [a] -> b
min   : a -> a -> a                     -- binary
max   : a -> a -> a                     -- binary
```

`sum` takes the relation directly — there is no projection argument. To sum a
field of a record relation, project first with `map`:

```knot
rows [{price (10.0 : Float Usd)} {price (20.0 : Float Usd)}]
totalPrice (base.sum (base.map (\r -> r.price) rows))
totalPrice2 (rows |> base.map (\r -> r.price) |> base.sum)
```

#### `show` and Units

`show` on a value with a concrete unit appends the unit string. The compiler knows the unit statically and emits the string as a constant:

```knot
base.show (9.8 : Float (M / S^2))  -- "9.8 M/S^2"
base.show (42.0 : Float M)         -- "42.0 M"
base.show 3.14                     -- "3.14"
```

`Int 1` units are appended the same way, including the built-in `Ms` that clock operations carry — `now : IO Int Ms`, so `show` on a timestamp reads `"1783814121719 Ms"`. Use `stripUnit` to print the bare number.

When the unit is polymorphic (inside a unit-generic function), `show` prints just the number: the function body is compiled once, for every unit its caller may instantiate.

The compiler uses a canonical form for unit strings: alphabetical numerator, alphabetical denominator, powers collapsed. This same canonical form determines type equality (`m * s` = `s * m`).

#### Records, Relations, and SQLite

Units are phantom — SQLite stores raw numbers. Schema descriptors ignore units.

```knot
type Measurement = {distance: Float M, time: Float S}

*measurements : [Measurement]

-- Units flow through queries
&speeds = do
  measurements <- full *measurements
  with {result do
    m <- measurements
    yield {speed (m.distance / m.time)}}   -- Float (M/S)
  (do
    yield result)
```

#### Units and Arithmetic

Units live outside the value level as a compile-time overlay. Arithmetic is
intrinsic (checked/panicking on `Int 1`); the compiler applies unit algebra
rules as an additional layer on top. `+` on `Float M` evaluates directly while
the compiler separately verifies that both operands share the unit `M` and
propagates `M` to the result.

### Refined Types

A refined type is a base type restricted by a predicate. The predicate is an ordinary Knot function (`T -> Bool`) — any pure function works, no restrictions.

#### Declaration

```knot
-- Standalone refined type
type Nat = Int 1 where \x -> x >= 0
type Percentage = Float 1 where \x -> x >= 0.0 && x <= 100.0
type NonEmptyText = Text where \s -> base.length s > 0
type Email = Text where \s -> base.contains "@" s && base.length s >= 3

-- Stacking: inner refinement inherited, predicates conjoin
type Age = Nat where \x -> x <= 150
-- equivalent to: Int 1 where \x -> x >= 0 && x <= 150
```

#### Per-Field Refinements

Refinements attach to individual record fields:

```knot
type Person = {
  name: Text where \s -> length s > 0,
  age: Int 1 where \x -> x >= 0 && x <= 150,
  email: Text
}
```

#### Cross-Field Refinements

A `where` after the closing `}` constrains the whole record. Multiple `where` clauses are conjunctive:

```knot
type DateRange = {
  start: Int 1,
  end: Int 1
} where \r -> r.start <= r.end

type Discount = {
  percent: Float 1 where \x -> x >= 0.0 && x <= 1.0,
  minQty: Int 1 where \x -> x >= 0
} where \d -> d.percent < 0.5 || d.minQty >= 10
```

#### ADT Constructor Refinements

Refinements can appear on constructor fields:

```knot
data Shape
  = Circle {radius: Float 1 where \r -> r > 0.0}
  | Rect {width: Float 1 where \w -> w > 0.0, height: Float 1 where \h -> h > 0.0}
```

#### Relation Constraints

Source declarations carry refinement predicates for each field; cross-relation
constraints (referential integrity, uniqueness) are written as top-level
**subset constraints** with `<=`:

```knot
*people : [{
  name: Text where \s -> length s > 0,
  age: Int 1 where \x -> x >= 0,
  email: Text where \s -> contains "@" s
}]

*orders : [{customer: Text, amount: Int 1 where \x -> x > 0}]

-- Referential integrity: every orders.customer must appear in people.email
*orders.customer <= *people.email

-- Uniqueness: people.email values must not duplicate (relation-on-itself form)
*people <= *people.email
```

Two subset-constraint shapes:

| Form | Meaning |
|------|---------|
| `*sub.field <= *sup.field` | Every value of `sub.field` must appear in `sup.field` (foreign key) |
| `*rel <= *rel.field` | Field values are unique within `rel` |

Field-level and cross-field refinements are enforced row-by-row before each
relation write commits. Subset constraints are enforced by runtime triggers maintained
on the underlying SQLite tables. Either failure mode panics with a refinement
error or a constraint-violation message.

#### Subtyping

`Refined(T, p) <: T`. A refined type is a subtype of its base.

```
Nat <: Int 1
Age <: Nat <: Int 1
```

Upcasting (refined → base) is implicit, no check:

```knot
type Nat = Int 1 where \x -> x >= 0
doubleIt : Int 1 -> Int 1
doubleIt \x -> x + x
-- A Nat argument upcasts implicitly to the Int 1 parameter (Nat <: Int 1)
```

Downcasting (base → refined) requires `refine`. `refine expr` has type `Result RefinementError T` where `T` is the target refined type, inferred from context. If context doesn't determine `T`, it's a type error.

```knot
type Nat = Int 1 where \x -> x >= 0
showNat : Nat -> Text
showNat \n -> base.show n
someInt (5 : Int 1)
```
```knot
with {
type Nat = Int 1 where \x -> x >= 0
showNat : Nat -> Text
showNat \n -> base.show n
someInt (5 : Int 1)
}
(do
  -- With ? (Nat inferred from showNat's parameter):
  (? (refine someInt)
    Result.Ok {value n}  base.println (showNat n)
    Result.Err {error e}  base.println "invalid")
  yield {})
```

Two refined types with the same base but different predicates are unrelated — no subtyping between `Nat` and `Percentage`. Stacked refinements are the exception: `Age <: Nat` because `Age` was defined as `Nat where ...`.

Arithmetic on refined types returns the base type:

```knot
type Nat = Int 1 where \x -> x >= 0

-- x + y : Int 1, not Nat (the compiler does not try to prove the result satisfies the predicate)
sumN do
  x <- refine (3 : Int 1)
  y <- refine (4 : Int 1)
  yield (x + y)
```

#### The `refine` Expression

`refine expr` checks the refinement predicate at runtime. It returns `Result RefinementError T` where `T` is the target refined type, inferred from context:

```knot
type Nat = Int 1 where \x -> x >= 0

-- Target type Nat inferred from the binding annotation
r : Result RefinementError Nat
r (refine 42)
-- r = Result.Ok {value 42}

r2 : Result RefinementError Nat
r2 (refine (0 - 1))
-- r2 = Result.Err {error {typeName "Nat" violations [{field (Maybe.Nothing {}) message "value does not satisfy 'Nat' predicate"}]}}
```

The error type:

```knot
type RefinementError = {
  typeName: Text,
  violations: [{
    field: Maybe Text,   -- Nothing for whole-value, Just "age" for field-level
    message: Text
  }]
}
```

`refine` checks all predicates and reports all violations, not just the first.

In do-blocks over `Result`, `<-` unwraps on `Ok` and short-circuits on `Err`:

```knot
type Nat = Int 1 where \x -> x >= 0
type NonEmptyText = Text where \s -> base.length s > 0

validateOrder : {customer: Text, amount: Int 1} -> Result RefinementError {customer: NonEmptyText, amount: Nat}
validateOrder \raw -> do
  customer <- refine raw.customer    -- NonEmptyText inferred from return type
  amount   <- refine raw.amount      -- Nat inferred from return type
  yield {customer customer amount amount}
```

#### Boundary Checking

Checks happen at two boundaries:

| Boundary | Mechanism | On failure |
|----------|-----------|------------|
| `refine expr` | Explicit coercion | Returns `Result RefinementError T` |
| `*rel = value` | Implicit per-row check | Panics with `RefinementError` |

A relation write panics because constraint violations at the persistence boundary are programming errors — input should be validated with `refine` first, so that error handling happens explicitly before the write rather than at the write itself.

#### Predicates

Predicates in type-level refinements must be **pure** — no IO, no relation references. They are ordinary Knot functions with no restrictions on what pure operations they use (recursion, pattern matching, higher-order functions, etc.).

Predicates in relation `where` clauses follow the same rule — they are pure functions over individual rows. Relational constraints (subset and uniqueness via `<=`) are separate top-level declarations, not predicates.

#### Interaction with Units

Units and refinements are orthogonal — units are compile-time phantom, refinements are runtime-checked:

```knot
type PositiveDistance = Float M where \x -> x > 0.0
type Speed = Float (M/S) where \x -> x >= 0.0
```

#### Schema Evolution

Refinements are part of the schema, tracked in the lockfile:

| Change | Compiler action |
|--------|-----------------|
| Add refinement to existing field | Warning: tightening — existing data may violate |
| Remove refinement | Auto-update lockfile (loosening) |
| Change predicate | Error: require `migrate` |

Adding a refinement to an existing relation requires a validation migration to ensure existing data satisfies the new predicate.

#### Full Example

```knot
type Nat = Int 1 where \x -> x >= 0
type NonEmptyText = Text where \s -> base.length s > 0
type Email = Text where \s -> base.contains "@" s && base.length s >= 3

type Person = {
  name: NonEmptyText,
  age: Nat where \x -> x <= 150,
  email: Email
}

*people : [Person]

*orders : [{
  customer: Email,
  amount: Nat where \x -> x <= 1000000,
  items: [{name: NonEmptyText, qty: Nat where \q -> q > 0}]
}]

-- Referential integrity is a separate top-level subset constraint:
*orders.customer <= *people.email

route Api where
  POST {name: Text, age: Int 1, email: Text} /users -> {ok: Bool, error: Maybe Text} = CreateUser

-- Refine each field; the Person return type pins each refine's target.
mkPerson : Text -> Int 1 -> Text -> Result RefinementError Person
mkPerson \n a e -> do
  name <- refine (n : Text)
  age <- refine (a : Int 1)
  email <- refine (e : Text)
  yield {name name age age email email}

api (< Api
  CreateUser = \{name name age age email email} ->
    (? (mkPerson name age email)
      Result.Ok {value person}  do)
        atomic do
          people <- full *people
          *people = base.union people [person]
        yield (Result.Ok {value {ok (Bool.True {}) error (Maybe.Nothing {})}})
      Result.Err {error e} ->
        with {msg (base.fold (\acc v -> acc ++ v.message ++ "; ") "" e.violations)}
        (do
          yield (Result.Ok {value {ok (Bool.False {}) error (Maybe.Just {value msg})}})))
```

### No User-Facing Traits

Knot has **no trait system you can use**. The parser rejects `trait`
declarations, `impl` blocks, `deriving`, associated types, and `Num a =>`-style
bounds — writing any of them is a syntax error. (Earlier drafts of this design
had a Rust-style trait system; it was removed. The mechanism that replaced it
for *ad-hoc* polymorphism is [Implicit Dictionaries](#implicit-dictionaries-field--t-), below.)

What remains:

- **Operators** (`==` `<` `+` `++` `%` …) are **intrinsic** — the compiler
  evaluates them directly on the supported types (`Int 1`, `Float 1`, `Text`,
  `Bool` for equality; numerics for arithmetic/ordering; `Text` and `[a]` for
  `++`). No type class, no instance, no dictionary.
- **Higher-order functions** (`base.map`, `base.fold`, `base.traverse`,
  `base.bind`, `base.display`, `base.show`, …) are ordinary polymorphic
  functions over concrete types (`[a]`, `Maybe a`, `Result e a`, `IO`, and any
  `a` for `display`/`show`, which work structurally via the runtime). They have
  no bounds.
- **Equality, ordering, and `show`/`display`** work structurally on any value —
  there is nothing to derive.

For polymorphism the *user* controls, see the next section: implicit record
dictionaries, where the operations travel as fields of an ordinary record
resolved from lexical scope.

### Implicit Dictionaries: `(^field : T) =>`

Traits are gone; the replacement is **record dictionaries** — ordinary records
whose fields carry the operations, resolved from lexical scope. The lightest
form is the implicit-field reference `^field` inside a function body, which
projects `field` off whichever in-scope record supplies it. The
`(^field : T) =>` signature constraint lifts that to a *function type*: it
declares that the function needs a dictionary record providing `field` at type
`T`, without naming the record.

```knot
clamp : (^compare : a -> a -> Int 1) => a -> a -> a -> a
clamp \lo hi x -> (? (((^compare) x lo) < 0)
  Bool.True {}  lo
  Bool.False {}  (? (((^compare) x hi) > 0)
    Bool.True {}  hi
    Bool.False {}  x))
```

`clamp` is elaborated to take a hidden leading dictionary parameter (a record
`{compare : a -> a -> Int 1}`); each `(^compare)` in the body reads the
`compare` field of that record. At a **full-arity callsite** the compiler
searches the lexical scope for a record supplying `compare` at the required
type and splices it in as the leading argument:

```knot
with {
clamp : (^compare : a -> a -> Int 1) => a -> a -> a -> a
clamp \lo hi x -> (? (((^compare) x lo) < 0)
  Bool.True {}  lo
  Bool.False {}  (? (((^compare) x hi) > 0)
    Bool.True {}  hi
    Bool.False {}  x))

intOrd ({compare (\a b -> (? (a > b)
  Bool.True {}  1
  Bool.False {}  (? (a < b)
    Bool.True {}  (0 - 1)
    Bool.False {}  0)))})
textOrd ({compare (\a b -> (? (a > b)
  Bool.True {}  1
  Bool.False {}  (? (a < b)
    Bool.True {}  (0 - 1)
    Bool.False {}  0)))})
intOrdDesc ({compare (\a b -> (? (a < b)
  Bool.True {}  1
  Bool.False {}  (? (a > b)
    Bool.True {}  (0 - 1)
    Bool.False {}  0)))})
}
(do
  base.println (base.show (clamp 0 10 42))           -- resolves to intOrd     → 10
  base.println (clamp "a" "m" "z")                   -- resolves to textOrd    → "m"
  base.println (base.show (with intOrdDesc (clamp 0 10 42)))  -- `with` shadows outer → 0
  yield {})
```

Resolution is **per-callsite** (the dictionary is chosen by the instantiation —
`a` becomes `Int` vs `Text`) and **lexical** (the innermost scope wins; a `with`
frame binding `compare` shadows outer records). If no in-scope record supplies
the field, the callsite is a compile error
(`no in-scope record supplies an implicit dictionary field 'compare'`).

Current limitation: only **full-arity** callsites resolve a dictionary. Passing
a constrained function partially applied (e.g. `map (clamp lo hi) xs`) does not
yet thread the dictionary — it must be applied to all its explicit arguments at
once.

#### The seeded conversion dictionary: `base.morph`

The prelude ships one implicit dictionary for the common case: **`base.morph`**,
a nested record of type-directed conversions. Each `<from>To<to>` field holds an
`into : S -> T` (e.g. `base.morph.textToInt.into : Text -> Maybe (Int 1)`).
Because `base` is bound in every program's top scope, the `^into` projection
resolves these conversions with no explicit dictionary:

```knot
asInt : Text -> Maybe (Int 1)
asInt (\s -> (^into) s)     -- resolves base.morph.textToInt.into
```

The conversion is chosen by **both** the argument type and the expected result
type, so the result must be pinned (an annotation, or use at a concrete type) —
`(^into) "42"` alone is ambiguous and a compile error. Each morph field carries
an explicit concrete signature: an un-annotated body whose type would stay
polymorphic (e.g. `\n -> show n`, inferring `a -> Text`) cannot be dispatched
type-directedly and would silently match the wrong conversion. See
[base.md](base.md#morphs-basemorph) for the full conversion table.

### Type Inference

Full Hindley-Milner style inference extended with row polymorphism (record fields and unit variables) and implicit-dictionary constraints. Type signatures are always optional — the compiler infers everything from usage.

## Full Example

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

type Todo = {title: Text, owner: Text, priority: Priority, status: Status}
*todos : [Todo]

route Api where
  GET /todos/{user: Text} -> [Todo] = GetTodos
  POST {title: Text, owner: Text, priority: Priority} /todos -> {ok: Bool} = AddTodo
  PUT {owner: Text, person: Text} /todos/{title: Text}/assign -> {ok: Bool} = AssignTodo
  PUT {owner: Text, msg: Text} /todos/{title: Text}/resolve -> {ok: Bool} = ResolveTodo

formatTitle \title -> base.toUpper (base.take 1 title) ++ base.drop 1 title

pendingFor \user -> do
  todos <- full *todos
  with {result do
    t <- todos
    where t.owner == user
    Status.Open {} <- t.status
    yield t}
  (do
    yield result)

add \title owner priority -> do
  todos <- full *todos
  *todos = base.union todos [{title (formatTitle title) owner owner priority priority status (Status.Open {})}]

assign \title owner person -> do
  todos <- full *todos
  *todos = do
    t <- todos
    yield ((? (t.title == title && t.owner == owner)
      Bool.True {}  (base.unify t {status (Status.InProgress {assignee person})})
      Bool.False {}  t))

resolve \title owner msg -> do
  todos <- full *todos
  *todos = do
    t <- todos
    yield ((? (t.title == title && t.owner == owner)
      Bool.True {}  (base.unify t {status (Status.Resolved {resolution msg})})
      Bool.False {}  t))

api (< Api
  GetTodos = \{user user} -> do
    todos <- pendingFor user
    yield (Result.Ok {value todos})
  AddTodo = \{title title owner owner priority priority} -> do
    atomic (add title owner priority)
    yield (Result.Ok {value {ok (Bool.True {})}})
  AssignTodo = \{title title owner owner person person} -> do
    atomic (assign title owner person)
    yield (Result.Ok {value {ok (Bool.True {})}})
  ResolveTodo = \{title title owner owner msg msg} -> do
    atomic (resolve title owner msg)
    yield (Result.Ok {value {ok (Bool.True {})}}))
```