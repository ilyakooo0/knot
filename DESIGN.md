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
names = ["Alice", "Bob", "Carol"]

-- Empty relation
none = []
```

### Declarations

There are five kinds of top-level declarations:

```knot
-- Source: stored in DB, mutable via `set`
*people : [Person]
*orders : [{customer: Text, amount: Int}]

-- View: defined by a query over source relations, settable (writes propagate back)
*openTodos = do
  t <- *todos
  yield {title: t.title, owner: t.owner, priority: t.priority, status: Open {}}

-- Constant: a pure expression with no DB references (zero-argument function)
maxRetries = 3
defaultPriority = Low {}
httpCodes = [{code: 200, name: "OK"}, {code: 404, name: "Not Found"}]

-- Derived: references source relations, recomputed on access (read-only)
&seniors = *people |> filter (\p -> p.age > 65)

-- Type alias: just a name for a type
type Person = {name: Text, age: Int}
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
  = Circle {radius: Float}
  | Rect {width: Float, height: Float}

*shapes : [Shape]  -- source (no body)
```

Single-variant types are equivalent to bare records:

```knot
-- These are the same:
*people : [{name: Text, age: Int}]
*people : [Person]
```

Constructors are the interface for building values, inserting, and querying. The tag/discriminator is an internal storage detail that never appears in the language.

Every constructor requires `{}` — even those with no fields. This keeps the syntax uniform: a constructor is always `Name {fields}`, whether it has fields or not. There is no distinction between "a constructor" and "a constructor applied to a record."

`Bool`, `Maybe`, and `Result` are built-in — their constructors (`True`/`False`, `Nothing`/`Just`, `Ok`/`Err`) are always available without a `data` declaration. `True {}` and `False {}` are interchangeable with the `true`/`false` literals and can be used in `case` patterns.

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
type Person = {name: Text, age: Int}

*teams : [{name: Text, members: [Person]}]
```

#### Querying into Nested Relations

Bind through multiple levels with `<-`:

```knot
-- All people across all teams
&allMembers = do
  t <- *teams
  m <- t.members
  yield {team: t.name, member: m.name}

-- Engineers on large teams
&engineers = do
  t <- *teams
  where (count t.members) > 10
  m <- t.members
  where m.role == "engineer"
  yield {team: t.name, name: m.name}
```

#### Updating Nested Relations

Use `set` with a `map` over the outer relation that transforms the nested relation:

```knot
-- Add a member to a team
addMember = \teamName person ->
  set *teams = do
    t <- *teams
    yield (if t.name == teamName
      then {t | members: union t.members [person]}
      else t)

-- Remove a member from all teams
removePerson = \personName ->
  set *teams = do
    t <- *teams
    yield {t | members: do
      m <- t.members
      where m.name != personName
      yield m}
```

#### Flattening and Nesting

Convert between flat and nested representations:

```knot
-- Flat relation
type FlatMembership = {team: Text, member: Text, age: Int}
*memberships : [FlatMembership]

-- Nest: group a flat relation into nested structure
&nested = do
  t <- do m <- *memberships; yield m.team
  yield {name: t, members: do
    m <- *memberships
    where m.team == t
    yield {name: m.member, age: m.age}}

-- Flatten: expand nested relation into flat rows
&flat = do
  t <- *teams
  m <- t.members
  yield {team: t.name, member: m.name, age: m.age}
```

#### Deeply Nested Relations

Nesting is arbitrarily deep:

```knot
type Course = {name: Text, students: [{name: Text, grades: [{subject: Text, score: Int}]}]}

*departments : [{name: Text, courses: [Course]}]

-- Find all failing grades across all departments
&failing = do
  d <- *departments
  c <- d.courses
  s <- c.students
  g <- s.grades
  where g.score < 50
  yield {dept: d.name, course: c.name, student: s.name, subject: g.subject, score: g.score}
```

## Primitives

### Trait Hierarchy

`do` syntax is not hardcoded to `[]`. It desugars to trait methods, so any type implementing `Monad` gets `do`/`yield`/`<-` for free. This requires higher-kinded types in the type system.

```knot
trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b

trait Functor f => Applicative (f : Type -> Type) where
  yield : a -> f a
  ap    : f (a -> b) -> f a -> f b

trait Applicative m => Monad (m : Type -> Type) where
  bind : (a -> m b) -> m a -> m b

trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt   : f a -> f a -> f a

trait Foldable (t : Type -> Type) where
  fold : (b -> a -> b) -> b -> t a -> b
```

### `do` Desugaring

`do` syntax works for any `Monad`. Do blocks can appear anywhere an expression is expected, including as function arguments: `f do ...` or `f (do ...)`.

- `x <- expr` desugars to `bind (\x -> ...) expr`
- `yield x` is `Applicative.yield`
- `where cond` desugars to `if cond then yield {} else empty` (requires `Alternative`)

IO do blocks (those containing IO-returning builtins like `println`, `readFile`, `now`) are not desugared — they use a dedicated compilation path that sequences IO actions directly.

```knot
-- do with [] (relation comprehension)
&richEmployees = do
  e <- *employees
  d <- *departments
  where e.dept == d.name
  yield {e.name, e.salary, d.budget}

-- do with Maybe
safeDivide = \a b -> do
  where b != 0
  yield (a / b)

-- do with Result
parseConfig = \text -> do
  json <- parseJson text
  name <- getField "name" json
  yield name
```

### `[]` Trait Implementations

`[]` implements the full hierarchy:

```knot
impl Functor [] where
  map f rel = do x <- rel; yield (f x)

impl Applicative [] where
  yield x = [x]
  ap fs xs = do f <- fs; x <- xs; yield (f x)

impl Monad [] where
  bind = ...  -- built-in

impl Alternative [] where
  empty = []
  alt = union

impl Foldable [] where
  fold = ...  -- built-in
```

### The Only `[]`-Specific Primitive

| Primitive | Type | Description |
|-----------|------|-------------|
| `set` | `[a] -> [a] -> {}` | Set a persistent relation to a new value |

Everything else comes from traits:

| Operation | Trait method |
|-----------|-------------|
| `empty` | `Alternative.empty` |
| `yield` | `Applicative.yield` |
| `<-` (bind) | `Monad.bind` |
| `union` | `Alternative.alt` |
| `fold` | `Foldable.fold` |
| `map` | `Functor.map` |

### Derived Operations

Everything else is built from trait methods + `set`. The compiler recognizes these patterns and executes them as efficient set operations (hash joins, indexed lookups, etc.) — the traits define semantics, the runtime chooses the strategy.

**`where`** — conditional empty (requires `Alternative`):

```knot
where = \cond -> if cond then yield {} else empty
```

**`filter`** — filter rows:

```knot
filter = \p rel -> do
  x <- rel
  where (p x)
  yield x
```

**`join`** — combine relations on a condition:

```knot
join = \a b -> do
  x <- a
  y <- b
  where (x.id == y.id)
  yield {x, y}
```

**`diff`** — rows in one relation but not another:

```knot
contains = \x rel -> fold (\acc r -> acc || r == x) False {} rel

diff = \a b -> do
  x <- a
  where (not (contains x b))
  yield x
```

**`inter`** — rows in both relations:

```knot
inter = \a b -> do
  x <- a
  where (contains x b)
  yield x
```

**`insert`** — add a value (union with a singleton):

```knot
insert = \x rel -> set rel (union rel (yield x))
```

**`delete`** — remove matching rows:

```knot
delete = \p rel -> set rel (filter (\x -> not (p x)) rel)
```

**`update`** — transform matching rows:

```knot
update = \p f rel -> set rel (map (\x -> if p x then f x else x) rel)
```

**`count`**, **`sum`**, **`avg`** — folds:

```knot
count = \rel -> fold (\n _ -> n + 1) 0 rel
sum = \f rel -> fold (\acc x -> acc + f x) 0 rel
```

**`match`** — filter to one variant:

```knot
match = \Circle shapes -> do
  Circle c <- shapes
  yield c
```

## Querying

### Comprehensions

Relation comprehensions use `do` syntax with `yield` to produce rows:

```knot
&richEmployees = do
  e <- *employees
  d <- *departments
  where e.dept == d.name
  where d.budget > 1_000_000
  yield {e.name, e.salary, d.budget}
```

`<-` draws from a relation (like a `FROM` clause). `where` filters (like a `WHERE` clause). `yield` emits a row into the result relation. This desugars to relational algebra so the optimizer can work on it.

### Pipe-Forward Composition

Derived combinators like `filter` compose with `|>`:

```knot
&highEarners =
  *employees
    |> filter (\e -> e.salary > 150000)
    |> map (\e -> {name: e.name, salary: e.salary})
```

### Querying by Variant: `match`

`match` filters to one variant and exposes its fields:

```knot
&circles = *shapes |> match Circle    -- : [{radius: Float}]
&rects   = *shapes |> match Rect      -- : [{width: Float, height: Float}]

&bigCircles = &circles |> filter (\c -> c.radius > 10)
```

### Pattern Matching in Comprehensions

Pattern matching on `<-` filters and binds in one step:

```knot
&bigCircleAreas = do
  Circle c <- *shapes
  where c.radius > 10
  yield {area: pi * c.radius * c.radius}

&blockedDetails = do
  t <- *tickets
  Blocked {dependencies} <- t.status
  dep <- dependencies
  yield {t.title, dep}
```

### Cross-Variant Operations

Operate on the whole relation with `case`:

```knot
scale = \factor ->
  set *shapes = do
    s <- *shapes
    yield (case s of
      Circle {radius}       -> Circle {radius: radius * factor}
      Rect {width, height}  -> Rect {width: width * factor, height: height * factor})
```

### Pattern Matching on Relations

```knot
describe = \rel -> case rel of
  []          -> "empty"
  [{name: n}] -> "just " ++ n
  _           -> show (count rel) ++ " rows"
```

### Grouping

`groupBy` partitions a relation by key fields, like SQL's `GROUP BY`. After `groupBy`, the bound variable becomes a sub-relation (the group), enabling aggregation:

```knot
&workload = do
  t <- *todos
  where t.done == 0
  groupBy {t.owner}
  yield {owner: t.owner, count: count t}
```

The key expression is a record literal whose fields select the grouping columns. After `groupBy {t.owner}`, `t` is rebound from a single row to a sub-relation of all rows sharing that `owner` value. Field access on a group (e.g. `t.owner`) returns the shared key value. Aggregate functions like `count` operate on the whole group.

Multiple key fields group by their combination:

```knot
&summary = do
  o <- *orders
  groupBy {o.region, o.status}
  yield {region: o.region, status: o.status, total: count o}
```

Grouping is executed via SQLite — key columns are inserted into a temp table and sorted with `ORDER BY`, then consecutive rows with matching keys are collected into groups.

## Effects and the IO Monad

### Two Kinds of Effects

Knot distinguishes two kinds of effects:

1. **DB effects** (`reads`, `writes`) — implicit, inferred by the compiler, part of the relational core. These are not wrapped in IO.
2. **External effects** (`console`, `fs`, `network`, `clock`, `random`) — tracked in the `IO` type. Functions that perform external effects return `IO {effects} a` values instead of executing immediately.

### The IO Type

External effects are pure — effectful functions return descriptions of effects (`IO {effects} a`) rather than performing them. IO values are thunks that execute when run.

```knot
-- println returns an IO action, doesn't print immediately
println : a -> IO {console} {}

-- readFile returns an IO action
readFile : Text -> IO {fs} Text

-- now returns an IO action
now : IO {clock} Int
```

### IO Do-Blocks

IO do-blocks sequence effects. The `<-` operator runs an IO action and binds its result:

```knot
main = do
  content <- readFile "input.txt"    -- IO {fs} Text → binds Text
  println content                     -- IO {console} {}
  t <- now                            -- IO {clock} Int → binds Int
  println ("time: " ++ show t)
  yield {}
-- overall type: IO {fs, console, clock} {}
```

### Relation Comprehensions Are Unaffected

Relation do-blocks (`<-` from `[T]`) still work exactly as before — no IO wrapping:

```knot
&seniors = do
  p <- *people          -- [Person] → binds Person
  where p.age > 65
  yield p
-- type: [Person]
```

The compiler detects whether a do-block is IO or relational based on the types of bound expressions. IO do-blocks work correctly in all positions, including as branches of `if`/`then`/`else`.

### DB Effect Inference

DB effects are still inferred as fine-grained capabilities:

```knot
-- Pure (inferred: no effects)
formatName = \n -> toUpper (take 1 n) ++ drop 1 n

-- DB read (inferred: {reads *people})
&seniors = *people |> filter (\p -> p.age > 65)

-- DB write (inferred: {reads *people, writes *people})
birthday = \name ->
  set *people = do
    p <- *people
    yield (if p.name == name then {p | age: p.age + 1} else p)
```

### Effect Annotations

Effect signatures are inferred but can be written explicitly:

```knot
birthday : {reads *people, writes *people} Text -> {}
birthday = \name ->
  set *people = do
    p <- *people
    yield (if p.name == name then {p | age: p.age + 1} else p)
```

If the body uses a capability not listed in the signature, the compiler rejects it.

### IO and Transactions

DB writes are transactional — they roll back on failure. IO cannot be undone. The compiler enforces: **IO effects and DB writes cannot mix in the same `atomic` block**.

```knot
-- DB writes go in `atomic`, IO happens after commit
handleOrder = \req -> do
  orderId <- atomic do
    set *orders = union *orders [{item: req.body.item, qty: 1}]
    yield (count *orders)
  println ("New order #" ++ show orderId)
  yield {orderId}
```

### File System

Built-in functions for file I/O. All return `IO {fs}` values.

| Function | Type | Description |
|----------|------|-------------|
| `readFile` | `Text -> IO {fs} Text` | Read entire file contents as text |
| `writeFile` | `Text -> Text -> IO {fs} {}` | Write text to a file (creates or overwrites) |
| `appendFile` | `Text -> Text -> IO {fs} {}` | Append text to a file |
| `fileExists` | `Text -> IO {fs} Bool` | Check whether a path exists |
| `removeFile` | `Text -> IO {fs} {}` | Delete a file |
| `listDir` | `Text -> IO {fs} [Text]` | List directory entries as a relation of filenames |

```knot
-- Copy a file (IO do-block)
copyFile = \src dst -> do
  content <- readFile src
  writeFile dst content

-- Append a log line
log = \msg -> appendFile "app.log" (msg ++ "\n")

-- List .knot files
knotFiles = do
  files <- listDir "."
  yield (filter (\f -> contains ".knot" f) files)

-- Conditional read
loadConfig = \path -> do
  exists <- fileExists path
  if exists
    then readFile path
    else yield "{}"
```

### Routes

Routes are first-class. A `route` declaration defines an ADT and its HTTP mapping in one place. Each line maps a method + typed path to a constructor. The constructor's fields are the union of path params, query params, and body fields.

- `/{name: Type}` in the path — path parameter
- `?{name: Type, ...}` after the path — query parameters
- `{name: Type, ...}` after the verb — request body

Constructors are bare names — their fields are automatically the union of path, query, and body params.

```knot
route Api where
  GET                                          /todos/{user: Text}?{page: Int, limit: Int}  = GetTodos
  POST {title: Text, owner: Text, priority: Priority}  /todos                               = AddTodo
  PUT  {owner: Text, person: Text}             /todos/{title: Text}/assign                   = AssignTodo
  GET                                          /workload                                     = GetWorkload
```

Dispatch is pattern matching — the compiler ensures exhaustive handling:

```knot
serve : Api -> Response
serve = \req -> case req of
  GetTodos {user, page, limit} -> pendingFor user page limit
  AddTodo {title, owner, priority} -> do
    atomic (add title owner priority)
    yield {ok: True {}}
  AssignTodo {title, owner, person} -> do
    atomic (assign title owner person)
    yield {ok: True {}}
  GetWorkload {} -> &workload

main = listen 8080 serve
```

No string routes, no untyped params, no missing handlers.

#### Typed Responses

Return types can be declared per-endpoint:

```knot
route Api where
  GET                              /todos/{user: Text} -> [{title: Text, priority: Priority}]  = GetTodos
  POST {title: Text, owner: Text}  /todos              -> {ok: Bool}                              = AddTodo
  GET                              /workload           -> [{owner: Text, count: Int}]           = GetWorkload
```

The compiler checks that each `serve` branch returns the declared type.

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
      GET  /{id: Int}              = GetUser
      POST {name: Text, email: Text}  /  = CreateUser
    /teams
      GET  /                       = ListTeams
      GET  /{id: Int}/members      = GetMembers
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
serve = \req -> case req of
  CreateOrder {item, qty} -> do
  orderId <- atomic do
    set *orders = union *orders [{item, qty}]
    yield (count *orders)
  println ("New order #" ++ show orderId)
  yield {orderId}
```

For sub-transaction boundaries:

```knot
batchTransfer = \transfers ->
  map (\t -> atomic (transfer t.from t.to t.amount)) transfers
```

## Persistence

### Mutation

All mutation is done through `set`, which replaces a persistent relation with a new value. The runtime diffs the old and new sets to apply minimal changes.

```knot
-- Insert: union with a singleton
set *people = union *people [{name: "Alice", age: 30}]

-- Update: map with a conditional
set *people = do
  p <- *people
  yield (if p.name == "Alice" then {p | age: p.age + 1} else p)

-- Delete: filter to keep the rest
set *people = filter (\p -> p.age >= 0) *people
```

### Identity is Structural

Relations are sets. Two rows are the same row iff all their fields are equal. Setting a relation to a value that includes a duplicate is a no-op for that row.

```knot
-- Adding an already-existing row changes nothing
set *people = union *people [{name: "Alice", age: 30}]
set *people = union *people [{name: "Alice", age: 30}]  -- no change
```

No surrogate IDs, no key declarations. Data identifies itself.

### Subset Constraints (Optional)

For referential integrity and uniqueness, express value relationships:

```knot
*orders : [{customer: Text, amount: Int}]
*users : [{email: Text, name: Text}]

-- Referential integrity: every order's customer must appear in people's names
*orders.customer <= *people.name

-- Uniqueness: email determines the full row (each email maps to at most one row)
*users <= *users.email
```

### Indexing

Automatic. The runtime observes query patterns and indexes accordingly. No `CREATE INDEX`, no key declarations.

## Views

A `*`-prefixed relation with a body is a **view** — a bidirectional query over source relations. Reads compute the query; writes propagate back to the underlying sources.

```knot
&seniorStaff = filter (\e -> e.salary > 100000) *employees  -- read-only (& prefix)

*openTodos = do                                              -- settable (* prefix)
  t <- *todos
  yield {title: t.title, owner: t.owner, priority: t.priority, status: Open {}}
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
*openTodos : [{title: Text, owner: Text, priority: Priority}]
```

Writing through a view auto-fills constants and propagates source columns:

```knot
-- Insert through view — status auto-filled as Open {}
set *openTodos = union *openTodos [{title: "New task", owner: "Alice", priority: High {}}]
-- Compiler rewrites →
-- set *todos = union *todos [{title: "New task", owner: "Alice", priority: High {}, status: Open {}}]

-- Delete through view — only affects rows matching the constant
set *openTodos = do
  t <- *openTodos
  where t.owner != "Alice"
  yield t
-- Only removes Alice's Open todos; resolved/in-progress ones are untouched
```

Multiple constants create narrow slices:

```knot
*criticalOpen = do
  t <- *todos
  yield {title: t.title, owner: t.owner, status: Open {}, priority: Critical {}}

-- Type: [{title: Text, owner: Text}]
-- Reads: only critical open todos
-- Writes: auto-fills status=Open, priority=Critical
```

### Recursive Derived Relations

Datalog-style transitive closure:

```knot
*manages : [{manager: Text, report: Text}]

&reportsTo : [{ancestor: Text, descendant: Text}] =
  union
    (do m <- *manages
        yield {m.manager, m.report})
    (do r <- &reportsTo
        m <- *manages
        where r.descendant == m.manager
        yield {r.ancestor, m.report})
```

The compiler checks stratification.

## Schema Evolution

The compiler maintains a **schema lockfile** (`schema.lock`) that records the persisted schema. The lockfile uses the same syntax as source code — it's valid Knot containing only type declarations, data definitions, and relation signatures.

### The Lockfile

```knot
-- schema.lock (auto-generated, do not edit)
-- Commit to source control.

data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

*people : [{name: Text, age: Int, email: Text}]

*todos : [{title: Text, owner: Text, priority: Priority, status: Status}]

migrate *people
  from {name: Text, age: Int}
  to   {name: Text, age: Int, email: Text}
  using (\old -> {old | email: old.name ++ "@unknown.com"})
```

Since it's valid Knot, it can be parsed by the same compiler frontend — no separate schema format. Migrations are recorded in the lockfile so the compiler can detect if a migration is accidentally removed from source.

### How It Works

On each compile, the compiler diffs the source types against `schema.lock`:

| Change | Compiler action |
|--------|-----------------|
| Add `Maybe` field to record | Auto-update lockfile |
| Add variant to ADT | Auto-update lockfile |
| Add new relation | Auto-add to lockfile |
| Add new `migrate` block | Auto-add to lockfile |
| Remove field or variant | Error: require `migrate` |
| Add non-Maybe field | Error: require `migrate` |
| Change field type | Error: require `migrate` |
| Remove a `migrate` block | Error: migration exists in lockfile |
| Remove relation | Warning (data will be orphaned) |

### Migrations

Breaking changes require a `migrate` block:

```knot
migrate *people
  from {name: Text, age: Int}
  to   {name: Text, age: Int, email: Text}
  using (\old -> {old | email: old.name ++ "@unknown.com"})
```

ADT migrations use pattern matching:

```knot
migrate *todos
  from {title: Text, owner: Text, priority: Priority, status: <Open {} | InProgress {assignee: Text} | Resolved {resolution: Text}>}
  to   {title: Text, owner: Text, priority: Priority, status: Status}
  using (\old -> {old | status: case old.status of
    InProgress {assignee} -> Resolved {resolution: "closed by " ++ assignee}
    other                 -> other})
```

After a successful compile, `schema.lock` is updated.

### Runtime

The runtime stores the compiled schema version in the database. On startup it compares against the stored version and applies any pending migrations in order. Already-applied migrations are skipped.

`migrate` blocks accumulate in source code. The lockfile tracks all migrations — if a migration present in the lockfile is missing from source, the compiler rejects the build. This prevents accidental deletion. Old migrations can be pruned only by explicitly removing them from both source and lockfile.

## Temporal Queries

Optional history tracking:

```knot
*employees : [{name: Text, salary: Int}]
  with history

salaryLastYear = \name -> do
  t <- now
  yield (*employees @(t - 365 days)
    |> filter (\e -> e.name == name)
    |> map (\e -> e.salary)
    |> single)
```

## Type System

### Row Polymorphism

Functions can be generic over records and relations with specific fields:

```knot
getName : {name: Text | r} -> Text
getName = \r -> r.name
```

### Row-Polymorphic Variants

Functions can be generic over any ADT that has a particular variant:

```knot
countOpen = \rel ->
  rel |> filter (\r -> case r.status of Open {} -> True {}; _ -> False {}) |> count

-- Inferred: [{status: <Open {} | r> | s}] -> Int
-- Works on tickets, issues, orders — anything with an Open status variant
```

### Traits

Traits define shared behavior that types can implement. Syntax follows Rust: `trait` for definition, `impl` for implementation.

```knot
trait Display a where
  display : a -> Text

impl Display Int where
  display n = showInt n

impl Display Text where
  display t = t
```

#### Trait Bounds

Trait bounds constrain type variables. They are inferred but can be written explicitly:

```knot
-- Bounds are inferred from usage
printAll = \rel -> do
  r <- rel
  yield (display r)

-- Inferred: Display a => [a] -> [Text]

-- Explicit bound (optional)
printAll : Display a => [a] -> [Text]
```

#### Multiple Bounds

```knot
sortAndShow : Ord a => Display a => [a] -> [Text]
```

#### Associated Types

Traits can have associated types:

```knot
trait Collection c where
  type Item c
  empty : c
  add : Item c -> c -> c
  toRel : c -> [Item c]

impl Collection [a] where
  type Item [a] = a
  empty = []
  add x rel = union rel (yield x)
  toRel = id
```

#### Deriving

Common traits are auto-derived:

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}
  deriving (Eq, Ord, Display)

data Shape
  = Circle {radius: Float}
  | Rect {width: Float, height: Float}
  deriving (Eq, Display)
```

#### Relation-Specific Traits

The standard library defines traits that interact with the relational model:

```knot
-- Types that can be used in where clauses and joins
trait Eq a where
  (==) : a -> a -> Bool

-- Types that support ordering
trait Ord a where
  compare : a -> a -> Ordering
```

#### Trait-Polymorphic Queries

Traits compose naturally with relational queries:

```knot
-- Works on any relation whose rows are displayable
report = \rel -> do
  r <- rel
  yield {summary: display r}

-- Works on any relation with a numeric column named `amount`
totalAmount : Num n => [{amount: n | r}] -> n
totalAmount = \rel -> fold (\acc r -> acc + r.amount) 0 rel
```

#### Default Implementations

```knot
trait Summarize a where
  summary : a -> Text
  detailed : a -> Text
  detailed x = summary x  -- default: same as summary

impl Summarize Shape where
  summary s = case s of
    Circle {radius} -> "circle r=" ++ display radius
    Rect {width, height} -> display width ++ "x" ++ display height
  -- detailed uses the default
```

### Type Inference

Full Hindley-Milner style inference extended with row polymorphism and trait bounds. Type signatures are always optional — the compiler infers trait bounds from usage just like it infers everything else.

## Full Example

```knot
data Priority = Low {} | Medium {} | High {} | Critical {}

data Status
  = Open {}
  | InProgress {assignee: Text}
  | Resolved {resolution: Text}

*todos : [{title: Text, owner: Text, priority: Priority, status: Status}]

route Api where
  GET                                /todos/{user: Text}           -> [{title: Text, priority: Priority}]  = GetTodos
  POST {title: Text, owner: Text, priority: Priority}
                                     /todos                        -> {ok: Bool}                             = AddTodo
  PUT  {owner: Text, person: Text}   /todos/{title: Text}/assign   -> {ok: Bool}                             = AssignTodo
  PUT  {owner: Text, msg: Text}      /todos/{title: Text}/resolve  -> {ok: Bool}                             = ResolveTodo
  GET                                /workload                     -> [{owner: Text, count: Int}]          = GetWorkload

formatTitle = \title -> toUpper (take 1 title) ++ drop 1 title

pendingFor = \user -> do
  t <- *todos
  where t.owner == user
  Open {} <- t.status
  yield {t.title, t.priority}

add = \title owner priority ->
  set *todos = union *todos [{title: formatTitle title, owner, priority, status: Open {}}]

assign = \title owner person ->
  set *todos = do
    t <- *todos
    yield (if t.title == title && t.owner == owner
      then {t | status: InProgress {assignee: person}}
      else t)

resolve = \title owner msg ->
  set *todos = do
    t <- *todos
    yield (if t.title == title && t.owner == owner
      then {t | status: Resolved {resolution: msg}}
      else t)

&workload = do
  t <- *todos
  Open {} <- t.status
  groupBy {t.owner}
  yield {owner: t.owner, count: count t}

serve : Api -> Response
serve = \req -> case req of
  GetTodos {user} -> pendingFor user
  AddTodo {title, owner, priority} -> do
    atomic (add title owner priority)
    yield {ok: True {}}
  AssignTodo {title, owner, person} -> do
    atomic (assign title owner person)
    yield {ok: True {}}
  ResolveTodo {title, owner, msg} -> do
    atomic (resolve title owner msg)
    yield {ok: True {}}
  GetWorkload {} -> &workload

main = listen 8080 serve
```
