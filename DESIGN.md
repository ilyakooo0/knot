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
&seniors = do
  people <- *people
  yield (filter (\p -> p.age > 65) people)

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
  teams <- *teams
  let result = do
    t <- teams
    m <- t.members
    yield {team: t.name, member: m.name}
  yield result

-- Engineers on large teams
&engineers = do
  teams <- *teams
  let result = do
    t <- teams
    where (count t.members) > 10
    m <- t.members
    where m.role == "engineer"
    yield {team: t.name, name: m.name}
  yield result
```

#### Updating Nested Relations

Use `set` with a `map` over the outer relation that transforms the nested relation:

```knot
-- Add a member to a team
addMember = \teamName person -> do
  teams <- *teams
  set *teams = do
    t <- teams
    yield (if t.name == teamName
      then {t | members: union t.members [person]}
      else t)

-- Remove a member from all teams
removePerson = \personName -> do
  teams <- *teams
  set *teams = do
    t <- teams
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
  memberships <- *memberships
  let result = do
    t <- do m <- memberships; yield m.team
    yield {name: t, members: do
      m <- memberships
      where m.team == t
      yield {name: m.member, age: m.age}}
  yield result

-- Flatten: expand nested relation into flat rows
&flat = do
  teams <- *teams
  let result = do
    t <- teams
    m <- t.members
    yield {team: t.name, member: m.name, age: m.age}
  yield result
```

#### Deeply Nested Relations

Nesting is arbitrarily deep:

```knot
type Course = {name: Text, students: [{name: Text, grades: [{subject: Text, score: Int}]}]}

*departments : [{name: Text, courses: [Course]}]

-- Find all failing grades across all departments
&failing = do
  departments <- *departments
  let result = do
    d <- departments
    c <- d.courses
    s <- c.students
    g <- s.grades
    where g.score < 50
    yield {dept: d.name, course: c.name, student: s.name, subject: g.subject, score: g.score}
  yield result
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

IO do blocks (those containing IO-returning expressions like `*rel`, `println`, `readFile`, `now`) are not desugared — they use a dedicated compilation path that sequences IO actions directly.

```knot
-- do with [] (pure relation comprehension over plain values)
richOnes = \employees departments -> do
  e <- employees
  d <- departments
  where e.dept == d.name
  yield {e.name, e.salary, d.budget}

-- IO do block (binds from *rel, which returns IO)
&richEmployees = do
  employees <- *employees
  departments <- *departments
  yield (richOnes employees departments)

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
| `set` | `*[a] -> [a] -> IO {} {}` | Set a persistent relation to a new value |

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

Relation comprehensions use `do` syntax with `yield` to produce rows. Since relation references (`*rel`, `&rel`) return `IO {} value`, you IO-bind to get the value, then use a pure comprehension on the plain value:

```knot
&richEmployees = do
  employees <- *employees
  departments <- *departments
  let result = do
    e <- employees
    d <- departments
    where e.dept == d.name
    where d.budget > 1_000_000
    yield {e.name, e.salary, d.budget}
  yield result
```

The outer do-block is an IO do-block that binds from `*employees` (type `IO {} [Employee]`) and `*departments` (type `IO {} [Department]`). The inner `let result = do ...` is a pure comprehension over plain relation values. `<-` draws from a relation (like a `FROM` clause). `where` filters (like a `WHERE` clause). `yield` emits a row into the result relation.

### Pipe-Forward Composition

Derived combinators like `filter` compose with `|>`:

```knot
&highEarners = do
  employees <- *employees
  yield (employees
    |> filter (\e -> e.salary > 150000)
    |> map (\e -> {name: e.name, salary: e.salary}))
```

### Querying by Variant: `match`

`match` filters to one variant and exposes its fields:

```knot
&circles = do                              -- : IO {} [{radius: Float}]
  shapes <- *shapes
  yield (shapes |> match Circle)

&rects = do                                -- : IO {} [{width: Float, height: Float}]
  shapes <- *shapes
  yield (shapes |> match Rect)

&bigCircles = do
  circles <- &circles
  yield (filter (\c -> c.radius > 10) circles)
```

### Pattern Matching in Comprehensions

Pattern matching on `<-` filters and binds in one step:

```knot
&bigCircleAreas = do
  shapes <- *shapes
  let result = do
    Circle c <- shapes
    where c.radius > 10
    yield {area: pi * c.radius * c.radius}
  yield result

&blockedDetails = do
  tickets <- *tickets
  let result = do
    t <- tickets
    Blocked {dependencies} <- t.status
    dep <- dependencies
    yield {t.title, dep}
  yield result
```

### Cross-Variant Operations

Operate on the whole relation with `case`:

```knot
scale = \factor -> do
  shapes <- *shapes
  set *shapes = do
    s <- shapes
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
  todos <- *todos
  let result = do
    t <- todos
    where t.done == 0
    groupBy {t.owner}
    yield {owner: t.owner, count: count t}
  yield result
```

The key expression is a record literal whose fields select the grouping columns. After `groupBy {t.owner}`, `t` is rebound from a single row to a sub-relation of all rows sharing that `owner` value. Field access on a group (e.g. `t.owner`) returns the shared key value. Aggregate functions like `count` operate on the whole group.

Multiple key fields group by their combination:

```knot
&summary = do
  orders <- *orders
  let result = do
    o <- orders
    groupBy {o.region, o.status}
    yield {region: o.region, status: o.status, total: count o}
  yield result
```

Grouping is executed via SQLite — key columns are inserted into a temp table and sorted with `ORDER BY`, then consecutive rows with matching keys are collected into groups.

## Effects and the IO Monad

### Unified Effect Model

All state operations in Knot return IO values. The IO type carries an effect set that distinguishes DB operations from external effects:

- **DB operations** return `IO {} value` — the empty effect set `{}` indicates pure database interaction with no external side effects. Source refs (`*rel`), derived refs (`&rel`), `set`, `replace`, and temporal queries (`@(timestamp)`) all return `IO {} value`.
- **External effects** carry specific tags: `IO {console} {}`, `IO {fs} Text`, `IO {network} Result`, `IO {clock} Int`, `IO {random} Float`.

This unified model means all stateful code lives in IO do-blocks, while pure comprehensions over plain values remain non-IO.

### The IO Type

Effectful functions return descriptions of effects (`IO {effects} a`) rather than performing them. IO values are thunks that execute when run.

```knot
-- DB operations return IO with empty effects
*people : [Person]
-- *people : IO {} [Person]

-- println returns an IO action with console effect
println : a -> IO {console} {}

-- readFile returns an IO action with fs effect
readFile : Text -> IO {fs} Text

-- now returns an IO action with clock effect
now : IO {clock} Int
```

### IO Do-Blocks

IO do-blocks sequence effects. The `<-` operator runs an IO action and binds its result. Since relation references return IO, you bind to get the plain value, then use pure comprehensions:

```knot
main = do
  people <- *people                  -- IO {} [Person] → binds [Person]
  content <- readFile "input.txt"    -- IO {fs} Text → binds Text
  println content                     -- IO {console} {}
  t <- now                            -- IO {clock} Int → binds Int
  println ("time: " ++ show t)
  yield {}
-- overall type: IO {fs, console, clock} {}
```

The pattern for querying relations is: IO-bind to get the value, then pure comprehension on the plain value:

```knot
&richEmployees = do
  employees <- *employees       -- IO bind: [Employee] from IO {} [Employee]
  let result = do               -- pure comprehension on the value
    e <- employees
    where e.salary > 100000
    yield e
  yield result
```

The compiler detects whether a do-block is IO or relational based on the types of bound expressions. IO do-blocks work correctly in all positions, including as branches of `if`/`then`/`else`.

### DB Effect Inference

DB effects are still inferred as fine-grained capabilities (`{r *rel}`, `{w *rel}`), but all relation access returns IO values:

```knot
-- Pure (inferred: no effects)
formatName = \n -> toUpper (take 1 n) ++ drop 1 n

-- DB read (inferred: {r *people})
&seniors = do
  people <- *people
  yield (filter (\p -> p.age > 65) people)

-- DB write (inferred: {rw *people})
birthday = \name -> do
  people <- *people
  set *people = do
    p <- people
    yield (if p.name == name then {p | age: p.age + 1} else p)
```

### Effect Annotations

Effect signatures are inferred but can be written explicitly:

```knot
birthday : {rw *people} Text -> IO {} {}
birthday = \name -> do
  people <- *people
  set *people = do
    p <- people
    yield (if p.name == name then {p | age: p.age + 1} else p)
```

If the body uses a capability not listed in the signature, the compiler rejects it.

### IO and Transactions

`atomic` takes an IO body and runs it in a transaction. The body must only contain DB interactions (empty effect set) — no external IO (console, fs, etc.) is allowed inside `atomic`.

```knot
atomic : IO {} a -> IO {} a
```

```knot
-- DB writes go in `atomic`, IO happens after commit
handleOrder = \req -> do
  orderId <- atomic do
    orders <- *orders
    set *orders = union orders [{item: req.body.item, qty: 1}]
    newOrders <- *orders
    yield (count newOrders)
  println ("New order #" ++ show orderId)
  yield {orderId}
```

#### `retry`

`retry` is used inside `atomic` blocks to implement STM (Software Transactional Memory) style concurrency. When executed, `retry` causes the transaction to rollback and wait until some relation changes, then re-executes the entire `atomic` block.

```knot
retry : forall a. a  -- bottom type, never returns
```

The compiler enforces that `retry` is only used inside `atomic`. This enables blocking waits on relation state without busy-polling:

```knot
-- Wait until a condition is met
waitForReady = atomic do
  status <- *status
  where (count (filter (\s -> s.ready) status)) == 0
  retry
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

### Concurrency

#### `fork`

`fork` runs an IO action on a new OS thread. It is fire-and-forget — the forked action runs independently. Each spawned thread gets its own SQLite connection (WAL mode enables concurrent access). The main thread waits for all spawned threads before exiting.

```knot
fork : IO {} {} -> IO {} {}
```

Do blocks can be passed as arguments without parentheses: `fork do ...`.

```knot
*counter : [{n: Int}]

increment = do
  c <- *counter
  set *counter = [{n: (fold (\_ x -> x.n) 0 c) + 1}]

main = do
  set *counter = [{n: 0}]
  fork do
    increment
    increment
  fork do
    increment
    increment
  -- main waits for all threads before exiting
```

#### `fork` + `atomic` + `retry`

The combination of `fork`, `atomic`, and `retry` enables STM-style concurrent coordination:

```knot
*tasks : [{id: Int, status: Text}]

waitForCompletion = \id -> atomic do
  tasks <- *tasks
  let task = do
    t <- tasks
    where t.id == id
    where t.status == "done"
    yield t
  where (count task) == 0
  retry
  yield task

main = do
  set *tasks = [{id: 1, status: "pending"}]
  fork do
    -- simulate work
    atomic do
      set *tasks = [{id: 1, status: "done"}]
  result <- waitForCompletion 1
  println result
```

SQLite WAL mode ensures that concurrent readers and writers do not block each other. Each thread operates on its own connection, and `atomic` provides transaction isolation within a thread.

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
  GET                                          /todos/{user: Text}?{page: Int, limit: Int}  = GetTodos
  POST {title: Text, owner: Text, priority: Priority}  /todos                               = AddTodo
  PUT  {owner: Text, person: Text}             /todos/{title: Text}/assign                   = AssignTodo
  GET                                          /workload                                     = GetWorkload
```

Handlers are bound per-endpoint with `serve API where` — the compiler ensures every endpoint has exactly one handler:

```knot
api = serve Api where
  GetTodos = \{user, page, limit} -> pendingFor user page limit
  AddTodo = \{title, owner, priority} -> do
    atomic (add title owner priority)
    yield {ok: True {}}
  AssignTodo = \{title, owner, person} -> do
    atomic (assign title owner person)
    yield {ok: True {}}
  GetWorkload = \{} -> &workload

main = listen 8080 api
```

`serve API where` produces a value of type `Server API`. Each handler receives the request record (path/query/body/header fields) and returns the response type declared on the endpoint. No string routes, no untyped params, no missing handlers.

#### Typed Responses

Return types can be declared per-endpoint:

```knot
route Api where
  GET                              /todos/{user: Text} -> [{title: Text, priority: Priority}]  = GetTodos
  POST {title: Text, owner: Text}  /todos              -> {ok: Bool}                              = AddTodo
  GET                              /workload           -> [{owner: Text, count: Int}]           = GetWorkload
```

The compiler checks that each handler returns the declared type.

#### Typed Headers

Request and response headers are declared with the `headers` keyword:

```knot
route Api where
  GET /todos headers {authorization: Text} -> [Todo] headers {xTotalCount: Int, xPage: Int} = GetTodos
  POST {title: Text} /todos headers {authorization: Text, xIdempotencyKey: Text} -> {id: Int} = CreateTodo
  GET /health -> {status: Text} = HealthCheck
```

Request headers become constructor fields, just like body/query/path params. The handler destructures them:

```knot
api = serve Api where
  GetTodos = \{authorization} -> do
    let todos = allTodos
    yield {body: todos, headers: {xTotalCount: length todos, xPage: 1}}
  CreateTodo = \{title, authorization, xIdempotencyKey} -> do
    let id = addTodo title
    yield {body: {id: id}, headers: {}}
  HealthCheck = \{} -> yield {status: "ok"}
```

When response headers are declared, the handler returns a `{body: ..., headers: ...}` record. Without response headers, it returns the body directly.

Optional headers use `Maybe`:

```knot
route Api where
  GET /todos headers {authorization: Maybe Text} -> [Todo] = GetTodos
```

The server gets `Nothing {}` if the header is absent, `Just {value: "..."}` if present. In `fetch`, `Nothing` headers are skipped.

On the fetch side, request headers are sent automatically from constructor fields. When response headers are declared, the result wraps as `{body: ResponseType, headers: {h: T}}`:

```knot
result <- fetch "https://api.example.com" (GetTodos {authorization: "Bearer tok"})
-- result : IO {network} (Result ... {body: [Todo], headers: {xTotalCount: Int, xPage: Int}})
```

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
api = serve Api where
  CreateOrder = \{item, qty} -> do
    orderId <- atomic do
      orders <- *orders
      set *orders = union orders [{item: item, qty: qty}]
      newOrders <- *orders
      yield (count newOrders)
    println ("New order #" ++ show orderId)
    yield {orderId: orderId}
```

For sub-transaction boundaries:

```knot
batchTransfer = \transfers ->
  map (\t -> atomic (transfer t.from t.to t.amount)) transfers
```

## Persistence

### Mutation

All mutation is done through `set`, which replaces a persistent relation with a new value. The runtime diffs the old and new sets to apply minimal changes. Since relation references return IO, you bind to get the current value first:

```knot
-- Insert: union with a singleton
addPerson = do
  people <- *people
  set *people = union people [{name: "Alice", age: 30}]

-- Update: map with a conditional
birthday = \name -> do
  people <- *people
  set *people = do
    p <- people
    yield (if p.name == "Alice" then {p | age: p.age + 1} else p)

-- Delete: filter to keep the rest
removePerson = \name -> do
  people <- *people
  set *people = do
    p <- people
    where p.name != name
    yield p
```

### Identity is Structural

Relations are sets. Two rows are the same row iff all their fields are equal. Setting a relation to a value that includes a duplicate is a no-op for that row.

```knot
-- Adding an already-existing row changes nothing
set *people = union *people [{name: "Alice", age: 30}]
set *people = union *people [{name: "Alice", age: 30}]  -- no change
```

No surrogate IDs, no key declarations. Data identifies itself.

### Indexing

Automatic. The runtime observes query patterns and indexes accordingly. No `CREATE INDEX`, no key declarations.

## Views

A `*`-prefixed relation with a body is a **view** — a bidirectional query over source relations. Reads compute the query; writes propagate back to the underlying sources.

```knot
&seniorStaff = do                                            -- read-only (& prefix)
  employees <- *employees
  yield (filter (\e -> e.salary > 100000) employees)

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
addOpenTodo = do
  openTodos <- *openTodos
  set *openTodos = union openTodos [{title: "New task", owner: "Alice", priority: High {}}]
-- Compiler rewrites →
-- set *todos = union *todos [{title: "New task", owner: "Alice", priority: High {}, status: Open {}}]

-- Delete through view — only affects rows matching the constant
removeAliceTodos = do
  openTodos <- *openTodos
  set *openTodos = do
    t <- openTodos
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

&reportsTo : [{ancestor: Text, descendant: Text}] = do
  manages <- *manages
  reportsTo <- &reportsTo
  yield (union
    (do m <- manages
        yield {m.manager, m.report})
    (do r <- reportsTo
        m <- manages
        where r.descendant == m.manager
        yield {r.ancestor, m.report}))
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
  employees <- *employees @(t - 365 days)
  yield (employees
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

### Units of Measure

Optional compile-time units on `Int` and `Float`. Units are fully erased at runtime — no performance cost, no runtime representation. Plain `Float` is dimensionless (`Float<1>`).

#### Declaration

```knot
unit M
unit S
unit Kg
unit Usd

-- Derived unit aliases (expand at use site)
unit N = Kg * M / S^2
unit Hz = 1 / S
```

#### Type Syntax

Angle brackets on numeric types only. Concrete units are uppercase; lowercase names are unit variables (see [Unit Polymorphism](#unit-polymorphism)).

```knot
height : Float<M>
mass : Float<Kg>
speed : Float<M / S>
force : Float<N>
acceleration : Float<M / S^2>
cents : Int<Usd>
```

#### Literal Syntax

```knot
distance = 42.0<M>
duration = 3.5<S>
price = 999<Usd>
pi = 3.14159              -- dimensionless (Float<1>)
```

#### Arithmetic

`+`/`-` require matching units. `*`/`/` compose units. The compiler normalizes unit expressions algebraically (`M * S / S` → `M`, `M / M` → `1`).

```knot
-- Same-unit addition/subtraction
10.0<M> + 5.0<M>                -- Float<M>
10.0<M> + 5.0<S>                -- type error

-- Unit composition
10.0<M> * 5.0<M>                -- Float<M^2>
100.0<M> / 10.0<S>              -- Float<M/S>
2.0<Kg> * 9.8<M / S^2>          -- Float<Kg * M / S^2> = Float<N>

-- Dimensionless scalars
2.0 * 5.0<M>                    -- Float<M>
5.0<M> / 2.0                    -- Float<M>

-- Negation preserves units
-(5.0<M>)                        -- Float<M>
```

Arbitrary integer powers arise naturally from multiplication: `M * M` = `M^2`, `S * S * S` = `S^3`. Powers can also be written directly in type annotations: `Float<M^2>`, `Float<S^-1>`.

#### Unit Polymorphism

Concrete units are uppercase; lowercase names inside `<...>` are unit variables — no extra syntax needed:

```knot
double : Float<u> -> Float<u>
double = \x -> x + x

computeSpeed : Float<d> -> Float<t> -> Float<d / t>
computeSpeed = \distance time -> distance / time
```

Unit variables are inferred like type variables:

```knot
double = \x -> x + x
-- inferred: Float<u> -> Float<u>  (or Int<u> -> Int<u> via Num)
```

#### Conversion

`stripUnit` / `withUnit` (Int) and `stripFloatUnit` / `withFloatUnit` (Float) are identity functions that exist only for the type checker. Use them to drop a unit tag and re-attach a different one. The result of `withUnit`/`withFloatUnit` carries a free unit variable, so the caller pins the target unit via the surrounding type context (e.g. the function's return signature) or an explicit annotation:

```knot
stripUnit       : Int<u> -> Int           -- drop unit from Int
withUnit        : Int -> Int<u>           -- attach unit to Int
stripFloatUnit  : Float<u> -> Float
withFloatUnit   : Float -> Float<u>

toS : Int<Ms> -> Int<S>
toS = \ms -> withUnit (stripUnit ms / 1000)

toMiles : Float<Km> -> Float<Mi>
toMiles = \d -> withFloatUnit (stripFloatUnit d * 0.621371)
```

Plain `Int`/`Float` are unit-agnostic and unify with any `Int<u>`/`Float<u>`, so passing a unit-tagged value where plain numeric is expected (or vice versa) needs no conversion. These helpers are only needed when you must rebrand a value with a *different* concrete unit.

#### Unit-Preserving Stdlib

Functions like `abs`, `min`, `max`, `sum`, `avg` preserve units:

```knot
abs : Float<u> -> Float<u>
min : Float<u> -> Float<u> -> Float<u>
sum : (a -> Float<u>) -> [a] -> Float<u>
avg : (a -> Float<u>) -> [a] -> Float<u>
```

#### `show` and Units

`show` on a value with a concrete unit appends the unit string. The compiler knows the unit statically and emits the string as a constant:

```knot
show 9.8<M / S^2>       -- "9.8 M/S^2"
show 42.0<M>             -- "42.0 M"
show 3.14                -- "3.14"
```

When the unit is polymorphic (inside a unit-generic function), `show` prints just the number.

The compiler uses a canonical form for unit strings: alphabetical numerator, alphabetical denominator, powers collapsed. This same canonical form determines type equality (`m * s` = `s * m`).

#### Records, Relations, and SQLite

Units are phantom — SQLite stores raw numbers. Schema descriptors ignore units.

```knot
type Measurement = {distance: Float<M>, time: Float<S>}

*measurements : [Measurement]

-- Units flow through queries
&speeds = do
  measurements <- *measurements
  let result = do
    m <- measurements
    yield {speed: m.distance / m.time}   -- Float<M/S>
  yield result
```

#### Interaction with Traits

Units live outside the trait system as a compile-time overlay. The `Num` trait handles runtime dispatch for arithmetic; the compiler applies unit algebra rules as an additional layer. No changes to trait definitions are needed — `+` on `Float<M>` dispatches through `Num.add` at runtime while the compiler separately verifies that both operands share the unit `M` and propagates `M` to the result.

### Refined Types

A refined type is a base type restricted by a predicate. The predicate is an ordinary Knot function (`T -> Bool`) — any pure function works, no restrictions.

#### Declaration

```knot
-- Standalone refined type
type Nat = Int where \x -> x >= 0
type Percentage = Float where \x -> x >= 0.0 && x <= 100.0
type NonEmptyText = Text where \s -> length s > 0
type Email = Text where \s -> contains "@" s && length s >= 3

-- Stacking: inner refinement inherited, predicates conjoin
type Age = Nat where \x -> x <= 150
-- equivalent to: Int where \x -> x >= 0 && x <= 150
```

#### Per-Field Refinements

Refinements attach to individual record fields:

```knot
type Person = {
  name: Text where \s -> length s > 0,
  age: Int where \x -> x >= 0 && x <= 150,
  email: Text
}
```

#### Cross-Field Refinements

A `where` after the closing `}` constrains the whole record. Multiple `where` clauses are conjunctive:

```knot
type DateRange = {
  start: Int,
  end: Int
} where \r -> r.start <= r.end

type Discount = {
  percent: Float where \x -> x >= 0.0 && x <= 1.0,
  minQty: Int where \x -> x >= 0
} where \d -> d.percent < 0.5 || d.minQty >= 10
```

#### ADT Constructor Refinements

Refinements can appear on constructor fields:

```knot
data Shape
  = Circle {radius: Float where \r -> r > 0.0}
  | Rect {width: Float where \w -> w > 0.0, height: Float where \h -> h > 0.0}
```

#### Relation Constraints

Source declarations support both value predicates and relational predicates:

```knot
*people : [{
  name: Text where \s -> length s > 0,
  age: Int where \x -> x >= 0
}]
  where \p -> p.age >= 13 || p.email == ""     -- cross-field value predicate

*orders : [{customer: Text, amount: Int where \x -> x > 0}]
  where .customer in *people.email              -- relational membership
  where unique .email                           -- uniqueness
  where \o -> o.amount <= 1000000               -- value predicate
```

Two relational constraint forms:

- `.field in *rel.field` — every value of `.field` must appear in the referenced relation's field (referential integrity)
- `unique .field` — field values must be unique across all rows

These replace the old subset constraint syntax:

```knot
-- Old (removed):
*orders.customer <= *people.name
*users <= *users.email

-- New:
*orders : [...] where .customer in *people.name
*users : [...] where unique .email
```

All constraints — field, cross-field, relational — are enforced by the Knot runtime at `set` boundaries. Nothing is pushed to SQLite. The runtime mediates all writes, so there is no escape hatch.

#### Subtyping

`Refined(T, p) <: T`. A refined type is a subtype of its base.

```
Nat <: Int
Age <: Nat <: Int
```

Upcasting (refined → base) is implicit, no check:

```knot
f : Int -> Int
f (x : Nat)         -- fine: Nat <: Int
```

Downcasting (base → refined) requires `refine`. `refine expr` has type `Result RefinementError T` where `T` is the target refined type, inferred from context. If context doesn't determine `T`, it's a type error.

```knot
f : Nat -> Text

-- In a Result do-block (bind unwraps the Result):
do
  n <- refine someInt        -- n : Nat (inferred from f's parameter type)
  yield (f n)
-- : Result RefinementError Text

-- With case:
case refine someInt of
  Ok {value: n} -> f n       -- Nat inferred from f's parameter
  Err {error}   -> "invalid"
```

Two refined types with the same base but different predicates are unrelated — no subtyping between `Nat` and `Percentage`. Stacked refinements are the exception: `Age <: Nat` because `Age` was defined as `Nat where ...`.

Arithmetic on refined types returns the base type:

```knot
x : Nat = ...
y : Nat = ...
x + y    -- Int, not Nat (no attempt to prove result satisfies predicate)
```

#### The `refine` Expression

`refine expr` checks the refinement predicate at runtime. It returns `Result RefinementError T` where `T` is the target refined type, inferred from context:

```knot
-- Target type Nat inferred from binding annotation
let r : Result RefinementError Nat = refine 42
-- r = Ok {value: 42}

let r : Result RefinementError Nat = refine (-1)
-- r = Err {error: {typeName: "Nat", violations: [{field: Nothing {}, message: "expected x >= 0, got -1"}]}}
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
validateOrder : {customer: Text, amount: Int} -> Result RefinementError {customer: NonEmptyText, amount: Nat}
validateOrder = \raw -> do
  customer <- refine raw.customer    -- NonEmptyText inferred from return type
  amount   <- refine raw.amount      -- Nat inferred from return type
  yield {customer, amount}
```

#### Boundary Checking

Checks happen at two boundaries:

| Boundary | Mechanism | On failure |
|----------|-----------|------------|
| `refine expr` | Explicit coercion | Returns `Result RefinementError T` |
| `set *rel = value` | Implicit per-row check | Panics with `RefinementError` |

`set` panics because constraint violations at the persistence boundary are programming errors — input should be validated with `refine` first. For explicit error handling at the `set` boundary, use `trySet`:

```knot
trySet : *[a] -> [a] -> IO {} (Result RefinementError {})
```

#### Predicates

Predicates in type-level refinements must be **pure** — no IO, no relation references. They are ordinary Knot functions with no restrictions on what pure operations they use (recursion, pattern matching, higher-order functions, etc.).

Predicates in relation `where` clauses follow the same rule — they are pure functions over individual rows. Relational constraints (`.field in`, `unique`) are separate syntactic forms, not predicates.

#### Interaction with Units

Units and refinements are orthogonal — units are compile-time phantom, refinements are runtime-checked:

```knot
type PositiveDistance = Float<M> where \x -> x > 0.0
type Speed = Float<M/S> where \x -> x >= 0.0
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
type Nat = Int where \x -> x >= 0
type NonEmptyText = Text where \s -> length s > 0
type Email = Text where \s -> contains "@" s && length s >= 3

type Person = {
  name: NonEmptyText,
  age: Nat where \x -> x <= 150,
  email: Email
} where \p -> p.age >= 13

*people : [Person]

*orders : [{
  customer: Email,
  amount: Nat where \x -> x <= 1000000,
  items: [{name: NonEmptyText, qty: Nat where \q -> q > 0}]
}]
  where .customer in *people.email

route Api where
  POST {name: Text, age: Int, email: Text}  /users -> {ok: Bool, error: Maybe Text}  = CreateUser

api = serve Api where
  CreateUser = \{name, age, email} ->
    case refine {name, age, email} of    -- Person inferred from set *people
      Ok {value: person} -> do
        atomic do
          people <- *people
          set *people = union people [person]
        yield {ok: true, error: Nothing {}}
      Err {error} -> do
        let msg = fold (\acc v -> acc ++ v.message ++ "; ") "" error.violations
        yield {ok: false, error: Just {value: msg}}
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
  todos <- *todos
  let result = do
    t <- todos
    where t.owner == user
    Open {} <- t.status
    yield {t.title, t.priority}
  yield result

add = \title owner priority -> do
  todos <- *todos
  set *todos = union todos [{title: formatTitle title, owner: owner, priority: priority, status: Open {}}]

assign = \title owner person -> do
  todos <- *todos
  set *todos = do
    t <- todos
    yield (if t.title == title && t.owner == owner
      then {t | status: InProgress {assignee: person}}
      else t)

resolve = \title owner msg -> do
  todos <- *todos
  set *todos = do
    t <- todos
    yield (if t.title == title && t.owner == owner
      then {t | status: Resolved {resolution: msg}}
      else t)

&workload = do
  todos <- *todos
  let result = do
    t <- todos
    Open {} <- t.status
    groupBy {t.owner}
    yield {owner: t.owner, count: count t}
  yield result

api = serve Api where
  GetTodos = \{user} -> pendingFor user
  AddTodo = \{title, owner, priority} -> do
    atomic (add title owner priority)
    yield {ok: True {}}
  AssignTodo = \{title, owner, person} -> do
    atomic (assign title owner person)
    yield {ok: True {}}
  ResolveTodo = \{title, owner, msg} -> do
    atomic (resolve title owner msg)
    yield {ok: True {}}
  GetWorkload = \{} -> &workload

main = listen 8080 api
```
