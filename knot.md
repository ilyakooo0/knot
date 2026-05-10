# Knot Language Reference

Knot is a functional relational programming language. Relations (typed sets) are the primary data structure, computation is pure and functional, and state is automatically persisted to SQLite.

## Quick Start

```knot
type Person = {name: Text, age: Int}

*people : [Person]

main = do
  set *people = [{name: "Alice", age: 30}, {name: "Bob", age: 25}]
  people <- *people
  let result = do
    p <- people
    where p.age > 27
    yield p.name
  println (show result)
  yield {}
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
| `Int` | Unbounded integer | `42`, `-7`, `1_000_000` |
| `Float` | 64-bit float | `3.14`, `-0.5` |
| `Text` | Unicode string | `"hello"`, `"line\n"` |
| `Bool` | Boolean | `True {}`, `False {}` |
| `Bytes` | Byte string | `b"hello"` |
| `{}` | Unit / empty record | `{}` |

### Units of Measure

Optional compile-time units on `Int` and `Float`. Units are fully erased at runtime — no performance cost.

```knot
unit M
unit S
unit Kg
unit N = Kg * M / S^2    -- derived unit alias

distance = 42.0<M>        -- Float<M>
speed : Float<M / S>
force : Float<N>
cents : Int<Usd>
```

Arithmetic rules: `+`/`-` require matching units, `*`/`/` compose units, negation preserves units.

```knot
10.0<M> + 5.0<M>              -- Float<M>
10.0<M> * 5.0<M>              -- Float<M^2>
100.0<M> / 10.0<S>            -- Float<M/S>
2.0 * 5.0<M>                  -- Float<M> (scalar mul)
-(5.0<M>)                     -- Float<M>
```

Unit polymorphism — concrete units are uppercase, lowercase names in `<...>` are unit variables:

```knot
double : Float<u> -> Float<u>
double = \x -> x + x
```

Unit-preserving stdlib: `sum`, `avg` propagate units from their projection function.

### Records

```knot
-- Anonymous record
{name: "Alice", age: 30}

-- Type alias
type Person = {name: Text, age: Int}
```

Field access: `person.name`

Record update: `{person | age: person.age + 1}`

Shorthand field pun: `yield {name: t.name, age: t.age}` can be written `yield {t.name, t.age}` when field name matches.

### Relations

A relation `[T]` is a typed **set** of `T` values. No duplicates. No ordering guarantees.

```knot
names = ["Alice", "Bob", "Carol"]     -- [Text]
empty = []                             -- [a]
people = [{name: "Alice", age: 30}]   -- [{name: Text, age: Int}]
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
type Person = {name: Text, age: Int}
type TodoList = [{title: Text, done: Bool}]
```

---

## Declarations

There are five kinds of top-level declarations:

| Declaration | Kind | Description |
|---|---|---|
| `*foo : [T]` | Source relation | Persisted in SQLite, mutable via `set` |
| `*foo = expr` | View | Bidirectional query over sources |
| `&foo = expr` | Derived relation | Read-only, recomputed on access |
| `foo = expr` | Constant/function | Pure value, no DB effects |
| `type Foo = T` | Type alias | Name for a type |

```knot
-- Source: stored in DB
*people : [{name: Text, age: Int}]

-- View: settable query over a source
*openTodos = do
  todos <- *todos
  let result = do
    t <- todos
    yield {title: t.title, owner: t.owner, status: Open {}}
  yield result

-- Derived: read-only computed relation
&seniors = do
  people <- *people
  yield (filter (\p -> p.age > 65) people)

-- Constant
maxRetries = 3

-- Function (constants bound to lambdas)
double = \x -> x * 2
```

---

## Functions and Lambdas

Functions are constants bound to lambdas:

```knot
-- Single parameter
greet = \name -> "Hello, " ++ name

-- Multiple parameters
add = \x y -> x + y

-- With type signature (optional — types are inferred)
formatName : Text -> Text
formatName = \n -> toUpper (take 1 n) ++ drop 1 n
```

Function application is juxtaposition:

```knot
greet "Alice"           -- "Hello, Alice"
add 2 3                 -- 5
filter (\p -> p.age > 30) people
```

Pipe-forward operator:

```knot
people
  |> filter (\p -> p.age > 30)
  |> map (\p -> p.name)
```

---

## Do Blocks

`do` blocks are the primary syntax for comprehensions and sequencing. They work with any type implementing the `Monad` trait — not just relations.

### Relation Comprehensions

```knot
&results = do
  employees <- *employees       -- IO bind: get [Employee]
  departments <- *departments   -- IO bind: get [Department]
  let joined = do               -- pure comprehension
    e <- employees
    d <- departments
    where e.dept == d.name
    let bonus = e.salary * 0.1
    yield {e.name, bonus, d.budget}
  yield joined
```

Statements in a `do` block:

| Statement | Meaning |
|-----------|---------|
| `x <- expr` | Bind: iterate over relation / unwrap monad |
| `where cond` | Filter: skip when condition is false |
| `let x = expr` | Local binding |
| `yield expr` | Emit a value into the result |
| `groupBy {fields}` | Group by key fields (see Grouping) |
| `expr` | Bare expression (for IO side effects) |

### IO Do Blocks

When bound expressions are `IO` values, the do block sequences IO actions:

```knot
-- IO do block with console effects
main = do
  content <- readFile "input.txt"    -- IO {fs} Text
  println content                     -- IO {console} {}
  yield {}

-- IO do block with DB operations
addPerson = \name age -> do
  people <- *people                  -- IO {} [Person]
  set *people = union people [{name: name, age: age}]
```

The compiler detects whether a do block is relational or IO from the types. Relation operations (`*rel`, `&rel`, `set`) all return `IO {} value` — the empty effect set `{}` distinguishes DB operations from external effects like `{console}` or `{fs}`.

### Pattern Matching in Bind

Filter and destructure in one step:

```knot
&circles = do
  shapes <- *shapes
  let result = do
    Circle c <- shapes
    yield c
  yield result

&inProgress = do
  tickets <- *tickets
  let result = do
    t <- tickets
    InProgress ip <- t.status
    yield {t.title, ip.assignee}
  yield result
```

---

## Mutation

All mutation uses `set`, which replaces a source relation with a new value. The runtime diffs old vs new to apply minimal changes.

```knot
-- Insert (union with singleton)
addPerson = do
  people <- *people
  set *people = union people [{name: "Alice", age: 30}]

-- Update (map with conditional)
birthday = \name -> do
  people <- *people
  set *people = do
    p <- people
    yield (if p.name == name then {p | age: p.age + 1} else p)

-- Delete (filter to keep)
removePerson = \name -> do
  people <- *people
  set *people = do
    p <- people
    where p.name != name
    yield p
```

Relations are sets — inserting a duplicate row is a no-op.

---

## Control Flow

### If/Else

```knot
result = if x > 0 then "positive" else "non-positive"
```

`if` is an expression — both branches must have the same type.

### Case Expressions

```knot
describe = \s -> case s of
  Circle {radius} -> "circle r=" ++ show radius
  Rect {width, height} -> show width ++ "x" ++ show height

-- With wildcard
priority = \p -> case p of
  Critical {} -> 1
  High {} -> 2
  _ -> 3
```

### Pattern Matching on Relations

```knot
describe = \rel -> case rel of
  [] -> "empty"
  [{name: n}] -> "just " ++ n
  _ -> show (count rel) ++ " rows"
```

---

## Grouping

`groupBy` partitions a relation by key fields. After `groupBy`, the bound variable becomes a sub-relation:

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

Multiple keys: `groupBy {o.region, o.status}`

After `groupBy {t.owner}`:
- `t.owner` returns the shared key value
- `count t` counts rows in the group
- `sum (\x -> x.points) t` aggregates over the group

---

## Nested Relations

Fields can hold `[T]` — sets nested inside rows:

```knot
*teams : [{name: Text, members: [{name: Text, age: Int}]}]

-- Query into nested relations
&allMembers = do
  teams <- *teams
  let result = do
    t <- teams
    m <- t.members
    yield {team: t.name, member: m.name}
  yield result

-- Update nested relations
updateTeams = do
  teams <- *teams
  set *teams = do
    t <- teams
    yield {t | members: do
      m <- t.members
      where m.name != "Eve"
      yield m}
```

---

## Views

A `*`-prefixed declaration with a body is a view — reads compute the query, writes propagate back:

```knot
*openTodos = do
  todos <- *todos
  let result = do
    t <- todos
    yield {title: t.title, owner: t.owner, priority: t.priority, status: Open {}}
  yield result
```

Constant columns (like `status: Open {}`) are:
- **On read**: used as a filter (only open todos)
- **On write**: auto-filled (inserting through the view adds `status: Open {}`)
- **Hidden from the type**: the view's type omits constant columns

```knot
-- Insert through view — status auto-filled
addOpenTodo = do
  openTodos <- *openTodos
  set *openTodos = union openTodos [{title: "New task", owner: "Alice", priority: High {}}]
```

---

## Derived Relations

Read-only computed relations, prefixed with `&`:

```knot
&seniors = do
  people <- *people
  yield (filter (\p -> p.age > 65) people)

&stats = do
  todos <- *todos
  let result = do
    t <- todos
    groupBy {t.owner}
    yield {owner: t.owner, total: count t}
  yield result
```

### Recursive Derived Relations

Datalog-style fixpoint iteration for transitive closure:

```knot
&reportsTo = do
  reportsTo <- &reportsTo     -- self-reference (IO bind)
  manages <- *manages
  base <- &base
  let step = do
    r <- reportsTo
    m <- manages
    where r.descendant == m.manager
    yield {ancestor: r.ancestor, descendant: m.report}
  yield (union base step)
```

---

## Traits

```knot
trait Describe a where
  describe : a -> Text
  describe x = show x       -- default implementation
  detailed : a -> Text
  detailed x = describe x   -- default using another method

impl Describe Shape where
  describe s = case s of
    Circle {radius} -> "a circle"
    Rect {width, height} -> "a rectangle"

-- Auto-derive from defaults
data Color = Red {} | Green {} | Blue {}
  deriving (Describe)
```

### Higher-Kinded Traits

```knot
trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b

impl Functor Maybe where
  map f m = case m of
    Nothing {} -> Nothing {}
    Just {value} -> Just {value: f value}
```

### Trait Bounds

```knot
printAll : Display a => [a] -> [Text]
printAll = \rel -> do
  r <- rel
  yield (display r)

-- Multiple bounds
sortAndShow : Ord a => Display a => [a] -> [Text]
```

### Built-in Trait Hierarchy

```
Eq          -- ==, !=
├── Ord     -- <, >, <=, >= (returns Ordering: LT {} | EQ {} | GT {})
└── Num     -- +, -, *, /, unary -

Semigroup   -- ++ (text concat, relation concat)
Display     -- display : a -> Text

Functor (f : Type -> Type)        -- map
└── Applicative (f : Type -> Type) -- yield, ap
    ├── Monad (m : Type -> Type)   -- bind (enables do/<-)
    └── Alternative (f : Type -> Type) -- empty, alt (enables where)

Foldable (t : Type -> Type)       -- fold
```

---

## IO and Effects

### IO Type

Effectful functions return `IO {effects} a` — descriptions of effects, not immediate execution:

```knot
-- External effects
println : a -> IO {console} {}
readFile : Text -> IO {fs} Text
now : IO {clock} Int

-- DB operations (empty effect set)
-- *rel : IO {} [T]
-- set *rel = val : IO {} {}
```

### Effect Kinds

| Effect | Functions |
|--------|-----------|
| `console` | `println`, `print`, `readLine` |
| `fs` | `readFile`, `writeFile`, `appendFile`, `fileExists`, `removeFile`, `listDir` |
| `clock` | `now` |
| `random` | `randomInt`, `randomFloat` |

### IO Do Blocks

```knot
main = do
  content <- readFile "data.txt"
  println ("Read " ++ show (length content) ++ " chars")
  yield {}
```

### DB Effects

All relation operations are IO-wrapped with an empty effect set:

```knot
-- All relation operations are IO:
birthday = \name -> do
  people <- *people              -- IO {} [Person]
  set *people = do               -- IO {} {}
    p <- people
    yield (if p.name == name then {p | age: p.age + 1} else p)
-- Inferred effects: {rw *people}
-- Type: Text -> IO {} {}
```

### Transactions

`atomic` takes an IO expression containing only DB operations and runs it in a transaction:

```knot
-- atomic : IO {} a -> IO {} a
handleOrder = \item -> do
  orderId <- atomic do
    orders <- *orders
    set *orders = union orders [{item: item, qty: 1}]
    newOrders <- *orders
    yield (count newOrders)
  println ("Order #" ++ show orderId)
  yield {orderId}
```

The body of `atomic` must be an IO expression containing only DB operations. External effects (console, fs, etc.) are not allowed inside `atomic`.

### Concurrency

#### `fork`

Fire-and-forget: runs an IO action on a new OS thread. Each thread gets its own SQLite connection (WAL mode).

```knot
fork : IO {} {} -> IO {} {}

main = do
  fork do
    println "hello from thread 1"
  fork do
    println "hello from thread 2"
  println "hello from main"
  -- main waits for all spawned threads before exiting
```

Do blocks can be passed directly as arguments: `fork do ...` (no parentheses needed).

#### `retry`

Used inside `atomic` blocks for STM-style waiting. Causes the transaction to rollback and wait until some relation changes, then re-executes the atomic block.

```knot
waitFor = \condition -> atomic do
  cond <- condition
  where (not cond)
  retry
```

The compiler enforces that `retry` is only used inside `atomic`.

---

## Operators

| Operator | Meaning | Trait |
|----------|---------|-------|
| `+` `-` `*` `/` | Arithmetic | `Num` |
| unary `-` | Negation | `Num` |
| `==` `!=` | Equality | `Eq` |
| `<` `>` `<=` `>=` | Comparison | `Ord` |
| `++` | Concatenation | `Semigroup` |
| `&&` `\|\|` | Boolean logic | (direct) |
| `\|>` | Pipe forward | `x \|> f` = `f x` |

---

## Standard Library Functions

### Relations

| Function | Type | Description |
|----------|------|-------------|
| `filter` | `(a -> Bool) -> [a] -> [a]` | Keep matching rows |
| `map` | `(a -> b) -> [a] -> [b]` | Transform each row |
| `match` | `Constructor -> [ADT] -> [Payload]` | Filter by variant |
| `fold` | `(b -> a -> b) -> b -> [a] -> b` | Left fold |
| `count` | `[a] -> Int` | Number of rows |
| `sum` | `(a -> b) -> [a] -> b` | Sum projected field (preserves units) |
| `avg` | `(a -> Float<u>) -> [a] -> Float<u>` | Average projected field (preserves units) |
| `single` | `[a] -> Maybe a` | Extract singleton |
| `union` | `[a] -> [a] -> [a]` | Set union |
| `diff` | `[a] -> [a] -> [a]` | Set difference |
| `inter` | `[a] -> [a] -> [a]` | Set intersection |

### Text

| Function | Type | Description |
|----------|------|-------------|
| `toUpper` | `Text -> Text` | Uppercase |
| `toLower` | `Text -> Text` | Lowercase |
| `length` | `Text -> Int` | Character count |
| `trim` | `Text -> Text` | Strip whitespace |
| `reverse` | `Text -> Text` | Reverse |
| `chars` | `Text -> [Text]` | Split to characters |
| `take` | `Int -> Text -> Text` | First n characters |
| `drop` | `Int -> Text -> Text` | Drop first n characters |
| `contains` | `Text -> Text -> Bool` | Substring check |

### Conversion

| Function | Type | Description |
|----------|------|-------------|
| `show` | `a -> Text` | Any value to text |
| `toJson` | `a -> Text` | Encode as JSON |
| `parseJson` | `Text -> a` | Decode JSON |

### IO

| Function | Type | Description |
|----------|------|-------------|
| `println` | `a -> IO {console} {}` | Print with newline |
| `print` | `a -> IO {console} {}` | Print without newline |
| `readLine` | `IO {console} Text` | Read stdin line |
| `readFile` | `Text -> IO {fs} Text` | Read file |
| `writeFile` | `Text -> Text -> IO {fs} {}` | Write file (path, content) |
| `appendFile` | `Text -> Text -> IO {fs} {}` | Append to file |
| `fileExists` | `Text -> IO {fs} Bool` | Check file exists |
| `removeFile` | `Text -> IO {fs} {}` | Delete file |
| `listDir` | `Text -> IO {fs} [Text]` | List directory |
| `now` | `IO {clock} Int` | Unix timestamp (ms) |
| `randomInt` | `Int -> IO {random} Int` | Random int [0, bound) |
| `randomFloat` | `IO {random} Float` | Random float [0, 1) |
| `atomic` | `IO {} a -> IO {} a` | Run DB operations in a transaction |
| `fork` | `IO {} {} -> IO {} {}` | Fire-and-forget on new OS thread |
| `retry` | `IO {} a` | Rollback and wait (inside `atomic` only) |

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
route TodoApi where
  /todos
    GET /{owner: Text} -> [Todo] = GetTodos
    POST {title: Text, owner: Text} / -> Todo = CreateTodo

route AdminApi where
  /admin
    GET /count -> Int = GetCount

-- Compose routes
route Api = TodoApi | AdminApi

-- Handler
api = serve Api where
  GetTodos = \{owner} -> do
    todos <- getTodos owner
    yield Ok {value: todos}
  CreateTodo = \{title, owner} -> do
    todo <- addTodo title owner
    yield Ok {value: todo}
  GetCount = \{} -> do
    todos <- *todos
    yield Ok {value: count todos}

main = listen 8080 api
```

`serve API where` produces a value of type `Server API`. Each handler takes the request record and returns `Result HttpError T`, where `T` is the response type declared on the endpoint and `HttpError = {status: Int, message: Text}`.

### HTTP Status Codes

`Ok {value: v}` responds 200 with `v` as JSON. `Err {error: {status, message}}` responds with the given status code and a JSON error body:

```knot
api = serve Api where
  GetUser = \{id} -> do
    users <- *people
    case filter (\u -> u.id == id) users of
      [] -> yield Err {error: {status: 404, message: "user not found"}}
      [u | _] -> yield Ok {value: u}
```

Status is clamped to `100..=599`. The runtime emits `400` for path/query/body parsing failures and refinement violations, and `404` for unmatched routes — only return `Err` for application-level errors.

### Typed Headers

Request and response headers use the `headers` keyword:

```knot
route Api where
  GET /todos headers {authorization: Text}
    -> [Todo] headers {xTotalCount: Int}
    = GetTodos
  POST {title: Text} /todos headers {authorization: Text}
    -> {id: Int}
    = CreateTodo
```

Field names use camelCase, auto-converted to HTTP-Header-Case: `authorization` → `Authorization`, `contentType` → `Content-Type`, `xRequestId` → `X-Request-Id`.

Request headers become constructor fields. When response headers are declared, the handler returns a `{body: ..., headers: ...}` record:

```knot
api = serve Api where
  GetTodos = \{authorization} -> do
    let todos = allTodos
    yield Ok {value: {body: todos, headers: {xTotalCount: length todos}}}
  CreateTodo = \{title, authorization} ->
    yield Ok {value: {body: addTodo title, headers: {}}}
```

Optional headers use `Maybe`:

```knot
route Api where
  GET /todos headers {authorization: Maybe Text} -> [Todo] = GetTodos
```

Server gets `Nothing {}` if absent, `Just {value: "..."}` if present. In `fetch`, `Nothing` headers are skipped.

On the `fetch` side, header fields are sent automatically. When response headers are declared, the result wraps as `{body: T, headers: H}`:

```knot
result <- fetch "https://api.example.com" (GetTodos {authorization: "Bearer tok"})
-- result : IO {network} (Result ... {body: [Todo], headers: {xTotalCount: Int}})
```

### Rate Limiting

Add a per-endpoint token-bucket rate limit with `rateLimit <expr>` (placed after the response type/headers, before `=`). The expression has type `RateLimit input a`:

```knot
type RequestCtx = {
  clientIp: Text,
  receivedAt: Int<Ms>,
  header: Text -> Maybe Text       -- case-insensitive lookup
}

type RateLimit input a = {
  key: input -> RequestCtx -> Maybe a,    -- Ord a; Nothing exempts the request
  limit: {requests: Int, window: Int<Ms>}
}
```

`key` receives the same input record the handler does (path/query/body/header fields, combined) plus the runtime-supplied `RequestCtx`, so you can key on any field of either:

```knot
byClientIp = \input ctx -> Just {value: ctx.clientIp}

byOwner = \{owner} ctx -> Just {value: owner}             -- key on a path/body field

route Api where
  GET /hello -> {message: Text}
    rateLimit {key: byClientIp, limit: {requests: 100, window: 60000<Ms>}}
    = Hello

  GET /user/{owner: Text} -> {message: Text}
    rateLimit {key: byOwner, limit: {requests: 10, window: 60000<Ms>}}
    = User

  GET /open -> {message: Text} = Open                  -- no clause = unlimited
```

The `key` value can be any `Ord` type — the runtime serializes it via `show` for the SQLite bucket key. Returning `Nothing` from `key` skips rate limiting for that request (e.g. exempt admin requests by reading `ctx.header "Authorization"`).

On rejection the runtime responds `429 Too Many Requests` with body `{"error":"Rate limit exceeded"}` and a `Retry-After: <seconds>` header — the handler is not invoked. Buckets persist in a hidden `_knot_rate_limits` SQLite table; concurrent requests for the same key serialize via `BEGIN IMMEDIATE`.

Common keying strategies are regular expressions, so extract them once and reuse:

```knot
serverLimit = {key: \input ctx -> Just {value: ctx.clientIp},
               limit: {requests: 1000, window: 60000<Ms>}}

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
migrate *people
  from {name: Text, age: Int}
  to   {name: Text, age: Int, email: Text}
  using (\old -> {old | email: old.name ++ "@unknown.com"})
```

---

## Subset Constraints

```knot
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
type Nat = Int where \x -> x >= 0

-- Per-field refinements
type ValidPerson = {name: Text, age: Int where \x -> x >= 0 && x <= 150}

-- Cross-field refinements
type Range = {lo: Int, hi: Int} where \r -> r.lo <= r.hi

-- ADT constructor refinements
data Shape
  = Circle {radius: Float where \r -> r > 0.0}
  | Rect {width: Float where \w -> w > 0.0, height: Float where \h -> h > 0.0}
```

### Checking with `refine`

`refine expr` validates a value against a refined type inferred from context. Returns `Result RefinementError T`:

```knot
-- Use with case
result = case refine someInt of
  Ok {value: n} -> println ("Valid: " ++ show n)
  Err {error: e} -> println ("Invalid: " ++ show e)

-- Use in Result do-block
validated = do
  n <- refine someInt        -- binds n : Nat on success, short-circuits on failure
  m <- refine otherInt
  yield (n + m)
-- validated : Result RefinementError Int
```

`RefinementError = {typeName: Text, violations: [{field: Maybe Text, message: Text}]}`

### Automatic Validation

**`set` validation**: refined fields on source relations are checked before writes. Panics on violation.

```knot
*people : [{name: Text, age: Nat}]

-- This panics if any age is negative:
set *people = [{name: "Alice", age: -1}]
```

**Route handlers**: refined body fields are auto-validated after JSON decoding. Returns HTTP 400 on failure.

```knot
route Api where
  POST {age: Nat} /users -> User = CreateUser

-- POST with {"age": -1} returns 400 automatically
```

### Subtyping

Refined types are subtypes of their base type. `Nat` is compatible with `Int` in both directions — passing a `Nat` where `Int` is expected works, and vice versa (but the latter is unchecked unless you use `refine`).

---

## Custom Monads

Any type implementing `Monad` gets `do`/`<-`/`yield` syntax:

```knot
data Maybe a = Nothing {} | Just {value: a}

impl Functor Maybe where
  map f m = case m of
    Nothing {} -> Nothing {}
    Just {value} -> Just {value: f value}

impl Applicative Maybe where
  yield x = Just {value: x}
  ap fs xs = case fs of
    Nothing {} -> Nothing {}
    Just {value: f} -> case xs of
      Nothing {} -> Nothing {}
      Just {value: x} -> Just {value: f x}

impl Monad Maybe where
  bind f m = case m of
    Nothing {} -> Nothing {}
    Just {value} -> f value

impl Alternative Maybe where
  empty = Nothing {}
  alt a b = case a of
    Nothing {} -> b
    Just {} -> a

-- Now do blocks work with Maybe:
result = do
  a <- Just {value: 10}
  b <- Just {value: 2}
  where b != 0
  yield (a / b)
```

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

add = \title owner priority -> do
  todos <- *todos
  set *todos = union todos [{title: title, owner: owner, priority: priority, status: Open {}}]

complete = \title -> do
  todos <- *todos
  set *todos = do
    t <- todos
    yield (if t.title == title
      then {t | status: Resolved {resolution: "done"}}
      else t)

assign = \title person -> do
  todos <- *todos
  set *todos = do
    t <- todos
    yield (if t.title == title
      then {t | status: InProgress {assignee: person}}
      else t)

pending = \owner -> do
  todos <- *todos
  let result = do
    t <- todos
    where t.owner == owner
    Open {} <- t.status
    yield {t.title, t.priority}
  yield result

&workload = do
  todos <- *todos
  let result = do
    t <- todos
    Open {} <- t.status
    groupBy {t.owner}
    yield {owner: t.owner, count: count t}
  yield result

main = do
  add "Write parser" "Alice" High {}
  add "Write codegen" "Alice" Critical {}
  add "Write runtime" "Bob" Medium {}
  assign "Write parser" "Carol"
  complete "Write runtime"
  p <- pending "Alice"
  println "Alice's pending:"
  println (show p)
  w <- &workload
  println "Workload:"
  println (show w)
  yield {}
```

---

## Common Patterns

### Insert

```knot
addRow = \newRow -> do
  rel <- *rel
  set *rel = union rel [newRow]
```

### Delete by condition

```knot
deleteWhere = \valueToDelete -> do
  rel <- *rel
  set *rel = do
    r <- rel
    where r.field != valueToDelete
    yield r
```

### Update by condition

```knot
updateWhere = \target newValue -> do
  rel <- *rel
  set *rel = do
    r <- rel
    yield (if r.id == target then {r | field: newValue} else r)
```

### Join two relations

```knot
&joined = do
  employees <- *employees
  departments <- *departments
  let result = do
    e <- employees
    d <- departments
    where e.dept == d.name
    yield {e.name, d.budget}
  yield result
```

### Aggregate

```knot
getTotal = do
  orders <- *orders
  yield (fold (\acc x -> acc + x.amount) 0 orders)

getCount = do
  people <- *people
  yield (count people)
```

### Filter by variant

```knot
-- Using match
&circles = do
  shapes <- *shapes
  yield (shapes |> match Circle)

-- Using pattern bind in do
&circles = do
  shapes <- *shapes
  let result = do
    Circle c <- shapes
    yield c
  yield result
```
