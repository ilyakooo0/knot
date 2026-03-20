# Knot Language Reference

Knot is a functional relational programming language. Relations (typed sets) are the primary data structure, computation is pure and functional, and state is automatically persisted to SQLite.

## Quick Start

```knot
type Person = {name: Text, age: Int}

*people : [Person]

main = do
  set *people = [{name: "Alice", age: 30}, {name: "Bob", age: 25}]
  p <- *people
  where p.age > 27
  yield p.name
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
  t <- *todos
  yield {title: t.title, owner: t.owner, status: Open {}}

-- Derived: read-only computed relation
&seniors = *people |> filter (\p -> p.age > 65)

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
filter (\p -> p.age > 30) *people
```

Pipe-forward operator:

```knot
*people
  |> filter (\p -> p.age > 30)
  |> map (\p -> p.name)
```

---

## Do Blocks

`do` blocks are the primary syntax for comprehensions and sequencing. They work with any type implementing the `Monad` trait — not just relations.

### Relation Comprehensions

```knot
&results = do
  e <- *employees          -- bind: draw from relation
  d <- *departments        -- bind: draw from another
  where e.dept == d.name   -- filter
  let bonus = e.salary * 0.1  -- local binding
  yield {e.name, bonus, d.budget}  -- emit row
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
main = do
  content <- readFile "input.txt"
  println content
  t <- now
  println ("time: " ++ show t)
  yield {}
```

The compiler detects whether a do block is relational or IO from the types.

### Pattern Matching in Bind

Filter and destructure in one step:

```knot
&circles = do
  Circle c <- *shapes    -- only Circle rows, bind payload to c
  yield c

&inProgress = do
  t <- *tickets
  InProgress ip <- t.status  -- match single-value field
  yield {t.title, ip.assignee}
```

---

## Mutation

All mutation uses `set`, which replaces a source relation with a new value. The runtime diffs old vs new to apply minimal changes.

```knot
-- Insert (union with singleton)
set *people = union *people [{name: "Alice", age: 30}]

-- Update (map with conditional)
set *people = do
  p <- *people
  yield (if p.name == "Alice" then {p | age: p.age + 1} else p)

-- Delete (filter to keep)
set *people = do
  p <- *people
  where p.name != "Alice"
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
  t <- *todos
  where t.done == 0
  groupBy {t.owner}
  yield {owner: t.owner, count: count t}
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
  t <- *teams
  m <- t.members
  yield {team: t.name, member: m.name}

-- Update nested relations
set *teams = do
  t <- *teams
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
  t <- *todos
  yield {title: t.title, owner: t.owner, priority: t.priority, status: Open {}}
```

Constant columns (like `status: Open {}`) are:
- **On read**: used as a filter (only open todos)
- **On write**: auto-filled (inserting through the view adds `status: Open {}`)
- **Hidden from the type**: the view's type omits constant columns

```knot
-- Insert through view — status auto-filled
set *openTodos = union *openTodos [{title: "New task", owner: "Alice", priority: High {}}]
```

---

## Derived Relations

Read-only computed relations, prefixed with `&`:

```knot
&seniors = *people |> filter (\p -> p.age > 65)

&stats = do
  t <- *todos
  groupBy {t.owner}
  yield {owner: t.owner, total: count t}
```

### Recursive Derived Relations

Datalog-style fixpoint iteration for transitive closure:

```knot
&reportsTo = do
  let step = do
    r <- &reportsTo        -- self-reference
    m <- *manages
    where r.descendant == m.manager
    yield {ancestor: r.ancestor, descendant: m.report}
  r <- union &base step
  yield r
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
println : a -> IO {console} {}
readFile : Text -> IO {fs} Text
now : IO {clock} Int
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

DB reads/writes are inferred, not IO-wrapped:

```knot
-- Inferred: {reads *people, writes *people}
birthday = \name ->
  set *people = do
    p <- *people
    yield (if p.name == name then {p | age: p.age + 1} else p)
```

### Transactions

```knot
handleOrder = \item -> do
  orderId <- atomic do
    set *orders = union *orders [{item: item, qty: 1}]
    yield (count *orders)
  println ("Order #" ++ show orderId)
  yield {orderId}
```

IO and DB writes cannot mix inside `atomic`.

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
| `sum` | `(a -> Int) -> [a] -> Int` | Sum projected field |
| `avg` | `(a -> Num) -> [a] -> Float` | Average projected field |
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
serve = \req -> case req of
  GetTodos {owner, respond} -> respond (getTodos owner)
  CreateTodo {title, owner, respond} -> respond (addTodo title owner)
  GetCount {respond} -> respond (count *todos)

main = listen 8080 serve
```

Each route constructor gets a `respond` callback for type-safe responses.

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

Request headers become constructor fields. When response headers are declared, `respond` takes two arguments — body then headers:

```knot
serve = \req -> case req of
  GetTodos {authorization, respond} ->
    let todos = allTodos
    respond todos {xTotalCount: length todos}
  CreateTodo {title, authorization, respond} ->
    respond (addTodo title) {}
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

## Temporal Queries

```knot
*employees : [{name: Text, salary: Int}]
  with history

-- Query past state
salaryLastYear = \name -> do
  t <- now
  yield (*employees @(t - 365 days)
    |> filter (\e -> e.name == name)
    |> map (\e -> e.salary)
    |> single)
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

add = \title owner priority ->
  set *todos = union *todos [{title: title, owner: owner, priority: priority, status: Open {}}]

complete = \title ->
  set *todos = do
    t <- *todos
    yield (if t.title == title
      then {t | status: Resolved {resolution: "done"}}
      else t)

assign = \title person ->
  set *todos = do
    t <- *todos
    yield (if t.title == title
      then {t | status: InProgress {assignee: person}}
      else t)

pending = \owner -> do
  t <- *todos
  where t.owner == owner
  Open {} <- t.status
  yield {t.title, t.priority}

&workload = do
  t <- *todos
  Open {} <- t.status
  groupBy {t.owner}
  yield {owner: t.owner, count: count t}

main = do
  add "Write parser" "Alice" High {}
  add "Write codegen" "Alice" Critical {}
  add "Write runtime" "Bob" Medium {}
  assign "Write parser" "Carol"
  complete "Write runtime"
  println "Alice's pending:"
  println (show (pending "Alice"))
  println "Workload:"
  println (show &workload)
  yield {}
```

---

## Common Patterns

### Insert

```knot
set *rel = union *rel [newRow]
```

### Delete by condition

```knot
set *rel = do
  r <- *rel
  where r.field != valueToDelete
  yield r
```

### Update by condition

```knot
set *rel = do
  r <- *rel
  yield (if r.id == target then {r | field: newValue} else r)
```

### Join two relations

```knot
&joined = do
  e <- *employees
  d <- *departments
  where e.dept == d.name
  yield {e.name, d.budget}
```

### Aggregate

```knot
total = fold (\acc x -> acc + x.amount) 0 *orders
n = count *people
```

### Filter by variant

```knot
-- Using match
&circles = *shapes |> match Circle

-- Using pattern bind in do
&circles = do
  Circle c <- *shapes
  yield c
```
