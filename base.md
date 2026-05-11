# Knot Standard Library

Every function listed here is built in to the compiler. No imports needed.

## IO

Effectful functions return `IO` values — pure descriptions of side effects. IO actions are executed when the program runs (via `main`). Use do-blocks with `<-` to sequence IO actions and extract their results.

```knot
println : a -> IO {console} {}
```
Print a value followed by a newline. Works on any type. Returns an IO action.

```knot
print : a -> IO {console} {}
```
Print a value without a trailing newline.

```knot
readLine : IO {console} Text
```
Read a line from stdin.

```knot
show : a -> Text
```
Convert any value to its text representation. Records print as `{field: value, ...}`, relations as `[v1, v2, ...]`, constructors as `Tag {fields}`. This is a pure function (no IO).

```knot
fork : IO r {} -> IO {} {}
```
Run an IO action on a new OS thread (fire-and-forget). The spawned action's effects are decoupled from the caller's. Each thread gets its own SQLite connection (WAL mode). Main waits for all threads before exiting.

```knot
race : IO r a -> IO r b -> IO r (Result a b)
```
Run two IO actions concurrently and return the winner. The result is `Err {error: a}` if the left action wins, `Ok {value: b}` if the right wins. Both arguments share an effect row, so effects from either side propagate to the caller. Cancellation is cooperative but aggressive — the loser unwinds at its next IO thunk boundary and `sleep` wakes immediately on a cancel signal.

```knot
atomic : IO {} a -> IO {} a
```
Run an IO body in a database transaction. Body must contain only DB operations. Supports `retry` for STM-style waiting.

```knot
retry : a
```
Inside `atomic` blocks only. Rolls back the transaction and waits for a relation change, then re-executes. The runtime uses row-level invalidation: only writes that touch rows the atomic block actually read (via `WHERE` / `single (filter ...)` predicates) wake the watcher.

### IO Do-Blocks

Use `do` to sequence IO actions:

```knot
main = do
  println "What is your name?"
  name <- readLine
  println ("Hello, " ++ name)
  yield {}
```

The `<-` operator runs an IO action and binds its result. Bare IO expressions (like `println`) are also executed. The overall block type is `IO {union of effects} result`.

## Relations

Source relations (`*rel`) and derived relations (`&rel`) return `IO {} [T]`. Use `<-` in an IO do-block to unwrap the relation value before passing it to these functions.

```knot
union : [a] -> [a] -> [a]
```
Set union of two relations. Duplicates (by structural equality) are removed.

```knot
count : [a] -> Int
```
Number of rows in a relation.

```knot
filter : (a -> Bool) -> [a] -> [a]
```
Keep rows where the predicate returns `True`.

```knot
-- Direct call
filter (\p -> p.age > 65) *people

-- With pipe
*people |> filter (\p -> p.age > 65)
```

```knot
map : (a -> b) -> [a] -> [b]
```
Apply a function to every row. Duplicates in the result are removed (relations are sets).

```knot
map (\p -> {p.name, p.age}) *people
```

```knot
fold : (b -> a -> b) -> b -> [a] -> b
```
Left fold over a relation. The accumulator function is curried: `fold f init rel`.

```knot
-- Sum
fold (\acc x -> acc + x) 0 [1, 2, 3]    -- 6

-- Build text
fold (\acc p -> acc ++ p.name ++ " ") "" *people
```

```knot
single : [a] -> a
```
Extract the single element from a one-element relation. Panics if the relation is empty or has more than one element.

```knot
single [{name: "Alice"}]    -- {name: "Alice"}
```

```knot
diff : [a] -> [a] -> [a]
```
Set difference. Returns rows in the first relation that are not in the second.

```knot
diff [1, 2, 3, 4, 5] [2, 4]    -- [1, 3, 5]

-- Remove known users from a list
diff *allUsers *bannedUsers
```

```knot
inter : [a] -> [a] -> [a]
```
Set intersection. Returns rows that appear in both relations.

```knot
inter [1, 2, 3, 4, 5] [2, 4, 6]    -- [2, 4]

-- Find users who are also admins
inter *users *admins
```

```knot
sum : (a -> b) -> [a] -> b
```
Sum a numeric projection over a relation. The projection function extracts the value to sum from each row. Works with `Int`, `Float`, and unit-annotated types — units are preserved.

```knot
sum (\x -> x) [10, 20, 30]              -- 60
sum (\o -> o.amount) *orders             -- total of all order amounts
*orders |> sum (\o -> o.amount)          -- same with pipe
sum (\t -> t.distance) *trips            -- Float<M> if distance : Float<M>
```

```knot
avg : (a -> Float<u>) -> [a] -> Float<u>
```
Average a numeric projection over a relation. Returns `Float`. Returns `0.0` for an empty relation. Preserves units — if the projection returns `Float<M>`, the average is `Float<M>`.

```knot
avg (\x -> x) [10.0, 20.0, 30.0]        -- 20.0
avg (\e -> e.salary) *employees          -- mean salary
avg (\t -> t.distance) *trips            -- Float<M> if distance : Float<M>
```

```knot
minOn : (a -> b) -> [a] -> b
maxOn : (a -> b) -> [a] -> b
```
Minimum and maximum of a projected field over a relation. Works with any orderable projection (`Int`, `Float`, `Text`, units). Panics on empty relations. Push down to `SELECT MIN(col)` / `SELECT MAX(col)` when applied to a source relation.

```knot
minOn (\e -> e.salary) *employees        -- lowest salary
maxOn (\t -> t.distance) *trips          -- Float<M> if distance : Float<M>
```

```knot
min : Ord a => a -> a -> a
max : Ord a => a -> a -> a
```
Binary min/max of two values. Use `minOn`/`maxOn` for aggregates over a relation; `min`/`max` operate on two scalars.

```knot
min 3 7        -- 3
max "a" "b"    -- "b"
```

```knot
countWhere : (a -> Bool) -> [a] -> Int
```
Number of rows satisfying a predicate. Equivalent to `count (filter p rel)` but pushes down to `SELECT COUNT(*) ... WHERE ...` when the predicate is SQL-compilable.

```knot
countWhere (\e -> e.dept == "Eng") *employees
```

## Text

```knot
toUpper : Text -> Text
```
Convert text to uppercase.

```knot
toLower : Text -> Text
```
Convert text to lowercase.

```knot
take : Int -> Text -> Text
```
First *n* characters of a text value.

```knot
take 5 "hello world"    -- "hello"
```

```knot
drop : Int -> Text -> Text
```
Skip the first *n* characters.

```knot
drop 6 "hello world"    -- "world"
```

```knot
length : Text -> Int
```
Number of characters in a text value.

```knot
trim : Text -> Text
```
Strip leading and trailing whitespace.

```knot
contains : Text -> Text -> Bool
```
Check whether a text value contains a substring. The first argument is the needle, the second is the haystack.

```knot
contains "ell" "hello"    -- True
"hello" |> contains "ell" -- True
```

```knot
reverse : Text -> Text
```
Reverse a text value.

```knot
chars : Text -> [Text]
```
Convert text into a relation of single-character text values.

```knot
chars "abc"    -- ["a", "b", "c"]
```

## Utility

```knot
id : a -> a
```
Identity function. Returns its argument unchanged.

```knot
not : Bool -> Bool
```
Boolean negation. Function form of the `!` operator.

```knot
now : IO {clock} Int<Ms>
```
Current time in milliseconds since the Unix epoch, tagged with the built-in `Ms` unit. Use `<-` in a do-block to get the value:

```knot
main = do
  t <- now
  println ("Time: " ++ show t)
  yield {}
```

```knot
sleep : Int<Ms> -> IO {clock} {}
```
Pause the current thread for the given number of milliseconds. Inside a `race` worker, `sleep` parks on the worker's cancel condvar and returns immediately if the peer wins the race.

```knot
randomInt : Int<u> -> IO {random} Int<u>
```
Random integer in `[0, bound)`. Unit-polymorphic — the bound's unit is preserved in the result, so `randomInt 100<Usd>` produces `Int<Usd>`.

```knot
randomFloat : IO {random} Float<u>
```
Random float in `[0.0, 1.0)`. Unit-polymorphic — the unit is inferred from context.

```knot
randomUuid : IO {random} Uuid
```
Generate a fresh RFC 9562 UUIDv7. Time-ordered, so values sort chronologically — well-suited for primary keys.

```knot
main = do
  u <- randomUuid
  println u
  yield {}
```

## JSON

```knot
toJson : a -> Text
```
Convert any value to its JSON text representation. Records become JSON objects, relations become JSON arrays, `Int`/`Float` become numbers, `Text` becomes a JSON string, `Bool` becomes `true`/`false`, and `{}` becomes `{}`.

```knot
toJson {name: "Alice", age: 30}    -- "{\"name\":\"Alice\",\"age\":30}"
toJson [1, 2, 3]                   -- "[1,2,3]"
```

```knot
parseJson : Text -> a
```
Parse a JSON string into a Knot value. JSON objects become records, arrays become relations, strings become `Text`, integers become `Int`, decimals become `Float`, booleans become `Bool`, and `null` becomes `{}`. Handles standard JSON escape sequences.

```knot
parseJson "{\"x\": 10}"           -- {x: 10}
parseJson "[1, 2, 3]"             -- [1, 2, 3]

-- Round-trip
let json = toJson {name: "Bob"}
parseJson json                     -- {name: "Bob"}
```

## Bytes

```knot
bytesLength : Bytes -> Int
```
Number of bytes in a byte string.

```knot
bytesSlice : Int -> Int -> Bytes -> Bytes
```
Extract a sub-range. `bytesSlice start len bytes` returns `len` bytes starting at offset `start`.

```knot
bytesConcat : Bytes -> Bytes -> Bytes
```
Concatenate two byte strings.

```knot
bytesGet : Int -> Bytes -> Int
```
Get the byte value (0–255) at the given index.

```knot
textToBytes : Text -> Bytes
```
UTF-8 encode a text value into bytes.

```knot
bytesToText : Bytes -> Text
```
UTF-8 decode bytes into text.

```knot
bytesToHex : Bytes -> Text
```
Encode bytes as a hexadecimal text string.

```knot
bytesFromHex : Text -> Bytes
```
Decode a hexadecimal text string into bytes.

```knot
bytesToHex (textToBytes "hi")    -- "6869"
bytesFromHex "6869" |> bytesToText  -- "hi"
```

## File System

All file system functions return `IO {fs}` values.

```knot
readFile : Text -> IO {fs} Text
```
Read the entire contents of a file as text. Returns an IO action.

```knot
main = do
  content <- readFile "config.json"
  println content
  yield {}
```

```knot
writeFile : Text -> Text -> IO {fs} {}
```
Write text to a file. Creates the file if it doesn't exist, overwrites if it does. The first argument is the path, the second is the contents.

```knot
writeFile "output.txt" "hello"
```

```knot
appendFile : Text -> Text -> IO {fs} {}
```
Append text to a file. Creates the file if it doesn't exist.

```knot
main = do
  t <- now
  appendFile "app.log" ("event at " ++ show t ++ "\n")
```

```knot
fileExists : Text -> IO {fs} Bool
```
Check whether a file or directory exists at the given path.

```knot
loadConfig = \path -> do
  exists <- fileExists path
  if exists
    then readFile path
    else yield "{}"
```

```knot
removeFile : Text -> IO {fs} {}
```
Delete a file.

```knot
listDir : Text -> IO {fs} [Text]
```
List the entries of a directory as a relation of filenames.

```knot
main = do
  files <- listDir "."
  println files
  yield {}
```

## Server

```knot
listen : Int -> Server a -> IO {network} {}
```
Start an HTTP server on the given port. The second argument is a `Server a` value, typically built with the `serve API where ...` expression. Has the `{network}` effect. See `route` declarations in the language spec for defining typed endpoints and the `serve` form for binding handlers.

Handlers return `Result HttpError T` where `HttpError = {status: Int, message: Text}` and `T` is the response type declared on the route. `Ok {value: T}` responds with HTTP 200; `Err {error: {status, message}}` responds with the given status code (clamped to 100..=599) and a JSON `{"error": message}` body — use this for application-level errors like 404 not found or 401 unauthorized.

### Rate limiting

Endpoints can declare a per-route token-bucket rate limit with the `rateLimit <expr>` clause (placed after the response type/headers, before `=`). The expression has type `RateLimit input a = {key: input -> RequestCtx -> Maybe a, limit: {requests: Int, window: Int<Ms>}}` with `Ord a`. `input` is the same record the handler receives (path/query/body/header fields). `RequestCtx = {clientIp: Text, receivedAt: Int<Ms>, header: Text -> Maybe Text}` is supplied by the runtime; `header` does case-insensitive lookup.

The `key` function returns `Just k` to put the request in bucket `k` (any `Ord` value — serialized via `show` for the SQLite key) or `Nothing` to exempt the request from rate limiting. On rejection the runtime responds `429 Too Many Requests` with `{"error":"Rate limit exceeded"}` and a `Retry-After: <seconds>` header; the handler is not invoked.

Buckets persist across restarts in a hidden `_knot_rate_limits` SQLite table; concurrent requests for the same `(route, key)` pair serialize via `BEGIN IMMEDIATE`.

```knot
byIp = \input ctx -> Just {value: ctx.clientIp}

byOwner = \{owner} ctx -> Just {value: owner}    -- key on a path/body field

route Api where
  GET /hello -> {message: Text}
    rateLimit {key: byIp, limit: {requests: 100, window: 60000<Ms>}}
    = Hello

  GET /user/{owner: Text} -> {message: Text}
    rateLimit {key: byOwner, limit: {requests: 10, window: 60000<Ms>}}
    = User
```

## Concurrency

```knot
fork : IO r {} -> IO {} {}
```
Run an IO action on a new OS thread. Fire-and-forget — the spawned thread runs independently and its effects are decoupled from the caller's. Each thread gets its own SQLite connection (WAL mode). Main waits for all threads before exiting. Do blocks can be passed without parentheses: `fork do ...`.

```knot
main = do
  fork do
    println "from thread"
  println "from main"
  yield {}
```

```knot
race : IO r a -> IO r b -> IO r (Result a b)
```
Run two IO actions concurrently and return the winner. The arguments share an effect row so effects from either side flow through to the caller's IO. The winner is reported via the built-in `Result` ADT — `Err {error: a}` when the left action wins, `Ok {value: b}` when the right wins.

Cancellation is cooperative: the loser's IO chain checks its cancel token between thunks and `sleep` parks on a condvar that wakes immediately on cancel. The parent does not join the loser — it returns as soon as a winner is observed, and the loser is tracked for the final program-exit join. `race` cannot be used inside `atomic` (its effects are not rollback-safe).

```knot
slow = do
  sleep 1000<Ms>
  yield "slow"

fast = do
  sleep 50<Ms>
  yield "fast"

main = do
  r <- race slow fast
  case r of
    Err {error: a} -> println ("left won: " ++ a)
    Ok {value: b}  -> println ("right won: " ++ b)
  yield {}
```

```knot
atomic : IO {} a -> IO {} a
```
Run an IO body as a database transaction. Body must contain only DB operations (no console, fs, etc.). Supports `retry` for STM-style waiting.

```knot
transfer = \from to amount -> atomic do
  accounts <- *accounts
  set *accounts = do
    a <- accounts
    yield (if a.name == from then {a | balance: a.balance - amount}
           else if a.name == to then {a | balance: a.balance + amount}
           else a)
```

```knot
retry : a
```
Inside `atomic` blocks only. Rolls back the transaction and waits for a relation to change, then re-executes. Implements STM (Software Transactional Memory).

```knot
waitForReady = atomic do
  status <- *status
  where (count (filter (\s -> s.ready) status)) == 0
  retry
```

The runtime narrows wakeups to rows the atomic block actually read. Codegen extracts the predicates inside `single (filter (\r -> r.col OP expr) rows)` and SQL-pushed-down query patterns — equality, inequality, ordered comparisons, and `IN` sets — and registers them as row-level read filters. When another transaction commits an UPDATE, DELETE, or INSERT, only watchers whose filters match the affected rows wake; everyone else stays parked. Bulk replacements (`set *rel = ...`) wake all watchers conservatively.

## IO Type Syntax

IO types can be annotated in type signatures:

```knot
IO {console} {}              -- IO action with console effect, returns unit
IO {fs} Text                 -- IO action with fs effect, returns Text
IO {clock, random} Int       -- IO action with multiple effects
IO {network | r} {}          -- open effect row (used by listen)
```

Effects tracked in IO types: `console`, `fs`, `network`, `clock`, `random`. A trailing `| r` makes the row open — additional effects may be added by unification (used for handler-effect propagation in `listen`).

## Traits

The following traits are defined in the prelude. All types that implement a trait can use its methods without imports.

### Eq

```knot
trait Eq a where
  eq : a -> a -> Bool
```
Structural equality as a trait method. Built-in impls for `Int`, `Float`, `Text`, `Bool`.

### Ord

```knot
trait Eq a => Ord a where
  compare : a -> a -> Int
```
Ordering comparison. Returns `-1`, `0`, or `1`. Requires `Eq`. Built-in impls for `Int`, `Float`, `Text`.

### Num

```knot
trait Eq a => Num a where
  add : a -> a -> a
  sub : a -> a -> a
  mul : a -> a -> a
  div : a -> a -> a
  mod : a -> a -> a
  negate : a -> a
```
Numeric operations as trait methods. Requires `Eq`. Built-in impls for `Int` and `Float`. Use as a trait bound for generic numeric code:

```knot
double : Num a => a -> a
double = \x -> add x x

double 21      -- 42
double 1.5     -- 3.0
```

The methods wrap the corresponding built-in operators (`+`, `-`, `*`, `/`, `%`, unary `-`), but can be passed as values and used in higher-order functions:

```knot
fold add 0 [1, 2, 3]    -- 6
```

### Display

```knot
trait Display a where
  display : a -> Text
```
Convert a value to its text representation. Built-in impls for `Int`, `Float`, `Text`, `Bool`. The `Text` impl returns the value as-is; the others delegate to `show`.

Use as a trait bound for generic formatting:

```knot
displayAll : Display a => [a] -> [Text]
displayAll = \rel -> do
  r <- rel
  yield (display r)
```

Implement for your own types:

```knot
data Color = Red {} | Green {} | Blue {}

impl Display Color where
  display c = case c of
    Red {} -> "red"
    Green {} -> "green"
    Blue {} -> "blue"
```

### Functor

```knot
trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b
```

### Applicative

```knot
trait Functor f => Applicative (f : Type -> Type) where
  yield : a -> f a
  ap : f (a -> b) -> f a -> f b
```

### Monad

```knot
trait Applicative m => Monad (m : Type -> Type) where
  bind : (a -> m b) -> m a -> m b
```
Enables `do` / `<-` / `yield` syntax. Built-in impls for `[]`, `IO`, `Maybe`, and `Result`.

### Alternative

```knot
trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt : f a -> f a -> f a
```
Built-in impls: `[]` (`empty = []`, `alt = union`) and `Maybe` (`empty = Nothing {}`, `alt` takes the first `Just`).

### Foldable

```knot
trait Foldable (t : Type -> Type) where
  fold : (b -> a -> b) -> b -> t a -> b
```

## Operators

These are not functions but are available as infix operators:

| Operator | Type | Description |
|----------|------|-------------|
| `+` | `Int -> Int -> Int` | Addition (also `Float`) |
| `-` | `Int -> Int -> Int` | Subtraction |
| `*` | `Int -> Int -> Int` | Multiplication |
| `/` | `Int -> Int -> Int` | Division |
| `%` | `Int -> Int -> Int` | Modulo / remainder (also `Float`) |
| `==` | `a -> a -> Bool` | Structural equality |
| `!=` | `a -> a -> Bool` | Structural inequality |
| `<` | `a -> a -> Bool` | Less than |
| `>` | `a -> a -> Bool` | Greater than |
| `<=` | `a -> a -> Bool` | Less than or equal |
| `>=` | `a -> a -> Bool` | Greater than or equal |
| `&&` | `Bool -> Bool -> Bool` | Logical and |
| `\|\|` | `Bool -> Bool -> Bool` | Logical or |
| `++` | `Text -> Text -> Text` | Text concatenation |
| `\|>` | `a -> (a -> b) -> b` | Pipe forward |
| `-` (unary) | `Int -> Int` | Negation |
| `!` (unary) | `Bool -> Bool` | Logical not |

## Currying

All multi-argument standard library functions support partial application. You can pass fewer arguments to get back a function:

```knot
-- Partially apply filter
isOld = filter (\p -> p.age > 65)
isOld *people    -- same as: filter (\p -> p.age > 65) *people

-- Pipe-forward works because filter returns a function after one arg
*people |> filter (\p -> p.age > 65) |> map (\p -> p.name)

-- Partially apply fold
sum = fold (\acc x -> acc + x) 0
sum [1, 2, 3]    -- 6
```
