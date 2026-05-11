# Knot Standard Library

Complete reference for all built-in functions, traits, and types.

## Table of Contents

- [Relation Operations](#relation-operations)
- [Concurrency](#concurrency)
- [Text Operations](#text-operations)
- [Console I/O](#console-io)
- [Control Flow](#control-flow)
- [File System](#file-system)
- [Time](#time)
- [Random](#random)
- [JSON](#json)
- [Bytes](#bytes)
- [HTTP](#http)
- [Cryptography](#cryptography)
- [Utility Functions](#utility-functions)
- [Built-in Traits](#built-in-traits)
- [Built-in Types](#built-in-types)
- [Operators](#operators)

---

## Relation Operations

### `filter`

```
filter : (a -> Bool) -> [a] -> [a]
```

Keep rows where the predicate returns `True`.

```knot
&seniors = *people |> filter (\p -> p.age > 65)
```

### `map`

```
map : (a -> b) -> [a] -> [b]
```

Apply a function to each row. Results are deduplicated (relations are sets).

```knot
&names = *people |> map (\p -> {name: p.name})
```

`map` is the `Functor` trait method for `[]`.

### `match`

```
match : Constructor -> [Constructor] -> [Payload]
```

Filter a relation to rows matching a constructor tag, extracting the payload.

```knot
&circles = *shapes |> match Circle    -- : [{radius: Float}]
&rects   = *shapes |> match Rect      -- : [{width: Float, height: Float}]
```

### `fold`

```
fold : (b -> a -> b) -> b -> [a] -> b
```

Left fold over a relation. `fold` is the `Foldable` trait method for `[]`.

```knot
totalAmount = \rel -> fold (\acc r -> acc + r.amount) 0 rel
```

### `single`

```
single : [a] -> Maybe a
```

Extract the single element of a relation. Returns `Just {value: x}` for a singleton, `Nothing {}` for empty or multi-element relations.

```knot
single [{name: "Alice"}]    -- Just {value: {name: "Alice"}}
single []                   -- Nothing {}
single [1, 2]               -- Nothing {}
```

### `count`

```
count : [a] -> Int
```

Return the number of rows in a relation.

```knot
numPeople = count *people
```

When the argument is a source relation (or its bound alias), the compiler emits a single `SELECT COUNT(*)` query. Pipe forms like `*people |> filter (\p -> p.age > 30) |> count` collapse into one `SELECT COUNT(*) FROM ... WHERE ...`.

### `countWhere`

```
countWhere : (a -> Bool) -> [a] -> Int
```

Count rows that satisfy a predicate. Equivalent to `count . filter`, but pushes down to a single `SELECT COUNT(*) FROM ... WHERE pred` when the predicate is SQL-compilable.

```knot
engHeadcount = do
  employees <- *employees
  yield (countWhere (\e -> e.dept == "Eng") employees)
```

### `sum`

```
sum : (a -> b) -> [a] -> b
```

Sum of a projected numeric field over a relation. Works with `Int`, `Float`, and unit-annotated types — units are preserved through the projection.

```knot
totalAge = sum (\p -> p.age) *people

-- Unit-preserving:
totalDistance = sum (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
```

### `avg`

```
avg : (a -> Float<u>) -> [a] -> Float<u>
```

Average of a projected numeric field over a relation. Returns `Float`. Preserves units from the projection function — if the projection returns `Float<M>`, the average is `Float<M>`.

### `minOn`

```
minOn : (a -> b) -> [a] -> b
```

Minimum of a projected field over a relation. The projection can return any orderable type — `Int`, `Float`, or `Text` (lexicographic ordering). Panics if the relation is empty.

```knot
lowestSalary = do
  employees <- *employees
  yield (minOn (\e -> e.salary) employees)

firstName = do
  employees <- *employees
  yield (minOn (\e -> e.name) employees)
```

When applied to a source (or bound source variable), it pushes down to `SELECT MIN(col) FROM ...`. Combined with `filter` it becomes `SELECT MIN(col) FROM ... WHERE ...`.

### `maxOn`

```
maxOn : (a -> b) -> [a] -> b
```

Maximum of a projected field over a relation. Like `minOn`, works with any orderable type. Panics if the relation is empty. Pushes down to `SELECT MAX(col) FROM ...`.

```knot
highestSalary = do
  employees <- *employees
  yield (maxOn (\e -> e.salary) employees)
```

### `min` / `max`

```
min : Ord a => a -> a -> a
max : Ord a => a -> a -> a
```

Binary minimum and maximum of two values. Use `minOn`/`maxOn` to aggregate
over a relation; `min`/`max` operate on two single values.

```knot
min 3 7         -- 3
max "a" "b"     -- "b"
```

### `union`

```
union : [a] -> [a] -> [a]
```

Set union of two relations.

```knot
&all = union *employees *contractors
```

### `diff`

```
diff : [a] -> [a] -> [a]
```

Set difference — rows in the first relation but not the second.

```knot
&nonManagers = diff *employees *managers
```

### `inter`

```
inter : [a] -> [a] -> [a]
```

Set intersection — rows present in both relations.

### `head`

```
head : [a] -> Maybe a
```

First row of a relation in iteration order, or `Nothing {}` if empty.

### `findFirst`

```
findFirst : [a] -> (a -> Bool) -> Maybe a
```

First row matching the predicate (left-to-right), or `Nothing {}` when no row matches. Stops at the first hit.

```knot
findFirst [1, 2, 3, 4, 5] (\x -> x > 3)   -- Just {value: 4}
```

### `any` / `all`

```
any : (a -> Bool) -> [a] -> Bool
all : (a -> Bool) -> [a] -> Bool
```

`any` is `True` when some row matches; `all` is `True` only when every row matches (vacuously `True` on `[]`).

### `elem`

```
elem : a -> [a] -> Bool
```

Membership check by structural equality.

### `sortBy`

```
sortBy : (a -> b) -> [a] -> [a]
```

Reorder rows by a projected key. The key type `b` must be `Ord`. Returns a new relation with rows in ascending key order. Sets have no inherent order; the result preserves the sorted order for downstream iteration (`fold`, `map`, `forEach`, etc.).

Pushes down to SQL `ORDER BY` when applied to a source relation. Combined with `take` it becomes `ORDER BY ... LIMIT`:

```knot
&topFive = do
  employees <- *employees
  yield (employees |> sortBy (\e -> -e.salary) |> take 5)
-- SQL: SELECT ... FROM _knot_employees ORDER BY -salary LIMIT 5
```

### `take` / `drop`

```
take : Int -> [a] -> [a]      -- Sequence.take
drop : Int -> [a] -> [a]      -- Sequence.drop
```

First / drop *n* rows. Both come from the `Sequence` trait, which also has a `Text` impl that operates on characters.

### `reverse`

```
reverse : [a] -> [a]
```

Reverse iteration order of a relation. Like `sortBy`, this controls iteration order; the underlying set is unchanged.

---

## Concurrency

### `fork`

```
fork : IO r {} -> IO r {}
```

Run an IO action on a new OS thread (fire-and-forget). The spawned action's effect row `r` propagates through `fork` to the caller, so a program that forks an IO performing `println` is visibly typed with `{console}` in its IO row. Each thread gets its own SQLite connection via WAL mode for safe concurrent access. The main thread waits for all spawned threads before exiting.

```knot
main = do
  fork do
    println "hello from thread 1"
  fork do
    println "hello from thread 2"
  println "hello from main"
```

Do blocks can be passed directly as arguments without parentheses.

### `race`

```
race : IO r a -> IO r b -> IO r (Result a b)
```

Run two IO actions concurrently and return the winner. Both arguments share a single effect row, so any effects required by either side flow into the result IO.

The winner is reported via the built-in `Result a b` ADT — `Err {error: a}` when the left action wins, `Ok {value: b}` when the right action wins.

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

Cancellation is cooperative but aggressive: the loser's `knot_io_run` checks its cancel token between every IO thunk, and `sleep` parks on a condvar that's signalled on cancel — so a loser stuck in a long sleep wakes immediately when the peer wins. The parent does not wait for the loser; it returns as soon as a winner is observed, and the loser unwinds at its next safe point (tracked for the final program-exit join).

`race` cannot be used inside `atomic` — its effects are not rollback-safe.

### `atomic`

```
atomic : IO {} a -> IO {} a
```

Run an IO body in a database transaction. The body must contain only DB operations — no external effects (console, fs, etc.) are allowed. If the body calls `retry`, the transaction rolls back and waits for a relation change before re-executing.

```knot
transfer = \from to amount -> atomic do
  accounts <- *accounts
  set *accounts = do
    a <- accounts
    yield (if a.name == from then {a | balance: a.balance - amount}
           else if a.name == to then {a | balance: a.balance + amount}
           else a)
```

### `retry`

```
retry : a
```

Used inside `atomic` blocks only. Causes the transaction to rollback and wait until some relation changes, then re-executes the atomic block. Implements STM (Software Transactional Memory) style concurrency.

```knot
waitForTask = \id -> atomic do
  tasks <- *tasks
  let done = do
    t <- tasks
    where t.id == id
    where t.status == "done"
    yield t
  where (count done) == 0
  retry
  yield done
```

The compiler enforces that `retry` is only used inside `atomic`.

**Row-level wakeup filtering.** The runtime tracks which rows the atomic block
actually read by inspecting `WHERE`/`single (filter ...)` patterns and the
predicates inside them (equality, inequality, ordered comparisons, and `IN`
sets). A parked retry is only woken when an UPDATE, DELETE, or INSERT touches
a matching row. So a worker retrying on `WHERE id = 1` is not woken by writes
to `id = 2`, and a worker retrying on `status IN ("queued", "running")` is
unaffected by writes that leave the status outside that set. Bulk
replacements (`set *rel = ...`) wake all watchers conservatively.

---

## Text Operations

### `toUpper`

```
toUpper : Text -> Text
```

Convert text to uppercase.

### `toLower`

```
toLower : Text -> Text
```

Convert text to lowercase.

### `length`

```
length : Text -> Int
```

Return the number of characters (Unicode-aware).

### `trim`

```
trim : Text -> Text
```

Strip leading and trailing whitespace.

### `reverse`

```
reverse : Text -> Text
```

Reverse text.

### `chars`

```
chars : Text -> [Text]
```

Split text into a relation of single characters.

### `take` / `drop`

`take` and `drop` are `Sequence` trait methods with built-in impls for both
`Text` (characters) and relations (rows):

```
take : Int -> Text -> Text         -- characters
take : Int -> [a]  -> [a]          -- rows
drop : Int -> Text -> Text
drop : Int -> [a]  -> [a]
```

```knot
take 3 "hello"        -- "hel"
take 2 [10, 20, 30]   -- [10, 20]
drop 3 "hello"        -- "lo"
drop 1 [10, 20, 30]   -- [20, 30]
```

### `contains`

```
contains : Text -> Text -> Bool
```

Check if the second argument contains the first as a substring.

```knot
has = contains "ell" "hello"   -- True
```

---

## Console I/O

### `println`

```
println : a -> IO {console} {}
```

Print a value to stdout followed by a newline.

### `print`

```
print : a -> IO {console} {}
```

Print a value to stdout without a trailing newline.

### `logInfo` / `logWarn` / `logError` / `logDebug`

```
logInfo  : a -> IO {console} {}
logWarn  : a -> IO {console} {}
logError : a -> IO {console} {}
logDebug : a -> IO {console} {}
```

Leveled logging to stderr (so output does not mix with `println` on stdout). When stderr is a TTY, output is colored; otherwise each record is written as one JSON line for log aggregators. `logDebug` only emits when the program is launched with `--debug` — debug records are dropped silently otherwise.

```knot
main = do
  logInfo "starting"
  logWarn {event: "low memory", availableMb: 64}
  yield {}
```

### `show`

```
show : a -> Text
```

Convert any value to its text representation. This is a pure function (no IO).

### `readLine`

```
readLine : IO {console} Text
```

Read a line of input from stdin.

---

## Control Flow

### `when` / `unless`

```
when   : Bool -> IO r {} -> IO r {}
unless : Bool -> IO r {} -> IO r {}
```

Run an IO action conditionally. `when cond a` runs `a` if `cond` is `True {}`; `unless cond a` runs `a` if `cond` is `False {}`. The skipped branch becomes `yield {}`. The action's effect row `r` propagates to the result.

```knot
when (n > 0) (println "positive")

unless verbose do
  println "(quiet mode)"
```

### `forEach`

```
forEach : [a] -> (a -> IO r {}) -> IO r {}
```

Sequence an IO action over each row of a relation. Iteration follows the relation's deterministic order (after any `sortBy`).

```knot
forEach ["a", "b", "c"] (\s -> println s)
```

---

## File System

All file system functions return `IO {fs}` values.

### `readFile`

```
readFile : Text -> IO {fs} Text
```

Read an entire file's contents as text.

### `writeFile`

```
writeFile : Text -> Text -> IO {fs} {}
```

Write text to a file (creates or overwrites). First argument is the path, second is the content.

### `appendFile`

```
appendFile : Text -> Text -> IO {fs} {}
```

Append text to a file.

### `fileExists`

```
fileExists : Text -> IO {fs} Bool
```

Check whether a file exists at the given path.

### `removeFile`

```
removeFile : Text -> IO {fs} {}
```

Delete a file.

### `listDir`

```
listDir : Text -> IO {fs} [Text]
```

List directory entries as a relation of filenames.

```knot
main = do
  files <- listDir "."
  yield (filter (\f -> contains ".knot" f) files)
```

---

## Time

### `now`

```
now : IO {clock} Int<Ms>
```

Return the current Unix timestamp in milliseconds. The result is tagged with the built-in `Ms` unit; use `stripUnit` if you need a plain `Int`.

### `sleep`

```
sleep : Int<Ms> -> IO {clock} {}
```

Pause the current thread for the given number of milliseconds. Inside a `race` worker, `sleep` parks on the worker's cancel condvar and wakes immediately if the peer wins.

---

## Random

### `randomInt`

```
randomInt : Int<u> -> IO {random} Int<u>
```

Generate a random integer in the range `[0, bound)`. Unit-polymorphic — the bound's unit is preserved in the result, so `randomInt 100<Usd>` returns `Int<Usd>`.

### `randomFloat`

```
randomFloat : IO {random} Float<u>
```

Generate a random float in the range `[0.0, 1.0)`. Unit-polymorphic — the unit is inferred from context.

### `randomUuid`

```
randomUuid : IO {random} Uuid
```

Generate a fresh UUID. The output is a RFC 9562 UUIDv7 — time-ordered, so values sort chronologically and are well-suited as primary keys.

```knot
main = do
  u <- randomUuid
  println u
  yield {}
```

`Uuid` values are stored as TEXT in SQLite and compare by their canonical string representation.

---

## JSON

### `toJson`

```
toJson : a -> Text
```

Encode any value as a JSON string.

### `parseJson`

```
parseJson : Text -> a
```

Parse a JSON string into a value. Objects become records, arrays become relations, strings become `Text`, numbers become `Int` or `Float`, booleans become `Bool`, and null becomes `{}`.

---

## Bytes

### `textToBytes`

```
textToBytes : Text -> Bytes
```

Encode text as UTF-8 bytes.

### `bytesToText`

```
bytesToText : Bytes -> Maybe Text
```

UTF-8 decode bytes to text. Returns `Nothing {}` on invalid UTF-8.

### `bytesLength`

```
bytesLength : Bytes -> Int<u>
```

Return the byte length.

### `bytesToHex`

```
bytesToHex : Bytes -> Text
```

Encode bytes as a hexadecimal string. Always succeeds.

### `bytesFromHex`

```
bytesFromHex : Text -> Maybe Bytes
```

Decode a hexadecimal string to bytes. Returns `Nothing {}` on odd-length, non-hex, or non-ASCII input. `hexDecode` is an alias.

### `bytesConcat`

```
bytesConcat : Bytes -> Bytes -> Bytes
```

Concatenate two byte strings.

### `bytesGet`

```
bytesGet : Int<u1> -> Bytes -> Int<u2>
```

Get the byte value (0–255) at the given index.

### `bytesSlice`

```
bytesSlice : Int<u1> -> Int<u2> -> Bytes -> Bytes
```

Extract a sub-range. Arguments: start index, length, bytes.

### `hash`

```
hash : a -> Bytes
```

SHA-256 hash of any value, returned as 32 bytes. `Bytes` and `Text` hash their raw contents; structured values (records, relations, constructors) hash a canonical serialisation, so equal logical values always produce equal digests.

```knot
bytesToHex (hash "hello")    -- "2cf24dba..."
```

---

## HTTP

The HTTP types and primitives are defined in the language spec (`DESIGN.md`). The standard library exposes:

### `listen` / `listenOn`

```
listen   : Int<u> -> Server a r -> IO {network | r} {}
listenOn : Text   -> Int<u> -> Server a r -> IO {network | r} {}
```

Start an HTTP server built with `serve API where ...`. `listen` binds to all interfaces; `listenOn` takes an explicit bind address. The `r` row variable unifies with the server's effect row, so handler effects (e.g. `console` from a handler that calls `println`) flow into the program's IO type.

### `fetch` / `fetchWith`

```
fetch     : Text -> Endpoint -> IO {network} (Result HttpError T)
fetchWith : Text -> {headers: [{name: Text, value: Text}]}
                -> Endpoint -> IO {network} (Result HttpError T)
```

Type-safe HTTP client built from route declarations. `Endpoint` is a route constructor; the response type `T` is inferred from the route. `fetchWith` lets you add ad-hoc headers on top of the route's declared ones. When the route declares response headers, the success body wraps as `{body: T, headers: H}` inside `Ok`.

---

## Cryptography

Knot provides elliptic-curve cryptography built-ins using X25519 (encryption) and Ed25519 (signing).

### `generateKeyPair`

```
generateKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
```

Generate an X25519 key pair for encryption/decryption. Inside a `do` block, bind with `keys <- generateKeyPair`.

### `generateSigningKeyPair`

```
generateSigningKeyPair : IO {random} {privateKey: Bytes, publicKey: Bytes}
```

Generate an Ed25519 key pair for signing/verification. Inside a `do` block, bind with `keys <- generateSigningKeyPair`.

### `encrypt`

```
encrypt : Bytes -> Bytes -> IO {random} Bytes
```

Encrypt plaintext bytes with a public key (sealed-box: X25519 ECDH + ChaCha20-Poly1305). First argument is the public key, second is the plaintext. Returns IO because a fresh ephemeral key pair and nonce are generated per call.

### `decrypt`

```
decrypt : Bytes -> Bytes -> Bytes
```

Decrypt ciphertext bytes with a private key. First argument is the private key, second is the ciphertext.

### `sign`

```
sign : Bytes -> Bytes -> Bytes
```

Sign a message with a private key (Ed25519). First argument is the private key, second is the message. Returns a 64-byte signature.

### `verify`

```
verify : Bytes -> Bytes -> Bytes -> Bool
```

Verify a signature. Arguments: public key, message, signature.

---

## Utility Functions

### `id`

```
id : a -> a
```

Identity function — returns its argument unchanged.

### `not`

```
not : Bool -> Bool
```

Boolean negation.

### `stripUnit` / `withUnit` / `stripFloatUnit` / `withFloatUnit`

```
stripUnit      : Int<u> -> Int
withUnit       : Int -> Int<u>
stripFloatUnit : Float<u> -> Float
withFloatUnit  : Float -> Float<u>
```

Drop or attach a unit tag. Identity at runtime — they only adjust the
compile-time type. Use them when you need to rebrand a value with a different
concrete unit (e.g. `Ms` → `S`).

---

## Built-in Traits

### `Eq`

```knot
trait Eq a where
  eq : a -> a -> Bool
```

Equality comparison. Built-in implementations for `Int`, `Float`, `Text`, `Bool`. Used by the `==` and `!=` operators.

### `Ord`

```knot
trait Eq a => Ord a where
  compare : a -> a -> Ordering
```

Ordering comparison. Returns `LT {}`, `EQ {}`, or `GT {}`. Built-in implementations for `Int`, `Float`, `Text`. Used by `<`, `>`, `<=`, `>=` operators.

### `Num`

```knot
trait Eq a => Num a where
  add : a -> a -> a
  sub : a -> a -> a
  mul : a -> a -> a
  div : a -> a -> a
  mod : a -> a -> a
  negate : a -> a
```

Numeric operations. Built-in implementations for `Int`, `Float`. Used by `+`, `-`, `*`, `/`, `%` operators and unary negation. Modulo on `Int` is the remainder (sign follows the dividend); on `Float` it is `fmod`. Modulo by zero panics. The `%` operator pushes down into SQLite as `%` when used inside a SQL-compilable comprehension.

### `Semigroup`

```knot
trait Semigroup a where
  append : a -> a -> a
```

Associative append. Built-in implementations for `Text` and `[]`. Used by the `++` operator.

### `Display`

```knot
trait Display a where
  display : a -> Text
```

Convert a value to a human-readable text representation. Built-in implementations for `Int`, `Float`, `Text`, `Bool`.

### `Sequence`

```knot
trait Sequence s where
  take : Int -> s -> s
  drop : Int -> s -> s
```

`take`/`drop` work on any sequenceable type. Built-in implementations for `Text` (characters) and `[]` (rows).

### `ToJSON` / `FromJSON`

```knot
trait ToJSON a where
  toJson : a -> Text

trait FromJSON a where
  parseJson : Text -> a
```

JSON encode/decode as trait methods. Built-in instances cover records, relations, primitives, ADTs, and `Maybe`/`Result`/`Bool`.

### `Functor`

```knot
trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b
```

Higher-kinded functor. Built-in implementations for `[]`, `Maybe`, `Result`, `IO`.

### `Applicative`

```knot
trait Functor f => Applicative (f : Type -> Type) where
  yield : a -> f a
  ap : f (a -> b) -> f a -> f b
```

Higher-kinded applicative functor. `yield` wraps a value; `ap` applies a wrapped function. Built-in implementations for `[]`, `Maybe`, `Result`, `IO`.

### `Monad`

```knot
trait Applicative m => Monad (m : Type -> Type) where
  bind : (a -> m b) -> m a -> m b
```

Higher-kinded monad. Enables `do` notation with `<-`. Built-in implementations for `[]`, `IO`, `Maybe`, and `Result`.

### `Alternative`

```knot
trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt : f a -> f a -> f a
```

Higher-kinded alternative. `empty` is the identity; `alt` combines alternatives. Built-in implementations for `[]` (where `empty = []` and `alt = union`) and `Maybe` (where `empty = Nothing {}` and `alt` takes the first `Just`).

### `Foldable`

```knot
trait Foldable (t : Type -> Type) where
  fold : (b -> a -> b) -> b -> t a -> b
```

Higher-kinded foldable. Built-in implementation for `[]`.

### `Traversable`

```knot
trait Foldable t => Traversable (t : Type -> Type) where
  traverse : (a -> f b) -> t a -> f (t b)
```

Walk a structure left-to-right and sequence through any `Applicative` `f`. Built-in impl for `[]` over `Maybe` — useful for validating every row and collecting the result or the first `Nothing`.

---

## Built-in Types

| Type | Description |
|------|-------------|
| `Int` | Unbounded integer (arbitrary precision) |
| `Float` | 64-bit floating point |
| `Int<u>` | Integer with compile-time unit (e.g. `Int<Usd>`) |
| `Float<u>` | Float with compile-time unit (e.g. `Float<M>`, `Float<M/S^2>`) |
| `Text` | Unicode string |
| `Bool` | `True {}` or `False {}` |
| `Bytes` | Byte string |
| `Uuid` | RFC 9562 UUIDv7 identifier (TEXT in SQLite) |
| `[a]` | Relation (set of values of type `a`) |
| `IO {effects} a` | IO action with tracked effects |
| `Ordering` | `LT {}`, `EQ {}`, or `GT {}` |
| `Maybe a` | `Nothing {}` or `Just {value: a}` (built-in monad) |
| `Result e a` | `Err {error: e}` or `Ok {value: a}` (built-in monad) |

### Units of Measure

Optional compile-time units on `Int` and `Float`. Fully erased at runtime — no performance cost, no runtime representation. Plain `Float` is unit-agnostic and unifies with any `Float<u>`.

#### Declaration

```knot
unit M
unit S
unit Kg
unit N = Kg * M / S^2    -- derived unit alias
unit Hz = 1 / S
```

#### Literals and Type Annotations

```knot
distance = 42.0<M>            -- Float<M>
speed : Float<M / S>
force : Float<N>
cents : Int<Usd>
```

#### Arithmetic Rules

- `+`/`-` require matching units
- `*`/`/` compose units algebraically
- Unary negation preserves units
- Scalar (dimensionless) multiplication preserves the other operand's unit

```knot
10.0<M> + 5.0<M>              -- Float<M>
10.0<M> + 5.0<S>              -- type error
10.0<M> * 5.0<M>              -- Float<M^2>
100.0<M> / 10.0<S>            -- Float<M/S>
2.0 * 5.0<M>                  -- Float<M>
-(5.0<M>)                     -- Float<M>
```

#### Unit Polymorphism

Concrete units are uppercase; lowercase names inside `<...>` are unit variables:

```knot
double : Float<u> -> Float<u>
double = \x -> x + x
```

#### Unit-Preserving Functions

`sum`, `avg`, `minOn`, and `maxOn` preserve units from their projection function:

```knot
avg   (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
sum   (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
minOn (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
maxOn (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
```

---

## Operators

| Operator | Trait | Method |
|----------|-------|--------|
| `+` | `Num` | `add` |
| `-` | `Num` | `sub` |
| `*` | `Num` | `mul` |
| `/` | `Num` | `div` |
| `%` | `Num` | `mod` |
| unary `-` | `Num` | `negate` |
| `==` | `Eq` | `eq` |
| `!=` | `Eq` | `eq` (negated) |
| `<` `>` `<=` `>=` | `Ord` | `compare` |
| `++` | `Semigroup` | `append` |
| `&&` | — | Boolean AND (direct) |
| `\|\|` | — | Boolean OR (direct) |
| `\|>` | — | Pipe-forward (`x \|> f` = `f x`) |
