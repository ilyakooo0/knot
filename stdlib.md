# Knot Standard Library

Complete reference for all built-in functions, traits, and types.

## Table of Contents

- [Relation Operations](#relation-operations)
- [Concurrency](#concurrency)
- [Text Operations](#text-operations)
- [Console I/O](#console-io)
- [File System](#file-system)
- [Time](#time)
- [Random](#random)
- [JSON](#json)
- [Bytes](#bytes)
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

Extract the single element of a relation. Returns `Just {value: x}` for a singleton, `Nothing {}` otherwise.

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

### `min`

```
min : (a -> b) -> [a] -> b
```

Minimum of a projected field over a relation. The projection can return any orderable type — `Int`, `Float`, or `Text` (lexicographic ordering). Panics if the relation is empty.

```knot
lowestSalary = do
  employees <- *employees
  yield (min (\e -> e.salary) employees)

firstName = do
  employees <- *employees
  yield (min (\e -> e.name) employees)
```

When applied to a source (or bound source variable), it pushes down to `SELECT MIN(col) FROM ...`. Combined with `filter` it becomes `SELECT MIN(col) FROM ... WHERE ...`.

### `max`

```
max : (a -> b) -> [a] -> b
```

Maximum of a projected field over a relation. Like `min`, works with any orderable type. Panics if the relation is empty. Pushes down to `SELECT MAX(col) FROM ...`.

```knot
highestSalary = do
  employees <- *employees
  yield (max (\e -> e.salary) employees)
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

---

## Concurrency

### `fork`

```
fork : IO {} {} -> IO {} {}
```

Run an IO action on a new OS thread (fire-and-forget). Each thread gets its own SQLite connection via WAL mode for safe concurrent access. The main thread waits for all spawned threads before exiting.

```knot
main = do
  fork do
    println "hello from thread 1"
  fork do
    println "hello from thread 2"
  println "hello from main"
```

Do blocks can be passed directly as arguments without parentheses.

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

### `take`

```
take : Int -> Text -> Text
```

Return the first *n* characters.

```knot
first3 = take 3 "hello"   -- "hel"
```

### `drop`

```
drop : Int -> Text -> Text
```

Drop the first *n* characters.

```knot
rest = drop 3 "hello"   -- "lo"
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
now : IO {clock} Int
```

Return the current Unix timestamp in milliseconds.

---

## Random

### `randomInt`

```
randomInt : Int -> IO {random} Int
```

Generate a random integer in the range [0, *bound*).

### `randomFloat`

```
randomFloat : IO {random} Float
```

Generate a random float in the range [0.0, 1.0).

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
bytesToText : Bytes -> Text
```

Decode UTF-8 bytes to text.

### `bytesLength`

```
bytesLength : Bytes -> Int
```

Return the byte length.

### `bytesToHex`

```
bytesToHex : Bytes -> Text
```

Encode bytes as a hexadecimal string.

### `bytesFromHex`

```
bytesFromHex : Text -> Bytes
```

Decode a hexadecimal string to bytes.

### `bytesConcat`

```
bytesConcat : Bytes -> Bytes -> Bytes
```

Concatenate two byte strings.

### `bytesGet`

```
bytesGet : Int -> Bytes -> Int
```

Get the byte value (0–255) at the given index.

### `bytesSlice`

```
bytesSlice : Int -> Int -> Bytes -> Bytes
```

Extract a sub-range. Arguments: start index, length, bytes.

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
  negate : a -> a
```

Numeric operations. Built-in implementations for `Int`, `Float`. Used by `+`, `-`, `*`, `/` operators and unary negation.

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

### `Functor`

```knot
trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b
```

Higher-kinded functor. Built-in implementation for `[]`.

### `Applicative`

```knot
trait Functor f => Applicative (f : Type -> Type) where
  yield : a -> f a
  ap : f (a -> b) -> f a -> f b
```

Higher-kinded applicative functor. `yield` wraps a value; `ap` applies a wrapped function. Built-in implementation for `[]`.

### `Monad`

```knot
trait Applicative m => Monad (m : Type -> Type) where
  bind : (a -> m b) -> m a -> m b
```

Higher-kinded monad. Enables `do` notation with `<-`. Built-in implementation for `[]` and `IO`.

### `Alternative`

```knot
trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt : f a -> f a -> f a
```

Higher-kinded alternative. `empty` is the identity; `alt` combines alternatives. Built-in implementation for `[]` (where `empty = []` and `alt = union`).

### `Foldable`

```knot
trait Foldable (t : Type -> Type) where
  fold : (b -> a -> b) -> b -> t a -> b
```

Higher-kinded foldable. Built-in implementation for `[]`.

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
| `[a]` | Relation (set of values of type `a`) |
| `IO {effects} a` | IO action with tracked effects |
| `Ordering` | `LT {}`, `EQ {}`, or `GT {}` |

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

`sum`, `avg`, `min`, and `max` preserve units from their projection function:

```knot
avg (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
sum (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
min (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
max (\t -> t.distance) *trips   -- Float<M> if distance : Float<M>
```

---

## Operators

| Operator | Trait | Method |
|----------|-------|--------|
| `+` | `Num` | `add` |
| `-` | `Num` | `sub` |
| `*` | `Num` | `mul` |
| `/` | `Num` | `div` |
| unary `-` | `Num` | `negate` |
| `==` | `Eq` | `eq` |
| `!=` | `Eq` | `eq` (negated) |
| `<` `>` `<=` `>=` | `Ord` | `compare` |
| `++` | `Semigroup` | `append` |
| `&&` | — | Boolean AND (direct) |
| `\|\|` | — | Boolean OR (direct) |
| `\|>` | — | Pipe-forward (`x \|> f` = `f x`) |
