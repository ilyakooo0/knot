# The `base` Namespace

Every standard-library function in Knot is reached through the **`base`**
record. The compiler injects a single top-level binding — `base` — into every
program; nothing else enters scope by default. You call `base.println`,
`base.map`, `base.listen`, and so on.

> **The bare forms are gated.** You cannot write `println x` — the compiler
> rejects it and asks for `base.println x`. This keeps the global namespace
> clean: any name you define is yours, and only `base.<name>` reaches the
> standard library. (The one exception is constructors and keywords — `case`,
> `do`, `yield`, `Just`, `Nothing`, `Ok`, `Err`, `True`, `False` — which are
> language, not library.)

This file is the complete reference for every field of `base`, grouped by
purpose, with the full path, type, and a description. Types are written in
Knot's own notation:

- `a`, `b`, `u`, … are type / unit variables (universally quantified).
- `[a]` is a relation (a set) of `a`.
- `IO a` is an IO action producing an `a`.
- `Maybe a` is `Nothing {}` or `Just {value: a}`; `Result e a` is
  `Err {error: e}` or `Ok {value: a}`.

The user-facing trait system is gone; the comparison/arithmetic operators
(`<`, `+`, `==`, `++`, …) are intrinsic, and the few functions below that once
carried a trait bound are now plain polymorphic functions. Where a function is
restricted in practice (e.g. `minOn` needs an orderable projection), that is
noted in prose rather than as a type constraint.

---

## Relation operations

Relations are the core data structure. Comprehensions over a **source**
relation (`*name`) push down to SQL — filters become `WHERE`, aggregates
become `SELECT COUNT/SUM/MIN/MAX`, `sortBy` becomes `ORDER BY`, and a
read-only comprehension binding two or more sources with an equi-join
predicate (`where e.dept == d.name`) becomes a single multi-table `SELECT`
with both join columns auto-indexed. The same functions over an in-memory
relation run natively. See [stdlib.md — Query Pushdown &
Auto-Indexing](stdlib.md#query-pushdown--auto-indexing) for the full picture.

### `base.filter`

```
base.filter : (a -> Bool) -> [a] -> [a]
```

Keep the rows where the predicate returns `True`. Pushes down to `WHERE` over
a source.

```knot
&seniors = *people |> base.filter (\p -> p.age > 65)
```

### `base.map`

```
base.map : (a -> b) -> [a] -> [b]
```

Apply a function to each row. Results are deduplicated (relations are sets).

### `base.fold`

```
base.fold : (b -> a -> b) -> b -> [a] -> b
```

Left fold over a relation: combine every row into an accumulator.

```knot
totalAmount \rel -> base.fold (\acc r -> acc + r.amount) 0 rel
```

### `base.count`

```
base.count : [a] -> Int u
```

Number of rows. Over a source this is a single `SELECT COUNT(*)`; a
`base.filter` / `base.count` pipe collapses into one
`SELECT COUNT(*) ... WHERE ...`.

### `base.countWhere`

```
base.countWhere : (a -> Bool) -> [a] -> Int u
```

Count rows satisfying a predicate — `base.count . base.filter` — but pushes
down to one `SELECT COUNT(*) ... WHERE pred` when the predicate compiles to
SQL.

### `base.sum`

```
base.sum : [a] -> a
```

Sum a numeric relation. Project a record field first with `base.map`. Units
are preserved: summing `[Float M]` gives `Float M`.

```knot
totalDist (base.sum (base.map (\t -> t.distance) *trips))
```

### `base.avg`

```
base.avg : (a -> Float u) -> [a] -> Float u
```

Average of a projected numeric field. Units are preserved from the
projection.

### `base.minOn` / `base.maxOn`

```
base.minOn : (a -> b) -> [a] -> b
base.maxOn : (a -> b) -> [a] -> b
```

Minimum / maximum of a projected field. The projection must be orderable
(`Int`, `Float`, or `Text`). **Panics on an empty relation.** Pushes down to
`SELECT MIN(col)` / `SELECT MAX(col)`.

### `base.min` / `base.max`

```
base.min : a -> a -> a
base.max : a -> a -> a
```

Binary minimum / maximum of **two values** (not a relation). Use
`base.minOn` / `base.maxOn` to aggregate over a relation.

```knot
base.min 3 7      -- 3
base.max "a" "b"  -- "b"
```

### `base.union`

```
base.union : [a] -> [a] -> [a]
```

Set union of two relations.

### `base.diff`

```
base.diff : [a] -> [a] -> [a]
```

Set difference — rows in the first relation but not the second.

### `base.inter`

```
base.inter : [a] -> [a] -> [a]
```

Set intersection — rows present in both.

### `base.bind`

```
base.bind : (a -> Maybe b) -> Maybe a -> Maybe b
```

Monadic bind. Over relations it is `flatMap`
(`(a -> [b]) -> [a] -> [b]`, compiled to `knot_relation_bind`); over `Maybe`
it threads a `Just` or short-circuits a `Nothing`. This is the function `<-`
desugars to in `do` notation.

### `base.head`

```
base.head : [a] -> Maybe a
```

First row in iteration order, or `Nothing {}` if empty.

### `base.single`

```
base.single : [a] -> Maybe a
```

`Just {value: x}` for a singleton relation, `Nothing {}` for empty or
multi-element relations.

### `base.findFirst`

```
base.findFirst : [a] -> (a -> Bool) -> Maybe a
```

First row matching the predicate (left-to-right), stopping at the first hit,
or `Nothing {}`.

### `base.any` / `base.all`

```
base.any : (a -> Bool) -> [a] -> Bool
base.all : (a -> Bool) -> [a] -> Bool
```

`any` is `True` when some row matches; `all` is `True` only when every row
matches (vacuously `True` on `[]`).

### `base.elem`

```
base.elem : a -> [a] -> Bool
```

Membership test by structural equality.

### `base.sortBy`

```
base.sortBy : (a -> b) -> [a] -> [a]
```

Reorder rows by a projected key (ascending). Pushes down to `ORDER BY`; with
`base.take` it becomes `ORDER BY ... LIMIT`.

```knot
&top5 = *people |> base.sortBy (\p -> (0 - p.age)) |> base.take 5
```

### `base.take` / `base.drop`

```
base.take : Int -> s -> s
base.drop : Int -> s -> s
```

First / skip *n* items. Polymorphic over the sequence type `s` — works on
relations (rows) and on `Text` (characters).

### `base.match`

```
base.match : (a -> b) -> [b] -> [a]
```

Filter a relation of ADT values to one constructor, extracting the payload.
`base.match Shape.Circle *shapes` yields the `Circle` payloads.

### `base.upsertBy`

```
base.upsertBy : (a -> Bool) -> a -> [a] -> [a]
```

Replace every element matching the predicate with the given value, or append
it if none match — the "insert or update" pattern.

### `base.traverse`

```
base.traverse : (a -> f b) -> [a] -> f [b]
```

Walk a relation left-to-right, sequencing through an effectful or short-circuiting step
(typically `Maybe` — validate every row and collect, or get the first
`Nothing`).

---

## Console I/O

### `base.println`

```
base.println : a -> IO {}
```

Print any value to stdout followed by a newline (uses `base.show`
formatting). `base.putLine` is an alias.

### `base.putLine`

```
base.putLine : a -> IO {}
```

Alias for `base.println`.

### `base.print`

```
base.print : a -> IO {}
```

Print without a trailing newline.

### `base.readLine`

```
base.readLine : IO Text
```

Read a line from stdin.

### `base.logInfo` / `base.logWarn` / `base.logError` / `base.logDebug`

```
base.logInfo  : a -> IO {}
base.logWarn  : a -> IO {}
base.logError : a -> IO {}
base.logDebug : a -> IO {}
```

Leveled logging to **stderr** (kept separate from `println`'s stdout). Colored
on a TTY, one JSON line per record otherwise. `logDebug` is silent unless the
program is run with `--debug`.

### `base.show`

```
base.show : a -> Text
```

Convert any value to its text representation. Pure (no IO).

---

## Control flow

### `base.when` / `base.unless`

```
base.when   : Bool -> IO {} -> IO {}
base.unless : Bool -> IO {} -> IO {}
```

Run an IO action conditionally — `base.when` when the condition is `True`,
`base.unless` when `False`. The skipped branch yields `{}`.

```knot
base.when (n > 0) (base.println "positive")
```

### `base.forEach`

```
base.forEach : [a] -> (a -> IO {}) -> IO {}
```

Sequence an IO action over each row (in iteration order).

### `base.not`

```
base.not : Bool -> Bool
```

Boolean negation.

### `base.id`

```
base.id : a -> a
```

Identity function.

---

## File system

All return `IO ` values.

### `base.readFile`

```
base.readFile : Text -> IO Text
```

Read an entire file as text.

### `base.writeFile`

```
base.writeFile : Text -> Text -> IO {}
```

Write text to a file (path, then contents); creates or overwrites.

### `base.appendFile`

```
base.appendFile : Text -> Text -> IO {}
```

Append text to a file.

### `base.fileExists`

```
base.fileExists : Text -> IO Bool
```

Whether a file exists at the path.

### `base.removeFile`

```
base.removeFile : Text -> IO {}
```

Delete a file.

### `base.listDir`

```
base.listDir : Text -> IO [Text]
```

List directory entries as a relation of filenames.

---

## Time

### `base.now`

```
base.now : IO Int Ms
```

Current Unix timestamp in **milliseconds** (`Int Ms`). `base.now` is a
0-argument IO thunk — forcing it twice runs it twice. Use `base.stripUnit`
for a plain `Int 1`.

### `base.sleep`

```
base.sleep : Int Ms -> IO {}
```

Pause the current thread for the given milliseconds. Inside `base.race` a
sleeping loser wakes immediately when the peer wins.

---

## Random

### `base.randomInt`

```
base.randomInt : Int u -> IO Int u
```

Random integer in `[0, bound)`. Unit-polymorphic — `base.randomInt 100 Usd`
returns `Int Usd`.

### `base.randomFloat`

```
base.randomFloat : IO Float u
```

Random float in `[0.0, 1.0)`. The unit is inferred from context.

### `base.randomUuid`

```
base.randomUuid : IO Uuid
```

A fresh RFC 9562 **UUIDv7** — time-ordered, so values sort chronologically
(good primary keys). Stored as TEXT in SQLite.

---

## JSON

### `base.toJson`

```
base.toJson : a -> Text
```

Encode any value as a JSON string.

### `base.parseJson`

```
base.parseJson : Text -> Maybe a
```

Parse JSON into a value — `Just decoded` on success, `Nothing {}` on malformed
input. Objects → records, arrays → relations, strings → `Text`, numbers →
`Int 1` / `Float 1`, booleans → `Bool`, null → `Nothing {}`. Decoding is
type-directed where a target type can be inferred.

---

## Text

### `base.toUpper` / `base.toLower`

```
base.toUpper : Text -> Text
base.toLower : Text -> Text
```

Uppercase / lowercase.

### `base.length`

```
base.length : Text -> Int u
```

Number of characters (Unicode-aware).

### `base.trim`

```
base.trim : Text -> Text
```

Strip leading and trailing whitespace.

### `base.reverse`

```
base.reverse : Text -> Text
```

Reverse text.

### `base.chars`

```
base.chars : Text -> [Text]
```

Split into a relation of single characters.

### `base.contains`

```
base.contains : Text -> Text -> Bool
```

`base.contains needle haystack` — whether `haystack` contains `needle`.

---

## Bytes

### `base.textToBytes`

```
base.textToBytes : Text -> Bytes
```

Encode text as UTF-8 bytes.

### `base.bytesToText`

```
base.bytesToText : Bytes -> Maybe Text
```

UTF-8 decode; `Nothing {}` on invalid UTF-8.

### `base.bytesLength`

```
base.bytesLength : Bytes -> Int u
```

Byte length.

### `base.bytesToHex`

```
base.bytesToHex : Bytes -> Text
```

Hex-encode (always succeeds).

### `base.bytesFromHex` / `base.hexDecode`

```
base.bytesFromHex : Text -> Maybe Bytes
base.hexDecode    : Text -> Maybe Bytes
```

Hex-decode; `Nothing {}` on odd-length, non-hex, or non-ASCII input.
`hexDecode` is an alias.

### `base.bytesConcat`

```
base.bytesConcat : Bytes -> Bytes -> Bytes
```

Concatenate two byte strings.

### `base.bytesGet`

```
base.bytesGet : Int u1 -> Bytes -> Maybe Int u2
```

Byte value (0–255) at an index, or `Nothing {}` when out of bounds (safe for
attacker-supplied indices).

### `base.bytesSlice`

```
base.bytesSlice : Int u1 -> Int u2 -> Bytes -> Bytes
```

Sub-range: start index, length, bytes.

### `base.hash`

```
base.hash : a -> Bytes
```

BLAKE3 hash of any value, as 32 bytes. `Bytes`/`Text` hash their raw contents;
structured values hash a canonical serialisation, so equal values give equal
digests.

---

## Numeric conversion

### `base.floor`

```
base.floor : Float u -> Int 1
```

Round toward negative infinity (`base.floor (-2.3)` is `-3`). The result is
dimensionless; attach a unit with `base.withUnit` if needed.

### `base.intToFloat`

```
base.intToFloat : Int u -> Float 1
```

Widen an `Int` to a `Float` (lossy past 2⁵³). The result is dimensionless.

### `base.textToInt`

```
base.textToInt : Text -> Maybe (Int 1)
```

Parse an integer; `Nothing {}` on malformed input.

### `base.textToFloat`

```
base.textToFloat : Text -> Maybe (Float 1)
```

Parse a float; `Nothing {}` on malformed input.

---

## Morphs (`base.morph`)

`base.morph` is a nested record of type-directed conversions, consumed by the
`^into` implicit-field projection. Each `<from>To<to>` field holds an
`into : S -> T` function:

```
base.morph.textToBytes.into       : Text -> Bytes
base.morph.bytesToText.into       : Bytes -> Maybe Text
base.morph.bytesToHex.into        : Bytes -> Text
base.morph.textToBytesFromHex.into : Text -> Maybe Bytes
base.morph.intToFloat.into        : Int 1 -> Float 1
base.morph.textToInt.into         : Text -> Maybe (Int 1)
base.morph.textToFloat.into       : Text -> Maybe (Float 1)
base.morph.intToText.into         : Int 1 -> Text
base.morph.floatToText.into       : Float 1 -> Text
base.morph.boolToText.into        : Bool -> Text
```

You rarely project these directly. Instead write `(^into) x` where the target
type is known from context, and the compiler picks the matching morph:

```knot
asInt : Text -> Maybe (Int 1)
asInt (\s -> (^into) s)     -- resolves base.morph.textToInt.into

asText : Int 1 -> Text
asText (\n -> (^into) n)    -- resolves base.morph.intToText.into
```

The conversion is chosen by **both** the argument type and the expected result
type. `(^into) "42"` with no result annotation is ambiguous (several morphs
accept `Text`) and is a compile error — pin the result type. See
[DESIGN.md](DESIGN.md#implicit-dictionaries-field--t-) for the `^field`
mechanism.

---

## Cryptography

X25519 (encryption) and Ed25519 (signing).

### `base.generateKeyPair`

```
base.generateKeyPair : IO {privateKey: Bytes, publicKey: Bytes}
```

X25519 key pair for encryption/decryption.

### `base.generateSigningKeyPair`

```
base.generateSigningKeyPair : IO {privateKey: Bytes, publicKey: Bytes}
```

Ed25519 key pair for signing/verification.

### `base.encrypt`

```
base.encrypt : Bytes -> Bytes -> IO (Maybe Bytes)
```

Encrypt plaintext with a public key (sealed box: X25519 ECDH +
ChaCha20-Poly1305). Args: public key, plaintext. IO because a fresh ephemeral
key/nonce is generated per call.

### `base.decrypt`

```
base.decrypt : Bytes -> Bytes -> Maybe Bytes
```

Decrypt with a private key. Args: private key, ciphertext. `Nothing {}` on
failure.

### `base.sign`

```
base.sign : Bytes -> Bytes -> Maybe Bytes
```

Sign a message (Ed25519). Args: private key, message. 64-byte signature, or
`Nothing {}` on failure.

### `base.verify`

```
base.verify : Bytes -> Bytes -> Bytes -> Bool
```

Verify a signature. Args: public key, message, signature.

---

## Concurrency

### `base.fork`

```
base.fork : IO a -> IO {}
```

Run an IO action on a new OS thread (fire-and-forget; the result is
discarded). Each thread gets its own SQLite connection (WAL). The main thread
joins all spawned threads before exit.

### `base.race`

```
base.race : IO a -> IO b -> IO (Result a b)
```

Run two IO actions concurrently, return the winner:
`Err {error: a}` if the left wins, `Ok {value: b}` if the right wins. The
loser is cancelled cooperatively (a sleeping loser wakes immediately). Cannot
be used inside `atomic`.

### `atomic` and `retry` (language keywords, not `base` fields)

`atomic` is a **keyword**, not a `base` field — you write `atomic do ...`,
never `base.atomic`. It runs an IO body as a database transaction; the block is
an `IO a` where `a` is the body's `yield` type. With `base.retry` it implements
STM.

`base.retry` (below) is the one STM primitive that *is* gated to the `base.`
prefix, even though it's a compile-time macro rather than a record field.

### `base.retry`

```
base.retry : a
```

STM retry, **only inside `atomic`**: roll back, wait for a relation change,
re-run. Row-level filtering means a watcher on `WHERE id = 1` is not woken by
writes to `id = 2`. (A compile-time macro, not a record field, but gated to
`base.retry` like the rest.)

---

## HTTP

The HTTP types (`route`, `serve`, `Endpoint`, `Server`) are part of the
language spec ([DESIGN.md](DESIGN.md)); these are the entry points. They are
compile-time special forms gated to the `base.` prefix rather than ordinary
record fields.

### `base.listen` / `base.listenOn`

```
base.listen   : Int u -> Server a -> IO {}
base.listenOn : Text -> Int u -> Server a -> IO {}
```

Start an HTTP server built with `serve Api where ...`. `base.listen` binds all
interfaces; `base.listenOn` takes an explicit bind address. Handler effects
flow into the program's IO type.

```knot
(base.listen 8080 api)
```

### `base.fetch` / `base.fetchWith`

```
base.fetch     : Text -> a -> IO (Result {status: Int 1, message: Text} b)
base.fetchWith : Text -> c -> a -> IO (Result {status: Int 1, message: Text} b)
```

Type-safe HTTP client from a route declaration. The first argument is the base
URL, the last is the route's `Endpoint` constructor; the response type `b` is
inferred from the route. `base.fetchWith` adds ad-hoc headers (the middle
argument). Response-header routes wrap the success body as
`{body: b, headers: H}` inside `Ok`.

---

## Debugging

Two gated special forms for development-time debugging. Both print a report
with the relevant type, source location (file:line:col + caret), and every
in-scope binding with its type and runtime value — and both emit a
**compile-time warning** so they aren't shipped accidentally.

### `base.todo`

```
base.todo : a
```

An unimplemented hole. Type-checks as **any** type, so you can stub a
definition and fill it in later; the program compiles, but aborts (exit 1)
with the context report when evaluation reaches it.

### `base.trace`

```
base.trace : a -> a
```

A transparent probe: prints the report plus the traced value, then returns it
unchanged. Drop it into an expression without changing its meaning.

---

## Unit conversion

Identity at runtime — these only adjust the compile-time unit tag. See
[stdlib.md](stdlib.md#units-of-measure) for the units system.

### `base.stripUnit` / `base.withUnit`

```
base.stripUnit : Int u -> Int 1
base.withUnit  : Int 1 -> Int u
```

Drop / attach a unit on an `Int`.

### `base.stripFloatUnit` / `base.withFloatUnit`

```
base.stripFloatUnit : Float u -> Float 1
base.withFloatUnit  : Float 1 -> Float u
```

Drop / attach a unit on a `Float`.

### `base.strip` / `base.dress`

```
base.strip : a u -> a 1
base.dress : a 1 -> a u
```

Generalized unit rebranding across both `Int` and `Float`. `base.strip` drops
a unit; `base.dress` attaches one to a dimensionless value (target pinned by
context/annotation).

```knot
toS : Int Ms -> Int S
toS \ms -> base.dress (base.strip ms / 1000)
```

---

## The `base` record itself

`base` is an ordinary record value. The prelude assembles it from the nested
helpers (`min`/`max`/`when`/`unless`, defined as lambdas) plus every stdlib
builtin as a field, and binds it globally. Because it's a record you can pass
it, project from it, or — in a `with` block — shadow individual fields for
testing. Your own top-level bindings never collide with it: only the
`base.<name>` path reaches the standard library.

The data type `base.Ordering` (`LT {}` / `EQ {}` / `GT {}`) is also reachable
through the record for ordering-returning operations.

## See also

- [DESIGN.md](DESIGN.md) — language specification (routes/HTTP, refined
  types, units, concurrency, schema migration).
- [stdlib.md](stdlib.md) — operator table, built-in types, and units of
  measure reference.
- [README.md](README.md) — guided tour and feature overview.