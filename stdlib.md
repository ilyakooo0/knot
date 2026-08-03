# Knot Standard Library

Complete reference for all built-in functions, traits, and types.

## Table of Contents

- [Query Pushdown & Auto-Indexing](#query-pushdown--auto-indexing)
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
- [Numeric Conversion](#numeric-conversion)
- [Record Operations](#record-operations)
- [Morphs (`base.morph`)](#morphs-basemorph)
- [HTTP](#http)
- [Cryptography](#cryptography)
- [Utility Functions](#utility-functions)
- [Operator Behavior (Intrinsic)](#operator-behavior-intrinsic)
- [Built-in Types](#built-in-types)
- [Operators](#operators)

---

## Query Pushdown & Auto-Indexing

Many relation operations over a **source relation** (`*name : [T]`) don't load
rows into memory and process them in Knot — the compiler pushes them down to a
single SQL query against the underlying SQLite table, and the runtime
auto-indexes the columns involved.

**What pushes down:**

- **Filters** — `where` clauses and `filter (\r -> r.f OP x) rows` become
  `WHERE`.
- **Joins** — a read-only comprehension binding two or more sources and
  relating them with an equi-join predicate plus single-table predicates
  compiles to one multi-table `SELECT`:
  ```knot
  with {result (do
    e <- *employees
    d <- *departments
    where e.dept == d.name
    where e.salary > 75
    yield {name e.name dept d.name})}
  yield result
  ```
  →
  ```sql
  SELECT t0."name", t1."name"
  FROM "_knot_employees" AS t0, "_knot_departments" AS t1
  WHERE (t0."dept" = t1."name") AND (t0."salary" > ?)
  ```
- **Aggregates** — `count`, `countWhere`, `sum`, `avg`, `minOn`, `maxOn`
  become `SELECT COUNT(*)`/`SUM(...)`/`MIN(...)`/`MAX(...) ...`.
- **`sortBy`** — becomes `ORDER BY`.
- **Pure helper functions** used in a predicate are inlined, so
  `where (salaryOf e) > 75` still becomes `WHERE salary > 75`.

**Auto-indexing.** Columns referenced in a pushed-down `WHERE` or `ORDER BY`
clause — including both join columns of a multi-table join (`employees.dept`
*and* `departments.name`) — get a `CREATE INDEX IF NOT EXISTS` on first use.
ADT tables also index the `_tag` discriminator at creation. There is no
`CREATE INDEX` syntax and nothing to declare; the runtime observes the queries
and indexes accordingly.

**Fallback.** Any comprehension or operation the planner cannot translate
falls back to reading the relation(s) and computing in memory (hash-join /
nested-loop for joins). The *results are identical* — only the execution
strategy differs — so you can write the natural comprehension and not worry
about whether it pushed down.

---

## Relation Operations

### `filter`

```
filter : (a -> Bool) -> [a] -> [a]
```

Keep rows where the predicate returns `True`.

```knot
*people : [{name: Text, age: Int 1}]
&seniors = (do
  people <- full *people
  yield (base.filter (\p -> p.age > 65) people))
```

### `map`

```
map : (a -> b) -> [a] -> [b]
```

Apply a function to each row. Results are deduplicated (relations are sets).

```knot
*people : [{name: Text, age: Int 1}]
&names = (do
  people <- full *people
  yield (base.map (\p -> {name p.name}) people))
```

`map` is the `Functor` trait method for `[]`.

### `match`

```
match : (a -> b) -> [b] -> [a]
```

Filter a relation to rows matching a constructor tag, extracting the payload.

```knot
data Shape = Circle {radius: Float 1} | Rect {width: Float 1, height: Float 1}

*shapes : [Shape]

&circles = (do
  shapes <- full *shapes
  yield (base.match Shape.Circle shapes))    -- : [{radius: Float 1}]
&rects = (do
  shapes <- full *shapes
  yield (base.match Shape.Rect shapes))      -- : [{width: Float 1, height: Float 1}]
```

### `fold`

```
fold : (b -> a -> b) -> b -> [a] -> b
```

Left fold over a relation. `fold` is the `Foldable` trait method for `[]`.

```knot
totalAmount \rel -> base.fold (\acc r -> acc + r.amount) 0 rel
```

### `single`

```
single : [a] -> Maybe a
```

Extract the single element of a relation. Returns `Just {value: x}` for a singleton, `Nothing {}` for empty or multi-element relations.

```knot
base.single [{name "Alice"}]    -- Just {value {name "Alice"}}
base.single []                   -- Nothing {}
base.single [1 2]               -- Nothing {}
```

### `count`

```
count : [a] -> Int u
```

Return the number of rows in a relation.

```knot
*people : [{name: Text, age: Int 1}]
&numPeople = (do
  people <- full *people
  yield (base.count people))
```

When the argument is a source relation (or its bound alias), the compiler emits a single `SELECT COUNT(*)` query. Pipe forms like `*people |> filter (\p -> p.age > 30) |> count` collapse into one `SELECT COUNT(*) FROM ... WHERE ...`.

### `countWhere`

```
countWhere : (a -> Bool) -> [a] -> Int u
```

Count rows that satisfy a predicate. Equivalent to `count . filter`, but pushes down to a single `SELECT COUNT(*) FROM ... WHERE pred` when the predicate is SQL-compilable.

```knot
*employees : [{name: Text, dept: Text, salary: Int 1}]
engHeadcount do
  employees <- full *employees
  yield (base.countWhere (\e -> e.dept == "Eng") employees)
```

### `sum`

```
sum : [a] -> a
```

Sum of a numeric relation. Takes the relation directly — there is no projection argument. To sum a field of a record relation, project first with `map`. Works with `Int 1`, `Float 1`, and unit-annotated types — units are preserved.

```knot
total (base.sum [10 20 30]) -- 60

-- Sum a record field by projecting first (source read is IO, so bind first):
*people : [{name: Text, age: Int 1}]
&totalAge = (do
  people <- full *people
  yield (base.sum (base.map (\p -> p.age) people)))

-- Unit-preserving:
*trips : [{distance: Float M}]
&totalDistance = (do
  trips <- full *trips
  yield (base.sum (base.map (\t -> t.distance) trips))) -- Float M if distance : Float M
```

### `avg`

```
avg : (a -> Float u) -> [a] -> Float u
```

Average of a projected numeric field over a relation. Returns `Float 1`. Preserves units from the projection function — if the projection returns `Float M`, the average is `Float M`.

### `minOn`

```
minOn : (a -> b) -> [a] -> b
```

Minimum of a projected field over a relation. The projection can return any orderable type — `Int 1`, `Float 1`, or `Text` (lexicographic ordering). Panics if the relation is empty.

```knot
*employees : [{name: Text, salary: Int 1}]
lowestSalary do
  employees <- full *employees
  yield (base.minOn (\e -> e.salary) employees)

firstName do
  employees <- full *employees
  yield (base.minOn (\e -> e.name) employees)
```

When applied to a source (or bound source variable), it pushes down to `SELECT MIN(col) FROM ...`. Combined with `filter` it becomes `SELECT MIN(col) FROM ... WHERE ...`.

### `maxOn`

```
maxOn : (a -> b) -> [a] -> b
```

Maximum of a projected field over a relation. Like `minOn`, works with any orderable type. Panics if the relation is empty. Pushes down to `SELECT MAX(col) FROM ...`.

```knot
*employees : [{name: Text, salary: Int 1}]
highestSalary do
  employees <- full *employees
  yield (base.maxOn (\e -> e.salary) employees)
```

### `min` / `max`

```
min : a -> a -> a
max : a -> a -> a
```

Binary minimum and maximum of two values. Use `minOn`/`maxOn` to aggregate
over a relation; `min`/`max` operate on two single values.

```knot
base.min 3 7         -- 3
base.max "a" "b"     -- "b"
```

### `union`

```
union : [a] -> [a] -> [a]
```

Set union of two relations.

```knot
*employees : [{name: Text}]
*contractors : [{name: Text}]
&everyone = (do
  employees <- full *employees
  contractors <- full *contractors
  yield (base.union employees contractors))
```

### `diff`

```
diff : [a] -> [a] -> [a]
```

Set difference — rows in the first relation but not the second.

```knot
*employees : [{name: Text}]
*managers : [{name: Text}]
&nonManagers = (do
  employees <- full *employees
  managers <- full *managers
  yield (base.diff employees managers))
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
base.findFirst [1 2 3 4 5] (\x -> x > 3)   -- Just {value 4}
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
*employees : [{name: Text, salary: Int 1}]
&topFive = do
  employees <- full *employees
  yield (employees |> base.sortBy (\e -> -e.salary) |> base.take 5)
-- SQL: SELECT ... FROM _knot_employees ORDER BY -salary LIMIT 5
```

### `take` / `drop`

```
take : Int 1 -> [a] -> [a]      -- Sequence.take
drop : Int 1 -> [a] -> [a]      -- Sequence.drop
```

First / drop *n* rows. `take`/`drop` are built-in polymorphic functions that work on both `[a]` (rows) and `Text` (characters).

### `upsertBy`

```
upsertBy : (a -> Bool) -> a -> [a] -> [a]
```

Replace every element matching the predicate with the supplied value. If no
element matches, append the value. Useful for "insert or update" patterns on
source relations.

```knot
-- Bump or insert a per-user counter
bump \user counters -> base.upsertBy (\c -> c.user == user) {user user n 1} counters
```

---

## Concurrency

### `fork`

```
fork : IO a -> IO {}
```

Run an IO action on a new OS thread (fire-and-forget). The spawned action can
return any value `a` (it is discarded). Each thread gets its own SQLite
connection via WAL mode for safe concurrent access. The main thread waits for
all spawned threads before exiting.

```knot
do
  base.fork do
    base.println "hello from thread 1"
  base.fork do
    base.println "hello from thread 2"
  base.println "hello from main"
```

Do blocks can be passed directly as arguments without parentheses.

### `race`

```
race : IO a -> IO b -> IO (Result a b)
```

Run two IO actions concurrently and return the winner.

The winner is reported via the built-in `Result a b` ADT — `Err {error: a}` when the left action wins, `Ok {value: b}` when the right action wins.

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

Cancellation is cooperative but aggressive: the loser's `knot_io_run` checks its cancel token between every IO thunk, and `sleep` parks on a condvar that's signalled on cancel — so a loser stuck in a long sleep wakes immediately when the peer wins. The parent does not wait for the loser; it returns as soon as a winner is observed, and the loser unwinds at its next safe point (tracked for the final program-exit join).

`race` cannot be used inside `atomic` — its effects are not rollback-safe.

### `atomic`

`atomic do ...` is a keyword form (not a `base.` function). It runs an IO body in a database transaction; the block is an `IO a` where `a` is the body's `yield` type. If the body calls `retry`, the transaction rolls back and waits for a relation change before re-executing.

```knot
*accounts : [{name: Text, balance: Int 1}]
transfer \from to amount -> atomic do
  accounts <- full *accounts
  *accounts = do
    a <- accounts
    yield (case a.name == from of
      Bool.True {} -> (base.unify a {balance (a.balance - amount)})
      Bool.False {} -> (case a.name == to of
        Bool.True {} -> (base.unify a {balance (a.balance + amount)})
        Bool.False {} -> a))
```

### `retry`

```
retry : a
```

Used inside `atomic` blocks only. Causes the transaction to rollback and wait until some relation changes, then re-executes the atomic block. Implements STM (Software Transactional Memory) style concurrency.

```knot
*tasks : [{id: Int 1, status: Text}]
waitForTask \id -> atomic do
  tasks <- full *tasks
  with {done do
    t <- tasks
    where t.id == id
    where t.status == "done"
    yield t}
  (do
    base.when (base.count done == 0) base.retry
    yield done)
```

The compiler enforces that `retry` is only used inside `atomic`.

**Row-level wakeup filtering.** The runtime tracks which rows the atomic block
actually read by inspecting `WHERE`/`single (filter ...)` patterns and the
predicates inside them (equality, inequality, ordered comparisons, and `IN`
sets). A parked retry is only woken when an UPDATE, DELETE, or INSERT touches
a matching row. So a worker retrying on `WHERE id = 1` is not woken by writes
to `id = 2`, and a worker retrying on `status IN ("queued", "running")` is
unaffected by writes that leave the status outside that set. Bulk
replacements (`*rel = ...`) wake all watchers conservatively.

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
length : Text -> Int 1
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
take : Int 1 -> Text -> Text         -- characters
take : Int 1 -> [a]  -> [a]          -- rows
drop : Int 1 -> Text -> Text
drop : Int 1 -> [a]  -> [a]
```

```knot
base.take 3 "hello"        -- "hel"
base.take 2 [10 20 30]   -- [10, 20]
base.drop 3 "hello"        -- "lo"
base.drop 1 [10 20 30]   -- [20, 30]
```

### `contains`

```
contains : Text -> Text -> Bool
```

Check if the second argument contains the first as a substring.

```knot
base.contains "ell" "hello" -- True
```

---

## Console I/O

### `println`

```
println : a -> IO {}
```

Print a value to stdout followed by a newline. `putLine` is an alias.

### `print`

```
print : a -> IO {}
```

Print a value to stdout without a trailing newline.

### `logInfo` / `logWarn` / `logError` / `logDebug`

```
logInfo  : a -> IO {}
logWarn  : a -> IO {}
logError : a -> IO {}
logDebug : a -> IO {}
```

Leveled logging to stderr (so output does not mix with `println` on stdout).
When stderr is a TTY, output is colored; otherwise each record is written as
one JSON line for log aggregators. `logDebug` only emits when the program is
launched with `--debug` — debug records are dropped silently otherwise. (Every
compiled Knot program accepts `--debug` automatically; see
[Runtime CLI](#runtime-cli).)

```knot
do
  base.logInfo "starting"
  base.logWarn {event "low memory" availableMb 64}
  yield {}
```

### `show`

```
show : a -> Text
```

Convert any value to its text representation. This is a pure function (no IO).

### `readLine`

```
readLine : IO Text
```

Read a line of input from stdin.

---

## Control Flow

### `when` / `unless`

```
when   : Bool -> IO {} -> IO {}
unless : Bool -> IO {} -> IO {}
```

Run an IO action conditionally. `when cond a` runs `a` if `cond` is `True {}`; `unless cond a` runs `a` if `cond` is `False {}`. The skipped branch becomes `yield {}`.

```knot
(do
  base.when (5 > 0) (base.println "positive")
  base.unless (1 > 2) (base.println "(quiet mode)")
  yield {})
```

### `forEach`

```
forEach : [a] -> (a -> IO {}) -> IO {}
```

Sequence an IO action over each row of a relation. Iteration follows the relation's deterministic order (after any `sortBy`).

```knot
base.forEach ["a" "b" "c"] (\s -> base.println s)
```

---

## File System

All file system functions return `IO ` values.

### `readFile`

```
readFile : Text -> IO Text
```

Read an entire file's contents as text.

### `writeFile`

```
writeFile : Text -> Text -> IO {}
```

Write text to a file (creates or overwrites). First argument is the path, second is the content.

### `appendFile`

```
appendFile : Text -> Text -> IO {}
```

Append text to a file.

### `fileExists`

```
fileExists : Text -> IO Bool
```

Check whether a file exists at the given path.

### `removeFile`

```
removeFile : Text -> IO {}
```

Delete a file.

### `listDir`

```
listDir : Text -> IO [Text]
```

List directory entries as a relation of filenames.

```knot
do
  files <- base.listDir "."
  yield (base.filter (\f -> base.contains ".knot" f) files)
```

---

## Time

### `now`

```
now : IO Int Ms
```

Return the current Unix timestamp in milliseconds. The result is tagged with the built-in `Ms` unit; use `stripUnit` if you need a plain `Int 1`.

### `sleep`

```
sleep : Int Ms -> IO {}
```

Pause the current thread for the given number of milliseconds. Inside a `race` worker, `sleep` parks on the worker's cancel condvar and wakes immediately if the peer wins.

---

## Random

### `randomInt`

```
randomInt : Int u -> IO Int u
```

Generate a random integer in the range `[0, bound)`. Unit-polymorphic — the bound's unit is preserved in the result, so `randomInt 100 Usd` returns `Int Usd`.

### `randomFloat`

```
randomFloat : IO Float u
```

Generate a random float in the range `[0.0, 1.0)`. Unit-polymorphic — the unit is inferred from context.

### `randomUuid`

```
randomUuid : IO Uuid
```

Generate a fresh UUID. The output is a RFC 9562 UUIDv7 — time-ordered, so values sort chronologically and are well-suited as primary keys.

```knot
do
  u <- base.randomUuid
  base.println u
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
parseJson : Text -> Maybe a
```

Parse a JSON string into a value, returning `Just value` on success and `Nothing` on a parse failure. Objects become records, arrays become relations, strings become `Text`, numbers become `Int 1` or `Float 1`, booleans become `Bool`, and null becomes `Nothing {}` (the `Maybe` wire convention). Decoding is type-directed where a target type can be inferred.

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
bytesLength : Bytes -> Int u
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
bytesGet : Int 1 -> Bytes -> Maybe (Int 1)
```

Get the byte value (0–255) at the given index.

### `bytesSlice`

```
bytesSlice : Int u1 -> Int u2 -> Bytes -> Bytes
```

Extract a sub-range. Arguments: start index, length, bytes.

### `hash`

```
hash : a -> Bytes
```

BLAKE3 hash of any value, returned as 32 bytes. `Bytes` and `Text` hash their
raw contents; structured values (records, relations, constructors) hash a
canonical serialisation, so equal logical values always produce equal digests.

```knot
base.bytesToHex (base.hash "hello")    -- "ea8f163..."
```

---

## Numeric Conversion

### `floor`

```
floor : Float 1 -> Int 1
```

Round toward negative infinity (`floor (-2.3)` is `-3`). The result is dimensionless.

### `intToFloat`

```
intToFloat : Int 1 -> Float 1
```

Widen an `Int` to a `Float` (lossy past 2⁵³). The result is dimensionless.

### `textToInt`

```
textToInt : Text -> Maybe (Int 1)
```

Parse an integer. Returns `Nothing {}` on malformed input.

### `textToFloat`

```
textToFloat : Text -> Maybe (Float 1)
```

Parse a float. Returns `Nothing {}` on malformed input.

---

## Record Operations

### `unify`

```
unify : {r1} -> {r2} -> {r1 ∪ r2}   (shape computed at the call site)
```

Merge two records. The result has every field from both arguments; on a name
conflict the **right** argument's value (and type) wins.

```knot
unify {a 1 name "x"} {b 2}            -- {a: 1, b: 2, name: "x"}
unify {name "l" a 1} {name "r" c 3.0} -- {a: 1, c: 3.0, name: "r"}
unify {a 1} {a 2}                      -- {a: 2}
```

Arguments may be closed records **or open rows**. Merging into an open row — a
lambda parameter or relation row whose full shape isn't pinned at the call
site — overlays the right's fields and constrains the base to contain them, so
`case r.id == x of Bool.True {} -> unify r {balance y}; Bool.False {} -> r`
type-checks (the merged result and the bare row agree on the other fields).
Relation rows work: `map (\row -> unify row {active 1}) (full *items)` merges
defaults into every row. A genuinely non-record argument is a type error.

`unify` is shape-dependent — its result type is a function of the two argument
field names, computed by the type checker at the call site rather than by a
single `forall` scheme. It does not push down to SQL (a per-row record merge is
not a column operation), so over a relation it reads the relation into memory
(`full *items`).

> **Known limitation (pre-existing):** projecting a field that exists *only* on
> the merged record inside a `map`/`filter` over a list or relation —
> `map (\r -> (unify r {active 1}).active) xs` — returns at most one row. This
> is a general bug with projecting a computed record's field in a list/relation
> `map` (it reproduces with a plain `{a r.a b 1}.b` construction, no `unify`
> involved), not specific to `unify`. Project the whole merged record, or a
> field from the original row, instead.

---

## Morphs (`base.morph`)

`base.morph` is a nested record of type-directed conversions consumed by the
`^into` implicit-field projection. Each `<from>To<to>` field holds an
`into : S -> T` function:

```
base.morph.textToBytes.into        : Text -> Bytes
base.morph.bytesToText.into        : Bytes -> Maybe Text
base.morph.bytesToHex.into         : Bytes -> Text
base.morph.textToBytesFromHex.into : Text -> Maybe Bytes
base.morph.intToFloat.into         : Int 1 -> Float 1
base.morph.textToInt.into          : Text -> Maybe (Int 1)
base.morph.textToFloat.into        : Text -> Maybe (Float 1)
base.morph.intToText.into          : Int 1 -> Text
base.morph.floatToText.into        : Float 1 -> Text
base.morph.boolToText.into         : Bool -> Text
```

Rather than projecting these directly, write `(^into) x` where the target type
is known from context; the compiler selects the matching morph by **both** the
argument and the expected result type:

```knot
asInt : Text -> Maybe (Int 1)
asInt (\s -> (^into) s)     -- resolves base.morph.textToInt.into

asText : Int 1 -> Text
asText (\n -> (^into) n)    -- resolves base.morph.intToText.into
```

`(^into) "42"` with no result annotation is ambiguous (several morphs accept
`Text`) and is a compile error — pin the result type. See
[DESIGN.md](DESIGN.md#implicit-dictionaries-field--t-) for the `^field`
mechanism.

---

## HTTP

The HTTP types and primitives are defined in the language spec (`DESIGN.md`). The standard library exposes:

### `listen` / `listenOn`

```
listen   : Int u -> Server a -> IO {}
listenOn : Text   -> Int u -> Server a -> IO {}
```

Start an HTTP server built with `serve API where ...`. `listen` binds to all interfaces; `listenOn` takes an explicit bind address.

### `fetch` / `fetchWith`

```
fetch     : Text -> Endpoint -> IO (Result HttpError T)
fetchWith : Text -> {headers: [{name: Text, value: Text}]}
                -> Endpoint -> IO (Result HttpError T)
```

Type-safe HTTP client built from route declarations. `Endpoint` is a route constructor; the response type `T` is inferred from the route. `fetchWith` lets you add ad-hoc headers on top of the route's declared ones. When the route declares response headers, the success body wraps as `{body: T, headers: H}` inside `Ok`.

---

## Cryptography

Knot provides elliptic-curve cryptography built-ins using X25519 (encryption) and Ed25519 (signing).

### `generateKeyPair`

```
generateKeyPair : IO ({privateKey: Bytes, publicKey: Bytes})
```

Generate an X25519 key pair for encryption/decryption. Inside a `do` block, bind with `keys <- generateKeyPair`.

### `generateSigningKeyPair`

```
generateSigningKeyPair : IO ({privateKey: Bytes, publicKey: Bytes})
```

Generate an Ed25519 key pair for signing/verification. Inside a `do` block, bind with `keys <- generateSigningKeyPair`.

### `encrypt`

```
encrypt : Bytes -> Bytes -> IO (Maybe Bytes)
```

Encrypt plaintext bytes with a public key (sealed-box: X25519 ECDH + ChaCha20-Poly1305). First argument is the public key, second is the plaintext. Returns IO because a fresh ephemeral key pair and nonce are generated per call.

### `decrypt`

```
decrypt : Bytes -> Bytes -> Maybe Bytes
```

Decrypt ciphertext bytes with a private key. First argument is the private key, second is the ciphertext.

### `sign`

```
sign : Bytes -> Bytes -> Maybe Bytes
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
stripUnit      : Int u -> Int 1
withUnit       : Int 1 -> Int u
stripFloatUnit : Float u -> Float 1
withFloatUnit  : Float 1 -> Float u
```

Drop or attach a unit tag. Identity at runtime — they only adjust the
compile-time type. Use them when you need to rebrand a value with a different
concrete unit (e.g. `Ms` → `S`).

### `strip` / `dress`

```
strip : Int u -> Int 1
dress : Int 1 -> Int u
```

Generalized unit rebranding that works across both `Int` and `Float` (the same
shapes hold with `Float` for `Int`; the polymorphic `a u -> a 1` form is not
writable surface syntax). `strip` drops a value's unit; `dress` attaches one to
a dimensionless value, the target pinned by context or annotation. The `u` is a
unit variable (kind `Unit`), so only unit-carrying numerics qualify. Identity
at runtime:

```knot
toS : Int Ms -> Int S
toS \ms -> base.dress (base.strip ms / 1000)

toMiles : Float M -> Float Mi
toMiles \d -> base.dress (base.strip d * 0.000621371)
```

A dimensionless `Int 1`/`Float 1` does **not** unify with a concrete unit, so
`dress` is the explicit escape hatch for re-attaching one — an annotation alone
cannot rebrand a value already pinned to `1`.

---

## Operator Behavior (Intrinsic)

There is **no user-facing trait system**. You cannot declare a `trait`, write an
`impl`, or put a `Num a =>` bound on a function — the parser rejects all of it.
The operators below are **intrinsic**: the compiler knows how to evaluate them
directly on the supported types, with no trait dictionary involved. This table
describes what each operator does and which types it works on.

### `==` / `!=` — equality

Works on `Int 1`, `Float 1`, `Text`, `Bool`, and unit-annotated numerics.
Pushes down to `=` in SQL when used in a SQL-compilable comprehension.

### `<` / `>` / `<=` / `>=` — ordering

Works on `Int 1`, `Float 1`, `Text`, and unit-annotated numerics. `Ordering` is
the `LT {}` / `EQ {}` / `GT {}` ADT (see `base.compare`). Pushes down to SQL
comparison operators.

### `+` / `-` / `*` / `/` and unary `-` — arithmetic

Works on `Int 1`, `Float 1`, and unit-annotated numerics. `Int 1` arithmetic is
checked and panics on overflow. Units compose algebraically (see [Units of
Measure](#units-of-measure)). `+`/`-` require matching units; `*`/`/` combine
units. Pushes down to SQL arithmetic.

### `%` — modulo

`Int 1` remainder (sign follows the dividend) and `Float 1` `fmod`. Modulo by
zero panics. Handled by intrinsic codegen — there is no user-overridable
operation. Pushes down to SQLite `%` in a SQL-compilable comprehension.

### `++` — concatenation

Works on `Text` (string append) and `[a]` (relation union). Pushes down to SQL
`||` for text.

### `&&` / `||` — boolean logic

Short-circuiting AND/OR on `Bool`. Pushes down to SQL `AND`/`OR`.

### `|>` — pipe forward

`x |> f` is `f x`. Purely syntactic.

### Higher-order functions without traits

Functions like `base.map`, `base.fold`, `base.traverse` are ordinary
polymorphic functions over concrete types (`[a]`, `Maybe a`, `Result e a`) —
not trait methods. They appear in this reference with plain `a`/`b` type
variables and no bounds.

---

## Built-in Types

| Type | Description |
|------|-------------|
| `Int 1` | 64-bit signed integer (`i64`); arithmetic is checked and panics on overflow |
| `Float 1` | 64-bit floating point |
| `Int u` | Integer with compile-time unit (e.g. `Int Usd`) |
| `Float u` | Float with compile-time unit (e.g. `Float M`, `Float (M/S^2)`) |
| `Text` | Unicode string |
| `Bool` | `True {}` or `False {}` |
| `Bytes` | Byte string |
| `Uuid` | RFC 9562 UUIDv7 identifier (TEXT in SQLite) |
| `[a]` | Relation (set of values of type `a`) |
| `IO a` | IO action producing a value of type `a` |
| `Ordering` | `LT {}`, `EQ {}`, or `GT {}` |
| `Maybe a` | `Nothing {}` or `Just {value: a}` (supports `do`/`<-`) |
| `Result e a` | `Err {error: e}` or `Ok {value: a}` (supports `do`/`<-`) |

### Units of Measure

Compile-time units on `Int` and `Float`. Fully erased at runtime — no performance cost, no runtime representation. Every numeric type carries a unit; write `Int 1` / `Float 1` for the dimensionless case.

#### No Declaration Needed

Units are not declared. Any name used in a unit position is a unit — there is nothing to declare since a unit has no body, only a name. Compound units are written inline as expressions.

#### Literals and Type Annotations

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

#### Arithmetic Rules

- `+`/`-` require matching units
- `*`/`/` compose units algebraically
- Unary negation preserves units
- Scalar (dimensionless) multiplication preserves the other operand's unit

```
(10.0 : Float M) + (5.0 : Float M)   -- Float M
(10.0 : Float M) + (5.0 : Float S)   -- type error
(10.0 : Float M) * (5.0 : Float M)   -- Float (M^2)
(100.0 : Float M) / (10.0 : Float S) -- Float (M/S)
2.0 * (5.0 : Float M)                -- Float M
-((5.0 : Float M))                   -- Float M
```

#### Unit Polymorphism

Concrete units are uppercase; lowercase names are unit variables:

```knot
double : Float u -> Float u
double \x -> x + x
```

#### Unit-Preserving Functions

`avg`, `minOn`, and `maxOn` preserve units from their projection function; `sum` preserves the units of the numeric relation it sums:

```knot
*trips : [{distance: Float M}]

(do
  base.println (base.show (base.avg   (\t -> t.distance) *trips))
  base.println (base.show (base.sum   (base.map (\t -> t.distance) (full *trips))))
  base.println (base.show (base.minOn (\t -> t.distance) (full *trips)))
  base.println (base.show (base.maxOn (\t -> t.distance) (full *trips)))
  yield {})
```

---

## Operators

| Operator | Works on | Behavior |
|----------|----------|----------|
| `+` `-` `*` `/` | `Int 1`, `Float 1`, unit-annotated | Arithmetic (checked on `Int 1`) |
| `%` | `Int 1`, `Float 1` | Modulo / `fmod` |
| unary `-` | `Int 1`, `Float 1`, unit-annotated | Negation |
| `==` `!=` | `Int 1`, `Float 1`, `Text`, `Bool` | Equality |
| `<` `>` `<=` `>=` | `Int 1`, `Float 1`, `Text` | Ordering |
| `++` | `Text`, `[a]` | Concatenation / union |
| `&&` `\|\|` | `Bool` | Short-circuiting logic |
| `\|>` | any | Pipe forward (`x \|> f` = `f x`) |

All are intrinsic (no trait mechanism); see [Operator Behavior
(Intrinsic)](#operator-behavior-intrinsic).

---

## Runtime CLI

Every compiled Knot program accepts a common set of runtime flags and
subcommands without any user wiring:

| Argument | Description |
|----------|-------------|
| `--debug` | Enable `logDebug` output; without this flag, `logDebug` calls are dropped silently. |
| `--help` | Print usage including any compile-time overrides exposed by the program. |
| `--http-max-body-bytes=N` | Cap HTTP request and response bodies. Suffixes: `K`, `M`, `G`. Default `16M`. Applies to both `listen` and `fetch`. |
| `--<name>=<value>` | Override a compile-time constant. The compiler exposes top-level constants annotated for override; see the program's own `--help`. The same flags may be set at build time via `knot build … --<name>=<value>`. |
| `<program> db` | Launch a terminal-UI database explorer over the program's `<name>.db` SQLite file. Browses every source relation, paginates rows, and lets you drill into individual records. |
| `<program> api <RouteName>` | Print an OpenAPI 3.0 JSON specification for the named `route` declaration. Useful for generating client SDKs or feeding into Swagger UI. |

The compiler itself (`knot`) supports:

| Command | Description |
|---------|-------------|
| `knot build <file.knot> [-o <path>] [--<name>=<value> …]` | Compile to a native executable. Overrides supply compile-time constants. |
| `knot fmt [--check] [--stdout] <file.knot> …` | Format source files in place. `--check` exits non-zero when files are unformatted; `--stdout` prints to stdout instead of rewriting. |
| `knot help` | Show CLI usage. |