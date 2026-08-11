# The `base` Namespace — Complete Reference

Every name reachable as `base.<name>` in a compiled Knot program. The `base`
record is injected into every program's top scope by the compiler; nothing else
enters the environment by default.

Sources of truth: `crates/knot-compiler/src/base.rs` (the prelude record +
`BASE_STDLIB_FNS`) and `crates/knot-compiler/src/codegen.rs` (registration).
This file is generated to match them — if a name is registered but missing
here, that's a doc bug.

Sections: [Relations](#relations) · [Lists](#lists-baselist) ·
[Text](#text) · [Bytes](#bytes) · [JSON](#json) · [Numbers](#numbers) ·
[Units](#units) · [Records](#records) · [Control flow](#control-flow) ·
[Console & logging](#console--logging) · [Files](#files) ·
[Time & randomness](#time--randomness) · [Concurrency](#concurrency) ·
[HTTP](#http) · [Crypto](#crypto) · [Reflection](#reflection) ·
[Debugging](#debugging) · [Morphs](#morphs-basemorph)

---

## Relations

A relation `[a]` is an unordered set. These are the ordinary pure operations;
query-shaped calls (`filter`, `map`, `count`, …) push down to SQL when the
compiler can translate them.

### `base.filter`
```
base.filter : (a -> Bool) -> [a] -> [a]
```
Keep rows satisfying the predicate.

### `base.map`
```
base.map : (a -> b) -> [a] -> [b]
```
Transform each row.

### `base.fold`
```
base.fold : (b -> a -> b) -> b -> [a] -> b
```
Left fold over the rows.

### `base.traverse`
```
base.traverse : (a -> f b) -> [a] -> f [b]
```
Map an effectful/partial function over each row, sequencing the results
(`f` is `IO`, `Maybe`, or `Result` — `do`-dispatch is structural).

### `base.bind`
```
base.bind : (a -> [b]) -> [a] -> [b]
```
Relation flatMap (function first, then the relation).

### `base.forEach`
```
base.forEach : [a] -> (a -> IO {}) -> IO {}
```
Run an IO action for each row, in order.

### `base.count`
```
base.count : [a] -> Int u
```
Number of rows. Unit-polymorphic.

### `base.countWhere`
```
base.countWhere : (a -> Bool) -> [a] -> Int u
```
Rows satisfying the predicate. SQL-pushed when possible.

### `base.sum`
```
base.sum : [Int u] -> Int u
```
Sum of a numeric relation, preserving the unit.

### `base.avg`
```
base.avg : (a -> Float u) -> [a] -> Float u
```
Mean of the projected key across rows.

### `base.minOn` / `base.maxOn`
```
base.minOn : (a -> b) -> [a] -> b
base.maxOn : (a -> b) -> [a] -> b
```
The smallest/largest projected key (the key, not the row). **Aborts the
program on an empty relation** — there is no `Nothing` case; guard with
`base.count`/non-emptiness first.

### `base.min` / `base.max`
```
base.min : a -> a -> a
base.max : a -> a -> a
```
Binary minimum/maximum (prelude polymorphic helpers).

### `base.union` / `base.inter` / `base.diff`
```
base.union : [a] -> [a] -> [a]
base.inter : [a] -> [a] -> [a]
base.diff  : [a] -> [a] -> [a]
```
Set union / intersection / difference.

### `base.distinct`
```
base.distinct : [a] -> [a]
```
Drop duplicate rows (relations are sets; this normalizes a value that
arrived with structural duplicates).

### `base.head`
```
base.head : [a] -> Maybe a
```
An arbitrary row (relations are unordered), `Nothing` on empty.

### `base.findFirst`
```
base.findFirst : [a] -> (a -> Bool) -> Maybe a
```
An arbitrary row satisfying the predicate (relation-first arg order).

### `base.any` / `base.all`
```
base.any : (a -> Bool) -> [a] -> Bool
base.all : (a -> Bool) -> [a] -> Bool
```
Existential / universal quantification over rows.

### `base.elem`
```
base.elem : a -> [a] -> Bool
```
Membership test.

### `base.single`
```
base.single : [a] -> Maybe a
```
`Just` the row iff the relation has exactly one row, else `Nothing`.

### `base.match`
```
base.match : Constructor -> [ADT] -> [Payload]
```
Keep only the rows of an ADT relation built with the given constructor,
returning their payloads.

### `base.sortBy` / `base.sortByDesc`
```
base.sortBy     : (a -> b) -> [a] -> [a]
base.sortByDesc : (a -> b) -> [a] -> [a]
```
Order rows by the projected key, ascending / descending. (The result is a
relation whose iteration order is fixed; SQL-pushed to `ORDER BY`.)

### `base.take` / `base.drop`
```
base.take : Int 1 -> [a] -> [a]
base.drop : Int 1 -> [a] -> [a]
```
Keep / skip the first *n* rows (only meaningful on a sorted relation).

### `base.reverse`
```
base.reverse : Text -> Text
```
Reverse a text. (Relations are unordered, so there is no relation `reverse`;
order a relation with `sortBy`/`sortByDesc` instead.)

### `base.upsertBy`
```
base.upsertBy : (a -> Bool) -> a -> [a] -> [a]
```
Replace the row matching the predicate with the given row, or insert it if
no row matches.

---

## Lists (`base.list`)

Ordered, persistent lists — an ADT (`Nil` / `Cons`), distinct from the
unordered relation `[a]`. Reachable under the nested `base.list` namespace.
`nil` is a 1-arg function ignoring its argument; call `base.list.nil {}` so
the element type stays polymorphic per call site.

```
base.list.nil          : {} -> List a
base.list.cons         : a -> List a -> List a
base.list.isNil        : List a -> Bool
base.list.head         : List a -> Maybe a
base.list.tail         : List a -> List a
base.list.length       : List a -> Int 1
base.list.map          : (a -> b) -> List a -> List b
base.list.filter       : (a -> Bool) -> List a -> List a
base.list.fold         : (b -> a -> b) -> b -> List a -> b
base.list.reverse      : List a -> List a
base.list.append       : List a -> List a -> List a
base.list.fromRelation : [a] -> List a
base.list.toRelation   : List a -> [a]
```

---

## Text

### `base.toUpper` / `base.toLower`
```
base.toUpper : Text -> Text
base.toLower : Text -> Text
```
Unicode-aware case mapping.

### `base.toAsciiUpper` / `base.toAsciiLower`
```
base.toAsciiUpper : Text -> Text
base.toAsciiLower : Text -> Text
```
ASCII-only case mapping (faster, locale-independent).

### `base.length`
```
base.length : Text -> Int 1
```
Length in characters.

### `base.byteLength`
```
base.byteLength : Text -> Int 1
```
Length in UTF-8 bytes.

### `base.trim`
```
base.trim : Text -> Text
```
Strip leading and trailing Unicode whitespace.

### `base.trimAscii` / `base.ltrimAscii` / `base.rtrimAscii`
```
base.trimAscii  : Text -> Text
base.ltrimAscii : Text -> Text
base.rtrimAscii : Text -> Text
```
Strip ASCII whitespace: both ends / left only / right only.

### `base.contains` / `base.startsWith` / `base.endsWith`
```
base.contains   : Text -> Text -> Bool
base.startsWith : Text -> Text -> Bool
base.endsWith   : Text -> Text -> Bool
```
Substring / prefix / suffix test. Argument order is `(needle, haystack)` —
`base.contains "ell" "hello"` is `True`.

### `base.chars`
```
base.chars : Text -> [Text]
```
Split into a relation of single-character strings.

### `base.strip`
See [Units](#units) — `strip`/`dress` are the unit record accessors.

---

## Bytes

### `base.textToBytes` / `base.bytesToText`
```
base.textToBytes : Text -> Bytes
base.bytesToText : Bytes -> Maybe Text
```
UTF-8 encode / decode (`Nothing` on invalid UTF-8).

### `base.bytesLength` / `base.bytesGet` / `base.bytesSlice`
```
base.bytesLength : Bytes -> Int 1
base.bytesGet    : Bytes -> Int 1 -> Maybe (Int 1)
base.bytesSlice  : Bytes -> Int 1 -> Int 1 -> Bytes
```
Length, indexed byte (`Nothing` out of range), and `[start, end)` slice.

### `base.bytesConcat`
```
base.bytesConcat : Bytes -> Bytes -> Bytes
```
Concatenation.

### `base.bytesToHex` / `base.bytesFromHex` / `base.hexDecode`
```
base.bytesToHex   : Bytes -> Text
base.bytesFromHex : Text -> Maybe Bytes
base.hexDecode    : Text -> Maybe Bytes
```
Hex encode / decode. `bytesFromHex` and `hexDecode` are the same decode;
`Nothing` on malformed input.

### `base.hash`
```
base.hash : a -> Int 1
```
BLAKE3 hash of any value, polymorphic — the structural hash of the value's
canonical encoding.

---

## JSON

### `base.toJson`
```
base.toJson : a -> Text
```
Serialize any value to JSON.

### `base.parseJson`
```
base.parseJson : Text -> Maybe a
```
Parse JSON into the expected type; `Nothing` on malformed input or a type
mismatch.

---

## Numbers

### `base.abs`
```
base.abs : Int u -> Int u
```
Absolute value, preserving the unit.

### `base.intMin` / `base.intMax`
```
base.intMin : Int u -> Int u -> Int u
base.intMax : Int u -> Int u -> Int u
```
Binary minimum/maximum, unit-preserving. (Unlike the polymorphic prelude
`base.min`/`base.max`, these are typed to `Int`.)

### `base.clamp`
```
base.clamp : Int u -> Int u -> Int u -> Int u
```
`clamp lo hi x` — constrain `x` to `[lo, hi]`, unit-preserving.

### `base.floor`
```
base.floor : Float u -> Int u
```
Round toward negative infinity, changing the representation but keeping the
unit.

### `base.intToFloat`
```
base.intToFloat : Int u -> Float u
```
Widen an `Int` to a `Float`, same unit.

### `base.textToInt` / `base.textToFloat`
```
base.textToInt   : Text -> Maybe (Int 1)
base.textToFloat : Text -> Maybe (Float 1)
```
Parse a number from text; `Nothing` on failure. Dimensionless.

---

## Units

Every numeric type carries a unit of measure. These strip/attach units and
the unit *record* (the compile-time dimension evidence).

### `base.stripUnit` / `base.withUnit`
```
base.stripUnit : Int u -> Int 1
base.withUnit  : Int 1 -> Int u
```
Drop / attach a unit on an `Int`. `withUnit` infers the target unit from
context.

### `base.stripFloatUnit` / `base.withFloatUnit`
```
base.stripFloatUnit : Float u -> Float 1
base.withFloatUnit  : Float 1 -> Float u
```
The `Float` pair.

### `base.strip` / `base.dress`
```
base.strip : a -> b
base.dress : b -> a
```
Remove (`strip`) / restore (`dress`) the unit *and* refinement wrapper on a
refined, unit-carrying type — the general escape hatches the
`stripUnit`/`withUnit` family specialize.

---

## Records

### `base.unify`
```
base.unify : {..a} -> {..b} -> {..a, ..b}
```
Merge two records; right-biased — the second record's fields win on a name
conflict. Row-polymorphic: the result has the union of the fields.

---

## Control flow

### `base.when` / `base.unless`
```
base.when   : Bool -> IO {} -> IO {}
base.unless : Bool -> IO {} -> IO {}
```
Run the IO action conditionally.

### `base.id`
```
base.id : a -> a
```
Identity function.

### `base.not`
```
base.not : Bool -> Bool
```
Boolean negation.

---

## Console & logging

### `base.println` / `base.print` / `base.putLine`
```
base.println : a -> IO {}
base.print   : a -> IO {}
base.putLine : a -> IO {}
```
Write to **stdout**. `println` appends a newline, `print` does not,
`putLine` is a newline-terminating alias.

### `base.readLine`
```
base.readLine : IO Text
```
Read a line from stdin.

### `base.debug` / `base.info` / `base.warn` / `base.error`
```
base.debug : (<>logCtx) => Text -> IO {}
base.info  : (<>logCtx) => Text -> IO {}
base.warn  : (<>logCtx) => Text -> IO {}
base.error : (<>logCtx) => Text -> IO {}
```
Leveled, structured logging to **stderr** (separate from `println`'s
stdout). Colored on a TTY, one JSON record per line otherwise. `debug` is
silent unless the program runs with `--debug`.

The `(<>logCtx)` collecting-fold constraint merges every `logCtx` record in
scope at the call site (innermost scope wins) and attaches the fields as
structured context — the caller passes only the message:

```knot
do
  base.info "starting"
  with {logCtx {availableMb 64}}
    (base.warn "low memory")
  yield {}
```

A single-line context field renders inline (`availableMb=64`); a field whose
value contains a newline is lifted out of the splat and shown as a
`│`-guttered block beneath the log line on a TTY, and stays `\n`-escaped
inline in JSON. In JSON mode context fields merge into the emitted record
(`{"level","msg",…ctx,"timestamp"}`), later-wins over the defaults. The
runtime's own events (HTTP serve-loop errors, migrations, watcher panics)
emit through the same machinery, so JSON-mode stderr stays one record per
line.

### `base.log`
```
base.log : (<>logCtx) => Level -> Text -> IO {}
```
The level-parameterized logger the four level wrappers fix. Use it when the
level is computed: `base.log (Level.Warn {}) "message"`.

### `base.logDebug` / `base.logInfo` / `base.logWarn` / `base.logError`
Deprecated renames of `base.debug`/`info`/`warn`/`error`, kept for source
compatibility. Each threads the caller's `logCtx` identically. Prefer the
short names.

### `base.show`
```
base.show : a -> Text
```
Convert any value to its text representation. Pure (no IO).

---

## Files

### `base.readFile` / `base.writeFile` / `base.appendFile`
```
base.readFile   : Text -> IO Text
base.writeFile  : Text -> Text -> IO {}
base.appendFile : Text -> Text -> IO {}
```
Read a whole file; write / append `(path, content)`.

### `base.fileExists` / `base.removeFile` / `base.listDir`
```
base.fileExists : Text -> IO Bool
base.removeFile : Text -> IO {}
base.listDir    : Text -> IO [Text]
```
Existence check, delete, and directory listing.

---

## Time & randomness

### `base.now`
```
base.now : IO (Int Ms)
```
Current Unix timestamp in milliseconds.

### `base.sleep`
```
base.sleep : Int Ms -> IO {}
```
Pause the current thread.

### `base.randomInt` / `base.randomFloat`
```
base.randomInt   : Int u -> IO (Int u)
base.randomFloat : IO (Float u)
```
Random int in `[0, bound)` (preserves the unit); random float in `[0.0, 1.0)`.

### `base.randomUuid`
```
base.randomUuid : IO Uuid
```
An RFC 9562 UUIDv7.

---

## Concurrency

### `base.fork`
```
base.fork : IO a -> IO {}
```
Fire-and-forget an IO action on a new OS thread.

### `base.race`
```
base.race : IO a -> IO b -> IO (Result a b)
```
Run two IO actions concurrently; return the winner's result.

### `base.atomic`
```
base.atomic : IO {} a -> IO {} a
```
Run a block of DB operations in a single transaction. Effect-tracked: an
action with a `console`/`net` effect is rejected inside `atomic`.

### `base.retry`
```
base.retry : a
```
STM retry — only inside `atomic`: roll back, park until a read-matching row
changes, re-run. A compile-time macro, not a record field.

---

## HTTP

### `base.listen` / `base.listenOn`
```
base.listen   : Int u -> Server a -> IO {}
base.listenOn : Text -> Int u -> Server a -> IO {}
```
Run an HTTP server for a `serve` block, on all interfaces / on a given
address. Compile-time macros.

### `base.fetch` / `base.fetchWith`
```
base.fetch     : Text -> Endpoint -> IO (Result HttpError T)
base.fetchWith : Text -> {headers: [..]} -> Endpoint -> IO (Result HttpError T)
```
Type-safe HTTP client against an `api` declaration; `fetchWith` adds ad-hoc
headers. Compile-time macros.

---

## Crypto

### `base.generateKeyPair` / `base.generateSigningKeyPair`
```
base.generateKeyPair        : IO KeyPair
base.generateSigningKeyPair : IO SigningKeyPair
```
Generate an encryption / signing key pair.

### `base.encrypt` / `base.decrypt`
```
base.encrypt : KeyPair -> Bytes -> IO Bytes
base.decrypt : KeyPair -> Bytes -> IO (Maybe Bytes)
```
Public-key encrypt / decrypt (`Nothing` on failure).

### `base.sign` / `base.verify`
```
base.sign   : SigningKeyPair -> Bytes -> IO Bytes
base.verify : VerifyKey -> Bytes -> Bytes -> IO Bool
```
Sign a payload; verify a signature.

---

## Reflection

### `base.extract`
```
base.extract : a -> Text
```
Render a value as evaluable Knot source (dependency-collecting — the output
re-parses to the same value).

### `base.compile`
```
base.compile : Text -> Maybe a
```
In-process JIT compile + eval of a source string; `Nothing` on a compile
error.

---

## Debugging

### `base.todo`
```
base.todo : a
```
An unimplemented hole: type-checks as any type, compiles with a warning, and
aborts (exit 1) with a full context report (source location, expected type,
in-scope bindings) when evaluation reaches it. A compile-time macro.

### `base.trace`
```
base.trace : a -> a
```
A transparent probe: prints the context report plus the traced value, then
returns the value unchanged. Emits a compile-time warning so a stray probe
isn't shipped. A compile-time macro.

---

## Morphs (`base.morph`)

`base.morph` is a nested record of type-directed conversions, consumed by the
`^into` implicit-field projection. Each `<from>To<to>` field holds an
`into : S -> T` with an explicit concrete signature; fallible conversions
return `Maybe`. Because `base` is in every program's top scope, `(^into) x`
resolves the conversion by both the argument's type and the expected result
type.

```
base.morph.textToBytes.into         : Text -> Bytes
base.morph.bytesToText.into         : Bytes -> Maybe Text
base.morph.bytesToHex.into          : Bytes -> Text
base.morph.textToBytesFromHex.into  : Text -> Maybe Bytes
base.morph.intToFloat.into          : Int 1 -> Float 1
base.morph.textToInt.into           : Text -> Maybe (Int 1)
base.morph.textToFloat.into         : Text -> Maybe (Float 1)
base.morph.intToText.into           : Int 1 -> Text
base.morph.floatToText.into         : Float 1 -> Text
base.morph.boolToText.into          : Bool -> Text
```

```knot
asInt : Text -> Maybe (Int 1)
asInt (\s -> (^into) s)     -- resolves base.morph.textToInt.into
```

---

## Not in `base`

These are language-level, not record fields: the constructors
(`Bool.True`/`False`, `Maybe.Nothing`/`Just`, `Result.Ok`/`Err`), the binary
operators (`+` `-` `*` `/` `%` `++` `==` `!=` `<` `>` `<=` `>=` `&&` `||`
`|>`), `do`/`case`/`with`/`refine`, and `emitLog` (the internal runtime
target of `base.log`, not user-facing). They route through dedicated compiler
arms; see [knot.md](knot.md) and [stdlib.md](stdlib.md).
