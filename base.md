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
fork : IO {} {} -> IO {} {}
```
Run an IO action on a new OS thread (fire-and-forget). Each thread gets its own SQLite connection (WAL mode). Main waits for all threads before exiting.

```knot
atomic : IO {} a -> IO {} a
```
Run an IO body in a database transaction. Body must contain only DB operations. Supports `retry` for STM-style waiting.

```knot
retry : a
```
Inside `atomic` blocks only. Rolls back the transaction and waits for a relation change, then re-executes.

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
Sum a numeric projection over a relation. The projection function extracts the value to sum from each row. Works with both `Int` and `Float`.

```knot
sum (\x -> x) [10, 20, 30]              -- 60
sum (\o -> o.amount) *orders             -- total of all order amounts
*orders |> sum (\o -> o.amount)          -- same with pipe
```

```knot
avg : (a -> Float) -> [a] -> Float
```
Average a numeric projection over a relation. Always returns `Float`. Returns `0.0` for an empty relation.

```knot
avg (\x -> x) [10.0, 20.0, 30.0]        -- 20.0
avg (\e -> e.salary) *employees          -- mean salary
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
now : IO {clock} Int
```
Current time in milliseconds since the Unix epoch. Returns an IO action. Use `<-` in a do-block to get the value:

```knot
main = do
  t <- now
  println ("Time: " ++ show t)
  yield {}
```

```knot
randomInt : Int -> IO {random} Int
```
Random integer in `[0, bound)`. Returns an IO action.

```knot
randomFloat : IO {random} Float
```
Random float in `[0.0, 1.0)`. Returns an IO action.

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
listen : Int -> (a -> b) -> {}
```
Start an HTTP server on the given port with a handler function. The handler receives a route ADT value and returns a response. Has the `{network}` effect. See `route` declarations in the language spec for defining typed endpoints.

## Concurrency

```knot
fork : IO {} {} -> IO {} {}
```
Run an IO action on a new OS thread. Fire-and-forget — the spawned thread runs independently. Each thread gets its own SQLite connection (WAL mode). Main waits for all threads before exiting. Do blocks can be passed without parentheses: `fork do ...`.

```knot
main = do
  fork do
    println "from thread"
  println "from main"
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

## IO Type Syntax

IO types can be annotated in type signatures:

```knot
IO {console} {}         -- IO action with console effect, returns unit
IO {fs} Text            -- IO action with fs effect, returns Text
IO {clock, random} Int  -- IO action with multiple effects
```

Effects tracked in IO types: `console`, `fs`, `network`, `clock`, `random`.

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
  negate : a -> a
```
Numeric operations as trait methods. Requires `Eq`. Built-in impls for `Int` and `Float`. Use as a trait bound for generic numeric code:

```knot
double : Num a => a -> a
double = \x -> add x x

double 21      -- 42
double 1.5     -- 3.0
```

The methods wrap the corresponding built-in operators (`+`, `-`, `*`, `/`, unary `-`), but can be passed as values and used in higher-order functions:

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

### Alternative

```knot
trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt : f a -> f a -> f a
```
Built-in impl for `[]`: `empty = []`, `alt = union`.

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
