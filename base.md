# Knot Standard Library

Every function listed here is built in to the compiler. No imports needed.

## IO

```knot
println : a -> {}
```
Print a value followed by a newline. Works on any type.

```knot
print : a -> {}
```
Print a value without a trailing newline.

```knot
show : a -> Text
```
Convert any value to its text representation. Records print as `{field: value, ...}`, relations as `[v1, v2, ...]`, constructors as `Tag {fields}`.

## Relations

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
now : Int
```
Current time in milliseconds since the Unix epoch. Has the `{clock}` effect.

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

All file system functions carry the `{fs}` effect.

```knot
readFile : Text -> Text
```
Read the entire contents of a file as text.

```knot
readFile "config.json"    -- "{\"port\": 8080}"
```

```knot
writeFile : Text -> Text -> {}
```
Write text to a file. Creates the file if it doesn't exist, overwrites if it does. The first argument is the path, the second is the contents.

```knot
writeFile "output.txt" "hello"
```

```knot
appendFile : Text -> Text -> {}
```
Append text to a file. Creates the file if it doesn't exist.

```knot
appendFile "app.log" ("event at " ++ show now ++ "\n")
```

```knot
fileExists : Text -> Bool
```
Check whether a file or directory exists at the given path.

```knot
if fileExists "config.json"
  then readFile "config.json"
  else "{}"
```

```knot
removeFile : Text -> {}
```
Delete a file.

```knot
listDir : Text -> [Text]
```
List the entries of a directory as a relation of filenames.

```knot
listDir "."    -- ["main.knot", "lib.knot", "knot.db"]

-- Filter to specific files
listDir "src" |> filter (\f -> contains ".knot" f)
```

## Server

```knot
listen : Int -> (a -> b) -> {}
```
Start an HTTP server on the given port with a handler function. The handler receives a route ADT value and returns a response. Has the `{network}` effect. See `route` declarations in the language spec for defining typed endpoints.

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
