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
