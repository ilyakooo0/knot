# Knot

<center>
<img src="logo.png" width="300px">
</center>

Knot is a functional relational programming language. Relations are the primary data structure, computation is pure and functional, and state is automatically persisted to SQLite.

## Quick Start

```sh
# Build
cargo build

# Compile and run a Knot program
cargo run -p knot-compiler -- build examples/hello.knot
./examples/hello
```

## Documentation

- [Language Design](DESIGN.md) — full language specification
- [Standard Library](stdlib.md) — all built-in functions, traits, and types

## Project Structure

```
crates/
  knot/              Frontend (lexer, parser, AST, diagnostics)
  knot-runtime/      Runtime library (value representation, SQLite persistence)
  knot-compiler/     Cranelift-based compiler (CLI: knotc)
examples/            Example .knot programs
```

## Example

```knot
type Person = {name: Text, age: Int}
*people : [Person]

&seniors = do
  people <- *people
  yield (filter (\p -> p.age > 65) people)

main = do
  set *people = [
    {name: "Alice", age: 30},
    {name: "Bob", age: 70}
  ]
  seniors <- &seniors
  println (count seniors)
  yield {}
```

## Tests

```sh
cargo test                   # All tests
cargo test -p knot           # Frontend only
```
