# Knot

Knot is a functional relational programming language. Relations are the
primary data structure, computation is pure and functional, and state is
automatically persisted to SQLite. Effects are inferred and tracked in the
type system.

## Quick Start

```sh
cargo build
cargo run -p knot-compiler -- build examples/hello.knot
./examples/hello
```

A compiled binary creates a `<name>.db` SQLite file for persistence and a
`<name>.schema.lock` file for migration tracking.
