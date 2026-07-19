# Knot record/`with` syntax migration spec

The knot language DROPPED the old record delimiters. Colon and comma are gone
from record EXPRESSIONS, record PATTERNS, and `with` blocks. Record TYPES keep
their old syntax. There is NO field punning anymore.

## The parser rule (so output must re-parse)
A record field value is a SINGLE ATOM or a PARENTHESIZED compound. After a
completed value, a bare lowercase identifier ALWAYS opens the next field. So a
value that is an application, binary/unary op, lambda, `if`, `do`, `case`,
`with`, or any multi-token expression MUST be wrapped in parentheses.

Atomic values (NO parens needed): integer/float/text/bool literals, a variable
(`x`), a field-access chain (`r.a`, `a.b.c`), a constructor (`Just`, `Nothing`),
a nested record (`{...}`), a list (`[...]`), an already-parenthesized expr.

## Transformations

1. Record EXPRESSION  `{f: v, g: w}`      -> `{f v g w}`
   - drop `:` after each field name, drop `,` separators, use ONE space.
   - parenthesize non-atomic `v`/`w`:
       `{value: f value}`   -> `{value (f value)}`
       `{n: 1 + 2}`         -> `{n (1 + 2)}`
       `{name: e.name}`     -> `{name e.name}`        (field-access is atomic)
       `{x: {a: 1}}`        -> `{x {a 1}}`            (nested record, recurse)

2. Record UPDATE  `{base | f: v, g: w}`   -> `{base | f v g w}`
   - keep `|`, same value-parenthesization rule.

3. `with` BLOCK  `with {x: e, y: e2} body` -> `with {x e y e2} body`
   - field names are the bindings; same value-parenthesization rule.

4. Record PATTERN  `{f: p, g: q}`          -> `{f p g q}`
   - PUNNING GONE: `Just {value}`          -> `Just {value value}`
                     `Just {value: x}`      -> `Just {value x}`
   - empty patterns `Nothing {}` / `Just {}` unchanged.

## DO NOT CHANGE
- Record TYPES in annotations/aliases: `{name: Text, age: Int}`  (KEEP `:` `,`).
- Type annotations `x : Int`, signatures `f : a -> b`.
- Effect rows `IO {| e} {}` (the `{| e}` row-tail and trailing `{}` stay).
- Rust code itself — only the knot source inside string literals / .knot files.

## Verify
After editing a file, run its test target and make it pass:
  cargo test -p <crate> --test <file_stem>
The compiler validates the new syntax; a green test run is the acceptance gate.
