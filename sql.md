# SQL Pushdown

How relational operations over a `*source` (a SQLite-backed table) compile to
SQL that runs **inside SQLite** instead of loading the whole table into memory
and processing it in Knot. All matching happens in `codegen.rs`; execution in
`knot-runtime`.

Fallback is always **correct but slower**: when a query can't be pushed down it
runs in memory and produces the same answer. Pushdown is a performance
optimization, never a semantic change.

---

## The mechanism

Pushdown recognizes a handful of syntactic shapes over a `*source` and lowers
each to a single SQL statement. There are three entry points:

1. **Filter pushdown** — a comprehension binding `x <- *source` followed by
   `where cond` statements. Each `cond` is translated to a SQL fragment; the
   fragments are joined with `AND` and the read goes through
   `knot_source_read_where`.

2. **Pipe-chain pushdown** (`try_compile_pipe_sql`) — a `|>` chain over a
   `SourceRef` collapses into ONE query with a fixed clause order
   `WHERE → SELECT → ORDER BY → LIMIT/OFFSET`.

3. **Single-op / aggregate pushdown** (`try_compile_app_sql` and the
   `compile_app` special-cases) — standalone forms like `count rel`,
   `sum (map … rel)`, `any pred rel`, `head rel`, etc.

All of these now share a **unified `Query` IR**: a `SqlQueryPlan` (the
relational spine — `tables / conditions / params / select_columns / order_by /
limit / offset`) plus a `QueryTerminal` (`Rows | Count | Aggregate | Exists |
SetOp`), rendered by one emitter (`emit_query`). The runtime side maps the
source to its physical `_knot_<name>` table, **auto-indexes** WHERE columns,
binds values as `?` parameters (never string-interpolated — injection-safe),
executes via rusqlite, and reconstructs a `Value::Relation`.

### Pushdown goes *through* function calls

Filter/`where` predicates are **beta-reduced before translation** — a named
helper whose body is itself SQL-expressible is inlined, so pushdown decides on
the inlined result. You can name and reuse filter logic without losing
pushdown. Boundaries: recursive functions are never inlined; the inlined body
must still be an allowlisted shape; a 50k-step fuel cap. (Distinct from
helper-wrapping an *aggregate head* — `myCount = \r -> count r` loses pushdown
because the head is matched pre-inlining.)

### Head recognition

Pushdown matches the operation by source-language name and accepts both the
bare and `base.`-prefixed forms (`base.count`, `base.filter`, …) via
`query_form_name`/`query_form_head`. A `Var`-only match silently misses the
`base.` form (bare names are gated — users write `base.X`). Shadowing is sound:
a user-defined `sum`/`count`/etc. is detected and run in memory instead of
misfiring SQL.

---

## What supports pushdown

### Row-returning ops

| Op | Form | SQL |
|----|------|-----|
| `filter` | `filter (\r -> p) *src`, `*src \|> filter p`, do-block `x <- *src; where p` | `SELECT … WHERE p` |
| `map` (record projection) | `map (\r -> {a: r.x, …}) *src` | `SELECT "x" AS "a", …` (with `DISTINCT` when the projection collapses rows) |
| `map` (scalar) | `map (\r -> r.f) *src` | pushed **only when a terminal aggregate/head consumes the column** (scalar relations reconstruct as records otherwise) |
| `sortBy` | `sortBy (\r -> r.k) *src` | `ORDER BY k` |
| `sortByDesc` | `sortByDesc (\r -> r.k) *src` | `ORDER BY k DESC` |
| `sortBy` / `sortByDesc` (multi-key) | `sortBy (\r -> {a: r.x, b: r.y}) *src` | `ORDER BY x, y` (record-literal key, each value a plain field; per-column type guard) |
| `take` | `take N *src` | `LIMIT MAX(CAST(? AS INTEGER), 0)` |
| `drop` | `drop N *src` | `LIMIT -1 OFFSET ?` |
| `distinct` | `distinct (map (\r -> r.f) *src)`, `distinct (map (\r -> {…}) *src)` | `SELECT DISTINCT …` |

### Aggregates / scalars

| Op | Form | SQL |
|----|------|-----|
| `count` | `count *src`, `count (filter … *src)`, `count (map … *src)`, `count (take/drop N *src)` | `COUNT(*)` (over a subquery for take/drop) |
| `countWhere` | `countWhere (\r -> p) *src` | `COUNT(*) WHERE p` |
| `sum` | `sum *src`, `sum (map (\r -> r.n) *src)` | `SUM(col)` |
| `avg` | `avg (\r -> r.n) *src` | `AVG(col)` |
| `minOn` / `maxOn` | `minOn (\r -> r.n) *src`, `maxOn …` | `MIN(col)` / `MAX(col)` |
| arithmetic map → aggregate | `map (\r -> r.a + r.b) \|> sum` | `SUM((a + b))` |

### First-row ops

| Op | Form | SQL |
|----|------|-----|
| `head` | `head *src`, `head (filter … *src)`, `head (sortBy … *src)`, `head (map (\r->r.f) *src)`, `head (take/drop N …)` | `… LIMIT 1` (plus `WHERE`/`ORDER BY`/projected column / `OFFSET`) |
| `findFirst` | `findFirst *src p`, `findFirst (sortBy … *src) p` | `… WHERE p [ORDER BY k] LIMIT 1` |
| `single` | `single *src`, `single (filter … *src)` | `… LIMIT 2` + `knot_relation_single` (errors unless exactly one row) |

### Existence / membership

| Op | Form | SQL |
|----|------|-----|
| `any` | `any pred *src`, `any pred (filter q *src)` | `EXISTS(SELECT 1 FROM … WHERE [q AND] pred)` |
| `all` | `all pred *src`, `all pred (filter q *src)` | `NOT EXISTS(SELECT 1 FROM … WHERE [q AND] NOT pred)` |
| `any`/`all` (correlated, in a WHERE) | `any (\u -> u.f == t.g) *other` | `EXISTS(SELECT 1 FROM other WHERE f = t.g)` — inner cols unqualified, outer cols table-qualified (`NOT EXISTS … NOT pred` for `all`) |
| `elem` | `elem val (map (\r -> r.f) *src)`, `elem val [lit, …]` | `EXISTS(… WHERE f = ?)` / `IN (?,?,…)` |
| `elem` (relation-derived, in a WHERE) | `elem t.f (*other \|> map (\u -> u.g))` | `t.f IN (SELECT g FROM other)` (text/bool/uuid cols only) |
| `contains` | `contains haystack needle` (in a predicate) | `INSTR(haystack, needle) > 0` |
| `startsWith` / `endsWith` | `startsWith "ap" t.title` / `endsWith "ce" t.title` | `title GLOB 'ap*'` / `title GLOB '*ce'` (GLOB, not LIKE — case-sensitive, metachars escaped) |
| `trimAscii` / `ltrimAscii` / `rtrimAscii` | `trimAscii t.title == "x"` (in a predicate) | `TRIM(title, X'20090A0D0B0C')` / `LTRIM(…)` / `RTRIM(…)` (ASCII-whitespace charset: space, tab, LF, CR, VT, FF — matches Knot's runtime exactly) |
| `byteLength` | `byteLength t.title == 5` (in a predicate) | `LENGTH(CAST(title AS BLOB))` (byte count, not char count) |
| `toAsciiLower` / `toAsciiUpper` | `toAsciiLower t.title == "x"` (in a predicate) | `LOWER(title)` / `UPPER(title)` (ASCII-only case map — byte-identical to SQLite) |
| `abs` | `abs t.a > 4` (in a predicate) | `ABS(CAST(a AS INTEGER))` (Int; both Rust `i64::abs` and SQLite `ABS` reject the min-int) |
| `intMin` / `intMax` | `intMin t.a t.b > 2` (in a predicate) | `min(CAST(a AS INTEGER), CAST(b AS INTEGER))` / `max(…)` (SQLite two-argument scalar form) |
| `clamp` | `clamp lo hi t.a == 5` (in a predicate) | `min(max(CAST(a AS INTEGER), lo), hi)` (i.e. `clamp lo hi x = min(max(x, lo), hi)`) |

These Int fns also push down inside `sum`/`minOn`/`maxOn` projections and
arithmetic (`+`/`-`/`*`/`/`) compositions, and are typed `int` in the result
column. Because Knot Ints are stored as `TEXT`, every argument is wrapped in
`CAST(… AS INTEGER)` so SQLite applies the builtin with integer ordering and
yields an integer result (otherwise `ABS("3")` → `3.0` and `min("10","9")` →
`"10"` lexicographic, diverging from the Int runtime).

### Set operations

| Op | Form | SQL |
|----|------|-----|
| `union` | `union a b` | `UNION` |
| `inter` | `inter a b` | `INTERSECT` |
| `diff` | `diff a b` | `EXCEPT` |

Each set-op composes two lowered `Rows` subqueries; the right side is
reprojected to the left's column order (SQLite matches set-op columns
positionally).

### Grouping

A `groupBy` do-block over a single source relation, yielding only its group
keys plus aggregates over the group var, compiles to a genuine fused
`SELECT key, AGG(…) … GROUP BY key`:

```knot
do
  t <- *tasks
  where t.done == 0
  groupBy { owner t.owner }
  yield { owner t.owner n (base.count t) total (base.sum (base.map (\r -> r.hours) t)) }
-- → SELECT owner, COUNT(*), SUM(hours) FROM _knot_tasks WHERE done = 0 GROUP BY owner
```

Aggregates that fuse: `count`, `sum`, `avg`, `minOn`, `maxOn` over the group
var. A `where` *after* the `groupBy` becomes a `HAVING` clause when it
references only aggregates over the group var, the group keys, and literals:

```knot
groupBy { owner t.owner }
where base.count t > 1
-- → … GROUP BY owner HAVING (COUNT(*) > 1)
```

A `groupBy` whose yield reaches into a group row (anything but keys +
aggregates), or whose `HAVING` uses a richer shape, stays on the older
in-memory path (materialize + `knot_relation_group_by`, per-group aggregates
row-by-row in Knot).

### Compositions

Pushdown also fires for compositions of the above, via three recipes:

- **In-order call-form chains** (`sortBy k (filter q *src)` written as nested
  calls) are rewritten to the equivalent pipe and reuse the pipe-chain plan.
- **Inner op folded into the outer plan** — e.g. `count (filter … *src)` →
  `COUNT(*) WHERE …`, `head (sortBy …)` → `ORDER BY … LIMIT 1`.
- **Out-of-order chains via staged `FROM (…)` subqueries** — `take |> drop`,
  `take |> map`, `drop |> take |> map`, `take |> map |> sum`, `take |> count`,
  record `map |> take/drop`. The longest pushable prefix becomes a subquery in
  the outer query's `FROM`.

---

## What is NOT pushed down (deliberately in-memory)

| Op | Why |
|----|-----|
| `fold` | Arbitrary accumulator; SQL can't express it. |
| `traverse` / `forEach` / `bind` | Effectful / monadic. |
| `match` | Pattern dispatch, not a `WHERE`. |
| `reverse` | Returns `IO`; not a query op. |
| `toUpper` / `toLower` | Unicode case-mapping diverges from SQLite's ASCII-only `UPPER`/`LOWER` (`straße` → `STRASSE` in Knot, `STRAßE` in SQLite). Use `toAsciiUpper`/`toAsciiLower` (ASCII-only) for the pushable form. |
| `trim` | Rust `str::trim` trims Unicode whitespace; SQLite `TRIM` can't express multi-byte whitespace. Use `trimAscii` (ASCII whitespace only) for the pushable form. |
| `length` (on Text) | SQLite `LENGTH` stops at NUL; Knot counts all characters. Use `byteLength` (bytes) for the pushable form. |
| `MIN`/`MAX` over non-numeric columns | Type divergence — stays in memory (`minmax_pushdown_type_ok`). |
| `sortBy` over Int projections or floats | `KNOT_INT` collation / SQL order vs `total_cmp` divergence (`sortby_projection_pushable`). A multi-key record whose **any** key column is float/tag/bool stays in memory for the whole sort. |
| ADT / nested-relation schemas (`#` / `[`) | Non-plain schemas don't map to a single flat table. |
| Views (`*view`) | Only real source tables push down; views never do. |

Decision rule for string ops: push down only when SQLite's semantics are
byte-identical to Knot's runtime (`contains`/`INSTR`: yes; case-mapping/length:
no). Where Knot's semantics are Unicode (`toUpper`/`toLower`/`trim`/`length`),
the language provides ASCII-only siblings (`toAsciiLower`/`toAsciiUpper`,
`trimAscii`/`ltrimAscii`/`rtrimAscii`, `byteLength`) whose runtime is *defined*
to match SQLite byte-for-byte — those are the pushable forms. `startsWith`/`endsWith` use `GLOB` (case-sensitive, matching Rust) rather
than `LIKE` (ASCII-case-insensitive by default), with every GLOB metacharacter
in the literal pattern escaped — so the gate holds only for a literal pattern
against a plain Text column.

---

## When pushdown refuses (safety gates)

Correctness always wins over speed. The compiler refuses to push down when:

- **Op order is non-canonical** (`pipe_ops_order_pushable`): only
  `filter* → sortBy? → map? → drop? → take? → aggregate?`. Anything else means
  something different → in-memory (or a staged subquery when it's a nested
  page composition).
- **The predicate isn't an allowlisted SQL fragment** (`try_compile_sql_expr`):
  `&&`/`||`, comparisons (incl. reversed operands and arithmetic-on-fields),
  arithmetic, field access, literals, unary ops, and a few whitelisted calls
  (`length`; `elem`/`contains` against a literal list → `IN (…)`). **Any other
  call inside a predicate kills pushdown for the whole query** → in-memory.
- **A type/collation divergence** (MIN/MAX, sortBy — see table above).
- **A non-plain schema or a view.**
- **The user shadowed the name** — the guard trio / `resolves_to_user` /
  `resolves_to_stdlib` run the user's code in memory instead.

---

## Verifying pushdown actually fired

A fallback is silent and correct, so result-correctness alone doesn't prove a
pushdown. To confirm:

- **Required-`full` marker:** an in-memory whole-table load triggers the
  required-`full` error at build time. `grep -c 'no SQL pushdown'` on the build
  output — non-zero means it fell back. (Trap: a probe that fails to typecheck
  also yields `0` — confirm the binary actually built.)
- **Inspect the SQL:** `strings <binary> | grep -oE 'SELECT …'`. Note knot
  concatenates a program's SQL onto one line — count **occurrences**
  (`grep -oE … | wc -l`), not lines (`grep -c`).
- **Baseline against master:** empty-relation aggregate panics
  (`min`/`minOn`/`maxOn`/`sum` on an empty source) are by design and can be
  pre-existing. Reproduce on a master build before claiming a regression.
