# SQL Pushdown — Current Coverage & Opportunities

## STATUS (updated during implementation)
- Delegation 1 (in flight): Feature A (arith-map→aggregate) + Feature B (map-dedup→SELECT DISTINCT). Confirmed gaps via `--debug` SQL.
- Remaining (scoped below): #1 GROUP BY, #3 elem-subquery→IN(SELECT), #5 EXISTS.

## Empirically confirmed today (via `./prog --debug`)
- A: `sum (map (\i -> i.price * i.qty) items)` → ships all rows, computes in memory. Should be `SELECT SUM("price"*"qty") FROM _knot_items`. `base.sum:[a]->a` is DIRECT; the projection is a preceding `base.map`.
- B: `map (\p -> p.dept) people` → `SELECT name,dept FROM _knot_people` + temp `SELECT DISTINCT`. Should be `SELECT DISTINCT "dept" FROM _knot_people`. Relations are sets → map already dedups; this pushes dedup+projection into SQL.

## Scoping for the hard three

### #3 elem-subquery → IN (SELECT ...)
codegen.rs:12805. `elem` has two paths: (a) literal list→`IN (?,...)`, (b) dynamic haystack→`IN (SELECT value FROM json_each(?))` (whole relation JSON-encoded as a param, gated by `elem_pushdown_ok.dynamic`, Text/Bool/Uuid only). 
GAP: when haystack is a do-block comprehension over a source (`do d <- *depts; where ...; yield d.dept`), it materializes + JSON-encodes instead of `needle IN (SELECT "dept" FROM _knot_depts WHERE ...)`. 
IMPL: detect Do-block haystack → reuse `analyze_sql_plan` to get the inner query → emit `needle_sql IN (<inner sql>)`. Keep the float/type gates. The needle and inner are on different sources — needle is the outer bind_var's column.
NOTE: in a `where` clause the haystack must be a pure relation value; comprehension-in-where needs the `with`-bound form. Verify the accepted surface syntax first (my direct `where elem p.dept (do ...)` failed to typecheck — needs binding).

### #5 EXISTS for count-guard
Shape: `where count (filter (\d -> ...) *depts) > 0` → `WHERE EXISTS (SELECT 1 FROM _knot_depts WHERE ...)`. Lower priority / less common. Only implement if cheap after #3 (similar subquery machinery).

### #1 GROUP BY + per-group aggregate (biggest, riskiest)
TODAY: groupBy pushes every row into a temp relation, closes loops (arena), then `knot_relation_group_by` reads the temp + ORDER BYs keys to group in memory. Per-group aggregates run row-by-row in Knot.
TARGET: `SELECT "key", COUNT(*)/SUM(..)/AVG(..)/MIN(..)/MAX(..) FROM _knot_src GROUP BY "key"` for the shape `t <- *src; groupBy {k: t.key}; ...aggregate over group...`.
CONSTRAINTS discovered: keys must be plain field accesses on the primary bind (codegen.rs:10803-10835 enforces this — good, maps to GROUP BY cols). The groupBy codegen is deeply tied to temp-relation + arena-promotion + loop-closing machinery (~10690-10850). A SQL path must DETECT the simple single-source shape EARLY and bypass that machinery entirely, emitting a GroupBy terminal.
EFFORT: large. New QueryTerminal::GroupBy { key_cols, agg } + runtime read-back of grouped rows (needs a schema for the {key, group-relation} shape Knot produces). RISK: high — must not break the 31 examples (several use groupBy) or the nested-relation result shape.
RECOMMEND: do A+B+#3 first, keep suite green, THEN attempt #1 in isolation with heavy example coverage.

Source of truth: `crates/knot-compiler/src/sql_lint.rs` (mirrors codegen),
`crates/knot-compiler/src/codegen.rs` (`aggregate_sql_func_runtime`,
`pipe_ops_order_pushable`, set-op terminals), `crates/knot-runtime/src/lib.rs`.

## Already pushed down (verified in codegen)
- `where` comprehensions → `WHERE` (single- and multi-relation joins →
  `FROM a, b WHERE join-cond AND filter`; auto-indexes WHERE/ORDER BY cols)
- comparisons `==` `!=` `<` `>` `<=` `>=`, `&&` `||` `not`
- arithmetic `+` `-` `*` `++`; `/` `%` only with provably-nonzero literal divisor
- `contains` → `INSTR(...) > 0`; `elem` → `IN (...)` / `IN (SELECT json_each(?))`
- `count` → `COUNT(*)`; `countWhere` → `COUNT(*) WHERE`
- `sum`/`avg`/`minOn`/`maxOn` → `SUM`/`AVG`/`MIN`/`MAX` (int/text only)
- `sortBy` → `ORDER BY` (int/text projections only)
- `take N` → `LIMIT`; `drop M` → `OFFSET`; `drop M |> take N` → `LIMIT N OFFSET M`
- set ops `union`/`inter`/`diff` → `UNION`/`INTERSECT`/`EXCEPT`
- pipe chains in the fixed stage order
  filter* → sortBy? → map? → drop? → take? → aggregate?

## Deliberate in-memory fallbacks (correctness, NOT gaps)
These stay in memory because SQLite semantics diverge from Knot — do NOT
"fix" these without changing semantics:
- **Float comparisons/ORDER BY/MIN/MAX**: SQLite -0.0/NaN-as-NULL vs Knot
  `total_cmp`. Fundamental.
- **`tag` columns (all-nullary ADTs) ordered comparisons**: SQLite compares
  constructor names alphabetically; Knot `Ord` uses declaration order.
- **json columns** (payload ADTs, nested records): runtime binds differently.
- **`toUpper`/`toLower`/`trim`/`length`**: SQLite UPPER/LOWER/TRIM are
  ASCII-only, LENGTH counts pre-NUL; Knot is Unicode-aware.
- `/` `%` with non-literal/zero-able divisor: SQLite NULLs, Knot panics.

## Real pushdown OPPORTUNITIES (ranked)

### 1. GROUP BY + per-group aggregate  ← highest value
`groupBy {key: t.field}` currently calls `knot_relation_group_by` which
materialises the whole relation, inserts keys into a temp table, and ORDER
BYs to group in memory. A following per-group aggregate
(`count`/`sum`/`avg`/`min`/`max` over each group) is done row-by-row in Knot.
SQL could do `SELECT key, COUNT(*) ... GROUP BY key` in one pass.
  - codegen.rs:1316 declares the runtime fn; no GROUP BY SQL is emitted.
  - Effort: large (groupBy has complex key/rebinding semantics — see
    `validate_group_by_references`, temp-relation machinery ~10059-10748).
  - Win: the canonical DB workload; turns O(N) row shipping into O(groups).

### 2. `distinct` / dedup → `SELECT DISTINCT`
No `DISTINCT` pushdown exists; dedup goes through `sql_dedup`
(runtime/lib.rs:6004) which round-trips rows. `SELECT DISTINCT cols`
for the common "yield a projection of unique rows" case.
  - Effort: medium. Win: moderate.

### 3. `elem x (subquery-relation)` — uncorrelated IN subquery
`elem` pushes down for literal lists and `json_each` haystacks, but an
IN-haystack that is itself a pushed-down relation comprehension could become
`... WHERE x IN (SELECT ...)`. Currently a dynamic haystack is evaluated
outside SQL row scope and shipped.
  - Effort: medium. Win: avoids shipping the inner relation.

### 4. Arithmetic projections in `map` that only feed a later aggregate
`map (\r -> r.a + r.b) |> sum` — the `+` is pushable as an atom but the
map-projection stage blocks the aggregate pushdown when it isn't the terminal.
Fold the arithmetic into the aggregate's SELECT expression.
  - Effort: small-medium (atoms already compile). Win: moderate.

### 5. `length`/`count` of a *filtered* subrelation feeding a comparison
`where count (filter ...) > 0` style guards could become `EXISTS`.
  - Effort: medium. Win: moderate, less common shape.

## Notes / constraints
- The lint (`sql_lint.rs`) MUST stay in lockstep with codegen — it
  re-implements every predicate so the "evaluated at runtime" info is accurate.
  Any new pushdown needs a mirrored lint predicate.
- Every pushdown entry point auto-indexes WHERE/ORDER BY columns
  (`ensure_indexes_for_sql`), so new pushdowns inherit index coverage.
- Float/tag/unicode fallbacks are documented in AGENTS.md/DESIGN.md as
  intentional — treat them as spec, not TODOs.
