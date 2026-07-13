# Bug Audit Findings

Full-codebase audit performed 2026-07-06 at commit `4336a72af5ff` (12 parallel static-review
passes over all four crates, ~121k lines). Every finding below was traced through the actual
code paths; line numbers refer to that commit. Findings are ranked by severity tier and
carry a confidence level. Check items off as they are fixed.

Cross-corroborated findings (discovered independently by two reviewers) are marked ⁂.

---

## Tier 1 — Memory safety & silent wrong-code in compiled programs

- [x] **B1. `groupBy` phase-1 use-after-free with non-equi-join multi-bind** — `crates/knot-compiler/src/codegen.rs:9680`
  In a multi-bind do-block whose primary bind is materialized inside an enclosing loop (cross
  join, non-equi `where c.pid < p.id`, or compound condition not matched by `match_equi_join`),
  phase 1 pushes the current row into the temp relation via `knot_relation_push` **without**
  `knot_arena_promote`. Closing the outer loops then runs `knot_arena_reset_to(mark)` per
  iteration, dropping those rows before `knot_relation_group_by` reads them. The ordinary yield
  path promotes for exactly this reason (codegen.rs:9942); the temp push does not. The hash-join
  path is accidentally safe, which masks the bug for plain equi-joins; all existing groupBy tests
  are single-bind.
  *Scenario:* `do { p <- *parents; c <- *children; where c.pid < p.id; groupBy {k: c.kind}; yield {kind: c.kind, n: count c} }`
  with ≥2 parent rows → grouping keys read from freed `Value`s → garbage groups or crash.
  **Confidence: high** (mechanism confirmed by direct read).

- [x] **B2. ADT relation writes serialize fields with the *first* constructor's column type** — `crates/knot-runtime/src/lib.rs:12410` (`adt_row_to_params`)
  The write loop uses `adt.all_fields[i].ty` (which keeps the first constructor's `ColType` per
  lib.rs:10764), while all read paths decode with the per-constructor type. Conflicting field
  types across constructors are an explicitly supported case (BLOB affinity, lib.rs:10769).
  *Scenario:* `data Event = Meta {info: {n: Int}} | Flag {info: Status}` → writing `Flag {info: Active {}}`
  stores full JSON where a bare tag is expected; read-back yields a corrupted constructor tag and
  `case` arms stop matching. Reversed constructor order discards payloads or panics the write.
  Affects `knot_source_write`, `_append`, `_diff_write`, `_migrate`.
  **Confidence: high** (confirmed by direct read).

- [ ] **B3. User functions shadowing stdlib names silently discarded by codegen (infer uses the user's definition)** — `crates/knot-compiler/src/codegen.rs:1478`, `crates/knot-compiler/src/codegen.rs:2631`
  `collect_declarations`/`define_functions` skip any `Fun` whose name is already in
  `user_fns`/`stdlib_fns` (`filter`, `length`, `sign`, `race`, …), but type inference binds the
  user's declaration after `register_builtins`, so the program typechecks against the user's
  semantics and runs the stdlib's — no diagnostic. Contradicts compile_app's documented rule
  (codegen.rs:5463: "A user-defined top-level function shadows any same-named builtin").
  *Scenario:* `length = \xs -> count xs` typechecks as `[a] -> Int` but every call runs
  `knot_text_length` → runtime panic; `sign = \x -> ...; println (show (sign 3))` prints a curried
  Ed25519 closure's source text.
  **Confidence: high** (confirmed by direct read).

- [ ] **B4. Bare references to user declarations named like zero-arg builtins are hijacked** — `crates/knot-compiler/src/codegen.rs:4036`
  The bare-Var arm checks the zero-arg builtin special cases (`now`, `randomFloat`, `randomUuid`,
  `generateKeyPair`, `generateSigningKeyPair`, `readLine`, `retry`) **before** the `user_fns`
  lookup; these names are not in `stdlib_names`, so the user's declaration is compiled but never
  referenced. The applied-call path checks `user_fns` first, so only bare references diverge.
  *Scenario:* `now = 5; main = println (show (now + 1))` typechecks as Int, emits `knot_now_io` →
  `knot_value_add(Value::IO, Int)` → runtime panic.
  **Confidence: medium.**

- [ ] **B5. `let x = atomic (...)` executes the transaction at let-binding time** — `crates/knot-compiler/src/codegen.rs:8395` (Let arm) + `codegen.rs:4787` (Atomic arm)
  The IO-do Let arm's deferral assumes `compile_expr` yields a deferred IO for effectful
  expressions, but the `Atomic` arm is inherently eager (emits the savepoint/retry loop inline) —
  contradicting the arm's own comment at codegen.rs:8140 ("Writes and atomic blocks must not run
  at `let` time").
  *Scenario:* `let bump = atomic do {...}; bump; bump` increments once (at the `let`); each `bump`
  statement re-runs `knot_io_run` on the already-computed result — a no-op.
  **Confidence: high.**

- [ ] **B6. `set *rel = union *rel <do-block>` passes a deferred IO thunk to `knot_source_append`** — `crates/knot-compiler/src/codegen.rs:4540`
  The append fast path compiles the "new rows" sub-expression with plain `compile_expr`; a
  do-block binding from a source is classified IO (`is_io_do_block`), so `compile_io_do` emits a
  `Value::IO` thunk which the runtime rejects: panic `"source_append expects a Relation, got …"`.
  Also fires through `beta_reduce`-inlined `let` bindings of do-block snapshots.
  *Scenario:* `*log = union *log (do { i <- *items; where i.v > 1; yield i })` → typechecks,
  panics at runtime.
  **Confidence: high.**

- [x] **B7. Do-blocks over a generalized monad type variable silently get Relation dispatch** — `crates/knot-compiler/src/infer.rs:9965` (with desugar.rs:1606, codegen.rs:6341)
  When the `__bind` span's monad var is unresolved at `monad_kind_of` time (let-generalized
  monad-polymorphic function), the `_ => MonadKind::Relation` default applies with no diagnostic;
  codegen emits `knot_relation_bind`, which panics or produces garbage when applied to `Maybe` —
  the exact polymorphic case the trait-based desugaring exists to support.
  *Scenario:* `firstAdult = \m -> do { u <- m; where u.age >= 18; yield u }` applied to
  `Just {...}` → runtime panic on a program that typechecks.
  **Confidence: high.**

- [x] **B8. Time-unit sugar swallows imported values and trait-method names** — `crates/knot/src/parser.rs:2734` (`maybe_time_unit`) + `parser.rs:259` (`scan_top_level_names`)
  Suppression covers locally-bound vars and column-0 top-level declarations, but not import items
  or trait/impl method names.
  *Scenario:* `import ./time (ms)` then `g = f 2 ms` → parses as `f (2 * 1)`, silently dropping
  the `ms` argument (arity changes). Same for trait methods named `seconds` etc. Wrong AST, no
  diagnostic.
  **Confidence: high.**

- [x] **B9. Nullary constructors exist in two runtime representations that don't compare equal** — `crates/knot-runtime/src/lib.rs:6601` (values_equal), `lib.rs:7221` (compare panic), `lib.rs:6416` (hash tags)
  `Constructor(tag, Unit)` (bare `Nothing`, `knot_relation_single`, SQLite `tag`-column reads,
  temp-table round-trips through `union`/`diff`/`inter`, JSON `tag` coercion) vs
  `Constructor(tag, Record([]))` (`Nothing {}` / `Red {}` via `knot_record_empty`, `make_nothing`).
  `values_equal` has no Unit≡empty-Record case (falls to `_ => false`), hash writes different tag
  bytes (Unit=5, Record=6), `compare_values` panics on the mixed pair. `show` prints both
  identically, hiding the mismatch.
  *Scenarios:* `single [] == head []` → `false`; `elem (Active {}) (map (\r -> r.status) dbRows)`
  → always `false` for enum columns; `union [Red {}] dbRows` keeps a duplicate `Red`;
  `min (head xs) (single ys)` aborts with "cannot compare Record with Unit".
  **Confidence: high** (confirmed by direct read).

---

## Tier 2 — Type-system soundness

- [ ] **B10. IO effect-widening rebinds an already-bound variable, laundering effects** — `crates/knot-compiler/src/infer.rs:1863` (`unify_dir` widening arm)
  The widening path fires when either side was *syntactically* a `Ty::Var` even if already bound
  to a concrete closed `IO`, then overwrites the chain-end binding with the widened effect set.
  Obligations discharged against the old narrower binding are never revisited.
  *Scenario:* `g : IO {} {} -> IO {} {}`; `f = \act -> do { let safe = g act; let x = if true then println "hi" else act; safe }`
  → `f : IO {console} {} -> IO {} {}`; `main = f (println "boom")` is typed `IO {} {}` while
  performing console IO.
  **Confidence: high.**

- [x] **B11. Duplicate field names in a record pattern leave the earlier binder unconstrained** — `crates/knot-compiler/src/infer.rs:5745` (`check_pattern` Record arm)
  The parser accepts duplicates; `check_pattern` inserts into a `BTreeMap`, so the second insert
  overwrites the first field's type — the first binder keeps a never-unified fresh var usable at
  any type. Record literals get a duplicate-field diagnostic (infer.rs:4273); patterns don't.
  *Scenario:* `case r of {x: a, x: b} -> toUpper a` with `r = {x: 1}` → type confusion / runtime
  panic.
  **Confidence: high.**

- [ ] **B12. A trait bound on an annotated function disables unit-of-measure soundness** — `crates/knot-compiler/src/infer.rs:8574`
  In the `has_constraints` branch of `infer_declarations`, the scheme is rebuilt from the
  annotation with fresh vars, but captured `DeferredUnitBinop`s reference the body-check skolems,
  which never occur in the rebuilt type. On instantiation the binop floats free and end-of-
  inference resolution degrades to a vacuous unify.
  *Scenario:* `unit M; scale : Num a => a -> a -> a; scale = \x y -> x * y` →
  `(scale 3.0<M> 4.0<M>) + 1.0<M>` accepted with the `M²` product typed `Float<M>`. Without the
  constraint the same program is correctly rejected.
  **Confidence: high.**

- [x] **B13. Exhaustiveness checking silently skipped for refined ADT aliases** — `crates/knot-compiler/src/infer.rs:5887`
  A refined alias (`type Warm = Color where …`) stays nominal as `Con("Warm", [])`, stored only in
  `refined_types`; `check_exhaustiveness` does `data_types.get("Warm")` → `None => return`, no
  wildcard requirement. (`resolve_refined_base` exists but is not consulted here.)
  *Scenario:* `f : Warm -> Int; f = \w -> case w of Red _ -> 1` compiles clean; a `Green` value is
  a runtime match failure.
  **Confidence: high.**

- [x] **B14. Open-variant values bypass the refined-type introduction guard** — `crates/knot-compiler/src/infer.rs:839` (`is_concrete_refinement_base`)
  The guard's concrete-base list omits `Ty::Variant`. Constructor patterns type scrutinees as open
  variants `<Ctor {} | r>`, which unify straight through `resolve_refined_base` with no `refine`
  and no runtime validation (function args are only trusted statically).
  *Scenario:* inside `case c of Red {} -> f c` where `f : Cold -> Int` and
  `type Cold = Color where \c -> …` — `Red` flows into `Cold` although the predicate rejects it.
  The direct `Con("Color")` form is correctly rejected.
  **Confidence: high.**

- [ ] **B15. IO inside `atomic` escapes the effect gate via a bare builtin reference in a record field** — `crates/knot-compiler/src/effects.rs:1406` (`reachable_io_lambda_from`)
  The opaque-callee backstop only detects IO inside `Lambda` nodes; a bare builtin reference is
  pure in the Var arm, and infer's `Atomic` arm never unifies the body's effect row with empty.
  *Scenario:* `r = {fn: println}; atomic (do { rows <- *items; _ <- r.fn "boom"; yield rows })`
  passes all gates; `println` runs inside the savepoint.
  **Confidence: high.**

- [ ] **B16. Impl-method / trait-default params are not pushed as shadowing binders during effect analysis** — `crates/knot-compiler/src/effects.rs:458` (also 469–479, 533–543, 616–622)
  A parameter named after an IO builtin or effectful decl poisons the method's inferred effects.
  *Scenario:* `impl Pretty Int where pretty now = show now` infers `{clock}` → correct annotations
  fail "inferred effects exceed declared effects"; a param named `race` triggers the
  "`race` cannot be used inside atomic" error on pure code.
  **Confidence: high.**

- [ ] **B17. Closed-row callee absorbs a callback's relation effects without validating them** — `crates/knot-compiler/src/effects.rs:768`, `effects.rs:823`
  A `fixed_row` callee swallows its callback argument's reads/writes.
  *Scenario:* `leak = runCb (\n -> *secrets)` infers `{}`; the dishonest annotation
  `leak : IO {} [Item]` passes and the honest one warns "declared effects are not used".
  **Confidence: medium.**

- [ ] **B18. Destructuring `let` records no latent-effect entry** — `crates/knot-compiler/src/effects.rs:1106` vs `1128`
  `let {fn} = {fn: \u -> *items}` then `rows <- fn {}` drops `r *items` from the declaration's
  effects; r/w under-reporting has no backstop.
  **Confidence: medium.**

- [ ] **B19. Refinement validation bypassed by `migrate … using`** — `crates/knot-compiler/src/codegen.rs:3835`
  Every `set`/`replace`/view/scalar write path calls `emit_refinement_checks*` first; the
  `Migrate` arm calls `knot_source_migrate` with no check, and the runtime has no per-source
  refinement registry.
  *Scenario:* `type Nat = Int where \x -> x >= 0; *scores : [{v: Nat}]` +
  `migrate *scores using \r -> {v: r.v - 100}` persists negative values into a `Nat` column.
  **Confidence: medium.**

- [ ] **B20. Refinements under a type application are dropped from `source_refinements`** — `crates/knot-compiler/src/types.rs:605` (`value_predicates`, no `TypeKind::App` arm)
  *Scenario:* `*people : [{age: Maybe Nat}]` → `set *people [{age: Just {value: -5}}]` commits
  with no violation.
  **Confidence: high** on path, **medium** on intent.

- [ ] **B21. Annotation aliases with free type variables are order-dependent** — `crates/knot-compiler/src/infer.rs:3641` + `infer.rs:6970`
  Fresh vars minted for alias-body free vars are not added to `annotation_vars`, so the
  pre-registered scheme leaves them unquantified and shared across call sites until the decl is
  re-generalized (never, for constrained functions).
  *Scenario:* with `type Box = {val: a}` and `f : Box -> Box` declared *after* `main`,
  `f {val: 1}` pins the alias var to Int; `f {val: "s"}` errors. Declaring `f` first compiles.
  **Confidence: medium.**

- [ ] **B22. Declared `r1 \/ r2` effect unions rejected for `if` in infer position** ⁂ — `crates/knot-compiler/src/infer.rs:4545`
  The If arm's row merge calls `unify(Var r1, Var r2)` directly on two rigid skolems, bypassing
  the `effect_union_sanctions` escape that `unify_io_effects` (infer.rs:2553) and
  `merge_do_io_row` (infer.rs:6201) provide → "cannot unify rigid type variables".
  *Scenario:* `pick : IO {| r1} {} -> IO {| r2} {} -> IO {| r1 \/ r2} {}` with body
  `do { let z = if c then x else y; z }` is rejected; the direct-body `if` form passes.
  Found independently by both inference reviewers.
  **Confidence: medium-high.**

---

## Tier 3 — Feature-level wrong results & crashes

### Relations / SQLite runtime

- [ ] **B23. Uniqueness trigger panics on idempotent re-append** — `crates/knot-runtime/src/lib.rs:14424` (trigger) + `lib.rs:12856` (`knot_source_append`)
  The BEFORE-INSERT trigger's `RAISE(ABORT)` fires before `INSERT OR IGNORE` dedup (SQLite does
  not suppress trigger aborts under `OR IGNORE` — verified against a real SQLite). The trigger's
  WHEN needs to exclude full-row matches.
  *Scenario:* `*users <= *users.email` + startup `set *users = union *users [{email: "admin@x", …}]`
  → first run succeeds, **second run of the same binary panics** on a set-semantics no-op.
  **Confidence: high.**

- [ ] **B24. Referential-integrity trigger rejects transitionally-valid full replaces** — `crates/knot-runtime/src/lib.rs:14545` + `lib.rs:12768`
  `replace`/`set` compile to DELETE-then-reinsert; the BEFORE-DELETE trigger aborts if any deleted
  row is referenced, even when the replacement re-inserts every referenced key — enforcement is
  per-statement, not on the final state the savepoint would commit (DESIGN.md says "validated at
  runtime before each write commits").
  *Scenario:* `*orders.customer <= *people.name`; `replace *people = [{name:"alice"}, …]` with
  alice referenced and re-inserted → panic.
  **Confidence: high** on mechanism, **medium** on intended contract.

- [ ] **B25. Reading a view that exposes a nested relation column crashes** — `crates/knot-runtime/src/lib.rs:14797` (`knot_view_read`)
  Uses `parse_schema`, which silently discards `field:[…]` nested descriptors, so the SELECT omits
  the column; first access of the field panics "field not found". The write side
  (`knot_view_write`, lib.rs:14992) fully handles nested child tables; nothing compiler-side
  rejects such views.
  **Confidence: medium.**

- [ ] **B26. Child-table naming can collide with an unrelated source's main table** — `crates/knot-runtime/src/lib.rs:11105` vs `lib.rs:11520`
  Source `users` with nested field `archive` and an independent source `users__archive` share the
  table name `_knot_users__archive`; second initializer inherits the other's columns (no-op
  `CREATE TABLE IF NOT EXISTS`) → "no such column" errors or intermixed rows. The delete cascade
  already works around this exact ambiguity (lib.rs:13368 cites the example), but create/read/
  write have no collision detection.
  **Confidence: medium** (exotic naming, certain mechanism).

- [ ] **B27. `minOn`/`maxOn` on legacy BigInt-era TEXT values silently degrades to lossy Float** — `crates/knot-runtime/src/lib.rs:11910` (`knot_source_query_value`)
  Inconsistent with `read_sql_column` (lib.rs:10996), which deliberately panics with a clear
  migration message for the same data.
  **Confidence: medium** (legacy databases only).

### sortBy (three-way inconsistency, found independently by three reviewers ⁂)

- [ ] **B28. `sortBy` SQL pushdown accepts tag/bool keys and orders them contrary to declared `Ord`; the in-memory path panics on the same keys** — `crates/knot-compiler/src/codegen.rs:13073` (`sortby_projection_pushable`), `crates/knot-compiler/src/sql_lint.rs:1271`, `crates/knot-runtime/src/lib.rs:5970` (`compare_values_primitive`)
  The pushdown gate excludes only float and Int-CASE keys, though `minmax_pushdown_type_ok`
  (codegen.rs:13027) and `try_compile_sql_comparison` (codegen.rs:11830) both treat tag ordering
  as unsound. Meanwhile `sortBy` carries no `Ord` constraint (infer.rs:7730), and the runtime sort
  panics on Bool/Bytes/ADT keys that typecheck (`minOn`/`maxOn` are protected by `Ord b`;
  `sortBy` is not — and Bool/Bytes order fine via `compare_values`, so the panic there looks
  unintended).
  *Scenario:* `data Status = Todo | Doing | Done deriving (Ord)`;
  `sortBy (\t -> t.status) *tasks` pushes down → alphabetical order (Doing, Done, Todo)
  contradicting declared Ord; the semantically identical non-pushable form panics
  "cannot compare Constructor…". `sortBy (\u -> u.active)` (Bool) panics in memory, sorts in SQL.
  **Confidence: high.**

### Stratification / schema resolution / lockfile

- [ ] **B29. Self-recursive view escapes stratification and panics at runtime** — `crates/knot-compiler/src/stratify.rs:588`
  A 1-node positive self-loop view (`*v = do { r <- *v; … }`) is excluded by
  `scc.len() >= 2 && !scc_has_negative` and isn't negative, yet only Derived has fixpoint codegen;
  `analyze_view` records the view as its own base source → runtime "no such table `_knot_v`"
  instead of a compile error (2-node view cycles are caught).
  **Confidence: medium.**

- [ ] **B30. Non-monotone recursion through aggregates passes stratification** — `crates/knot-compiler/src/stratify.rs:92`
  Only `diff` creates negative edges; `where count self == 0` compiles to a fixpoint that
  oscillates and panics "did not converge after 10000 iterations".
  **Confidence: medium.**

- [ ] **B31. Negation laundered through a user wrapper defeats the polarity check** — `crates/knot-compiler/src/stratify.rs:131`
  `minus = \a b -> diff a b`: `collect_edges` never expands function bodies (unlike codegen's
  `beta_reduce`), so `self` is collected positive → unstratifiable program compiles, panics after
  10000 iterations. Adjacent laundering forms (let-aliases, partial application, pipes) were
  hardened.
  **Confidence: medium.**

- [ ] **B32. Type alias applying a later-declared parameterized data type resolves to `unknown`** — `crates/knot-compiler/src/types.rs:875` (fallthrough at :981)
  `type Wrapped = [Box Int]` before `data Box a = …` silently yields a `_value:text` schema
  (re-resolve can't repair; application structure lost); inference accepts the program, so the
  wrong table shape ships.
  **Confidence: high.**

- [ ] **B33. Lockfile classifies the first nested-relation field as Safe, but the runtime refuses it** — `crates/knot-compiler/src/lockfile.rs:199` + `crates/knot-runtime/src/lib.rs:11416`
  Adding the first nested field needs an `_id INTEGER PRIMARY KEY` on the parent, which
  `ALTER TABLE` can't add → compile succeeds, program panics at startup; the lockfile was already
  rewritten, so a later compile with a migrate block sees `Identical` and skips chain validation.
  **Confidence: high.**

- [ ] **B34. Nested child-table schemas compared as opaque type strings → spurious Breaking** — `crates/knot-compiler/src/lockfile.rs:95`, `lockfile.rs:199`
  A pure field reorder or safe column addition inside `items: [{…}]` demands a migrate block for
  changes the runtime's name-based `auto_apply_child_change` handles fine.
  **Confidence: high.**

- [ ] **B35. Sources declared in imported modules bypass the lockfile entirely** — `crates/knot-compiler/src/main.rs:451` + `lockfile.rs:428` + `modules.rs:189`
  `generate` uses only entry-module decls; `imported_type_snippets` covers only param-less
  TypeAlias/Data. Schema changes to imported sources surface only as a runtime startup panic.
  **Confidence: high.**

- [x] **B36. Lockfile written non-atomically** — `crates/knot-compiler/src/lockfile.rs:401`
  Plain `fs::write`; a crash mid-write leaves a corrupt lockfile that hard-errors every compile
  until deleted. Use temp+rename.
  **Confidence: medium** (minor).

### Routes / fetch / serve

- [ ] **B37. `fetch` with a bare nullary route constructor is spuriously rejected** — `crates/knot-compiler/src/infer.rs:5272` (`try_infer_fetch`)
  `record_arg` falls back to the `Constructor` node itself; `infer_expr(Constructor)` returns the
  ADT type, unified against the empty expected record → "expected {}, found API".
  `fetch_ctor_name` (infer.rs:10094) and `compile_fetch` (codegen.rs:6784) both support the form;
  only inference rejects it (multi-endpoint routes only).
  **Confidence: high.**

- [ ] **B38. fetch route metadata keyed by constructor name globally; infer and codegen disagree on the winner** — `crates/knot-compiler/src/infer.rs:7060`, `infer.rs:5225` vs `crates/knot-compiler/src/codegen.rs:6794`
  Distinct ADTs may share constructor names (legal). `try_infer_fetch` uses last-wins
  (`constructors[ctor].last()`, `fetch_response_types` insert order); `compile_fetch` uses
  first-match in HashMap iteration order → a `fetch` can typecheck against route B and compile the
  HTTP call against route A; response decoded with the wrong descriptor → runtime panic on field
  access. Also `fetch_response_headers` is only inserted when non-empty, so a later headerless
  same-name entry keeps the earlier route's headers (chimera `{body, headers}` type).
  **Confidence: medium-high.**

- [ ] **B39. `monad_info` keyed by raw byte spans collides with prelude spans** — `crates/knot-compiler/src/infer.rs:4138`, `infer.rs:9829` + `crates/knot-compiler/src/base.rs:202`
  Expression-position `yield` uses its real span; the prelude (`when`/`unless`/`forEach`) contains
  such nodes at offsets that overlap user files. Last-write-wins on `monad_info.insert` → a user
  `yield` at the exact same byte offset re-dispatches the prelude's `yield` through the wrong
  Applicative (Relation instead of IO). Silent when hit; an LSP comment
  (knot-lsp/src/analysis.rs:1892) confirms prelude spans leak.
  **Confidence: medium** (rare trigger, certain mechanism).

### Concurrency / HTTP runtime

- [ ] **B40. Nested `race` is un-cancellable; outer cancellation never propagates** — `crates/knot-runtime/src/lib.rs:8200`
  The race parent parks on the outcome condvar without consulting `current_cancel_token()`, and
  inner workers get fresh tokens tied to nothing. `sleep` and STM wait were special-cased for
  cancellation; the race-parent wait was not.
  *Scenario:* `race (pure x) (race (sleep 3600000) (sleep 7200000))` resolves instantly but
  `knot_threads_join` blocks program exit ~1 hour (all three threads counted in ACTIVE_FORKS).
  **Confidence: high.**

- [ ] **B41. HTTP worker leaks the deep-cloned handler tree if per-request `knot_db_open` panics** — `crates/knot-runtime/src/lib.rs:16226`
  `knot_db_open` panics via `.expect` (lib.rs:10431/10440/10442); the call sits after the
  body-read error paths but before the `catch_unwind`, with no drop guard for `handler` — unlike
  `knot_fork_io` (IoDropGuard, lib.rs:7886) and the race worker (lib.rs:8036), which were hardened
  against exactly this window. Client sees a connection reset instead of a 500; sustained traffic
  under a persistent DB failure grows memory without bound.
  **Confidence: high** (mechanism), **medium** (real-world trigger).

- [ ] **B42. Listen response-body cap checked only after the full body is materialized** — `crates/knot-runtime/src/lib.rs:16643`, `lib.rs:16767`
  The comment claims the cap prevents a multi-GB relation from OOMing the server, but
  `json_encode_value` builds the complete string first; `json.len() > max` runs afterwards. The
  inbound path and `fetch_read_capped_body` cap during streaming; this is a post-hoc send-guard,
  not a memory bound.
  **Confidence: high.**

### IO / value runtime

- [ ] **B43. IO-do bind from an inline desugared comprehension binds the whole relation instead of iterating** — `crates/knot-compiler/src/codegen.rs:8334` vs `codegen.rs:8434`
  `rhs_iterates` checks `List`/known-relation/relation-var but not
  `desugared_monad_kind(expr) == Relation` (which the Let arm consults). Infer types the pattern
  as the ELEMENT type.
  *Scenario:* `main = do { x <- do { a <- [1,2,3]; yield a }; println (show x) }` prints
  `[1, 2, 3]` once instead of three lines; a field access on `x` would panic.
  **Confidence: medium.**

- [ ] **B44. Pipe `|> match Ctor` bypasses local shadowing of `match`** — `crates/knot-compiler/src/codegen.rs:4317`
  Unlike compile_app (env-locals first, :5487) and the non-pipe `match` form
  (`user_shadows_special`, :5741), the pipe arm fires on the name before any shadow check.
  **Confidence: medium** (contrived trigger).

- [ ] **B45. `show`/`println`/`toJson` still call-stack-recursive on constructor spines** — `crates/knot-runtime/src/lib.rs:7448`, `lib.rs:7291`, `lib.rs:15683` (also `value_to_json_with`, `value_contains_nonfinite_float` at :5650)
  The recursive-spine class fixed for hash/eq/compare in commit 2e7620d still applies here: a
  ~50–100k-node `Cons` spine passes eq/hash then SIGSEGVs on `show`/`toJson`.
  **Confidence: medium** (mechanically certain; depth-dependent).

- [x] **B46. Compile-time-constant override path bypasses `-0.0` canonicalization** — `crates/knot-runtime/src/lib.rs:3375`
  Allocates `Value::Float` directly instead of `alloc_float`, producing the only reachable `-0.0`;
  `compare_values` uses `total_cmp`, so `threshold < 0.0 && threshold == 0.0` both hold.
  One-line fix.
  **Confidence: medium** (narrow trigger, certain invariant break).

- [x] **B47. NaN has three disagreeing equality notions** — `crates/knot-runtime/src/lib.rs:6544` vs `lib.rs:6611`/`lib.rs:7161`
  Scalar `==` says two NaNs differ; relation membership (hash canonicalization) says they're the
  same element; `compare` says EQ. Design call needed on which is canonical.
  **Confidence: medium.**

### Parser / formatter

- [ ] **B48. `Server Api {console}` type annotation is unwritable** — `crates/knot/src/parser.rs:4386`
  A trailing `{console}` type atom always parses as an `Effectful` prefix requiring a body type →
  "expected type". The type the checker itself displays for `serve` values cannot be written back;
  only `Server Api _` parses.
  **Confidence: medium.**

- [ ] **B49. `knot fmt` silently no-ops whole files when lowercase unit literals lose parens** — `crates/knot/src/format.rs:1705`
  `f (999<usd>) 6` renders as `f 999<usd> 6`, which reparses as `(f 999 < usd) > 6`; the AST
  round-trip safety net then reverts the entire file with no message. Uppercase units are fine.
  **Confidence: high.**

- [ ] **B50. `knot fmt` silently no-ops files with parenthesized multi-line `do`/`case` whose next statement starts with `-`** — `crates/knot/src/format.rs:2199`, `format.rs:2041` vs `crates/knot/src/parser.rs:2330`
  Inside parens `delimiter_depth > 0` disables the layout column guard, so the formatter's
  parenthesized rendering glues `let a = 1` and `-2` into `let a = 1 - 2` on reparse → AST
  mismatch → whole-file revert.
  **Confidence: high.**

- [ ] **B51. `knot fmt` permanently rewrites time-unit sugar to raw multiplication** — `crates/knot/src/parser.rs:2750`
  `2 seconds` desugars at parse time to `BinOp::Mul(2, 1000)` with no AST marker, so
  `sleep (2 seconds)` formats to `sleep (2 * 1000)` (and `2.5 seconds` → `2.5 * 1000.0`) — passes
  the safety net, destroys surface syntax.
  **Confidence: high** (fidelity bug; semantics preserved).

- [x] **B52. Verbatim formatter fallback replaces tabs inside string literals** — `crates/knot/src/format.rs:557` (`normalize_source_slice`)
  `s.replace('\t', " ")` runs over the whole slice including strings; a decl with an internal
  comment (forcing verbatim) plus a raw-tab string changes the string value → reparse mismatch →
  whole-file silent revert.
  **Confidence: medium-high.**

- [x] **B53. Route paths reject dashed segments with keyword parts that import paths accept** — `crates/knot/src/parser.rs:701` vs `parser.rs:681`
  `/foo-type`, `/x-do`, `/a-in` fail ("expected '='…") because the route suffix helper admits only
  `Lower`/`Upper`, unlike the import version which also takes `keyword_str()`. `/do-foo` works.
  **Confidence: high** on behavior, **medium** on intent.

### Desugar / sql_lint (diagnostics-level)

- [ ] **B54. Trait-only-IO desugar gate breaks on a final bare expression** — `crates/knot-compiler/src/desugar.rs:909` vs `desugar.rs:1163`
  The gate's comment assumes the final bare expression is wrapped in `__yield`, but
  `desugar_stmts` only wraps literal `yield` syntax → `do { x <- traitIoMethod 1; show x }`
  desugars into a continuation returning `Text` instead of `IO Text` → spurious type error.
  **Confidence: medium.**

- [ ] **B55. sql_lint doesn't mirror codegen's beta-reduction → false "will be evaluated at runtime" lints** — `crates/knot-compiler/src/sql_lint.rs:1237`
  `*items |> filter (\i -> isGood i)` compiles to SQL WHERE, but the lint claims otherwise; the
  file's own tests codify the wrong claims (`lint_on_pipe_filter_complex`,
  `lint_on_complex_min_lambda`).
  **Confidence: high** (info-diagnostic impact only).

- [ ] **B56. Nested-pipe lint skip loses diagnostics when chain flattening bails** — `crates/knot-compiler/src/sql_lint.rs:142`
  The double-report fix skips `lint_expr(lhs)` whenever lhs is a pipe, but `lint_pipe_chain` only
  covers the chain on successful flattening with a plain source head; on early return the inner
  sub-chain gets zero linting, and middle-stage lambda bodies are never generically recursed.
  **Confidence: high** mechanics, info-level impact.

- [x] **B57. `length` pushdown diverges on NUL-containing text** — `crates/knot-compiler/src/sql_lint.rs:881`
  SQLite `LENGTH()` counts chars before the first NUL; `knot_text_length` counts all.
  `where length p.name == 5` filters differently pushed vs in-memory.
  **Confidence: medium** (exotic data).

- Notes (dead code / doc drift, no runtime impact):
  - `desugar.rs:567` — the Set/ReplaceSet arm in `recurse_into_children` lacks top-level-do
    protection; currently unreachable, a trap for future callers.
  - `desugar.rs:13`, `desugar.rs:1278` — `desugar_ctor_bind` is advertised in module docs but
    unreachable (`is_pure_comprehension` rejects refutable binds first).
  - `codegen.rs` — `nullable_ctors` is never populated; all NullableRole paths are dead code.
  - CLAUDE.md mentions an `ExprKind::At` variant that does not exist in ast.rs.

### CLI / build

- [x] **B58. A compile-time constant named `output` can't be overridden at build time** — `crates/knot-compiler/src/main.rs:50`
  `--output`/`--output=` is reserved by `build` (value repurposed as the output path, no
  diagnostic), yet at runtime `./app --output=x` does override the constant.
  **Confidence: medium.**

- [x] **B59. Duplicate `--name=value` flags: last-wins at build, first-wins at run** — `crates/knot-compiler/src/main.rs:71` vs runtime `knot_override_lookup`
  `--port=1 --port=2` bakes 2 at build, uses 1 at run.
  **Confidence: medium.**

- [x] **B60. `find_runtime` prefers a stale `libknot_runtime.a` next to the compiler over the newer embedded runtime** — `crates/knot-compiler/src/main.rs:502`
  No freshness/content check; after rebuilding only knot-compiler, `knot build` silently links the
  stale archive.
  **Confidence: medium.**

- [ ] **B61. build.rs ancestor walk for the runtime archive continues past the target directory to `/`** — `crates/knot-compiler/build.rs:68`
  A stray `libknot_runtime.a` in any ancestor (e.g. `$HOME`) with a recent mtime gets embedded
  (mtime-only freshness).
  **Confidence: medium** (low likelihood).

- [x] **B62. `db` explorer loads entire tables unbounded** — `crates/knot-runtime/src/tui.rs:174`, `tui.rs:636`
  `load_rows` has no LIMIT; runs before `enable_raw_mode`, so a large table makes `<program> db`
  appear frozen (or OOM), and every selection change re-scans the full table. Docs claim
  pagination; none exists.
  **Confidence: high** (deviation), **medium** (as a bug for small DBs).

---

## Tier 4 — LSP

### Code actions: paren-widened span class (one root cause, six sites)

The parser folds parentheses into the wrapped node's span (`parser.rs:2934`); these consumers
replace/measure that widened span assuming it excludes parens:

- [ ] **B63. "Convert to pipe" reverses application order** — `crates/knot-lsp/src/code_action.rs:3306`
  `g (f x)` → replaces `(f x)` with `x |> f` → `g x |> f` parses as `f (g x)`.
  **Confidence: high.**

- [ ] **B64. "Flip operands" re-associates same-precedence neighbors and drops enclosing parens** — `crates/knot-lsp/src/code_action.rs:3245`
  `a / b * c` → `c * a / b` (0 vs 2 for 1,2,4); `f (a == b)` → `f b == a`.
  **Confidence: high.**

- [ ] **B65. "Add missing case arms"/"Add wildcard arm" insert outside the closing paren** — `crates/knot-lsp/src/code_action.rs:1627`, `code_action.rs:1242`
  `show (case c of Red {} -> 1)` → arm inserted after the `)` → parse error introduced by the
  exhaustiveness quickfix.
  **Confidence: high.**

- [ ] **B66. "Negate condition"/"Convert if to case" on a parenthesized `if` in operand position swallow the trailing operand** — `crates/knot-lsp/src/code_action.rs:1368`, `code_action.rs:3161`
  `(if c then a else b) * 2` → `if not (c) then b else a * 2`.
  **Confidence: high.**

- [ ] **B67. "Extract to function" call site never parenthesized** — `crates/knot-lsp/src/code_action.rs:719`
  `show (x + 2)` → `show extracted_fn x` parses as `(show extracted_fn) x`.
  **Confidence: high.**

- [ ] **B68. Monad-context inlay hint anchored mid-keyword for parenthesized do** — `crates/knot-lsp/src/inlay_hints.rs:1151`
  Anchors at `span.start + 2` assuming the span starts at `do`; for `f (do …)` the hint renders
  between `d` and `o`.
  **Confidence: medium-high.**

### Other LSP

- [ ] **B69. "Remove unused import"/"Organize imports" delete imports used in route compositions and `rateLimit` expressions** — `crates/knot-lsp/src/code_action.rs:1695` (`collect_referenced_names`)
  No `RouteComposite` arm; the `Route` arm walks types only, never `entry.rate_limit` (also
  misses `SubsetConstraint` relation names). Applying the action breaks compilation.
  **Confidence: high.**

- [ ] **B70. Rename mid-debounce produces a partial workspace edit** — `crates/knot-lsp/src/rename.rs:633`
  A stale (pending-analysis) non-initiating open document is silently skipped and also marked
  `scanned` so the disk phase skips it too → importers renamed, owner declaration untouched.
  Should abort or retry instead.
  **Confidence: medium.**

- [ ] **B71. Rename has no capture/conflict detection** — `crates/knot-lsp/src/rename.rs:138`
  `f = \x -> \y -> x + y` rename `x`→`y` yields `\y -> \y -> y + y` — silently wrong code, no
  warning.
  **Confidence: medium.**

- [ ] **B72. Extract actions accept selections that aren't expression nodes** — `crates/knot-lsp/src/code_action.rs:589`
  Selecting `a + b` inside `2 * a + b` extracts `2 * (a + b)` — different value, no diagnostic.
  **Confidence: medium.**

- [ ] **B73. Find References misses impl-method definition tokens for trait methods (same file); returns None from the impl token** — `crates/knot-lsp/src/references.rs:157` (root cause `defs.rs:267`)
  `resolve_definitions` never links `ImplItem::Method::name_span` to the trait method; rename.rs
  compensates (rename.rs:1510), references.rs does not; cross-file importer scans DO include impl
  tokens, so behavior flips on file layout.
  **Confidence: high.**

- [ ] **B74. Field-refinement hover attributes fields to whichever decl first binds the variable name module-wide** — `crates/knot-lsp/src/hover.rs:352` (mechanism `shared.rs:1232`)
  `resolve_var_to_source` walks decls in order, ignoring the cursor's decl — with two do-blocks
  both binding `p`, hover shows the other relation's refinement (or omits the real one).
  **Confidence: medium.**

- [ ] **B75. Field-token hover suppression blind in trait default bodies, `rateLimit` exprs, `migrate using` exprs** — `crates/knot-lsp/src/hover.rs:76` (mechanism `shared.rs:1203`)
  `find_field_access_at_offset` never visits those bodies, so hovering `r.count` there leaks a
  same-named global's signature/doc.
  **Confidence: medium-high.**

- [ ] **B76. Shared inference cache is unbounded; per-task clone grows with session length** — `crates/knot-lsp/src/analysis.rs:228` (merge), `analysis.rs:614` (eviction)
  The LRU cap applies only to the worker's local clone; merge-back only inserts. Each task clones
  the whole shared map (`analysis.rs:141`). Long sessions → hundreds of MB RSS and increasing
  per-keystroke latency. (The import cache gets `enforce_import_cache_cap`; this one gets
  nothing.)
  **Confidence: high.**

- [ ] **B77. Imported-file diagnostics leak into the importer via numeric span containment** — `crates/knot-lsp/src/analysis.rs:532` (same pattern `workspace_diagnostics.rs:788`)
  Inference runs on the import-inlined module; a foreign diagnostic whose byte offsets happen to
  fall inside an importer decl span passes `anchored_in_user` → phantom squiggle at an arbitrary
  position. Same leak for `local_type_info`/`monad_info` retains (ghost hovers/inlays).
  **Confidence: medium.**

- [ ] **B78. Stale version published after a no-op didChange** — `crates/knot-lsp/src/main.rs:1284` + `main.rs:695`
  `already_pending` refreshes only `pending.version`, but `apply_analysis_result` publishes
  `result.version` (the older task's). Version-checking clients (Helix) discard the publish; the
  dedup path (`main.rs:1581`) then suppresses every subsequent identical analysis → diagnostics
  stuck until the next real edit. Fix: use the pending entry's version when the source matches.
  **Confidence: medium.**

- [ ] **B79. willRenameFiles resolves imports with `format!("{}.knot")` while everything else uses `with_extension`** — `crates/knot-lsp/src/main.rs:596`
  For `import ./lib.v2` the compiler loads `lib.knot` (`with_extension` replaces the suffix), but
  willRenameFiles resolves `lib.v2.knot` → file moves silently skip rewriting that importer.
  **Confidence: medium.**

- [x] **B80. `parse_app` drops application args when the head is a type variable** — `crates/knot-lsp/src/parsed_type.rs:372`
  `map : (a -> b) -> f a -> f b` renders in hover/signature help as `(a -> b) -> f -> f`.
  **Confidence: medium.**

- [ ] **B81. Added workspace folders dedup-checked pre-canonicalization** — `crates/knot-lsp/src/main.rs:1534`
  A symlinked spelling of an existing root is pushed again → duplicate scans (waste, not
  corruption).
  **Confidence: medium** (low impact).

- [x] **B82. `rateLimit` completion gate is end-exclusive** — `crates/knot-lsp/src/completion.rs:1024`
  Cursor at the end of the rateLimit expression (the common append position) falls back to route
  method/type completions instead of expression completions. `find_enclosing_do_span` uses
  inclusive end.
  **Confidence: medium** (low severity).

---

## Areas audited and found clean

- **STM machinery** (runtime): version-before-SELECT ordering, notify-after-commit, SeqCst fence
  pairing in `bump_table_version`/`knot_stm_wait`, conservative filter fallbacks (missing
  columns/incomparable types wake), spec parse failure → broad All filter, UPDATE pre+post row
  images, `WriteEvent::merge` cap upgrade. No lost-wakeup path found.
- **Race/fork lifecycle**: winner value Box-cloned before publication, single free, loser late
  result discarded under the outcome mutex, both-panicked paths publish, write-lock release on
  cancelled unwind, ACTIVE_FORKS ordering (modulo B40).
- **HTTP plumbing**: header case conversion both directions, CR/LF injection rejection, percent
  encoding of path/query, streaming caps on inbound and fetch paths, Retry-After math.
- **Pure value runtime**: checked arithmetic incl. `i64::MIN % -1`, `float_as_exact_i64`
  boundaries, UTF-8 char safety in text builtins, bytes bounds checks, crypto, wire-vs-db JSON
  encoder pairing at every call site, deep_clone DAG handling.
- **groupBy runtime + fixpoint**: SQLite-vs-Rust key comparison alignment (canonical Int text,
  `COLLATE KNOT_INT`), symmetric-EXCEPT convergence.
- **Codegen core**: record field ordering, closure capture symmetry, stdlib currying arg order,
  comparison lowering, short-circuit wiring, case block sealing, IO-do free-var capture/unpack
  order, atomic retry-loop block graph, STM pred spec index alignment (codegen ↔ runtime parser),
  route/rate-limit/refinement registration on both main_init and listen paths, fetch arg order,
  aggregate pushdown typing, schema descriptor generation ↔ runtime parsers.
- **Inference core**: occurs check covers all var-carrying variants, substitution recursion
  complete, scheme instantiation freshens TyVars/UnitVars/constraints/unions together, row
  unification tail re-resolution, unit exponent algebra signs, `route_input_record_ty` field
  classes, serve exhaustiveness incl. composites, `ty_to_wire_descriptor` vs runtime
  `apply_wire_type_checked`.
- **Frontend**: lexer escapes/overflow/UTF-8 spans, Pratt table ↔ formatter `Prec` mirror
  consistency, postfix-`:` commit points, no parser infinite-loop path found, diagnostic
  byte→char accounting, formatter `strip_spans` safety net soundness (why B49/B50/B52 degrade to
  silent no-ops rather than corruption).
- **LSP**: UTF-16 position↔offset conversion (surrogate pairs, CRLF, lone CR), didChange
  sequential application semantics, semantic-token delta/full/range encoding, byte-range
  diagnostic rebase boundaries, workspace scan symlink/depth caps, rename edit machinery (sigil
  narrowing, pun expansion, overlap dedup).
