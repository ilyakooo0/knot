# Knot logging — syntax proposal (v2, with `<>`)

Structured, level-typed, lexically-scoped logging. Everything to stderr;
level filtering is the existing `--debug` flag. Context propagation uses a
new projection-collect operator `<>` that generalizes `^`. No trait system.

---

## 1. The level is a knot type

```knot
data Level = Debug {} | Info {} | Warn {} | Error {}
```

An ordinary ADT, registered as a builtin (like `Maybe`/`Result`). A
first-class value: pass it, store it, compute it, pattern-match it.

## 2. A log event is a record

```knot
base.log : Level -> {msg: Text | r} -> IO {console} {}
```

Mandatory `msg: Text` plus an **open row `| r`** — any extra fields,
type-checked per call site:

```knot
base.log (Info {}) {msg "user logged in" userId 42 ip "1.2.3.4"}
base.log (Warn {}) {msg "slow query" durationMs 2340}
```

Terminal stderr:
```
INFO user logged in  {userId: 42, ip: "1.2.3.4"}
```

Non-terminal stderr keeps the existing JSON-lines path, extended to splat
the event fields into the JSON object.

## 3. Ergonomic wrappers

```knot
base.debug = base.log (Debug {})
base.info  = base.log (Info {})
base.warn  = base.log (Warn {})
base.error = base.log (Error {})
```

`base.info {msg "..." userId 42}` reads like the old `base.logInfo "..."`,
but structured. `base.log` takes a *computed* level:

```knot
base.log (case status >= 500 of
  Bool.True {} -> Error {}
  Bool.False {} -> Info {}) {msg "request done" status status}
```

**Migration:** `logInfo/Warn/Error/Debug` are kept one release as deprecated
aliases wrapping their argument as `{msg (base.show a)}`, then removed.

## 4. Filtering — the existing `--debug` flag, no more

- `Debug` events emit only with `--debug` (today's behavior).
- `Info`/`Warn`/`Error` always emit.

No per-module config, no runtime mutation. The runtime stays as simple as the
current `AtomicBool` check. Because the level is a value, a program can gate
its own logging without runtime support (see §7 note).

## 5. Context propagation via `<>` — collect the `logCtx` field, don't project

### 5a. Why not `^`

`(^field)` requires a *unique* resolution: two in-scope records sharing a
field name is a hard **ambiguous projection** error, and re-binding the same
`with` field is rejected as **shadowing** (knot forbids shadowing). Both are
deliberate. So `^` alone cannot express "override the context in a nested
scope." (Verified by prototyping.)

### 5b. `<>name` — fold the in-scope projections, innermost-first

`<>` is the collecting counterpart to `^`. Where `^` needs exactly one match,
`<>` folds **every** match into an accumulator via a caller-supplied
function:

```knot
<>name : (acc -> { | c} -> acc) -> acc -> acc
```

**Why a fold and not a list (the decisive constraint, verified by
probing):** a knot list cannot hold differently-shaped records. The element
type is a single record shape — `[{requestId "abc"} {span "db"}]` is a hard
error ("record fields don't match"), *even under an explicit `[{ | c}]`
annotation*: each element must equal the one element type, not merely fit an
open row. So `<>` can never return the collected fragments as a list when
they have different shapes — and different shapes are the entire point of
collecting context. The fold avoids ever materializing a list: each fragment
is consumed **at its projection site, where its concrete type is known**, and
unified individually against the folder's open-row parameter. One record
against one row typechecks; two records forced into one list element does
not.

- **Reach: all enclosing `with`-scopes (rule A).** `<>` walks *every*
  enclosing scope innermost→outermost and collects the matching field from
  each — it does NOT stop at the first scope with a match. (This is where it
  departs from `^`'s finder, which early-exits at the nearest match: `^`
  wants the single closest, `<>` wants the whole nested stack so inner and
  outer context both contribute.) Every `with`-ed record carrying the field
  participates, whether or not you "meant" it as context — so the field name
  (`logCtx`) is a reserved-by-convention slot: don't put a record-typed
  `logCtx` field on a record unless you intend it as log context.
- **Name-only collection (no type filter).** `<>` collects purely by field
  name and unrolls the provided folder over every match; shape compatibility
  is enforced by the fold itself, so a mis-shaped fragment (e.g. a `logCtx`
  that isn't a record, or whose field types conflict on a shared key) fails
  the fold's own `unify`/arithmetic at its precise site — a compile error,
  not a silent skip. `logCtx` is reserved-by-convention: don't reuse it for
  anything but log context.
- **The same finder as `^`, a different final step.** `^name` works in three
  steps: (1) collect in-scope fields named `name`, (2) keep the ones that
  unify with the expected type, (3) require *exactly one*. `<>` runs (1) —
  extended to all scopes, per rule A — and (2), then changes only step (3):
  instead of demanding uniqueness, it folds the unifying candidates,
  **innermost scope first**, through the caller's function.
- **Empty folds to the initial accumulator.** No in-scope match → the fold
  returns the initial `acc` unchanged, never an error. This is what lets
  `base.log` be called with no context established (the merge is a no-op).
- **The folder sees each fragment's concrete type.** Because the fold runs
  per-fragment at the projection site, the folder is applied to each
  fragment's actual record shape, not to an open-row list element. The
  accumulator accumulates the field-union.

For logging, the folder is a record merge:

```knot
merged = (<>logCtx) (\acc frag -> base.unify frag acc) {}
```

(`base.unify` is right-biased, so folding innermost-first makes the
innermost fragment win per-field — the override semantics.)

**Typing `<>` — the operator instantiates the folder per fragment.** The
naive desugar — "build a list, hand it to a normal `fold`" — fails twice
over. First, a knot list cannot hold differently-shaped records (above).
Second, a generic folder lambda is typed *once*: `(\acc frag -> base.unify
frag acc)` has `frag` and `acc` as unresolved parameters, and `base.unify` on
unresolved record params errors ("unify expects record arguments, got a
non-record type") — the compiler does NOT trace the lambda body once per
fold iteration on its own. (Verified: the unannotated fold chain fails.)

So `<>` must drive the iteration itself. It knows each fragment's concrete
type — it collected them. For each candidate (innermost-first) it
**instantiates the folder fresh**, pinning `frag` to that fragment's concrete
record type and `acc` to the running field-union (initially the initial
value's type). Each instantiation is `base.unify` with a *known* `frag`,
which typechecks and yields the merged type. (Verified: with `frag`
annotated to each concrete shape and `acc` open, the chain
`f2 (f1 {} {span "db"}) {requestId "abc"}` compiles to the field-union
`{requestId, span}`.) The accumulator threads the union forward, so the
fold's result type is the merged context record.

`<>` is therefore a type-checker-level fold over the collected candidates —
the folder is checked N times, once per candidate — not a runtime `fold` over
a materialized list. The candidate-collection machinery is shared with `^`;
the per-candidate instantiation is the new part. The general fix that would
let a *user-written* unannotated fold over records typecheck (making deferred
`unify` wait for call-site pinning) is a separate improvement and not
required for `<>`.

### 5c. Context lives on the dictionaries you already have

Because `<>` collects the `logCtx` field from **every** in-scope record,
context attaches to whatever dictionary you've `with`-ed — no dedicated
context record required. Each dictionary carries its own `logCtx` fragment
saying "when logging inside my scope, attach this":

```knot
with { httpCtx {requestId "abc" handler "createUser" logCtx {requestId "abc"}} } (do
  with { dbCtx {pool "primary" logCtx {span "db" query "insert"}} } (do
    -- <>logCtx : [ {span "db" query "insert"}, {requestId "abc"} ]
    base.info {msg "creating user"})))
```

`base.log` folds the collected fragments **innermost-first, earlier entries
winning per-field**, with the event's own fields winning overall:

```
INFO creating user  {span: "db", query: "insert", requestId: "abc", msg: "creating user"}
```

Inside the inner `with`, `<>logCtx` is `[{span "db" query "insert"},
{requestId "abc"}]`; folded innermost-first that merges to `{span "db", query
"insert", requestId "abc"}`. Outside, only the `httpCtx` fragment is in scope.
**Override-in-a-new-context falls out of ordinary nested `with` blocks — no
intrinsic, no special form.** The `with` structure *is* the scoping, and any
dictionary can carry context by adding a `logCtx` field.

### 5d. The HTTP layer sets the base context

`serve`/`listen` wrap each handler invocation in a dictionary carrying a
`logCtx` fragment:

```knot
with { request {method <m> path <p> logCtx {requestId <fresh uuid> method <m> path <p>}} }
  (handler ...)
```

so every log inside a handler is request-scoped for free.

## 6. Everything to stderr

Terminal/JSON as today. The JSON path splats structured fields, with context
merged in by `base.log` before the sink sees the event:

```json
{"level":"info","msg":"user logged in","userId":42,"requestId":"abc123","timestamp":1710900045.0}
```

## 7. Out of scope (per constraints)

- Per-module / per-level filtering config — `--debug` only.
- Typed sinks / a distinct `IO {log}` effect — stderr under `IO {console}`.
- A `base.hasFlag`-style builtin (used for illustration in §4) is a separate,
  optional nicety, not required by this design.

---

## New surface

| Item | Form | New machinery? |
|---|---|---|
| `Level` ADT | `data Level = Debug {} \| Info {} \| Warn {} \| Error {}` | builtin registration |
| core log | `base.log : Level -> {msg: Text \| r} -> IO {console} {}` | no |
| wrappers | `base.debug/info/warn/error` | no |
| structured event | record, `msg` + open row | no (existing records) |
| context collect | `<>name : (acc -> { \| c} -> acc) -> acc -> acc` — `^`'s finder, folding all matches at their projection sites; innermost-first; no-match → initial acc | **yes — one operator** |
| context scope | ordinary `with {dict {... logCtx {...}}} (...)` — any dictionary opts in via a `logCtx` field | no (existing `with`) |
| filtering | `--debug` gates `Debug` | no (existing) |
| sink | stderr, terminal/JSON | no (existing) |

**One new language feature: `<>`.** It generalizes the existing `^`
projection from "the unique match" to "all matches, innermost-first" — and in
doing so removes the need for any context-passing intrinsic. Everything else
is ordinary knot.

## The `base.log` reference implementation (sketch)

```knot
base.log (\level event ->
  with { merged ((<>logCtx) (\acc frag -> base.unify frag acc) {}) }   -- fold context, innermost wins
    (emitStderr level (base.unify merged event)))                     -- event fields win overall
```

(`<>` folds the collected `logCtx` fragments through the caller's merge
function, each at its own projection site and concrete type — no list is ever
built, so no shared element type is needed. `base.unify` is right-biased, so
innermost-first folding makes the innermost fragment win per-field; the final
`base.unify merged event` lets the event's own fields override the context.)
