# Plan: Sources in records with static-resolution enforcement

## Goal

Allow source declarations as fields of a record literal, so a record can group a
persisted source with its `data`/`type` and operations (a self-contained
"module"). Enforce the **static-resolution rule**: a source may only be used
where the compiler can resolve it, at compile time, to a specific declared source
with a known name + schema. This preserves the entire safety story — schema
lockfile, migrations, effect tracking, SQL pushdown.

## Core design decision (locked with user): `*` is part of the NAME, not a prefix operator

A source is a binding/field **whose name begins with `*`**. There is no `*`
prefix operator and no separate `SourceRef` atom — the sigil is folded into the
identifier, and "source-ness" is a property of the name.

- **Declaration:** a field or top-level binding named `*todos` is a persisted
  source. `db = { *todos : [Todo] }` declares a source field literally named
  `*todos`. Bare top-level `*todos : [Todo]` keeps working (a top-level source
  binding named `*todos`).
- **Read:** `db.*todos` is ordinary field access on a field named `*todos`;
  yields the relation value `[Todo]`.
- **Write:** `=` is permitted **only** on a path whose final segment is a
  `*`-named field: `db.*todos = …`. Settable-ness comes from the field *being a
  source* (name starts with `*`), resolved from the declaration — not from any
  use-site marker.
- The lexer must treat `*` followed by a lowercase ident as a single
  `*`-prefixed identifier token in field/binding position (see Parser below).

This removes the read/write ambiguity by construction: `db.todos` (a plain field)
is never a source; `db.*todos` (a `*`-named field) is always a source. The
distinction lives in the record's field names, not at the use site.

## The static-resolution rule (agreed)

A `*`-named source may only be used where the compiler can trace it to a
declaration:

- ✅ `*todos` — bare top-level source binding (today).
- ✅ `db.*todos` where `db` is a **statically-known record literal** containing a
  `*todos : [Todo]` field — resolves to that declared source.
- ❌ A source in an **opaque** position — a lambda param, a `let` bound to a
  non-literal, a function return, a list element — anywhere the compiler can't
  trace the record back to a literal. The write/`*`-field reference there is a
  compile error (not read-only).

## Design decisions (locked)

- **Path-qualified source identity.** A source's identity is its access path:
  `todos` (top-level) or `db.*todos` (record field). The qualified path is the
  key for `source_schemas` / `source_types`, the effect name (`w db.*todos`),
  the lockfile key, the migration target, and the SQLite table name.
- **Table name = `_knot_` + path verbatim (NO mangling).** The physical SQLite
  table is `format!("_knot_{}", path)` used via `quote_ident`, and quoted
  identifiers accept `.`/`*` fine. `parse_table_aliases` reads to the closing
  quote (dots/`*` inside are harmless) and `ensure_index` is already injective
  via length-prefixing. So `_knot_db.*todos` is valid; no character mangling is
  needed. Just centralize the mapping in one `table_name(path)` helper.
- **Static resolution via literal tracing.** Resolution succeeds only when the
  record expression is a literal (or traces through `let`/`with` to one), like
  implicit-dictionary records and embedded `data`/`type` are resolved today.
- **No dynamic handles.** No runtime name/schema resolution, no fallback path.
- **Reads of plain relation fields stay free.** `db.todos` (non-`*` field) is a
  normal value and flows anywhere; only `*`-named fields are restricted.
- **Duplicate source names are a hard compile error — GLOBALLY, across all
  scopes.** A source's identity is its qualified path, and that namespace is
  **flat and program-wide**, not lexical: every source maps to one physical
  table (`_knot_<path>`) and one lockfile entry, so two sources that resolve to
  the same path cannot coexist **even in non-overlapping scopes**. `*todos` in
  one record and `*todos` in a different record, one in a function body and one
  at top level, two in separate `with` blocks that never interleave — all of
  these collide, because scope does not isolate the underlying table. The check
  is a whole-program uniqueness pass over every source declaration (top-level +
  every record-literal `*`-field), erroring on any repeated qualified path and
  naming all declaration sites. Never silently overwrite a
  `source_schemas`/`source_types` entry (today a duplicate `HashMap::insert`
  would clobber). This matters more once record fields can introduce sources,
  since collisions become easier to write accidentally.

## Work items

### 1. Lexer (`crates/knot/src/lexer.rs`)
- **Language-wide rule (user-locked): binary operators REQUIRE whitespace on both
  sides.** So `a * b`, `a + b`, `x - y`, `Float M * S` are infix; an operator
  char immediately followed by a letter is never infix. This makes `*`
  unambiguous with NO lookahead: `*` immediately followed by an ASCII lowercase
  letter ⇒ source identifier; otherwise (whitespace/EOF/non-letter after) ⇒
  operator.
- Recognize `*lower` as a single `TokenKind::StarIdent(String)` where the String
  INCLUDES the leading `*` (e.g. `"*todos"`). At the `b'*'` case, peek the next
  byte: if ASCII-lowercase, lex a StarIdent (`*` + ident-continuation chars);
  else emit `TokenKind::Star`.
- Existing `Star` (binary op) uses all have spaces, so they're unaffected. The
  `*name` in top-level source decls / `replace *name` / source refs currently
  lex as `Star`+`Lower`; after this change they're a single `StarIdent`, and the
  parser sites that matched `Star`+`Lower` must match `StarIdent` instead.
- Migration: existing no-space binops must be reformatted. Grep found only
  `f-8`-style subtractions in examples/bytes.knot and examples/crypto.knot (the
  `replace *name` hits are source refs, unaffected). Run the formatter over
  affected files; the formatter should emit spaces around binops going forward.

### 2. AST (`crates/knot/src/ast.rs`)
- Field/binding names can now start with `*` (the `Name` is `"*todos"`). No
  change to `RecordField.name` (it's already a `Name` String).
- `DeclKind::Source { name, .. }`: `name` is `"*todos"` (or keep bare `todos`
  and treat "starts with `*`" as the source marker — **decide one canonical
  form**; prefer storing the name WITH the `*` so name-based dispatch is just a
  `starts_with('*')` check everywhere).
- `ExprKind::SourceRef(Name)` may become unnecessary for the record case (it's
  just `FieldAccess`/`Var` on a `*`-name), but keep it for top-level source
  reads if that simplifies effects. **Decide during impl:** likely unify so a
  source read/write is recognized by "name starts with `*`" rather than a
  dedicated node — this is the simplification the user's change enables.

### 3. Parser (`crates/knot/src/parser.rs`)
- **Record-literal field arm** (≈2914 loop): accept `*name : Type` as a field
  whose name is `*name`. Reuse the existing `type`/`data`/sig-arm structure.
- **Top-level source decl** (`parse_source_or_view` ≈1180): now keyed by the
  `*`-name directly.
- **Set/ReplaceSet** (≈2011, 2030): target is a field-access path whose final
  segment starts with `*`; produce `Set{ target, value }` where `target` is the
  path expr. Validation that the final segment is a source moves to infer (it
  can resolve the path).
- **Field access**: `db.*todos` — after `.`, accept a `StarIdent`.

### 4. Types env (`crates/knot-compiler/src/types.rs`)
- `source_schemas: HashMap<String,String>` keyed by qualified path
  (`todos`, `db.*todos`). Add a walk over top-level record-let literals
  registering each `*`-named field's source under `<record>.<field>`.
- **Global duplicate-name check (whole-program, scope-agnostic):** collect every
  source declaration — top-level `DeclKind::Source` plus every record-literal
  `*`-field reachable in the module — into one map keyed by qualified path. Any
  repeated path is a hard error naming the path and ALL declaration sites,
  regardless of whether the declarations' scopes overlap (they share one
  physical table / lockfile entry, so scope is irrelevant). Never silently
  overwrite. Run this once over the whole module, not per-scope.
- `schema_for_source` unchanged (operates on the field's type); only the key
  changes. Top-level sources keyed by bare `todos` (length-1).

### 5. Infer (`crates/knot-compiler/src/infer.rs`)
- `source_types` keyed by qualified path (populate at ≈8787/8796 + the
  record-literal walk).
- **Source read** (`db.*todos`): type `IO {Reads "db.*todos"} [Todo]`. Resolve
  the root of the path to a static record literal (through `let_bindings` /
  `with` frames / top-level literals), confirm the field is a source.
- **Write** (`Set`/`ReplaceSet` ≈6385, 6440): resolve the target path to a
  source; effect `Writes path`; refinement + set/replace distinction by path.
- **Opacity enforcement:** if a `*`-field reference's root is not statically
  resolvable (lambda param, non-literal let, etc.) → compile error: "source
  `<path>` is not statically known; sources may only be used where their
  declaration is visible." This is the single choke point for the rule.

### 6. Codegen (`crates/knot-compiler/src/codegen.rs`)
- `source_schemas`, `views`, `scalar_sources`, `source_refinements` keyed by
  qualified path.
- **Central `table_name(path) -> String`** = `format!("_knot_{}", path)`
  verbatim (no mangling — quoted idents accept `.`/`*`); replace the scattered
  `format!("_knot_{}", name)` sites. Runtime unchanged (still gets a string).
- Source read (≈3817), `Set` (≈4463), `ReplaceSet` (≈4717),
  `collect_direct_write_targets` (≈5330): resolve path → qualified name. The
  `if let SourceRef(name)` guards become path resolution + `starts_with('*')`.

### 7. Lockfile (`crates/knot-compiler/src/lockfile.rs`)
- Walk top-level record literals for `*`-named source fields, registering under
  qualified paths so `check` sees them (≈619/636/645 loops). Lockfile key =
  qualified path. Migrations match on it. Single-name sources unchanged.

### 8. Migrations / subset constraints
- `migrate` target and `<=` subset paths resolve record-literal source paths the
  same way (they already use `RelationPath`).

### 9. Formatter / LSP
- Round-trip `*name : Type` record fields and `db.*todos` access.
- LSP `analysis.rs`: update source patterns to the `*`-name / path form.

## Testing

New `crates/knot-compiler/tests/regress_source_in_record.rs` (build + run via
the real binary, following `regress_implicit_dict.rs`):
1. **Read via path:** `db = { *todos : [Todo] }`; `ts <- db.*todos` reads rows.
2. **Write via path:** `db.*todos = union db.*todos […]` persists; re-read shows
   the row. Table created under the mangled name.
3. **replace via path:** `replace db.*todos = []` empties it.
4. **with-peel (decide):** does `with db (… *todos …)` bring the source field
   into scope as bare `*todos`? Resolution must still trace to the literal.
5. **Opacity error:** `process = \db -> db.*todos = …` → compile error naming
   the unresolved source. Opaque **read** `ts <- db.*todos` through a param —
   decide allow-or-error (leaning: error too, since it's still a source).
6. **Lockfile:** build twice → drift on `db.*todos` detected; `migrate` to
   `db.*todos` validates.
7. **Effects:** a fn writing `db.*todos` infers the write effect; `main` accepts.
8. **Duplicate-name errors (global, scope-agnostic):** (a) two `*todos` fields
   in one record literal; (b) `*todos` in two DIFFERENT records whose paths
   don't overlap lexically; (c) `*todos` in a function-body record vs a
   top-level `*todos`; (d) two top-level sources with the same name — every one
   is a hard compile error naming the duplicate path and all sites, even when
   the scopes never overlap.

## Open questions to settle during implementation
- Lexer disambiguation of `*ident` vs `*` infix (spacing rule).
- Store source names WITH or WITHOUT the `*` in the various maps (canonical
  form). Prefer WITH for a uniform `starts_with('*')` dispatch.
- `with db` peel of a source field: support or require the qualified path.
- Opaque reads of `*`-fields: allowed or also static-only.

## Verification
- `cargo build --workspace` clean.
- New regression suite green.
- Full `cargo test --workspace` green (watch lockfile, effects, SQL pushdown,
  codegen source guards).
- Ad-hoc end-to-end script building + running the examples against the binary.
