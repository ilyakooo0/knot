# Implementation brief — unified value/type namespace with type-directed explicit type arguments (erased)

Scope: investigation only; no code changes. Anchors cite `crates/knot` and
`crates/knot-compiler` sources as of this writing.

## 0. Verified starting facts

- Parser emits a neutral `ExprKind::Constructor(name)` for any Upper name in
  expr position (parser.rs:2924-2927); lowercase → `ExprKind::Var`
  (parser.rs:2919-2922). No parser change needed for the *argument* side.
- Typechecker `Constructor(name)` arm: infer.rs:5064; `instantiate_ctor`
  (infer.rs:4935) returns `Some((data_ty, record_ty))` for a known data ctor,
  else the arm errors "unknown constructor" (infer.rs:5091-5096). This else
  branch is the unified-namespace hook. Confirmed today: `f = Int 1` in value
  position errors exactly there ("unknown constructor 'Int'").
- Application: infer-mode `App` arm at infer.rs:5313; generic path at
  infer.rs:5450-5457 (`arg_ty = infer_expr(arg); unify(func_ty, Fun(arg_ty,
  result))`). There is already a *directed* precedent: the higher-rank slot
  check at infer.rs:5435-5448 inspects the *function's* parameter type before
  deciding how to handle the argument (checks arg against `Ty::Forall`).
  There is NO separate check-mode `App` arm; `check_expr` falls through to
  infer+unify at infer.rs:6088-6093.
- Type binder form `\(T : Type)` is free syntax: any parenthesized
  typed-pattern is a parse error today ("expected ')' to close pattern
  group", parser.rs:3799 and 3922 — verified for both `\(T : Type)` and
  `\(x : Int 1)`). `parse_lambda` (parser.rs:3294) parses params via
  `parse_pat` (parser.rs:3709) in the loop at parser.rs:3310-3321.
- Erasure precedent: `ExprKind::TypeCtor` (ast.rs:293-297) is typed as its
  kind `Type -> ... -> Type` in infer (infer.rs:5868-5897) and erased to
  `knot_value_unit` in codegen (codegen.rs:5563-5567).
- `Ty::Con("Type", [])` is the kind marker (infer.rs:5874, 5890-5894);
  `Ty::Unit(..)` shows the "kind ≠ Type, no value inhabitants" pattern
  (types in infer.rs:376-381). A signature `f : Type -> Type` already
  typechecks today (`Type` elaborates via the default `Named` arm of
  `ast_type_to_ty`, infer.rs:4504-4506, to `Ty::Con("Type", [])`).
- `Ty::Forall(Vec<TyVar>, Box<Ty>)` exists (infer.rs:386);
  `skolemise_scheme` (infer.rs:3471) + the `check_expr` Forall arm
  (infer.rs:5912-5944) + `skolemise_forall_body` (infer.rs:1922) implement
  rigid-var scoping. Top-level `id : forall a. a -> a` works through
  pre-register (infer.rs:8114-8124 lifts outermost Forall into the Scheme).
- Inline `: forall` on an *infer-mode* lambda value is not rejected by
  syntax; it is handled (and effectively rejected via the escape check) at
  infer.rs:5801-5829 — message "polymorphic type escapes its scope … an
  inline `forall` annotation cannot make a monomorphic value polymorphic".

## 1. TYPE BINDER — `\(T : Type)` AST + parser design

Recommendation: **new `PatKind` variant**, not a lambda attribute or expr
wrapper.

```rust
// ast.rs, in PatKind (after `Var`):
/// `(T : Type)` — a lambda-only type-witness binder. Binds the *type* `T`
/// in the body; erased at runtime (no value is passed).
TypeWitness { name: Name, kind: Type },
```

Why a pattern:
- Lambda params are `Vec<Pat>` (ast.rs:218-221); every consumer already
  matches on `PatKind`, so the new form is additive at well-known sites
  (§6). A lambda-level attribute (`type_params: Vec<Name>`) would force
  positional alignment with `params` and complicate interleaving
  (`\(T : Type) x (U : Type) y -> …`); an `Expr` wrapper
  (`ExprKind::TyLam`) would duplicate the entire lambda handling in infer,
  codegen, desugar, effects, formatter, LSP — far from minimal-diff.
- Patterns already have a precedent for "binder with payload": `Constructor
  { name, payload }` (ast.rs:380).

Parser change (small, two places):
- In `parse_lambda`'s param loop (parser.rs:3310-3321), before calling
  `parse_pat`, peek for `LParen Upper Colon` — or better, add the form
  inside `parse_pat_inner`'s `LParen` arm (parser.rs:3789-3802): after
  consuming `(`, if `peek` is `Upper(_)` *and* the token after is
  `TokenKind::Colon`, parse `Name : Type )` into `PatKind::TypeWitness`.
  Putting it in `parse_pat_inner` keeps one pattern entry point, but note
  `parse_pat` is also used for case arms — the new kind must then be
  *rejected in case patterns* (typechecker: `check_pattern` arm errors
  "type-witness binder is only allowed as a lambda parameter") unless you
  deliberately want type-level case. Recommend lambda-only for v1: enforce
  by a flag param (`parse_pat(allow_witness: bool)`) or by post-parse
  validation in `parse_lambda`.
- `push_pat_vars` (parser.rs:285-309): new arm — push nothing (a witness
  binds no *value* var) or push the name into a separate
  `bound_type_vars` stack if you want the parser to know `T` is in type
  scope (not strictly needed; the typechecker can own this).

Signature surface form `\(T : Type) -> T -> T`:
- `ast::Type` does NOT need a new node for the *witness arrow itself* if
  you represent it as an ordinary `TypeKind::Function { param: Named
  "Type", result }` — i.e. the surface signature for the example is
  literally `f : Type -> T -> T`. But then `T` is an unbound lowercase var
  unless the *declaration* signature quantifies it: the natural surface
  spelling is `f : forall a. Type -> a -> a` … which is confusing.
- Recommended: add ONE new `TypeKind`:

```rust
/// `(a : Type) -> T` — explicit type-witness arrow (dependent-ish arrow,
/// erased). Binds `a` in `T`.
TyArrow { var: Name, kind: Box<Type>, body: Box<Type> },
```

  It elaborates in `ast_type_to_ty` (infer.rs:4406) to
  `Ty::Fun(Con("Type",[]), body)` with `annotation_vars[var]` bound to a
  fresh var *that is also registered as the witness* (see §2). The
  advantage over reusing `Function{Named "Type"}`: the name binding is
  explicit and scoped, and the formatter round-trips the user's syntax.
  Minimal alternative (zero new Type nodes): require users to write
  `f : forall a. Type -> a -> a` and treat the *first* `Type` argument
  positionally. Cheaper but worse UX and fragile with multiple witnesses.

## 2. TYPE-WITNESS IN SCOPE

Represent a bound `T` exactly like today's signature-bound rigid vars —
**reuse skolems + `annotation_vars`**, do not build a new mechanism:

- When checking a lambda against an expected type (the `check_expr` Lambda
  arm, infer.rs:5959-5993, which peels `Fun` arrows), a peeled param whose
  type is `Ty::Con("Type", [])` paired with a `PatKind::TypeWitness{name}`
  binder should:
  1. allocate a fresh TyVar `s`, insert into `self.skolems` (rigid),
  2. `self.annotation_vars.insert(name, s)` — this is what makes a later
     param `x : T` (or an inner annotation `(e : T)`) resolve `T` to the
     same rigid var, because `ast_type_to_ty`'s `TypeKind::Var` arm
     (infer.rs:4509-4512) goes through `annotation_var`
     (infer.rs:4698-4706). NOTE: `TypeKind::Var` requires *lowercase*
     names; for uppercase witnesses (`T`) you need `TypeKind::Named("T")`
     to consult `annotation_vars` FIRST (before aliases/data) — a small,
     contained change at infer.rs:4408: at the top of the `Named` arm, `if
     let Some(v) = self.annotation_vars.get(name) { return Ty::Var(*v); }`
     guarded by a new flag (e.g. `in_witness_scope`) so ordinary
     annotations don't start resolving type names to vars.
  3. NOT bind anything in the value scope (`self.bind`) — the witness has
     no value.
- At a call site, the witness var is just a scheme variable: when `f`'s
  scheme is instantiated (`instantiate_at`), the witness position becomes a
  fresh flexible var, and the explicit type argument unifies with it (§3).
  This is precisely how `forall a.` already scopes rigid vars through a
  body — no new scoping machinery.
- Drop the witness skolems at lambda scope exit (mirror the `for s in
  fresh_skolems { self.skolems.remove(&s); }` pattern at infer.rs:5941-5943)
  and restore the previous `annotation_vars` entry (mirror the save/restore
  in `ast_type_to_ty`'s Forall arm, infer.rs:4670-4692).
- Escape check: reuse the existing free-vars-in-env leak check
  (infer.rs:5930-5940) so a witness can't leak into the enclosing type.

## 3. ARG REINTERPRETATION (type-directed application)

The signal must live in the **function's type**: a leading parameter whose
type is the kind marker `Ty::Con("Type", [])`. That is already the
convention established by `TypeCtor` (infer.rs:5889-5896), so "this arrow
is a type arrow" is representable today with zero new `Ty` variants.

In the infer-mode `App` arm (infer.rs:5313), extend the *existing* directed
precedent at infer.rs:5435-5448 (which already inspects the function's
param type before handling the arg):

```rust
let func_ty = self.infer_expr(func);
let func_applied = self.apply(&func_ty);
if let Ty::Fun(arg_slot, ret_ty) = &func_applied {
    let arg_slot_resolved = self.apply(arg_slot);
    // NEW ARM — before the existing Forall arm:
    if matches!(arg_slot_resolved.peel_alias(), Ty::Con(n, args) if n == "Type" && args.is_empty()) {
        // The function expects a TYPE as its next argument. Reinterpret
        // the argument expression as a type.
        let ty_arg = self.expr_as_type(arg);   // see below
        self.unify(arg_slot, &ty_arg, arg.span); // or unify_dir with polarity
        return (**ret_ty).clone();
    }
    if matches!(arg_slot_resolved, Ty::Forall(..)) { …existing… }
}
```

`expr_as_type(arg)` — synthesize an `ast::Type` from the arg `Expr` and run
the existing `ast_type_to_ty`:
- `ExprKind::Constructor(name)` → `TypeKind::Named(name)` (aliases,
  parameterized data (`TyCon`), and plain `Con` all handled by the existing
  `Named` arm, infer.rs:4408-4508).
- `ExprKind::Var(name)` → `TypeKind::Var(name)` — resolves to an in-scope
  witness via `annotation_vars` (needs the §2 guard so lowercase works too,
  or map Var→Named for uniformity).
- `ExprKind::App{func, arg}` (e.g. `f (Maybe Int)`) →
  `TypeKind::App{func: expr_as_type(func), arg: expr_as_type(arg)}` —
  `ast_type_to_ty`'s App arm (infer.rs:4532-4562) handles `Con` arg
  accumulation, `TyCon`/`Var` HK application, and `expand_param_alias`
  (infer.rs:4353) already peels App spines.
- Anything else → error "expected a type expression".

Important subtleties:
- `Int` / `Float` special case: the `Named` arm *errors* on bare `Int` in
  annotation mode (infer.rs:4409-4416: "bare `Int` requires a unit").
  `expr_as_type` should set `in_type_annotation = true` (consistent with
  how annotations behave) — so the explicit type arg for a dimensionless
  int is written `Int 1`, which parses in expr position as
  `App(Constructor("Int"), Lit(1))`. Handle by mapping that App shape to
  `TypeKind::UnitAnnotated` (ast.rs:498-501) when func is `Named
  "Int"/"Float"` and arg is `Lit(Int("1"))` — or more generally route
  through `parse_unit_type_arg` semantics. This is the main
  `Int 1`-collision risk (§7) and is solvable entirely inside
  `expr_as_type`.
- Also wire the same check into the two early-return App paths that bypass
  the generic path if they can precede a witness slot: the ctor-application
  path (infer.rs:5358-5364) and the lambda-last path (5402-5427). The
  lambda-last path triggers on `Var` heads with 2-arg signatures —
  `f Int (\x -> x)` where `f : Type -> (Int 1 -> Int 1) -> …` would hit it;
  make `takes_two_args` peeling (infer.rs:11474-11478) witness-aware or
  exclude functions whose first param is kind `Type`.
- Erasure of the arg: because the new App arm returns before ever calling
  `infer_expr(arg)` on the type argument, the argument's inferred-type
  record never binds a value type for it. For codegen, mark the arg span in
  a side set (`self.type_arg_spans.insert(arg.span)`) so codegen's
  `compile_app` can skip emitting it (§5). Record the *witness→type*
  binding (`(func.span, TyVar) → Ty`) in another side table
  (`witness_bindings`, same pattern as `show_calls` infer.rs:5472-5475 and
  `traverse_calls` 5480-5486) if codegen ever needs the resolved type
  (e.g. for runtime type-directed dispatch later). For pure erasure, the
  span set alone suffices.

## 4. UNIFIED NAMESPACE — `Constructor(name)` else-branch

Change infer.rs:5087-5096 from unconditional error to:

1. If `instantiate_ctor` succeeded → existing value behavior (unchanged).
2. Else, if the name resolves as a *type* in scope — i.e. any of:
   - `self.aliases.contains_key(name)` or
     `self.param_aliases.contains_key(name)` (alias / parameterized alias),
   - `self.data_types.contains_key(name)` (a data type name used as a
     type — note `instantiate_ctor` looks up *constructor* names in
     `self.constructors`, which is keyed by ctor name, so a data type whose
     ctor names differ from the type name lands here),
   - `self.annotation_vars` contains `name` under the §2 witness flag,
   - `name` is `Int`/`Float`/`Text`/`Bool`/`Bytes`/`Uuid` (builtin types),
   then return the kind type `Ty::Con("Type", [])` (or `Type -> … -> Type`
   for a parameterized head, mirroring the fold at infer.rs:5889-5895) and
   record `(expr.span → name)` in a side table so codegen erases this
   occurrence to unit (exactly like `TypeCtor`, codegen.rs:5563-5567).
3. Else keep the "unknown constructor" error.

This makes `map Int [1,2]` (type-witness argument position) work through §3
*and* lets a bare type name appear wherever a kind-`Type` value is
expected.

Precedence when a name is BOTH a data ctor and a type (e.g. `data Box a =
Box {value: a}` — `Box` is both `self.constructors["Box"]` and
`self.data_types["Box"]`):
- Value position: `instantiate_ctor` succeeds → data ctor wins. Already
  the behavior; unchanged.
- Type-witness position (§3 arm): `expr_as_type` maps
  `Constructor("Box")` → `TypeKind::Named("Box")` → `ast_type_to_ty`
  resolves the *type* (data_types/aliases), never consulting
  `self.constructors`. Type wins.
- So precedence is positional, not global: **ctor in value position, type
  in witness position** — confirmed implementable because the two paths
  never share a lookup. The only overlap is the §4 else-branch, which only
  fires when the name is NOT a ctor, so no ambiguity is introduced there.
- `check_expr` expected-type-aware variant (optional refinement): in
  `check_expr`'s fallthrough (infer.rs:6088-6093), a `Constructor` expr
  whose expected type is kind `Type` could be routed directly to
  `expr_as_type`. Not required for v1 since §3's App-directed path covers
  the intended use.

## 5. ERASURE / CODEGEN

- Lambda params become runtime params in `compile_lambda_inner`
  (codegen.rs:10996; signature fixed at codegen.rs:11060-11065 as
  `(db, env, arg)`; multi-param currying at 11005-11022; `PendingLambda`
  carries `param_pat`, 11072-11079; binding emitted via the
  `PatKind::Var(name) => env.set(name, …)` pattern at codegen.rs:3728 and
  the general binder at 8081-8126).
- Dropping witness params: in `compile_lambda_inner`, filter
  `PatKind::TypeWitness` out of `params` BEFORE the curry split and before
  `pat_bound_names` (codegen.rs:11029-11032). Because infer already typed
  the lambda as `Fun(Con("Type"), rest)`, erasing the param in codegen
  changes the *runtime* arity relative to the static arity — this is fine
  only if call sites also drop the corresponding argument. Hence:
- Call-site erasure: `compile_app` (called from codegen.rs:5064) must skip
  arguments whose span is in the `type_arg_spans` side set (§3). The
  curried-application machinery (codegen.rs:8635 peels nested lambdas for
  arity; check how partial application counts args — grep
  `ExprKind::App` spines around codegen.rs:5054 and the `compile_app`
  body) must count only runtime args. This is the trickiest codegen bit:
  every place that computes a function's arity from its *type* or from the
  lambda's `params.len()` (e.g. codegen.rs:1792 `params.len()`, 2959)
  needs to subtract witness params. Audit those three sites.
- Type-ctor-field erasure precedent confirmed: `TypeCtor` →
  `knot_value_unit` (codegen.rs:5563-5567); the same treatment applies to
  a bare `Constructor(name)` reinterpreted as a type in value position
  (§4): side-table the span in infer, erase to unit in codegen's
  `Constructor` arm.
- `pat_bound_names` (codegen.rs:16561-16583), `pretty_pat`
  (codegen.rs:17313-17342): new arm — witness binds no names; pretty-prints
  as `(T : Type)`.

## 6. ALL MATCH/VISITOR SITES FOR NEW NODES

New `PatKind::TypeWitness` arms needed:
- knot/parser.rs: `push_pat_vars` (285) — push nothing.
- knot/format.rs: `render_pat` (2345-2401 region, exact fn at 2345) — print
  `(T : Type)`; also the multiline lambda paths at 1007/1020/1075 reuse
  `render_pat`.
- knot-compiler/infer.rs: `check_pattern` (6779) — new arm implementing §2
  (only valid against kind-`Type` expected, else error "type-witness
  parameter must have kind Type"); ensure infer-mode Lambda
  (5295-5311) gives a witness param `Ty::Con("Type",[])` instead of
  `self.fresh()` when no expected type (or better: require an annotation —
  see Risks).
- knot-compiler/desugar.rs: `pat_bound_names` (670-674 region), the
  pattern walks at 656/726/961/1188 — new arm returning no names, and
  ensure no desugar rewrite synthesizes/breaks witness pats.
- knot-compiler/codegen.rs: `pat_bound_names` (16561), `pretty_pat`
  (17313), binder emission (3728, 8081-8126), lambda compile
  (10996-11035).
- knot-compiler/unused.rs: pattern walk (410-411) — do not mark the type
  name as a used *value*; ideally record the type-name use so `data` types
  referenced only via witnesses aren't flagged unused.
- knot-compiler/effects.rs: `pat`-walking helper at 1922-1923 region +
  lambda-param inspection at 1324 and 1659 (both match
  `ast::PatKind::Var(name)` on lambda params — with a witness param these
  just don't match; confirm no index-based param/arg correspondence is
  assumed there — effects.rs:1324/1659 pair params with effect info, audit).
- knot-compiler/stratify.rs: expr walker (247 region) — patterns not
  matched there beyond existing kinds; add arm if the walker is
  exhaustive.
- knot-compiler/sql_lint.rs: pattern sites (it matched `ExprKind` at
  242/1097 for TypeCtor; check for PatKind matches) — likely none; verify
  with a build after adding the variant (exhaustive matches will flag).
- knot-lsp: any `PatKind` matches (semantic_tokens, hover, defs,
  references, document_highlight, rename) — the compiler will flag
  exhaustive matches; ensure hover on `(T : Type)` reports the *type*
  binding, and rename/references treat witness occurrences (which are
  `TypeKind::Named`/`Var` in annotations + `Constructor`/`Var` exprs in
  type-arg position) consistently.

New `TypeKind::TyArrow` arms needed (only if you take the recommended
signature node):
- knot/format.rs: `render_type` (search `TypeKind::Function` around
  format.rs; render as `(a : Type) -> …`).
- knot-compiler/infer.rs: `ast_type_to_ty` (4406), `expand_param_alias`
  spine peeling (4353-4364 — treat TyArrow as non-App head), the
  type-walkers (`collect_refined_names` 1428, `collect_free_unit_vars`
  3866, subst/apply at 1549/1831/3699/3769/4251 — only if they pattern
  match `ast::Type`, most operate on `Ty`).
- knot-lsp: `collect_names_in_type` (code_action.rs:2089-2119),
  `defs.rs` resolve_type (564-594), completion.rs:325/390/1668,
  signature_help/type_format — add recursion into `body`, and register
  `var` as a local type binder for goto-def/rename.
- knot-compiler/types.rs: `resolve_type` (schema resolution) — should
  treat `TyArrow` as transparent to `body` (witnesses erased; a source
  field can't have kind Type) or reject it in source schemas.

No new `ExprKind` is required for the core design (arguments stay
`Constructor`/`Var`; reinterpretation happens in infer). If you prefer an
explicit marker for erased type args post-inference, you could rewrite the
arg into `ExprKind::TypeCtor`-like node in a post-inference pass (the
codebase already rewrites AST post-inference — see
`resolve_result_markers` and the rewrite at infer.rs:11598), but the
span side-table is less invasive.

## 7. RISKS

1. **`Int 1` collision**: In value position `Int 1` is currently an error
   (verified), so no regression, but in *witness* position `f Int` where
   the witness should be a dimensionless int must spell `Int 1` — and in
   expr position that is `App(Constructor("Int"), Lit(Int "1"))`. The §3
   `expr_as_type` must map that shape to `TypeKind::UnitAnnotated` (or set
   `in_type_annotation` and synthesize the unit arg). If missed, users get
   the confusing "bare `Int` requires a unit" error from a value-position
   argument. Contained in `expr_as_type`, but must be designed explicitly.
2. **HKT interplay**: `Ty::TyCon`/`Ty::App` unify arms (infer.rs:2238-2310)
   treat `App(f,a)` vs `Relation`/`Int`/`Float`/`IO` specially. A witness
   bound to a `TyCon` (e.g. `f Maybe`) produces a kind `Type -> Type`
   argument — the §3 arm's `unify(arg_slot, ty_arg)` would equate
   `Con("Type",[])` with `TyCon("Maybe")` and FAIL. So the witness slot's
   *kind* must be checked structurally: arg_slot `Type` accepts only
   fully-applied types (`Con(..)`, `Record`, `Relation`, `Var`,
   `Alias`); a `TyCon`/partially-applied arg needs arg_slot
   `Fun(Type, Type)`. Implement a tiny `kind_of(&Ty) ->
   Option<Kind-as-Ty-shape>` and unify kinds instead of hard-coding
   `Con("Type",[])`. The `TypeCtor` kind-fold (infer.rs:5889-5895) is the
   template.
3. **Forall instantiation**: witness params generalize like ordinary vars
   (`generalize`/`instantiate_at`), so `f = \(T : Type) x -> x` gets
   `∀t. Type -> t -> t` — good. But `check_pattern`'s Forall arm
   (infer.rs:6787) binds *value* vars with polymorphic schemes; do NOT
   route witness binders through it. Also the escape check
   (infer.rs:5930-5940) will fire if a witness skolem leaks into the
   enclosing env — that's desired, but the error message should mention
   type witnesses (currently says "add an explicit `forall` annotation").
4. **Infer-mode lambdas without signatures**: infer.rs:5295-5311 gives
   every param a fresh var; a `\(T : Type)` param there would unify
   `Con("Type",[])` with a flexible var — fine — but then the *body*'s use
   of `T` must connect: the body annotation `(x : T)` resolves `T` via
   `annotation_vars` — works only if the infer-mode Lambda arm also runs
   the §2 registration when it sees `PatKind::TypeWitness` (give the param
   type `Con("Type",[])` and bind name→fresh skolem/var in
   `annotation_vars`). Without a signature the witness var stays flexible
   and will be generalized — acceptable, mirrors HM.
5. **Formatter round-trip**: two new syntaxes — `(T : Type)` in lambda
   params and the witness arg (plain `Int`/`Maybe Int` in arg position).
   The former round-trips via `render_pat`. The latter is *just an
   expression* — `map Int xs` formats as an ordinary app, which re-parses
   to the same AST. Round-trip safe by construction; add
   format_roundtrip tests (crates/knot/tests/format_roundtrip.rs) with
   both forms. Careful: `render_pat` for `TypeWitness` must include the
   parens or the reparse breaks (`\(T : Type) -> …` without parens is a
   different — invalid — pattern sequence).
6. **Ambiguity of `Constructor` in witness position when both a ctor and a
   type exist**: resolved positionally (§4), but the *error messages* must
   not regress: `unknown constructor` currently fires for type names used
   as values; after §4, `Box` alone (a non-nullary ctor) used as a value
   argument where a value is expected still works (ctor wins), while `map
   Box xs` reinterprets. Document the rule; add tests where a name is both
   (e.g. `data Maybe a = Just {value: a} | Nothing` + `data Box a = Box
   {value: a}`).
7. **Desugar before infer**: desugar rewrites lambdas for do-blocks and
   comprehensions (desugar.rs:1223-1292 synthesizes wildcard/var params);
   ensure no rewrite assumes all params are value patterns (e.g. eta-expansion
   or `__result` wrapping at infer.rs:5327-5347 operates on exprs, fine).
8. **`expand_param_alias` spine peeling** (infer.rs:4353-4364): a witness
   arg like `Pair Int Text` goes through this path already; but if the
   witness head is a *witness variable* (`\(F : Type -> Type) … f (F
   Int)`), `expr_as_type` yields `App(Var F, Int)` — `ast_type_to_ty`'s
   App arm handles `Ty::Var` func via `Ty::App` (4556-4558). Requires the
   §2 change so `Named "F"` resolves to the witness var first.

## Recommended minimal-diff plan (ordered)

1. `PatKind::TypeWitness{name, kind}` + parser LParen-arm extension +
   `push_pat_vars`/`render_pat`/pretty_pat arms; reject in case patterns.
2. infer: `check_pattern` arm (witness registration via skolems +
   `annotation_vars`, guarded); infer-mode Lambda arm support;
   `ast_type_to_ty` `Named` arm consults witness scope (flagged).
3. infer App arm: kind-`Type` param detection + `expr_as_type` + side
   tables (`type_arg_spans`, `witness_bindings`); kind checker for HK
   witnesses; extend lambda-last/ctor paths to not shadow it.
4. `Constructor` else-branch (§4) for bare type names in value position +
   erase-to-unit side table.
5. codegen: filter witness params in `compile_lambda_inner`, skip
   `type_arg_spans` args in `compile_app`, audit arity sites
   (codegen.rs:1792, 2959, 8635), erase marked `Constructor` exprs to unit.
6. Remaining visitor arms (§6) driven by compiler exhaustiveness errors;
   LSP hover/rename polish.
7. Tests: parser (crates/knot/tests/parser.rs), formatter round-trip,
   infer (crates/knot-compiler/tests/regress_infer*.rs), codegen end-to-end
   (`knot build` + run) covering: witness identity, witness used in body
   annotation, HK witness (`F : Type -> Type`), `Int 1` witness, name that
   is both ctor and type, leak/escape error, formatter round-trip.
