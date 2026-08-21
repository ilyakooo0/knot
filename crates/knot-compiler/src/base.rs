//! Standard prelude: ordinary polymorphic functions injected into every program.
//!
//! The prelude is a small set of plain functions. Builtin operator semantics
//! (`+`, `<`, `++`, unary `-`, `==`) are enforced intrinsically by the type
//! checker and code generator, and monadic do-blocks dispatch structurally, so
//! no trait declarations are needed here.
//!
//! A `.knot` file is a single expression, so the prelude is injected by
//! wrapping the program: `with { …prelude record… } <program>`. The program's
//! own bindings shadow the prelude's (a `with` body's later/literal bindings
//! win for the same name).

use knot::ast;

/// Byte offset added to every parsed prelude span so prelude spans can never
/// collide with user-file spans (bug B39). Chosen far above any plausible real
/// file size and above `desugar::SYNTH_SPAN_BASE` (1 << 31) so it also clears
/// the synthesized monad-span range.
pub(crate) const PRELUDE_SPAN_OFFSET: usize = 1 << 40;

/// Knot source for the standard prelude. The ONLY binding the prelude splices
/// into scope is `base` — a record holding every standard-library function.
/// User code refers to `base.println`, `base.map`, etc.; nothing else enters
/// the environment by default.
///
/// The prelude's own polymorphic helpers (`min`/`max`/`when`/`unless`) are
/// defined as nested lambdas inside the `base` record. The data declaration
/// `Ordering` is hoisted out (a `data` decl cannot be a record *field value*),
/// then re-exposed as `base.Ordering` by the compiler's ctor routing.
const PRELUDE_SOURCE: &str = r#"
{
base {
min (\a b -> case a < b of
  Bool.True {} -> a
  Bool.False {} -> b)

max (\a b -> case a > b of
  Bool.True {} -> a
  Bool.False {} -> b)

Bool -> IO {} -> IO {}  when
when (\cond action -> case cond of
  Bool.True {} -> action
  Bool.False {} -> yield {})

Bool -> IO {} -> IO {}  unless
unless (\cond action -> case cond of
  Bool.True {} -> yield {}
  Bool.False {} -> action)

-- Structured logging. `log` carries a `(<>logCtx)` fold constraint: at each
-- callsite the compiler merges every in-scope `logCtx` record (`base.unify`
-- from `{}`, innermost scope wins) and passes the merged record as the hidden
-- dictionary, so the caller writes only the level and message. The body reads
-- the merged context via `^logCtx` and emits through `knot_emit_log`
-- (`emitLog`). The `debug`/`info`/`warn`/`error` wrappers fix the level; each
-- is self-contained (a record field can't reference a sibling bare) and
-- re-declares the constraint so the caller's context threads through.
(<>logCtx) => Level -> Text -> IO {}  log
log (\level msg -> emitLog level msg ^logCtx)

(<>logCtx) => Text -> IO {}  debug
debug (\msg -> emitLog (Level.Debug {}) msg ^logCtx)

(<>logCtx) => Text -> IO {}  info
info (\msg -> emitLog (Level.Info {}) msg ^logCtx)

(<>logCtx) => Text -> IO {}  warn
warn (\msg -> emitLog (Level.Warn {}) msg ^logCtx)

(<>logCtx) => Text -> IO {}  error
error (\msg -> emitLog (Level.Error {}) msg ^logCtx)

-- List ADT namespace (`base.list.*`). Each field is a codegen builtin
-- (`Var(listX)` resolves to the registered knot_list_* function value), so
-- the prelude record needs no self-reference for recursion. `nil` is the
-- `listNil` builtin (a 1-arg fn that ignores its argument and returns the
-- empty list); call it as `base.list.nil {}` so the element type stays
-- polymorphic per call site. The rest are the function values themselves.
list {
nil listNil
cons listCons
isNil listIsNil
head listHead
tail listTail
length listLength
map listMap
filter listFilter
fold listFold
reverse listReverse
append listAppend
fromRelation listFromRelation
toRelation listToRelation
}

-- Vec namespace (`base.vec.*`): the concrete-sequence overloads of the data
-- ops, resolved via `^count` (etc.) against the relation-typed `base.*`
-- forms. The `^` resolver searches the full record with name+type matching,
-- so a `Vec a` argument reaches `base.vec.count` (nested here) even though
-- `base.count : [a] -> Int u` fails to unify with it.
vec {
count vecCount
}

-- Morph namespace (`base.morph.*`): type-directed conversions resolved by the
-- `^into` projection. Each `<from>To<to>` record holds an `into : S -> T`
-- function with an EXPLICIT concrete signature (an un-annotated body could
-- infer a polymorphic type and silently mis-dispatch — see the `^into`
-- resolver's unify-based candidate match). Fallible conversions return
-- `Maybe`. Conversions needing a primitive not exposed as a base builtin are
-- written as lambdas over the existing builtins.
morph {
textToBytes { Text -> Bytes  into
              into textToBytes }
bytesToText { Bytes -> Maybe Text  into
              into bytesToText }
bytesToHex  { Bytes -> Text  into
              into bytesToHex }
textToBytesFromHex { Text -> Maybe Bytes  into
                     into bytesFromHex }
intToFloat  { Int 1 -> Float 1  into
              into intToFloat }
textToInt   { Text -> Maybe (Int 1)  into
              into textToInt }
textToFloat { Text -> Maybe (Float 1)  into
              into textToFloat }
intToText   { Int 1 -> Text  into
              into (\n -> show n) }
floatToText { Float 1 -> Text  into
              into (\f -> show f) }
boolToText  { Bool -> Text  into
              into (\b -> show b) }
}
}
}
"#;

/// Stdlib builtins exposed as fields of `base` (`base.map`, `base.filter`, …).
/// Each is registered by codegen as a curried function value in `global_fns`;
/// the prelude record references them as `name: Var(name)` so the `base`
/// record assembles to real function values. Dispatch-only builtins
/// (`println`, `show`, `now`, …) and intrinsic constructors
/// (`Just`/`Nothing`/`Ok`/`Err`/`True`/`False`) are routed separately by the
/// compiler, not via this record.
pub(crate) const BASE_STDLIB_FNS: &[&str] = &[
    "all",
    "any",
    "appendFile",
    "avg",
    "bytesConcat",
    "bytesFromHex",
    "bytesGet",
    "bytesLength",
    "bytesSlice",
    "bytesToHex",
    "bytesToText",
    "chars",
    "contains",
    "countWhere",
    "decrypt",
    "diff",
    "distinct",
    "dress",
    "drop",
    "endsWith",
    "elem",
    "encrypt",
    "fileExists",
    "filter",
    "findFirst",
    "fold",
    "forEach",
    "fork",
    "hash",
    "head",
    "hexDecode",
    "id",
    "inter",
    "length",
    "listDir",
    "map",
    "match",
    "maxOn",
    "minOn",
    "not",
    "parseJson",
    "race",
    "randomInt",
    "readFile",
    "removeFile",
    "reverse",
    "run",
    "sign",
    "single",
    "sleep",
    "sortBy",
    "sortByDesc",
    "startsWith",
    "strip",
    "stripFloatUnit",
    "stripUnit",
    "take",
    "textToBytes",
    "toAsciiLower",
    "toAsciiUpper",
    "abs",
    "intMin",
    "intMax",
    "clamp",
    "unify",
    "toJson",
    "toLower",
    "toUpper",
    "traverse",
    "trim",
    "trimAscii",
    "ltrimAscii",
    "rtrimAscii",
    "byteLength",
    "upsertBy",
    "verify",
    "withFloatUnit",
    "withUnit",
    "writeFile",
    // Numeric/text conversions.
    "floor",
    "intToFloat",
    "textToInt",
    "textToFloat",
    // Relation query forms as first-class function values (`base.count`,
    // `base.union`, `base.sum`, `base.bind`). Each is registered as a curried
    // function value in codegen; the call-site SQL-pushdown optimization
    // recognizes both the bare (`count rel`) and namespaced (`base.count rel`)
    // application head. `bind` here is relation flatMap (`knot_relation_bind`).
    "count",
    "union",
    "sum",
    "bind",
    // Console IO builtins (registered as stdlib function values in codegen).
    "println",
    "print",
    "putLine",
    "show",
    // Value → evaluable Knot source (dependency-collecting). Registered as a
    // stdlib function value in codegen.
    "extract",
    // In-process JIT compile+eval: `compile : Text -> Maybe a`. Registered as
    // a stdlib function value in codegen (calls knot_builtin_compile, which
    // dispatches through the runtime's registered compile implementation).
    "compile",
    // 0-arg IO builtins. Each is a re-runnable IO thunk (`Value::IO(thunk, _)`)
    // produced fresh by the bare-`Var` dispatch, so holding the action as a
    // record field is safe — forcing `base.now` twice runs `knot_now` twice.
    "now",
    "readLine",
    "randomFloat",
    "randomUuid",
    "generateKeyPair",
    "generateSigningKeyPair",
];

/// Names a bare user-span `Var` may NOT reference directly; the hard gate
/// (option A) requires `base.<name>` instead. This is every stdlib builtin
/// plus the prelude's polymorphic helpers. Constructors
/// (`Just`/`Nothing`/`Ok`/`Err`/`True`/`False`) are deliberately NOT here —
/// they route through the dedicated `base.Ctor` arms, which gate separately.
pub(crate) fn is_gated_stdlib(name: &str) -> bool {
    BASE_STDLIB_FNS.contains(&name) || matches!(name, "min" | "max" | "when" | "unless")
}

/// Server special forms (`fetch`/`fetchWith`/`listen`/`listenOn`) and the STM
/// primitive `retry`. These are compile-time macros, NOT `base` record fields,
/// but the bare form is still gated for consistency — the user must write
/// `base.fetch`, `base.listen`, `base.retry`. Codegen routes the namespaced
/// head through the same macro dispatch (`server_form_name`); `retry` keeps its
/// own "only inside `atomic`" check, which fires first.
pub(crate) fn is_gated_special_form(name: &str) -> bool {
    matches!(
        name,
        "fetch" | "fetchWith" | "listen" | "listenOn" | "retry" | "todo" | "trace"
    )
}

/// Parse the prelude source and return the fully-assembled `base` record
/// expression (the prelude helpers `min`/`max`/`when`/`unless` plus every
/// stdlib builtin as a `name: Var(name)` field), with all spans shifted by
/// `PRELUDE_SPAN_OFFSET`. This is the single source of truth for the `base`
/// record's VALUE; infer (`bind_base_record`) and codegen
/// (`define_base_record`) each compile it to bind `base` globally — there is
/// no `with` wrapper around the program.
pub(crate) fn prelude_base_record() -> ast::Expr {
    let lexer = knot::lexer::Lexer::new(PRELUDE_SOURCE);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        !lex_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to lex: {:?}",
        lex_diags
    );
    let parser = knot::parser::Parser::new(PRELUDE_SOURCE.to_string(), tokens);
    let (mut prelude_record, parse_diags) = parser.parse_file_expr();
    assert!(
        !parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to parse: {:?}",
        parse_diags
    );

    // Inject the stdlib builtins as fields of the nested `base` record.
    let dummy_span = ast::Span::new(0, 0);
    let mut base_record = None;
    if let ast::ExprKind::Record(outer) = &mut prelude_record.node
        && let Some(base_field) = outer.iter_mut().find(|f| f.name == "base")
    {
        if let ast::ExprKind::Record(base_fields) = &mut base_field.value.node {
            for name in BASE_STDLIB_FNS {
                base_fields.push(ast::RecordField {
                    name: (*name).to_string(),
                    // Reference the stdlib fn by its FLATTENED `base.<name>`
                    // key — a `Var` string containing a dot, unutterable in
                    // user source, so it always resolves to the stdlib fn and
                    // is never shadowed by a user decl named `<name>`.
                    value: ast::Spanned::new(
                        ast::ExprKind::Var(crate::infer::Binding::User(format!("base.{}", name))),
                        dummy_span,
                    ),
                    sig: None,
                    doc: None,
                });
            }
        }
        base_record = Some(base_field.value.clone());
    }
    let mut record = base_record.expect("prelude source has no `base` field");
    // Elaborate the constrained `base.*` fns (`log`, `info`, …): prepend the
    // hidden `__dict_<field>` parameter and rewrite the body's `^field` to it.
    // The prelude is parsed raw (no `desugar` pass), so without this their
    // `^field` would resolve against the prelude scope, not the dictionary
    // the callsite splices in.
    crate::desugar::elaborate_all_implicit_dicts(&mut record);
    shift_expr_spans(&mut record, PRELUDE_SPAN_OFFSET);
    record
}

// ── Prelude span shifting (bug B39) ──────────────────────────────────
//
// Add `offset` to every declaration/expression/statement/pattern span in a
// prelude decl so prelude spans can never alias user-file spans in
// `monad_info` (and the other span-keyed inference maps). Type spans are left
// alone — they never key `monad_info`. Mirrors the AST shape walked by
// `unused::walk_decl`; keep the two in sync when the AST grows a node.
//
// "Every span" includes the standalone `name_span`/`api_span` fields, not just
// the `Spanned` wrappers: inference keys a punned record field's binder on
// `FieldPat::name_span`, so leaving it unshifted leaks a raw PRELUDE_SOURCE
// offset into `local_type_info` — a span the LSP cannot tell apart from a
// user span, since its provenance filter can only compare byte ranges. It
// then anchors an inlay hint at that offset in the user's file.

fn shift_expr_spans(e: &mut ast::Expr, offset: usize) {
    use ast::ExprKind::*;
    e.span.start += offset;
    e.span.end += offset;
    match &mut e.node {
        TypeLiteral(_) => {}
        App { func, arg } => {
            shift_expr_spans(func, offset);
            shift_expr_spans(arg, offset);
        }
        With { record, body, .. } => {
            shift_expr_spans(record, offset);
            shift_expr_spans(body, offset);
        }
        Lambda { params, body, .. } => {
            for p in params {
                shift_pat_spans(p, offset);
            }
            shift_expr_spans(body, offset);
        }
        BinOp { lhs, rhs, .. } => {
            shift_expr_spans(lhs, offset);
            shift_expr_spans(rhs, offset);
        }
        UnaryOp { operand, .. } => shift_expr_spans(operand, offset),
        Case { scrutinee, arms } => {
            shift_expr_spans(scrutinee, offset);
            for arm in arms {
                shift_pat_spans(&mut arm.pat, offset);
                shift_expr_spans(&mut arm.body, offset);
            }
        }
        Do(stmts) => {
            for s in stmts {
                shift_stmt_spans(s, offset);
            }
        }
        Set { target, value } | FullSet { target, value } => {
            shift_expr_spans(target, offset);
            shift_expr_spans(value, offset);
        }
        Atomic(inner) | Refine(inner) => shift_expr_spans(inner, offset),
        TimeUnitLit { value, .. } => shift_expr_spans(value, offset),
        Record(fields) => {
            for fl in fields {
                shift_expr_spans(&mut fl.value, offset);
            }
        }
        List(items) => {
            for it in items {
                shift_expr_spans(it, offset);
            }
        }
        FieldAccess { expr, .. } | Annot { expr, .. } => shift_expr_spans(expr, offset),
        Serve {
            api_span, handlers, ..
        } => {
            api_span.start += offset;
            api_span.end += offset;
            for h in handlers {
                h.endpoint_span.start += offset;
                h.endpoint_span.end += offset;
                shift_expr_spans(&mut h.body, offset);
            }
        }
        Lit(_)
        | Var(_)
        | Constructor(_)
        | SourceRef { .. }
        | ImplicitRef(_)
        | CollectFold(_)
        | TypeHole => {}
        TypeCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } => {}
    }
}

fn shift_stmt_spans(s: &mut ast::Stmt, offset: usize) {
    use ast::StmtKind::*;
    s.span.start += offset;
    s.span.end += offset;
    match &mut s.node {
        Bind { pat, expr } => {
            shift_pat_spans(pat, offset);
            shift_expr_spans(expr, offset);
        }
        Where { cond } => shift_expr_spans(cond, offset),
        GroupBy { key } => shift_expr_spans(key, offset),
        Expr(e) => shift_expr_spans(e, offset),
    }
}

fn shift_pat_spans(p: &mut ast::Pat, offset: usize) {
    use ast::PatKind::*;
    p.span.start += offset;
    p.span.end += offset;
    match &mut p.node {
        Var(_) | Wildcard | Lit(_) => {}
        Constructor { payload, .. } => shift_pat_spans(payload, offset),
        Record(fields) => {
            for fp in fields {
                // The field-name token's own span. For a punned field
                // (`{value}`) this IS the binder's span, and inference records
                // it in `binding_types` — so it must be shifted like any other
                // binder span, or it escapes as a raw prelude offset.
                fp.name_span.start += offset;
                fp.name_span.end += offset;
                if let Some(inner) = &mut fp.pattern {
                    shift_pat_spans(inner, offset);
                }
            }
        }
        List(items) => {
            for it in items {
                shift_pat_spans(it, offset);
            }
        }
        Cons { head, tail } => {
            shift_pat_spans(head, offset);
            shift_pat_spans(tail, offset);
        }
        Annot { pat, .. } => shift_pat_spans(pat, offset),
    }
}
