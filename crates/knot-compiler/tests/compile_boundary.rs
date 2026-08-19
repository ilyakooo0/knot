//! Compile-boundary ADT subsumption: `base.compile`'s expected-type check,
//! driven through the public `check_with_expected` / `take_subsumption_verdict`
//! seam the JIT uses. Each test parses a snippet program and subsumes its body
//! type against an expected payload (host `type` decls + a trailing type),
//! asserting the verdict.
//!
//! Verdict: `Some(true)` accept, `Some(false)` reject, `None` no-check
//! (unparseable expected / no inferrable body type).
//!
//! Constructor-set direction follows variance:
//! - Covariant (snippet produces): snippet ctors ⊆ host ctors — a snippet-only
//!   ctor the host can't destructure is rejected.
//! - Contravariant (snippet consumes a host value, e.g. a function param): host
//!   ctors ⊆ snippet ctors — a host-only ctor the snippet can't match is rejected.

use knot_compiler::infer::{check_with_expected, take_subsumption_verdict};

/// `take_subsumption_verdict` reads a GLOBAL slot, so tests that exercise it
/// must not run concurrently — serialize them on one mutex, else a verdict is
/// consumed by the wrong test and they flake under the default parallel runner.
static VERDICT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Parse a knot source string into a program expression, panicking on any
/// lex/parse error (tests must use well-formed sources).
fn parse(src: &str) -> knot::ast::Expr {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        lex_diags
            .iter()
            .all(|d| d.severity != knot::diagnostic::Severity::Error),
        "lex errors: {lex_diags:?}"
    );
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (expr, parse_diags) = parser.parse_file_expr();
    assert!(
        parse_diags
            .iter()
            .all(|d| d.severity != knot::diagnostic::Severity::Error),
        "parse errors: {parse_diags:?}"
    );
    expr
}

/// Subsume `snippet`'s body type against `expected` (which may carry leading
/// host `data` decls). Returns the verdict.
fn subsumes(snippet: &str, expected: &str) -> Option<bool> {
    let _guard = VERDICT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut prog = parse(snippet);
    let _ = check_with_expected(&mut prog, expected);
    take_subsumption_verdict()
}

// ── Scalars & the no-check path ─────────────────────────────────────────────

#[test]
fn scalar_int_subsumes() {
    assert_eq!(subsumes("42", "Int 1"), Some(true));
}

#[test]
fn scalar_text_subsumes() {
    assert_eq!(subsumes("\"hello\"", "Text"), Some(true));
}

#[test]
fn scalar_mismatch_rejected() {
    assert_eq!(subsumes("\"hello\"", "Int 1"), Some(false));
}

#[test]
fn polymorphic_lambda_subsumes_identity() {
    assert_eq!(subsumes("\\x -> x", "a -> a"), Some(true));
}

// ── Single-level fieldless ADT (covariant) ──────────────────────────────────

#[test]
fn adt_fieldless_identical_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\n}\n(Priority.High {})",
            "Priority  Low {}  High {}\nPriority"
        ),
        Some(true)
    );
}

/// Snippet declares an extra ctor the host has no arm for: under covariance the
/// snippet produces ⊆ the host consumes, so a snippet-only ctor is unsound.
#[test]
fn adt_fieldless_extra_snippet_ctor_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}  Medium {}\n}\n(Priority.Medium {})",
            "Priority  Low {}  High {}\nPriority"
        ),
        Some(false)
    );
}

/// Snippet is narrower than the host: producing only `Low` where the host can
/// consume `{Low, High}` is sound (covariant subset).
#[test]
fn adt_fieldless_narrower_snippet_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}\n}\n(Priority.Low {})",
            "Priority  Low {}  High {}\nPriority"
        ),
        Some(true)
    );
}

// ── Nested ADT (ADT with an ADT payload field) ──────────────────────────────
// The host's `type Task` decl references `Priority`; both decls must travel so
// the JIT can resolve the payload and recurse the ctor-set check into it.

#[test]
fn adt_nested_identical_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nTask  Todo {pri Priority}\n}\n(Task.Todo {pri (Priority.High {})})",
            "Priority  Low {}  High {}\nTask  Todo {pri Priority}\nTask"
        ),
        Some(true)
    );
}

/// The regression this feature fixed: a nested payload ctor the host can't
/// destructure must reject, not cross unchecked and panic the host.
#[test]
fn adt_nested_extra_inner_ctor_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}  Medium {}\nTask  Todo {pri Priority}\n}\n(Task.Todo {pri (Priority.Medium {})})",
            "Priority  Low {}  High {}\nTask  Todo {pri Priority}\nTask"
        ),
        Some(false)
    );
}

/// Outer ctor sets match but the inner payload ADT is narrower on the snippet
/// side: still sound (covariant subset at the nested position).
#[test]
fn adt_nested_narrower_inner_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}\nTask  Todo {pri Priority}\n}\n(Task.Todo {pri (Priority.Low {})})",
            "Priority  Low {}  High {}\nTask  Todo {pri Priority}\nTask"
        ),
        Some(true)
    );
}

// ── Scalar payload fields ───────────────────────────────────────────────────

#[test]
fn adt_scalar_payload_match_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nWrap  W {n (Int 1)}\n}\n(Wrap.W {n 7})",
            "Wrap  W {n (Int 1)}\nWrap"
        ),
        Some(true)
    );
}

#[test]
fn adt_scalar_payload_mismatch_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nWrap  W {n Text}\n}\n(Wrap.W {n \"x\"})",
            "Wrap  W {n (Int 1)}\nWrap"
        ),
        Some(false)
    );
}

// ── Variance: ADT in contravariant (function-parameter) position ────────────
// The snippet produces a `Priority -> Text` the host calls. The parameter is
// contravariant: the host's ctors must be ⊆ the snippet's, so the snippet can
// match every value the host passes.

#[test]
fn adt_contravariant_host_wider_rejected() {
    // Host may pass {Low, High, Medium}; the snippet matches only {Low, High}.
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\n}\n(\\p -> case p of\n  Priority.Low {} -> \"l\"\n  Priority.High {} -> \"h\")",
            "Priority  Low {}  High {}  Medium {}\nPriority -> Text"
        ),
        Some(false)
    );
}

#[test]
fn adt_contravariant_snippet_wider_accepted() {
    // Snippet matches {Low, High, Medium}; the host passes only {Low, High}.
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}  Medium {}\n}\n(\\p -> case p of\n  Priority.Low {} -> \"l\"\n  Priority.High {} -> \"h\"\n  Priority.Medium {} -> \"m\")",
            "Priority  Low {}  High {}\nPriority -> Text"
        ),
        Some(true)
    );
}

#[test]
fn adt_contravariant_identical_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\n}\n(\\p -> case p of\n  Priority.Low {} -> \"l\"\n  Priority.High {} -> \"h\")",
            "Priority  Low {}  High {}\nPriority -> Text"
        ),
        Some(true)
    );
}

// ── Parameterized wrappers around an ADT ────────────────────────────────────
// The ctor-set check must recurse through `Maybe` / relation `[…]` to reach the
// ADT inside, at the wrapper's (covariant) variance.

#[test]
fn adt_inside_maybe_narrower_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\n}\n(Maybe.Just {value (Priority.Low {})})",
            "Priority  Low {}  High {}  Medium {}\nMaybe Priority"
        ),
        Some(true)
    );
}

#[test]
fn adt_inside_maybe_wider_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}  Medium {}\n}\n(Maybe.Just {value (Priority.Medium {})})",
            "Priority  Low {}  High {}\nMaybe Priority"
        ),
        Some(false)
    );
}

#[test]
fn adt_inside_relation_narrower_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}\n}\nRel (Priority.Low {})",
            "Priority  Low {}  High {}\nRel Priority"
        ),
        Some(true)
    );
}

#[test]
fn adt_inside_relation_wider_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}  Medium {}\n}\nRel (Priority.Medium {})",
            "Priority  Low {}  High {}\nRel Priority"
        ),
        Some(false)
    );
}

// ── Invariant position (ADT in both param and result of one function) ───────
// `Priority -> Priority` puts `Priority` at mixed polarity: the ctor sets must
// be exactly EQUAL — neither a narrower nor a wider snippet is usable.

#[test]
fn adt_invariant_identical_accepted() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\n}\n(\\p -> case p of\n  Priority.Low {} -> (Priority.Low {})\n  Priority.High {} -> (Priority.High {}))",
            "Priority  Low {}  High {}\nPriority -> Priority"
        ),
        Some(true)
    );
}

#[test]
fn adt_invariant_narrower_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}\n}\n(\\p -> case p of\n  Priority.Low {} -> (Priority.Low {}))",
            "Priority  Low {}  High {}\nPriority -> Priority"
        ),
        Some(false)
    );
}

// ── Payload field-set checks ────────────────────────────────────────────────
// The consumed side binds ctor fields; those fields must be PRESENT in the
// produced side (a name the host binds but the snippet lacks is a runtime
// "field not found" panic). Field-name and missing-field mismatches reject.

/// The host binds `n`; the snippet's ctor has `count` instead. Reject.
#[test]
fn adt_field_name_mismatch_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nWrap  W {count (Int 1)}\n}\n(Wrap.W {count 7})",
            "Wrap  W {n (Int 1)}\nWrap"
        ),
        Some(false)
    );
}

/// The host binds a field the snippet's ctor doesn't have at all. Reject.
#[test]
fn adt_field_missing_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nWrap  W {n (Int 1)}\n}\n(Wrap.W {n 7})",
            "Wrap  W {n (Int 1) extra Text}\nWrap"
        ),
        Some(false)
    );
}

/// A payload field that is itself an ADT recurses the ctor-set check: the
/// inner ctor sets must relate at the enclosing variance.
#[test]
fn adt_payload_multi_ctor_field_mismatch_rejected() {
    assert_eq!(
        subsumes(
            "with {\nPriority  Low {}  High {}\nShape  Circle {r Text}  Square {}\n}\n(Shape.Circle {r \"x\"})",
            "Shape  Circle {r (Int 1)}  Square {}\nShape"
        ),
        Some(false)
    );
}
