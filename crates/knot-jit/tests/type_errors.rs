//! Negative tests: type errors, arity, and malformed programs must be
//! rejected at compile time with a diagnostic.

mod harness;
use harness::{assert_compile_err, assert_show};

#[test]
fn int_plus_text() {
    assert_compile_err("1 + \"x\"", "");
}

#[test]
fn text_plus_int() {
    assert_compile_err("\"x\" + 1", "");
}

#[test]
fn wrong_arg_type() {
    assert_compile_err("base.length 42", "");
}

#[test]
fn non_exhaustive_case() {
    assert_compile_err(
        "(match Maybe.Nothing {}\n  Maybe.Just {value v}  v)",
        "",
    );
}

#[test]
fn unbound_variable() {
    assert_compile_err("undefinedName", "");
}

#[test]
fn annotation_mismatch() {
    assert_compile_err("(base.the (Text) 42)", "");
}

#[test]
fn mixed_relation_literal() {
    assert_compile_err("[1  \"two\"]", "");
}

#[test]
fn if_without_else_type() {
    // match arms must agree in type.
    assert_compile_err(
        "(match (1 == 1)\n  Bool.True {}  1\n  Bool.False {}  \"x\")",
        "",
    );
}

#[test]
fn apply_non_function() {
    assert_compile_err("(42 3)", "");
}

#[test]
fn field_on_missing() {
    assert_compile_err("{x 1}.y", "");
}

#[test]
fn ambiguous_morph_projection() {
    assert_compile_err("(^into) \"42\"", "ambiguous projection");
}

#[test]
fn unparenthesized_unit_type_argument() {
    // `Maybe Int 1` reads ambiguously (is `1` Maybe's or Int's argument?) — a
    // unit-carrying type used as a type argument must be parenthesized.
    assert_compile_err(
        "with {\n_  f  (\\(Maybe Int 1  x) -> 0)\n}\n(f 0)",
        "parenthesize a unit-carrying type argument",
    );
    // The parenthesized form is accepted.
    assert_show(
        "with {\n_  f  (\\(Maybe (Int 1)  x) -> x)\n}\n(f (Maybe.Just {value 3}))",
        "Just {value 3}",
    );
}

#[test]
fn cross_scope_shadowing() {
    assert_compile_err(
        "with {\nfoo 1\n}\n((with {\nfoo 2\n}\nfoo))",
        "shadowing is not allowed",
    );
}

#[test]
fn unit_mismatch() {
    assert_compile_err(
        "with {  unit Ms  unit Usd } ((base.the (Int Ms) 100) + (base.the (Int Usd) 50))",
        "",
    );
}

#[test]
fn record_unify_wrong_field_type() {
    // unify replacing a field with an incompatible type on a typed record.
    assert_compile_err(
        "with {\nP  {x (Int 1)}\np (base.the (P) {})\n}\n(base.unify {x 1} {x \"s\"})",
        "",
    );
}