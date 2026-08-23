//! Units of measure and refined types.
//!
//! Units are not declared — any name in a unit position is a unit. Refined
//! types are `type Name = Base where \x -> <pred>`, validated with `refine`.

mod harness;
use harness::{assert_compile_err, assert_prog, assert_show};

// ── Units ────────────────────────────────────────────────────────────────

#[test]
fn unit_annotation_keeps_value() {
    assert_show("(base.the (Int Ms) 250)", "250");
    assert_show("(base.the (Float Ms) 2.5)", "2.5");
}

#[test]
fn same_unit_addition() {
    assert_show("(base.the (Int Ms) 100) + (base.the (Int Ms) 50)", "150");
}

#[test]
fn unit_scalar_multiply() {
    assert_show("(base.the (Int Ms) 100) * 2", "200");
}

#[test]
fn incompatible_units_rejected() {
    assert_compile_err("((base.the (Int Ms) 100) + (base.the (Int Usd) 50))", "");
}

#[test]
fn unit_division_derives_ratio() {
    assert_show("((base.the (Int Ms) 100) / (base.the (Int Ms) 50))", "2");
}

#[test]
fn strip_unit() {
    assert_show("base.stripUnit (base.the (Int Ms) 250)", "250");
    assert_show("base.stripFloatUnit (base.the (Float M) 2.5)", "2.5");
}

#[test]
fn with_unit_roundtrip() {
    assert_show(
        "(base.the (Int Ms) (base.withUnit (base.stripUnit (base.the (Int Ms) 250))))",
        "250",
    );
}

#[test]
fn unit_comparison() {
    assert_show("((base.the (Int Ms) 100) < (base.the (Int Ms) 200))", "True");
}

#[test]
fn unit_mismatch_comparison_rejected() {
    assert_compile_err("((base.the (Int Ms) 100) < (base.the (Int Usd) 200))", "");
}

#[test]
fn unit_polymorphic_function() {
    // A unit hole `_` binds the unit by unification; the unit flows through.
    assert_prog(
        "with {\n_  double  (\\((Int _)  n) -> n + n)\n}\n(base.show (double (base.the (Int M) 5)))",
        "10",
    );
}

// ── Refined types ────────────────────────────────────────────────────────

#[test]
fn refine_valid_value() {
    assert_prog(
        "with {\nNat  Int 1 where \\x ->  x >= 0\n}\n(match refine (base.the (Int 1) 5)\n  Result.Ok {value n}  n\n  Result.Err {error e}  (0 - 1))",
        "5",
    );
}

#[test]
fn refine_invalid_value() {
    assert_prog(
        "with {\nNat  Int 1 where \\x ->  x >= 0\n}\n(match refine (base.the (Int 1) (0 - 5))\n  Result.Ok {value n}  n\n  Result.Err {error e}  (0 - 1))",
        "-1",
    );
}

#[test]
fn refined_subtype_of_base() {
    // A refined value is usable where its base type is expected.
    assert_prog(
        "with {\nNat  Int 1 where \\x ->  x >= 0\n}\n(match refine (base.the (Int 1) 5)\n  Result.Ok {value n}  n + 10\n  Result.Err {error e}  0)",
        "15",
    );
}

#[test]
fn strip_refined() {
    assert_prog(
        "with {\nNat  Int 1 where \\x ->  x >= 0\n}\n(match refine (base.the (Int 1) 7)\n  Result.Ok {value n}  base.strip n\n  Result.Err {error e}  0)",
        "7",
    );
}