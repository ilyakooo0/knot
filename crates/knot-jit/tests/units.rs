//! Units of measure and refined types.
//!
//! Units are not declared — any name in a unit position is a unit. Refined
//! types are `type Name = Base where \x -> <pred>`, validated with `refine`.

mod harness;
use harness::{assert_compile_err, assert_prog, assert_show};

// ── Units ────────────────────────────────────────────────────────────────

#[test]
fn unit_annotation_keeps_value() {
    assert_show("(250 : Int Ms)", "250");
    assert_show("(2.5 : Float Ms)", "2.5");
}

#[test]
fn same_unit_addition() {
    assert_show("(100 : Int Ms) + (50 : Int Ms)", "150");
}

#[test]
fn unit_scalar_multiply() {
    assert_show("(100 : Int Ms) * 2", "200");
}

#[test]
fn incompatible_units_rejected() {
    assert_compile_err("((100 : Int Ms) + (50 : Int Usd))", "");
}

#[test]
fn unit_division_derives_ratio() {
    assert_show("((100 : Int Ms) / (50 : Int Ms))", "2");
}

#[test]
fn strip_unit() {
    assert_show("base.stripUnit (250 : Int Ms)", "250");
    assert_show("base.stripFloatUnit (2.5 : Float M)", "2.5");
}

#[test]
fn with_unit_roundtrip() {
    assert_show(
        "(base.withUnit (base.stripUnit (250 : Int Ms)) : Int Ms)",
        "250",
    );
}

#[test]
fn unit_comparison() {
    assert_show("((100 : Int Ms) < (200 : Int Ms))", "True");
}

#[test]
fn unit_mismatch_comparison_rejected() {
    assert_compile_err("((100 : Int Ms) < (200 : Int Usd))", "");
}

#[test]
fn unit_polymorphic_function() {
    // A unit hole `_` binds the unit by unification; the unit flows through.
    assert_prog(
        "with {\ndouble (\\(n : Int _) -> n + n)\n}\n(base.show (double (5 : Int M)))",
        "10",
    );
}

// ── Refined types ────────────────────────────────────────────────────────

#[test]
fn refine_valid_value() {
    assert_prog(
        "with {\ntype Nat = Int 1 where \\x -> x >= 0\n}\n\
         (case refine (5 : Int 1) of\n  Result.Ok {value n} -> n\n  Result.Err {error e} -> (0 - 1))",
        "5",
    );
}

#[test]
fn refine_invalid_value() {
    assert_prog(
        "with {\ntype Nat = Int 1 where \\x -> x >= 0\n}\n\
         (case refine ((0 - 5) : Int 1) of\n  Result.Ok {value n} -> n\n  Result.Err {error e} -> (0 - 1))",
        "-1",
    );
}

#[test]
fn refined_subtype_of_base() {
    // A refined value is usable where its base type is expected.
    assert_prog(
        "with {\ntype Nat = Int 1 where \\x -> x >= 0\n}\n\
         (case refine (5 : Int 1) of\n  Result.Ok {value n} -> n + 10\n  Result.Err {error e} -> 0)",
        "15",
    );
}

#[test]
fn strip_refined() {
    assert_prog(
        "with {\ntype Nat = Int 1 where \\x -> x >= 0\n}\n\
         (case refine (7 : Int 1) of\n  Result.Ok {value n} -> base.strip n\n  Result.Err {error e} -> 0)",
        "7",
    );
}
