//! ADTs, case-expressions, Maybe/Result, and base.match.

mod harness;
use harness::{assert_compile_err, assert_show, assert_show_set};

#[test]
fn adt_construction_and_show() {
    assert_show("Maybe.Just {value 5}", "Just {value 5}");
    assert_show("Maybe.Nothing {}", "Nothing");
    assert_show("Result.Ok {value 5}", "Ok {value 5}");
    assert_show("Result.Err {error \"bad\"}", "Err {error bad}");
}

#[test]
fn case_on_maybe() {
    assert_show(
        "(case Maybe.Just {value 5} of\n  Maybe.Just {value v} -> v\n  Maybe.Nothing {} -> 0)",
        "5",
    );
}

#[test]
fn case_on_nothing() {
    assert_show(
        "(case Maybe.Nothing {} of\n  Maybe.Just {value v} -> v\n  Maybe.Nothing {} -> 0)",
        "0",
    );
}

#[test]
fn case_bool() {
    assert_show(
        "(case (3 > 2) of\n  Bool.True {} -> \"yes\"\n  Bool.False {} -> \"no\")",
        "yes",
    );
}

#[test]
fn custom_adt() {
    assert_show(
        "with {\nColor  Red {}  Green {}  Blue {}\n}\n(Color.Red {})",
        "Red",
    );
}

#[test]
fn custom_adt_payload() {
    assert_show(
        "with {\nShape  Circle {radius (Int 1)}  Rect {w (Int 1) h (Int 1)}\n}\n(Shape.Rect {w 3 h 4})",
        "Rect {h 4 w 3}",
    );
}

#[test]
fn case_on_custom_adt() {
    assert_show(
        "with {\nShape  Circle {radius (Int 1)}  Rect {w (Int 1) h (Int 1)}\n}\n\
         (case Shape.Rect {w 3 h 4} of\n  Shape.Circle {radius r} -> r * r\n  Shape.Rect {w ww h hh} -> ww * hh)",
        "12",
    );
}

#[test]
fn case_non_exhaustive_rejected() {
    assert_compile_err(
        "(case Maybe.Just {value 5} of\n  Maybe.Just {value v} -> v)",
        "",
    );
}

#[test]
fn adt_equality() {
    assert_show("Maybe.Just {value 5} == Maybe.Just {value 5}", "True");
    assert_show("Maybe.Just {value 5} == Maybe.Nothing {}", "False");
}

#[test]
fn adt_in_relation() {
    assert_show_set(
        "[(Maybe.Just {value 1})  (Maybe.Nothing {})  (Maybe.Just {value 2})]",
        &["Just {value 1}", "Nothing", "Just {value 2}"],
    );
}

#[test]
fn match_filters_constructor() {
    // base.match keeps only rows of the given constructor, returning payloads.
    assert_show_set(
        "with {\nEvt  Click {x (Int 1)}  Key {code (Int 1)}\n}\n\
         (base.match Evt.Click [(Evt.Click {x 1})  (Evt.Key {code 9})  (Evt.Click {x 2})])",
        &["{x 1}", "{x 2}"],
    );
}

#[test]
fn maybe_map_via_case() {
    // Maybe map, written structurally with case.
    assert_show(
        "(case Maybe.Just {value 3} of\n  Maybe.Just {value v} -> Maybe.Just {value (v * 2)}\n  Maybe.Nothing {} -> Maybe.Nothing {})",
        "Just {value 6}",
    );
}

#[test]
fn result_error_prop() {
    assert_show(
        "(case Result.Err {error \"nope\"} of\n  Result.Ok {value v} -> base.show v\n  Result.Err {error e} -> \"err: \" ++ e)",
        "err: nope",
    );
}
