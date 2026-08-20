//! Exhaustive `base.list` (ordered persistent List ADT) tests.
//!
//! Constructor shape (observed): `Nil` and `Cons {head: a, tail: List a}`.

mod harness;
use harness::{assert_show, assert_show_set};

#[test]
fn nil_and_cons() {
    assert_show("base.list.nil {}", "Nil");
    assert_show(
        "base.list.cons 1 (base.list.nil {})",
        "Cons {head 1 tail Nil}",
    );
}

#[test]
fn cons_chain() {
    assert_show(
        "base.list.cons 1 (base.list.cons 2 (base.list.nil {}))",
        "Cons {head 1 tail Cons {head 2 tail Nil}}",
    );
}

#[test]
fn is_nil() {
    assert_show("base.list.isNil (base.list.nil {})", "True");
    assert_show(
        "base.list.isNil (base.list.cons 1 (base.list.nil {}))",
        "False",
    );
}

#[test]
fn head() {
    assert_show(
        "base.list.head (base.list.cons 1 (base.list.cons 2 (base.list.nil {})))",
        "Just {value 1}",
    );
    assert_show("base.list.head (base.list.nil {})", "Nothing");
}

#[test]
fn tail() {
    // tail returns a Maybe (Nothing on empty, Just <list> otherwise).
    assert_show(
        "base.list.tail (base.list.cons 1 (base.list.cons 2 (base.list.nil {})))",
        "Just {value Cons {head 2 tail Nil}}",
    );
    assert_show("base.list.tail (base.list.nil {})", "Nothing");
}

#[test]
fn length() {
    assert_show("base.list.length (base.list.nil {})", "0");
    assert_show(
        "base.list.length (base.list.cons 1 (base.list.cons 2 (base.list.nil {})))",
        "2",
    );
}

#[test]
fn map() {
    assert_show(
        "base.list.map (\\n -> n * 2) (base.list.cons 1 (base.list.cons 2 (base.list.nil {})))",
        "Cons {head 2 tail Cons {head 4 tail Nil}}",
    );
}

#[test]
fn filter() {
    assert_show(
        "base.list.filter (\\n -> n > 1) (base.list.cons 1 (base.list.cons 2 (base.list.cons 3 (base.list.nil {}))))",
        "Cons {head 2 tail Cons {head 3 tail Nil}}",
    );
}

#[test]
fn fold() {
    assert_show(
        "base.list.fold (\\acc x -> acc + x) 0 (base.list.cons 1 (base.list.cons 2 (base.list.cons 3 (base.list.nil {}))))",
        "6",
    );
}

#[test]
fn reverse() {
    assert_show(
        "base.list.reverse (base.list.cons 1 (base.list.cons 2 (base.list.cons 3 (base.list.nil {}))))",
        "Cons {head 3 tail Cons {head 2 tail Cons {head 1 tail Nil}}}",
    );
}

#[test]
fn append() {
    assert_show(
        "base.list.append (base.list.cons 1 (base.list.nil {})) (base.list.cons 2 (base.list.nil {}))",
        "Cons {head 1 tail Cons {head 2 tail Nil}}",
    );
}

#[test]
fn relation_roundtrip() {
    assert_show_set(
        "base.list.toRelation (base.list.fromRelation [1  2  3])",
        &["1", "2", "3"],
    );
}

#[test]
fn from_relation_preserves_sorted_order() {
    assert_show(
        "base.list.head (base.list.fromRelation (base.sortBy (\\n -> n) [3  1  2]))",
        "Just {value 1}",
    );
}
