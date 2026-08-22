//! Pattern matching: nested ADT patterns, record patterns, guards, wildcard,
//! literal patterns, and comprehension joins (equi-join + cross product).

mod harness;
use harness::assert_show;

// ── Nested & literal patterns ──────────────────────────────────────────────

#[test]
fn nested_adt_pattern() {
    assert_show(
        "with { M  J {v (Int 1)}  N {} }
         (match (M.J {v 5})
            M.J {v n}  n
            M.N {}  0)",
        "5",
    );
}

#[test]
fn literal_pattern_in_case() {
    assert_show(
        "(match 2
           1  \"one\"
           2  \"two\"
           _  \"other\")",
        "two",
    );
}

#[test]
fn record_pattern_partial() {
    // bind only the fields you need
    assert_show(
        "(match {name \"a\"  age 30}\n           {name n}  n)",
        "a",
    );
}

#[test]
fn wildcard_binds_nothing() {
    assert_show(
        "(match (Maybe.Just {value 9})
           Maybe.Just {value _}  \"some\"
           Maybe.Nothing {}  \"none\")",
        "some",
    );
}

#[test]
fn bool_case_exhaustive() {
    assert_show(
        "(match (1 < 2)
           Bool.True {}  \"yes\"
           Bool.False {}  \"no\")",
        "yes",
    );
}

// ── Comprehension joins ────────────────────────────────────────────────────

#[test]
fn comprehension_cross_product() {
    assert_show(
        "base.sortBy (\\p -> p) (do
           a <- [1  2]
           b <- [10  20]
           yield (a + b))",
        "[11, 12, 21, 22]",
    );
}

#[test]
fn comprehension_equi_join() {
    // join employees to departments on a shared key
    assert_show(
        "with {\n           emps [{name \"a\"  dept \"eng\"}  {name \"b\"  dept \"ops\"}  {name \"c\"  dept \"eng\"}]\n           depts [{dname \"eng\"  floor 3}  {dname \"ops\"  floor 1}]\n         }\n         (base.sortBy (\\r -> r.name) (do\n            e <- emps\n            d <- depts\n            where e.dept == d.dname\n            yield {name e.name  floor d.floor}))",
        "[{floor 3  name a}, {floor 1  name b}, {floor 3  name c}]",
    );
}

#[test]
fn comprehension_with_let_and_where() {
    assert_show(
        "(do
           x <- [1  2  3  4  5]
           where x % 2 == 0
           yield (x * 10))",
        "[20, 40]",
    );
}

#[test]
fn comprehension_nested_yield_relation() {
    // flatMap: each element expands to a sub-relation
    assert_show(
        "(do
           n <- [1  2  3]
           m <- [n  (n * 10)]
           yield m)",
        "[1, 10, 2, 20, 3, 30]",
    );
}