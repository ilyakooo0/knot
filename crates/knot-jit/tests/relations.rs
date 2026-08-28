//! Exhaustive relation and record operation tests, driven through the JIT
//! harness with exact `show` assertions.
//!
//! Observed rendering conventions (from the runtime's `format_value`):
//! - `show` prints Text WITHOUT surrounding quotes, including inside records.
//! - A bare relation's rows print in an arbitrary (hash) order; tests over
//!   multi-row unsorted relations use `assert_show_set`.

mod harness;
use harness::{assert_compile_err, assert_show, assert_show_set};

// ── Construction & basic shape ───────────────────────────────────────────

#[test]
fn empty_relation() {
    assert_show("(base.the (Rel (Int 1)) [])", "[]");
}

#[test]
fn scalar_relation() {
    assert_show_set("[1  2  3]", &["1", "2", "3"]);
}

#[test]
fn record_relation() {
    assert_show_set(
        "[{name \"Ada\"  age 36}  {name \"Grace\"  age 17}]",
        &["{age 36  name Ada}", "{age 17  name Grace}"],
    );
}

#[test]
fn nested_relation_field() {
    assert_show(
        "{team \"eng\"  members [{name \"a\"}]}",
        "{members [{name a}]  team eng}",
    );
}

// ── filter / map / fold / bind ───────────────────────────────────────────

#[test]
fn filter_basic() {
    assert_show_set("base.filter (\\n -> n > 2) [1  2  3  4]", &["3", "4"]);
}

#[test]
fn filter_none() {
    assert_show("base.filter (\\n -> n > 100) [1  2  3]", "[]");
}

#[test]
fn map_basic() {
    assert_show_set("base.map (\\n -> n * 2) [1  2  3]", &["2", "4", "6"]);
}

#[test]
fn map_to_record() {
    assert_show_set(
        "base.map (\\n -> {sq (n * n)}) [1  2  3]",
        &["{sq 1}", "{sq 4}", "{sq 9}"],
    );
}

#[test]
fn fold_sum() {
    assert_show("base.fold (\\a b -> a + b) 0 [1  2  3  4]", "10");
}

#[test]
fn fold_empty() {
    assert_show("base.fold (\\a b -> a + b) 42 (base.the (Rel (Int 1)) [])", "42");
}

#[test]
fn bind_flatmap() {
    // base.bind takes the function first, then the relation.
    assert_show_set(
        "base.bind (\\n -> [n  (n * 10)]) [1  2  3]",
        &["1", "10", "2", "20", "3", "30"],
    );
}

// ── aggregates ───────────────────────────────────────────────────────────

#[test]
fn count_basic() {
    assert_show("base.count [1  2  3  4]", "4");
}

#[test]
fn count_empty() {
    assert_show("base.count (base.the (Rel (Int 1)) [])", "0");
}

#[test]
fn count_where() {
    assert_show("base.countWhere (\\n -> n % 2 == 0) [1  2  3  4  5  6]", "3");
}

#[test]
fn sum_ints() {
    assert_show("base.sum [1  2  3  4]", "10");
}

#[test]
fn avg_floats() {
    // base.avg takes a projection function: (a -> Float u) -> [a] -> Float u.
    assert_show("base.avg (\\x -> x) [1.0  2.0  3.0]", "2.0");
}

#[test]
fn min_on() {
    // minOn/maxOn return the bare projected key (b), NOT Maybe — and panic on
    // an empty relation (see the note at the bottom of this file).
    assert_show(
        "base.minOn (\\p -> p.age) [{name \"a\"  age 30}  {name \"b\"  age 25}]",
        "25",
    );
}

#[test]
fn max_on_basic() {
    assert_show(
        "base.maxOn (\\p -> p.age) [{name \"a\"  age 30}  {name \"b\"  age 25}]",
        "30",
    );
}

#[test]
fn min_max_scalars() {
    assert_show("base.min 3 7", "3");
    assert_show("base.max 3 7", "7");
}

// ── set operations ───────────────────────────────────────────────────────

#[test]
fn union_relations() {
    assert_show_set("base.union [1  2] [3  4]", &["1", "2", "3", "4"]);
}

#[test]
fn inter_relations() {
    assert_show_set("base.inter [1  2  3] [2  3  4]", &["2", "3"]);
}

#[test]
fn diff_relations() {
    assert_show_set("base.diff [1  2  3] [2]", &["1", "3"]);
}

// ── membership / quantifiers ─────────────────────────────────────────────

#[test]
fn elem_present() {
    assert_show("base.elem 2 [1  2  3]", "True");
}

#[test]
fn elem_absent() {
    assert_show("base.elem 9 [1  2  3]", "False");
}

#[test]
fn any_all() {
    assert_show("base.any (\\n -> n > 2) [1  2  3]", "True");
    assert_show("base.all (\\n -> n > 0) [1  2  3]", "True");
    assert_show("base.all (\\n -> n > 1) [1  2  3]", "False");
}

#[test]
fn single_row() {
    assert_show("base.single [7]", "Just {value 7}");
}

#[test]
fn single_empty_or_many() {
    // Nullary constructors show without a payload: `Nothing`, not `Nothing {}`.
    assert_show("base.single (base.the (Rel (Int 1)) [])", "Nothing");
    assert_show("base.single [1  2]", "Nothing");
}

#[test]
fn head_findfirst() {
    // Constructor payloads show with `: ` separators (field-style).
    assert_show("base.head [5  6]", "Just {value 5}");
    // findFirst is relation-FIRST: [a] -> (a -> Bool) -> Maybe a.
    assert_show("base.findFirst [1  2  3] (\\n -> n > 1)", "Just {value 2}");
}

// ── ordering / slicing (sorted relations have a fixed iteration order) ───

#[test]
fn sort_by() {
    assert_show(
        "base.sortBy (\\p -> p.age) [{n \"a\"  age 30}  {n \"b\"  age 20}]",
        "[{age 20  n b}, {age 30  n a}]",
    );
}

#[test]
fn sort_by_desc() {
    assert_show("base.sortByDesc (\\n -> n) [1  3  2]", "[3, 2, 1]");
}

#[test]
fn take_drop_sorted() {
    assert_show("base.take 2 (base.sortBy (\\n -> n) [3  1  2])", "[1, 2]");
    assert_show("base.drop 1 (base.sortBy (\\n -> n) [3  1  2])", "[2, 3]");
}

#[test]
fn reverse_text() {
    // base.reverse is Text -> Text only (there is no relation reverse).
    assert_show("base.reverse \"abc\"", "cba");
}

// NOTE: groupBy over a persisted *source relation evaluates correctly in a
// compiled binary but returns an unevaluated do-block under the in-process
// JIT harness (source-relation ops don't fully evaluate there). It needs the
// subprocess e2e path; verified manually: owner "x" (2 open) + owner "y" (0
// open) groups to `[{count: 1, owner: x}]` after the `where`. Deferring to a
// subprocess suite rather than assert a JIT artifact.

// ── records ──────────────────────────────────────────────────────────────

#[test]
fn record_field_access() {
    assert_show("{x 3  y 4}.x", "3");
}

#[test]
fn record_nested_access() {
    assert_show("{addr {city \"London\"  zip \"E1\"}}.addr.city", "London");
}

#[test]
fn unify_replaces_field() {
    assert_show("base.unify {x 3  y 4} {x 10}", "{x 10  y 4}");
}

#[test]
fn unify_adds_field() {
    assert_show("base.unify {x 3} {z 5}", "{x 3  z 5}");
}

#[test]
fn unify_right_biased() {
    assert_show("base.unify {a 1  b 2} {b 20  c 30}", "{a 1  b 20  c 30}");
}

#[test]
fn upsert_replaces() {
    assert_show(
        "base.upsertBy (\\c -> c.user == \"a\") {user \"a\"  n 5} [{user \"a\"  n 1}]",
        "[{n 5  user a}]",
    );
}

#[test]
fn upsert_inserts() {
    assert_show_set(
        "base.upsertBy (\\c -> c.user == \"b\") {user \"b\"  n 1} [{user \"a\"  n 1}]",
        &["{n 1  user a}", "{n 1  user b}"],
    );
}

// ── comprehension do-blocks ──────────────────────────────────────────────

#[test]
fn comprehension_filter() {
    assert_show_set(
        "(|\n  n <- [1  2  3  4  5]\n  where n % 2 == 0\n  yield n)",
        &["2", "4"],
    );
}

#[test]
fn comprehension_map() {
    assert_show_set("(|\n  n <- [1  2  3]\n  yield (n * n))", &["1", "4", "9"]);
}

#[test]
fn comprehension_join() {
    assert_show_set(
        "(|\n  a <- [1  2]\n  b <- [10  20]\n  yield (a + b))",
        &["11", "21", "12", "22"],
    );
}

// ── type errors surfaced ─────────────────────────────────────────────────

#[test]
fn mixed_type_relation_rejected() {
    assert_compile_err("[1  \"two\"]", "");
}

// ── minOn/maxOn on an empty relation (not asserted — aborts the process) ───
//
// base.minOn/base.maxOn on an EMPTY relation panic in the runtime
// ("knot runtime: min/max on empty relation" → SIGABRT). This is the
// DOCUMENTED contract: both base.md and knot.md specify `-> b` (the bare
// projected key, no `Nothing` case) and "aborts the program on an empty
// relation — guard with base.count first". It cannot be exercised in-process
// (it kills the test runner); verified manually against a compiled binary.
