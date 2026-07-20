//! Embedded `data` declarations inside record VALUE literals:
//!   {data Status = Open {} | Done {}
//!    answer 42}
//! The declaration contributes a field named after the data type whose value is
//! erased to unit at runtime, but whose constructors stay reachable through
//! field access (`r.Status.Open`) and whose type name enters type scope
//! (`x : Status`).
//!
//! CONFINEMENT: nothing inside a record leaks into the enclosing namespace.
//! The type name and constructors are reachable ONLY through the record value
//! (`r.Status.Open`) or a `with` peel (which scopes them to the body). A bare
//! `Open {}` or `x : Status` outside such a peel is an error.

use knot::diagnostic::Diagnostic;

fn check_src(src: &str) -> Vec<Diagnostic> {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (mut module, parse_diags) = parser.parse_module();
    assert!(
        parse_diags.is_empty(),
        "unexpected parse diagnostics: {:?}",
        parse_diags
    );
    knot_compiler::base::inject_prelude(&mut module);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, _monad, _type_info, _local, _targets, _refined, _from_json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields, _with_fields, _ty_args, _implicit_refs) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn record_data_nullary_ctor_access_compiles() {
    let diags = check_src(
        "main = with {r {data Status = Open {} | Done {}}}\n\
         (println (r.Status.Open {}))\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_data_payload_ctor_access_compiles() {
    let diags = check_src(
        "main = with {r {data Status = Open {} | InProgress {assignee: Text} | Done {}}}\n\
         (println (r.Status.InProgress {assignee \"Bob\"}))\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_data_type_name_in_annotation_compiles() {
    // The data type name enters type scope: usable in a `with` field sig.
    let diags = check_src(
        "main = with {r {data Status = Open {} | Done {}}}\n\
         (with {s : Status\n\
         s (r.Status.Open {})} (println s))\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_data_parameterized_compiles() {
    let diags = check_src(
        "main = with {r {data Maybe a = None {} | Some {val: a}}}\n\
         (println (r.Maybe.Some {val 5}))\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_data_does_not_eat_next_field() {
    // The constructor list must not absorb the following `answer 42` field.
    let diags = check_src(
        "main = with {r {data Status = Open {} | Done {}\n\
         answer 42}} (println r.answer)\n",
    );
    assert_clean(&diags);
}

fn assert_has_error(diags: &[Diagnostic]) {
    assert!(
        !diags.is_empty(),
        "expected at least one diagnostic, got none"
    );
}

#[test]
fn record_data_ctor_does_not_leak_globally() {
    // A bare `Open {}` outside the record / a `with` peel must NOT resolve:
    // the constructor is confined to the record.
    let diags = check_src(
        "g = with {r {data Status = Open {} | Done {}}} 0\n\
         s = Open {}\n\
         main = println s\n",
    );
    assert_has_error(&diags);
}

#[test]
fn record_data_type_name_does_not_leak_globally() {
    // A top-level `x : Status` annotation referencing an embedded data type
    // must NOT resolve: the type name is confined to the record. (It should
    // fall through to an unknown/opaque constructor and then fail to unify
    // against the record-built value.)
    let diags = check_src(
        "g = with {r {data Status = Open {} | Done {}}} 0\n\
         s : Status\n\
         s = g\n\
         main = println s\n",
    );
    assert_has_error(&diags);
}

#[test]
fn record_data_with_peel_scopes_type_to_body() {
    // Inside a `with` peel over the record that directly contains the `data`,
    // the type name IS in scope for an annotation (`x : Status`) — one layer
    // of peeling. The constructor value is reached through the same record,
    // bound to a (lowercase) local so field access parses.
    let diags = check_src(
        "main = with {data Status = Open {} | Done {}\n\
         rec {data Status = Open {} | Done {}}}\n\
         (with {s : Status\n\
         s (rec.Status.Open {})} (println s))\n",
    );
    assert_clean(&diags);
}
