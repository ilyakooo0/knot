//! Embedded `data` declarations inside record VALUE literals:
//!   {data Status = Open {} | Done {}
//!    answer 42}
//! The declaration contributes a field named after the data type whose value is
//! erased to unit at runtime, but whose constructors stay reachable through
//! field access (`r.Status.Open`) and whose type name enters type scope
//! (`x : Status`).

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _from_json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields, _with_fields, _ty_args) =
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
