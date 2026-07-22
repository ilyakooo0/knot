//! Embedded `type` alias declarations inside record VALUE literals:
//!   {type Pair a b = {fst: a, snd: b}
//!    answer 42}
//! The declaration contributes a field named after the alias whose value is the
//! (erased) first-class type constructor, typed by its KIND (`Type` for 0
//! params, `Type -> Type` for 1, …). The alias is brought into type scope; the
//! field is erased to unit at codegen.

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _from_json, _elem,  _show_units, _sum_floats, _rel_fields, _with_fields, _ty_args, _implicit_refs, _implicit_dict_args) =
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
fn record_type_alias_nullary_compiles() {
    let diags = check_src(
        "main = with {r {type Point = {x: Int 1, y: Int 1}\n                answer 42}} (println r.answer)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_type_alias_parameterized_compiles() {
    let diags = check_src(
        "main = with {r {type Pair a b = {fst: a, snd: b}\n                answer 7}} (println r.answer)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_type_alias_body_does_not_eat_next_field() {
    // The alias body `{fst: a, snd: b}` must not absorb the following
    // `name 99` field as a type argument (the `record_value_sig_type` guard).
    let diags = check_src(
        "main = with {r {type Pair a b = {fst: a, snd: b}\n                name 99}} (println r.name)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_type_ctor_field_accessible_and_erased() {
    // The type-constructor field can be extracted by name (uppercase field
    // access); it is erased (unit) at runtime but type-checks.
    let diags = check_src(
        "main = with {r {type Pair a b = {fst: a, snd: b}\n                answer 7}}\n         with {ctor r.Pair} (println r.answer)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_type_alias_coexists_with_other_fields_and_sigs() {
    // A `type` line, a sig line, and plain fields in one literal.
    let diags = check_src(
        "main = with {r {type Point = {x: Int 1}\n                name : Text\n                name \"a\"\n                answer 42}} (println r.answer)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_type_alias_does_not_leak_globally() {
    // A top-level `x : Point` referencing an embedded alias must NOT resolve
    // outside the record / a `with` peel.
    let diags = check_src(
        "g = with {r {type Point = {x: Int 1}}} 0\n\
         s : Point\n\
         s = {x 5}\n\
         main = println s\n",
    );
    assert!(
        !diags.is_empty(),
        "embedded alias leaked into global type scope: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn record_type_alias_scoped_under_with() {
    // Inside a `with` peel over the record that directly contains the `type`,
    // the alias IS usable in an annotation (one layer of peeling).
    let diags = check_src(
        "getx = with {type Point = {x: Int 1}}\n\
         (\\(p : Point) -> p.x)\n\
         main = println (getx {x 5})\n",
    );
    assert_clean(&diags);
}
