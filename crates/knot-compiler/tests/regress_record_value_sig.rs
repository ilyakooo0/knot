//! Standalone type-signature lines inside record VALUE literals:
//!   {name : Text
//!    name "a"}
//! The sig attaches to its value field and is enforced against the value's
//! inferred type. Fields without sigs are inferred normally.

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
    let (diags, _monad, _type_info, _local, _targets, _refined, _from_json, _elem, _trait_calls, _show_units, _sum_floats, _rel_fields, _with_fields) =
        knot_compiler::infer::check(&mut module);
    diags
}

fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
    diags.iter().any(|d| d.message.contains(needle))
}

fn assert_clean(diags: &[Diagnostic]) {
    assert!(
        diags.is_empty(),
        "expected no diagnostics, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn record_value_sig_matching_compiles() {
    let diags = check_src(
        "main = with {r {name : Text\n         name \"a\"\n         age : Int 1\n         age 30}} (println r.name)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_value_sig_mismatch_is_error() {
    let diags = check_src(
        "main = with {r {name : Text\n         name 5}} (println 1)\n",
    );
    assert!(
        has_error(&diags, "type mismatch"),
        "expected type mismatch for sig'd field, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn record_value_sig_partial_fields_compile() {
    // Only `name` is annotated; `age` is inferred — partial sigs are allowed.
    let diags = check_src(
        "main = with {r {name : Text\n         name \"a\"\n         age 30}} (println r.name)\n",
    );
    assert_clean(&diags);
}

#[test]
fn record_value_sig_function_type_mismatch_is_error() {
    let diags = check_src(
        "main = with {r {handler : Int 1 -> Text\n         handler 5\n         name \"a\"}} (println 1)\n",
    );
    assert!(
        has_error(&diags, "type mismatch"),
        "expected fn-type mismatch for sig'd field, got: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}

#[test]
fn record_value_sig_drives_field_type() {
    // The sig fixes the field's type: using it at the sig type is fine.
    let diags = check_src(
        "main = with {r {name : Text\n         name \"a\"}} (println (r.name ++ \"!\"))\n",
    );
    assert_clean(&diags);
}
