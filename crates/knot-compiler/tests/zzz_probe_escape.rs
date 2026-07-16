//! TEMPORARY probe test for auditing. Delete after use.
use knot::diagnostic::Diagnostic;

fn parse(src: &str) -> knot::ast::Module {
    let lexer = knot::lexer::Lexer::new(src);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    assert!(parse_diags.is_empty(), "parse diags: {:?}", parse_diags);
    module
}

fn check_src(src: &str) -> Vec<Diagnostic> {
    let mut module = parse(src);
    knot_compiler::desugar::desugar(&mut module);
    let (diags, ..) = knot_compiler::infer::check(&mut module);
    diags
}

#[test]
fn probe_infer_mode_annot_forall_escape() {
    // g takes a monomorphic h and coerces it to forall a. a -> a via an
    // *inline* annotation in infer position. If accepted (no error), that is
    // a soundness hole: g addOne : forall a. a -> a, usable at any type.
    let src = r#"g = \h -> (h : forall a. a -> a)
addOne : Int -> Int
addOne = \x -> x + 1
bad : Text
bad = (g addOne) "hello"
main = println bad
"#;
    let diags = check_src(src);
    eprintln!("PROBE1 diags: {:#?}", diags);
    // We EXPECT this to be rejected. Report whether it is.
    assert!(
        !diags.is_empty(),
        "SOUNDNESS HOLE: infer-mode forall annotation accepted with no error"
    );
}

#[test]
fn probe_check_mode_annot_forall_escape() {
    // Same but where the annotation is in a checked position (return type).
    let src = r#"g : (Int -> Int) -> (forall a. a -> a)
g = \h -> h
main = println "x"
"#;
    let diags = check_src(src);
    eprintln!("PROBE2 diags: {:#?}", diags);
    assert!(!diags.is_empty(), "check-mode hole");
}
