fn main() {
    let src = "with {\ntype R = {n Text}\n}\n{x}";
    let lex = knot::lexer::Lexer::new(src).tokenize();
    let (_p, diags) = knot::parser::Parser::new(src.to_string(), lex.0).parse_file_expr();
    for d in &diags {
        println!("DIAG: {} @ {:?}", d.message, d.labels);
    }
}
