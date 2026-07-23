// TEMPORARY audit scratch. Delete after use.
use knot::format::format_module;
use knot::lexer::Lexer;
use knot::parser::Parser;
use knot::diagnostic::Severity;

fn parse(src: &str) -> Option<knot::ast::Module> {
    let (tokens, ld) = Lexer::new(src).tokenize();
    if ld.iter().any(|d| d.severity == Severity::Error) { return None; }
    let (m, pd) = Parser::new(src.to_string(), tokens).parse_module();
    if pd.iter().any(|d| d.severity == Severity::Error) { return None; }
    Some(m)
}
fn fmt(src: &str) -> Option<String> {
    let m = parse(src)?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| format_module(src, &m))).ok()
}

#[test]
fn comment_edge() {
    let cases: &[(&str, &str)] = &[
        ("internal+trailing",
         "main = do\n  -- inside\n  yield 1 -- tail last\ntype B = Int"),
        ("verbatim-then-nextdecl",
         "main = do\n  -- inside\n  yield 1\ntype B = Int -- b trail"),
        ("do-header-trailing",
         "main = do -- header\n  yield 1"),
        ("sig-trailing-body",
         "foo : Int -- sig\nfoo = 1 -- body"),
        ("between-3",
         "type A = Int\n-- c1\n-- c2\ntype B = Int"),
        ("comment-inside-string-not-comment",
         "s = \"http://x -- not a comment\"\ntype B = Int"),
        ("double-dash-in-string",
         "s = \"a--b\" -- real\ntype B = Int"),
        ("trailing-on-multiline-nonverbatim",
         "main =\n  foo -- after foo\ntype B = Int"),
        ("blankline-between-comments",
         "-- a\n\n-- b\ntype A = Int"),
        ("trailer-nonstandalone-lastline",
         "type A = Int\ntype B = Int -- last"),
        ("nested-verbatim-tabs",
         "main = do\n\t-- tabbed comment\n\tyield 1"),
        ("crlf-comments", "-- a\r\ntype A = Int\r\n-- b\r\ntype B = Int\r\n"),
        ("only-blank-and-comments", "\n\n-- lonely\n\n"),
        ("record-with-internal-comment",
         "x = {\n  a: 1, -- field a\n  b: 2\n}"),
        ("case-with-arm-comment",
         "x = case y of\n  A {} -> 1 -- arm a\n  B {} -> 2"),
    ];
    for (name, src) in cases {
        let out = match fmt(src) { Some(o) => o, None => { println!("=== {name}: PARSE/PANIC\n"); continue; } };
        println!("=== {name}\n--in--\n{src}\n--out--\n{out}");
        // crude comment count preservation (outside strings ignored roughly)
    }
}
