use std::fs;
use knot::lexer::Lexer;
use knot::parser::Parser;
use knot::diagnostic::Severity;

#[test]
fn check_examples_roundtrip() {
    let mut count = 0;
    let mut mismatches = Vec::new();
    for entry in fs::read_dir("../../examples").unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("knot") {
            continue;
        }
        let src = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let (tokens, lex_diags) = Lexer::new(&src).tokenize();
        if lex_diags.iter().any(|d| d.severity == Severity::Error) {
            continue;
        }
        let parser = Parser::new(src.clone(), tokens);
        let (m, parse_diags) = parser.parse_module();
        if parse_diags.iter().any(|d| d.severity == Severity::Error) {
            continue;
        }
        count += 1;
        let out = knot::format::format_module(&src, &m);
        let (tokens2, _) = Lexer::new(&out).tokenize();
        let parser2 = Parser::new(out.clone(), tokens2);
        let (m2, _) = parser2.parse_module();
        let strip = |s: String| {
            let mut out = String::new();
            let bytes = s.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                let rest = &s[i..];
                if rest.starts_with("span: Span {") {
                    match rest.find('}') {
                        Some(close) => {
                            i += close + 1;
                            if s[i..].starts_with(", ") { i += 2; }
                            continue;
                        }
                        None => { out.push_str(rest); break; }
                    }
                }
                let ch = rest.chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
            }
            out
        };
        let s1 = strip(format!("{:?}", m));
        let s2 = strip(format!("{:?}", m2));
        if s1 != s2 {
            mismatches.push(path.display().to_string());
        }
    }
    println!("Checked {} example files", count);
    println!("Mismatches: {:?}", mismatches);
    assert!(mismatches.is_empty(), "formatter round-trip mismatches: {:?}", mismatches);
}
