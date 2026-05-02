//! Round-trip tests for the formatter: format every example, re-parse,
//! verify the AST shape is preserved, and check that formatting is
//! idempotent.

use std::path::{Path, PathBuf};

fn parse(source: &str) -> Result<knot::ast::Module, String> {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    if lex_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return Err("lex errors".into());
    }
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, diags) = parser.parse_module();
    let errs: Vec<String> = diags
        .iter()
        .filter(|d| d.severity == knot::diagnostic::Severity::Error)
        .map(|d| d.render(source, "<input>"))
        .collect();
    if !errs.is_empty() {
        return Err(format!("parse errors:\n{}", errs.join("\n")));
    }
    Ok(module)
}

/// `Debug`-printed module with `span` payloads stripped, so structural
/// equality tolerates any byte-position drift caused by formatting.
fn normalize(module: &knot::ast::Module) -> String {
    let raw = format!("{:#?}", module);
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c == 's' {
            let lookahead: String = chars.clone().take(11).collect();
            if lookahead.starts_with("pan: Span {") {
                let mut depth = 0;
                while let Some(c2) = chars.next() {
                    if c2 == '{' {
                        depth += 1;
                    } else if c2 == '}' {
                        if depth == 0 {
                            break;
                        }
                        depth -= 1;
                    }
                }
                while let Some(&c2) = chars.peek() {
                    if c2 == ',' || c2 == ' ' || c2 == '\n' {
                        chars.next();
                    } else {
                        break;
                    }
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples")
}

fn check(path: &Path) {
    let source = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    let m1 = parse(&source).unwrap_or_else(|e| panic!("parse {}: {}", path.display(), e));
    let f1 = knot::format::format_module(&source, &m1);
    let m2 = parse(&f1).unwrap_or_else(|e| {
        panic!(
            "{}: formatted output failed to parse:\n--- output ---\n{}\n--- error ---\n{}",
            path.display(),
            f1,
            e
        )
    });
    let n1 = normalize(&m1);
    let n2 = normalize(&m2);
    assert_eq!(
        n1,
        n2,
        "{}: AST changed after formatting",
        path.display()
    );
    let f2 = knot::format::format_module(&f1, &m2);
    assert_eq!(
        f1,
        f2,
        "{}: formatter is not idempotent",
        path.display()
    );
}

#[test]
fn round_trip_examples() {
    let dir = examples_dir();
    let mut count = 0usize;
    for entry in std::fs::read_dir(&dir).expect("read examples").flatten() {
        let p = entry.path();
        if p.is_file() && p.extension().map(|e| e == "knot").unwrap_or(false) {
            check(&p);
            count += 1;
        }
    }
    assert!(count > 0, "no examples found in {}", dir.display());
}
