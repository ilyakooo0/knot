//! Regression tests for formatter round-trip bugs: associativity-changing
//! parenthesis loss, nested negation printing as a comment, broken inline
//! `case`/`serve` separators, unsupported `\x` text escapes, dropped parens
//! around inline `do`, and byte-string `\x` error recovery eating the
//! closing quote.

use knot::diagnostic::Severity;
use knot::lexer::TokenKind;

fn parse(source: &str) -> Result<knot::ast::Module, String> {
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    let lex_errs: Vec<String> = lex_diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.render(source, "<input>"))
        .collect();
    if !lex_errs.is_empty() {
        return Err(format!("lex errors:\n{}", lex_errs.join("\n")));
    }
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, diags) = parser.parse_module();
    let errs: Vec<String> = diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
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

/// Format `source`, verify the output reparses without diagnostics to the
/// same AST (modulo spans), and that formatting is idempotent. Returns the
/// formatted text for additional assertions.
fn check_str(label: &str, source: &str) -> String {
    let m1 = parse(source).unwrap_or_else(|e| panic!("parse {}: {}", label, e));
    let f1 = knot::format::format_module(source, &m1);
    let m2 = parse(&f1).unwrap_or_else(|e| {
        panic!(
            "{}: formatted output failed to parse:\n--- output ---\n{}\n--- error ---\n{}",
            label, f1, e
        )
    });
    let n1 = normalize(&m1);
    let n2 = normalize(&m2);
    assert_eq!(
        n1, n2,
        "{}: AST changed after formatting\n--- output ---\n{}",
        label, f1
    );
    let f2 = knot::format::format_module(&f1, &m2);
    assert_eq!(f1, f2, "{}: formatter is not idempotent", label);
    f1
}

// ── 1. Left-associative operators: same-precedence RHS keeps parens ──

#[test]
fn rhs_parens_subtraction() {
    let out = check_str("sub", "f = 10 - (5 - 2)\n");
    assert!(out.contains("10 - (5 - 2)"), "output: {}", out);
}

#[test]
fn rhs_parens_division() {
    let out = check_str("div", "g = 100 / (10 / 2)\n");
    assert!(out.contains("100 / (10 / 2)"), "output: {}", out);
}

#[test]
fn rhs_parens_modulo() {
    let out = check_str("mod", "b = 10 % (7 % 3)\n");
    assert!(out.contains("10 % (7 % 3)"), "output: {}", out);
}

#[test]
fn rhs_parens_pipe() {
    let out = check_str("pipe", "a = \\x f g -> x |> (f |> g)\n");
    assert!(out.contains("x |> (f |> g)"), "output: {}", out);
}

#[test]
fn rhs_parens_mixed_add_sub() {
    // `10 - (5 + 2)` must keep parens (3 vs 17); left-nested needs none.
    let out = check_str("sub_add", "f = 10 - (5 + 2)\ng = 10 - 5 + 2\n");
    assert!(out.contains("10 - (5 + 2)"), "output: {}", out);
    assert!(out.contains("10 - 5 + 2"), "output: {}", out);
}

#[test]
fn rhs_parens_mul_div() {
    // Integer division: `2 * (3 / 2)` != `2 * 3 / 2`.
    let out = check_str("mul_div", "f = 2 * (3 / 2)\n");
    assert!(out.contains("2 * (3 / 2)"), "output: {}", out);
}

#[test]
fn left_nested_stays_minimal() {
    // Left-nested same-precedence chains need no parens for left-assoc ops.
    let out = check_str("left_chain", "f = 10 - 5 - 2\ng = 100 / 10 / 2\n");
    assert!(out.contains("10 - 5 - 2"), "output: {}", out);
    assert!(out.contains("100 / 10 / 2"), "output: {}", out);
}

#[test]
fn concat_right_assoc_stays_minimal() {
    // `++` is right-associative: right-nested chains need no parens, but a
    // left-nested chain keeps its parens to preserve the AST shape.
    let out = check_str(
        "concat",
        "f = \"a\" ++ \"b\" ++ \"c\"\ng = (\"a\" ++ \"b\") ++ \"c\"\n",
    );
    assert!(out.contains("\"a\" ++ \"b\" ++ \"c\""), "output: {}", out);
    assert!(out.contains("(\"a\" ++ \"b\") ++ \"c\""), "output: {}", out);
}

#[test]
fn cmp_vs_rel_precedence() {
    // `<` binds tighter than `==` in the parser; `(a == b) < c` keeps parens.
    let out = check_str("cmp_rel", "f = \\a b c -> (a == b) < c\ng = \\a b c -> a == b < c\n");
    assert!(out.contains("(a == b) < c"), "output: {}", out);
    assert!(out.contains("a == b < c"), "output: {}", out);
}

// ── 2. Nested negation must not print `--` (line comment) ──

#[test]
fn nested_negation_not_a_comment() {
    let out = check_str("neg", "h = -(-3)\n");
    assert!(out.contains("-(-3)"), "output: {}", out);
    assert!(!out.contains("--"), "output contains a comment: {}", out);
}

#[test]
fn triple_negation() {
    let out = check_str("neg3", "h = -(-(-3))\n");
    assert!(!out.contains("--"), "output contains a comment: {}", out);
}

// ── 3. Inline case/serve: no leading `;` before the first arm/handler ──

#[test]
fn inline_case_in_binop() {
    let src = "f = \\x -> 1 + (case x of\n  A {} -> 1\n  B {} -> 2)\n";
    let out = check_str("inline_case", src);
    assert!(
        !out.contains("of;"),
        "leading separator before first arm: {}",
        out
    );
}

#[test]
fn inline_case_in_list() {
    // The case is a non-final list element: the formatter must keep the
    // parens, otherwise the last arm swallows `, 3` on reparse.
    let src = "f = \\x -> [(case x of\n  A {} -> 1\n  B {} -> 2), 3]\n";
    check_str("inline_case_list", src);
}

#[test]
fn inline_case_in_record() {
    let src = "f = \\x -> {v: (case x of\n  A {} -> 1\n  B {} -> 2), w: 3}\n";
    check_str("inline_case_record", src);
}

// ── 5. Inline `do` keeps parens in list/record positions ──

#[test]
fn inline_do_in_list() {
    let out = check_str("do_list", "f = [(do yield 1), 2]\n");
    assert!(out.contains("(do yield 1)"), "output: {}", out);
}

#[test]
fn inline_do_in_record() {
    check_str("do_record", "f = {a: (do yield 1), b: 2}\n");
}

#[test]
fn inline_do_in_if_branch() {
    check_str("do_if", "f = \\c -> if c then (do yield 1) else (do yield 2)\n");
}

// ── 4. Text escapes: only lexer-supported escapes are emitted ──

#[test]
fn nul_escape_round_trips() {
    let out = check_str("nul", "s = \"a\\0b\"\n");
    assert!(out.contains("\\0"), "output: {}", out);
    assert!(!out.contains("\\x00"), "output: {}", out);
}

#[test]
fn control_char_hex_escape_round_trips() {
    // \x01 (SOH) has no named escape; the formatter emits \x01 and the
    // lexer must read it back to the same value.
    let out = check_str("ctrl", "s = \"a\\x01b\"\n");
    assert!(out.contains("\\x01"), "output: {}", out);
}

#[test]
fn lexer_text_hex_escape() {
    let (tokens, diags) = knot::lexer::Lexer::new("\"a\\x00\\x1fb\"").tokenize();
    assert!(
        diags.iter().all(|d| d.severity != Severity::Error),
        "diags: {:?}",
        diags
    );
    match &tokens[0].kind {
        TokenKind::Text(s) => assert_eq!(s, "a\u{0}\u{1f}b"),
        other => panic!("expected Text token, got {:?}", other),
    }
}

#[test]
fn lexer_text_invalid_hex_escape_single_digit() {
    // One hex digit then closing quote: one diagnostic, recovered value
    // keeps the literal digit, string still terminates.
    let (tokens, diags) = knot::lexer::Lexer::new("\"\\x5\"").tokenize();
    let errs: Vec<_> = diags.iter().filter(|d| d.severity == Severity::Error).collect();
    assert_eq!(errs.len(), 1, "diags: {:?}", diags);
    match &tokens[0].kind {
        TokenKind::Text(s) => assert_eq!(s, "5"),
        other => panic!("expected Text token, got {:?}", other),
    }
}

#[test]
fn lexer_text_invalid_hex_escape_no_digit() {
    // `"\x"` — recovery must not eat the closing quote: exactly one
    // diagnostic (invalid hex escape), not a cascading "unterminated".
    let src = "s = \"\\x\"\n";
    let (tokens, diags) = knot::lexer::Lexer::new(src).tokenize();
    let errs: Vec<_> = diags.iter().filter(|d| d.severity == Severity::Error).collect();
    assert_eq!(errs.len(), 1, "diags: {:?}", diags);
    assert!(
        errs[0].message.contains("invalid hex escape"),
        "diags: {:?}",
        diags
    );
    assert!(
        tokens.iter().any(|t| matches!(t.kind, TokenKind::Text(_))),
        "tokens: {:?}",
        tokens
    );
}

// ── 6. Byte-string `\x` recovery must not consume the closing quote ──

#[test]
fn lexer_byte_string_invalid_hex_before_quote() {
    let src = "k = b\"\\x\"\n";
    let (tokens, diags) = knot::lexer::Lexer::new(src).tokenize();
    let errs: Vec<_> = diags.iter().filter(|d| d.severity == Severity::Error).collect();
    assert_eq!(
        errs.len(),
        1,
        "expected exactly one diagnostic, got: {:?}",
        diags
    );
    assert!(
        errs[0].message.contains("invalid hex escape"),
        "diags: {:?}",
        diags
    );
    assert!(
        !errs[0].message.contains("unterminated"),
        "bogus unterminated diagnostic: {:?}",
        diags
    );
    assert!(
        tokens.iter().any(|t| matches!(t.kind, TokenKind::Bytes(_))),
        "tokens: {:?}",
        tokens
    );
}

#[test]
fn lexer_byte_string_invalid_hex_before_newline() {
    // The bad-escape recovery must also not swallow a line break; the
    // unterminated diagnostic should point at this string, not eat the
    // next line.
    let src = "k = b\"\\x\nm = 1\n";
    let (_tokens, diags) = knot::lexer::Lexer::new(src).tokenize();
    assert!(
        diags.iter().any(|d| d.message.contains("invalid hex escape")),
        "diags: {:?}",
        diags
    );
    assert!(
        diags.iter().any(|d| d.message.contains("unterminated")),
        "diags: {:?}",
        diags
    );
}

#[test]
fn lexer_byte_string_valid_hex_still_works() {
    let (tokens, diags) = knot::lexer::Lexer::new("b\"\\x00\\xff\"").tokenize();
    assert!(
        diags.iter().all(|d| d.severity != Severity::Error),
        "diags: {:?}",
        diags
    );
    match &tokens[0].kind {
        TokenKind::Bytes(bs) => assert_eq!(bs, &vec![0x00u8, 0xff]),
        other => panic!("expected Bytes token, got {:?}", other),
    }
}

// ── Combined stress: everything at once still round-trips ──

#[test]
fn combined_round_trip() {
    let src = r#"f = 10 - (5 - 2)
g = 100 / (10 / 2)
b = 10 % (7 % 3)
h = -(-3)
s = "a\0b"
l = [(do yield 1), 2]
c = \x -> 1 + (case x of
  A {} -> 1
  B {} -> 2)
"#;
    check_str("combined", src);
}
