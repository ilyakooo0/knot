use knot_migrate::diagnostic::Severity;
use knot_migrate::format::format_module_inner;
use knot_migrate::lexer::Lexer;
use knot_migrate::parser::Parser;

fn migrate(src: &str) -> Result<Option<String>, String> {
    let (tokens, lex_diags) = Lexer::new(src).tokenize();
    if lex_diags.iter().any(|d| d.severity == Severity::Error) {
        return Err("lex error".into());
    }
    let parser = Parser::new(src.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    let errs: Vec<String> = parse_diags
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .map(|d| d.message.clone())
        .collect();
    if !errs.is_empty() {
        return Err(errs.join("; "));
    }
    let out = format_module_inner(src, &module);
    if out == src { Ok(None) } else { Ok(Some(out)) }
}

// ── Rust string-literal scanner ────────────────────────────────────

#[derive(Debug, Clone)]
struct StrLit {
    /// Byte range of the whole literal in the file (including delimiters).
    start: usize,
    end: usize,
    /// Byte range of the raw content (between quotes / hash-quote pairs).
    cstart: usize,
    cend: usize,
    raw: bool,
}

fn scan_string_literals(file: &str) -> Vec<StrLit> {
    let b = file.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let n = b.len();
    while i < n {
        let c = b[i] as char;
        match c {
            // line comment
            '/' if i + 1 < n && b[i + 1] == b'/' => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            // block comment (handle nesting)
            '/' if i + 1 < n && b[i + 1] == b'*' => {
                let mut depth = 1;
                i += 2;
                while i < n && depth > 0 {
                    if b[i] == b'/' && i + 1 < n && b[i + 1] == b'*' {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && i + 1 < n && b[i + 1] == b'/' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            // char literal or lifetime — a quote followed by not-a-quote is a
            // char only if it closes quickly; lifetimes like 'a are common in
            // these files? Assume chars: 'x' or '\n'. Skip naive.
            '\'' => {
                // try to skip a char literal; lifetimes we leave (they don't
                // contain quotes so scanning continues safely)
                if i + 2 < n && b[i + 2] == b'\'' {
                    i += 3;
                } else if i + 3 < n && b[i + 1] == b'\\' && b[i + 3] == b'\'' {
                    i += 4;
                } else {
                    i += 1; // lifetime
                }
            }
            'r' if i + 1 < n && (b[i + 1] == b'#' || b[i + 1] == b'"') => {
                // raw string r"..." or r#"..."# (maybe more hashes)
                let mut j = i + 1;
                let mut hashes = 0;
                while j < n && b[j] == b'#' {
                    hashes += 1;
                    j += 1;
                }
                if j < n && b[j] == b'"' {
                    let cstart = j + 1;
                    // find closing " followed by `hashes` '#'
                    let mut k = cstart;
                    let mut end = None;
                    while k < n {
                        if b[k] == b'"' {
                            let mut ok = true;
                            for h in 0..hashes {
                                if k + 1 + h >= n || b[k + 1 + h] != b'#' {
                                    ok = false;
                                    break;
                                }
                            }
                            if ok {
                                end = Some((k, k + 1 + hashes));
                                break;
                            }
                        }
                        k += 1;
                    }
                    if let Some((cend, lend)) = end {
                        out.push(StrLit {
                            start: i,
                            end: lend,
                            cstart,
                            cend,
                            raw: true,
                        });
                        i = lend;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            '"' => {
                let cstart = i + 1;
                let mut k = cstart;
                while k < n {
                    if b[k] == b'\\' {
                        k += 2;
                    } else if b[k] == b'"' {
                        break;
                    } else {
                        k += 1;
                    }
                }
                if k < n {
                    out.push(StrLit {
                        start: i,
                        end: k + 1,
                        cstart,
                        cend: k,
                        raw: false,
                    });
                    i = k + 1;
                } else {
                    break;
                }
            }
            _ => i += 1,
        }
    }
    out
}

fn escape_for_rust(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

/// Heuristic: does this string look like knot source with OLD record syntax?
fn looks_like_old_knot(src: &str) -> bool {
    // old record expr/pattern/with: `{name:` (lowercase field, colon)
    // but not a record TYPE. We can't perfectly distinguish; migration is
    // AST-driven so types are preserved anyway. Gate on presence of `{x:`.
    let bytes = src.as_bytes();
    for i in 0..bytes.len().saturating_sub(2) {
        if bytes[i] == b'{' {
            let mut j = i + 1;
            while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            if j < bytes.len() && (bytes[j] as char).is_ascii_lowercase() {
                let mut k = j;
                while k < bytes.len()
                    && ((bytes[k] as char).is_ascii_alphanumeric() || bytes[k] == b'_')
                {
                    k += 1;
                }
                // skip ws
                let mut m = k;
                while m < bytes.len() && (bytes[m] as char) == ' ' {
                    m += 1;
                }
                if m < bytes.len() && bytes[m] == b':' {
                    return true;
                }
            }
        }
    }
    false
}

fn main() {
    let mut changed_files = 0;
    let mut migrated_lits = 0;
    let mut failures: Vec<String> = Vec::new();
    for path in std::env::args().skip(1) {
        let text = std::fs::read_to_string(&path).expect("read");
        let lits = scan_string_literals(&text);
        let mut out = text.clone();
        let mut file_changed = false;
        // apply edits from the end so byte ranges stay valid
        for lit in lits.iter().rev() {
            let content = &text[lit.cstart..lit.cend];
            if !looks_like_old_knot(content) {
                continue;
            }
            match migrate(content) {
                Ok(Some(new)) => {
                    let replacement = if lit.raw {
                        // need raw delimiter that doesn't clash: content may
                        // contain `"#`? use r##"..."## if content has "#
                        let delim = if new.contains("\"#") { "##" } else { "#" };
                        let closing: String = std::iter::repeat('#')
                            .take(delim.len())
                            .collect();
                        format!("r{delim}\"{new}\"{closing}")
                    } else {
                        format!("\"{}\"", escape_for_rust(&new))
                    };
                    out.replace_range(lit.start..lit.end, &replacement);
                    migrated_lits += 1;
                    file_changed = true;
                }
                Ok(None) => {}
                Err(e) => {
                    failures.push(format!("{path}: bytes {}..{}: {e}", lit.cstart, lit.cend));
                }
            }
        }
        if file_changed {
            std::fs::write(&path, out).unwrap();
            changed_files += 1;
            println!("CHANGED {path}");
        }
    }
    println!("== files changed: {changed_files}, literals migrated: {migrated_lits}");
    if !failures.is_empty() {
        println!("== FAILURES ({})", failures.len());
        for f in &failures {
            println!("  {f}");
        }
    }
}
