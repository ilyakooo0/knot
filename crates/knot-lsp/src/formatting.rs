//! `textDocument/formatting`, `textDocument/rangeFormatting`, and
//! `textDocument/onTypeFormatting` handlers.

use lsp_types::*;

use crate::state::ServerState;
use crate::utils::offset_to_position;

/// UTF-16 code-unit length of a string slice — what LSP `Position::character`
/// requires (LSP defaults to UTF-16, and this server doesn't negotiate
/// `positionEncodingKind`). Using `str::len()` would emit byte counts, which
/// are wrong for any line containing non-ASCII characters.
fn utf16_len(s: &str) -> u32 {
    s.chars().map(|c| c.len_utf16() as u32).sum()
}

// ── Document Formatting ─────────────────────────────────────────────

pub(crate) fn handle_formatting(
    state: &ServerState,
    params: &DocumentFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let source = &doc.source;

    // Formatter:
    // 1. Convert tabs to spaces (2 spaces per tab)
    // 2. Trim trailing whitespace from all lines
    // 3. Normalize blank lines between top-level declarations (exactly one blank line)
    // 4. Collapse consecutive blank lines inside blocks to at most one
    // 5. Ensure trailing newline
    // 6. Normalize imports (single blank line after import block)
    // 7. Sort the leading import block alphabetically
    // 8. Normalize whitespace inside expressions (commas, arrows) on a per-line
    //    basis — full AST pretty-printing is deferred since it requires a
    //    layout-aware printer for do blocks, case arms, record literals, etc.

    // Convert tabs to spaces first, then sort imports.
    let source = &normalize_imports(&source.replace('\t', "  "));
    let lines: Vec<&str> = source.split('\n').collect();
    let mut result_lines: Vec<String> = Vec::with_capacity(lines.len());

    // Compute line ranges for each top-level declaration
    let mut decl_line_ranges: Vec<(u32, u32)> = Vec::new();
    for decl in &doc.module.decls {
        let start = offset_to_position(source, decl.span.start);
        let end = offset_to_position(source, decl.span.end);
        decl_line_ranges.push((start.line, end.line));
    }
    // Also track import line ranges
    let mut import_line_ranges: Vec<(u32, u32)> = Vec::new();
    for imp in &doc.module.imports {
        let start = offset_to_position(source, imp.span.start);
        let end = offset_to_position(source, imp.span.end);
        import_line_ranges.push((start.line, end.line));
    }

    // Merge all block ranges (imports + declarations) sorted by start line
    let mut block_ranges: Vec<(u32, u32)> = Vec::new();
    block_ranges.extend_from_slice(&import_line_ranges);
    block_ranges.extend_from_slice(&decl_line_ranges);
    block_ranges.sort_by_key(|r| r.0);

    let mut i = 0;
    while i < lines.len() {
        let line_num = i as u32;

        // Check if this line is between two top-level blocks (a gap line)
        let in_block = block_ranges
            .iter()
            .any(|(start, end)| line_num >= *start && line_num <= *end);
        let prev_block_end = block_ranges
            .iter()
            .filter(|(_, end)| *end < line_num)
            .max_by_key(|(_, end)| *end);
        let next_block_start = block_ranges
            .iter()
            .filter(|(start, _)| *start > line_num)
            .min_by_key(|(start, _)| *start);

        if !in_block && lines[i].trim().is_empty() {
            // We're in a gap between blocks — check if this is part of
            // a run of blank lines that should be collapsed to exactly one
            let gap_start = i;
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            // Only emit a blank line if there are blocks on both sides
            if prev_block_end.is_some() && next_block_start.is_some() {
                result_lines.push(String::new());
            } else if prev_block_end.is_some() {
                // Trailing blank lines at end — skip (trailing newline added later)
            } else {
                // Leading blank lines — preserve one at most
                if gap_start == 0 {
                    // skip leading blank lines
                } else {
                    result_lines.push(String::new());
                }
            }
            continue;
        }

        // Collapse consecutive blank lines inside blocks to at most one
        if lines[i].trim().is_empty() && in_block {
            let mut blank_count = 0;
            while i < lines.len() && lines[i].trim().is_empty() {
                blank_count += 1;
                i += 1;
            }
            if blank_count > 0 {
                result_lines.push(String::new());
            }
            continue;
        }

        // Trim trailing whitespace and apply per-line spacing normalization.
        result_lines.push(normalize_line_spacing(lines[i].trim_end()));
        i += 1;
    }

    // Ensure trailing newline
    if result_lines.last().map_or(true, |l| !l.is_empty()) {
        result_lines.push(String::new());
    }

    let formatted = result_lines.join("\n");

    // Only return edits if something changed
    if formatted == *source {
        return None;
    }

    // Replace entire document
    let last_line = lines.len().saturating_sub(1) as u32;
    let last_col = lines.last().map_or(0, |l| utf16_len(l));
    Some(vec![TextEdit {
        range: Range {
            start: Position::new(0, 0),
            end: Position::new(last_line, last_col),
        },
        new_text: formatted,
    }])
}

/// Sort the leading run of `import ...` lines alphabetically (case-insensitive).
/// Only affects a contiguous block at the very top of the file (after any
/// initial blank lines or comments).
fn normalize_imports(source: &str) -> String {
    let lines: Vec<&str> = source.split('\n').collect();
    let mut idx = 0;
    // Skip leading blank lines / line comments.
    while idx < lines.len() {
        let trimmed = lines[idx].trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            idx += 1;
        } else {
            break;
        }
    }
    let block_start = idx;
    while idx < lines.len() && lines[idx].trim_start().starts_with("import ") {
        idx += 1;
    }
    let block_end = idx;
    if block_end - block_start < 2 {
        return source.to_string();
    }
    let mut imports: Vec<&str> = lines[block_start..block_end].to_vec();
    let mut sorted = imports.clone();
    sorted.sort_by(|a, b| a.trim().to_lowercase().cmp(&b.trim().to_lowercase()));
    if imports == sorted {
        return source.to_string();
    }
    imports = sorted;
    let mut out = Vec::with_capacity(lines.len());
    out.extend_from_slice(&lines[..block_start]);
    out.extend_from_slice(&imports);
    out.extend_from_slice(&lines[block_end..]);
    out.join("\n")
}

/// Normalize whitespace inside a single line. Conservative — only fixes
/// patterns that don't change semantics regardless of context. Notably, this
/// runs PER LINE so it can safely operate on any code without parsing.
fn normalize_line_spacing(line: &str) -> String {
    // Skip lines that look like they contain string literals to avoid
    // mangling content. A real formatter would parse the line; here we just
    // bail when in doubt.
    if line.contains('"') {
        return line.to_string();
    }
    // Skip line-comment lines (and tail content of `--` comments) — we don't
    // want to mangle prose. Comments starting mid-line are handled by
    // splitting at the first `--` boundary; the prefix is normalized, the
    // suffix preserved verbatim.
    let (code, comment) = match find_line_comment_start(line) {
        Some(idx) => (&line[..idx], Some(&line[idx..])),
        None => (line, None),
    };

    // Preserve leading indentation literally — it's already validated by the
    // tab-to-space pass. Only normalize what comes after.
    let leading_ws_end = code
        .as_bytes()
        .iter()
        .position(|b| !matches!(*b, b' ' | b'\t'))
        .unwrap_or(code.len());
    let indent = &code[..leading_ws_end];
    let body = &code[leading_ws_end..];

    let normalized_body = normalize_body(body);
    let mut out = String::with_capacity(line.len());
    out.push_str(indent);
    out.push_str(&normalized_body);
    if let Some(c) = comment {
        // Preserve a single space gap between code and the trailing comment.
        if !out.ends_with(' ') && !out.is_empty() {
            out.push(' ');
        }
        out.push_str(c);
    }
    out
}

/// Find the byte index of the first `--` line-comment marker in a code line,
/// ignoring `--` inside string literals (those are filtered out by the caller
/// already, but defending against future relaxations of that policy).
fn find_line_comment_start(line: &str) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'-' && bytes[i + 1] == b'-' {
            // The `-` could be part of a binary subtraction or a `->` arrow;
            // require it to be preceded by whitespace or start-of-line for
            // a comment context.
            if i == 0 || matches!(bytes[i - 1], b' ' | b'\t') {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Normalize whitespace inside the non-indented body of a line. Operators we
/// touch: `,` `->` `<-` `=>`, plus collapsing runs of internal spaces. We
/// leave `=` alone — distinguishing `=` from `==`/`<=`/`>=`/`/=` reliably
/// without a parser is more trouble than it's worth.
fn normalize_body(body: &str) -> String {
    let mut out = String::with_capacity(body.len());
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Collapse runs of internal whitespace to a single space — but only
        // for runs that don't separate operators we're about to insert space
        // around. We handle this by emitting at most one space.
        if matches!(b, b' ' | b'\t') {
            if !out.ends_with(' ') {
                out.push(' ');
            }
            i += 1;
            continue;
        }
        // `,` followed by non-space, non-`)`, non-`]`, non-`}` → insert space.
        if b == b',' {
            out.push(',');
            i += 1;
            if i < bytes.len()
                && !matches!(bytes[i], b' ' | b')' | b']' | b'}' | b'\n' | b'\r' | b'\t')
            {
                out.push(' ');
            }
            continue;
        }
        // `->`: ensure single space before and after.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'>' {
            if !out.ends_with(' ') && !out.is_empty() {
                let last = out.chars().last().unwrap_or(' ');
                if !matches!(last, '(' | '[' | '{') {
                    out.push(' ');
                }
            }
            out.push_str("->");
            i += 2;
            if i < bytes.len()
                && !matches!(bytes[i], b' ' | b')' | b']' | b'}' | b'\n' | b'\r' | b'\t')
            {
                out.push(' ');
            }
            continue;
        }
        // `<-`: ditto, used in do-block monadic binds.
        if b == b'<' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            if !out.ends_with(' ') && !out.is_empty() {
                out.push(' ');
            }
            out.push_str("<-");
            i += 2;
            if i < bytes.len()
                && !matches!(bytes[i], b' ' | b')' | b']' | b'}' | b'\n' | b'\r' | b'\t')
            {
                out.push(' ');
            }
            continue;
        }
        // `=>`: trait constraint arrow; same treatment.
        if b == b'=' && i + 1 < bytes.len() && bytes[i + 1] == b'>' {
            if !out.ends_with(' ') && !out.is_empty() {
                out.push(' ');
            }
            out.push_str("=>");
            i += 2;
            if i < bytes.len()
                && !matches!(bytes[i], b' ' | b')' | b']' | b'}' | b'\n' | b'\r' | b'\t')
            {
                out.push(' ');
            }
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    out
}

// ── Range Formatting ────────────────────────────────────────────────

pub(crate) fn handle_range_formatting(
    state: &ServerState,
    params: &DocumentRangeFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let source = &doc.source;
    let tab_size = params.options.tab_size as usize;
    let use_spaces = params.options.insert_spaces;

    let start_line = params.range.start.line as usize;
    let end_line = params.range.end.line as usize;

    let lines: Vec<&str> = source.split('\n').collect();
    let mut edits = Vec::new();

    let mut prev_was_blank = false;
    for i in start_line..=end_line.min(lines.len().saturating_sub(1)) {
        let line = lines[i];

        // Convert tabs to spaces
        if use_spaces && line.contains('\t') {
            let indent_str = " ".repeat(tab_size);
            let new_line = line.replace('\t', &indent_str);
            let trimmed = new_line.trim_end();
            edits.push(TextEdit {
                range: Range {
                    start: Position::new(i as u32, 0),
                    end: Position::new(i as u32, utf16_len(line)),
                },
                new_text: trimmed.to_string(),
            });
            prev_was_blank = trimmed.is_empty();
            continue;
        }

        // Collapse consecutive blank lines to at most one
        if line.trim().is_empty() {
            if prev_was_blank {
                edits.push(TextEdit {
                    range: Range {
                        start: Position::new(i as u32, 0),
                        end: Position::new((i + 1).min(lines.len()) as u32, 0),
                    },
                    new_text: String::new(),
                });
                continue;
            }
            prev_was_blank = true;
        } else {
            prev_was_blank = false;
        }

        // Trim trailing whitespace
        let trimmed = line.trim_end();
        if trimmed.len() != line.len() {
            edits.push(TextEdit {
                range: Range {
                    start: Position::new(i as u32, utf16_len(trimmed)),
                    end: Position::new(i as u32, utf16_len(line)),
                },
                new_text: String::new(),
            });
        }
    }

    if edits.is_empty() {
        None
    } else {
        Some(edits)
    }
}

// ── On-Type Formatting ──────────────────────────────────────────────

pub(crate) fn handle_on_type_formatting(
    state: &ServerState,
    params: &DocumentOnTypeFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document_position.text_document.uri)?;
    let source = &doc.source;
    let pos = params.text_document_position.position;

    // We triggered on '\n' — look at the previous line to decide indentation
    if pos.line == 0 {
        return None;
    }

    let prev_line_idx = (pos.line - 1) as usize;
    let lines: Vec<&str> = source.split('\n').collect();
    if prev_line_idx >= lines.len() {
        return None;
    }

    let prev_line = lines[prev_line_idx];
    let prev_trimmed = prev_line.trim();

    // Measure the previous line's indentation
    let prev_indent = prev_line.len() - prev_line.trim_start().len();

    // Keywords that should increase indent on the next line
    let should_indent = prev_trimmed == "do"
        || prev_trimmed.ends_with(" do")
        || prev_trimmed.ends_with(" of")
        || prev_trimmed == "where"
        || prev_trimmed.ends_with(" where")
        || prev_trimmed.ends_with(" then")
        || prev_trimmed.ends_with(" else")
        || prev_trimmed.ends_with("->")
        || prev_trimmed.ends_with('=')
        || (prev_trimmed.starts_with("impl ") && !prev_trimmed.contains('='));

    if !should_indent {
        return None;
    }

    let new_indent = prev_indent + 2;
    let current_line_idx = pos.line as usize;

    // Only add indent if the current line is empty or has less indentation
    if current_line_idx < lines.len() {
        let current_line = lines[current_line_idx];
        let current_indent = current_line.len() - current_line.trim_start().len();
        if current_indent >= new_indent && !current_line.trim().is_empty() {
            return None;
        }
    }

    let indent_str = " ".repeat(new_indent);
    Some(vec![TextEdit {
        range: Range {
            start: Position::new(pos.line, 0),
            end: Position::new(pos.line, pos.character),
        },
        new_text: indent_str,
    }])
}
