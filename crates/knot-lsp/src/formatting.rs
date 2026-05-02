//! `textDocument/formatting`, `textDocument/rangeFormatting`, and
//! `textDocument/onTypeFormatting` handlers.
//!
//! Document formatting delegates to [`knot::format::format_module`] — the
//! same AST-based pretty printer that powers `knotc fmt`. Range formatting
//! still does the heuristic line-level cleanup (trim trailing whitespace,
//! collapse consecutive blanks) since rendering an arbitrary subrange would
//! need expression-level slicing the printer can't currently provide.

use lsp_types::*;

use crate::state::ServerState;

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

    // Skip when the document didn't parse cleanly — we don't want to
    // rewrite a half-broken file from a partial AST.
    let has_parse_errors = doc
        .knot_diagnostics
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error);
    if has_parse_errors {
        return None;
    }

    let formatted = knot::format::format_module(source, &doc.module);
    if formatted == *source {
        return None;
    }

    let lines: Vec<&str> = source.split('\n').collect();
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
