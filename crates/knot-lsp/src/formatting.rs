//! `textDocument/formatting`, `textDocument/rangeFormatting`, and
//! `textDocument/onTypeFormatting` handlers.
//!
//! Document formatting delegates to [`knot::format::format_module`] — the
//! same AST-based pretty printer that powers `knot fmt`. Range formatting
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

/// Sane upper bound on a single indent step. Clients normally send 2/4/8;
/// anything larger is either a misconfiguration or an attempt to wedge the
/// server with a giant `" ".repeat(tab_size)` allocation.
const MAX_TAB_SIZE: usize = 16;

/// Clamp the client-supplied tab size to a sane range. The LSP type is `u32`,
/// and clients have been observed to send `0` (which would suppress all
/// indentation) and absurdly large values (which would explode `repeat()`).
fn clamp_tab_size(raw: u32) -> usize {
    (raw as usize).clamp(1, MAX_TAB_SIZE)
}

// ── Document Formatting ─────────────────────────────────────────────

pub(crate) fn handle_formatting(
    state: &ServerState,
    params: &DocumentFormattingParams,
) -> Option<Vec<TextEdit>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;

    // Staleness guard: `doc.source` is the last *analyzed* text; with the
    // 150–500ms analysis debounce, a format-on-save fired right after a
    // keystroke would otherwise return a whole-document replacement built
    // from text that's missing the user's latest edits — silently reverting
    // them. Formatting only needs a parse (not full analysis), so when
    // newer text is pending we format that text directly.
    if let Some(pending) = state
        .pending_sources
        .get(uri)
        .filter(|p| p.source != doc.source)
    {
        return format_whole_source(&pending.source);
    }

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

    Some(vec![whole_document_edit(source, formatted)])
}

/// Lex + parse `source` from scratch and return a whole-document formatting
/// edit against it. Used when the latest editor text hasn't been analyzed
/// yet. Returns `None` on any lex/parse error (don't rewrite a half-broken
/// file from a partial AST) or when the text is already formatted.
fn format_whole_source(source: &str) -> Option<Vec<TextEdit>> {
    let (tokens, lex_diags) = knot::lexer::Lexer::new(source).tokenize();
    if lex_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return None;
    }
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    if parse_diags
        .iter()
        .any(|d| d.severity == knot::diagnostic::Severity::Error)
    {
        return None;
    }
    let formatted = knot::format::format_module(source, &module);
    if formatted == source {
        return None;
    }
    Some(vec![whole_document_edit(source, formatted)])
}

/// Build a `TextEdit` replacing all of `source` with `new_text`.
fn whole_document_edit(source: &str, new_text: String) -> TextEdit {
    let lines: Vec<&str> = source.split('\n').collect();
    let last_line = lines.len().saturating_sub(1) as u32;
    let last_col = lines.last().map_or(0, |l| utf16_len(l));
    TextEdit {
        range: Range {
            start: Position::new(0, 0),
            end: Position::new(last_line, last_col),
        },
        new_text,
    }
}

// ── Range Formatting ────────────────────────────────────────────────

pub(crate) fn handle_range_formatting(
    state: &ServerState,
    params: &DocumentRangeFormattingParams,
) -> Option<Vec<TextEdit>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    // Prefer the pending (freshest) text over the last-analyzed source so
    // edits line up with what the editor currently displays. This handler
    // is purely line-based — it doesn't need the AST.
    let source = state
        .pending_sources
        .get(uri)
        .map(|p| p.source.as_str())
        .unwrap_or(&doc.source);
    let tab_size = clamp_tab_size(params.options.tab_size);
    let use_spaces = params.options.insert_spaces;

    let start_line = params.range.start.line as usize;
    let end_line = params.range.end.line as usize;

    let lines: Vec<&str> = source.split('\n').collect();
    let mut edits = Vec::new();

    let mut prev_was_blank = false;
    for i in start_line..=end_line.min(lines.len().saturating_sub(1)) {
        // `split('\n')` on CRLF leaves a trailing `\r` on each line. Strip it
        // before processing so `trim_end` doesn't treat `\r` as trailing
        // whitespace (which would produce a spurious edit on every CRLF line,
        // deleting the carriage return and corrupting line endings), and so
        // `utf16_len(line)` doesn't count the `\r` (LSP positions cannot
        // denote `\r|\n`).
        let line = lines[i].strip_suffix('\r').unwrap_or(lines[i]);

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
    let uri = &params.text_document_position.text_document.uri;
    let doc = state.documents.get(uri)?;
    // Same staleness consideration as range formatting: this fires right
    // after a keystroke (the newline that triggered it), so the analyzed
    // source is almost always behind the editor. Use the pending text.
    let source = state
        .pending_sources
        .get(uri)
        .map(|p| p.source.as_str())
        .unwrap_or(&doc.source);
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

    // Measure the previous line's indentation (in characters, so tab-based
    // indents are counted correctly for stacking the same indent unit).
    let prev_indent = prev_line.len() - prev_line.trim_start().len();

    // Honor the editor's formatting options rather than hardcoding 2 spaces.
    let tab_size = clamp_tab_size(params.options.tab_size);
    let use_spaces = params.options.insert_spaces;
    let step = if use_spaces { tab_size } else { 1 };

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

    let new_indent = prev_indent + step;
    let current_line_idx = pos.line as usize;

    // Only add indent if the current line is empty or has less indentation
    if current_line_idx < lines.len() {
        let current_line = lines[current_line_idx];
        let current_indent = current_line.len() - current_line.trim_start().len();
        if current_indent >= new_indent && !current_line.trim().is_empty() {
            return None;
        }
    }

    let indent_unit = if use_spaces { " " } else { "\t" };
    let indent_str = indent_unit.repeat(new_indent);
    Some(vec![TextEdit {
        range: Range {
            start: Position::new(pos.line, 0),
            end: Position::new(pos.line, pos.character),
        },
        new_text: indent_str,
    }])
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PendingSource;
    use crate::test_support::TestWorkspace;

    fn fmt_params(uri: &Uri) -> DocumentFormattingParams {
        DocumentFormattingParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            options: FormattingOptions {
                tab_size: 2,
                insert_spaces: true,
                ..Default::default()
            },
            work_done_progress_params: Default::default(),
        }
    }

    /// Regression: `handle_formatting` built a whole-document replacement
    /// from the last *analyzed* source. With the 150–500ms analysis
    /// debounce, format-on-save fired right after a keystroke replaced the
    /// editor buffer with text missing the user's latest edits. When newer
    /// text is pending, formatting must operate on that text.
    #[test]
    fn formatting_uses_pending_source_not_stale_analysis() {
        let mut ws = TestWorkspace::new();
        // Analyzed (stale) text mentions `oldName`; the pending text the
        // editor actually holds mentions `newName`.
        let uri = ws.open("main", "oldName   =   1\n");
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: "newName   =   2\n".into(),
                version: Some(2),
            },
        );
        let edits = handle_formatting(&ws.state, &fmt_params(&uri))
            .expect("pending text is unformatted, so an edit is produced");
        assert_eq!(edits.len(), 1);
        assert!(
            edits[0].new_text.contains("newName"),
            "formatted output must come from the pending text; got: {:?}",
            edits[0].new_text
        );
        assert!(
            !edits[0].new_text.contains("oldName"),
            "formatting must never resurrect the stale analyzed text; got: {:?}",
            edits[0].new_text
        );
    }

    /// When the pending text has parse errors, formatting must do nothing
    /// rather than fall back to a stale whole-document replacement.
    #[test]
    fn formatting_bails_when_pending_source_does_not_parse() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "x   =   1\n");
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: "x = = broken (((\n".into(),
                version: Some(2),
            },
        );
        let edits = handle_formatting(&ws.state, &fmt_params(&uri));
        assert!(
            edits.is_none(),
            "must not return edits built from stale or broken text; got: {edits:?}"
        );
    }

    /// Without pending edits, formatting still works off the analyzed doc.
    #[test]
    fn formatting_formats_analyzed_source_when_no_pending() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "x   =   1\n");
        let edits = handle_formatting(&ws.state, &fmt_params(&uri))
            .expect("unformatted source produces an edit");
        assert!(edits[0].new_text.contains("x = 1"), "got: {:?}", edits[0].new_text);
    }
}
