//! `textDocument/prepareRename` and `textDocument/rename` handlers.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use lsp_types::*;

use knot::ast::Span;

use crate::analysis::get_or_parse_file_shared;
use crate::defs::resolve_definitions;
use crate::shared::scan_knot_files;
use crate::state::ServerState;
use crate::utils::{
    path_to_uri, position_to_offset, safe_slice, span_to_range, uri_to_path, word_at_position,
};

// ── Rename ──────────────────────────────────────────────────────────

pub(crate) fn handle_prepare_rename(
    state: &ServerState,
    params: &TextDocumentPositionParams,
) -> Option<PrepareRenameResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let pos = params.position;
    let offset = position_to_offset(&doc.source, pos);

    // Check if cursor is on a renameable symbol
    let word = word_at_position(&doc.source, pos)?;

    // Must be on a known definition or a reference to one
    let is_ref = doc
        .references
        .iter()
        .any(|(usage, _)| usage.start <= offset && offset < usage.end);
    let is_def = doc.definitions.values().any(|span| span.start <= offset && offset < span.end);

    if !is_ref && !is_def {
        return None;
    }

    // Return the word range
    let word_offset = position_to_offset(&doc.source, pos);
    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let start = (0..word_offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = (word_offset..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    let range = span_to_range(Span::new(start, end), &doc.source);
    Some(PrepareRenameResponse::RangeWithPlaceholder {
        range,
        placeholder: word.to_string(),
    })
}

pub(crate) fn handle_rename(
    state: &ServerState,
    params: &RenameParams,
) -> Option<WorkspaceEdit> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let new_name = &params.new_name;

    // Find the definition span
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        })?;

    let old_name = word_at_position(&doc.source, pos)?;
    let symbol_name = old_name.to_string();

    let mut changes: HashMap<Uri, Vec<TextEdit>> = HashMap::new();

    // Rename at definition site in current doc
    let def_text = safe_slice(&doc.source, def_span);
    if let Some(name_start) = def_text.find(old_name) {
        let name_span = Span::new(def_span.start + name_start, def_span.start + name_start + old_name.len());
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(name_span, &doc.source),
            new_text: new_name.clone(),
        });
    } else {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(def_span, &doc.source),
            new_text: new_name.clone(),
        });
    }

    // Rename all usage sites in current doc
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(*usage_span, &doc.source),
                new_text: new_name.clone(),
            });
        }
    }

    // Cross-file: rename in all other open documents
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        // Rename definition if it exists in this doc
        if let Some(other_def) = other_doc.definitions.get(&symbol_name) {
            let def_text = safe_slice(&other_doc.source, *other_def);
            if let Some(name_start) = def_text.find(old_name) {
                let name_span = Span::new(other_def.start + name_start, other_def.start + name_start + old_name.len());
                changes.entry(other_uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(name_span, &other_doc.source),
                    new_text: new_name.clone(),
                });
            }
        }
        // Rename usages
        for (usage_span, target_span) in &other_doc.references {
            let target_name = safe_slice(&other_doc.source, *target_span);
            if other_doc.definitions.get(&symbol_name) == Some(target_span) || target_name == symbol_name {
                changes.entry(other_uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, &other_doc.source),
                    new_text: new_name.clone(),
                });
            }
        }
    }

    // Workspace-wide: scan .knot files not currently open that may import this symbol
    if let Some(root) = &state.workspace_root {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if let Ok(files) = scan_knot_files(root) {
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let (module, file_source) =
                    match get_or_parse_file_shared(&canonical, &state.import_cache) {
                        Some(v) => v,
                        None => continue,
                    };
                // Quick check: does the file contain the symbol name at all?
                if !file_source.contains(old_name) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let (defs, refs, _) = resolve_definitions(&module, &file_source);

                // Rename at definition sites
                if let Some(def_span) = defs.get(&symbol_name) {
                    let def_text = safe_slice(&file_source, *def_span);
                    if let Some(name_start) = def_text.find(old_name) {
                        let name_span = Span::new(
                            def_span.start + name_start,
                            def_span.start + name_start + old_name.len(),
                        );
                        changes
                            .entry(file_uri.clone())
                            .or_default()
                            .push(TextEdit {
                                range: span_to_range(name_span, &file_source),
                                new_text: new_name.clone(),
                            });
                    }
                }
                // Rename at usage sites
                for (usage_span, target_span) in &refs {
                    if defs.get(&symbol_name) == Some(target_span) {
                        changes
                            .entry(file_uri.clone())
                            .or_default()
                            .push(TextEdit {
                                range: span_to_range(*usage_span, &file_source),
                                new_text: new_name.clone(),
                            });
                    }
                }
            }
        }
    }

    Some(WorkspaceEdit {
        changes: Some(changes),
        ..Default::default()
    })
}
