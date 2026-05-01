//! `textDocument/references` handler.

use lsp_types::*;

use crate::state::ServerState;
use crate::utils::{position_to_offset, safe_slice, span_to_range, uri_to_path, word_at_position};

// ── Find References ─────────────────────────────────────────────────

pub(crate) fn handle_references(
    state: &ServerState,
    params: &ReferenceParams,
) -> Option<Vec<Location>> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);

    // Find the symbol name and definition span in current document
    let word = word_at_position(&doc.source, pos)?;
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| doc.definitions.get(word).copied())
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        })?;

    let symbol_name = word.to_string();
    let mut locations = Vec::new();

    // Include declaration if requested
    if params.context.include_declaration {
        locations.push(Location {
            uri: uri.clone(),
            range: span_to_range(def_span, &doc.source),
        });
    }

    // Find all usages in current document
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            locations.push(Location {
                uri: uri.clone(),
                range: span_to_range(*usage_span, &doc.source),
            });
        }
    }

    // Determine the canonical file that defines this symbol (for scoped matching)
    let defining_file = doc.import_origins.get(&symbol_name);
    let _current_file = uri_to_path(uri).and_then(|p| p.canonicalize().ok());

    // Cross-file: search all other open documents for references to the same name
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        // Scope check: if the symbol is imported, only match in documents that import
        // from the same origin, or that define the symbol themselves
        let _other_file = uri_to_path(other_uri).and_then(|p| p.canonicalize().ok());
        let is_defining_file = defining_file.is_some()
            && other_doc.import_defs.get(&symbol_name)
                .map(|(path, _)| Some(path.clone()) == doc.import_defs.get(&symbol_name).map(|(p, _)| p.clone()))
                .unwrap_or(false);
        let is_local_def = other_doc.definitions.contains_key(&symbol_name);
        let shares_origin = defining_file.is_none() // locally defined — match by name
            || is_defining_file
            || is_local_def
            || other_doc.import_origins.get(&symbol_name) == defining_file;

        if !shares_origin {
            continue;
        }

        for (usage_span, target_span) in &other_doc.references {
            let target_name = safe_slice(&other_doc.source, *target_span);
            if other_doc.definitions.get(&symbol_name) == Some(target_span) {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*usage_span, &other_doc.source),
                });
            } else if target_name == symbol_name {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*usage_span, &other_doc.source),
                });
            }
        }
        // Also check if the other doc has a definition of this name (for include_declaration)
        if params.context.include_declaration {
            if let Some(other_def) = other_doc.definitions.get(&symbol_name) {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*other_def, &other_doc.source),
                });
            }
        }
    }

    if locations.is_empty() {
        None
    } else {
        Some(locations)
    }
}
