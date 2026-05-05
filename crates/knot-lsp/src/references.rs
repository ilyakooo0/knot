//! `textDocument/references` handler.

use std::collections::HashSet;
use std::path::PathBuf;

use lsp_types::*;

use crate::analysis::get_or_parse_file_shared;
use crate::shared::scan_knot_files_in_roots;
use crate::state::ServerState;
use crate::utils::{
    offset_to_position, path_to_uri, position_to_offset, safe_slice, span_to_range, uri_to_path,
    word_at_position,
};

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

    // Cross-file: scan workspace files that are not currently open. Cheap when
    // they're already cached in `import_cache`; falls back to a one-shot parse
    // otherwise. Limited to a name-equality match (we can't share the open
    // doc's `references` table for unopened files).
    let open_paths: HashSet<PathBuf> = state
        .documents
        .keys()
        .filter_map(|u| uri_to_path(u))
        .filter_map(|p| p.canonicalize().ok())
        .collect();
    let workspace_files =
        scan_knot_files_in_roots(&state.workspace_roots, state.workspace_root.as_deref());
    for file_path in workspace_files {
        let canonical = match file_path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };
        if open_paths.contains(&canonical) {
            continue;
        }
        let (_module, source) = match get_or_parse_file_shared(&canonical, &state.import_cache) {
            Some(p) => p,
            None => continue,
        };
        // Skip files whose path can't be encoded as a URI rather than
        // emitting a junk `file:///` location — locations with nonsense URIs
        // would silently mislead the editor's "Find References" pane.
        let Some(other_uri) = path_to_uri(&canonical) else {
            continue;
        };
        scan_word_occurrences(&source, &symbol_name, &other_uri, &mut locations);
    }

    if locations.is_empty() {
        None
    } else {
        // De-duplicate by (uri, range) — opens and unopened scans can overlap.
        let mut seen: HashSet<(String, u32, u32, u32, u32)> = HashSet::new();
        locations.retain(|loc| {
            let key = (
                loc.uri.to_string(),
                loc.range.start.line,
                loc.range.start.character,
                loc.range.end.line,
                loc.range.end.character,
            );
            seen.insert(key)
        });
        Some(locations)
    }
}

/// Append every whole-word occurrence of `name` in `source` as a Location.
fn scan_word_occurrences(source: &str, name: &str, uri: &Uri, out: &mut Vec<Location>) {
    let bytes = source.as_bytes();
    let needle = name.as_bytes();
    if needle.is_empty() || bytes.len() < needle.len() {
        return;
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok =
                i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                let start_pos = offset_to_position(source, i);
                let end_pos = offset_to_position(source, i + needle.len());
                out.push(Location {
                    uri: uri.clone(),
                    range: Range {
                        start: start_pos,
                        end: end_pos,
                    },
                });
                i += needle.len();
                continue;
            }
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;

    fn ref_params(uri: &Uri, position: Position, include_decl: bool) -> ReferenceParams {
        ReferenceParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: ReferenceContext {
                include_declaration: include_decl,
            },
        }
    }

    #[test]
    fn references_finds_all_call_sites() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"double = \x -> x * 2
a = double 1
b = double 2
main = println (show (double 3))
"#,
        );
        let doc = ws.doc(&uri);
        let def_pos = doc.source.find("double = ").expect("def");
        let pos = offset_to_position(&doc.source, def_pos);
        let locs = handle_references(&ws.state, &ref_params(&uri, pos, false))
            .expect("references found");
        // Three call sites: `double 1`, `double 2`, `double 3`.
        assert_eq!(locs.len(), 3, "got: {locs:?}");
    }

    #[test]
    fn references_includes_declaration_when_requested() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\nmain = id 5\n");
        let doc = ws.doc(&uri);
        let def_pos = doc.source.find("id =").expect("def");
        let pos = offset_to_position(&doc.source, def_pos);
        let locs = handle_references(&ws.state, &ref_params(&uri, pos, true))
            .expect("references found");
        // Declaration + one usage
        assert_eq!(locs.len(), 2, "got: {locs:?}");
    }
}
