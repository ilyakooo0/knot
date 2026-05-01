//! `textDocument/definition`, `textDocument/typeDefinition`, and
//! `textDocument/implementation` handlers.

use std::collections::HashSet;
use std::path::PathBuf;

use lsp_types::*;

use knot::ast::{self, DeclKind, Module};

use crate::shared::{extract_principal_type_name, scan_knot_files};
use crate::state::ServerState;
use crate::utils::{path_to_uri, position_to_offset, span_to_range, uri_to_path, word_at_position};

// ── Go to definition ────────────────────────────────────────────────

pub(crate) fn handle_goto_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    let offset = position_to_offset(&doc.source, pos);

    // Try span-based reference lookup first
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            // Fallback: name-based lookup
            let word = word_at_position(&doc.source, pos)?;
            doc.definitions.get(word).copied()
        });

    if let Some(span) = def_span {
        let range = span_to_range(span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Cross-file: check imported definitions
    let word = word_at_position(&doc.source, pos)?;
    let (path, span) = doc.import_defs.get(word)?;
    let imported_source = doc.imported_files.get(path)?;
    let range = span_to_range(*span, imported_source);
    let import_uri = path_to_uri(path)?;
    Some(GotoDefinitionResponse::Scalar(Location {
        uri: import_uri,
        range,
    }))
}

// ── Go to type definition ────────────────────────────────────────────

pub(crate) fn handle_goto_type_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Get the type string for the symbol at cursor
    let type_str = doc
        .local_type_info
        .iter()
        .find(|(span, _)| span.start <= offset && offset < span.end)
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            doc.references
                .iter()
                .find(|(usage, _)| usage.start <= offset && offset < usage.end)
                .and_then(|(_, def_span)| doc.local_type_info.get(def_span).cloned())
        })
        .or_else(|| doc.type_info.get(word).cloned())?;

    // Extract the principal named type from the type string
    let type_name = extract_principal_type_name(&type_str)?;

    // Look up the definition of that type in the current document
    if let Some(def_span) = doc.definitions.get(&type_name) {
        let range = span_to_range(*def_span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Check imported definitions
    if let Some((path, span)) = doc.import_defs.get(&type_name) {
        let imported_source = doc.imported_files.get(path)?;
        let range = span_to_range(*span, imported_source);
        let import_uri = path_to_uri(path)?;
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: import_uri,
            range,
        }));
    }

    None
}

// ── Go to implementation ─────────────────────────────────────────────

/// Resolve `textDocument/implementation`:
/// - On a trait name: jump to all `impl Trait ...` blocks across the workspace.
/// - On a trait method name: jump to each impl's version of that method.
/// - On a type name with traits implemented for it: list all impls.
pub(crate) fn handle_goto_implementation(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let word = word_at_position(&doc.source, pos)?;

    let mut locations: Vec<Location> = Vec::new();

    // Helper: collect impls from a parsed module that target a given trait or
    // contain a method of the given name.
    let collect_from_module =
        |module: &Module,
         module_uri: &Uri,
         module_source: &str,
         word: &str,
         locs: &mut Vec<Location>| {
            // Determine what kind of symbol the cursor is on:
            // - trait name: collect every `impl <word> ...` block
            // - method name: for each impl block, find that method and add its span
            let is_trait_name = module.decls.iter().any(|d| {
                matches!(&d.node, DeclKind::Trait { name, .. } if name == word)
            });
            let is_method_name = module.decls.iter().any(|d| {
                if let DeclKind::Trait { items, .. } = &d.node {
                    items.iter().any(|i| {
                        matches!(i, ast::TraitItem::Method { name, .. } if name == word)
                    })
                } else {
                    false
                }
            });
            for decl in &module.decls {
                if let DeclKind::Impl {
                    trait_name, items, ..
                } = &decl.node
                {
                    if is_trait_name && trait_name == word {
                        locs.push(Location {
                            uri: module_uri.clone(),
                            range: span_to_range(decl.span, module_source),
                        });
                    } else if is_method_name {
                        for item in items {
                            if let ast::ImplItem::Method { name, body, .. } = item {
                                if name == word {
                                    // Use the body span as the navigation target
                                    // (keeps the method declaration in view).
                                    locs.push(Location {
                                        uri: module_uri.clone(),
                                        range: span_to_range(body.span, module_source),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        };

    // Phase 1: search the current document
    collect_from_module(&doc.module, uri, &doc.source, word, &mut locations);

    // Phase 2: search all open documents
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        collect_from_module(&other_doc.module, other_uri, &other_doc.source, word, &mut locations);
    }

    // Phase 3: search workspace files not currently open
    if let Some(root) = &state.workspace_root {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if let Ok(files) = scan_knot_files(root) {
            for path in files {
                let canonical = match path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if !source.contains(word) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let lexer = knot::lexer::Lexer::new(&source);
                let (tokens, _) = lexer.tokenize();
                let parser = knot::parser::Parser::new(source.clone(), tokens);
                let (module, _) = parser.parse_module();
                collect_from_module(&module, &file_uri, &source, word, &mut locations);
            }
        }
    }

    if locations.is_empty() {
        None
    } else if locations.len() == 1 {
        Some(GotoDefinitionResponse::Scalar(locations.into_iter().next().unwrap()))
    } else {
        Some(GotoDefinitionResponse::Array(locations))
    }
}
