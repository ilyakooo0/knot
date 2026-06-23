//! `textDocument/references` handler.

use std::collections::HashSet;
use std::path::PathBuf;

use lsp_types::*;

use crate::analysis::get_or_parse_file_shared;
use crate::defs::resolve_definitions;
use crate::rename::{
    collect_name_uses_in_decl, file_imports_owner, imports_name_from_other_module,
    module_defines_name,
};
use crate::shared::scan_knot_files_in_roots;
use crate::state::ServerState;
use crate::utils::{
    ident_lookup_offset, path_to_uri, position_to_offset, span_to_range, uri_to_path,
    word_at_position,
};

/// Cap on the number of locations returned by a single `textDocument/references`
/// request. Common identifier names (`x`, `i`, `name`) in a multi-file workspace
/// can match thousands of times across open docs and disk; without a cap, the
/// reply takes long enough to encode that the editor's "Find References" pane
/// hangs, and the resulting payload usually overflows what's actually useful
/// to the user. Truncation is silent — the editor renders the first 10k hits,
/// which is far more than anyone scrolls through anyway.
const MAX_REFERENCE_LOCATIONS: usize = 10_000;

/// True if `usage` is a definition-name token of a top-level declaration —
/// i.e. it begins at column 0 (after an optional `*`/`&` relation sigil), the
/// invariant `defs::register_extra_definition_tokens` relies on. Such tokens
/// (e.g. the `f =` line of a `f : T` ⏎ `f = body` decl) are recorded in
/// `references` as self-references so position-based goto/highlight work from
/// the body line, but they are *declarations*, not usages — emitting them in a
/// Find-References result over-counts. Body-line usages are layout-indented, so
/// column-0 reliably distinguishes the two.
pub(crate) fn is_declaration_token(source: &str, usage: knot::ast::Span) -> bool {
    let start = usage.start.min(source.len());
    let line_start = source[..start].rfind('\n').map_or(0, |i| i + 1);
    let prefix = &source[line_start..start];
    prefix.is_empty() || prefix == "*" || prefix == "&"
}

// ── Find References ─────────────────────────────────────────────────

pub(crate) fn handle_references(
    state: &ServerState,
    params: &ReferenceParams,
) -> Option<Vec<Location>> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Find the symbol name and definition span in current document. The
    // definition resolution is strictly position-based: a recorded reference
    // covering
    // the cursor, or the cursor sitting on a definition's name token. A
    // name-keyed fallback would misfire — on a record field (or any other
    // token) that merely *shares its name* with a top-level symbol, it
    // returned that unrelated symbol's references.
    let word = word_at_position(&doc.source, pos)?;
    let symbol_name = word.to_string();

    // Case A (mirrors `rename::resolve_canonical_owner`): a recorded reference
    // covering the cursor, or the cursor on a definition's own name token —
    // both resolve to a module-local declaration span. A name-keyed fallback
    // would misfire on a record field (or any token) that merely *shares its
    // name* with a top-level symbol.
    let local_def = doc
        .references
        .iter()
        .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
        .min_by_key(|(usage, _)| usage.end - usage.start)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        });

    let current_path = uri_to_path(uri).and_then(|p| p.canonicalize().ok());

    // Case B: the cursor sits on a *usage* of an imported symbol. These usages
    // are not recorded in `doc.references` (which only resolves module-local
    // declarations), so Case A fails — fall back to `import_defs` to recover
    // the owning file, exactly like `rename`. Without this, Find References on
    // an imported symbol's use returned nothing even though Rename worked from
    // the same cursor. A local definition takes priority over an import of the
    // same name (when this file both declares and imports `parse`, references
    // here resolve to the local declaration).
    // The `import_defs` lookup is name-keyed, so guard it like Case A is
    // position-based: a record-field token must not fall through to an imported
    // symbol that merely shares its name (field tokens aren't in `references`, so
    // Case A always fails for them and they'd otherwise hit this fallback).
    let imported_owner: Option<PathBuf> = if local_def.is_none()
        && !crate::rename::is_at_record_field(&doc.module, &doc.source, offset)
    {
        doc.import_defs.get(&symbol_name).map(|(p, _)| p.clone())
    } else {
        None
    };

    // Nothing resolved: not a definition, a local usage, or an imported symbol.
    if local_def.is_none() && imported_owner.is_none() {
        return None;
    }

    let mut locations = Vec::new();

    // Current-document contributions.
    if let Some(def_span) = local_def {
        // Include declaration if requested.
        if params.context.include_declaration {
            locations.push(Location {
                uri: uri.clone(),
                range: span_to_range(def_span, &doc.source),
            });
        }
        // All local usages resolving to this definition. Local binders record a
        // self-reference (usage == def) so position-based resolution works from
        // the binder token; skip it here — the declaration is handled above
        // (and only emitted when `include_declaration` is set), so without this
        // guard the binder would surface as a usage even for
        // `include_declaration = false`. Mirrors `document_highlight`.
        for (usage_span, target_span) in &doc.references {
            if locations.len() >= MAX_REFERENCE_LOCATIONS {
                break;
            }
            if *usage_span == def_span {
                continue;
            }
            if *target_span == def_span {
                // Skip declaration-name tokens of multi-line decls; they're
                // recorded as self-references but are not usages.
                if is_declaration_token(&doc.source, *usage_span) {
                    continue;
                }
                locations.push(Location {
                    uri: uri.clone(),
                    range: span_to_range(*usage_span, &doc.source),
                });
            }
        }
    } else {
        // Imported symbol: its usages in this file are not in `doc.references`,
        // so walk the AST (scope-aware, skipping shadowed locals) plus the
        // import items that surface the name — mirroring the importer branches
        // below.
        let mut sites = Vec::new();
        for decl in &doc.module.decls {
            collect_name_uses_in_decl(decl, &symbol_name, &doc.source, &mut sites);
        }
        for imp in &doc.module.imports {
            if let Some(items) = &imp.items {
                for item in items {
                    if item.name == symbol_name {
                        sites.push(item.span);
                    }
                }
            }
        }
        for site in sites {
            if locations.len() >= MAX_REFERENCE_LOCATIONS {
                break;
            }
            locations.push(Location {
                uri: uri.clone(),
                range: span_to_range(site, &doc.source),
            });
        }
    }

    // Origin discipline (mirrors the rename path): cross-file usages count
    // only when the other file imports the symbol from its owning file and
    // doesn't declare a same-named symbol of its own. Resolve where the
    // symbol's canonical definition lives:
    // - declared at top level in the current doc → the current file;
    // - a local binding (lambda param, let, do-bind) → nowhere else; other
    //   files can't reference it, so cross-file matching is skipped;
    // - a usage of an imported symbol → the imported (owning) file.
    let owner_path: Option<PathBuf> = match (&local_def, &imported_owner) {
        (Some(def_span), _) if doc.definitions.values().any(|s| s == def_span) => {
            current_path.clone()
        }
        (Some(_), _) => None,
        (None, Some(p)) => Some(p.clone()),
        (None, None) => None,
    };

    // Cross-file: search all other open documents for references that resolve
    // to the same origin.
    if let Some(owner_path) = &owner_path {
        'open_docs: for (other_uri, other_doc) in &state.documents {
            if locations.len() >= MAX_REFERENCE_LOCATIONS {
                break;
            }
            if other_uri == uri {
                continue;
            }
            let other_path = uri_to_path(other_uri).and_then(|p| p.canonicalize().ok());
            if other_path.as_ref() == Some(owner_path) {
                // The owning file itself (open while we started from an
                // importer): its own references to the definition count.
                if let Some(other_def) = other_doc.definitions.get(&symbol_name).copied() {
                    if params.context.include_declaration {
                        locations.push(Location {
                            uri: other_uri.clone(),
                            range: span_to_range(other_def, &other_doc.source),
                        });
                    }
                    for (usage_span, target_span) in &other_doc.references {
                        if locations.len() >= MAX_REFERENCE_LOCATIONS {
                            break 'open_docs;
                        }
                        if *target_span == other_def {
                            if is_declaration_token(&other_doc.source, *usage_span) {
                                continue;
                            }
                            locations.push(Location {
                                uri: other_uri.clone(),
                                range: span_to_range(*usage_span, &other_doc.source),
                            });
                        }
                    }
                }
            } else if other_doc.definitions.contains_key(&symbol_name) {
                // The other file declares its own, unrelated symbol with the
                // same name — every reference there resolves locally.
                continue;
            } else if other_doc
                .import_defs
                .get(&symbol_name)
                .map(|(p, _)| p == owner_path)
                .unwrap_or(false)
            {
                // Importer of the same origin. If it ALSO imports the name
                // from a different module, its body references are ambiguous —
                // skip rather than misattribute them (mirrors the rename path).
                if let Some(other_path) = &other_path {
                    if imports_name_from_other_module(
                        &other_doc.module,
                        other_path,
                        owner_path,
                        &symbol_name,
                    ) {
                        continue;
                    }
                }
                // Imported-symbol usages don't appear in `references` (which
                // only resolves local decls), so walk the AST — scope-aware,
                // skipping shadowed locals.
                let mut sites = Vec::new();
                for decl in &other_doc.module.decls {
                    collect_name_uses_in_decl(decl, &symbol_name, &other_doc.source, &mut sites);
                }
                for imp in &other_doc.module.imports {
                    if let Some(items) = &imp.items {
                        for item in items {
                            if item.name == symbol_name {
                                sites.push(item.span);
                            }
                        }
                    }
                }
                for site in sites {
                    if locations.len() >= MAX_REFERENCE_LOCATIONS {
                        break 'open_docs;
                    }
                    locations.push(Location {
                        uri: other_uri.clone(),
                        range: span_to_range(site, &other_doc.source),
                    });
                }
            }
        }
    }

    // Cross-file: scan workspace files that are not currently open. Cheap when
    // they're already cached in `import_cache`; falls back to a one-shot parse
    // otherwise. The same origin discipline applies: a file only contributes
    // usages when it imports the owner (and doesn't define its own same-named
    // symbol); the unopened owner file contributes its declaration + local
    // references.
    if let Some(owner_path) = &owner_path {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();
        let workspace_files =
            scan_knot_files_in_roots(&state.workspace_roots, state.workspace_root.as_deref());
        for file_path in workspace_files {
            if locations.len() >= MAX_REFERENCE_LOCATIONS {
                break;
            }
            let canonical = match file_path.canonicalize() {
                Ok(p) => p,
                Err(_) => continue,
            };
            if open_paths.contains(&canonical) {
                continue;
            }
            let (module, source) =
                match get_or_parse_file_shared(&canonical, &state.import_cache) {
                    Some(p) => p,
                    None => continue,
                };
            // Quick rejection before any AST walk.
            if !source.contains(symbol_name.as_str()) {
                continue;
            }
            // Skip files whose path can't be encoded as a URI rather than
            // emitting a junk `file:///` location — locations with nonsense
            // URIs would silently mislead the editor's references pane.
            let Some(other_uri) = path_to_uri(&canonical) else {
                continue;
            };
            if canonical == *owner_path {
                // Unopened owner: recompute defs/refs from the disk copy.
                let (defs, refs, _) = resolve_definitions(&module, &source);
                if let Some(def) = defs.get(&symbol_name).copied() {
                    if params.context.include_declaration {
                        locations.push(Location {
                            uri: other_uri.clone(),
                            range: span_to_range(def, &source),
                        });
                    }
                    for (usage_span, target_span) in &refs {
                        if locations.len() >= MAX_REFERENCE_LOCATIONS {
                            break;
                        }
                        if *target_span == def {
                            if is_declaration_token(&source, *usage_span) {
                                continue;
                            }
                            locations.push(Location {
                                uri: other_uri.clone(),
                                range: span_to_range(*usage_span, &source),
                            });
                        }
                    }
                }
            } else if !module_defines_name(&module, &symbol_name)
                && file_imports_owner(&module, &canonical, owner_path, &symbol_name)
                && !imports_name_from_other_module(
                    &module,
                    &canonical,
                    owner_path,
                    &symbol_name,
                )
            {
                let mut sites = Vec::new();
                for decl in &module.decls {
                    collect_name_uses_in_decl(decl, &symbol_name, &source, &mut sites);
                }
                for imp in &module.imports {
                    if let Some(items) = &imp.items {
                        for item in items {
                            if item.name == symbol_name {
                                sites.push(item.span);
                            }
                        }
                    }
                }
                for site in sites {
                    if locations.len() >= MAX_REFERENCE_LOCATIONS {
                        break;
                    }
                    locations.push(Location {
                        uri: other_uri.clone(),
                        range: span_to_range(site, &source),
                    });
                }
            }
        }
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
    fn references_does_not_count_two_line_decl_body_token_as_usage() {
        // Regression: a decl written with a separate signature line
        // (`greet : Text` ⏎ `greet = …`) registers the body-line `greet` token
        // as a self-reference so position-based goto works from it. Find
        // References must not report that declaration token as a usage.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "greet : Text\ngreet = \"hi\"\nmain = println greet\n",
        );
        let doc = ws.doc(&uri);
        let pos = offset_to_position(&doc.source, doc.source.find("greet :").expect("sig"));

        // Without the declaration, the only usage is the call in `main`.
        let locs = handle_references(&ws.state, &ref_params(&uri, pos, false))
            .expect("references found");
        assert_eq!(locs.len(), 1, "exactly one usage expected; got: {locs:?}");
        let usage_off = crate::utils::position_to_offset(&doc.source, locs[0].range.start);
        assert!(
            usage_off > doc.source.find("println").unwrap(),
            "the single usage must be the call site in `main`, got offset {usage_off}"
        );

        // With the declaration included, we get the declaration + the one call
        // — but NOT the `greet =` body-line token (which would make it 3).
        let with_decl = handle_references(&ws.state, &ref_params(&uri, pos, true))
            .expect("references found");
        assert_eq!(
            with_decl.len(),
            2,
            "declaration + one usage expected; got: {with_decl:?}"
        );
    }

    #[test]
    fn references_excludes_unrelated_same_named_symbol_in_other_open_files() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\nmain = parse 5\n");
        let unrelated_uri = tw.write_and_open(
            "unrelated.knot",
            "parse = \\y -> y\nrun = parse 1\n",
        );
        let owner_doc = tw.workspace.doc(&owner_uri);
        let pos = offset_to_position(
            &owner_doc.source,
            owner_doc.source.find("parse =").expect("def"),
        );
        let locs = handle_references(&tw.workspace.state, &ref_params(&owner_uri, pos, false))
            .expect("references found");
        assert!(
            locs.iter().all(|l| l.uri == owner_uri),
            "usages of the unrelated same-named local must be excluded; got: {locs:?}"
        );
        let _ = unrelated_uri;
    }

    #[test]
    fn references_includes_importer_usages_but_not_shadowed_locals() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nrun = parse 1\nshadow = \\parse -> parse 2\n",
        );
        let owner_doc = tw.workspace.doc(&owner_uri);
        let pos = offset_to_position(
            &owner_doc.source,
            owner_doc.source.find("parse =").expect("def"),
        );
        let locs = handle_references(&tw.workspace.state, &ref_params(&owner_uri, pos, false))
            .expect("references found");
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let consumer_locs: Vec<_> = locs.iter().filter(|l| l.uri == consumer_uri).collect();
        assert_eq!(
            consumer_locs.len(),
            1,
            "only the import-resolved call site counts; got: {locs:?}"
        );
        let expected = offset_to_position(
            &consumer_doc.source,
            consumer_doc.source.find("parse 1").expect("call site"),
        );
        assert_eq!(
            consumer_locs[0].range.start, expected,
            "the included usage must be `parse 1`, not the shadowed lambda body"
        );
    }

    #[test]
    fn references_disk_scan_respects_origin() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        // Unopened files on disk: one imports the owner (its usage counts),
        // one declares its own same-named symbol (must be excluded).
        std::fs::write(
            tw.root.join("importer.knot"),
            "import ./owner\nrun = parse 1\n",
        )
        .unwrap();
        std::fs::write(
            tw.root.join("unrelated.knot"),
            "parse = \\y -> y\nz = parse 3\n",
        )
        .unwrap();
        let owner_doc = tw.workspace.doc(&owner_uri);
        let pos = offset_to_position(
            &owner_doc.source,
            owner_doc.source.find("parse =").expect("def"),
        );
        let locs = handle_references(&tw.workspace.state, &ref_params(&owner_uri, pos, false))
            .expect("references found");
        assert!(
            locs.iter()
                .any(|l| l.uri.as_str().ends_with("importer.knot")),
            "the unopened importer's usage must be included; got: {locs:?}"
        );
        assert!(
            locs.iter()
                .all(|l| !l.uri.as_str().ends_with("unrelated.knot")),
            "the unopened file with its own `parse` must be excluded; got: {locs:?}"
        );
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
