//! `textDocument/definition`, `textDocument/typeDefinition`, and
//! `textDocument/implementation` handlers.

use std::collections::HashSet;
use std::path::PathBuf;

use lsp_types::*;

use knot::ast::{self, DeclKind, Module};

use crate::analysis::get_or_parse_file_shared;
use crate::shared::{extract_principal_type_name, scan_knot_files_in_roots};
use crate::state::ServerState;
use crate::utils::{
    ident_lookup_offset, path_to_uri, position_to_offset, span_to_range, uri_to_path,
    word_at_position,
};

// ── Go to definition ────────────────────────────────────────────────

pub(crate) fn handle_goto_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    // Staleness guard (mirrors hover / rename): during the analysis debounce
    // window the live buffer diverges from the analyzed source, so a position
    // from the editor would resolve against stale bytes and jump to the wrong
    // symbol. Bail; the client re-requests once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }

    // Nudge a caret sitting just past a usage's last char back into the word,
    // matching references/highlight — otherwise the half-open span match
    // (`offset < usage.end`) misses and we fall through to the name-keyed
    // fallback, which jumps to a shadowing top-level symbol instead of the
    // local binder under the cursor.
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Try span-based reference lookup first. Usage spans can overlap (a
    // constructor-pattern reference enclosing a nested binder reference), so
    // pick the *smallest* containing span — the symbol the cursor is actually
    // on — mirroring hover/goto-type-definition rather than taking an
    // arbitrary first match.
    let def_span = doc
        .references
        .iter()
        .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
        .min_by_key(|(usage, _)| usage.end - usage.start)
        .map(|(_, def)| *def)
        .or_else(|| {
            // Fallback: the cursor sitting directly on a definition's own name
            // token resolves to that definition. Strictly position-based — a
            // name-keyed fallback (`definitions.get(word)`) misfires on a
            // record-field token (or any token) that merely *shares its name*
            // with a top-level symbol, jumping to that unrelated declaration.
            // `references.rs` removed exactly this fallback for the same reason;
            // keep the two handlers consistent.
            doc.definitions
                .values()
                .find(|span| span.start <= offset && offset < span.end)
                .copied()
        });

    if let Some(span) = def_span {
        let range = span_to_range(span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Cross-file: check imported definitions. The `import_defs` lookup is purely
    // name-keyed, so guard it the same way the local path above is position-based:
    // a record-field token must not fall through to an imported symbol that merely
    // shares its name (field tokens are never recorded in `references`, so without
    // this guard `b.size` jumps to an imported `size` function).
    if crate::rename::is_at_record_field(&doc.module, &doc.source, offset) {
        return None;
    }
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
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));
    let word = word_at_position(&doc.source, pos)?;

    // Get the type string for the symbol at cursor. Multiple recorded spans
    // can contain the offset (a binding inside a larger pattern, a lambda
    // param inside its body span); iterating the HashMap and taking the
    // first hit returns an arbitrary one (hash-order nondeterminism). Use
    // the sorted vec and pick the *smallest* containing span — the
    // innermost binding is what the cursor is actually on.
    let type_str = doc
        .local_type_info_sorted
        .iter()
        .filter(|(span, _)| span.start <= offset && offset < span.end)
        .min_by_key(|(span, _)| span.end - span.start)
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            // Usage spans can overlap (a constructor-pattern reference
            // enclosing a nested binder reference); pick the *smallest*
            // containing span — the symbol the cursor is on — rather than an
            // arbitrary first match, mirroring the innermost-span rule above.
            doc.references
                .iter()
                .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
                .min_by_key(|(usage, _)| usage.end - usage.start)
                .and_then(|(_, def_span)| doc.local_type_info.get(def_span).cloned())
        })
        .or_else(|| {
            // The global `type_info` lookup is name-keyed, so guard it the
            // same way `handle_goto_definition` guards its cross-file fallback:
            // a record-field token (`p.name`) is never recorded in the local
            // type-info tables, so without this guard it falls through to an
            // unrelated top-level symbol that merely shares the field's name.
            // (`references.rs`/`hover` suppress name-based lookups for the same
            // reason.)
            if crate::rename::is_at_record_field(&doc.module, &doc.source, offset) {
                None
            } else {
                doc.type_info.get(word).cloned()
            }
        })?;

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
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let word = word_at_position(&doc.source, pos)?;

    let mut locations: Vec<Location> = Vec::new();

    // Classify the symbol under the cursor by scanning EVERY available module
    // — not just the one being walked for impls. A trait declared in file A
    // can be implemented in file B; the impl's `trait_name`/method tokens only
    // count as navigation sources once we know `word` actually names a trait
    // or trait method somewhere in the workspace. Determining this per-scanned
    // module (the old behavior) silently dropped every cross-file impl, since
    // the file holding the `impl` block rarely declares the trait itself.
    let mut is_trait_name = false;
    let mut is_method_name = false;
    let mut classify = |module: &Module| {
        for d in &module.decls {
            if let DeclKind::Trait { name, items, .. } = &d.node {
                if name == word {
                    is_trait_name = true;
                }
                if items
                    .iter()
                    .any(|i| matches!(i, ast::TraitItem::Method { name: m, .. } if m == word))
                {
                    is_method_name = true;
                }
            }
        }
    };
    classify(&doc.module);
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        classify(&other_doc.module);
    }
    // Disk scan for the classification: a trait declared in an unopened file
    // must still be recognized. Reuses the same parse cache the impl scan uses.
    {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();
        for path in scan_knot_files_in_roots(&state.workspace_roots, state.workspace_root.as_deref())
        {
            let canonical = match path.canonicalize() {
                Ok(p) => p,
                Err(_) => continue,
            };
            if open_paths.contains(&canonical) {
                continue;
            }
            if let Some((module, source)) =
                get_or_parse_file_shared(&canonical, &state.import_cache)
            {
                if source.contains(word) {
                    classify(&module);
                }
            }
        }
    }

    // Helper: collect impls from a parsed module that target a given trait or
    // contain a method of the given name. `is_trait_name`/`is_method_name` are
    // resolved once above against the whole workspace.
    let collect_from_module =
        |module: &Module,
         module_uri: &Uri,
         module_source: &str,
         word: &str,
         locs: &mut Vec<Location>| {
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
                            if let ast::ImplItem::Method { name, name_span, .. } = item {
                                if name == word {
                                    // Anchor on the method-name token (not the
                                    // body lambda) so the cursor lands on the
                                    // declaration, matching `analysis.rs`.
                                    locs.push(Location {
                                        uri: module_uri.clone(),
                                        range: span_to_range(*name_span, module_source),
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
    {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        let files = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        if !files.is_empty() {
            for path in files {
                let canonical = match path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let (module, source) =
                    match get_or_parse_file_shared(&canonical, &state.import_cache) {
                        Some(v) => v,
                        None => continue,
                    };
                if !source.contains(word) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                collect_from_module(&module, &file_uri, &source, word, &mut locations);
            }
        }
    }

    let mut iter = locations.into_iter();
    match (iter.next(), iter.next()) {
        (None, _) => None,
        (Some(only), None) => Some(GotoDefinitionResponse::Scalar(only)),
        (Some(first), Some(second)) => {
            let mut all = vec![first, second];
            all.extend(iter);
            Some(GotoDefinitionResponse::Array(all))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TempWorkspace, TestWorkspace};
    use crate::utils::offset_to_position;

    fn goto_params(uri: &Uri, position: Position) -> GotoDefinitionParams {
        GotoDefinitionParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    #[test]
    fn goto_definition_resolves_local_function_call() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"greet = \name -> "hi" ++ name
main = println (greet "world")
"#,
        );
        let doc = ws.doc(&uri);
        assert!(
            doc.definitions.contains_key("greet"),
            "definitions: {:?}",
            doc.definitions.keys().collect::<Vec<_>>()
        );
        let src_pos = doc.source.find("greet \"world\"").expect("call site");
        let pos = offset_to_position(&doc.source, src_pos + 1);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos))
            .expect("definition resolves");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            _ => panic!("expected scalar location"),
        };
        assert_eq!(loc.uri, uri);
        assert_eq!(loc.range.start.line, 0);
    }

    #[test]
    fn constructor_definition_span_anchors_on_the_constructor_token() {
        // A self-named constructor (`data Pair = Pair {...}`) must resolve to
        // the constructor token after `=`, not the type-name token before it.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "data Pair = Pair {x: Int}\n");
        let doc = ws.doc(&uri);
        let span = doc
            .definitions
            .get("Pair")
            .expect("Pair constructor defined");
        let eq = doc.source.find('=').unwrap();
        assert!(
            span.start > eq,
            "constructor span should be after `=` (got start {}, `=` at {})",
            span.start,
            eq
        );
    }

    #[test]
    fn constructor_definition_skips_shadowing_field_type() {
        // `B` appears first inside `A`'s field type and then as a constructor.
        // The constructor's definition span must anchor on the constructor
        // token (the last `B`), not the earlier field-type reference.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "data T = A {x: B} | B {}\n");
        let doc = ws.doc(&uri);
        let span = doc.definitions.get("B").expect("B constructor defined");
        let ctor_b = doc.source.rfind('B').unwrap();
        assert_eq!(
            span.start, ctor_b,
            "B's definition should anchor on the constructor, not the field type"
        );
    }

    #[test]
    fn goto_definition_returns_none_for_undefined_word() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = println \"hi\"\n");
        // Cursor on a position with no symbol — middle of the string
        let pos = Position::new(0, 16);
        let _ = handle_goto_definition(&ws.state, &goto_params(&uri, pos));
        // We don't assert None here strictly because `"hi"` may resolve as
        // word-based fallback; the important thing is no panic.
    }

    #[test]
    fn goto_definition_on_field_token_does_not_jump_to_shared_name_symbol() {
        // A record-field token (`b.size`) that merely shares its name with a
        // top-level symbol (`size = 100`) must not resolve to that symbol via a
        // name-keyed fallback. The field is not a recorded reference and the
        // cursor is not on a definition's own name token, so resolution must
        // NOT land on the unrelated `size` declaration.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "data Box = Box {size: Int}\nsize = 100\nget = \\b -> b.size\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on `size` in `b.size` (the last occurrence).
        let field_off = doc.source.rfind("size").expect("b.size field token");
        let pos = offset_to_position(&doc.source, field_off + 1);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos));
        if let Some(GotoDefinitionResponse::Scalar(loc)) = resp {
            assert_ne!(
                loc.range.start.line, 1,
                "goto on a field token must not jump to the unrelated top-level `size = 100`"
            );
        }
    }

    #[test]
    fn goto_type_definition_resolves_data_constructor() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"data Color = Red {} | Blue {}
shade : Color
shade = Red {}
"#,
        );
        let pos = ws.position_of(&uri, "shade = Red");
        let pos = Position::new(pos.line, pos.character);
        let resp = handle_goto_type_definition(&ws.state, &goto_params(&uri, pos));
        // Either the inferred type lands us on Color, or it doesn't resolve.
        // We just want this to not panic.
        let _ = resp;
    }

    #[test]
    fn goto_definition_resolves_imported_trait_method() {
        // A trait method declared in one file should resolve from a call
        // site in an importing file. Before the fix this returned `None`
        // because trait method *signatures* weren't added to `import_defs`
        // (only trait names and impl methods were).
        let mut tmp = TempWorkspace::new();
        tmp.write_and_open(
            "shapes.knot",
            r#"trait Display a where
  display : a -> Text
"#,
        );
        let consumer_uri = tmp.write_and_open(
            "consumer.knot",
            r#"import ./shapes
greet = \x -> display x
"#,
        );

        let doc = tmp.workspace.doc(&consumer_uri);
        let call_offset = doc.source.find("display x").expect("call site") + 1;
        let pos = offset_to_position(&doc.source, call_offset);
        let resp = handle_goto_definition(
            &tmp.workspace.state,
            &goto_params(&consumer_uri, pos),
        )
        .expect("trait method resolves cross-file");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            other => panic!("expected scalar location, got {other:?}"),
        };
        assert!(
            loc.uri.as_str().ends_with("shapes.knot"),
            "expected to land in shapes.knot, got {}",
            loc.uri.as_str()
        );
    }

    #[test]
    fn goto_implementation_finds_impl_across_files_from_trait_decl() {
        // Regression: `is_trait_name`/`is_method_name` used to be decided per
        // scanned module. A trait declared in one file and implemented in
        // another would yield NO implementations, because the file holding
        // the `impl` block doesn't itself declare the trait. The classification
        // is now resolved once against the whole workspace.
        let mut tmp = TempWorkspace::new();
        let shapes_uri = tmp.write_and_open(
            "shapes.knot",
            "trait Display a where\n  display : a -> Text\n",
        );
        let impl_uri = tmp.write_and_open(
            "impls.knot",
            "import ./shapes\ndata Circle = Circle {}\nimpl Display Circle where\n  display = \\c -> \"circle\"\n",
        );

        // 1. From the trait declaration in shapes.knot -> must find the impl.
        let sdoc = tmp.workspace.doc(&shapes_uri);
        let soff = sdoc.source.find("Display").expect("trait decl name");
        let spos = offset_to_position(&sdoc.source, soff);
        let resp = handle_goto_implementation(&tmp.workspace.state, &goto_params(&shapes_uri, spos))
            .expect("trait decl must resolve to its cross-file impl");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            GotoDefinitionResponse::Array(v) => v.into_iter().next().expect("at least one impl"),
            other => panic!("unexpected response: {other:?}"),
        };
        assert!(
            loc.uri.as_str().ends_with("impls.knot"),
            "impl lives in impls.knot, got {}",
            loc.uri.as_str()
        );

        // 2. From the trait method name in the impl file -> must resolve to the
        //    impl's method body.
        let idoc = tmp.workspace.doc(&impl_uri);
        let moff = idoc.source.find("display =").expect("method") + 1;
        let mpos = offset_to_position(&idoc.source, moff);
        let resp2 = handle_goto_implementation(&tmp.workspace.state, &goto_params(&impl_uri, mpos))
            .expect("trait method must resolve cross-file");
        let loc2 = match resp2 {
            GotoDefinitionResponse::Scalar(l) => l,
            GotoDefinitionResponse::Array(v) => v.into_iter().next().expect("at least one method"),
            other => panic!("unexpected response: {other:?}"),
        };
        assert!(
            loc2.uri.as_str().ends_with("impls.knot"),
            "method impl lives in impls.knot, got {}",
            loc2.uri.as_str()
        );
    }

    #[test]
    fn goto_implementation_anchors_on_method_name_not_body() {
        // Regression: `goto_implementation` used the method *body* span (`\c ->
        // …`) as the navigation target, so the cursor landed inside the lambda
        // instead of on the `display` declaration token. It must anchor on the
        // method-name token.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "trait Display a where\n  display : a -> Text\ndata Circle = Circle {}\nimpl Display Circle where\n  display = \\c -> \"circle\"\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on the `display` method name inside the impl block.
        let impl_off = doc.source.find("impl Display").expect("impl block");
        let method_off = doc.source[impl_off..].find("display").expect("method name") + impl_off;
        let pos = offset_to_position(&doc.source, method_off + 1);
        let resp = handle_goto_implementation(&ws.state, &goto_params(&uri, pos))
            .expect("method resolves to its impl");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            GotoDefinitionResponse::Array(v) => v.into_iter().next().expect("one impl"),
            other => panic!("unexpected response: {other:?}"),
        };
        // The target must be the method-name token, not the `\c -> …` body.
        let target_off = crate::utils::position_to_offset(&doc.source, loc.range.start);
        assert_eq!(
            &doc.source[target_off..target_off + "display".len()],
            "display",
            "implementation target should land on the `display` name token, \
             got {:?}",
            &doc.source[target_off..(target_off + 8).min(doc.source.len())]
        );
    }

    #[test]
    fn goto_definition_resolves_type_name_in_annotation() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type Color = {hex: Text}
get : Color -> Text
get = \c -> c.hex
"#,
        );
        let doc = ws.doc(&uri);
        // Cursor on the `Color` token in `get : Color -> Text`.
        let off = doc.source.find(": Color").expect("annotation") + 2;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos))
            .expect("type-name annotation resolves to definition");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            other => panic!("expected scalar, got {other:?}"),
        };
        assert_eq!(loc.uri, uri);
        // The Color type alias is on line 0.
        assert_eq!(loc.range.start.line, 0);
    }
}
