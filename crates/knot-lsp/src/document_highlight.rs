//! `textDocument/documentHighlight` handler.

use lsp_types::*;

use crate::rename::{collect_name_uses_in_decl, is_at_record_field, owner_trait_method_scope};
use crate::state::ServerState;
use crate::utils::{ident_lookup_offset, position_to_offset, span_to_range, word_at_position};

// ── Document Highlights ─────────────────────────────────────────────

pub(crate) fn handle_document_highlight(
    state: &ServerState,
    params: &DocumentHighlightParams,
) -> Option<Vec<DocumentHighlight>> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    // Staleness guard (mirrors hover / goto): during the analysis debounce
    // window the live buffer diverges from the analyzed source, so a position
    // from the editor would resolve against stale bytes — the wrong symbol
    // would be highlighted. Bail; the client re-requests once analysis
    // catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Find the definition span for the symbol at cursor. `references` holds
    // deliberately overlapping spans (a constructor-pattern name ref can
    // enclose a nested binder ref), so pick the INNERMOST span containing the
    // cursor rather than the first match — mirrors goto/hover, which were
    // fixed the same way. Taking the first match could resolve to the wrong
    // (outer) symbol.
    let def_span = doc
        .references
        .iter()
        .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
        .min_by_key(|(usage, _)| usage.end - usage.start)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions
                .values()
                .find(|span| span.start <= offset && offset < span.end)
                .copied()
        });

    // Cursor sits on a usage of an imported symbol. Those usages are not
    // recorded in `doc.references` (which only resolves module-local
    // declarations), so the local resolution above fails — fall back to
    // `import_defs`, exactly like goto-definition and find-references. Without
    // this, highlight returned nothing for imported symbols even though goto
    // and references both resolved them.
    let Some(def_span) = def_span else {
        // Guard the name-keyed `import_defs` lookup like goto does: a
        // record-field token (`p.name`) is never in `references`, so it always
        // falls through here and must not resolve to an imported symbol that
        // merely shares its name.
        if is_at_record_field(&doc.module, &doc.source, offset) {
            return None;
        }
        let word = word_at_position(&doc.source, pos)?;
        let (owner_path, decl_span) = doc.import_defs.get(word)?;
        let symbol_name = word.to_string();
        // Trait-method scope: confine impl-method highlights to the imported
        // method's own trait(s) (empty for non-trait-method symbols), mirroring
        // the rename / references oracle.
        let target_traits = owner_trait_method_scope(state, owner_path, *decl_span, &symbol_name);
        let mut sites = Vec::new();
        for decl in &doc.module.decls {
            collect_name_uses_in_decl(decl, &symbol_name, &doc.source, &target_traits, &mut sites);
        }
        // The import item that surfaces the name (`import ./m {name}`) is also a
        // local occurrence worth highlighting.
        for imp in &doc.module.imports {
            if let Some(items) = &imp.items {
                for item in items {
                    if item.name == symbol_name {
                        sites.push(item.span);
                    }
                }
            }
        }
        sites.sort_by_key(|s| s.start);
        sites.dedup_by_key(|s| s.start);
        let highlights: Vec<DocumentHighlight> = sites
            .into_iter()
            .map(|span| DocumentHighlight {
                range: span_to_range(span, &doc.source),
                kind: Some(DocumentHighlightKind::READ),
            })
            .collect();
        return if highlights.is_empty() {
            None
        } else {
            Some(highlights)
        };
    };

    let mut highlights = Vec::new();

    // Highlight the definition itself
    highlights.push(DocumentHighlight {
        range: span_to_range(def_span, &doc.source),
        kind: Some(DocumentHighlightKind::WRITE),
    });

    // Highlight all usages. Local binders record a self-reference
    // (usage == def) so position-based resolution works from the binder
    // token; skip it here — the definition was already pushed above.
    for (usage_span, target_span) in &doc.references {
        if *usage_span == def_span {
            continue;
        }
        // Skip declaration-name tokens of multi-line decls: they are recorded as
        // self-references so position-based resolution works from the body line,
        // but they are *declarations*, not READ usages. `references.rs` and
        // `code_lens.rs` apply the same filter — without it the `f =` body-line
        // token of a `f : T` ⏎ `f = …` decl gets wrongly highlighted as a READ.
        if *target_span == def_span
            && !crate::references::is_declaration_token(&doc.source, *usage_span)
        {
            highlights.push(DocumentHighlight {
                range: span_to_range(*usage_span, &doc.source),
                kind: Some(DocumentHighlightKind::READ),
            });
        }
    }

    if highlights.is_empty() {
        None
    } else {
        Some(highlights)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempWorkspace;
    use crate::utils::offset_to_position;

    fn highlight_params(uri: &Uri, position: Position) -> DocumentHighlightParams {
        DocumentHighlightParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    #[test]
    fn highlights_imported_symbol_usages() {
        // Regression (Bug 2): document highlight resolved only via module-local
        // `references`/`definitions`, so an imported symbol resolved to nothing
        // and no usages were highlighted — even though goto-definition and
        // find-references both resolve it. It now consults `import_defs` and
        // highlights the local usages.
        let mut tw = TempWorkspace::new();
        let _owner = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\na = parse 1\nb = parse 2\n",
        );
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let off = consumer_doc.source.find("parse 1").expect("first use");
        let pos = offset_to_position(&consumer_doc.source, off);
        let hls =
            handle_document_highlight(&tw.workspace.state, &highlight_params(&consumer_uri, pos))
                .expect("highlights for imported symbol");
        let use1 = offset_to_position(
            &consumer_doc.source,
            consumer_doc.source.find("parse 1").unwrap(),
        );
        let use2 = offset_to_position(
            &consumer_doc.source,
            consumer_doc.source.find("parse 2").unwrap(),
        );
        assert!(
            hls.iter().any(|h| h.range.start == use1),
            "first imported usage must be highlighted; got: {hls:?}"
        );
        assert!(
            hls.iter().any(|h| h.range.start == use2),
            "second imported usage must be highlighted; got: {hls:?}"
        );
    }

    #[test]
    fn does_not_highlight_two_line_decl_body_token_as_read() {
        // Regression (M2): a decl written with a separate signature line
        // (`greet : Text` ⏎ `greet = …`) records the body-line `greet` token as
        // a self-reference so position-based resolution works from it. Document
        // highlight must not emit that declaration token as a READ usage — only
        // `references.rs` and `code_lens.rs` filtered it before.
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "greet : Text\ngreet = \"hi\"\nmain = println greet\n");
        let doc = ws.doc(&uri);
        // Cursor on the `greet` usage in `main`.
        let use_off = doc.source.find("println greet").unwrap() + "println ".len();
        let pos = offset_to_position(&doc.source, use_off);
        let hls = handle_document_highlight(&ws.state, &highlight_params(&uri, pos))
            .expect("highlights for local symbol");

        // The `greet =` body-line token sits at line 1, column 0. It must not be
        // highlighted (as READ or otherwise).
        assert!(
            hls.iter().all(|h| h.range.start.line != 1),
            "the `greet =` declaration body token (line 1) must not be highlighted; got: {hls:?}"
        );
        // Exactly one READ usage: the call site in `main` (line 2).
        let reads: Vec<_> = hls
            .iter()
            .filter(|h| h.kind == Some(DocumentHighlightKind::READ))
            .collect();
        assert_eq!(
            reads.len(),
            1,
            "exactly one READ usage (the call in main) expected; got: {hls:?}"
        );
        assert_eq!(reads[0].range.start.line, 2, "the READ usage must be in `main`");
    }
}
