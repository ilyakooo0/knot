//! `textDocument/documentHighlight` handler.

use lsp_types::*;

use crate::state::ServerState;
use crate::utils::{ident_lookup_offset, position_to_offset, span_to_range};

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
        })?;

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
        if *target_span == def_span {
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
