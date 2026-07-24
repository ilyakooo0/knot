//! `textDocument/references` handler.

use std::collections::HashSet;

use lsp_types::*;

use crate::state::ServerState;
use crate::utils::{
    ident_lookup_offset, position_to_offset, span_to_range, word_at_position,
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
    if !(prefix.is_empty() || prefix == "*" || prefix == "&") {
        return false;
    }
    // A genuine top-level declaration name is followed (after optional
    // whitespace) by its separator — `:` for a type signature / source, `=`
    // for a definition / view / derived. A subset-constraint LHS puts a
    // *relation reference* at the same column-0-after-sigil position
    // (`*people <= *people.email`, or `*people.email <= …`), but is followed
    // by `<=` (or a `.field` access first), so without this suffix check it
    // would be misclassified as a declaration and silently dropped from
    // Find-References / document-highlight results.
    let end = usage.end.min(source.len());
    matches!(
        source[end..].trim_start().as_bytes().first(),
        Some(b':') | Some(b'=')
    )
}

// ── Find References ─────────────────────────────────────────────────

pub(crate) fn handle_references(
    state: &ServerState,
    params: &ReferenceParams,
) -> Option<Vec<Location>> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    // Staleness guard (mirrors hover / goto): during the analysis debounce
    // window the live buffer diverges from the analyzed source, so a position
    // from the editor would resolve against stale bytes. Bail; the client
    // re-requests once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Find the symbol name and definition span in current document. The
    // definition resolution is strictly position-based: a recorded reference
    // covering
    // the cursor, or the cursor sitting on a definition's name token. A
    // name-keyed fallback would misfire — on a record field (or any other
    // token) that merely *shares its name* with a top-level symbol, it
    // returned that unrelated symbol's references.
    word_at_position(&doc.source, pos)?;

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

    // Nothing resolved: not a definition and not a local usage.
    let Some(def_span) = local_def else {
        return None;
    };

    let mut locations = Vec::new();

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


