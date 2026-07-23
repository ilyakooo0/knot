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
    fn references_includes_subset_constraint_lhs_relation() {
        // Regression: a subset-constraint LHS relation (`*people <= …`) sits at
        // column 0 right after the `*` sigil — the same shape as a declaration
        // name token — but it is a genuine reference. `is_declaration_token`
        // used to filter it out, silently dropping it from Find-References.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "*people : [{name: Text, email: Text}]\n*people <= *people.email\nmain = do\n  p <- *people\n  println p.name\n",
        );
        let doc = ws.doc(&uri);
        let pos = offset_to_position(&doc.source, doc.source.find("*people :").unwrap() + 1);
        let locs = handle_references(&ws.state, &ref_params(&uri, pos, false))
            .expect("references found");
        // The LHS `people` token starts one byte past the constraint line's `*`.
        let lhs_off = doc.source.find("*people <=").unwrap() + 1;
        let has_lhs = locs
            .iter()
            .any(|l| crate::utils::position_to_offset(&doc.source, l.range.start) == lhs_off);
        assert!(
            has_lhs,
            "subset-constraint LHS reference at offset {lhs_off} missing; got: {locs:?}"
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
