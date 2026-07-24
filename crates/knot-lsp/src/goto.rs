//! `textDocument/definition`, `textDocument/typeDefinition`, and
//! `textDocument/implementation` handlers.

use lsp_types::*;

use knot::ast::{self, ExprKind};
use crate::utils::top_fields;

use crate::shared::extract_principal_type_name;
use crate::state::ServerState;
use crate::utils::{
    find_word_in_source, ident_lookup_offset, position_to_offset, span_to_range,
    word_at_position,
};

/// Find the span of a type *declaration's* name token (`data T = …` /
/// `type T = …`). `doc.definitions` maps a self-named data type
/// (`data Circle = Circle {}`) to the *constructor* token (last-write-wins),
/// which is the wrong target for goto-type-definition — resolve the type-name
/// token directly from the AST instead.
fn type_decl_name_span(program: &ast::Expr, source: &str, type_name: &str) -> Option<ast::Span> {
    for decl in top_fields(program) {
        let dspan = decl.value.span;
        let is_match = match &decl.value.node {
            ExprKind::DataCtor { name, .. } | ExprKind::TypeCtor { name, .. } => name == type_name,
            _ => false,
        };
        if is_match {
            return Some(
                find_word_in_source(source, type_name, dspan.start, dspan.end)
                    .unwrap_or(dspan),
            );
        }
    }
    None
}

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

    None
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

    // Look up the definition of that type in the current document. Prefer the
    // type *declaration's* name token (`doc.definitions` maps a self-named data
    // type to its constructor token, the wrong target here); fall back to the
    // generic definitions map for anything not found as a local type decl.
    if let Some(def_span) = type_decl_name_span(&doc.module, &doc.source, &type_name)
        .or_else(|| doc.definitions.get(&type_name).copied())
    {
        let range = span_to_range(def_span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    None
}


