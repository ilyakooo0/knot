//! `textDocument/codeLens` handler. Surfaces reference counts, route URLs, and
//! impl counts.

use lsp_types::*;

use knot::ast::ExprKind;
use crate::utils::top_fields;

use crate::shared::{format_route_path, http_method_str, route_is_listened};
use crate::state::ServerState;
use crate::utils::span_to_range;

// ── Code Lens ───────────────────────────────────────────────────────

pub(crate) fn handle_code_lens(
    state: &ServerState,
    params: &CodeLensParams,
) -> Option<Vec<CodeLens>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let mut lenses = Vec::new();

    for decl in top_fields(&doc.module) {
        let dspan = decl.value.span;
        let decl_name = match &decl.value.node {
            ExprKind::SourceDecl { name, .. }
            | ExprKind::ViewDecl { name, .. }
            | ExprKind::DataCtor { name, .. }
            | ExprKind::RouteDecl { name, .. }
            | ExprKind::RouteCompositeDecl { name, .. } => name.as_str(),
            ExprKind::SubsetConstraint { .. } => continue,
            _ => decl.name.as_str(),
        };

        // Collect reference locations for this declaration. Reference target
        // spans recorded by `defs.rs` are the declaration's *name-token*
        // span (with the whole decl span only as a fallback when the name
        // can't be located in source), so compare against the span stored in
        // `doc.definitions` rather than `decl.span` — the latter never
        // matches and would show "0 references" for everything.
        let def_span = doc
            .definitions
            .get(decl_name)
            .copied()
            .unwrap_or(dspan);
        // Filter out self-references the way `references.rs` and
        // `call_hierarchy.rs` do: the declaration's own name token (and, for
        // multi-line decls, the definition-line name token recorded by
        // `defs::register_extra_definition_tokens`) are stored as references to
        // `def_span` but are not real usages — counting them inflates the lens.
        let ref_locations: Vec<Location> = doc
            .references
            .iter()
            .filter(|(usage, def)| {
                *def == def_span
                    && *usage != def_span
                    && !crate::references::is_declaration_token(&doc.source, *usage)
            })
            .map(|(usage, _)| Location {
                uri: uri.clone(),
                range: span_to_range(*usage, &doc.source),
            })
            .collect();
        let ref_count = ref_locations.len();

        let range = span_to_range(dspan, &doc.source);
        let title = if ref_count == 1 {
            "1 reference".to_string()
        } else {
            format!("{ref_count} references")
        };

        lenses.push(CodeLens {
            range: Range {
                start: range.start,
                end: range.start,
            },
            command: Some(Command {
                title,
                command: "editor.action.showReferences".to_string(),
                arguments: Some(vec![
                    serde_json::to_value(uri.as_str()).unwrap(),
                    serde_json::to_value(range.start).unwrap(),
                    serde_json::to_value(&ref_locations).unwrap(),
                ]),
            }),
            data: None,
        });

        // Route URL preview + dead-route lint lens.
        if let ExprKind::RouteDecl { name, entries } = &decl.value.node {
            // Per-entry URL preview lens, anchored at the route header. Each
            // entry's constructor is also separately hoverable for the same
            // info; this lens makes the URL space visible at a glance.
            for entry in entries {
                let method = http_method_str(entry.method);
                let path = format_route_path(entry);
                lenses.push(CodeLens {
                    range: Range {
                        start: range.start,
                        end: range.start,
                    },
                    command: Some(Command {
                        title: format!("{method} {path} → {}", entry.constructor),
                        command: String::new(),
                        arguments: None,
                    }),
                    data: None,
                });
            }
            // Dead-route lint: this route is never composed into a `listen`
            // call within the current document. Surface it as a lens so the
            // user can see at a glance.
            if !route_is_listened(&doc.module, name) {
                lenses.push(CodeLens {
                    range: Range {
                        start: range.start,
                        end: range.start,
                    },
                    command: Some(Command {
                        title: "⚠ no `listen` handler references this route".to_string(),
                        command: String::new(),
                        arguments: None,
                    }),
                    data: None,
                });
            }
        }
    }

    Some(lenses)
}
