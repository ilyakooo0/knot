//! `callHierarchy/{prepare,incomingCalls,outgoingCalls}` handlers.

use std::collections::{HashMap, HashSet};

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};

use crate::state::ServerState;
use crate::utils::{
    find_word_in_source, position_to_offset, recurse_expr, span_to_range, word_at_position,
};

// ── Call Hierarchy ───────────────────────────────────────────────────

pub(crate) fn handle_call_hierarchy_prepare(
    state: &ServerState,
    params: &CallHierarchyPrepareParams,
) -> Option<Vec<CallHierarchyItem>> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Find the declaration containing this name
    for decl in &doc.module.decls {
        let name = match &decl.node {
            DeclKind::Fun { name, .. } => name,
            DeclKind::Source { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. } => name,
            DeclKind::Data { name, .. } | DeclKind::Trait { name, .. } => name,
            _ => continue,
        };
        if name != word {
            continue;
        }
        // Check if cursor is on or references this declaration
        let on_def = decl.span.start <= offset && offset < decl.span.end;
        let on_ref = doc.references.iter().any(|(usage, def)| {
            usage.start <= offset && offset < usage.end && *def == decl.span
        });
        if !on_def && !on_ref {
            continue;
        }

        let range = span_to_range(decl.span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, decl.span.start, decl.span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = match &decl.node {
            DeclKind::Fun { .. } => SymbolKind::FUNCTION,
            DeclKind::Data { .. } => SymbolKind::STRUCT,
            DeclKind::Trait { .. } => SymbolKind::INTERFACE,
            _ => SymbolKind::VARIABLE,
        };

        return Some(vec![CallHierarchyItem {
            name: name.clone(),
            kind,
            tags: None,
            detail: doc.type_info.get(name).cloned(),
            uri: uri.clone(),
            range,
            selection_range,
            data: None,
        }]);
    }

    None
}

pub(crate) fn handle_call_hierarchy_incoming(
    state: &ServerState,
    params: &CallHierarchyIncomingCallsParams,
) -> Option<Vec<CallHierarchyIncomingCall>> {
    let target_name = &params.item.name;
    let target_uri = &params.item.uri;
    let doc = state.documents.get(target_uri)?;

    // Find all declarations that reference the target name
    let target_def = doc.definitions.get(target_name)?;
    let mut calls: HashMap<String, (ast::Span, Vec<Span>)> = HashMap::new(); // caller_name -> (decl_span, [call_site_spans])

    for decl in &doc.module.decls {
        let caller_name = match &decl.node {
            DeclKind::Fun { name, .. } => name.clone(),
            DeclKind::View { name, .. } => name.clone(),
            DeclKind::Derived { name, .. } => name.clone(),
            _ => continue,
        };
        // Collect call sites within this declaration that point to target_def
        let call_sites: Vec<Span> = doc
            .references
            .iter()
            .filter(|(usage, def)| {
                *def == *target_def
                    && usage.start >= decl.span.start
                    && usage.end <= decl.span.end
            })
            .map(|(usage, _)| *usage)
            .collect();

        if !call_sites.is_empty() {
            calls.insert(caller_name, (decl.span, call_sites));
        }
    }

    let mut result = Vec::new();
    for (name, (decl_span, sites)) in &calls {
        let range = span_to_range(*decl_span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, decl_span.start, decl_span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = doc
            .module
            .decls
            .iter()
            .find(|d| d.span == *decl_span)
            .map(|d| match &d.node {
                DeclKind::Fun { .. } => SymbolKind::FUNCTION,
                DeclKind::Data { .. } => SymbolKind::STRUCT,
                _ => SymbolKind::VARIABLE,
            })
            .unwrap_or(SymbolKind::FUNCTION);

        result.push(CallHierarchyIncomingCall {
            from: CallHierarchyItem {
                name: name.clone(),
                kind,
                tags: None,
                detail: doc.type_info.get(name).cloned(),
                uri: target_uri.clone(),
                range,
                selection_range,
                data: None,
            },
            from_ranges: sites.iter().map(|s| span_to_range(*s, &doc.source)).collect(),
        });
    }

    if result.is_empty() { None } else { Some(result) }
}

pub(crate) fn handle_call_hierarchy_outgoing(
    state: &ServerState,
    params: &CallHierarchyOutgoingCallsParams,
) -> Option<Vec<CallHierarchyOutgoingCall>> {
    let source_name = &params.item.name;
    let source_uri = &params.item.uri;
    let doc = state.documents.get(source_uri)?;

    // Find the declaration for the source item
    let source_decl = doc
        .module
        .decls
        .iter()
        .find(|d| match &d.node {
            DeclKind::Fun { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. } => name == source_name,
            _ => false,
        })?;

    // Higher-order call sites: a `Var(name)` that appears as the *argument* of
    // an `App` rather than its head means the function is being passed around
    // (e.g. `map handler list`). The outgoing-call view treats those as edges
    // so users can navigate from a caller to functions they hand off to.
    let mut higher_order_arg_spans: HashSet<Span> = HashSet::new();
    fn collect_higher_order_args(expr: &ast::Expr, out: &mut HashSet<Span>) {
        if let ast::ExprKind::App { arg, .. } = &expr.node {
            if matches!(&arg.node, ast::ExprKind::Var(_)) {
                out.insert(arg.span);
            }
        }
        recurse_expr(expr, |e| collect_higher_order_args(e, out));
    }
    match &source_decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } => {
            collect_higher_order_args(body, &mut higher_order_arg_spans);
        }
        _ => {}
    }

    // Collect all references within this declaration that point to other
    // declarations. Track whether each call site is a direct call or a
    // higher-order pass so we can label them in the outgoing list.
    let mut outgoing: HashMap<String, (Span, Vec<(Span, bool)>)> = HashMap::new();

    for (usage_span, def_span) in &doc.references {
        if usage_span.start < source_decl.span.start || usage_span.end > source_decl.span.end {
            continue;
        }
        if let Some((name, _)) = doc.definitions.iter().find(|(_, s)| *s == def_span) {
            if name == source_name {
                continue;
            }
            let is_higher_order = higher_order_arg_spans.contains(usage_span);
            outgoing
                .entry(name.clone())
                .or_insert_with(|| (*def_span, Vec::new()))
                .1
                .push((*usage_span, is_higher_order));
        }
    }

    let mut result = Vec::new();
    for (name, (def_span, sites)) in &outgoing {
        let range = span_to_range(*def_span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, def_span.start, def_span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = doc
            .module
            .decls
            .iter()
            .find(|d| d.span == *def_span)
            .map(|d| match &d.node {
                DeclKind::Fun { .. } => SymbolKind::FUNCTION,
                DeclKind::Data { .. } => SymbolKind::STRUCT,
                DeclKind::Trait { .. } => SymbolKind::INTERFACE,
                _ => SymbolKind::VARIABLE,
            })
            .unwrap_or(SymbolKind::FUNCTION);

        // Suffix the detail string with `(passed as argument)` when every
        // edge to this callee is a higher-order pass — useful for users
        // skimming the outgoing list to spot indirect calls.
        let all_higher_order = !sites.is_empty() && sites.iter().all(|(_, ho)| *ho);
        let detail_base = doc.type_info.get(name).cloned();
        let detail = if all_higher_order {
            Some(match detail_base {
                Some(t) => format!("{t}  -- passed as argument"),
                None => "passed as argument".to_string(),
            })
        } else {
            detail_base
        };

        result.push(CallHierarchyOutgoingCall {
            to: CallHierarchyItem {
                name: name.clone(),
                kind,
                tags: None,
                detail,
                uri: source_uri.clone(),
                range,
                selection_range,
                data: None,
            },
            from_ranges: sites
                .iter()
                .map(|(s, _)| span_to_range(*s, &doc.source))
                .collect(),
        });
    }

    if result.is_empty() { None } else { Some(result) }
}
