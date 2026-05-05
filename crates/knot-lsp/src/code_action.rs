//! `textDocument/codeAction` handler. Synthesizes quick fixes, refactors,
//! and the unused-import organize action.

use std::collections::{HashMap, HashSet};

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span, TypeKind};

use crate::builtins::EFFECTFUL_BUILTINS;
use crate::shared::{
    extract_principal_type_name, find_enclosing_atomic_expr, render_signature_with_effects,
};
use crate::state::{builtins as state_builtins, DocumentState, ServerState};
use crate::utils::{
    edit_distance, offset_to_position, position_to_offset, safe_slice, span_to_range,
    word_at_position,
};

// ── Code Actions ────────────────────────────────────────────────────

pub(crate) fn handle_code_action(
    state: &ServerState,
    params: &CodeActionParams,
) -> Option<CodeActionResponse> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let mut actions = Vec::new();

    let range_start = position_to_offset(&doc.source, params.range.start);
    let range_end = position_to_offset(&doc.source, params.range.end);

    for decl in &doc.module.decls {
        // Only consider declarations overlapping the cursor range
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }

        // Action: Add type annotation to unannotated functions. Effects belong
        // inside the IO row of the rendered type — `render_signature_with_effects`
        // merges any per-decl effect-checker findings into that row when HM
        // inference dropped them (e.g., forward references through annotated
        // callers can collapse the row to `{}`).
        if let DeclKind::Fun { name, ty: None, .. } = &decl.node {
            if let Some(inferred) = doc.type_info.get(name) {
                let signature = match doc.effect_sets.get(name) {
                    Some(eff) => render_signature_with_effects(inferred, eff),
                    None => inferred.clone(),
                };
                let insert_pos = offset_to_position(&doc.source, decl.span.start);

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: insert_pos,
                            end: insert_pos,
                        },
                        new_text: format!("{name} : {signature}\n"),
                    }],
                );

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Add type annotation: {signature}"),
                    kind: Some(CodeActionKind::QUICKFIX),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
        }

        // Action: Add type annotation to unannotated views/derived. Same
        // effect-merging treatment as the Fun case above.
        match &decl.node {
            DeclKind::View { name, ty: None, .. } | DeclKind::Derived { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let signature = match doc.effect_sets.get(name) {
                        Some(eff) => render_signature_with_effects(inferred, eff),
                        None => inferred.clone(),
                    };
                    let decl_text = safe_slice(&doc.source, decl.span);
                    if let Some(eq_pos) = decl_text.find('=') {
                        let insert_offset = decl.span.start + eq_pos;
                        let insert_pos = offset_to_position(&doc.source, insert_offset);

                        let mut changes = HashMap::new();
                        changes.insert(
                            uri.clone(),
                            vec![TextEdit {
                                range: Range {
                                    start: insert_pos,
                                    end: insert_pos,
                                },
                                new_text: format!(": {signature} "),
                            }],
                        );

                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Add type annotation: {signature}"),
                            kind: Some(CodeActionKind::QUICKFIX),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                    }
                }
            }
            _ => {}
        }

        // Action: Add missing trait methods to impl blocks
        if let DeclKind::Impl {
            trait_name, items, ..
        } = &decl.node
        {
            // Find the trait declaration to know which methods are required.
            // We need the full TraitItem (not just the name) so we can look up
            // each method's type signature for param-count and default body.
            let trait_items: Vec<&ast::TraitItem> = doc
                .module
                .decls
                .iter()
                .filter_map(|d| {
                    if let DeclKind::Trait {
                        name,
                        items: trait_items,
                        ..
                    } = &d.node
                    {
                        if name == trait_name {
                            return Some(trait_items);
                        }
                    }
                    None
                })
                .flatten()
                .filter(|item| {
                    matches!(
                        item,
                        ast::TraitItem::Method {
                            default_body: None,
                            ..
                        }
                    )
                })
                .collect();

            let impl_methods: HashSet<&str> = items
                .iter()
                .filter_map(|item| {
                    if let ast::ImplItem::Method { name, .. } = item {
                        Some(name.as_str())
                    } else {
                        None
                    }
                })
                .collect();

            let missing: Vec<&&ast::TraitItem> = trait_items
                .iter()
                .filter(|item| {
                    if let ast::TraitItem::Method { name, .. } = item {
                        !impl_methods.contains(name.as_str())
                    } else {
                        false
                    }
                })
                .collect();

            if !missing.is_empty() {
                let insert_pos = offset_to_position(&doc.source, decl.span.end);
                let stubs: String = missing
                    .iter()
                    .map(|item| build_trait_method_stub(item))
                    .collect();

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: insert_pos,
                            end: insert_pos,
                        },
                        new_text: stubs,
                    }],
                );

                let missing_names: Vec<String> = missing
                    .iter()
                    .filter_map(|item| {
                        if let ast::TraitItem::Method { name, .. } = item {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Add missing methods: {}", missing_names.join(", ")),
                    kind: Some(CodeActionKind::QUICKFIX),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }

            // Convert empty / fully-default impl to a `deriving` clause on the
            // data declaration. The user wrote `impl Eq for Foo where` with no
            // body and the trait's required methods all have default bodies —
            // `deriving (Eq)` is the more idiomatic spelling.
            if let Some(action) = build_deriving_action(decl, trait_name, items, doc, uri) {
                actions.push(action);
            }
        }
    }

    // Diagnostic-attached quick fixes: suggest similar names for unknown identifiers
    let lsp_diags = &params.context.diagnostics;
    for diag in lsp_diags {
        let diag_offset = position_to_offset(&doc.source, diag.range.start);
        let msg = &diag.message;

        // Effect-related quick fixes
        if msg.contains("IO effects are not allowed inside atomic")
            || msg.contains("atomic block must interact with relations")
        {
            // Find the enclosing `atomic` expression in the AST and offer to
            // unwrap it (replace `atomic expr` with `expr`).
            if let Some((atomic_span, inner_text)) =
                find_enclosing_atomic_expr(&doc.module, &doc.source, diag_offset)
            {
                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: span_to_range(atomic_span, &doc.source),
                        new_text: inner_text,
                    }],
                );
                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: "Remove `atomic` wrapper".to_string(),
                    kind: Some(CodeActionKind::QUICKFIX),
                    diagnostics: Some(vec![diag.clone()]),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }

            // Additionally, if the diagnostic is "IO in atomic", suggest
            // wrapping the offending IO call in `fork` (fire-and-forget) so it
            // runs outside the transaction.
            if msg.contains("IO effects are not allowed inside atomic") {
                if let Some(call_span) = find_io_call_in_range(&doc, diag_offset) {
                    let inner_text = safe_slice(&doc.source, call_span).to_string();
                    let mut changes = HashMap::new();
                    changes.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: span_to_range(call_span, &doc.source),
                            new_text: format!("fork ({inner_text})"),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: "Wrap IO in `fork`".to_string(),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }));
                }
            }
        }

        // Quick fix for "inferred effects exceed declared effects"
        if msg.contains("inferred effects exceed declared effects") {
            // Extract the inferred-effects line from the diagnostic message
            if let Some(inferred) = extract_effect_set_from_message(msg, "inferred effects:") {
                // Find the declaration whose span overlaps this diagnostic
                if let Some((decl, fun_name)) = doc
                    .module
                    .decls
                    .iter()
                    .find_map(|d| match &d.node {
                        DeclKind::Fun {
                            name, ty: Some(_), ..
                        } if d.span.start <= diag_offset && diag_offset < d.span.end => {
                            Some((d, name.clone()))
                        }
                        _ => None,
                    })
                {
                    if let Some(edit) = build_effect_widen_edit(decl, &doc.source, &inferred) {
                        let mut changes = HashMap::new();
                        changes.insert(uri.clone(), vec![edit]);
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Widen declared effects to: {inferred}"),
                            kind: Some(CodeActionKind::QUICKFIX),
                            diagnostics: Some(vec![diag.clone()]),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            is_preferred: Some(true),
                            ..Default::default()
                        }));
                        let _ = fun_name; // for diagnostics in future
                    }
                }
            }
        }

        // Wrap-in-constructor quick fixes: when a type mismatch shows that an
        // expression of type `T` is being passed where `Maybe T`, `Result e T`,
        // or `IO ... T` is expected, offer to wrap the expression in the
        // appropriate constructor. Cheaper than asking users to rewrite the
        // expression themselves.
        if msg.starts_with("type mismatch:") {
            if let Some((expected, found)) = parse_type_mismatch(msg) {
                let diag_start = position_to_offset(&doc.source, diag.range.start);
                let diag_end = position_to_offset(&doc.source, diag.range.end);
                if diag_end > diag_start && diag_end <= doc.source.len() {
                    let snippet = doc.source[diag_start..diag_end].trim();
                    if !snippet.is_empty() {
                        let refined_names: HashSet<&str> =
                            doc.refined_types.keys().map(String::as_str).collect();
                        for wrap in detect_wrap_suggestions(&expected, &found, &refined_names) {
                            let mut changes = HashMap::new();
                            let wrapped = wrap.format_wrapping(snippet);
                            changes.insert(
                                uri.clone(),
                                vec![TextEdit {
                                    range: diag.range,
                                    new_text: wrapped,
                                }],
                            );
                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: wrap.title.clone(),
                                kind: Some(CodeActionKind::QUICKFIX),
                                diagnostics: Some(vec![diag.clone()]),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }));
                        }
                    }
                }
            }
        }

        // Unit-mismatch quick fixes: when the inferred unit on a numeric
        // expression doesn't match what the surrounding context expects
        // (e.g. `Float<M>` flowing into a `Float<Ft>` slot), offer to wrap the
        // expression in the strip/with conversion idiom. The user supplies the
        // numeric factor; the wrapper just gets the types to line up so they
        // see the call site rather than a type error.
        if msg.starts_with("unit mismatch:") || msg.contains("unit mismatch") {
            let diag_start = position_to_offset(&doc.source, diag.range.start);
            let diag_end = position_to_offset(&doc.source, diag.range.end);
            if diag_end > diag_start && diag_end <= doc.source.len() {
                let snippet = &doc.source[diag_start..diag_end];
                let trimmed = snippet.trim();
                if !trimmed.is_empty() {
                    // Float variant — most unit work in the stdlib is Float.
                    let mut changes_f = HashMap::new();
                    changes_f.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: diag.range,
                            new_text: format!("withFloatUnit (stripFloatUnit ({trimmed}))"),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: "Wrap in `withFloatUnit (stripFloatUnit …)`"
                            .to_string(),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes_f),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }));

                    // Int variant — for `Int<u1>` ↔ `Int<u2>` mismatches.
                    let mut changes_i = HashMap::new();
                    changes_i.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: diag.range,
                            new_text: format!("withUnit (stripUnit ({trimmed}))"),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: "Wrap in `withUnit (stripUnit …)`".to_string(),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes_i),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }));
                }
            }
        }

        // Pattern: "Unknown variable/type/constructor" → suggest similar names
        if msg.contains("nknown") || msg.contains("ndefined") || msg.contains("not found") || msg.contains("unresolved") {
            // Extract the unknown name from the diagnostic range
            let unknown_name = word_at_position(&doc.source, diag.range.start)
                .unwrap_or("");
            if !unknown_name.is_empty() {
                // Find similar names using edit distance
                let mut candidates: Vec<(&str, usize)> = Vec::new();
                for name in doc.definitions.keys() {
                    let dist = edit_distance(unknown_name, name);
                    if dist <= 2 && dist > 0 {
                        candidates.push((name, dist));
                    }
                }
                // Also check builtins
                for name in state_builtins() {
                    let dist = edit_distance(unknown_name, name);
                    if dist <= 2 && dist > 0 {
                        candidates.push((name, dist));
                    }
                }
                candidates.sort_by_key(|(_, d)| *d);

                for (suggestion, _) in candidates.iter().take(3) {
                    let mut changes = HashMap::new();
                    changes.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: diag.range,
                            new_text: suggestion.to_string(),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: format!("Did you mean `{suggestion}`?"),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes),
                            ..Default::default()
                        }),
                        is_preferred: Some(candidates.first().map_or(false, |(s, _)| *s == *suggestion)),
                        ..Default::default()
                    }));
                }

                // Auto-import: search the workspace for files that declare
                // `unknown_name` at top level and offer to add an import.
                if !already_imports(doc, unknown_name) {
                    let auto_candidates =
                        find_auto_import_candidates(state, uri, unknown_name);
                    for cand in auto_candidates {
                        let (insert_pos, insert_text) =
                            import_insert_position_and_text(doc, &cand.import_path);
                        let mut changes = HashMap::new();
                        changes.insert(
                            uri.clone(),
                            vec![TextEdit {
                                range: Range {
                                    start: insert_pos,
                                    end: insert_pos,
                                },
                                new_text: insert_text,
                            }],
                        );
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!(
                                "Import `{unknown_name}` from `{}`",
                                cand.import_path
                            ),
                            kind: Some(CodeActionKind::QUICKFIX),
                            diagnostics: Some(vec![diag.clone()]),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                    }
                }
            }
        }
    }

    // Action: Fill case arms — check if cursor is inside a case expression
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_case_actions(body, doc, uri, range_start, range_end, &mut actions);
            }
            DeclKind::Fun { body: None, .. } => {}
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_case_actions(body, doc, uri, range_start, range_end, &mut actions);
                    }
                }
            }
            _ => {}
        }
    }

    // Action: Extract variable — if a non-trivial expression is selected, offer to extract it
    if range_start != range_end {
        let selected_text = &doc.source[range_start..range_end.min(doc.source.len())];
        let trimmed = selected_text.trim();
        // Only offer for non-trivial selections (not just a name or empty)
        if !trimmed.is_empty()
            && trimmed.len() > 1
            && !trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            // Find the line where the selection starts to determine indentation
            let line_start = doc.source[..range_start]
                .rfind('\n')
                .map(|p| p + 1)
                .unwrap_or(0);
            let current_line = &doc.source[line_start..];
            let indent = current_line.len() - current_line.trim_start().len();
            let indent_str = " ".repeat(indent);

            // Pick fresh names that don't collide with anything in scope. Stable
            // numbering keeps the result deterministic and easy to test.
            let let_name = fresh_extract_name(doc, "extracted");
            let fn_name = fresh_extract_name(doc, "extracted_fn");

            let mut changes = HashMap::new();
            changes.insert(
                uri.clone(),
                vec![
                    // Insert let binding before the current line
                    TextEdit {
                        range: Range {
                            start: offset_to_position(&doc.source, line_start),
                            end: offset_to_position(&doc.source, line_start),
                        },
                        new_text: format!("{indent_str}let {let_name} = {trimmed}\n"),
                    },
                    // Replace the selected expression with the variable name
                    TextEdit {
                        range: params.range,
                        new_text: let_name.clone(),
                    },
                ],
            );

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("Extract to let `{let_name}`"),
                kind: Some(CodeActionKind::REFACTOR_EXTRACT),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));

            // Extract function: wrap selected expression in a named function
            let mut fn_changes = HashMap::new();
            // Find free variables in the selected text that are bound in scope
            let free_vars = find_free_vars_in_selection(doc, range_start, range_end);
            let params_str = if free_vars.is_empty() {
                String::new()
            } else {
                format!(" {}", free_vars.join(" "))
            };
            let call_args = if free_vars.is_empty() {
                String::new()
            } else {
                format!(" {}", free_vars.join(" "))
            };

            // Find the enclosing top-level declaration to place the function before it
            let fn_insert_offset = doc
                .module
                .decls
                .iter()
                .find(|d| d.span.start <= range_start && range_end <= d.span.end)
                .map(|d| d.span.start)
                .unwrap_or(0);
            let fn_insert_pos = offset_to_position(&doc.source, fn_insert_offset);

            fn_changes.insert(
                uri.clone(),
                vec![
                    // Insert new function before the enclosing declaration
                    TextEdit {
                        range: Range {
                            start: fn_insert_pos,
                            end: fn_insert_pos,
                        },
                        new_text: format!(
                            "{fn_name}{params_str} = {trimmed}\n\n"
                        ),
                    },
                    // Replace the selected expression with a call
                    TextEdit {
                        range: params.range,
                        new_text: format!("{fn_name}{call_args}"),
                    },
                ],
            );

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("Extract to function `{fn_name}`"),
                kind: Some(CodeActionKind::REFACTOR_EXTRACT),
                edit: Some(WorkspaceEdit {
                    changes: Some(fn_changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
    }

    // Action: Inline variable — if cursor is on a let binding's name, offer to inline it
    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_inline_actions(body, doc, uri, range_start, &mut actions);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_inline_actions(body, doc, uri, range_start, &mut actions);
                    }
                }
            }
            _ => {}
        }
    }

    // Action: Organize imports — remove unused, sort, deduplicate
    if !doc.module.imports.is_empty() {
        // Collect names referenced in the document to detect unused imports.
        let referenced = collect_referenced_names(&doc.module);

        // For each import, check whether any of its top-level names are referenced.
        // We need to parse each imported file to know what it exports.
        let unused_imports: HashSet<String> = doc
            .module
            .imports
            .iter()
            .filter(|imp| !import_is_used(imp, doc, &referenced))
            .map(|imp| imp.path.clone())
            .collect();

        let original_paths: Vec<String> =
            doc.module.imports.iter().map(|i| i.path.clone()).collect();

        let mut kept_paths: Vec<String> = original_paths
            .iter()
            .filter(|p| !unused_imports.contains(p.as_str()))
            .cloned()
            .collect();
        kept_paths.sort();
        kept_paths.dedup();

        // Only emit the action if something would change. Both `first` and
        // `last` are guaranteed to be `Some` here because the outer
        // `!doc.module.imports.is_empty()` check holds, but defensively
        // pattern-match anyway — the cost of a single `if let` is nothing
        // compared to a panic in the LSP loop.
        let changed = kept_paths != original_paths;
        if let (true, Some(first_import), Some(last_import)) =
            (changed, doc.module.imports.first(), doc.module.imports.last())
        {
            let import_range = Range {
                start: offset_to_position(&doc.source, first_import.span.start),
                end: offset_to_position(&doc.source, last_import.span.end),
            };

            let new_text = if kept_paths.is_empty() {
                String::new()
            } else {
                kept_paths
                    .iter()
                    .map(|p| format!("import {p}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            };

            let mut changes = HashMap::new();
            changes.insert(uri.clone(), vec![TextEdit { range: import_range, new_text }]);

            let title = if !unused_imports.is_empty() {
                format!(
                    "Organize imports (remove {} unused)",
                    unused_imports.len()
                )
            } else {
                "Organize imports".to_string()
            };

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title,
                kind: Some(CodeActionKind::SOURCE_ORGANIZE_IMPORTS),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }

        // Also offer per-import "Remove unused import" actions for each unused
        // import (single-shot, simpler than the bulk organize action).
        for imp in &doc.module.imports {
            if !unused_imports.contains(&imp.path) {
                continue;
            }
            // Compute the line range to remove (include trailing newline)
            let line_start = doc.source[..imp.span.start]
                .rfind('\n')
                .map(|p| p + 1)
                .unwrap_or(imp.span.start);
            let line_end = doc.source[imp.span.end..]
                .find('\n')
                .map(|p| imp.span.end + p + 1)
                .unwrap_or(imp.span.end);
            let mut changes = HashMap::new();
            changes.insert(
                uri.clone(),
                vec![TextEdit {
                    range: Range {
                        start: offset_to_position(&doc.source, line_start),
                        end: offset_to_position(&doc.source, line_end),
                    },
                    new_text: String::new(),
                }],
            );
            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("Remove unused import `{}`", imp.path),
                kind: Some(CodeActionKind::QUICKFIX),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
    }

    // Action: convert `if cond then a else b` to `case cond of True -> a | False -> b`.
    // Useful when the user wants to extend the conditional with additional
    // arms (e.g. matching on the cond's variant) without re-typing the body.
    if let Some((if_span, replacement)) =
        find_if_to_case_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(if_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Convert `if` to `case`".to_string(),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: flip a commutative binary operator's operands (e.g. `a == b` → `b == a`).
    // Helpful when a comparison reads more naturally with the other operand
    // first, especially in `if` conditions or `where` filters.
    if let Some((bin_span, replacement)) =
        find_flip_binary_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(bin_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Flip operands".to_string(),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: convert `f x` to `x |> f` (pipe form). Limited to single-argument
    // applications — multi-arg piping invites ambiguity (`f x y` could become
    // `x |> f y` or `y |> f x`), so we punt on those rather than guess.
    if let Some((app_span, replacement)) =
        find_pipe_conversion_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(app_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Convert to pipe".to_string(),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: wrap a `refine expr` in a `case ... of Ok | Err` match. Refined
    // values are returned as `Result RefinementError T`; this action expands
    // the boilerplate of unwrapping it.
    if let Some((refine_span, target_name)) = find_refine_at(doc, range_start) {
        let inner_text = safe_slice(&doc.source, refine_span).to_string();
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(refine_span, &doc.source),
                new_text: format!(
                    "case {inner_text} of\n  Ok {{value: x}} -> x\n  Err {{error: e}} -> e"
                ),
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: format!("Match `Result RefinementError {target_name}`"),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: negate `if` condition and swap branches. Useful when the
    // positive case is rare and the user wants to lead with the common path.
    if let Some((if_span, replacement)) =
        find_if_negate_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(if_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Negate condition (swap branches)".to_string(),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: add `deriving (Eq, Show)` clause to a data declaration that
    // doesn't yet derive any traits. Common boilerplate for fresh ADTs.
    if let Some((data_span, name, insert_pos)) =
        find_deriving_insertion_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: Range {
                    start: insert_pos,
                    end: insert_pos,
                },
                new_text: " deriving (Eq, Show)".to_string(),
            }],
        );
        let _ = data_span;
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: format!("Add `deriving (Eq, Show)` to `{name}`"),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: add a wildcard `_ -> ...` arm to the case expression at the cursor.
    // Useful when the scrutinee is an open variant (constructor pattern from a
    // do-block bind) where exhaustiveness can't be statically verified, or as a
    // quick "stub the rest" while drafting. Skips cases that already have a
    // wildcard arm so we don't add duplicates.
    if let Some((case_span, replacement)) =
        find_add_wildcard_arm_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(case_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Add wildcard `_` arm".to_string(),
            kind: Some(CodeActionKind::QUICKFIX),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: convert a plain type alias into a refined type alias. Inserts
    // a stub predicate `where \x -> True` that the user can edit. Skips type
    // aliases that already have a predicate (refined type aliases parse with
    // `TypeKind::Refined`), and skips record/function types where a top-level
    // refinement isn't idiomatic (those use per-field refinements instead).
    if let Some((alias_span, name, insert_pos)) =
        find_alias_to_refine_at(&doc.module, &doc.source, range_start)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: Range {
                    start: insert_pos,
                    end: insert_pos,
                },
                new_text: " where \\x -> True".to_string(),
            }],
        );
        let _ = alias_span;
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: format!("Make `{name}` a refined type"),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    // Action: wrap selected expression in `Err {error: ...}`. Complements the
    // existing `Wrap in Ok` quick-fix for ergonomic `Result` construction.
    if let Some((expr_span, replacement)) =
        find_wrap_in_err_at(&doc.module, &doc.source, range_start, range_end)
    {
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(expr_span, &doc.source),
                new_text: replacement,
            }],
        );
        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
            title: "Wrap in `Err`".to_string(),
            kind: Some(CodeActionKind::REFACTOR_REWRITE),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    if actions.is_empty() {
        None
    } else {
        Some(actions)
    }
}

/// Find the innermost `case` expression containing `offset` whose arms have no
/// wildcard pattern, and produce a rewrite that appends `_ -> todo` after the
/// last arm. The replacement reuses the existing arms verbatim and adds a new
/// final arm with consistent indentation.
fn find_add_wildcard_arm_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(expr: &ast::Expr, source: &str, offset: usize) -> Option<(Span, String)> {
        if expr.span.start > offset || expr.span.end < offset {
            return None;
        }
        if let ast::ExprKind::Case { arms, .. } = &expr.node {
            // Recurse first — pick the innermost match so nested cases work.
            let mut inner = None;
            crate::utils::recurse_expr(expr, |child| {
                if inner.is_none() {
                    if let Some(hit) = walk(child, source, offset) {
                        inner = Some(hit);
                    }
                }
            });
            if inner.is_some() {
                return inner;
            }

            // Skip if a wildcard already exists. Wildcards parse as
            // `PatKind::Wildcard` or as a `Var` bound that's the underscore.
            let has_wildcard = arms.iter().any(|arm| {
                matches!(&arm.pat.node, ast::PatKind::Wildcard)
                    || matches!(&arm.pat.node, ast::PatKind::Var(n) if n == "_")
            });
            if has_wildcard || arms.is_empty() {
                return None;
            }

            // Compute the indentation of the last arm so the new arm aligns.
            let last = arms.last()?;
            let last_line_start = source[..last.pat.span.start]
                .rfind('\n')
                .map(|p| p + 1)
                .unwrap_or(0);
            let indent: String = source[last_line_start..last.pat.span.start]
                .chars()
                .take_while(|c| c.is_whitespace())
                .collect();

            let case_text = source.get(expr.span.start..expr.span.end)?;
            let mut rewritten = case_text.to_string();
            // Append the new arm. `todo` is intentionally an undefined name so
            // the user gets a clear "fill me in" diagnostic.
            rewritten.push('\n');
            rewritten.push_str(&indent);
            rewritten.push_str("_ -> todo");
            return Some((expr.span, rewritten));
        }
        let mut found = None;
        crate::utils::recurse_expr(expr, |child| {
            if found.is_none() {
                if let Some(hit) = walk(child, source, offset) {
                    found = Some(hit);
                }
            }
        });
        found
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                if let Some(hit) = walk(body, source, offset) {
                    return Some(hit);
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        if let Some(hit) = walk(body, source, offset) {
                            return Some(hit);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Find a `type Name = Base` declaration at the cursor that isn't already
/// refined and isn't a record/function (those use per-field refinements).
/// Returns the alias span, the alias name, and the position to insert the
/// `where \x -> True` clause (immediately after the base type).
fn find_alias_to_refine_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String, Position)> {
    for decl in &module.decls {
        if !(decl.span.start <= offset && offset < decl.span.end) {
            continue;
        }
        if let DeclKind::TypeAlias { name, ty, .. } = &decl.node {
            // Skip if already refined.
            if matches!(&ty.node, TypeKind::Refined { .. }) {
                return None;
            }
            // Refining records or functions at the top level isn't idiomatic.
            // Records use per-field refinements; functions can't be refined
            // by a value-level predicate.
            if matches!(&ty.node, TypeKind::Record { .. } | TypeKind::Function { .. }) {
                return None;
            }
            // Insert at the end of the base type's span — that's where the
            // `where` clause syntactically belongs.
            let pos = offset_to_position(source, ty.span.end);
            return Some((decl.span, name.clone(), pos));
        }
    }
    None
}

/// Find an `if cond then a else b` at `offset` and produce
/// `if not cond then b else a` as the rewritten text.
fn find_if_negate_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(expr: &ast::Expr, source: &str, offset: usize) -> Option<(Span, String)> {
        if expr.span.start > offset || expr.span.end < offset {
            return None;
        }
        if let ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } = &expr.node
        {
            // Recurse first — prefer the innermost match.
            if let Some(inner) = walk(cond, source, offset) {
                return Some(inner);
            }
            if let Some(inner) = walk(then_branch, source, offset) {
                return Some(inner);
            }
            if let Some(inner) = walk(else_branch, source, offset) {
                return Some(inner);
            }
            let cond_text = source.get(cond.span.start..cond.span.end)?;
            let then_text = source.get(then_branch.span.start..then_branch.span.end)?;
            let else_text = source.get(else_branch.span.start..else_branch.span.end)?;
            // Strip a leading `not ` if present so the negation cancels out.
            let new_cond = if let Some(rest) = cond_text.strip_prefix("not ") {
                rest.to_string()
            } else if let Some(rest) = cond_text
                .strip_prefix("(not ")
                .and_then(|s| s.strip_suffix(')'))
            {
                rest.to_string()
            } else {
                format!("not ({cond_text})")
            };
            return Some((
                expr.span,
                format!("if {new_cond} then {else_text} else {then_text}"),
            ));
        }
        let mut found = None;
        crate::utils::recurse_expr(expr, |child| {
            if found.is_none() {
                if let Some(hit) = walk(child, source, offset) {
                    found = Some(hit);
                }
            }
        });
        found
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                if let Some(hit) = walk(body, source, offset) {
                    return Some(hit);
                }
            }
            _ => {}
        }
    }
    None
}

/// Locate a `data Name = ...` declaration at the cursor that has no `deriving`
/// clause. Returns the decl span, the data type name, and the position to
/// insert the deriving clause (immediately after the last constructor).
fn find_deriving_insertion_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String, Position)> {
    for decl in &module.decls {
        if !(decl.span.start <= offset && offset < decl.span.end) {
            continue;
        }
        if let DeclKind::Data {
            name,
            constructors,
            deriving,
            ..
        } = &decl.node
        {
            if !deriving.is_empty() {
                return None;
            }
            let _ = constructors;
            // Insert at the very end of the data declaration's span. Cleanest
            // anchor that doesn't require constructor-level spans, and matches
            // the syntax `data Foo = A | B deriving (Eq, Show)`.
            let pos = offset_to_position(source, decl.span.end);
            return Some((decl.span, name.clone(), pos));
        }
    }
    None
}

/// Wrap the cursor's enclosing expression in `Err {error: ...}` if it sits at
/// an expression position. Best-effort: skips when the cursor isn't on a
/// well-formed expression span.
fn find_wrap_in_err_at(
    module: &Module,
    source: &str,
    range_start: usize,
    range_end: usize,
) -> Option<(Span, String)> {
    fn walk(expr: &ast::Expr, range_start: usize, range_end: usize) -> Option<Span> {
        if expr.span.start > range_end || expr.span.end < range_start {
            return None;
        }
        // Skip wrapping bindings/lambdas/blocks — only wrap leaf-ish exprs.
        match &expr.node {
            ast::ExprKind::Lambda { .. }
            | ast::ExprKind::Do(_)
            | ast::ExprKind::Case { .. }
            | ast::ExprKind::If { .. } => {
                let mut found = None;
                crate::utils::recurse_expr(expr, |child| {
                    if found.is_none() {
                        if let Some(s) = walk(child, range_start, range_end) {
                            found = Some(s);
                        }
                    }
                });
                found
            }
            _ => {
                let mut inner = None;
                crate::utils::recurse_expr(expr, |child| {
                    if inner.is_none() {
                        if let Some(s) = walk(child, range_start, range_end) {
                            inner = Some(s);
                        }
                    }
                });
                inner.or(Some(expr.span))
            }
        }
    }
    // Only fire when the user has an explicit selection — wrapping every
    // expression at a bare cursor position would produce too many actions.
    if range_start == range_end {
        return None;
    }
    for decl in &module.decls {
        if let DeclKind::Fun {
            body: Some(body), ..
        }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } = &decl.node
        {
            if let Some(span) = walk(body, range_start, range_end) {
                let text = source.get(span.start..span.end)?;
                return Some((span, format!("Err {{error: {text}}}")));
            }
        }
    }
    None
}

/// Locate the innermost `refine expr` containing the cursor, returning its full
/// span (including the `refine` keyword) and the resolved target type name.
fn find_refine_at(doc: &DocumentState, offset: usize) -> Option<(Span, String)> {
    let span = doc
        .refine_targets
        .iter()
        .filter(|(s, _)| s.start <= offset && offset < s.end)
        .min_by_key(|(s, _)| s.end - s.start)?;
    Some((*span.0, span.1.clone()))
}

/// Find case expressions at the cursor and offer to fill missing arms.
fn find_case_actions(
    expr: &ast::Expr,
    doc: &DocumentState,
    uri: &Uri,
    range_start: usize,
    range_end: usize,
    actions: &mut Vec<CodeActionOrCommand>,
) {
    if expr.span.end < range_start || expr.span.start > range_end {
        return;
    }

    if let ast::ExprKind::Case { scrutinee, arms } = &expr.node {
        // Try to find the ADT type of the scrutinee
        let scrutinee_type = match &scrutinee.node {
            ast::ExprKind::Var(name) => doc
                .local_type_info
                .iter()
                .find(|(span, _)| safe_slice(&doc.source, **span) == name.as_str())
                .map(|(_, ty)| ty.clone())
                .or_else(|| doc.type_info.get(name).cloned()),
            _ => None,
        };

        if let Some(type_str) = scrutinee_type {
            // Extract the principal type name (handles parametrized types like
            // `Maybe Int`, `Result Text Person`, `[Shape]`, `IO {} Maybe`)
            let type_name = extract_principal_type_name(&type_str);

            if let Some(type_name) = type_name {
                // Find the data declaration for this type
                for decl in &doc.module.decls {
                    if let DeclKind::Data {
                        name, constructors, ..
                    } = &decl.node
                    {
                        if *name != type_name {
                            continue;
                        }
                        let existing: HashSet<String> = arms
                            .iter()
                            .filter_map(|arm| match &arm.pat.node {
                                ast::PatKind::Constructor { name, .. } => Some(name.clone()),
                                _ => None,
                            })
                            .collect();

                        let missing: Vec<&ast::ConstructorDef> = constructors
                            .iter()
                            .filter(|c| !existing.contains(&c.name))
                            .collect();

                        if missing.is_empty() {
                            continue;
                        }

                        // Determine indentation from the existing arms or the case
                        // expression itself, so generated arms align nicely.
                        let arm_indent = arm_indentation(expr, arms, &doc.source);
                        // Default body: the first bound variable, or `todo` if
                        // the constructor is nullary. `todo` is intentionally an
                        // undefined identifier so the user sees a clear error.
                        let new_arms: String = missing
                            .iter()
                            .map(|c| build_case_arm(c, &arm_indent))
                            .collect();

                        let insert_pos = offset_to_position(&doc.source, expr.span.end);
                        let mut changes = HashMap::new();
                        changes.insert(
                            uri.clone(),
                            vec![TextEdit {
                                range: Range {
                                    start: insert_pos,
                                    end: insert_pos,
                                },
                                new_text: new_arms,
                            }],
                        );

                        let names: Vec<&str> =
                            missing.iter().map(|c| c.name.as_str()).collect();
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Add missing case arms: {}", names.join(", ")),
                            kind: Some(CodeActionKind::QUICKFIX),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                        break;
                    }
                }
            }
        }
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_case_actions(func, doc, uri, range_start, range_end, actions);
            find_case_actions(arg, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_case_actions(body, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        find_case_actions(expr, doc, uri, range_start, range_end, actions);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        find_case_actions(e, doc, uri, range_start, range_end, actions);
                    }
                    _ => {}
                }
            }
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            find_case_actions(cond, doc, uri, range_start, range_end, actions);
            find_case_actions(then_branch, doc, uri, range_start, range_end, actions);
            find_case_actions(else_branch, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_case_actions(scrutinee, doc, uri, range_start, range_end, actions);
            for arm in arms {
                find_case_actions(&arm.body, doc, uri, range_start, range_end, actions);
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            find_case_actions(e, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            find_case_actions(target, doc, uri, range_start, range_end, actions);
            find_case_actions(value, doc, uri, range_start, range_end, actions);
        }
        _ => {}
    }
}

/// Collect every identifier name that appears in expressions, types, or
/// patterns in the module. Used to detect unused imports.
fn collect_referenced_names(module: &Module) -> HashSet<String> {
    let mut names = HashSet::new();
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body),
                ty,
                ..
            } => {
                collect_names_in_expr(body, &mut names);
                if let Some(scheme) = ty {
                    collect_names_in_type(&scheme.ty, &mut names);
                }
            }
            DeclKind::View { body, ty, .. } | DeclKind::Derived { body, ty, .. } => {
                collect_names_in_expr(body, &mut names);
                if let Some(scheme) = ty {
                    collect_names_in_type(&scheme.ty, &mut names);
                }
            }
            DeclKind::Source { ty, .. } => {
                collect_names_in_type(ty, &mut names);
            }
            DeclKind::TypeAlias { ty, .. } => {
                collect_names_in_type(ty, &mut names);
            }
            DeclKind::Data { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        collect_names_in_type(&f.value, &mut names);
                    }
                }
            }
            DeclKind::Trait {
                items, supertraits, ..
            } => {
                for sup in supertraits {
                    names.insert(sup.trait_name.clone());
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        ty,
                        default_body: Some(b),
                        ..
                    } = item
                    {
                        collect_names_in_type(&ty.ty, &mut names);
                        collect_names_in_expr(b, &mut names);
                    } else if let ast::TraitItem::Method { ty, .. } = item {
                        collect_names_in_type(&ty.ty, &mut names);
                    }
                }
            }
            DeclKind::Impl {
                trait_name,
                args,
                items,
                constraints,
                ..
            } => {
                names.insert(trait_name.clone());
                for c in constraints {
                    names.insert(c.trait_name.clone());
                }
                for arg in args {
                    collect_names_in_type(arg, &mut names);
                }
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_names_in_expr(body, &mut names);
                    }
                }
            }
            DeclKind::Migrate {
                using_fn,
                from_ty,
                to_ty,
                ..
            } => {
                collect_names_in_expr(using_fn, &mut names);
                collect_names_in_type(from_ty, &mut names);
                collect_names_in_type(to_ty, &mut names);
            }
            DeclKind::Route { entries, .. } => {
                for e in entries {
                    for seg in &e.path {
                        if let ast::PathSegment::Param { ty, .. } = seg {
                            collect_names_in_type(ty, &mut names);
                        }
                    }
                    for f in &e.body_fields {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    for f in &e.query_params {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    for f in &e.request_headers {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    if let Some(t) = &e.response_ty {
                        collect_names_in_type(t, &mut names);
                    }
                    for f in &e.response_headers {
                        collect_names_in_type(&f.value, &mut names);
                    }
                }
            }
            _ => {}
        }
    }
    names
}

fn collect_names_in_expr(expr: &ast::Expr, out: &mut HashSet<String>) {
    match &expr.node {
        ast::ExprKind::Var(n)
        | ast::ExprKind::Constructor(n)
        | ast::ExprKind::SourceRef(n)
        | ast::ExprKind::DerivedRef(n) => {
            out.insert(n.clone());
        }
        ast::ExprKind::Lambda { body, .. } => collect_names_in_expr(body, out),
        ast::ExprKind::App { func, arg } => {
            collect_names_in_expr(func, out);
            collect_names_in_expr(arg, out);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_names_in_expr(lhs, out);
            collect_names_in_expr(rhs, out);
        }
        ast::ExprKind::UnaryOp { operand, .. } => collect_names_in_expr(operand, out),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_names_in_expr(cond, out);
            collect_names_in_expr(then_branch, out);
            collect_names_in_expr(else_branch, out);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_names_in_expr(scrutinee, out);
            for arm in arms {
                collect_names_in_pat(&arm.pat, out);
                collect_names_in_expr(&arm.body, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } | ast::StmtKind::Let { pat, expr } => {
                        collect_names_in_pat(pat, out);
                        collect_names_in_expr(expr, out);
                    }
                    ast::StmtKind::Where { cond } => collect_names_in_expr(cond, out),
                    ast::StmtKind::GroupBy { key } => collect_names_in_expr(key, out),
                    ast::StmtKind::Expr(e) => collect_names_in_expr(e, out),
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => collect_names_in_expr(e, out),
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            collect_names_in_expr(target, out);
            collect_names_in_expr(value, out);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_names_in_expr(&f.value, out);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_names_in_expr(base, out);
            for f in fields {
                collect_names_in_expr(&f.value, out);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_names_in_expr(e, out);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => collect_names_in_expr(expr, out),
        ast::ExprKind::At { relation, time } => {
            collect_names_in_expr(relation, out);
            collect_names_in_expr(time, out);
        }
        ast::ExprKind::UnitLit { value, .. } => collect_names_in_expr(value, out),
        ast::ExprKind::Annot { expr, ty } => {
            collect_names_in_expr(expr, out);
            collect_names_in_type(ty, out);
        }
        ast::ExprKind::Lit(_) => {}
    }
}

fn collect_names_in_pat(pat: &ast::Pat, out: &mut HashSet<String>) {
    match &pat.node {
        ast::PatKind::Constructor { name, payload } => {
            out.insert(name.clone());
            collect_names_in_pat(payload, out);
        }
        ast::PatKind::Record(fields) => {
            for f in fields {
                if let Some(p) = &f.pattern {
                    collect_names_in_pat(p, out);
                }
            }
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_names_in_pat(p, out);
            }
        }
        _ => {}
    }
}

fn collect_names_in_type(ty: &ast::Type, out: &mut HashSet<String>) {
    match &ty.node {
        TypeKind::Named(n) => {
            out.insert(n.clone());
        }
        TypeKind::App { func, arg } => {
            collect_names_in_type(func, out);
            collect_names_in_type(arg, out);
        }
        TypeKind::Record { fields, .. } => {
            for f in fields {
                collect_names_in_type(&f.value, out);
            }
        }
        TypeKind::Relation(inner) => collect_names_in_type(inner, out),
        TypeKind::Function { param, result } => {
            collect_names_in_type(param, out);
            collect_names_in_type(result, out);
        }
        TypeKind::Variant { constructors, .. } => {
            for c in constructors {
                for f in &c.fields {
                    collect_names_in_type(&f.value, out);
                }
            }
        }
        TypeKind::Effectful { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::IO { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::UnitAnnotated { base, .. } => collect_names_in_type(base, out),
        TypeKind::Refined { base, .. } => collect_names_in_type(base, out),
        TypeKind::Forall { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::Var(_) | TypeKind::Hole => {}
    }
}

/// Decide whether an import is used by checking whether any of its top-level
/// definitions appear in the document's referenced-names set. If we can't parse
/// the imported file, conservatively treat the import as used.
fn import_is_used(
    imp: &ast::Import,
    doc: &DocumentState,
    referenced: &HashSet<String>,
) -> bool {
    // Fast path: selective imports list the names directly
    if let Some(items) = &imp.items {
        return items.iter().any(|i| referenced.contains(&i.name));
    }

    // Otherwise scan the import's exported declarations from the cache
    for (name, origin_path) in &doc.import_origins {
        if origin_path == &imp.path && referenced.contains(name) {
            return true;
        }
    }
    // Also check direct names from import_defs (in case origins aren't tracked)
    for (name, (path, _)) in &doc.import_defs {
        // Reconstruct the "origin" from path: this is best-effort, prefer origins
        let origin = doc.import_origins.get(name);
        if origin == Some(&imp.path) && referenced.contains(name) {
            return true;
        }
        let _ = path;
    }
    false
}

/// Locate an effectful builtin call at or near the given offset, for `fork`-wrap suggestions.
fn find_io_call_in_range(doc: &DocumentState, offset: usize) -> Option<Span> {
    // Scan literal/reference info: find a Var span that names an effectful builtin
    // and whose containing AppChain encloses the offset.
    for decl in &doc.module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        let body_opt: Option<&ast::Expr> = match &decl.node {
            DeclKind::Fun { body: Some(b), .. }
            | DeclKind::View { body: b, .. }
            | DeclKind::Derived { body: b, .. } => Some(b),
            _ => None,
        };
        if let Some(body) = body_opt {
            if let Some(span) = find_io_call(body, offset) {
                return Some(span);
            }
        }
        if let DeclKind::Impl { items, .. } = &decl.node {
            for item in items {
                if let ast::ImplItem::Method { body, .. } = item {
                    if let Some(span) = find_io_call(body, offset) {
                        return Some(span);
                    }
                }
            }
        }
    }
    None
}

fn find_io_call(expr: &ast::Expr, offset: usize) -> Option<Span> {
    if expr.span.start > offset || offset > expr.span.end {
        return None;
    }
    // If this expression is an App whose head is an effectful builtin, return
    // the entire App's span.
    if let ast::ExprKind::App { .. } = &expr.node {
        let mut head = expr;
        while let ast::ExprKind::App { func, .. } = &head.node {
            head = func;
        }
        if let ast::ExprKind::Var(name) = &head.node {
            if EFFECTFUL_BUILTINS.contains(&name.as_str()) {
                return Some(expr.span);
            }
        }
    }
    // Recurse, keeping the smallest match
    let mut best: Option<Span> = None;
    let consider = |s: Span, best: &mut Option<Span>| {
        if best
            .as_ref()
            .map_or(true, |b| s.end - s.start < b.end - b.start)
        {
            *best = Some(s);
        }
    };
    let recur = |e: &ast::Expr, best: &mut Option<Span>| {
        if let Some(s) = find_io_call(e, offset) {
            consider(s, best);
        }
    };
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            recur(func, &mut best);
            recur(arg, &mut best);
        }
        ast::ExprKind::Lambda { body, .. } => recur(body, &mut best),
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            recur(lhs, &mut best);
            recur(rhs, &mut best);
        }
        ast::ExprKind::UnaryOp { operand, .. } => recur(operand, &mut best),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            recur(cond, &mut best);
            recur(then_branch, &mut best);
            recur(else_branch, &mut best);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            recur(scrutinee, &mut best);
            for arm in arms {
                recur(&arm.body, &mut best);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. }
                    | ast::StmtKind::Let { expr, .. }
                    | ast::StmtKind::Expr(expr)
                    | ast::StmtKind::Where { cond: expr } => recur(expr, &mut best),
                    ast::StmtKind::GroupBy { key } => recur(key, &mut best),
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => recur(e, &mut best),
        _ => {}
    }
    best
}

/// Pull a `{...}` block out of an effects diagnostic note like
/// `inferred effects: {console, r *foo}`.
fn extract_effect_set_from_message(msg: &str, prefix: &str) -> Option<String> {
    let start = msg.find(prefix)? + prefix.len();
    let rest = msg[start..].trim_start();
    let open = rest.find('{')?;
    let close = rest[open..].find('}')?;
    Some(rest[open..=open + close].to_string())
}

/// Build a TextEdit that widens a function's declared effects to a target set.
/// Looks for the `: ... -> ...` signature in the source and rewrites the head.
fn build_effect_widen_edit(decl: &ast::Decl, source: &str, target_effects: &str) -> Option<TextEdit> {
    // The strategy: find the type annotation signature line that looks like
    // `name : ...` within the declaration span and replace the existing IO
    // effect set or insert one if none exists. We do a minimal textual rewrite
    // rather than re-rendering the whole type, to preserve user formatting.
    let decl_text = source.get(decl.span.start..decl.span.end.min(source.len()))?;
    // Find `: ` after the function name to locate the start of the type signature
    let colon = decl_text.find(": ")?;
    let after_colon = &decl_text[colon + 2..];
    // Find an existing IO effect set: `IO {...}`
    let abs_after_colon = decl.span.start + colon + 2;
    if let Some(io_pos) = after_colon.find("IO {") {
        let abs_io = abs_after_colon + io_pos;
        // Find the matching `}`
        let depth_start = abs_io + 3; // position of `{`
        let bytes = source.as_bytes();
        let mut depth = 0i32;
        for i in depth_start..source.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        // Replace `{...}` with target effects (which already
                        // includes braces).
                        return Some(TextEdit {
                            range: Range {
                                start: offset_to_position(source, depth_start),
                                end: offset_to_position(source, i + 1),
                            },
                            new_text: target_effects.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    // No existing IO effects: insert IO before the result type. We just append
    // a comment hint at the end of the signature line so the user can review.
    None
}

/// A wrap-in-constructor suggestion derived from a type mismatch.
struct WrapSuggestion {
    title: String,
    template: String,
}

/// Placeholder substituted inside `WrapSuggestion::template`. Picked to be
/// unambiguous against Knot syntax: the language's empty-record literal `{}`
/// would collide with a naïve `{}` placeholder (matters for the
/// `Nothing {}` template, which intentionally discards the offending
/// expression and keeps the empty-record syntax verbatim).
const WRAP_PLACEHOLDER: &str = "__EXPR__";

impl WrapSuggestion {
    fn format_wrapping(&self, snippet: &str) -> String {
        // Parenthesize if the snippet contains whitespace so precedence is
        // preserved. Identifiers, parenthesized exprs, and literals don't
        // need extra parens.
        let needs_parens = snippet.contains(' ') && !is_already_parenthesized(snippet);
        let body = if needs_parens {
            format!("({snippet})")
        } else {
            snippet.to_string()
        };
        self.template.replacen(WRAP_PLACEHOLDER, &body, 1)
    }
}

fn is_already_parenthesized(s: &str) -> bool {
    let trimmed = s.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return false;
    }
    // Verify the outer parens match — `(a) (b)` shouldn't count.
    let bytes = trimmed.as_bytes();
    let mut depth: i32 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 && i != bytes.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

/// Pull `(expected, found)` out of a `type mismatch: expected X, found Y`
/// message. Returns `None` if the message doesn't match the expected shape.
fn parse_type_mismatch(msg: &str) -> Option<(String, String)> {
    let after_prefix = msg.strip_prefix("type mismatch:")?.trim_start();
    let after_expected = after_prefix.strip_prefix("expected ")?;
    // Find `, found ` — uses a comma so we can split robustly.
    let split_at = after_expected.find(", found ")?;
    let expected = after_expected[..split_at].trim().to_string();
    let found = after_expected[split_at + ", found ".len()..]
        .trim()
        .trim_end_matches('.')
        .to_string();
    Some((expected, found))
}

/// Decide which wrap suggestions apply for a given expected/found pair.
/// Returns up to a handful of templates whose `{}` placeholder should be
/// substituted with the offending expression. `refined_names` is the set of
/// refined-type aliases visible in the current document — used to suggest
/// `refine` wrapping for `expected: RefinedAlias, found: BaseType` mismatches.
fn detect_wrap_suggestions(
    expected: &str,
    found: &str,
    refined_names: &HashSet<&str>,
) -> Vec<WrapSuggestion> {
    let mut out = Vec::new();
    // `expected Maybe T, found T` → wrap in Just
    if let Some(inner) = expected.strip_prefix("Maybe ") {
        if inner.trim() == found.trim() {
            out.push(WrapSuggestion {
                title: "Wrap in `Just`".to_string(),
                template: format!("Just {{value: {WRAP_PLACEHOLDER}}}"),
            });
        }
    }
    // `expected Maybe T, found {}` → suggest `Nothing {}`. The `{}` here is
    // the empty-record literal Knot uses for unit-like values; if the user
    // wrote `{}` where a `Maybe T` was expected, replacing with `Nothing {}`
    // is almost always what they meant. The replacement template ignores the
    // matched expression entirely (`Nothing {}` takes no payload), so it has
    // no `WRAP_PLACEHOLDER` — `format_wrapping` will return the template
    // verbatim.
    if expected.starts_with("Maybe ") && found.trim() == "{}" {
        out.push(WrapSuggestion {
            title: "Replace with `Nothing {}`".to_string(),
            template: "Nothing {}".to_string(),
        });
    }
    // `expected Result E T, found T` → wrap in Ok. Result is two-arg, but
    // when the success type matches the found type we can offer Ok.
    if let Some(rest) = expected.strip_prefix("Result ") {
        // Result E A: split off the last whitespace-separated token as A.
        // Handles `Result Text Int`. For nested types like
        // `Result Text (Maybe Int)` we fall back to checking whether the
        // suffix matches — best-effort, not exhaustive.
        let trimmed = rest.trim();
        if trimmed.ends_with(found.trim()) {
            // Verify there's at least one whitespace before the suffix so
            // `Result T` (one arg) is rejected.
            let prefix_len = trimmed.len().saturating_sub(found.trim().len());
            if prefix_len > 0
                && trimmed.as_bytes()[prefix_len - 1].is_ascii_whitespace()
            {
                out.push(WrapSuggestion {
                    title: "Wrap in `Ok`".to_string(),
                    template: format!("Ok {{value: {WRAP_PLACEHOLDER}}}"),
                });
            }
        }
    }
    // `expected RefinedAlias, found BaseType` → suggest `refine`. Refined
    // aliases stay nominal in inference; the only way to lift a value into
    // the refined type is through `refine expr`, which yields a
    // `Result RefinementError T` the user must unwrap. Surface this as a
    // wrapping action so the user can opt in to the runtime check.
    let expected_t = expected.trim();
    if refined_names.contains(expected_t) {
        out.push(WrapSuggestion {
            title: format!("Refine into `{expected_t}` (returns `Result`)"),
            template: format!("refine ({WRAP_PLACEHOLDER})"),
        });
    }
    // `expected IO {…} T, found T` → wrap in pure-IO via `\_ -> ...`. We
    // don't know if the user wants the side-effect, so this is best left to
    // the user manually. Skip.
    out
}

/// Build a "Convert to deriving" code action for an empty impl whose trait is
/// fully default-bodied and whose target is a local data type without an
/// existing entry for that trait. Returns `None` when any precondition fails.
fn build_deriving_action(
    impl_decl: &ast::Decl,
    trait_name: &str,
    impl_items: &[ast::ImplItem],
    doc: &DocumentState,
    uri: &Uri,
) -> Option<CodeActionOrCommand> {
    // Only consider literally-empty impls. A non-empty impl with hand-written
    // bodies isn't equivalent to `deriving` — users may rely on the override.
    if !impl_items.is_empty() {
        return None;
    }

    // The impl arg list must be a single Named type referring to a local data
    // declaration. Refuse multi-arg impls (HKT impls like `impl Functor []`)
    // and impls over non-data types — `deriving` only works on data decls.
    let target_name = match doc
        .module
        .decls
        .iter()
        .find_map(|d| match &d.node {
            DeclKind::Impl { args, .. }
                if d.span == impl_decl.span && args.len() == 1 =>
            {
                if let TypeKind::Named(n) = &args[0].node {
                    Some(n.clone())
                } else {
                    None
                }
            }
            _ => None,
        }) {
        Some(n) => n,
        None => return None,
    };

    // Find the data decl. Bail if it already lists this trait — the impl is
    // redundant in that case but `deriving` would be a no-op edit.
    let data_decl = doc.module.decls.iter().find(|d| {
        matches!(&d.node, DeclKind::Data { name, .. } if name == &target_name)
    })?;
    let existing_deriving = match &data_decl.node {
        DeclKind::Data { deriving, .. } => deriving.clone(),
        _ => return None,
    };
    // `data_decl.span.end` covers everything up to (but not including) the
    // start of the next decl. If the file already has `deriving (...)` we
    // bailed above, so the span ends at the last constructor.
    let data_decl_end = data_decl.span.end;
    if existing_deriving.iter().any(|n| n == trait_name) {
        return None;
    }

    // The trait must be fully default-bodied so deriving produces equivalent
    // behavior. If we can't find the trait in this module, bail — the trait
    // may live in another file and we'd need cross-file analysis to verify.
    let trait_decl = doc.module.decls.iter().find_map(|d| {
        if let DeclKind::Trait { name, items, .. } = &d.node {
            if name == trait_name {
                return Some(items.clone());
            }
        }
        None
    })?;
    // The Knot parser splits a method's signature line and its default body
    // line into two separate `TraitItem::Method` entries with the same name —
    // one with `default_body: None` (the signature) and one with
    // `default_body: Some(...)` (the default impl). For deriving to be valid,
    // every distinct method name in the trait must have at least one item
    // carrying a default body.
    use std::collections::HashSet as _HashSet;
    let mut all_method_names: _HashSet<&str> = _HashSet::new();
    let mut names_with_default: _HashSet<&str> = _HashSet::new();
    for item in &trait_decl {
        if let ast::TraitItem::Method {
            name,
            default_body,
            ..
        } = item
        {
            all_method_names.insert(name.as_str());
            if default_body.is_some() {
                names_with_default.insert(name.as_str());
            }
        }
    }
    if all_method_names.is_empty() {
        return None;
    }
    if all_method_names != names_with_default {
        return None;
    }

    // Build the edit: insert `\n  deriving (Trait)` after the last constructor
    // of the data decl, and remove the impl decl entirely.
    let insert_pos = offset_to_position(&doc.source, data_decl_end);

    // Compute the impl removal range — include the trailing newline so we
    // don't leave a blank gap behind.
    let impl_line_end = doc.source[impl_decl.span.end..]
        .find('\n')
        .map(|p| impl_decl.span.end + p + 1)
        .unwrap_or(impl_decl.span.end);
    let impl_line_start = doc.source[..impl_decl.span.start]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(impl_decl.span.start);

    let new_deriving = if existing_deriving.is_empty() {
        format!("\n  deriving ({trait_name})")
    } else {
        let mut all = existing_deriving.clone();
        all.push(trait_name.to_string());
        format!("\n  deriving ({})", all.join(", "))
    };

    let mut changes = HashMap::new();
    changes.insert(
        uri.clone(),
        vec![
            TextEdit {
                range: Range {
                    start: insert_pos,
                    end: insert_pos,
                },
                new_text: new_deriving,
            },
            TextEdit {
                range: Range {
                    start: offset_to_position(&doc.source, impl_line_start),
                    end: offset_to_position(&doc.source, impl_line_end),
                },
                new_text: String::new(),
            },
        ],
    );

    Some(CodeActionOrCommand::CodeAction(CodeAction {
        title: format!("Convert to `deriving ({trait_name})` on `{target_name}`"),
        kind: Some(CodeActionKind::REFACTOR_REWRITE),
        edit: Some(WorkspaceEdit {
            changes: Some(changes),
            ..Default::default()
        }),
        ..Default::default()
    }))
}

/// Build a trait method stub `name p1 p2 = todo` from a trait method declaration.
/// Counts arrows in the type signature to determine arity, then synthesizes
/// fresh `a`, `b`, `c`... parameter names.
fn build_trait_method_stub(item: &ast::TraitItem) -> String {
    let (name, arity) = match item {
        ast::TraitItem::Method { name, ty, .. } => {
            let arity = count_function_arity(&ty.ty);
            (name.clone(), arity)
        }
        _ => return String::new(),
    };
    let params: Vec<String> = (0..arity)
        .map(|i| {
            // Generate a, b, c, ... aa, ab, ...
            let mut s = String::new();
            let mut n = i;
            loop {
                s.insert(0, (b'a' + (n % 26) as u8) as char);
                n = n / 26;
                if n == 0 {
                    break;
                }
                n -= 1;
            }
            s
        })
        .collect();
    let params_str = if params.is_empty() {
        String::new()
    } else {
        format!(" {}", params.join(" "))
    };
    format!("\n  {name}{params_str} = todo")
}

/// Count the arity of a function type by walking the arrow spine.
/// `Int -> Text -> Bool` → 2.
fn count_function_arity(ty: &ast::Type) -> usize {
    let mut count = 0;
    let mut cur = ty;
    loop {
        match &cur.node {
            ast::TypeKind::Function { result, .. } => {
                count += 1;
                cur = result;
            }
            // Look through Forall, IO, and Effectful wrappers
            ast::TypeKind::Forall { ty: inner, .. } => cur = inner,
            ast::TypeKind::IO { ty: inner, .. } => cur = inner,
            ast::TypeKind::Effectful { ty: inner, .. } => cur = inner,
            _ => break,
        }
    }
    count
}

/// Compute the indentation prefix for a new case arm, matching the existing arms
/// or falling back to a default indent relative to the case expression.
fn arm_indentation(case_expr: &ast::Expr, arms: &[ast::CaseArm], source: &str) -> String {
    // Prefer the indentation of an existing arm
    if let Some(arm) = arms.first() {
        let line_start = source[..arm.pat.span.start]
            .rfind('\n')
            .map(|p| p + 1)
            .unwrap_or(0);
        let prefix = &source[line_start..arm.pat.span.start];
        if prefix.chars().all(char::is_whitespace) {
            return format!("\n{prefix}");
        }
    }
    // Fall back: case expression's column + 2
    let line_start = source[..case_expr.span.start]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    let case_col = case_expr.span.start - line_start;
    format!("\n{}", " ".repeat(case_col + 2))
}

/// Build a single case arm string for the given constructor.
/// Bodies use bound-field references when available (e.g. `Just {value} -> value`
/// for return-the-value-as-is) or an undefined `todo` placeholder otherwise,
/// which produces a clear "unknown variable" error rather than a parse error.
fn build_case_arm(c: &ast::ConstructorDef, indent: &str) -> String {
    if c.fields.is_empty() {
        format!("{indent}{} {{}} -> todo", c.name)
    } else {
        let field_names: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
        // Default body: the first bound field name (often the right type for
        // identity-style mappings). User can edit; `todo` is the safe fallback.
        let body = field_names[0];
        format!(
            "{indent}{} {{{}}} -> {body}",
            c.name,
            field_names.join(", ")
        )
    }
}

/// Find free variables in a selection that are bound in surrounding scope.
/// Pick a fresh extract name. Tries the requested base first, then base2,
/// base3, ... until none collide with the document's known top-level decls
/// or local bindings. Used by Extract-to-let / Extract-to-function so we
/// never shadow an existing binding in the user's code.
/// Pick a fresh name for an extracted variable/function that doesn't
/// collide with anything visible to this declaration. Considers:
///  1. Top-level declarations and every reference span in this file.
///  2. Imports — names brought in by `import` statements would shadow
///     a fresh top-level extracted function and break callers, so a
///     name colliding with `import_defs` is also rejected.
///
/// The cross-file collision check is what makes "Extract to function"
/// safe to use in workspaces with many imports. Without it, extracting
/// inside a file that already imports `parse` from elsewhere could pick
/// the name `parse` and silently shadow the import.
fn fresh_extract_name(doc: &DocumentState, base: &str) -> String {
    // Build the set of names to avoid: top-level declarations + every
    // identifier currently bound somewhere in the source. Using
    // `definitions` covers both since it carries name→span for every
    // resolved declaration; we additionally walk references for names
    // bound in nested scopes.
    let mut taken: HashSet<String> = doc.definitions.keys().cloned().collect();
    for (usage_span, _) in &doc.references {
        let name = safe_slice(&doc.source, *usage_span).to_string();
        taken.insert(name);
    }
    // Cross-file collisions: extend the avoid-set with every imported
    // symbol so we don't pick a name that would shadow `import` bindings.
    for name in doc.import_defs.keys() {
        taken.insert(name.clone());
    }
    // Also avoid colliding with built-in names — extracting `filter`
    // would shadow the prelude.
    for builtin in crate::state::builtins() {
        taken.insert(builtin.to_string());
    }
    if !taken.contains(base) {
        return base.to_string();
    }
    for n in 2..1000 {
        let candidate = format!("{base}{n}");
        if !taken.contains(&candidate) {
            return candidate;
        }
    }
    base.to_string()
}

fn find_free_vars_in_selection(
    doc: &DocumentState,
    start: usize,
    end: usize,
) -> Vec<String> {
    let mut free_vars = Vec::new();
    let mut seen = HashSet::new();

    // Check all references that start within the selection range
    for (usage_span, _def_span) in &doc.references {
        if usage_span.start >= start && usage_span.end <= end {
            let name = safe_slice(&doc.source, *usage_span);
            // Only include if it looks like a lowercase variable (not a constructor/type)
            if !name.is_empty()
                && name.chars().next().map_or(false, |c| c.is_lowercase())
                && !seen.contains(name)
            {
                // Check it's a local binding, not a top-level definition
                if doc.local_type_info.keys().any(|span| {
                    span.start < start && safe_slice(&doc.source, *span) == name
                }) {
                    seen.insert(name.to_string());
                    free_vars.push(name.to_string());
                }
            }
        }
    }

    free_vars
}

/// Find inline variable opportunities in do-block let bindings.
fn find_inline_actions(
    expr: &ast::Expr,
    doc: &DocumentState,
    uri: &Uri,
    cursor_offset: usize,
    actions: &mut Vec<CodeActionOrCommand>,
) {
    if expr.span.end < cursor_offset || expr.span.start > cursor_offset {
        return;
    }

    if let ast::ExprKind::Do(stmts) = &expr.node {
        for stmt in stmts {
            if let ast::StmtKind::Let { pat, expr: value_expr } = &stmt.node {
                // Check if cursor is on the let binding
                if stmt.span.start <= cursor_offset && cursor_offset <= stmt.span.end {
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        let value_text = &doc.source
                            [value_expr.span.start..value_expr.span.end.min(doc.source.len())];

                        // Count usages of this variable in subsequent statements
                        let use_count = doc
                            .references
                            .iter()
                            .filter(|(usage, def)| {
                                *def == pat.span
                                    && usage.start > stmt.span.end
                                    && usage.start < expr.span.end
                            })
                            .count();

                        if use_count > 0 {
                            // Build edits: remove the let line, replace all usages with the value
                            let mut edits = Vec::new();

                            // Remove the let statement (including the newline)
                            let let_line_start = doc.source[..stmt.span.start]
                                .rfind('\n')
                                .map(|p| p + 1)
                                .unwrap_or(stmt.span.start);
                            let let_line_end = doc.source[stmt.span.end..]
                                .find('\n')
                                .map(|p| stmt.span.end + p + 1)
                                .unwrap_or(stmt.span.end);

                            edits.push(TextEdit {
                                range: Range {
                                    start: offset_to_position(&doc.source, let_line_start),
                                    end: offset_to_position(&doc.source, let_line_end),
                                },
                                new_text: String::new(),
                            });

                            // Replace each usage with the value (parenthesized if complex)
                            let replacement = if value_text.contains(' ') && use_count > 0 {
                                format!("({value_text})")
                            } else {
                                value_text.to_string()
                            };

                            for (usage_span, def_span) in &doc.references {
                                if *def_span == pat.span
                                    && usage_span.start > stmt.span.end
                                    && usage_span.start < expr.span.end
                                {
                                    edits.push(TextEdit {
                                        range: span_to_range(*usage_span, &doc.source),
                                        new_text: replacement.clone(),
                                    });
                                }
                            }

                            let mut changes = HashMap::new();
                            changes.insert(uri.clone(), edits);

                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: format!("Inline `{var_name}`"),
                                kind: Some(CodeActionKind::REFACTOR_INLINE),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }));
                        }
                    }
                }
            }
        }

        // Recurse into statements
        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { expr: e, .. }
                | ast::StmtKind::Let { expr: e, .. }
                | ast::StmtKind::Expr(e)
                | ast::StmtKind::Where { cond: e } => {
                    find_inline_actions(e, doc, uri, cursor_offset, actions);
                }
                ast::StmtKind::GroupBy { key } => {
                    find_inline_actions(key, doc, uri, cursor_offset, actions);
                }
            }
        }
    }

    // Recurse into other expression types
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_inline_actions(func, doc, uri, cursor_offset, actions);
            find_inline_actions(arg, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_inline_actions(body, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            find_inline_actions(cond, doc, uri, cursor_offset, actions);
            find_inline_actions(then_branch, doc, uri, cursor_offset, actions);
            find_inline_actions(else_branch, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_inline_actions(scrutinee, doc, uri, cursor_offset, actions);
            for arm in arms {
                find_inline_actions(&arm.body, doc, uri, cursor_offset, actions);
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            find_inline_actions(e, doc, uri, cursor_offset, actions);
        }
        _ => {}
    }
}

// ── Auto-import ─────────────────────────────────────────────────────

/// A candidate file that defines `name` as a top-level declaration.
pub(crate) struct AutoImportCandidate {
    /// Already-formatted relative import path (e.g. `./lib/util`).
    pub import_path: String,
}

/// Find files in the workspace that define `name` as a top-level declaration
/// and produce a relative-import path string usable in an `import` statement.
/// Searches both open documents and the cached on-disk parse list. Skips the
/// current file. Returns at most a handful of candidates, sorted by import
/// path so the action list is stable across runs.
pub(crate) fn find_auto_import_candidates(
    state: &ServerState,
    current_uri: &Uri,
    name: &str,
) -> Vec<AutoImportCandidate> {
    let current_path = match crate::utils::uri_to_path(current_uri) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let current_canonical = current_path.canonicalize().unwrap_or(current_path.clone());

    let mut seen: HashSet<std::path::PathBuf> = HashSet::new();
    let mut out: Vec<AutoImportCandidate> = Vec::new();

    // 1. Open documents — fastest, in-memory
    for (other_uri, other_doc) in &state.documents {
        if other_uri == current_uri {
            continue;
        }
        if !other_doc.definitions.contains_key(name) {
            continue;
        }
        let other_path = match crate::utils::uri_to_path(other_uri) {
            Some(p) => p,
            None => continue,
        };
        let canonical = other_path.canonicalize().unwrap_or(other_path);
        push_candidate(&current_canonical, canonical, &mut seen, &mut out);
    }

    // 2. Cached on-disk symbol entries
    if let Ok(cache) = state.workspace_symbol_cache.lock() {
        for (path, (_, _, entries)) in &cache.by_path {
            if entries.iter().any(|e| symbol_entry_matches_name(e, name)) {
                push_candidate(&current_canonical, path.clone(), &mut seen, &mut out);
            }
        }
    }

    // 3. Parsed-but-not-symbolized cache (covers freshly-imported files)
    if let Ok(cache) = state.import_cache.lock() {
        for (path, entry) in cache.iter() {
            if module_declares_name(&entry.module, name) {
                push_candidate(&current_canonical, path.clone(), &mut seen, &mut out);
            }
        }
    }

    // Be defensive: also try a direct scan of workspace roots for files we may
    // not have parsed yet. Only do this if we found no candidates from the
    // in-memory sources, since disk reads aren't free.
    if out.is_empty() {
        let roots = if state.workspace_roots.is_empty() {
            state.workspace_root.iter().cloned().collect::<Vec<_>>()
        } else {
            state.workspace_roots.clone()
        };
        let entries =
            crate::shared::scan_knot_files_in_roots(&roots, state.workspace_root.as_deref());
        for path in entries {
            let canonical = match path.canonicalize() {
                Ok(p) => p,
                Err(_) => continue,
            };
            if seen.contains(&canonical) || canonical == current_canonical {
                continue;
            }
            let source = match std::fs::read_to_string(&canonical) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let lexer = knot::lexer::Lexer::new(&source);
            let (tokens, _) = lexer.tokenize();
            let parser = knot::parser::Parser::new(source.clone(), tokens);
            let (module, _) = parser.parse_module();
            if module_declares_name(&module, name) {
                push_candidate(&current_canonical, canonical, &mut seen, &mut out);
            }
            if out.len() >= 4 {
                break;
            }
        }
    }

    out.sort_by(|a, b| a.import_path.cmp(&b.import_path));
    out.truncate(5);
    out
}

/// Try to add a candidate keyed on a canonical target path. Skips the current
/// file and de-duplicates via the `seen` set.
fn push_candidate(
    current_canonical: &std::path::Path,
    target_canonical: std::path::PathBuf,
    seen: &mut HashSet<std::path::PathBuf>,
    out: &mut Vec<AutoImportCandidate>,
) {
    if target_canonical == current_canonical {
        return;
    }
    if !seen.insert(target_canonical.clone()) {
        return;
    }
    if let Some(rel) = relative_import_path(current_canonical, &target_canonical) {
        out.push(AutoImportCandidate { import_path: rel });
    }
}

/// Check whether a workspace-symbol cache entry matches the bare name. The
/// cache stores names with sigils (`*src`, `&derived`, `route Foo`,
/// `impl T A`) — strip those when comparing.
fn symbol_entry_matches_name(e: &crate::state::WorkspaceSymbolEntry, name: &str) -> bool {
    let stripped = e
        .name
        .strip_prefix('*')
        .or_else(|| e.name.strip_prefix('&'))
        .or_else(|| e.name.strip_prefix("route "))
        .unwrap_or(&e.name);
    if stripped == name {
        return true;
    }
    // `impl` entries are not import-target candidates; ignore them.
    false
}

/// Whether `module` declares a top-level value or type whose name matches
/// `name`. Matches the same set of decls that `import` makes available.
fn module_declares_name(module: &Module, name: &str) -> bool {
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Data {
                name: dname,
                constructors,
                ..
            } => {
                if dname == name {
                    return true;
                }
                for ctor in constructors {
                    if ctor.name == name {
                        return true;
                    }
                }
            }
            DeclKind::TypeAlias { name: dname, .. }
            | DeclKind::Source { name: dname, .. }
            | DeclKind::View { name: dname, .. }
            | DeclKind::Derived { name: dname, .. }
            | DeclKind::Fun { name: dname, .. }
            | DeclKind::Trait { name: dname, .. }
            | DeclKind::Route { name: dname, .. }
            | DeclKind::RouteComposite { name: dname, .. } => {
                if dname == name {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Build a relative import path string (e.g. `./lib/util`, `../shared/x`)
/// from the current file's canonical path to the target file's canonical
/// path. Strips the `.knot` extension from the target. Returns `None` if a
/// relative path can't be computed (e.g. cross-volume on Windows).
pub(crate) fn relative_import_path(from: &std::path::Path, to: &std::path::Path) -> Option<String> {
    let from_dir = from.parent()?;
    let to_no_ext = to.with_extension("");
    let rel = path_diff(&to_no_ext, from_dir)?;
    let s = rel.to_str()?.replace('\\', "/");
    if s.starts_with('.') {
        Some(s)
    } else {
        Some(format!("./{s}"))
    }
}

/// Standard relative-path computation. Walks both paths to find the common
/// prefix, then emits `..` for each base component left over and the
/// remaining target components. Returns `None` if either path contains
/// non-normal-or-root-or-prefix components (the `.` and `..` segments must
/// already be resolved by canonicalize).
fn path_diff(target: &std::path::Path, base: &std::path::Path) -> Option<std::path::PathBuf> {
    use std::path::{Component, PathBuf};

    let mut target_iter = target.components();
    let mut base_iter = base.components();
    let mut common_prefix = 0usize;

    let mut t = target_iter.next();
    let mut b = base_iter.next();
    while let (Some(tc), Some(bc)) = (t, b) {
        if tc == bc {
            common_prefix += 1;
            t = target_iter.next();
            b = base_iter.next();
        } else {
            break;
        }
    }

    let mut out = PathBuf::new();
    let total_base = base.components().count();
    let ascend_count = total_base - common_prefix;
    for _ in 0..ascend_count {
        out.push("..");
    }
    if let Some(tc) = t {
        out.push(tc.as_os_str());
        for c in target_iter {
            out.push(c.as_os_str());
        }
    }
    if out.as_os_str().is_empty() {
        out.push(Component::CurDir.as_os_str());
    }
    Some(out)
}

/// Compute where to insert a new `import` line and what text to use. We
/// always insert at column 0 and append a newline. Position depends on
/// whether the file already has imports.
fn import_insert_position_and_text(doc: &DocumentState, import_path: &str) -> (Position, String) {
    if let Some(last) = doc.module.imports.last() {
        // Insert after the last existing import line. We anchor to the end of
        // the line containing `last.span.end` so the new line is a sibling.
        let after = doc.source[last.span.end..]
            .find('\n')
            .map(|p| last.span.end + p + 1)
            .unwrap_or(doc.source.len());
        let pos = offset_to_position(&doc.source, after);
        (pos, format!("import {import_path}\n"))
    } else {
        // No imports: insert at the very top of the file. If the first line
        // is a comment or whitespace, we still insert above it — keeps the
        // import block visually adjacent to other top-of-file declarations.
        let pos = Position::new(0, 0);
        // Add a trailing blank line so the import doesn't crowd the next decl.
        (pos, format!("import {import_path}\n\n"))
    }
}

/// Whether the document already imports `name` (either by selective list or
/// by importing a module that re-exports it).
fn already_imports(doc: &DocumentState, name: &str) -> bool {
    if doc.import_defs.contains_key(name) {
        return true;
    }
    // Selective imports list specific names — guard against the case where
    // a duplicate `import` action would be redundant.
    for imp in &doc.module.imports {
        if let Some(items) = &imp.items {
            if items.iter().any(|i| i.name == name) {
                return true;
            }
        }
    }
    false
}

/// Locate the smallest `if cond then a else b` expression containing the
/// cursor, and return its span plus the equivalent `case` rewrite. Returns
/// `None` if the cursor isn't inside an if-expression.
fn find_if_to_case_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(
        expr: &ast::Expr,
        source: &str,
        offset: usize,
        best: &mut Option<(Span, String)>,
    ) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } = &expr.node
        {
            let size = expr.span.end - expr.span.start;
            if best.as_ref().map_or(true, |b| size < b.0.end - b.0.start) {
                let cond_text = safe_slice(source, cond.span);
                let then_text = safe_slice(source, then_branch.span);
                let else_text = safe_slice(source, else_branch.span);
                let replacement = format!(
                    "case {cond_text} of\n  True {{}} -> {then_text}\n  False {{}} -> {else_text}"
                );
                *best = Some((expr.span, replacement));
            }
        }
        crate::utils::recurse_expr(expr, |e| walk(e, source, offset, best));
    }
    let mut best = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Locate the smallest commutative binary expression containing the cursor,
/// returning the span and the operand-flipped source text. Limited to ops
/// where flipping preserves semantics — `+`, `*`, `==`, `!=`, `&&`, `||`.
fn find_flip_binary_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(
        expr: &ast::Expr,
        source: &str,
        offset: usize,
        best: &mut Option<(Span, String)>,
    ) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::BinOp { op, lhs, rhs } = &expr.node {
            let op_str = match op {
                ast::BinOp::Add => Some("+"),
                ast::BinOp::Mul => Some("*"),
                ast::BinOp::Eq => Some("=="),
                ast::BinOp::Neq => Some("!="),
                ast::BinOp::And => Some("&&"),
                ast::BinOp::Or => Some("||"),
                _ => None,
            };
            if let Some(op_text) = op_str {
                let size = expr.span.end - expr.span.start;
                if best.as_ref().map_or(true, |b| size < b.0.end - b.0.start) {
                    let lhs_text = safe_slice(source, lhs.span);
                    let rhs_text = safe_slice(source, rhs.span);
                    let replacement = format!("{rhs_text} {op_text} {lhs_text}");
                    *best = Some((expr.span, replacement));
                }
            }
        }
        crate::utils::recurse_expr(expr, |e| walk(e, source, offset, best));
    }
    let mut best = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Locate the smallest single-argument application `f x` containing the
/// cursor and rewrite it to pipe form `x |> f`. Multi-arg applications are
/// skipped — `f x y` could pipe in either argument and we'd rather not
/// guess.
fn find_pipe_conversion_at(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(
        expr: &ast::Expr,
        source: &str,
        offset: usize,
        best: &mut Option<(Span, String)>,
    ) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::App { func, arg } = &expr.node {
            // Only convert applications whose function is a simple identifier
            // (`f x` rather than `(g h) x`). Piping a curried partial
            // application reads strangely.
            let is_simple = matches!(
                &func.node,
                ast::ExprKind::Var(_) | ast::ExprKind::Constructor(_)
            );
            if is_simple {
                let size = expr.span.end - expr.span.start;
                if best.as_ref().map_or(true, |b| size < b.0.end - b.0.start) {
                    let func_text = safe_slice(source, func.span);
                    let arg_text = safe_slice(source, arg.span);
                    // Wrap the arg in parens when it isn't a single token —
                    // a do-block or lambda piped raw would change parse
                    // structure.
                    let arg_needs_parens = arg_text.chars().any(|c| c.is_whitespace());
                    let arg_part = if arg_needs_parens && !is_already_parenthesized(arg_text) {
                        format!("({arg_text})")
                    } else {
                        arg_text.to_string()
                    };
                    let replacement = format!("{arg_part} |> {func_text}");
                    *best = Some((expr.span, replacement));
                }
            }
        }
        crate::utils::recurse_expr(expr, |e| walk(e, source, offset, best));
    }
    let mut best = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempWorkspace;

    fn make_unknown_diag(range: Range, name: &str) -> Diagnostic {
        Diagnostic {
            range,
            message: format!("unknown variable: {name}"),
            severity: Some(DiagnosticSeverity::ERROR),
            ..Default::default()
        }
    }

    #[test]
    fn auto_import_offers_quickfix_for_unknown_name_defined_in_sibling_file() {
        let mut tw = TempWorkspace::new();
        let _other_uri = tw.write_and_open(
            "util.knot",
            "doubleIt : Int -> Int\ndoubleIt x = x * 2\n",
        );
        let main_uri = tw.write_and_open(
            "main.knot",
            "main : IO {} {}\nmain = println (show (doubleIt 5))\n",
        );

        let doc = tw.workspace.doc(&main_uri);
        let needle = "doubleIt 5";
        let off = doc.source.find(needle).expect("needle found");
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + "doubleIt".len()),
        };
        let diag = make_unknown_diag(range, "doubleIt");

        let params = CodeActionParams {
            text_document: TextDocumentIdentifier {
                uri: main_uri.clone(),
            },
            range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };

        let actions = handle_code_action(&tw.workspace.state, &params)
            .expect("code action response");
        let import_actions: Vec<&CodeAction> = actions
            .iter()
            .filter_map(|a| match a {
                CodeActionOrCommand::CodeAction(ca)
                    if ca.title.starts_with("Import `doubleIt`") =>
                {
                    Some(ca)
                }
                _ => None,
            })
            .collect();
        assert!(
            !import_actions.is_empty(),
            "expected an auto-import action, got: {:?}",
            actions
                .iter()
                .map(|a| match a {
                    CodeActionOrCommand::CodeAction(ca) => ca.title.clone(),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
        );

        let edit = import_actions[0].edit.as_ref().expect("has edit");
        let changes = edit.changes.as_ref().expect("changes map");
        let edits = changes.get(&main_uri).expect("edits for main.knot");
        assert_eq!(edits.len(), 1);
        let new_text = &edits[0].new_text;
        assert!(
            new_text.contains("import ./util"),
            "expected `import ./util` in new_text, got {new_text:?}"
        );
    }

    #[test]
    fn auto_import_skips_when_name_already_imported() {
        let mut tw = TempWorkspace::new();
        let _other_uri = tw.write_and_open(
            "util.knot",
            "doubleIt : Int -> Int\ndoubleIt x = x * 2\n",
        );
        // main.knot already imports util — auto-import should not offer to add
        // it again. (The diagnostic is hypothetical; we still check that no
        // duplicate import edit is proposed.)
        let main_uri = tw.write_and_open(
            "main.knot",
            "import ./util\nmain : IO {} {}\nmain = println (show (doubleIt 5))\n",
        );
        let doc = tw.workspace.doc(&main_uri);
        let off = doc.source.find("doubleIt 5").unwrap();
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + "doubleIt".len()),
        };
        let diag = make_unknown_diag(range, "doubleIt");
        let params = CodeActionParams {
            text_document: TextDocumentIdentifier {
                uri: main_uri.clone(),
            },
            range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let actions = handle_code_action(&tw.workspace.state, &params).unwrap_or_default();
        let has_import = actions.iter().any(|a| match a {
            CodeActionOrCommand::CodeAction(ca) => ca.title.starts_with("Import `doubleIt`"),
            _ => false,
        });
        assert!(
            !has_import,
            "should not offer auto-import when already imported"
        );
    }

    #[test]
    fn relative_import_path_handles_subdirs_and_parent() {
        use std::path::PathBuf;
        // Same dir
        let from = PathBuf::from("/a/b/main.knot");
        let to = PathBuf::from("/a/b/util.knot");
        assert_eq!(
            relative_import_path(&from, &to),
            Some("./util".to_string())
        );
        // Subdir
        let to2 = PathBuf::from("/a/b/lib/helper.knot");
        assert_eq!(
            relative_import_path(&from, &to2),
            Some("./lib/helper".to_string())
        );
        // Parent
        let to3 = PathBuf::from("/a/shared.knot");
        assert_eq!(
            relative_import_path(&from, &to3),
            Some("../shared".to_string())
        );
    }

    #[test]
    fn parse_type_mismatch_extracts_expected_and_found() {
        let m = "type mismatch: expected Maybe Int, found Int";
        assert_eq!(
            parse_type_mismatch(m),
            Some(("Maybe Int".to_string(), "Int".to_string()))
        );
        let m2 = "type mismatch: expected Result Text Int, found Int.";
        assert_eq!(
            parse_type_mismatch(m2),
            Some(("Result Text Int".to_string(), "Int".to_string()))
        );
        assert_eq!(parse_type_mismatch("not a real diagnostic"), None);
    }

    #[test]
    fn detect_wrap_suggestions_offers_just_for_maybe() {
        let empty: HashSet<&str> = HashSet::new();
        let suggestions = detect_wrap_suggestions("Maybe Int", "Int", &empty);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].title, "Wrap in `Just`");
        assert_eq!(suggestions[0].format_wrapping("5"), "Just {value: 5}");
        assert_eq!(
            suggestions[0].format_wrapping("a + b"),
            "Just {value: (a + b)}"
        );
    }

    #[test]
    fn detect_wrap_suggestions_offers_nothing_for_unit_in_maybe_slot() {
        // The user wrote `{}` (empty record / unit) where `Maybe T` was
        // expected. The conventional fix is `Nothing {}` — discarding the
        // unit literal entirely, since `Nothing` takes no payload.
        let empty: HashSet<&str> = HashSet::new();
        let suggestions = detect_wrap_suggestions("Maybe Int", "{}", &empty);
        let nothing = suggestions
            .iter()
            .find(|s| s.title.contains("Nothing"))
            .expect("expected a Nothing suggestion");
        // Template has no placeholder — the body is discarded verbatim.
        assert_eq!(nothing.format_wrapping("{}"), "Nothing {}");
        assert_eq!(nothing.format_wrapping("anything"), "Nothing {}");
    }

    #[test]
    fn detect_wrap_suggestions_offers_ok_for_result() {
        let empty: HashSet<&str> = HashSet::new();
        let suggestions = detect_wrap_suggestions("Result Text Int", "Int", &empty);
        assert!(suggestions.iter().any(|s| s.title == "Wrap in `Ok`"));
    }

    #[test]
    fn detect_wrap_suggestions_no_match_when_unrelated() {
        let empty: HashSet<&str> = HashSet::new();
        let suggestions = detect_wrap_suggestions("Text", "Int", &empty);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn detect_wrap_suggestions_offers_refine_for_refined_alias() {
        let mut refined: HashSet<&str> = HashSet::new();
        refined.insert("Nat");
        let suggestions = detect_wrap_suggestions("Nat", "Int", &refined);
        assert!(
            suggestions.iter().any(|s| s.title.starts_with("Refine into")),
            "expected refine suggestion; got: {:?}",
            suggestions
                .iter()
                .map(|s| s.title.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn wrap_in_just_quickfix_emitted_on_type_mismatch_diagnostic() {
        let mut tw = TempWorkspace::new();
        let uri = tw.write_and_open(
            "main.knot",
            "f : Maybe Int -> Int\nf m = case m of\n  Just {value} -> value\n  Nothing {} -> 0\n\nmain : IO {} {}\nmain = println (show (f 5))\n",
        );
        let doc = tw.workspace.doc(&uri);
        // Synthesize a diagnostic at `5` claiming Maybe Int was expected.
        let off = doc.source.rfind("5").unwrap();
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + 1),
        };
        let diag = Diagnostic {
            range,
            message: "type mismatch: expected Maybe Int, found Int".into(),
            severity: Some(DiagnosticSeverity::ERROR),
            ..Default::default()
        };
        let params = CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let actions = handle_code_action(&tw.workspace.state, &params).expect("response");
        let wrap_action = actions.iter().find_map(|a| match a {
            CodeActionOrCommand::CodeAction(ca) if ca.title == "Wrap in `Just`" => Some(ca),
            _ => None,
        });
        let action = wrap_action.expect("wrap action expected");
        let edit = &action
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&uri)
            .unwrap()[0];
        assert_eq!(edit.new_text, "Just {value: 5}");
    }

    #[test]
    fn deriving_action_offered_for_empty_impl_with_default_methods() {
        let mut tw = TempWorkspace::new();
        let uri = tw.write_and_open(
            "main.knot",
            r#"data Color
  = Red {}
  | Green {}
  | Blue {}

trait Describe a where
  describe : a -> Text
  describe x = show x

impl Describe Color where
"#,
        );
        let doc = tw.workspace.doc(&uri);
        let off = doc.source.find("impl Describe Color").unwrap();
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + 4),
        };
        let params = CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let actions = handle_code_action(&tw.workspace.state, &params)
            .expect("expected action response");
        let derive_action = actions.iter().find_map(|a| match a {
            CodeActionOrCommand::CodeAction(ca)
                if ca.title.starts_with("Convert to `deriving (Describe)`") =>
            {
                Some(ca)
            }
            _ => None,
        });
        let action = derive_action
            .expect("deriving action not found");
        let edits = action
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&uri)
            .unwrap();
        // Expect 2 edits: insert deriving on data decl + remove impl decl.
        assert_eq!(edits.len(), 2, "expected 2 edits");
        let has_deriving = edits.iter().any(|e| e.new_text.contains("deriving (Describe)"));
        assert!(has_deriving, "no edit inserts deriving clause");
        let has_remove = edits.iter().any(|e| e.new_text.is_empty());
        assert!(has_remove, "no edit removes the empty impl");
    }

    #[test]
    fn deriving_action_skipped_when_impl_has_methods() {
        let mut tw = TempWorkspace::new();
        let uri = tw.write_and_open(
            "main.knot",
            r#"data Color
  = Red {}

trait Describe a where
  describe : a -> Text
  describe x = show x

impl Describe Color where
  describe c = "red"
"#,
        );
        let doc = tw.workspace.doc(&uri);
        let off = doc.source.find("impl Describe Color").unwrap();
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + 4),
        };
        let params = CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let actions = handle_code_action(&tw.workspace.state, &params).unwrap_or_default();
        let has = actions.iter().any(|a| match a {
            CodeActionOrCommand::CodeAction(ca) => ca.title.starts_with("Convert to `deriving"),
            _ => false,
        });
        assert!(
            !has,
            "should not offer deriving for impl that overrides methods"
        );
    }

    #[test]
    fn auto_import_inserts_after_existing_imports() {
        let mut tw = TempWorkspace::new();
        let _ = tw.write_and_open(
            "util.knot",
            "helper : Int -> Int\nhelper x = x\n",
        );
        let _ = tw.write_and_open(
            "other.knot",
            "doubleIt : Int -> Int\ndoubleIt x = x * 2\n",
        );
        let main_uri = tw.write_and_open(
            "main.knot",
            "import ./util\nmain : IO {} {}\nmain = println (show (doubleIt 5))\n",
        );
        let doc = tw.workspace.doc(&main_uri);
        let off = doc.source.find("doubleIt 5").unwrap();
        let range = Range {
            start: crate::utils::offset_to_position(&doc.source, off),
            end: crate::utils::offset_to_position(&doc.source, off + "doubleIt".len()),
        };
        let diag = make_unknown_diag(range, "doubleIt");
        let params = CodeActionParams {
            text_document: TextDocumentIdentifier {
                uri: main_uri.clone(),
            },
            range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let actions = handle_code_action(&tw.workspace.state, &params).expect("response");
        let import_action = actions
            .iter()
            .find_map(|a| match a {
                CodeActionOrCommand::CodeAction(ca)
                    if ca.title.starts_with("Import `doubleIt`") =>
                {
                    Some(ca)
                }
                _ => None,
            })
            .expect("has import action");
        let edit = &import_action
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&main_uri)
            .unwrap()[0];
        // Expect insertion at the start of line 1 (after `import ./util\n`).
        assert_eq!(edit.range.start.line, 1);
        assert_eq!(edit.range.start.character, 0);
        assert!(edit.new_text.contains("import ./other"));
    }
}
