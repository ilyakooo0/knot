//! `textDocument/codeAction` handler. Synthesizes quick fixes, refactors,
//! and the unused-import organize action.

use std::collections::{HashMap, HashSet};

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span, TypeKind};

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
    // Staleness guard: code actions compute edits from spans in the last
    // *analyzed* source. If the editor holds newer (pending) text, those
    // edits would land at the wrong offsets — bail and let the client
    // re-request once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let mut actions = Vec::new();

    // The LSP spec doesn't guarantee `range.start <= range.end`; a buggy
    // client can send an inverted range. Normalize so the offsets are ordered
    // (the extract-variable path slices `doc.source[range_start..range_end]`,
    // which panics when `range_start > range_end`).
    let (range_start, range_end) = {
        let a = position_to_offset(&doc.source, params.range.start);
        let b = position_to_offset(&doc.source, params.range.end);
        if a <= b { (a, b) } else { (b, a) }
    };

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
        if let DeclKind::Fun { name, ty: None, .. } = &decl.node
            && let Some(inferred) = doc.type_info.get(name) {
                let signature = match doc.effect_sets.get(name) {
                    Some(eff) => render_signature_with_effects(inferred, eff),
                    None => inferred.clone(),
                };
                // Insert the annotation inline on the definition
                // (`name : Sig = body`), mirroring the View/Derived branch
                // below — not as a separate standalone signature line.
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

            // NOTE: a "Wrap IO in `fork`" quickfix used to be offered here for
            // the "IO effects are not allowed inside atomic" diagnostic, but
            // the effect inferencer propagates the argument's effects through
            // `fork` (`fork : ∀a r. IO {| r} a -> IO {| r} {}`), so the wrap
            // never fixed the diagnostic — it was just re-offered on the inner
            // span, nesting `fork (fork (…))` forever. A quickfix that doesn't
            // fix is worse than none, so it was removed; "Remove `atomic`
            // wrapper" above remains the effective fix.
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
                    && let Some(edit) = build_effect_widen_edit(decl, &doc.source, &inferred) {
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

        // Wrap-in-constructor quick fixes: when a type mismatch shows that an
        // expression of type `T` is being passed where `Maybe T`, `Result e T`,
        // or `IO ... T` is expected, offer to wrap the expression in the
        // appropriate constructor. Cheaper than asking users to rewrite the
        // expression themselves.
        if msg.starts_with("type mismatch:")
            && let Some((expected, found)) = parse_type_mismatch(msg) {
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

        // Unit-mismatch quick fixes: when the inferred unit on a numeric
        // expression doesn't match what the surrounding context expects
        // (e.g. `Float M` flowing into a `Float Ft` slot), offer to wrap the
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

                    // Int variant — for `Int u1` ↔ `Int u2` mismatches.
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
                // Find similar names using edit distance. Dedup across
                // definitions and builtins (a name can appear in both) so we
                // don't emit duplicate quick-fixes, and sort by (distance,
                // name) for a deterministic order — `doc.definitions` iterates
                // in random hash order, which previously made the suggestion
                // subset and the `is_preferred` flag nondeterministic.
                let mut candidates: Vec<(&str, usize)> = Vec::new();
                let mut seen_names: HashSet<&str> = HashSet::new();
                for name in doc.definitions.keys() {
                    let dist = edit_distance(unknown_name, name);
                    if dist > 0 && dist <= 2 && seen_names.insert(name.as_str()) {
                        candidates.push((name.as_str(), dist));
                    }
                }
                for name in state_builtins() {
                    let dist = edit_distance(unknown_name, name);
                    if dist > 0 && dist <= 2 && seen_names.insert(name) {
                        candidates.push((name, dist));
                    }
                }
                candidates.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));

                for (idx, (suggestion, _)) in candidates.iter().take(3).enumerate() {
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
                        // The closest match (deterministic first entry) is the
                        // preferred quick-fix.
                        is_preferred: Some(idx == 0),
                        ..Default::default()
                    }));
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
            _ => {}
        }
    }

    // Action: Extract variable — if a non-trivial expression is selected, offer to extract it
    if range_start != range_end {
        let selected_text = &doc.source[range_start..range_end.min(doc.source.len())];
        let trimmed = selected_text.trim();
        // The extracted text is the TRIMMED selection, so the replacement must
        // cover exactly the trimmed span — not the raw one. Replacing the raw
        // span would delete the surrounding whitespace along with the
        // expression; for a selection that runs to the end of a line that means
        // eating the trailing newline, gluing the call site onto the next
        // declaration and breaking the layout-sensitive parse. The trimmed
        // offsets also keep the enclosing-decl / do-statement lookups below from
        // being thrown off by whitespace that spills past the node's span.
        let sel_start =
            range_start + (selected_text.len() - selected_text.trim_start().len());
        let sel_end = sel_start + trimmed.len();
        let sel_range = Range {
            start: offset_to_position(&doc.source, sel_start),
            end: offset_to_position(&doc.source, sel_end),
        };
        // Only offer for non-trivial selections (not just a name or empty) that
        // line up exactly with a whole expression node. Replacing the selection
        // with a variable/call only preserves meaning when the selection IS a
        // complete sub-expression; a fragment that straddles operator-precedence
        // boundaries — e.g. `a + b` inside `2 * a + b`, which parses as
        // `(2 * a) + b` — matches no node, and extracting it would silently
        // rewrite the value (`2 * (a + b)`). The exact-span check rejects those.
        if !trimmed.is_empty()
            && trimmed.len() > 1
            && !trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
            && selection_matches_expr_node(&doc.module, &doc.source, sel_start, sel_end)
        {
            // Pick fresh names that don't collide with anything in scope. Stable
            // numbering keeps the result deterministic and easy to test.
            let let_name = fresh_extract_name(doc, "extracted");
            let fn_name = fresh_extract_name(doc, "extracted_fn");

            // Statement-form `with {x: e} (do …)` only parses inside `do`
            // blocks, so the with-extraction is offered only when the
            // selection sits inside a do-block statement. The `with` wraps
            // the enclosing statement and every following statement in the
            // same block: `with {name: e} (do\n  <stmt>\n  <rest>)`.
            if let Some((stmt_start, block_end)) =
                enclosing_do_stmt_range(&doc.module, sel_start, sel_end)
            {
                let line_start = doc.source[..stmt_start]
                    .rfind('\n')
                    .map(|p| p + 1)
                    .unwrap_or(0);
                let prefix = &doc.source[line_start..stmt_start];
                // Build the replacement: swap the selection for the bound
                // name inside the statement text, then re-indent every line
                // of the wrapped statements by two extra spaces so the
                // layout parser sees them as the `with` body.
                let stmt_text = &doc.source[stmt_start..block_end.min(doc.source.len())];
                let sel_off_start = sel_start - stmt_start;
                let sel_off_end = sel_end - stmt_start;
                let mut body_text = stmt_text.to_string();
                body_text.replace_range(sel_off_start..sel_off_end, &let_name);
                let mut body_lines: Vec<&str> = body_text.lines().collect();
                for line in body_lines.iter_mut() {
                    *line = line.trim_start();
                }
                // The wrapped statements sit inside the `with … (do …)` body,
                // indented past the `with` keyword's own column. The original
                // line prefix (`main = do `, leading whitespace, etc.) must NOT
                // be duplicated into the body — only its width matters.
                let body_indent = " ".repeat(prefix.len() + 2);
                let cont_indent = " ".repeat(prefix.len() + 6);
                let reindented_body = body_lines
                    .iter()
                    .enumerate()
                    .map(|(i, line)| {
                        if i == 0 {
                            format!("{body_indent}{line}")
                        } else {
                            // Continuation lines keep their original relative
                            // indent inside the statement, shifted by the
                            // body's indent.
                            format!("{cont_indent}{}", line)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                // Non-atomic values must be parenthesized in the new
                // brace-pattern syntax: `{x v}` for atoms, `{x (a + b)}`
                // otherwise — a bare `{x 1 + 2}` parses `1` as the value
                // and chokes on `+`.
                let value_text = if is_atomic_expr_text(trimmed) {
                    trimmed.to_string()
                } else {
                    format!("({trimmed})")
                };
                let with_text = format!(
                    "{prefix}with {{{let_name} {value_text}}} (do\n{reindented_body})"
                );

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: offset_to_position(&doc.source, line_start),
                            end: offset_to_position(&doc.source, block_end),
                        },
                        new_text: with_text,
                    }],
                );

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Extract to with `{let_name}`"),
                    kind: Some(CodeActionKind::REFACTOR_EXTRACT),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }

            // Extract function: wrap selected expression in a named function
            let mut fn_changes = HashMap::new();
            // Find free variables in the selected text that are bound in scope.
            // Top-level Knot functions take parameters via lambdas
            // (`name = \x y -> body`), NOT via parameters on the left-hand
            // side — `name x = body` is a parse error at top level.
            let free_vars = find_free_vars_in_selection(doc, sel_start, sel_end);
            let helper_decl = if free_vars.is_empty() {
                format!("{fn_name} = {trimmed}\n\n")
            } else {
                format!("{fn_name} = \\{} -> {trimmed}\n\n", free_vars.join(" "))
            };
            // Build the call-site replacement. When the helper takes
            // arguments, the call must be parenthesized: without parens,
            // replacing `x + 2` inside `show (x + 2)` yields
            // `show extracted_fn x`, which parses as `(show extracted_fn) x`
            // — the wrong application order. A bare zero-arg call needs no
            // parens (it's a single atom already).
            let call_site = if free_vars.is_empty() {
                fn_name.clone()
            } else {
                format!("({fn_name} {})", free_vars.join(" "))
            };

            // Find the enclosing top-level declaration to place the function
            // before it.
            let fn_insert_offset = doc
                .module
                .decls
                .iter()
                .find(|d| d.span.start <= sel_start && sel_end <= d.span.end)
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
                        new_text: helper_decl,
                    },
                    // Replace the selected expression with a call
                    TextEdit {
                        range: sel_range,
                        new_text: call_site,
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

    // Action: Inline variable — if the cursor is on a field NAME of a
    // `with {name: value} body` binding, offer to substitute the value at
    // every usage in `body` and unwrap the `with`.
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
            DeclKind::Fun { body: None, .. } => {}
            _ => {}
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
        let indent = indent_for_expr_start(&doc.source, refine_span.start);
        let mut changes = HashMap::new();
        changes.insert(
            uri.clone(),
            vec![TextEdit {
                range: span_to_range(refine_span, &doc.source),
                new_text: format!(
                    "case {inner_text} of{indent}Ok {{value x}} -> x{indent}Err {{error e}} -> e"
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
                if inner.is_none()
                    && let Some(hit) = walk(child, source, offset) {
                        inner = Some(hit);
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

            // Compute the indentation for the new arm. `arm_indentation`
            // prefers an existing arm's own-line indentation and falls back
            // to the case expression's column + 2 — the fallback matters for
            // single-line cases (`v = case x of A {} -> 1`), where naively
            // taking the arm's line-leading whitespace yields an empty
            // indent that would terminate the layout-sensitive case block.
            let indent = arm_indentation(expr, arms, source);

            // Insert the new arm right after the last arm's body, NOT at
            // `expr.span.end`. When the case is wrapped in parens
            // (`show (case c of Red {} -> 1)`), the parser folds the enclosing
            // parens into the case node's span, so `expr.span.end` points past
            // the closing `)`; inserting there would land the arm outside the
            // parens and break the parse. The last arm's body always ends
            // inside the parens. `arms` is guaranteed non-empty here (the
            // empty-arms case returned above).
            let insert_at = arms.last()?.body.span.end;
            // The new arm. `todo` is intentionally an undefined name so the
            // user gets a clear "fill me in" diagnostic.
            let mut rewritten = String::new();
            rewritten.push_str(&indent);
            rewritten.push_str("_ -> todo");
            return Some((Span::new(insert_at, insert_at), rewritten));
        }
        let mut found = None;
        crate::utils::recurse_expr(expr, |child| {
            if found.is_none()
                && let Some(hit) = walk(child, source, offset) {
                    found = Some(hit);
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
            // Multi-line branches carry indentation tied to their original
            // position; swapping them onto one line (or into each other's
            // columns) breaks the layout-sensitive parse. Only offer the
            // action when the whole rewrite stays on a single line.
            if cond_text.contains('\n') || then_text.contains('\n') || else_text.contains('\n') {
                return None;
            }
            // Strip the `not` only when the condition's AST ROOT is the
            // negation — a textual prefix check is wrong for `not a && b`,
            // which parses as `(not a) && b`: stripping the prefix would
            // negate only the first conjunct. Otherwise wrap the whole
            // condition in `not (…)`.
            let new_cond = if let ast::ExprKind::UnaryOp {
                op: ast::UnaryOp::Not,
                operand,
            } = &cond.node
            {
                source.get(operand.span.start..operand.span.end)?.to_string()
            } else {
                format!("not ({cond_text})")
            };
            let rewrite = format!("if {new_cond} then {else_text} else {then_text}");
            // When the `if` sits in operand position it's parenthesized, and
            // the parser folds those parens into this expr's span while keeping
            // the bare `If` node (see parse_atom's `LParen` arm). Replacing the
            // whole `(if …)` span with an unparenthesized rewrite would let a
            // trailing operator bind into the else branch —
            // `(if c then a else b) * 2` → `if not c then b else a * 2`. Re-wrap
            // to keep the operand outside the conditional.
            let expr_text = source.get(expr.span.start..expr.span.end)?;
            let rewrite = if is_already_parenthesized(expr_text) {
                format!("({rewrite})")
            } else {
                rewrite
            };
            return Some((expr.span, rewrite));
        }
        let mut found = None;
        crate::utils::recurse_expr(expr, |child| {
            if found.is_none()
                && let Some(hit) = walk(child, source, offset) {
                    found = Some(hit);
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

/// Byte offset just past the last non-whitespace character of a declaration.
/// Decl spans include the trailing newline run the parser consumed, so
/// "insert at the end of the decl" anchors must trim that whitespace first —
/// otherwise inserted text glues onto the start of the next declaration.
fn decl_text_end(source: &str, span: Span) -> usize {
    let text = safe_slice(source, span);
    span.start.min(source.len()) + text.trim_end().len()
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
            // Insert at the end of the declaration's TEXT, not its span —
            // data decl spans include the trailing newline run (the parser's
            // skip_newlines collapses them into the decl), so inserting at
            // `span.end` would glue the clause onto the NEXT declaration.
            let pos = offset_to_position(source, decl_text_end(source, decl.span));
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
        // Only expressions that FULLY CONTAIN the selection are candidates.
        // Descending into merely-overlapping children would wrap a fragment
        // of the selection (e.g. selecting `x + 1` in `\x -> x + 1` would
        // wrap just `x`, producing `Err {error: x} + 1`).
        if !(expr.span.start <= range_start && range_end <= expr.span.end) {
            return None;
        }
        // Prefer the smallest child that still contains the whole selection.
        let mut inner = None;
        crate::utils::recurse_expr(expr, |child| {
            if inner.is_none()
                && let Some(s) = walk(child, range_start, range_end) {
                    inner = Some(s);
                }
        });
        if inner.is_some() {
            return inner;
        }
        // Skip wrapping bindings/lambdas/blocks — only wrap leaf-ish exprs.
        match &expr.node {
            ast::ExprKind::Lambda { .. }
            | ast::ExprKind::Do(_)
            | ast::ExprKind::Case { .. }
            | ast::ExprKind::If { .. } => None,
            _ => Some(expr.span),
        }
    }
    // Only fire when the user has an explicit selection — wrapping every
    // expression at a bare cursor position would produce too many actions.
    if range_start == range_end {
        return None;
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                if let Some(span) = walk(body, range_start, range_end) {
                    let text = source.get(span.start..span.end)?;
                    let body = if is_atomic_expr_text(text) {
                        text.to_string()
                    } else {
                        format!("({text})")
                    };
                    return Some((span, format!("Err {{error {body}}}")));
                }
            }
            _ => {}
        }
    }
    None
}

/// Strip surrounding whitespace and any balanced enclosing parentheses from the
/// byte range `[lo, hi)`, returning the trimmed "core" range. Used to compare a
/// selection against an expression node's span up to redundant parens: the
/// parser folds a parenthesized expression's parens into the node's span (so
/// `(x * 2)` and `x * 2` are the same node — see `parser.rs`'s `LParen` atom
/// arm), and the user may or may not include those parens in the selection.
fn strip_ws_and_enclosing_parens(source: &str, mut lo: usize, mut hi: usize) -> (usize, usize) {
    let bytes = source.as_bytes();
    hi = hi.min(bytes.len());
    lo = lo.min(hi);
    loop {
        while lo < hi && bytes[lo].is_ascii_whitespace() {
            lo += 1;
        }
        while hi > lo && bytes[hi - 1].is_ascii_whitespace() {
            hi -= 1;
        }
        if hi - lo < 2 || bytes[lo] != b'(' || bytes[hi - 1] != b')' {
            break;
        }
        // Only strip when the leading `(` is closed by the trailing `)` — not
        // when they belong to different groups, as in `(a) + (b)` (whose top
        // node span also starts with `(` and ends with `)`).
        let mut depth = 0i32;
        let mut wraps = false;
        for (i, &b) in bytes[lo..hi].iter().enumerate() {
            match b {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        wraps = lo + i == hi - 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !wraps {
            break;
        }
        lo += 1;
        hi -= 1;
    }
    (lo, hi)
}

/// True when the selection `[lo, hi)` denotes a whole expression node (up to
/// redundant surrounding parentheses/whitespace). Gates the extract-to-let /
/// extract-to-function refactors: those replace the selection with a variable
/// or call, which only preserves program meaning when the selection is a
/// complete sub-expression. A fragment straddling operator-precedence
/// boundaries matches no node and is rejected.
fn selection_matches_expr_node(module: &Module, source: &str, lo: usize, hi: usize) -> bool {
    let target = strip_ws_and_enclosing_parens(source, lo, hi);
    // A whitespace-only or empty selection has no meaningful core.
    if target.0 >= target.1 {
        return false;
    }
    fn walk(expr: &ast::Expr, source: &str, target: (usize, usize)) -> bool {
        // Prune: only a node whose span covers the (stripped) selection can
        // match it or contain a descendant that does.
        if expr.span.start > target.0 || target.1 > expr.span.end {
            return false;
        }
        if strip_ws_and_enclosing_parens(source, expr.span.start, expr.span.end) == target {
            return true;
        }
        let mut found = false;
        crate::utils::recurse_expr(expr, |child| {
            found = found || walk(child, source, target);
        });
        found
    }
    module.decls.iter().any(|decl| match &decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } => walk(body, source, target),
        _ => false,
    })
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
        // A wildcard (`_`) or bare-variable arm already catches every
        // remaining constructor — the case is exhaustive, and arms inserted
        // after the catch-all would be unreachable dead code. Suppress the
        // fill action entirely (recursion into sub-expressions still runs).
        let has_catch_all_arm = arms.iter().any(|arm| {
            matches!(
                &arm.pat.node,
                ast::PatKind::Wildcard | ast::PatKind::Var(_)
            )
        });
        // Try to find the ADT type of the scrutinee. Resolve the scrutinee
        // expression's *own span* against the local-type table (innermost
        // containing span, via the deterministic sorted vec) rather than
        // text-matching binding spans against the variable name — text
        // matching is scope-blind and HashMap-iteration-order
        // nondeterministic when several bindings share the name.
        let scrutinee_type = match &scrutinee.node {
            ast::ExprKind::Var(name) => {
                let offset = scrutinee.span.start;
                doc.local_type_info_sorted
                    .iter()
                    .filter(|(span, _)| span.start <= offset && offset < span.end)
                    .min_by_key(|(span, _)| span.end - span.start)
                    .map(|(_, ty)| ty.clone())
                    .or_else(|| {
                        // Use-site → definition-site lookup via references.
                        doc.references
                            .iter()
                            .find(|(usage, _)| usage.start <= offset && offset < usage.end)
                            .and_then(|(_, def_span)| {
                                doc.local_type_info.get(def_span).cloned()
                            })
                    })
                    .or_else(|| doc.type_info.get(name).cloned())
            }
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

                        if has_catch_all_arm || missing.is_empty() {
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

                        // Insert after the last arm's body rather than at
                        // `expr.span.end`. A parenthesized case
                        // (`show (case c of Red {} -> 1)`) has its enclosing
                        // parens folded into the case node's span by the
                        // parser, so `expr.span.end` points past the closing
                        // `)`; the last arm's body always ends inside the
                        // parens. Falls back to `expr.span.end` only for the
                        // (parser-impossible) empty-arms case.
                        let insert_offset = arms
                            .last()
                            .map(|a| a.body.span.end)
                            .unwrap_or(expr.span.end);
                        let insert_pos = offset_to_position(&doc.source, insert_offset);
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

    // Recurse into every sub-expression via the canonical traversal, so a
    // `case` nested under any expression kind (record/list/binop/field access/
    // serve handler/groupBy key/…) is still found — a hand-written match here
    // silently dropped those parents.
    crate::utils::recurse_expr(expr, |e| {
        find_case_actions(e, doc, uri, range_start, range_end, actions);
    });
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
    let after_colon_off = colon + 2;
    // Bound the search to the SIGNATURE text: the signature ends where the
    // next column-0 line begins (the body line `name = …`). Continuation
    // lines of a multi-line signature are indented and stay included.
    let sig_end = {
        let bytes = decl_text.as_bytes();
        let mut end = decl_text.len();
        let mut i = after_colon_off;
        while i < bytes.len() {
            if bytes[i] == b'\n'
                && bytes
                    .get(i + 1)
                    .is_none_or(|b| *b != b' ' && *b != b'\t' && *b != b'\r')
            {
                end = i;
                break;
            }
            i += 1;
        }
        end
    };
    let sig = &decl_text[after_colon_off..sig_end];
    // The declared row to widen is the RESULT row — the outermost top-level
    // `IO { … }` along the arrow spine. Taking the first textual `IO {`
    // would mutate a callback parameter's row in signatures like
    // `(Int -> IO {} {}) -> IO {} {}`. `find_outermost_io_row` already
    // implements the depth-aware spine walk.
    let (row_start, row_end) = crate::shared::find_outermost_io_row(sig)?;
    // Replace the row INCLUDING braces (target_effects carries its own).
    let abs_open = decl.span.start + after_colon_off + row_start - 1;
    let abs_close = decl.span.start + after_colon_off + row_end + 1;
    Some(TextEdit {
        range: Range {
            start: offset_to_position(source, abs_open),
            end: offset_to_position(source, abs_close),
        },
        new_text: target_effects.to_string(),
    })
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
    if let Some(inner) = expected.strip_prefix("Maybe ")
        && inner.trim() == found.trim() {
            out.push(WrapSuggestion {
                title: "Wrap in `Just`".to_string(),
                template: format!("Just {{value {WRAP_PLACEHOLDER}}}"),
            });
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
                    template: format!("Ok {{value {WRAP_PLACEHOLDER}}}"),
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

/// Compute the indentation prefix for a new case arm, matching the existing arms
/// or falling back to a default indent relative to the case expression.
/// Newline + indentation for synthesized case arms when rewriting the
/// expression starting at `span_start` into a `case`. Arms are indented two
/// columns past the rewritten expression's own column, which keeps them
/// strictly deeper than the enclosing layout context (do-block statements,
/// let bindings) so the layout-sensitive parser accepts the result.
fn indent_for_expr_start(source: &str, span_start: usize) -> String {
    let line_start = source[..span_start.min(source.len())]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    // Count CHARACTERS, not bytes — a byte count over-indents (and can corrupt
    // the layout-sensitive parse) when multibyte text precedes on the line.
    let col = source[line_start..span_start.min(source.len())].chars().count();
    format!("\n{}", " ".repeat(col + 2))
}

fn arm_indentation(case_expr: &ast::Expr, arms: &[ast::CaseArm], source: &str) -> String {
    // Prefer the column of an existing arm — the layout block's indent is
    // fixed at the first arm's column even when that arm sits inline on the
    // `of` line (`case x of A {} -> 1`), so new arms must land at the SAME
    // column, not at case-column+2 (which would be shallower than the block
    // indent and fail to parse).
    if let Some(arm) = arms.first() {
        let line_start = source[..arm.pat.span.start]
            .rfind('\n')
            .map(|p| p + 1)
            .unwrap_or(0);
        let prefix = &source[line_start..arm.pat.span.start];
        if prefix.chars().all(char::is_whitespace) {
            return format!("\n{prefix}");
        }
        // Inline arm: non-whitespace precedes it, so synthesize the column
        // with spaces. Count CHARACTERS, not bytes (multibyte text on the line
        // would otherwise over-indent and break the layout-sensitive parse).
        let col = source[line_start..arm.pat.span.start].chars().count();
        return format!("\n{}", " ".repeat(col));
    }
    // No arms at all: fall back to the case expression's column + 2.
    let line_start = source[..case_expr.span.start]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    let case_col = source[line_start..case_expr.span.start].chars().count();
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
/// collide with anything visible to this declaration. Considers top-level
/// declarations, every reference span in this file, and the built-in
/// prelude names.
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
    for (usage_span, def_span) in &doc.references {
        if usage_span.start >= start && usage_span.end <= end {
            let name = safe_slice(&doc.source, *usage_span);
            // Only include if it looks like a lowercase variable (not a constructor/type)
            if !name.is_empty()
                && name.chars().next().is_some_and(|c| c.is_lowercase())
                && !seen.contains(name)
            {
                // A captured free variable is one bound by a LOCAL binder
                // sitting BEFORE the selection (not a top-level definition,
                // and not bound within the selection itself). Detect locals
                // two ways so a binder missing from `local_type_info` (or whose
                // binding-span text isn't the bare name) isn't silently
                // dropped — which would generate a helper missing a parameter
                // and a call site that omits it, producing uncompilable code:
                //   (a) an entry in `local_type_info` before the selection, or
                //   (b) a reference whose definition span sits before the
                //       selection and is not this name's top-level definition.
                let local_via_type_info = doc.local_type_info.keys().any(|span| {
                    span.start < start && safe_slice(&doc.source, *span) == name
                });
                let resolves_to_top_level = doc
                    .definitions
                    .get(name)
                    .is_some_and(|s| *s == *def_span);
                let local_via_ref = def_span.end <= start && !resolves_to_top_level;
                if local_via_type_info || local_via_ref {
                    seen.insert(name.to_string());
                    free_vars.push(name.to_string());
                }
            }
        }
    }

    free_vars
}

/// Conservative syntactic check: is `s` an ATOMIC expression that can be
/// spliced into any operand position without parentheses? Accepts single
/// identifiers, numeric literals, simple string literals, and fully-wrapped
/// `( … )` / `{ … }` / `[ … ]` forms whose outer delimiters actually match.
/// Anything else (operators, applications, field accesses with arguments…)
/// reports `false` so the caller parenthesizes — over-parenthesizing is
/// safe, under-parenthesizing changes semantics.
fn is_atomic_expr_text(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let bytes = t.as_bytes();
    // Identifier or field-access chain (`x`, `a.b.c`, incl. primes) — a chain
    // of `.name` segments never rebinds into an enclosing operator, so it can
    // be spliced bare. A bare `a.b` is NOT an application; treating it as
    // non-atomic would pointlessly parenthesize record projections.
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
    if bytes[0].is_ascii_alphabetic() || bytes[0] == b'_' {
        let mut chain_ok = true;
        for seg in t.split('.') {
            let sb = seg.as_bytes();
            if sb.is_empty()
                || !(sb[0].is_ascii_alphabetic() || sb[0] == b'_')
                || !seg.bytes().all(is_ident)
            {
                chain_ok = false;
                break;
            }
        }
        if chain_ok {
            return true;
        }
    }
    // Numeric literal: digits with optional `.`, `_` separators, and a
    // trailing `<Unit>` annotation (`42.0 M`).
    if bytes[0].is_ascii_digit() {
        let (num, unit) = match t.find('<') {
            Some(p) if t.ends_with('>') => (&t[..p], Some(&t[p + 1..t.len() - 1])),
            _ => (t, None),
        };
        let num_ok = num
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'.' || b == b'_');
        let unit_ok = unit.is_none_or(|u| {
            !u.is_empty() && u.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        });
        if num_ok && unit_ok {
            return true;
        }
    }
    // Simple string literal with no internal quotes.
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') && !t[1..t.len() - 1].contains('"') {
        return true;
    }
    // Fully-delimited: the opening bracket must match the LAST closing one.
    let pairs: &[(u8, u8)] = &[(b'(', b')'), (b'{', b'}'), (b'[', b']')];
    for (open, close) in pairs {
        if bytes[0] == *open && bytes[bytes.len() - 1] == *close {
            let mut depth = 0i32;
            for (i, b) in bytes.iter().enumerate() {
                if *b == *open {
                    depth += 1;
                } else if *b == *close {
                    depth -= 1;
                    if depth == 0 && i != bytes.len() - 1 {
                        return false; // `(a) (b)` — outer delimiters don't wrap
                    }
                }
            }
            return depth == 0;
        }
    }
    false
}

/// Find inline-variable opportunities: the cursor sits on the bound field
/// NAME of a `with {name: value} body` expression. The offered action
/// substitutes the value at every usage of `name` inside `body` and unwraps
/// the `with`, replacing the whole `with {name: value} body` span with the
/// rewritten body.
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

    if let ast::ExprKind::With { record, body } = &expr.node
        && let ast::ExprKind::Record(fields) = &record.node
        && fields.len() == 1
    {
        let field = &fields[0];
        // `Field` carries no name span, so locate the field-name token
        // inside the record: the name must sit between the record's `{` and
        // the field value's span. A punned pattern shadowing the same name
        // elsewhere in the record can't occur in a single-field record.
        let name_span = crate::utils::find_word_in_source(
            &doc.source,
            &field.name,
            record.span.start,
            field.value.span.start,
        );
        if let Some(name_span) = name_span
            && name_span.start <= cursor_offset
            && cursor_offset <= name_span.end
        {
            let value_text = safe_slice(&doc.source, field.value.span);
            // Find the body's *inner* span: the parser folds wrapping parens
            // (`(do …)`) into the expr's span, but the with's own parens
            // belong to the `with` syntax — replacing the whole with span
            // with `body`'s span text would keep them (`(do …)`), while we
            // want the bare body (`do …`). Strip one paren layer when the
            // body text is a fully-parenthesized wrapper.
            let body_text_raw = safe_slice(&doc.source, body.span);
            let (body_start, body_end) = if body_text_raw.starts_with('(')
                && body_text_raw.ends_with(')')
            {
                (body.span.start + 1, body.span.end - 1)
            } else {
                (body.span.start, body.span.end)
            };
            let body_text = &doc.source[body_start..body_end];

            // Collect usages of the bound name inside the body. The old
            // let-form matched `def_span == pat.span`; `with` fields have no
            // binder span registered in `doc.references`, so fall back to a
            // whole-word text scan for `name` inside the body's inner span.
            let mut usage_spans: Vec<Span> = Vec::new();
            let mut from = body_start;
            while let Some(sp) =
                crate::utils::find_word_in_source(&doc.source, &field.name, from, body_end)
            {
                // Skip the field name itself if it re-occurs (it can't — the
                // record sits before the body — but the scan is cheap).
                usage_spans.push(sp);
                from = sp.end;
            }

            if !usage_spans.is_empty() {
                // Parenthesize unless the value is syntactically atomic — a
                // whitespace check misses operators without spaces (`n-1`
                // inlined into `2 * y` becomes `2 * n-1`, changing
                // semantics). Over-parenthesizing is harmless; under is not.
                let replacement = if is_atomic_expr_text(value_text) {
                    value_text.to_string()
                } else {
                    format!("({value_text})")
                };

                // Build the new body text by splicing the value at each
                // usage (back-to-front keeps offsets valid).
                let mut new_body = body_text.to_string();
                for sp in usage_spans.iter().rev() {
                    new_body.replace_range(
                        (sp.start - body_start)..(sp.end - body_start),
                        &replacement,
                    );
                }

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: offset_to_position(&doc.source, expr.span.start),
                            end: offset_to_position(&doc.source, expr.span.end),
                        },
                        new_text: new_body,
                    }],
                );

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Inline `{}`", field.name),
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

    // Recurse into every child via the canonical traversal so a `with`
    // nested under any expression kind (record/list/binop/field access/
    // set value/serve handler/…) is still offered. `With` itself is
    // handled above; its record/body children are revisited by the
    // traversal (nested `with` inside the value or body still works).
    crate::utils::recurse_expr(expr, |e| {
        find_inline_actions(e, doc, uri, cursor_offset, actions);
    });
}

pub(crate) fn enclosing_do_stmt_range(
    module: &Module,
    sel_start: usize,
    sel_end: usize,
) -> Option<(usize, usize)> {
    fn walk(
        expr: &ast::Expr,
        sel_start: usize,
        sel_end: usize,
        best: &mut Option<(usize, usize)>,
    ) {
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                if stmt.span.start <= sel_start && sel_end <= stmt.span.end {
                    // Recursion visits parents before children, so the last
                    // assignment wins — the innermost matching statement.
                    let block_end = stmts.last().map(|s| s.span.end).unwrap_or(stmt.span.end);
                    *best = Some((stmt.span.start, block_end));
                }
            }
        }
        crate::utils::recurse_expr(expr, |e| walk(e, sel_start, sel_end, best));
    }
    let mut best = None;
    for decl in &module.decls {
        if decl.span.start > sel_start || sel_end > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, sel_start, sel_end, &mut best),
            _ => {}
        }
    }
    best
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
        if expr.span.start > offset || offset >= expr.span.end {
            return;
        }
        if let ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } = &expr.node
        {
            let size = expr.span.end - expr.span.start;
            if best.as_ref().is_none_or(|b| size < b.0.end - b.0.start) {
                let cond_text = safe_slice(source, cond.span);
                let then_text = safe_slice(source, then_branch.span);
                let else_text = safe_slice(source, else_branch.span);
                // Multi-line branch/condition text carries indentation tied
                // to its original column; splicing it after a case arm's
                // `->` at a new column breaks the layout-sensitive parse.
                // Same guard as `find_if_negate_at`. (Recursion below may
                // still find a single-line inner `if`.)
                let multi_line = cond_text.contains('\n')
                    || then_text.contains('\n')
                    || else_text.contains('\n');
                if !multi_line {
                    let indent = indent_for_expr_start(source, expr.span.start);
                    let replacement = format!(
                        "case {cond_text} of{indent}True {{}} -> {then_text}{indent}False {{}} -> {else_text}"
                    );
                    // A parenthesized `if` in operand position folds its parens
                    // into `expr.span` (parse_atom keeps the bare node), so
                    // replacing the whole `(if …)` span with a bare `case …`
                    // would let a trailing operator bind into the last arm —
                    // `(if c then a else b) * 2` → `case … False {} -> b * 2`.
                    // Re-wrap in parens; inside delimiters the `)` terminates
                    // the arm block, so the multi-line layout still parses.
                    let expr_text = safe_slice(source, expr.span);
                    let replacement = if is_already_parenthesized(expr_text) {
                        format!("({replacement})")
                    } else {
                        replacement
                    };
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
        if expr.span.start > offset || offset >= expr.span.end {
            return;
        }
        if let ast::ExprKind::BinOp { op, lhs, rhs } = &expr.node {
            let op_str = match op {
                ast::BinOp::Add => Some("+"),
                ast::BinOp::Mul => Some("*"),
                ast::BinOp::Eq => Some("=="),
                ast::BinOp::Neq => Some("!="),
                // `&&`/`||` are intentionally excluded: they short-circuit, so
                // the right operand is only evaluated when the left permits it
                // (e.g. `n != 0 && x % n == 0` guards a division). Flipping the
                // operands would evaluate the formerly-guarded side first and
                // can panic, so this rewrite is not semantics-preserving.
                _ => None,
            };
            if let Some(op_text) = op_str {
                // Replace only the operator's own extent (`lhs … rhs`), never
                // the enclosing parens. The parser folds surrounding `(` `)`
                // into a binary expression's own span, so replacing
                // `expr.span` would strip parens that hold the expression
                // together in its context: `f (a == b)` must stay
                // `f (b == a)`, not become `f b == a` (which reparses as
                // `(f b) == a`).
                let flip_span = Span::new(lhs.span.start, rhs.span.end);
                let size = flip_span.end - flip_span.start;
                if best.as_ref().is_none_or(|b| size < b.0.end - b.0.start) {
                    // A moved operand keeps its own source text but lands in a
                    // new neighbor context that can re-parse it. Keyword forms
                    // (if/case/lambda/do) greedily consume everything to their
                    // right (`false && if … else false` flips to
                    // `if … else false && false`). Binary/unary operands
                    // re-associate with the flip operator when they share or
                    // undercut its precedence: `a / b * c` is `(a / b) * c`,
                    // and a naive flip to `c * a / b` reparses as `(c * a) / b`
                    // — a different value under integer division. Parenthesize
                    // both families unless the source already wraps them;
                    // over-parenthesizing is harmless.
                    let operand_text = |e: &ast::Expr| -> String {
                        let text = safe_slice(source, e.span);
                        let needs_parens = matches!(
                            &e.node,
                            ast::ExprKind::If { .. }
                                | ast::ExprKind::Case { .. }
                                | ast::ExprKind::Lambda { .. }
                                | ast::ExprKind::Do(_)
                                | ast::ExprKind::BinOp { .. }
                                | ast::ExprKind::UnaryOp { .. }
                        );
                        if needs_parens && !is_already_parenthesized(text) {
                            format!("({text})")
                        } else {
                            text.to_string()
                        }
                    };
                    let replacement =
                        format!("{} {op_text} {}", operand_text(rhs), operand_text(lhs));
                    *best = Some((flip_span, replacement));
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
        is_app_head: bool,
        needs_parens: bool,
    ) {
        if expr.span.start > offset || offset >= expr.span.end {
            return;
        }
        if let ast::ExprKind::App { func, arg } = &expr.node {
            // Only convert applications whose function is a simple identifier
            // (`f x` rather than `(g h) x`). Piping a curried partial
            // application reads strangely.
            //
            // `is_app_head` guards the multi-arg case: the inner `App(f, x)`
            // of `f x y` is the head of the enclosing application, and
            // rewriting it to `x |> f` would produce `x |> f y`, which
            // parses as `f y x` — arguments silently swapped. Only offer
            // the action when the whole chain has exactly one argument.
            let is_simple = matches!(
                &func.node,
                ast::ExprKind::Var(_) | ast::ExprKind::Constructor(_)
            );
            if is_simple && !is_app_head {
                let size = expr.span.end - expr.span.start;
                if best.as_ref().is_none_or(|b| size < b.0.end - b.0.start) {
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
                    // `|>` has the lowest precedence, so a bare pipe
                    // re-associates whenever it lands in a position that binds
                    // tighter than it. Two such positions need parens:
                    //   - an operand of a Bin/UnaryOp: `1 + double x` →
                    //     `1 + x |> double` parses as `(1 + x) |> double`.
                    //   - the argument of an enclosing application: the source
                    //     `g (f x)` gives the inner `App` a span that swallows
                    //     the parens, so replacing it with `x |> f` yields
                    //     `g x |> f`, i.e. `f (g x)` — application order
                    //     silently reversed.
                    // Parenthesize the pipe in either case.
                    let replacement = if needs_parens {
                        format!("({arg_part} |> {func_text})")
                    } else {
                        format!("{arg_part} |> {func_text}")
                    };
                    *best = Some((expr.span, replacement));
                }
            }
            // Recurse manually so the function side knows it is an
            // application head. The argument slot binds tighter than `|>`:
            // a complex arg is parenthesized in the source and the `App`
            // span covers those parens, so a pipe spliced there must be
            // re-parenthesized to preserve application order.
            walk(func, source, offset, best, true, false);
            walk(arg, source, offset, best, false, true);
            return;
        }
        // Operator operands need the flag so a converted App inside them is
        // parenthesized; everything else resets it (their children sit in
        // delimited or otherwise pipe-safe positions where a bare pipe does
        // not re-associate).
        match &expr.node {
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                walk(lhs, source, offset, best, false, true);
                walk(rhs, source, offset, best, false, true);
            }
            ast::ExprKind::UnaryOp { operand, .. } => {
                walk(operand, source, offset, best, false, true);
            }
            _ => {
                crate::utils::recurse_expr(expr, |e| {
                    walk(e, source, offset, best, false, false)
                });
            }
        }
    }
    let mut best = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best, false, false),
            _ => {}
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_module(src: &str) -> Module {
        let (tokens, lex_diags) = knot::lexer::Lexer::new(src).tokenize();
        assert!(lex_diags.is_empty(), "lex errors in test source: {lex_diags:?}");
        let (module, parse_diags) = knot::parser::Parser::new(src.to_string(), tokens).parse_module();
        assert!(parse_diags.is_empty(), "parse errors in test source: {parse_diags:?}");
        module
    }

    fn parses_cleanly(src: &str) -> bool {
        let (tokens, lex_diags) = knot::lexer::Lexer::new(src).tokenize();
        let (_, parse_diags) = knot::parser::Parser::new(src.to_string(), tokens).parse_module();
        lex_diags.is_empty() && parse_diags.is_empty()
    }

    #[test]
    fn pipe_conversion_not_offered_inside_multi_arg_application() {
        // Regression: with the cursor inside `f x` of `f x y`, the action used
        // to rewrite the inner application to `x |> f`, producing `x |> f y`
        // which parses as `f y x` — arguments silently swapped.
        let src = "g = \\f x y -> f x y\n";
        let module = parse_module(src);
        let off = src.find("f x y").expect("application") + 2; // on `x`
        assert!(
            find_pipe_conversion_at(&module, src, off).is_none(),
            "no pipe action may be offered anywhere in a multi-arg application"
        );
    }

    #[test]
    fn pipe_conversion_still_offered_for_single_arg_application() {
        let src = "h = \\x -> show x\n";
        let module = parse_module(src);
        let off = src.find("show x").expect("application") + 1;
        let (_, replacement) =
            find_pipe_conversion_at(&module, src, off).expect("single-arg app offers pipe");
        assert_eq!(replacement, "x |> show");
    }

    #[test]
    fn if_to_case_inside_do_block_keeps_layout_parseable() {
        // Regression: arms were emitted at a hard-coded 2-space indent, which
        // collided with the do-block statement column and failed to reparse.
        let src = "main = do\n  x <- *items\n  with {y (if x > 1 then 1 else 2)} (do\n    yield y)\n";
        let module = parse_module(src);
        let off = src.find("if x").expect("if expr");
        let (span, replacement) =
            find_if_to_case_at(&module, src, off).expect("if-to-case offered");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(
            parses_cleanly(&out),
            "if-to-case rewrite must reparse cleanly; got:\n{out}"
        );
    }

    /// Bug B66: a parenthesized `if` in operand position folds its parens into
    /// the expr span. Replacing the whole `(if …)` span with a bare `case …`
    /// dropped the parens, so the trailing `* 2` bound into the last arm
    /// (`False {} -> b * 2`). The rewrite must re-wrap in parens.
    #[test]
    fn if_to_case_reparenthesizes_operand_position() {
        let src = "f = \\c a b -> (if c then a else b) * 2\n";
        let module = parse_module(src);
        let off = src.find("if c").expect("if expr");
        let (span, replacement) =
            find_if_to_case_at(&module, src, off).expect("if-to-case offered");
        assert!(
            replacement.starts_with('(') && replacement.ends_with(')'),
            "parenthesized-if rewrite must stay parenthesized; got: {replacement}"
        );
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(
            out.contains(") * 2"),
            "trailing operand must stay outside the case; got:\n{out}"
        );
        assert!(
            parses_cleanly(&out),
            "reparenthesized if-to-case rewrite must reparse cleanly; got:\n{out}"
        );
    }

    #[test]
    fn if_negate_not_offered_for_multiline_branches() {
        // Swapping multi-line branches inline breaks layout-sensitive parses;
        // the action is suppressed instead.
        let src = "f = \\x -> if x > 1\n  then do\n    yield 1\n  else do\n    yield 2\n";
        if let Ok(module) = std::panic::catch_unwind(|| parse_module(src)) {
            let off = src.find("if x").expect("if expr");
            assert!(
                find_if_negate_at(&module, src, off).is_none(),
                "negate action must not be offered for multi-line branches"
            );
        }
    }

    fn plain_params(uri: &Uri, range: Range) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    fn selection_range(source: &str, needle: &str) -> Range {
        let off = source.find(needle).expect("needle found");
        Range {
            start: crate::utils::offset_to_position(source, off),
            end: crate::utils::offset_to_position(source, off + needle.len()),
        }
    }

    fn action_titles(actions: &[CodeActionOrCommand]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                CodeActionOrCommand::CodeAction(ca) => Some(ca.title.clone()),
                _ => None,
            })
            .collect()
    }

    /// Regression: statement-form `with {x: e} (do …)` only parses inside
    /// `do` blocks. The action used to be offered everywhere, producing
    /// parse errors when applied in plain expression bodies.
    #[test]
    fn extract_to_with_not_offered_outside_do_block() {
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\x -> x * 2 + 1\n");
        let doc_source = ws.doc(&uri).source.clone();
        let range = selection_range(&doc_source, "x * 2");
        let actions = handle_code_action(&ws.state, &plain_params(&uri, range))
            .expect("code action response");
        let titles = action_titles(&actions);
        assert!(
            titles.iter().all(|t| !t.starts_with("Extract to with")),
            "with-extraction must not be offered outside a do block; got: {titles:?}"
        );
        // The function-extraction variant works in expression position and
        // should still be offered.
        assert!(
            titles.iter().any(|t| t.starts_with("Extract to function")),
            "function extraction should remain available; got: {titles:?}"
        );
    }

    /// A client may send a code-action request whose range is inverted
    /// (start position after end). The handler must normalize it instead of
    /// panicking while slicing `doc.source[range_start..range_end]`.
    #[test]
    fn inverted_range_does_not_panic() {
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\x -> x * 2 + 1\n");
        let doc_source = ws.doc(&uri).source.clone();
        let normal = selection_range(&doc_source, "x * 2");
        // Invert: start and end swapped so start > end.
        let inverted = Range {
            start: normal.end,
            end: normal.start,
        };
        // Must not panic (used to panic in the extract-variable slice).
        let _ = handle_code_action(&ws.state, &plain_params(&uri, inverted));
    }

    #[test]
    fn extract_to_with_offered_inside_do_block() {
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = do\n  x <- [1, 2]\n  yield (x * 2)\n");
        let doc_source = ws.doc(&uri).source.clone();
        let range = selection_range(&doc_source, "x * 2");
        let actions = handle_code_action(&ws.state, &plain_params(&uri, range))
            .expect("code action response");
        let titles = action_titles(&actions);
        assert!(
            titles.iter().any(|t| t.starts_with("Extract to with")),
            "with-extraction should be offered inside a do block; got: {titles:?}"
        );
    }

    /// Regression: the `with` binding used to be inserted before the
    /// *cursor's* line, splitting multi-line do-statements mid-expression.
    /// It must be inserted before the START of the enclosing statement.
    #[test]
    fn extract_to_with_inserts_before_enclosing_do_stmt() {
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        // The `yield` statement starts on line 2 and continues onto line 3.
        let source = "main = do\n  x <- [1, 2]\n  yield (x +\n    (x * 2))\n";
        let uri = ws.open("main", source);
        let doc_source = ws.doc(&uri).source.clone();
        // Selection sits on the continuation line (line 3).
        let range = selection_range(&doc_source, "(x * 2)");
        let actions = handle_code_action(&ws.state, &plain_params(&uri, range))
            .expect("code action response");
        let with_action = actions
            .iter()
            .find_map(|a| match a {
                CodeActionOrCommand::CodeAction(ca)
                    if ca.title.starts_with("Extract to with") =>
                {
                    Some(ca)
                }
                _ => None,
            })
            .expect("with extraction offered inside do block");
        let edits = with_action
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&uri)
            .unwrap();
        let insert = edits
            .iter()
            .find(|e| e.new_text.contains("with {"))
            .expect("with insertion edit");
        assert_eq!(
            insert.range.start.line, 2,
            "must insert before the `yield` statement's line, not the \
             continuation line; edits: {edits:?}"
        );
        assert_eq!(insert.range.start.character, 0);
        assert!(
            insert.new_text.starts_with("  with {"),
            "indent must match the statement's line; got: {:?}",
            insert.new_text
        );
    }

    /// Staleness guard: when the editor holds newer (pending) text than the
    /// analyzed doc, span-derived edits would corrupt the buffer. The
    /// handler must bail.
    #[test]
    fn code_action_bails_when_pending_text_is_newer() {
        use crate::state::PendingSource;
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = do\n  x <- [1, 2]\n  yield (x * 2)\n");
        let doc_source = ws.doc(&uri).source.clone();
        let range = selection_range(&doc_source, "x * 2");
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: format!("-- edited\n{doc_source}"),
                version: Some(2),
            },
        );
        let actions = handle_code_action(&ws.state, &plain_params(&uri, range));
        assert!(
            actions.is_none(),
            "code actions against stale analysis must bail; got: {actions:?}"
        );
    }
}

// Regression tests for the 2026-06 LSP bug-fix batch (code-action group).
// Kept in a separate module from `tests` above so the original test file
// content stays untouched.
#[cfg(test)]
mod regress_fixes_tests {
    use super::*;
    use crate::test_support::{TempWorkspace, TestWorkspace};

    fn params_for(uri: &Uri, range: Range) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    fn action_titled(
        actions: &[CodeActionOrCommand],
        pred: impl Fn(&str) -> bool,
    ) -> Option<&CodeAction> {
        actions.iter().find_map(|a| match a {
            CodeActionOrCommand::CodeAction(ca) if pred(&ca.title) => Some(ca),
            _ => None,
        })
    }

    fn edits_for<'a>(action: &'a CodeAction, uri: &Uri) -> &'a [TextEdit] {
        action
            .edit
            .as_ref()
            .and_then(|e| e.changes.as_ref())
            .and_then(|c| c.get(uri))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Item 1: "Add type annotation" for functions must produce the inline
    /// form `name : Sig = body`, not a standalone signature line.
    #[test]
    fn add_type_annotation_is_inline_on_definition() {
        let mut tw = TestWorkspace::new();
        let src = "double = \\x -> x * 2\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "double");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Add type annotation"))
            .expect("annotation action offered");
        let edits = edits_for(action, &uri);
        assert_eq!(edits.len(), 1);
        let edit = &edits[0];
        // Inserted inline before the `=`, like the View/Derived branch.
        assert!(
            edit.new_text.starts_with(": ") && edit.new_text.ends_with(' '),
            "expected inline `: Sig ` insertion, got {:?}",
            edit.new_text
        );
        assert!(
            !edit.new_text.contains('\n'),
            "annotation must not be a standalone line: {:?}",
            edit.new_text
        );
        // Insertion point is exactly at the `=`.
        let eq_col = src.find('=').unwrap() as u32;
        assert_eq!(edit.range.start.line, 0);
        assert_eq!(edit.range.start.character, eq_col);
        assert_eq!(edit.range.start, edit.range.end);
    }

    /// Item 5: "Wrap in Err" with a selection covering `x + 1` inside
    /// `\x -> x + 1` must wrap the whole BinOp, not just `x`.
    #[test]
    fn wrap_in_err_wraps_smallest_expr_containing_selection() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\x -> x + 1\n";
        let uri = tw.open("main", src);
        let start = tw.position_of(&uri, "x + 1");
        let end = tw.position_after(&uri, "x + 1");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start, end }),
        )
        .unwrap_or_default();
        let action =
            action_titled(&actions, |t| t == "Wrap in `Err`").expect("wrap action offered");
        let edits = edits_for(action, &uri);
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].new_text, "Err {error (x + 1)}");
    }

    /// Item 6: adding a wildcard arm to a single-line case must not emit the
    /// new arm at column 0 (which would terminate the layout block).
    #[test]
    fn add_wildcard_arm_single_line_case_keeps_indentation() {
        let mut tw = TestWorkspace::new();
        let src = "data Color = Red {} | Blue {}\n\nf = \\c -> case c of Red {} -> 1\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "case c of");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.contains("wildcard"))
            .expect("wildcard-arm action offered");
        let edits = edits_for(action, &uri);
        let new_text = &edits[0].new_text;
        let last_line = new_text.lines().last().unwrap_or("");
        assert!(
            last_line.starts_with(' '),
            "wildcard arm must be indented past column 0, got {new_text:?}"
        );
    }

    /// Apply `TextEdit`s to `source`, back-to-front so offsets stay valid.
    fn apply_edits_to(source: &str, edits: &[TextEdit]) -> String {
        let mut spans: Vec<(usize, usize, &str)> = edits
            .iter()
            .map(|e| {
                (
                    crate::utils::position_to_offset(source, e.range.start),
                    crate::utils::position_to_offset(source, e.range.end),
                    e.new_text.as_str(),
                )
            })
            .collect();
        spans.sort_by_key(|(s, _, _)| std::cmp::Reverse(*s));
        let mut out = source.to_string();
        for (start, end, text) in spans {
            out.replace_range(start..end, text);
        }
        out
    }

    fn selection(source: &str, needle: &str) -> Range {
        let off = source.find(needle).expect("needle found");
        Range {
            start: crate::utils::offset_to_position(source, off),
            end: crate::utils::offset_to_position(source, off + needle.len()),
        }
    }

    /// "Extract to function" must emit the lambda form for parameters —
    /// `helper = \x -> body` — since `helper x = body` doesn't parse at
    /// top level.
    #[test]
    fn extract_function_emits_lambda_form_for_free_vars() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\n -> n * 2 + 1\n";
        let uri = tw.open("main", src);
        let range = selection(src, "n * 2");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to function"))
            .expect("extract-to-function offered");
        let edits = edits_for(action, &uri);
        let helper = edits
            .iter()
            .find(|e| e.new_text.contains("= "))
            .expect("helper insertion edit");
        assert!(
            helper.new_text.contains("= \\n -> n * 2"),
            "helper must use the lambda form, got: {:?}",
            helper.new_text
        );
        // The whole result must round-trip through the parser cleanly.
        let out = apply_edits_to(src, edits);
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// Bug B72: extract must not fire on a selection that straddles
    /// operator-precedence boundaries. `2 * a + b` parses as `(2 * a) + b`, so
    /// `a + b` is not an expression node; extracting it would rewrite the value
    /// to `2 * (a + b)`. The action must be suppressed for such fragments.
    #[test]
    fn extract_not_offered_for_precedence_straddling_fragment() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\a b -> 2 * a + b\n";
        let uri = tw.open("main", src);
        let range = selection(src, "a + b");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        assert!(
            action_titled(&actions, |t| t.starts_with("Extract to function")).is_none(),
            "extract must not be offered for a non-node fragment `a + b`; \
             actions: {:?}",
            actions
                .iter()
                .filter_map(|a| match a {
                    CodeActionOrCommand::CodeAction(ca) => Some(ca.title.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
    }

    /// Companion to the B72 fragment check: a selection that DOES coincide with
    /// a whole expression node (`2 * a` inside `2 * a + b`) must still offer the
    /// extract, and the extracted result must round-trip through the parser.
    #[test]
    fn extract_offered_for_whole_expression_node() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\a b -> 2 * a + b\n";
        let uri = tw.open("main", src);
        let range = selection(src, "2 * a");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to function"))
            .expect("extract-to-function offered for a whole node");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// B67: "Extract to function" must parenthesize the call site when the
    /// helper takes arguments. Extracting the argument `(n + 2)` from
    /// `show (n + 2)` must yield `show (extracted_fn n)`, not
    /// `show extracted_fn n` — the latter parses as `(show extracted_fn) n`,
    /// the wrong application order.
    #[test]
    fn extract_function_parenthesizes_call_site_with_args() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\n -> show (n + 2)\n";
        let uri = tw.open("main", src);
        // Select the parenthesized argument, parens included — this is the
        // shape that used to drop the wrapping and misapply the call.
        let range = selection(src, "(n + 2)");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to function"))
            .expect("extract-to-function offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("show (extracted_fn n)"),
            "call site must be parenthesized as `(extracted_fn n)`; got:\n{out}"
        );
        assert!(
            !out.contains("show extracted_fn n"),
            "call site must not be a bare `show extracted_fn n`; got:\n{out}"
        );
        // The result must round-trip through the parser cleanly.
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// A full-line selection carries the line's trailing newline. The extracted
    /// helper is built from the TRIMMED text, so the replacement must cover only
    /// the trimmed span — replacing the raw selection deletes the newline and
    /// glues the call site onto the next declaration (`f = extracted_fng = 3`),
    /// which no longer parses.
    #[test]
    fn extract_function_preserves_trailing_newline_of_full_line_selection() {
        let mut tw = TestWorkspace::new();
        let src = "f = 1 + 2\ng = 3\n";
        let uri = tw.open("main", src);
        // Selection runs to the start of the next line, newline included.
        let range = selection(src, "1 + 2\n");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to function"))
            .expect("extract-to-function offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("f = extracted_fn\n"),
            "the selection's trailing newline must survive the replacement; got:\n{out}"
        );
        assert!(
            out.contains("\ng = 3"),
            "the following declaration must stay on its own line; got:\n{out}"
        );
        // The glued-together form is exactly what the raw-range replacement
        // produced, so pin it explicitly.
        assert!(
            !out.contains("extracted_fng"),
            "call site must not be glued to the next decl; got:\n{out}"
        );
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// Same trailing-newline hazard on the with path: eating the newline would
    /// pull the next do-statement onto the `with` line and break the layout.
    #[test]
    fn extract_to_with_preserves_trailing_newline_of_full_line_selection() {
        let mut tw = TestWorkspace::new();
        let src = "main = do\n  println (1 + 2)\n  println \"next\"\n";
        let uri = tw.open("main", src);
        let range = selection(src, "1 + 2");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to with"))
            .expect("extract-to-with offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("extracted"),
            "the selected expression must be bound to a name; got:\n{out}"
        );
        assert!(
            out.contains("println \"next\""),
            "the next do-statement must stay on its own line; got:\n{out}"
        );
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// A zero-arg extraction needs NO wrapping parens — the call is a single
    /// atom already, so `show (1 + 2)` → `show extracted_fn` is correct.
    #[test]
    fn extract_function_no_parens_for_zero_arg_call() {
        let mut tw = TestWorkspace::new();
        let src = "f = show (1 + 2)\n";
        let uri = tw.open("main", src);
        let range = selection(src, "(1 + 2)");
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to function"))
            .expect("extract-to-function offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("show extracted_fn\n"),
            "zero-arg call needs no wrapping parens; got:\n{out}"
        );
        // Still must parse cleanly.
        let lexer = knot::lexer::Lexer::new(&out);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(out.clone(), tokens);
        let (_, diags) = parser.parse_module();
        assert!(
            diags.iter().all(|d| !matches!(d.severity, knot::diagnostic::Severity::Error)),
            "extracted result must parse; got {diags:?}\nsource:\n{out}"
        );
    }

    /// "Add deriving" must attach to the data decl itself — decl spans
    /// include the trailing newline run, so inserting at span.end used to
    /// glue the clause onto the NEXT declaration.
    #[test]
    fn add_deriving_attaches_to_data_decl_not_next_decl() {
        let mut tw = TestWorkspace::new();
        let src = "data Color = Red {} | Blue {}\n\nnext = 1\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "Color");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Add `deriving"))
            .expect("add-deriving offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert_eq!(
            out, "data Color = Red {} | Blue {} deriving (Eq, Show)\n\nnext = 1\n",
            "deriving clause must end the data decl line"
        );
    }

    /// "Widen declared effects" must rewrite the RESULT row, not the first
    /// textual `IO {` (which can be a callback parameter's row).
    #[test]
    fn widen_effects_targets_result_row_not_callback_param() {
        let src = "runCb : (Int 1 -> IO {} {}) -> IO {} {}\nrunCb = \\cb -> cb 1\n";
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (module, _) = parser.parse_module();
        let decl = module
            .decls
            .iter()
            .find(|d| matches!(&d.node, DeclKind::Fun { name, .. } if name == "runCb"))
            .expect("runCb decl");
        let edit = build_effect_widen_edit(decl, src, "{console}")
            .expect("widen edit produced");
        let start = crate::utils::position_to_offset(src, edit.range.start);
        let result_row = src.find("-> IO {} {}\n").expect("result row") + 3 + 3;
        assert_eq!(
            start, result_row,
            "edit must target the result row's braces; got offset {start} \
             (text {:?})",
            &src[start..start + 4.min(src.len() - start)]
        );
        let end = crate::utils::position_to_offset(src, edit.range.end);
        let mut out = src.to_string();
        out.replace_range(start..end, &edit.new_text);
        assert_eq!(
            out,
            "runCb : (Int 1 -> IO {} {}) -> IO {console} {}\nrunCb = \\cb -> cb 1\n"
        );
    }

    /// "Inline variable" must parenthesize non-atomic values even without
    /// spaces — `with {y (n-1)}` into `2 * y` is `2 * (n-1)`, not `2 * n-1`.
    #[test]
    fn inline_variable_parenthesizes_operator_value_without_spaces() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\n -> with {y (n-1)} (2 * y)\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "y (");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Inline"))
            .expect("inline action offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("2 * (n-1)"),
            "inlined operator expression must be parenthesized:\n{out}"
        );
    }

    /// Atomic values (bare identifiers/literals) stay unparenthesized.
    #[test]
    fn inline_variable_leaves_atomic_values_bare() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\n -> with {y n} (2 * y)\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "y n");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Inline"))
            .expect("inline action offered");
        let edits = edits_for(action, &uri);
        let out = apply_edits_to(src, edits);
        assert!(
            out.contains("2 * n") && !out.contains("(n)"),
            "atomic value must inline without parens:\n{out}"
        );
    }

    #[test]
    fn is_atomic_expr_text_classification() {
        assert!(is_atomic_expr_text("n"));
        assert!(is_atomic_expr_text("x'"));
        assert!(is_atomic_expr_text("42"));
        assert!(is_atomic_expr_text("1_000.5"));
        assert!(is_atomic_expr_text("\"hello\""));
        assert!(is_atomic_expr_text("(a + b)"));
        assert!(is_atomic_expr_text("{a: 1, b: 2}"));
        assert!(is_atomic_expr_text("[1, 2]"));
        // Field-access chains are atomic — a bare `p.x` never rebinds into
        // an enclosing operator, so inlining it needs no parens.
        assert!(is_atomic_expr_text("p.x"));
        assert!(is_atomic_expr_text("a.b.c"));
        assert!(!is_atomic_expr_text("n-1"));
        assert!(!is_atomic_expr_text("n - 1"));
        assert!(!is_atomic_expr_text("f x"));
        assert!(!is_atomic_expr_text("(a) (b)"));
        assert!(!is_atomic_expr_text(""));
    }

    /// "Convert if to case" must refuse multi-line branches — splicing
    /// indented branch text at a new column breaks the layout parse.
    #[test]
    fn convert_if_to_case_refuses_multiline_branches() {
        let mut tw = TestWorkspace::new();
        let src = "f = \\x -> if x > 1\n  then do\n    yield 1\n  else do\n    yield 2\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "if x > 1");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        assert!(
            action_titled(&actions, |t| t.contains("if") && t.contains("case")).is_none(),
            "if-to-case must not be offered for multi-line branches"
        );
    }
}

#[cfg(test)]
mod regress_case_arm_tests {
    use super::*;
    use crate::test_support::TestWorkspace;

    fn params_at(uri: &Uri, pos: Position) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range: Range { start: pos, end: pos },
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    /// Item 7 (2026-06 batch 2): a `_` wildcard arm makes the case
    /// exhaustive — offering "Add missing case arms" would insert dead arms
    /// after the wildcard. The action must be suppressed.
    #[test]
    fn fill_case_arms_not_offered_when_wildcard_arm_exists() {
        let mut tw = TestWorkspace::new();
        let src = "data Color = Red {} | Blue {} | Green {}\n\npick : Color -> Int 1\npick = \\v -> case v of\n  Red {} -> 1\n  _ -> 0\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "case v of");
        let actions = handle_code_action(&tw.state, &params_at(&uri, pos)).unwrap_or_default();
        assert!(
            !actions.iter().any(|a| match a {
                CodeActionOrCommand::CodeAction(ca) =>
                    ca.title.starts_with("Add missing case arms"),
                _ => false,
            }),
            "fill-case-arms must not be offered when a wildcard arm exists"
        );
    }

    /// Same for a bare-variable catch-all arm (`other -> …`).
    #[test]
    fn fill_case_arms_not_offered_when_var_catch_all_exists() {
        let mut tw = TestWorkspace::new();
        let src = "data Color = Red {} | Blue {} | Green {}\n\npick : Color -> Int 1\npick = \\v -> case v of\n  Red {} -> 1\n  other -> 0\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "case v of");
        let actions = handle_code_action(&tw.state, &params_at(&uri, pos)).unwrap_or_default();
        assert!(
            !actions.iter().any(|a| match a {
                CodeActionOrCommand::CodeAction(ca) =>
                    ca.title.starts_with("Add missing case arms"),
                _ => false,
            }),
            "fill-case-arms must not be offered when a catch-all binder arm exists"
        );
    }

    /// Item 4: scrutinee type resolution must be span-based (innermost
    /// binding at the scrutinee), not text-matching across all bindings.
    /// Two same-named bindings with different types in different scopes
    /// must each resolve to their own type deterministically.
    #[test]
    fn fill_case_arms_resolves_scrutinee_by_span() {
        let mut tw = TestWorkspace::new();
        let src = "data Color = Red {} | Blue {} | Green {}\n\ndata Shape = Circle {} | Square {}\n\nuseShape : Shape -> Int 1\nuseShape = \\v -> 0\n\nother = \\v -> useShape v\n\npick : Color -> Int 1\npick = \\v -> case v of\n  Red {} -> 1\n";
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "case v of");
        let actions = handle_code_action(&tw.state, &params_at(&uri, pos)).unwrap_or_default();
        let fill = actions.iter().find_map(|a| match a {
            CodeActionOrCommand::CodeAction(ca)
                if ca.title.starts_with("Add missing case arms") =>
            {
                Some(ca)
            }
            _ => None,
        });
        let fill = fill.expect("fill-case-arms action offered");
        assert!(
            fill.title.contains("Blue") && fill.title.contains("Green"),
            "expected Color arms, got: {}",
            fill.title
        );
        assert!(
            !fill.title.contains("Circle"),
            "scrutinee resolved to the wrong same-named binding: {}",
            fill.title
        );
    }
}

// Regression tests for the 2026-06 LSP bug-fix batch 2 (code-action group).
// Kept in a separate module so the earlier regression files stay untouched.
#[cfg(test)]
mod regress_fixes_batch2_tests {
    use super::*;
    use crate::test_support::{TempWorkspace, TestWorkspace};

    fn parse_module(src: &str) -> Module {
        let (tokens, lex_diags) = knot::lexer::Lexer::new(src).tokenize();
        assert!(lex_diags.is_empty(), "lex errors in test source: {lex_diags:?}");
        let (module, parse_diags) =
            knot::parser::Parser::new(src.to_string(), tokens).parse_module();
        assert!(parse_diags.is_empty(), "parse errors in test source: {parse_diags:?}");
        module
    }

    fn parses_cleanly(src: &str) -> bool {
        let (tokens, lex_diags) = knot::lexer::Lexer::new(src).tokenize();
        let (_, parse_diags) =
            knot::parser::Parser::new(src.to_string(), tokens).parse_module();
        lex_diags.is_empty() && parse_diags.is_empty()
    }

    fn params_for(uri: &Uri, range: Range) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            context: CodeActionContext {
                diagnostics: Vec::new(),
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    fn action_titled(
        actions: &[CodeActionOrCommand],
        pred: impl Fn(&str) -> bool,
    ) -> Option<&CodeAction> {
        actions.iter().find_map(|a| match a {
            CodeActionOrCommand::CodeAction(ca) if pred(&ca.title) => Some(ca),
            _ => None,
        })
    }

    fn edits_for<'a>(action: &'a CodeAction, uri: &Uri) -> &'a [TextEdit] {
        action
            .edit
            .as_ref()
            .and_then(|e| e.changes.as_ref())
            .and_then(|c| c.get(uri))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Apply `TextEdit`s to `source`, back-to-front so offsets stay valid.
    fn apply_edits_to(source: &str, edits: &[TextEdit]) -> String {
        let mut spans: Vec<(usize, usize, &str)> = edits
            .iter()
            .map(|e| {
                (
                    crate::utils::position_to_offset(source, e.range.start),
                    crate::utils::position_to_offset(source, e.range.end),
                    e.new_text.as_str(),
                )
            })
            .collect();
        spans.sort_by_key(|(s, _, _)| std::cmp::Reverse(*s));
        let mut out = source.to_string();
        for (start, end, text) in spans {
            out.replace_range(start..end, text);
        }
        out
    }

    /// Bug 1: converting `double x` to pipe form under a binary operator must
    /// parenthesize the pipe — `1 + x |> double` parses as `(1 + x) |> double`
    /// because `|>` has the lowest precedence.
    #[test]
    fn pipe_conversion_parenthesizes_under_binary_operator() {
        let src = "double = \\x -> x * 2\n\nf = \\x -> 1 + double x\n";
        let module = parse_module(src);
        let off = src.rfind("double x").expect("application");
        let (span, replacement) =
            find_pipe_conversion_at(&module, src, off).expect("pipe action offered");
        assert_eq!(replacement, "(x |> double)");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(parses_cleanly(&out), "pipe rewrite must reparse: {out}");
        assert!(out.contains("1 + (x |> double)"), "got: {out}");
    }

    /// Bug 1 (control): top-level applications keep the bare pipe form.
    #[test]
    fn pipe_conversion_stays_bare_outside_operators() {
        let src = "h = \\x -> show x\n";
        let module = parse_module(src);
        let off = src.find("show x").expect("application") + 1;
        let (_, replacement) =
            find_pipe_conversion_at(&module, src, off).expect("pipe action offered");
        assert_eq!(replacement, "x |> show");
    }

    /// B63: converting the inner application of `g (f x)` to pipe form must
    /// parenthesize the pipe. The parser gives the inner `App` a span that
    /// covers the source parens, so an unparenthesized `x |> f` would replace
    /// `(f x)` with `x |> f`, producing `g x |> f` — which parses as
    /// `f (g x)`, silently reversing the application order.
    #[test]
    fn pipe_conversion_parenthesizes_in_argument_position() {
        let src = "k = \\x -> g (f x)\n";
        let module = parse_module(src);
        let off = src.find("f x").expect("inner application"); // on `f`
        let (span, replacement) =
            find_pipe_conversion_at(&module, src, off).expect("pipe action offered");
        assert_eq!(replacement, "(x |> f)");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(parses_cleanly(&out), "pipe rewrite must reparse: {out}");
        assert!(
            out.contains("g (x |> f)"),
            "application order must be preserved: {out}"
        );
    }

    /// Bug 2: adding a wildcard arm to a case whose first arm sits inline on
    /// the `of` line must indent the new arm at the FIRST ARM's column (the
    /// layout block indent), not case-column+2 — the latter is shallower than
    /// the block indent and fails to parse.
    #[test]
    fn add_wildcard_arm_aligns_with_inline_first_arm() {
        let src = "data Color = Red {} | Blue {}\n\nf = \\c -> case c of Red {} -> 1\n";
        let module = parse_module(src);
        let off = src.find("case c").expect("case expr");
        let (span, replacement) =
            find_add_wildcard_arm_at(&module, src, off).expect("wildcard action offered");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(
            parses_cleanly(&out),
            "wildcard arm rewrite must reparse cleanly; got:\n{out}"
        );
        // The new arm must sit at the first arm's column (`Red` is at col 20).
        let arm_col = src.lines().nth(2).unwrap().find("Red").unwrap();
        let last_line = replacement.lines().last().unwrap();
        assert_eq!(
            last_line.len() - last_line.trim_start().len(),
            arm_col,
            "new arm must align with the inline first arm; got {replacement:?}"
        );
    }

    /// Bug 3: `if not a && b …` parses as `(not a) && b`, so the textual
    /// `not `-prefix strip negated only the first conjunct. The whole
    /// condition must be wrapped in `not (…)` instead.
    #[test]
    fn negate_condition_wraps_when_not_binds_only_first_conjunct() {
        let src = "f = \\a b -> if not a && b then 1 else 2\n";
        let module = parse_module(src);
        let off = src.find("if not").expect("if expr");
        let (span, replacement) =
            find_if_negate_at(&module, src, off).expect("negate action offered");
        assert_eq!(replacement, "if not (not a && b) then 2 else 1");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(parses_cleanly(&out), "negate rewrite must reparse: {out}");
    }

    /// Bug 3 (control): when the condition's AST root IS the negation, the
    /// `not` is stripped so the double negation cancels.
    #[test]
    fn negate_condition_strips_root_level_not() {
        let src = "f = \\a -> if not a then 1 else 2\n";
        let module = parse_module(src);
        let off = src.find("if not").expect("if expr");
        let (_, replacement) =
            find_if_negate_at(&module, src, off).expect("negate action offered");
        assert_eq!(replacement, "if a then 2 else 1");
    }

    /// Bug B66: a parenthesized `if` in operand position folds its parens into
    /// the expr span. Replacing the whole `(if …)` span with a bare `if …`
    /// dropped the parens, so the trailing `* 2` bound into the else branch
    /// (`if not c then b else a * 2`). The rewrite must re-wrap in parens.
    #[test]
    fn negate_condition_reparenthesizes_operand_position() {
        let src = "f = \\c a b -> (if c then a else b) * 2\n";
        let module = parse_module(src);
        let off = src.find("if c").expect("if expr");
        let (span, replacement) =
            find_if_negate_at(&module, src, off).expect("negate action offered");
        assert_eq!(replacement, "(if not (c) then b else a)");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert_eq!(out, "f = \\c a b -> (if not (c) then b else a) * 2\n");
        assert!(parses_cleanly(&out), "negate rewrite must reparse: {out}");
    }

    /// Bug 4: flipping a commutative operator whose operand is a keyword form
    /// must parenthesize the moved `if` — keyword forms greedily consume to
    /// their right, so the bare flip `if … else 2 == 0` would swallow `== 0`
    /// into the else branch. (`&&`/`||` are deliberately excluded from the
    /// flippable set — see `flip_operands_excludes_short_circuit_ops` — so
    /// this uses `==`, which evaluates both operands unconditionally.)
    #[test]
    fn flip_operands_parenthesizes_keyword_form_operand() {
        let src = "g = \\x -> 0 == if x then 1 else 2\n";
        let module = parse_module(src);
        let off = src.find("0 ==").expect("lhs operand");
        let (span, replacement) =
            find_flip_binary_at(&module, src, off).expect("flip action offered");
        assert_eq!(replacement, "(if x then 1 else 2) == 0");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert!(parses_cleanly(&out), "flip rewrite must reparse: {out}");
    }

    /// Bug B64a: `a / b * c` parses as `(a / b) * c`. Flipping the `*`'s
    /// operands must produce `c * (a / b)`, not `c * a / b` — the latter
    /// reparses as `(c * a) / b`, a re-association that changes the value
    /// under integer division (a=1,b=2,c=4: 0 vs 2). The moved `a / b`
    /// operand shares `*`'s precedence, so it must be parenthesized.
    #[test]
    fn flip_operands_parenthesizes_reassociating_operand() {
        let src = "f = \\a b c -> a / b * c\n";
        let module = parse_module(src);
        let off = src.find("* c").expect("mul operator");
        let (span, replacement) =
            find_flip_binary_at(&module, src, off).expect("flip action offered");
        assert_eq!(replacement, "c * (a / b)");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert_eq!(out, "f = \\a b c -> c * (a / b)\n");
        assert!(parses_cleanly(&out), "flip rewrite must reparse: {out}");
    }

    /// Bug B64b: the parser folds enclosing parens into a binary expression's
    /// own span, so `f (a == b)` has an `==` node whose span covers `(a == b)`.
    /// Flipping must preserve the parens (`f (b == a)`); dropping them yields
    /// `f b == a`, which reparses as `(f b) == a` — a completely different
    /// expression.
    #[test]
    fn flip_operands_preserves_enclosing_parens() {
        let src = "f = \\a b -> f (a == b)\n";
        let module = parse_module(src);
        let off = src.find("a ==").expect("eq operand");
        let (span, replacement) =
            find_flip_binary_at(&module, src, off).expect("flip action offered");
        assert_eq!(replacement, "b == a");
        let mut out = src.to_string();
        out.replace_range(span.start..span.end, &replacement);
        assert_eq!(out, "f = \\a b -> f (b == a)\n");
        assert!(parses_cleanly(&out), "flip rewrite must reparse: {out}");
    }

    /// `&&` / `||` must NOT be flippable: they short-circuit, so swapping
    /// operands can change evaluation semantics — e.g. flipping the
    /// divide-by-zero guard `x != 0 && 10 / x > 1` would evaluate `10 / x`
    /// first and panic.
    #[test]
    fn flip_operands_excludes_short_circuit_ops() {
        for (src, needle) in [
            ("g = \\x -> x != 0 && 10 / x > 1\n", "&&"),
            ("g = \\a b -> a || b\n", "||"),
        ] {
            let module = parse_module(src);
            let off = src.find(needle).expect("operator present");
            assert!(
                find_flip_binary_at(&module, src, off).is_none(),
                "short-circuit operator `{needle}` must not be flippable: {src}"
            );
        }
    }

    /// Bug 5a: inlining a `with` binding that sits inline on the `do` line
    /// must not delete `main = do` itself — only the statement's own text
    /// goes.
    #[test]
    fn inline_variable_on_do_line_keeps_do_header() {
        let mut tw = TestWorkspace::new();
        let src = "main = do with {y 5} (do yield (y + 1))\n";
        assert!(parses_cleanly(src), "fixture must parse");
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "y ");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Inline"))
            .expect("inline action offered");
        let out = apply_edits_to(src, edits_for(action, &uri));
        assert!(
            out.contains("main = do"),
            "`main = do` header was deleted:\n{out}"
        );
        assert!(out.contains("yield (5 + 1)"), "usage not inlined:\n{out}");
        assert!(parses_cleanly(&out), "inline result must reparse:\n{out}");
    }

    /// Bug 5a (control): a `with` statement on its own line still unwraps to
    /// the rewritten body, dropping the whole statement.
    #[test]
    fn inline_variable_own_line_removes_whole_line() {
        let mut tw = TestWorkspace::new();
        let src = "main = do\n  with {y 5} (do\n    yield (y + 1))\n";
        assert!(parses_cleanly(src), "fixture must parse");
        let uri = tw.open("main", src);
        let pos = tw.position_of(&uri, "y ");
        let actions = handle_code_action(
            &tw.state,
            &params_for(&uri, Range { start: pos, end: pos }),
        )
        .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Inline"))
            .expect("inline action offered");
        let out = apply_edits_to(src, edits_for(action, &uri));
        // The `with` statement unwraps to its (rewritten) body: the inner
        // `do` stays, the binding line disappears wholesale.
        assert_eq!(out, "main = do\n  do\n    yield (5 + 1)\n");
        assert!(parses_cleanly(&out), "inline result must reparse:\n{out}");
    }

    /// Bug 5b: extracting to let from a statement that sits inline on the
    /// `do` line must insert at the statement's own offset (after `do `),
    /// not at column 0 before the declaration.
    #[test]
    fn extract_to_with_on_do_line_wraps_after_do() {
        let mut tw = TestWorkspace::new();
        let src = "main = do yield (1 + 2)\n";
        assert!(parses_cleanly(src), "fixture must parse");
        let uri = tw.open("main", src);
        let doc_source = tw.doc(&uri).source.clone();
        let off = doc_source.find("1 + 2").expect("selection");
        let range = Range {
            start: crate::utils::offset_to_position(&doc_source, off),
            end: crate::utils::offset_to_position(&doc_source, off + "1 + 2".len()),
        };
        let actions = handle_code_action(&tw.state, &params_for(&uri, range))
            .unwrap_or_default();
        let action = action_titled(&actions, |t| t.starts_with("Extract to with"))
            .expect("extract-to-with offered inside do block");
        let out = apply_edits_to(src, edits_for(action, &uri));
        assert!(
            out.starts_with("main = do with {extracted (1 + 2)} (do"),
            "with-binding must be inserted after `do `, wrapping the statement:\\n{out}"
        );
        assert!(
            out.contains("yield (extracted)"),
            "the selected expression must be replaced by the bound name:\\n{out}"
        );
        assert!(parses_cleanly(&out), "extract result must reparse:\\n{out}");
    }

    /// Bug 6: the "Wrap IO in `fork`" quickfix never fixed the IO-in-atomic
    /// diagnostic (fork propagates its argument's effects) and re-offered
    /// itself forever on the inner span. It must no longer be offered.
    #[test]
    fn no_fork_quickfix_for_io_in_atomic() {
        let mut tw = TestWorkspace::new();
        let src = "main = atomic (println \"hi\")\n";
        let uri = tw.open("main", src);
        let doc_source = tw.doc(&uri).source.clone();
        let off = doc_source.find("println").expect("io call");
        let range = Range {
            start: crate::utils::offset_to_position(&doc_source, off),
            end: crate::utils::offset_to_position(&doc_source, off + "println".len()),
        };
        let diag = Diagnostic {
            range,
            message: "IO effects are not allowed inside atomic blocks".into(),
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
        let actions = handle_code_action(&tw.state, &params).unwrap_or_default();
        assert!(
            action_titled(&actions, |t| t.contains("fork")).is_none(),
            "the ineffective fork quickfix must not be offered"
        );
        // The effective fix (unwrapping `atomic`) remains available.
        assert!(
            action_titled(&actions, |t| t == "Remove `atomic` wrapper").is_some(),
            "remove-atomic quickfix should still be offered"
        );
    }
}
